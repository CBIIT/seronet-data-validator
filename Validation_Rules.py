bio_type_list = ["Serum",  "EDTA Plasma",  "PBMC",  "Saliva",  "Nasal swab"]


def Validation_Rules(re, datetime, current_object, data_table, file_name, valid_cbc_ids, drop_list):
    col_list = current_object.Data_Object_Table[file_name]["Column_List"]
    if len(col_list) > 0:
        data_table.drop_duplicates(col_list, inplace=True)
    data_table.reset_index(inplace=True)
    data_table.drop(columns="index", inplace=True)
    data_table = data_table.apply(lambda x: x.replace('–', '-'))

    min_date = datetime.date(1900,  1,  1)
    max_date = datetime.date.today()
    curr_year = max_date.year
    for header_name in data_table.columns:
        if header_name in drop_list:
            continue
        Rule_Found = [True]*2
########################################################################################################################
        Required_column, Rule_Found = check_ID_validation(header_name, current_object, file_name, data_table, re,
                                                          valid_cbc_ids, Rule_Found, 0)
        if file_name in ["prior_clinical_test.csv"]:
            Required_column, Rule_Found = check_prior_clinical(header_name, current_object, data_table, file_name,
                                                               datetime, max_date, curr_year, Rule_Found, 1)
        if file_name in ["demographic.csv"]:
            Required_column, Rule_Found = check_demographic(header_name, current_object, data_table, file_name,
                                                            datetime, curr_year, max_date, Rule_Found, 1)
        if file_name in ["biospecimen.csv"]:
            Required_column, Rule_Found = check_biospecimen(header_name, current_object, data_table, file_name,
                                                            datetime, max_date, curr_year, Rule_Found, 1)
        if file_name in ["aliquot.csv", "equipment.csv", "reagent.csv", "consumable.csv"]:
            Required_column, Rule_Found = check_processing_rules(header_name, current_object, data_table, file_name,
                                                                 datetime, max_date, Rule_Found, 1)
        if file_name in ["confirmatory_clinical_test.csv", "serology_confirmation_test_results.csv",
                         "assay_validation.csv"]:
            Required_column, Rule_Found = check_confimation_rules(header_name, current_object, data_table, file_name,
                                                                  datetime, min_date, max_date, Rule_Found, 1, re)
        if file_name in ["assay.csv", "assay_target.csv", "assay_qc.csv"]:
            Required_column, Rule_Found = check_assay_rules(header_name, current_object, data_table, file_name,
                                                            Rule_Found, 1)
        if file_name in ["biorepository_id_map.csv", "reference_panel.csv"]:
            Required_column, Rule_Found = check_biorepo_rules(header_name, current_object, data_table, file_name,
                                                              Rule_Found, 1, valid_cbc_ids)
###################################################################################################################
        if (header_name in ['Total_Cells_Hemocytometer_Count', 'Total_Cells_Automated_Count']):
            current_object.compare_total_to_live(file_name, data_table, header_name)
        if (header_name in ['Viability_Hemocytometer_Count',  'Viability_Automated_Count']):
            current_object.compare_viability(file_name, data_table, header_name)
        if header_name in ["Comments"]:   # must be a non-empty string,  N/A is allowed if no comments
            Required_column = "No"
            Rule_Found = [True]*2
            current_object.check_if_string(file_name, data_table, header_name, "None", "None", True)
        if True not in Rule_Found:
            print("Column_Name: " + header_name + " has no validation rules set")
        else:
            current_object.get_missing_values(file_name, data_table, header_name, Required_column)
    if ('Research_Participant_ID' in data_table.columns) and ('Research_Participant_ID' not in drop_list):
        current_object.Part_List.append(file_name)
    if ('Biospecimen_ID' in data_table.columns) and ('Biospecimen_ID' not in drop_list):
        current_object.Bio_List.append(file_name)
    return current_object


def compare_tests(current_object):
    file_list = current_object.Part_List      # list of files with Research Part ID
    if ("prior_clinical_test.csv" in file_list) and ("confirmatory_clinical_test.csv" in file_list):
        prior_data = current_object.Data_Object_Table["prior_clinical_test.csv"]["Data_Table"]
        confirm_data = current_object.Data_Object_Table["confirmatory_clinical_test.csv"]["Data_Table"]
        assay_data = current_object.Data_Object_Table["assay.csv"]["Data_Table"]
        merged_data = prior_data.merge(confirm_data, on="Research_Participant_ID", how="outer")
        merged_data = merged_data.merge(assay_data, on=["Assay_ID", "Assay_Target_Organism"], how="left")
        merged_data = merged_data[["Research_Participant_ID", "SARS_CoV_2_PCR_Test_Result",
                                   "Assay_Target_Organism", "Interpretation"]]
        part_list = list(set(merged_data["Research_Participant_ID"].tolist()))
        target_virus = ["SARS-CoV-2 Virus", "SARS-COV-2", "SARS-CoV-2"]

        header = "Research_Participant_ID"
        for iterP in part_list:
            curr_part = merged_data.query("Research_Participant_ID == @iterP and Assay_Target_Organism in @target_virus")
            if len(curr_part) == 0:     # SARS_Cov-2 confirm test is missing
                error_msg = "Participant is Missing SARS_Cov-2 Confirmatory Test"
                curr_part = merged_data.query("Research_Participant_ID == @iterP")
                current_object.add_error_values("Error", "confirmatory_clinical_test.csv", -5,
                                                header, curr_part.iloc[0][header], error_msg)
            else:
                test_res, neg_count = get_curr_tests(curr_part, "Negative", target_virus)
                if (len(neg_count) > 0) and (test_res[0] != len(neg_count)):
                    error_msg = ("Participant has a prior test of SARS-Cov2: Negative, but has one or more " +
                                 "SARS_Cov2 Confimatory Tests that are Positive/Reactive or Indetertimate")
                    current_object.add_error_values("Error", "Prior_Vs_Confirm_Test.csv",
                                                    -5, header, curr_part.iloc[0][header], error_msg)
                test_res, pos_count = get_curr_tests(curr_part, "Positive", target_virus)
                if (len(pos_count) > 0) and (test_res[1] == 0):
                    error_msg = ("Participant has a prior test of SARS-Cov2: Positive, " +
                                 "but all SARS_Cov2 Confimatory Tests are Negative/Non-Reactive or Indetertimate")
                    current_object.add_error_values("Error", "Prior_Vs_Confirm_Test.csv", -5, header,
                                                    curr_part.iloc[0][header], error_msg)


def get_curr_tests(curr_part, prior_stat, target_virus):
    part_test = curr_part[curr_part.apply(lambda x: (x['SARS_CoV_2_PCR_Test_Result'] == prior_stat) and
                                          (x["Assay_Target_Organism"] in target_virus), axis=1)]["Interpretation"]
    lower_list = [i.lower() for i in part_test.tolist()]
    neg_count = 0
    pos_count = 0
    inc_count = 0
    for iterZ in lower_list:
        if (("negative" in iterZ) or
           (("no" in iterZ) and ("reaction" in iterZ)) or
           (("non" in iterZ) and ("reactive" in iterZ))):
            neg_count = neg_count + 1
        elif (("positive" in iterZ) or (("no" not in iterZ) and ("reaction" in iterZ)) or
              (("non" not in iterZ) and ("reactive" in iterZ))):
            pos_count = pos_count + 1
        else:
            inc_count = inc_count + 1
    part_count = curr_part.query("SARS_CoV_2_PCR_Test_Result == @prior_stat")
    return (neg_count, pos_count, inc_count), part_count


def check_ID_Cross_Sheet(current_object, re):
    current_object.get_all_unique_ids(re)
    current_object.get_passing_part_ids()
    current_object.get_cross_sheet_ID(re, 'Research_Participant_ID', '[_]{1}[0-9]{6}$', "Cross_Participant_ID.csv")
    current_object.get_cross_sheet_ID(re, 'Biospecimen_ID', '[_]{1}[0-9]{6}[_]{1}[0-9]{3}$', "Cross_Biospecimen_ID.csv")


def check_ID_validation(header_name, current_object, file_name, data_table, re, valid_cbc_ids,
                        Rule_Found, index, Required_column="Yes"):
    if header_name in ['Research_Participant_ID']:
        pattern_str = '[_]{1}[0-9]{6}$'
        current_object.check_id_field(file_name, data_table, re, header_name, pattern_str, valid_cbc_ids, "XX_XXXXXX")
        if (file_name not in ["biospecimen.csv", "confirmatory_clinical_test.csv"]):
            current_object.check_for_dup_ids(file_name, header_name)
    elif header_name in ["Biospecimen_ID"]:
        pattern_str = '[_]{1}[0-9]{6}[_]{1}[0-9]{3}$'
        current_object.check_id_field(file_name, data_table, re, header_name, pattern_str, valid_cbc_ids, "XX_XXXXXX_XXX")
        if (header_name in ['Research_Participant_ID']) and (header_name in ["Biospecimen_ID"]):
            current_object.check_if_substr(data_table, "Research_Participant_ID", "Biospecimen_ID", file_name, header_name)
        if file_name in ["biospecimen.csv"]:
            current_object.check_for_dup_ids(file_name, header_name)
    elif header_name in ["Aliquot_ID", "CBC_Biospecimen_Aliquot_ID"]:
        pattern_str = '[_]{1}[0-9]{6}[_]{1}[0-9]{3}[_]{1}[0-9]{1,2}$'
        current_object.check_id_field(file_name, data_table, re, header_name, pattern_str, valid_cbc_ids, "XX_XXXXXX_XXX_XX")

        if ("Aliquot_ID" in data_table.columns) and ("Biospecimen_ID" in data_table.columns):
            current_object.check_if_substr(data_table, "Biospecimen_ID", "Aliquot_ID", file_name, header_name)
        if ("Aliquot_ID" in data_table.columns):
            current_object.check_for_dup_ids(file_name, header_name)
    elif header_name in ["Assay_ID"]:
        pattern_str = '[_]{1}[0-9]{3}$'
        current_object.check_id_field(file_name, data_table, re, header_name, pattern_str, valid_cbc_ids, "XX_XXX")
        current_object.check_assay_special(data_table, header_name, "assay.csv", file_name, re)
        if file_name in ["assay.csv"]:
            current_object.check_for_dup_ids(file_name, header_name)
    elif header_name in ["Biorepository_ID", "Parent_Biorepository__ID"]:
        pattern_str = 'LP[0-9]{5}[ ]{1}0001$'
        current_object.check_id_field(file_name, data_table, re, header_name, pattern_str, valid_cbc_ids, "LPXXXXXX 0001")
    elif header_name in ["Subaliquot_ID"]:
        pattern_str = 'LP[0-9]{5}[ ]{1}1[0-9]{3}$'
        current_object.check_id_field(file_name, data_table, re, header_name, pattern_str, valid_cbc_ids, "LPXXXXXX 1XXX")
        current_object.check_for_dup_ids(file_name, header_name)
        if ("Biorepository_ID" in data_table.columns):
            current_object.check_if_substr_2(data_table, "Biorepository_ID", "Subaliquot_ID", file_name, header_name)
        elif ("Parent_Biorepository_ID" in data_table.columns):
            current_object.check_if_substr_2(data_table, "Parent_Biorepository_ID", "Subaliquot_ID", file_name, header_name)
    elif header_name in "Reporting_Laboratory_ID":
        current_object.check_if_cbc_num(file_name, header_name, data_table, valid_cbc_ids)
    else:
        Rule_Found[index] = False
    return Required_column, Rule_Found


def check_prior_clinical(header_name, current_object, data_table, file_name, datetime, max_date, curr_year,
                         Rule_Found, index, Required_column="Yes"):
    if header_name in ['SARS_CoV_2_PCR_Test_Result_Provenance']:
        list_values = ['From Medical Record', 'Self-Reported']
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif header_name in ['SARS_CoV_2_PCR_Test_Result']:
        list_values = ['Positive', 'Negative']
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif 'Test_Result_Provenance' in header_name:  # checks result proveance for valid input options
        Required_column = "Yes: SARS-Negative"
        list_values = ['Self-Reported', 'From Medical Record', 'N/A']
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif ('Test_Result' in header_name) or (header_name in ["Seasonal_Coronavirus_Serology_Result",
                                                            "Seasonal_Coronavirus_Molecular_Result"]):
        Required_column = "Yes: SARS-Negative"
        pos_list = ['Positive', 'Negative', 'Equivocal', 'Not Performed', 'N/A']
        neg_list = ['Positive', 'Negative', 'Equivocal', 'Not Performed']
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Positive"], pos_list)
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Negative"], neg_list)
        current_object.unknown_list_dependancy(file_name, header_name, data_table,
                                               'SARS_CoV_2_PCR_Test_Result', ["Positive", "Negative"])
    elif ('infection_unit' in header_name) or ('HAART_Therapy_unit' in header_name):
        Required_column = "No"
        duration_name = header_name.replace('_unit', '')
        current_object.check_in_list(file_name, data_table, header_name, duration_name, "Is A Number",
                                     ["Day", "Month", "Year"])
        current_object.check_in_list(file_name, data_table, header_name, duration_name, ["N/A"], ["N/A"])
        current_object.unknow_number_dependancy(file_name, header_name, data_table, duration_name, ["N/A"])
    elif ('Duration_of' in header_name) and (('infection' in header_name) or ("HAART_Therapy" in header_name)):
        Required_column = "No"
        if 'HAART_Therapy' in header_name:
            current_name = 'On_HAART_Therapy'
        else:
            current_name = header_name.replace("Duration_of_Current", 'Current')
            current_name = current_name.replace('Duration_of', 'Current')

        current_object.check_in_list(file_name, data_table, header_name, current_name, ['No', 'Unknown', 'N/A'], ["N/A"])
        current_object.check_if_number(file_name, data_table, header_name, current_name, ['Yes'], True, 0, 365, "int")
        current_object.unknow_number_dependancy(file_name, header_name, data_table, current_name, ['Yes', 'No', 'Unknown', 'N/A'])
    elif (('Current' in header_name) and ('infection' in header_name)) or (header_name in ["On_HAART_Therapy"]):
        Required_column = "Yes: SARS-Negative"
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Positive"], ['Yes', 'No', 'Unknown', 'N/A'])
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Negative"], ['Yes', 'No', 'Unknown'])
        current_object.unknown_list_dependancy(file_name, header_name, data_table,
                                               'SARS_CoV_2_PCR_Test_Result', ["Positive", "Negative"])
    else:
        Duration_Rules = []
        if ("SARS_CoV_2_PCR" in header_name):
            Duration_Rules = get_duration("SARS_CoV_2_PCR", "Sample_Collection")
        elif ("CMV_Serology" in header_name):
            Duration_Rules = get_duration("CMV", "Serology_Test")
        elif ("CMV_Molecular" in header_name):
            Duration_Rules = get_duration("CMV", "Molecular_Test")
        elif ("EBV_Serology" in header_name):
            Duration_Rules = get_duration("EBV", "Serology_Test")
        elif ("EBV_Molecular" in header_name):
            Duration_Rules = get_duration("EBV", "Molecular_Test")
        elif ("HIV_Serology" in header_name):
            Duration_Rules = get_duration("EBV", "Serology_Test")
        elif ("HIV_Molecular" in header_name):
            Duration_Rules = get_duration("EBV", "Molecular_Test")
        elif ("HepB_Serology" in header_name):
            Duration_Rules = get_duration("HepB", "Serology_Test")
        elif ("HepB_sAg" in header_name):
            Duration_Rules = get_duration("HepB", "sAg")
        elif ("Seasonal_Coronavirus" in header_name):
            Duration_Rules = get_duration("EBV", "Serology_Test")
        elif ("Seasonal_Coronavirus" in header_name):
            Duration_Rules = get_duration("EBV", "Molecular_Test")

        if len(Duration_Rules) > 0:
            current_object.check_duration_rules(file_name, data_table, header_name, "None", "None",
                                                max_date, curr_year, Duration_Rules)
        else:
            Rule_Found[index] = False
    return Required_column, Rule_Found


def check_demographic(header_name, current_object, data_table, file_name, datetime, curr_year, max_date,
                      Rule_Found, index, Required_column="Yes"):
    if (header_name in ['Age']):
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", False, 1, 200, "int")
    elif (header_name in ['Race', 'Ethnicity', 'Gender']):
        if (header_name in ['Race']):
            list_values = ['White', 'American Indian or Alaska Native', 'Black or African American', 'Asian',
                           'Native Hawaiian or Other Pacific Islander', 'Other', 'Multirace', 'Not Reported', 'Unknown']
        elif (header_name in ['Ethnicity']):
            list_values = ['Hispanic or Latino', 'Not Hispanic or Latino', 'Unknown',  'Not Reported']
        elif (header_name in ['Gender']):
            list_values = ['Male',  'Female',  'Other', 'Not Reported',  'Unknown']
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif (header_name in ['Is_Symptomatic']):
        Required_column = "Yes: SARS-Positive"
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Positive"], ['Yes', 'No'])
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Negative"], ['No', 'N/A'])
        current_object.unknown_list_dependancy(file_name, header_name, data_table,
                                               "SARS_CoV_2_PCR_Test_Result", ['Positive', 'Negative'])
    elif ("Post_Symptom_Onset" in header_name) or (header_name in ['Symptom_Onset_Year']):
        Required_column = "Yes: SARS-Positive"
        current_object.check_in_list(file_name, data_table, header_name, "Is_Symptomatic", ["No", "N/A"], ["N/A"])
        current_object.unknown_list_dependancy(file_name, header_name, data_table, "Is_Symptomatic", ["Yes", "No", "N/A"])
        Duration_Rules = ['Post_Symptom_Onset_Duration', 'Post_Symptom_Onset_Duration_Unit',
                          'Symptom_Onset_Year']
        current_object.check_duration_rules(file_name, data_table, header_name, "Is_Symptomatic", ["Yes"],
                                            max_date, curr_year, Duration_Rules)
    elif ("Post_Symptom_Resolution" in header_name) or (header_name in ['Symptom_Resolution_Year']):
        Required_column = "Yes: SARS-Positive"
        current_object.check_in_list(file_name, data_table, header_name, "Symptoms_Resolved", ["No", "N/A"], ["N/A"])
        current_object.unknown_list_dependancy(file_name, header_name, data_table, "Symptoms_Resolved", ["Yes", "No", "N/A"])
        Duration_Rules = ['Post_Symptom_Resolution_Duration', 'Post_Symptom_Resolution_Duration_Unit',
                          'Symptom_Resolution_Year']
        current_object.check_duration_rules(file_name, data_table, header_name, "Symptoms_Resolved", ["Yes"],
                                            max_date, curr_year, Duration_Rules)
    elif (header_name in ['Symptoms_Resolved']):
        Required_column = "Yes: SARS-Positive"
        current_object.check_in_list(file_name, data_table, header_name, "Is_Symptomatic", ["Yes"], ["Yes", "No"])
        current_object.check_in_list(file_name, data_table, header_name, "Is_Symptomatic", ["No", "N/A"], ["N/A"])
        current_object.unknown_list_dependancy(file_name, header_name, data_table, 'Is_Symptomatic', ["Yes", "No", "N/A"])
    elif (header_name in ['Covid_Disease_Severity']):
        Required_column = "Yes: SARS-Positive"
        current_object.check_if_number(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                       ["Positive"], False, 1, 8, "int")
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Negative"], [0])
        current_object.unknown_list_dependancy(file_name, header_name, data_table,
                                               "SARS_CoV_2_PCR_Test_Result", ['Positive', 'Negative'])
    elif (header_name in ["Diabetes_Mellitus", "Hypertension", "Severe_Obesity", "Cardiovascular_Disease",
                          "Chronic_Renal_Disease", "Chronic_Liver_Disease", "Chronic_Lung_Disease",
                          "Immunosuppressive_conditions", "Autoimmune_condition", "Inflammatory_Disease"]):
        Required_column = "Yes: SARS-Positive"
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Positive"], ['Yes', 'No', "Unknown"])
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Negative"], ["Yes", "No", "Unknown"])
        current_object.unknown_list_dependancy(file_name, header_name, data_table,
                                               "SARS_CoV_2_PCR_Test_Result", ['Positive', 'Negative'])
    elif (header_name in ["Other_Comorbidity"]):
        Required_column = "No"
        current_object.check_icd10(file_name, data_table, header_name)
    else:
        Rule_Found[index] = False
    return Required_column, Rule_Found


def check_biospecimen(header_name, current_object, data_table, file_name, datetime, max_date, curr_year, Rule_Found,
                      index, Required_column="Yes"):
    if(header_name in ["Biospecimen_Group"]):
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Positive"], ['Positive Sample'])
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Negative"], ['Negative Sample'])
        current_object.unknown_list_dependancy(file_name, header_name, data_table,
                                               "SARS_CoV_2_PCR_Test_Result", ['Positive', 'Negative'])
    elif(header_name in ["Biospecimen_Type"]):
        list_values = ["Serum",  "EDTA Plasma",  "PBMC",  "Saliva",  "Nasal swab"]
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif(header_name in ["Initial_Volume_of_Biospecimen (mL)"]):
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", True, 0, 1e9, "float")
    elif(header_name in ["Biospecimen_Collection_Year"]):
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", True, 1900, curr_year, "int")
    elif (header_name in ['Collection_Tube_Type_Expiration_Date']):
        Required_column = "No"
        current_object.check_date(datetime, file_name, data_table, header_name, "None", "None", True, "Date",
                                  max_date, datetime.date(3000, 1, 1))
    elif (header_name in ['Collection_Tube_Type_Lot_Number']):
        Required_column = "No"
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", True)
    elif ('Biospecimen_Processing_Batch_ID' in header_name):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", False)
    elif(header_name in ["Storage_Time_at_2_8_Degrees_Celsius"]):
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", True, 0, 1000, "float")
    elif(header_name in ["Storage_Start_Time_at_2-8_Initials", "Storage_End_Time_at_2-8_Initials"]):
        current_object.check_if_string(file_name, data_table, header_name, "Storage_Time_at_2_8_Degrees_Celsius", "Is A Number", False)
        current_object.check_in_list(file_name, data_table, header_name, "Storage_Time_at_2_8_Degrees_Celsius", ["N/A"], ['N/A'])
        current_object.unknow_number_dependancy(file_name, header_name, data_table, "Storage_Time_at_2_8_Degrees_Celsius", ['N/A'])
    elif ((header_name.find('Company_Clinic') > -1) or (header_name.find('Initials') > -1)
          or (header_name.find('Collection_Tube_Type') > -1)):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", False)
    elif(header_name.find('Hemocytometer_Count') > -1) or (header_name.find('Automated_Count') > -1):
        current_object.check_if_number(file_name, data_table, header_name, "Biospecimen_Type", ["PBMC"],
                                       True, 0, 1e9, "float")
        current_object.unknown_list_dependancy(file_name, header_name, data_table, "Biospecimen_Type", bio_type_list)
    elif(header_name in ["Centrifugation_Time (min)", "RT_Serum_Clotting_Time (min)"]):
        current_object.check_if_number(file_name, data_table, header_name, "Biospecimen_Type", ["Serum"],
                                       True, 0, 1e9, "float")
        current_object.unknown_list_dependancy(file_name, header_name, data_table, "Biospecimen_Type", bio_type_list)
    elif ("Duration_Units" in header_name):
        Required_column = "Yes"
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", ['Minute', 'Hour', 'Day'])
    elif ("Duration" in header_name):
        Required_column = "Yes"
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", False, 0, 500, "float")
    else:
        Rule_Found[index] = False
    return Required_column, Rule_Found


def check_processing_rules(header_name, current_object, data_table, file_name, datetime, max_date,
                           Rule_Found, index, Required_column="Yes"):
    if (header_name in ["Aliquot_Volume"]):
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", True, 0, 1e9, "float")
    elif (header_name in ["Aliquot_Concentration (cells/mL)"]):
        current_object.check_if_number(file_name, data_table, header_name, "Biospecimen_Type", ["PBMC"],
                                       True, 0, 1e9, "float")
        current_object.unknown_list_dependancy(file_name, header_name, data_table, "Biospecimen_Type", bio_type_list)
        current_object.check_in_list(file_name, data_table, header_name, "Biospecimen_Type",
                                     ["Serum", "EDTA Plasma", "Saliva", "Nasal swab"], ["N/A"])
        current_object.unknown_list_dependancy(file_name, header_name, data_table, "Biospecimen_Type", bio_type_list)
    elif ('Expiration_Date' in header_name) or ('Calibration_Due_Date' in header_name):
        Required_column = "No"
        current_object.check_date(datetime, file_name, data_table, header_name, "None", "None", True, "Date",
                                  max_date, datetime.date(3000, 1, 1))
    elif ('Lot_Number' in header_name) or ('Catalog_Number' in header_name):
        Required_column = "No"
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", True)
    elif (header_name in ["Equipment_Type", "Reagent_Name", "Consumable_Name"]):
        if (header_name in ["Equipment_Type"]):
            list_values = ['Refrigerator', '-80 Refrigerator', 'LN Refrigerator', 'Microsope', 'Pipettor',
                           'Controlled-Rate Freezer', 'Automated-Cell Counter', '-80 Freezer',
                           'LN Freezer', 'Centrifuge', 'Microscope']

        elif (header_name in ["Reagent_Name"]):
            list_values = (['DPBS', 'Ficoll-Hypaque', 'RPMI-1640, no L-Glutamine', 'Fetal Bovine Serum',
                            '200 mM L-Glutamine', '1M Hepes', 'Penicillin/Streptomycin',
                            'DMSO, Cell Culture Grade', 'Vital Stain Dye'])
        elif (header_name in ["Consumable_Name"]):
            list_values = ["50 mL Polypropylene Tube", "15 mL Conical Tube", "Cryovial Label",
                           "2mL Cryovial"]
        current_object.check_in_list(file_name, data_table, header_name, "Biospecimen_Type", ["PBMC"], list_values)
        current_object.unknown_list_dependancy(file_name, header_name, data_table, "Biospecimen_Type", bio_type_list)
    elif ("Aliquot" in header_name) or ("Equipment_ID" in header_name):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", False)
    else:
        Rule_Found[index] = False
    return Required_column, Rule_Found


def check_confimation_rules(header_name, current_object, data_table, file_name, datetime, min_date, max_date,
                            Rule_Found, index, re, Required_column="Yes"):
    if header_name in ["Assay_Target"]:
        current_object.check_assay_special(data_table, header_name, "assay_target.csv", file_name, re)
    elif (header_name in ["Instrument_ID", "Test_Operator_Initials", "Assay_Kit_Lot_Number"]):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", False)
    elif ('Test_Batch_ID' in header_name):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", False)
    elif ("Assay_Target_Organism" in header_name):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", False)
        current_object.check_assay_special(data_table, header_name, "assay.csv", file_name, re)
    elif (header_name in ["Assay_Target_Sub_Region", "Measurand_Antibody", "Derived_Result"]):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", True)
    elif (header_name in ["Interpretation"]):
        list_values = ['positive', 'negative', 'reactive', 'reaction']
        current_object.check_interpertation(file_name, data_table, header_name, list_values)
    elif (header_name in ["Assay_Replicate", "Sample_Dilution"]):
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", False, 0, 200, "int")
    elif (header_name in ["Raw_Result", "Positive_Control_Reading", "Negative_Control_Reading"]):
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", True, 0, 1e9, "float")
    elif header_name in ["Sample_Type"]:
        list_values = ['Serum', 'Plasma', 'Venous Whole Blood', 'Dried Blood Spot', 'Nasal Swab',
                       'Broncheolar Lavage', 'Sputum']
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif header_name in ["Derived_Result_Units"]:
        current_object.check_if_string(file_name, data_table, header_name, "Derived_Result", "Is A Number", False)
        current_object.check_in_list(file_name, data_table, header_name, "Derived_Result", ["N/A"], ["N/A"])
#        current_object.unknow_number_dependancy(file_name, header_name, data_table, "Derived_Result", ["N/A"])
    elif header_name in ["Raw_Result_Units"]:
        current_object.check_if_string(file_name, data_table, header_name, "Raw_Result", "Is A Number", False)
        current_object.check_in_list(file_name, data_table, header_name, "Raw_Result", ["N/A"], ["N/A"])
#        current_object.unknow_number_dependancy(file_name, header_name, data_table, "Raw_Result", ["N/A"])
    elif ("Biospecimen_Collection_to_Test_Duration" in header_name):
        Required_column = "Yes"
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", False, 0, 500, "float")
    else:
        Rule_Found[index] = False
    return Required_column, Rule_Found


def check_assay_rules(header_name, current_object, data_table, file_name, Rule_Found,
                      index, Required_column="Yes"):
    if (header_name in ["Technology_Type", "Assay_Name", "Assay_Manufacturer", "Assay_Target_Organism"]):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", False)
    elif "Quality_Control" in header_name:
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", False)
    elif (header_name in ["EUA_Status", "Assay_Multiplicity", "Assay_Control_Type", "Measurand_Antibody_Type",
                          "Assay_Result_Type", "Peformance_Statistics_Source", "Assay_Antigen_Source"]):
        if (header_name in ["EUA_Status"]):
            list_values = ['Approved', 'Submitted', 'Not Submitted', 'N/A']
        if (header_name in ["Assay_Multiplicity"]):
            list_values = ['Multiplex', 'Singleplex']
        if (header_name in ["Assay_Control_Type"]):
            list_values = ['Internal', 'External', 'Internal and External', 'N/A']
        if (header_name in ["Measurand_Antibody_Type"]):
            list_values = ['IgG', 'IgM', 'IgA', 'IgG + IgM', 'Total', 'N/A']
        if (header_name in ["Assay_Result_Type"]):
            list_values = ['Qualitative', 'Quantitative', 'Semi-Quantitative']
        if (header_name in ["Peformance_Statistics_Source"]):
            list_values = ['Manufacturer',  'In-house']
        if (header_name in ["Assay_Antigen_Source"]):
            list_values = ['Manufacturer',  'In-house', 'N/A']
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif ("Target_biospecimen_is_" in header_name):
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", ["T", "F"])
    elif (header_name in ["Postive_Control", "Negative_Control", "Calibration_Type", "Calibrator_High_or_Positive",
                          "Calibrator_Low_or_Negative"]):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", True)
    elif (header_name in ["Assay_Result_Unit",  "Cut_Off_Unit",  "Assay_Target"]):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", False)
    elif (header_name in ["Positive_Cut_Off_Threshold",  "Negative_Cut_Off_Ceiling",  "Assay_Target_Sub_Region"]):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", True)
    elif (header_name in ["N_true_positive",  "N_true_negative",  "N_false_positive",  "N_false_negative"]):
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", False, 0, 1e9, "int")
    else:
        Rule_Found[index] = False
    return Required_column, Rule_Found


def check_biorepo_rules(header_name, current_object, data_table, file_name, Rule_Found,
                        index, cbc_list, Required_column="Yes"):
    if header_name in ["Destination"]:   # string
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", False)
    elif header_name in ["Reserved", "Used", "Consented_For_Research_Use"]:
        if header_name in ["Reserved", "Used"]:
            list_values = ["Yes", "No"]
        elif header_name in ["Consented_For_Research_Use"]:
            list_values = ["Yes", "No", "Withdrawn"]
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif header_name in ["Reference_Panel_ID", "Batch_ID"]:
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", False, 0, 1e9, "int")
    elif header_name in ["Panel_Type"]:
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", False)
    elif header_name in ["Destination_ID"]:
        current_object.check_if_cbc_num(file_name, header_name, data_table, cbc_list)
    return Required_column, Rule_Found


def get_duration(duration_col, col_string):
    dur_list = [f'Post_{duration_col}_{col_string}_Duration',
                f'Post_{duration_col}_{col_string}_Duration_Unit',
                f'{duration_col}_{col_string}_Year']
    return dur_list
