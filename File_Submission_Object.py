import icd10
import pandas as pd
from dateutil.parser import parse
import datetime
from termcolor import colored
######################################################################################################################


def clean_up_column_names(header_name):
    header_name = header_name.replace(" (cells/mL)", "")
    header_name = header_name.replace(" (mL)", "")
    header_name = header_name.replace(" (Years)", "")
    header_name = header_name.replace(" (Days)", "")
    header_name = header_name.replace(" (min)", "")
    header_name = header_name.replace(" (hrs)", "")
    header_name = header_name.replace("°C", "")
    header_name = header_name.replace("-80", "80")
    header_name = header_name.replace("-", "_")
    return header_name


def convert_data_type(v):
    if isinstance(v, (datetime.datetime, datetime.time)):
        return v
    if str(v).find('_') > 0:
        return v
    try:
        float(v)
        return float(v)
    except ValueError:
        try:
            return parse(v)
        except ValueError:
            return v


def check_multi_rule(data_table, depend_col, depend_val):
    if len(data_table) == 0:
        error_str = depend_col + " is not found, unable to validate Data "
        return data_table, error_str
    if depend_col not in data_table.columns.to_list():
        error_str = depend_col + " is not found, unable to validate Data "
        data_table = -1
        return data_table, error_str
    if depend_val == "Is A Number":
        data_table = data_table[data_table[depend_col].apply(lambda x: isinstance(x, (float, int)))]
        error_str = depend_col + " is a Number "
    elif depend_val == "Is A Date":
        data_table = data_table[data_table[depend_col].apply(lambda x: isinstance(x, pd.Timestamp))]
        error_str = depend_col + " is a Date "
    else:
        data_table = data_table.query("{0} in @depend_val".format(depend_col))
        error_str = depend_col + " is in " + str(depend_val) + " "
    return data_table, error_str
######################################################################################################################


class Submission_Object:
    def __init__(self, file_name):
        """An Object that contains information for each Submitted File that Passed File_Validation."""
        self.File_Name = file_name
        self.Data_Object_Table = {}
        self.Part_List = []
        self.Bio_List = []
        self.Rule_Count = 0
        self.Column_error_count = pd.DataFrame(columns=["Message_Type", "CSV_Sheet_Name", "Column_Name", "Error_Message"])
        self.Curr_col_errors = pd.DataFrame(columns=["Message_Type", "CSV_Sheet_Name", "Column_Name", "Error_Message"])
        self.Error_list = pd.DataFrame(columns=["Message_Type", "CSV_Sheet_Name", "Row_Index", "Column_Name",
                                                "Column_Value", "Error_Message"])

    def get_data_tables(self, file_name, file_path):
        file_name = file_name.replace(".xlsx", ".csv")
        self.Data_Object_Table[file_name] = {"Data_Table": [], "Column_List": [], "Key_Cols": []}
        try:
            if ".csv" in file_path:
                Data_Table = pd.read_csv(file_path, na_filter=False)
            elif ".xlsx" in file_path:
                Data_Table = pd.read_excel(file_path, na_filter=False)
            else:
                print("Unknown file extension")
                Data_Table = pd.DataFrame()
        except Exception:
            Data_Table = pd.DataFrame()
        self.Data_Object_Table[file_name]["Data_Table"].append(Data_Table)
        self.set_key_cols(file_name)
        if file_name not in ["submission.csv", "shipping_manifest.csv"]:
            self.cleanup_table(file_name)

    def set_key_cols(self, file_name):
        if file_name == "prior_clinical_test.csv":
            col_list = ["Research_Participant_ID", "SARS_CoV_2_PCR_Test_Result"]
        elif file_name == "demographic.csv":
            col_list = ["Research_Participant_ID", "Age"]
        elif file_name == "biospecimen.csv":
            col_list = ["Research_Participant_ID", "Biospecimen_ID", "Biospecimen_Type"]
        elif file_name == "confirmatory_clinical_test.csv":
            col_list = ["Research_Participant_ID", "Assay_ID", "Assay_Target"]
        elif file_name == "aliquot.csv":
            col_list = ["Biospecimen_ID", "Aliquot_ID"]
        elif file_name == "equipment.csv":
            col_list = ["Biospecimen_ID", "Equipment_ID"]
        elif file_name == "reagent.csv":
            col_list = ["Biospecimen_ID", "Reagent_Name"]
        elif file_name == "consumable.csv":
            col_list = ["Biospecimen_ID", "Consumable_Name"]
        elif file_name == "assay.csv":
            col_list = ["Assay_ID", "Assay_Name"]
        elif file_name == "assay_target.csv":
            col_list = ["Assay_ID", "Assay_Target"]
        else:
            col_list = []
        self.Data_Object_Table[file_name]["Key_Cols"] = col_list

    def update_object(self, assay_df, file_name):
        Data_Table = assay_df
        self.Data_Object_Table[file_name] = {"Data_Table": [], "Column_List": [], "Key_Cols": []}
        self.Data_Object_Table[file_name]["Data_Table"].append(Data_Table)
        if isinstance(self.Data_Object_Table[file_name]["Data_Table"], list):
            self.Data_Object_Table[file_name]["Data_Table"] = self.Data_Object_Table[file_name]["Data_Table"][0]
        self.set_key_cols(file_name)

    def cleanup_table(self, file_name):
        curr_table = self.Data_Object_Table[file_name]["Data_Table"][0]
        curr_table.dropna(axis=0, how="all", thresh=None, subset=None, inplace=True)
        if len(curr_table) > 0:
            missing_logic = curr_table.eq(curr_table.iloc[:, 0], axis=0).all(axis=1)
            curr_table = curr_table[[i is not True for i in missing_logic]]
            curr_table = curr_table .loc[:, ~curr_table .columns.str.startswith('Unnamed')]
            for iterC in curr_table.columns:
                try:
                    curr_table[iterC] = curr_table[iterC].apply(lambda x: x.replace('–', '-'))
                except Exception:
                    pass
        self.Data_Object_Table[file_name]["File_Size"] = len(curr_table)
        self.Data_Object_Table[file_name]["Data_Table"] = curr_table

    def column_validation(self, file_name, Support_Files):
        file_name = file_name.replace(".xlsx", ".csv")
        if file_name in ["submission.csv", "shipping_manifest.csv"]:
            return
        header_list = self.Data_Object_Table[file_name]['Data_Table'].columns.tolist()
        check_file = [i for i in Support_Files if file_name.replace('csv', 'xlsx') in i]
#        header_list = [clean_up_column_names(i) for i in header_list]
        self.Data_Object_Table[file_name]['Data_Table'].columns = header_list

        if len(check_file) == 0:
            return
        check_file = pd.read_excel(check_file[0], engine='openpyxl')
        col_list = check_file.columns.tolist()
#        col_list = [clean_up_column_names(i) for i in col_list]

        in_csv_not_excel = [i for i in header_list if i not in col_list]
        in_excel_not_csv = [i for i in col_list if i not in header_list]

        csv_errors = ["Column Found in CSV is not Expected"] * len(in_csv_not_excel)
        excel_errors = ["This Column is Expected and is missing from CSV File"] * len(in_excel_not_csv)
        name_list = [file_name] * (len(in_csv_not_excel) + len(in_excel_not_csv))

        if len(name_list) > 0:
            self.Curr_col_errors["Message_Type"] = ["Error"]*len(name_list)
            self.Curr_col_errors["CSV_Sheet_Name"] = name_list
            self.Curr_col_errors["Column_Name"] = (in_csv_not_excel + in_excel_not_csv)
            self.Curr_col_errors["Error_Message"] = (csv_errors+excel_errors)
            self.Column_error_count = self.Column_error_count.append(self.Curr_col_errors)
            self.Curr_col_errors.drop(labels=range(0, len(name_list)), axis=0, inplace=True)

    def get_submission_metadata(self, Support_Files):
        if "submission.csv" not in self.Data_Object_Table:
            print(colored("Submission File was not included in the list of files to validate", 'red'))
        else:
            try:
                submit_table = self.Data_Object_Table['submission.csv']['Data_Table'][0]
                id_list = [i for i in Support_Files if "SeroNet_Org_IDs.xlsx" in i]
                id_conv = pd.read_excel(id_list[0], engine='openpyxl')
                submit_name = submit_table.columns.values[1]

                self.CBC_ID = id_conv.query("Institution == @submit_name")["Org ID"].tolist()[0]
                self.Submitted_Name = submit_name
                if len(str(self.CBC_ID)) == 0:
                    self.CBC_ID = -1
                self.Submit_Participant_IDs = self.Data_Object_Table['submission.csv']['Data_Table'][0].iloc[1][1]
                self.Submit_Biospecimen_IDs = self.Data_Object_Table['submission.csv']['Data_Table'][0].iloc[2][1]
            except Exception as e:
                print(e)
                self.CBC_ID = -1
                self.Submit_Participant_IDs = "00"
                self.Submit_Biospecimen_IDs = "00"
            if len(self.Submit_Participant_IDs) == 0:
                self.Submit_Participant_IDs = "0"
            if len(self.Submit_Biospecimen_IDs) == 0:
                self.Submit_Biospecimen_IDs = "0"
        if self.CBC_ID > 0:
            print("The CBC Code for " + self.Submitted_Name + " Is: " + str(self.CBC_ID) + "\n")
        else:
            print("The Submitted CBC name: " + self.Submitted_Name + " does NOT exist in the Database")

#    def correct_var_types(self, file_name):
#        data_table = self.Data_Object_Table[file_name]['Data_Table']
#        data_table, drop_list = self.merge_tables(file_name, data_table)
#        col_names = data_table.columns
#        data_table = pd.DataFrame([convert_data_type(c) for c in l] for l in data_table.values)
#        data_table.columns = col_names
#        return data_table, drop_list

    def correct_var_types(self, file_name):
        data_table = self.Data_Object_Table[file_name]['Data_Table']
        data_table, drop_list = self.merge_tables(file_name, data_table)
        col_names = data_table.columns
        for curr_col in col_names:
            if ("Batch_ID" in curr_col) or ("Catalog_Number" in curr_col) or ("Lot_Number" in curr_col):
                data_table[curr_col] = [str(i) for i in data_table[curr_col]]
            elif curr_col in ["Derived_Result", "Equipment_ID", "Instrument_ID"]:
                data_table[curr_col] = [str(i) for i in data_table[curr_col]]
            else:
                data_table[curr_col] = [convert_data_type(c) for c in data_table[curr_col]]
        data_table.columns = col_names
        return data_table, drop_list

    def merge_tables(self, file_name, data_table):
        self.Data_Object_Table[file_name]["Column_List"] = data_table.columns
        if file_name == "prior_clinical_test.csv":
            data_table = self.check_merge(data_table, "demographic.csv", "Research_Participant_ID")
        elif file_name == "demographic.csv":
            data_table = self.check_merge(data_table, "prior_clinical_test.csv", "Research_Participant_ID")
        elif file_name == "biospecimen.csv":
            data_table = self.check_merge(data_table, "prior_clinical_test.csv", "Research_Participant_ID")
            data_table = self.check_merge(data_table, "demographic.csv", "Research_Participant_ID")
        elif file_name in ["aliquot.csv", "equipment.csv", "reagent.csv", "consumable.csv"]:
            data_table = self.check_merge(data_table, "biospecimen.csv", "Biospecimen_ID")
        elif file_name in ["assay_target.csv"]:
            data_table = self.check_merge(data_table, "assay.csv", "Assay_ID")
        if ("Comments_x" in data_table.columns) and ("Comments" not in data_table.columns):
            data_table.rename(columns={"Comments_x": "Comments"}, inplace=True)
        drop_list = [i for i in data_table.columns if i not in self.Data_Object_Table[file_name]["Column_List"]]
        return data_table, drop_list

    def check_merge(self, data_table, table_name, merge_field):
        if table_name in self.Data_Object_Table:
            try:
                data_table = data_table.merge(self.Data_Object_Table[table_name]["Data_Table"],
                                              how='left', on=merge_field)
            except Exception:
                data_table = data_table.merge(self.Data_Object_Table[table_name]["Data_Table"][0],
                                              how='left', on=merge_field)
        return data_table

    def add_error_values(self, msg_type, sheet_name, row_index, col_name, col_value, error_msg):
        new_row = {"Message_Type": msg_type, "CSV_Sheet_Name": sheet_name, "Row_Index": row_index,
                   "Column_Name": col_name, "Column_Value": col_value, "Error_Message": error_msg}
        self.Error_list = self.Error_list.append(new_row, ignore_index=True)

    def sort_and_drop(self, header_name, keep_blank=False):
        self.Error_list.drop_duplicates(["CSV_Sheet_Name", "Row_Index", "Column_Name", "Column_Value"], inplace=True)
#        if keep_blank is False:
#            drop_idx = self.Error_list.query("Column_Name == @header_name and Column_Value == ''").index
#            self.Error_list.drop(drop_idx, inplace=True)

    def update_error_table(self, msg_type, error_data, sheet_name, header_name, error_msg, keep_blank=False):
        for i in error_data.index:
            self.add_error_values(msg_type, sheet_name, i+2, header_name, error_data.loc[i][header_name], error_msg)
        if sheet_name not in ["Cross_Biospecimen_ID.csv", "Cross_Participant_ID.csv"]:
            self.sort_and_drop(header_name, keep_blank)

    def check_for_dependancy(self, data_table, depend_col, depend_val, sheet_name, header_name):
        error_str = "Unexpected Value. "
        if depend_col != "None":
            data_table, error_str = check_multi_rule(data_table, depend_col, depend_val)
        if isinstance(data_table, (int, float)):
            self.add_error_values("Error", sheet_name, 0, header_name, "Entire Column", error_str)
            data_table = []
        return data_table, error_str

    def unknown_list_dependancy(self, sheet_name, header_name, data_table, depend_col, depend_list):
        error_data = data_table.query("{0} not in {1}".format(depend_col, depend_list))
        self.add_unkown_warnings(error_data, sheet_name, header_name, depend_col)

    def unknow_number_dependancy(self, sheet_name, header_name, data_table, depend_col, depend_list):
        data_table = data_table[data_table[depend_col].apply(lambda x: not isinstance(x, (float, int)))]
        error_data = data_table[data_table[depend_col].apply(lambda x: x not in depend_list)]
        self.add_unkown_warnings(error_data, sheet_name, header_name, depend_col)

    def add_unkown_warnings(self, error_data, sheet_name, header_name, depend_col):
        error_msg = depend_col + " is a dependant column and has an invalid value for this record, unable to validate value for " + header_name + " "
        self.update_error_table("Not Validated", error_data, sheet_name, header_name, error_msg, keep_blank=False)

    def check_assay_special(self, data_table, header_name, file_name, sheet_name, re):
        self.Rule_Count = self.Rule_Count + 1
        assay_table = self.Data_Object_Table[file_name]["Data_Table"]
        assay_table.rename(columns={"Target_Organism": "Assay_Target_Organism"}, inplace=True)
        data_table.replace('EBV Nuclear antigen � 1', 'EBV Nuclear antigen - 1', inplace=True)

        curr_assay = assay_table[assay_table["Assay_ID"].apply(lambda x: re.compile('^' + str(self.CBC_ID)
                                                                                    + '_[0-9]{3}').match(str(x)) is not None)]
        error_data = data_table.merge(curr_assay, on=header_name, indicator=True, how="outer")
        error_data = error_data.query("_merge in ['left_only']")
        error_msg = header_name + " is not found in the table of valid " + header_name + " in databse or submitted file"
        self.update_error_table("Error", error_data, sheet_name, header_name, error_msg, keep_blank=False)

    def check_id_field(self, sheet_name, data_table, re, field_name, pattern_str, cbc_id, pattern_error):
        self.Rule_Count = self.Rule_Count + 1
        if field_name in ["Biorepository_ID", "Parent_Biorepository__ID", "Subaliquot_ID"]:
            single_invalid = data_table[data_table[field_name].apply(
                lambda x: re.compile(pattern_str).match(str(x)) is None)]
            wrong_cbc_id = []
            if field_name in ["Subaliquot_ID"]:  # N/A is a valid subaliquot value
                single_invalid = single_invalid.query("Subaliquot_ID not in ['N/A']")
        elif isinstance(cbc_id, list):
            single_invalid = data_table[data_table[field_name].apply(
                lambda x: re.compile('^[0-9]{2}' + pattern_str).match(str(x)) is None)]
            single_good = data_table[data_table[field_name].apply(
                lambda x: re.compile('^[0-9]{2}' + pattern_str).match(str(x)) is not None)]
            wrong_cbc_id = single_good[single_good[field_name].apply(lambda x: int(x[:2]) not in cbc_id)]
        else:
            single_invalid = data_table[data_table[field_name].apply(
                lambda x: re.compile('^[0-9]{2}' + pattern_str).match(str(x)) is None)]
            wrong_cbc_id = data_table[data_table[field_name].apply(
                lambda x: (re.compile('^' + cbc_id + pattern_str).match(str(x)) is None))]

        for i in single_invalid.index:
            if single_invalid[field_name][i] != '':
                error_msg = "ID is Not Valid Format, Expecting " + pattern_error
                self.add_error_values("Error", sheet_name, i+2, field_name, single_invalid[field_name][i], error_msg)
        if len(wrong_cbc_id) > 0:
            for i in wrong_cbc_id.index:
                if isinstance(cbc_id, list):
                    error_msg = ("ID is Valid however CBC code supplied is not expected," +
                                 " ensure first 2 digits are the correct CBC code")
                elif int(cbc_id) == 0:
                    error_msg = "ID is Valid however submission file is missing, unable to validate CBC code"
                else:
                    error_msg = "ID is Valid however has wrong CBC code. Expecting CBC Code (" + str(cbc_id) + ")"
                self.add_error_values("Error", sheet_name, i+2, field_name, wrong_cbc_id[field_name][i], error_msg)
        self.sort_and_drop(field_name)

    def check_if_cbc_num(self, sheet_name, field_name, data_table, cbc_list):
        self.Rule_Count = self.Rule_Count + 1
        wrong_code = data_table[data_table[field_name].apply(lambda x: x not in cbc_list)]
        for i in wrong_code.index:
            error_msg = "Lab ID is not valid, please check against list of approved ID values"
            self.add_error_values("Error", sheet_name, i+2, field_name, wrong_code[field_name][i], error_msg)

    def check_for_dup_ids(self, sheet_name, field_name):
        self.Rule_Count = self.Rule_Count + 1
        if sheet_name in self.Data_Object_Table:
            data_table = self.Data_Object_Table[sheet_name]['Data_Table']
            data_table = data_table[data_table[field_name].apply(lambda x: x not in ["N/A"])]
            table_counts = data_table[field_name].value_counts(dropna=False).to_frame()
            dup_id_count = table_counts[table_counts[field_name] > 1]
            for i in dup_id_count.index:
                error_msg = "Id is repeated " + str(dup_id_count[field_name][i]) + " times, Multiple repeats are not allowed"
                self.add_error_values("Error", sheet_name, -3, field_name, i, error_msg)

    def check_if_substr(self, data_table, id_1, id_2, file_name, header_name):
        self.Rule_Count = self.Rule_Count + 1
        id_compare = data_table[data_table.apply(lambda x: x[id_1] not in x[id_2], axis=1)]
        Error_Message = id_1 + " is not a substring of " + id_2 + ".  Data is not Valid, please check data"
        self.update_error_table("Error", id_compare, file_name, header_name, Error_Message)

    def check_if_substr_2(self, data_table, id_1, id_2, file_name, header_name):
        self.Rule_Count = self.Rule_Count + 1
        id_compare = data_table[data_table.apply(lambda x: str(x[id_1])[0:6] not in str(x[id_2])[0:6], axis=1)]
        Error_Message = id_1 + " is not a substring of " + id_2 + ".  Data is not Valid, please check data"
        id_compare = id_compare.query("Subaliquot_ID not in ['N/A']")
        self.update_error_table("Error", id_compare, file_name, header_name, Error_Message)

    def check_in_list(self, sheet_name, data_table, header_name, depend_col, depend_val, list_values):
        self.Rule_Count = self.Rule_Count + 1
        data_table, error_str = self.check_for_dependancy(data_table, depend_col, depend_val, sheet_name, header_name)
        if len(data_table) > 0:
            error_msg = error_str + "Value must be one of the following: " + str(list_values)
            if list_values == ["N/A"]:
                passing_values = data_table[data_table[header_name].apply(lambda x: x == "N/A")]
            else:
                try:
                    list_values = list(set(list_values + [i.lower() for i in list_values]))
                except Exception:   # list of numbers does not have a lower
                    pass
                query_str = "{0} in @list_values or {0} in ['']".format(header_name)
                passing_values = data_table.query(query_str)
            row_index = [iterI for iterI in data_table.index if (iterI not in passing_values.index)]
            error_data = data_table.loc[row_index]
            self.update_error_table("Error", error_data, sheet_name, header_name, error_msg)

    def check_interpertation(self, sheet_name, data_table, header_name, list_values):
        self.Rule_Count = self.Rule_Count + 1
        error_msg = "Value must contain of the following options: " + str(list_values)
        curr_data = data_table[header_name]
        row_index = []
        for iterC in curr_data.index:
            logic_list = [i for i in list_values if i in curr_data[iterC].lower()]
            if len(logic_list) == 0:
                row_index.append(iterC)
        error_data = data_table.loc[row_index]
        self.update_error_table("Error", error_data, sheet_name, header_name, error_msg)

    def check_date(self, datetime, sheet_name, data_table, header_name, depend_col, depend_val,
                   na_allowed, time_check, lower_lim=0, upper_lim=24):
        self.Rule_Count = self.Rule_Count + 1
        data_table, error_str = self.check_for_dependancy(data_table, depend_col, depend_val, sheet_name, header_name)
        if len(data_table) == 0:
            return{}
        date_only = data_table[header_name].apply(lambda x: isinstance(x, datetime.datetime))
        good_date = data_table[date_only]
        if time_check == "Date":
            error_msg = error_str + "Value must be a Valid Date MM/DD/YYYY"
        else:
            error_msg = error_str + "Value must be a Valid Time HH:MM:SS"
        if na_allowed is False:
            date_logic = data_table[header_name].apply(lambda x: isinstance(x, datetime.datetime) or x in [''])
        else:
            date_logic = data_table[header_name].apply(lambda x: isinstance(x, datetime.datetime) or x in ['N/A', ''])
            error_msg = error_msg + " Or N/A"
        error_data = data_table[[not x for x in date_logic]]
        self.update_error_table("Error", error_data, sheet_name, header_name, error_msg)
        if time_check == "Date":
            to_early = good_date[header_name].apply(lambda x: x.date() < lower_lim)
            to_late = good_date[header_name].apply(lambda x: x.date() > upper_lim)
            if "Expiration_Date" in header_name:
                error_msg = "Expiration Date has already passed, check to make sure date is correct"
                self.update_error_table("Warning", good_date[to_early], sheet_name, header_name, error_msg)
            elif "Calibration_Due_Date" in header_name:
                error_msg = "Calibration Date has already passed, check to make sure date is correct"
                self.update_error_table("Warning", good_date[to_early], sheet_name, header_name, error_msg)
            else:
                error_msg = "Date is valid however must be between " + str(lower_lim) + " and " + str(upper_lim)
                self.update_error_table("Error", good_date[to_early], sheet_name, header_name, error_msg)
            error_msg = "Date is valid however must be between " + str(lower_lim) + " and " + str(upper_lim)
            self.update_error_table("Error", good_date[to_late], sheet_name, header_name, error_msg)

    def check_if_number(self, sheet_name, data_table, header_name, depend_col, depend_val, na_allowed,
                        lower_lim, upper_lim, num_type):
        self.Rule_Count = self.Rule_Count + 1
        data_table, error_str = self.check_for_dependancy(data_table, depend_col, depend_val, sheet_name, header_name)

        if len(data_table) == 0:
            return{}
        error_msg = error_str + "Value must be a number between " + str(lower_lim) + " and " + str(upper_lim)
        data_list = data_table[header_name].tolist()
        for iterD in enumerate(data_list):
            if isinstance(iterD[1], pd.Timestamp):
                time_conv = iterD[1].hour + (iterD[1].minute)/60
                data_table.at[iterD[0], header_name] = time_conv
        number_only = data_table[header_name].apply(lambda x: isinstance(x, (int, float)))
        good_data = data_table[number_only]

        good_logic = data_table[header_name].apply(lambda x: isinstance(x, (int, float)) or x in [''])
        to_low = good_data[header_name].apply(lambda x: x < lower_lim)
        to_high = good_data[header_name].apply(lambda x: x > upper_lim)
        if num_type == "int":
            is_float = good_data[header_name].apply(lambda x: x.is_integer() is False)
            error_msg = (error_str + "Value must be an interger between " + str(lower_lim) + " and " +
                         str(upper_lim) + ", decimal values are not allowed")
            self.update_error_table("Error", good_data[is_float], sheet_name, header_name, error_msg)
        if na_allowed is True:
            good_logic = data_table[header_name].apply(lambda x: isinstance(x, (int, float)) or x in ['N/A', ''])
            error_msg = error_str + " Or N/A"

        error_data = data_table[[not x for x in good_logic]]
        self.update_error_table("Error", error_data, sheet_name, header_name, error_msg)
        self.update_error_table("Error", good_data[to_low], sheet_name, header_name, error_msg)
        self.update_error_table("Error", good_data[to_high], sheet_name, header_name, error_msg)

        if ('Duration_of' in header_name) and (('infection' in header_name) or ("HAART_Therapy" in header_name)):
            warn_data = data_table.query("{0} == 'N/A'".format(header_name))
            warn_msg = f"{depend_col} is in {depend_val} and {header_name} is N/A"
            self.update_error_table("Warning", warn_data, sheet_name, header_name, warn_msg)

    def check_duration_rules(self, file_name, data_table, header_name, depend_col, depend_val,
                             max_date, curr_year, Duration_Rules):
        self.Rule_Count = self.Rule_Count + 1
        if (header_name in [Duration_Rules[0]]):
            self.check_if_number(file_name, data_table, header_name, depend_col, depend_val, True, 0, 100000, "int")
            self.compare_dates_to_curr(file_name, data_table, header_name,
                                       (header_name + "_Unit"), Duration_Rules[2], max_date)
        elif (header_name in [Duration_Rules[1]]):
            if Duration_Rules[1] in data_table.columns:
                self.check_in_list(file_name, data_table, header_name, Duration_Rules[0], ["N/A"], ["N/A"])
                self.check_in_list(file_name, data_table, header_name, Duration_Rules[0], "Is A Number",
                                   ["Day", "Week", "Month", "Year"])
                self.unknow_number_dependancy(file_name, header_name, data_table, Duration_Rules[0], ["N/A"])
        elif (header_name in [Duration_Rules[2]]):
            self.check_in_list(file_name, data_table, header_name, Duration_Rules[0], ["N/A"], ["N/A"])
            self.check_if_number(file_name, data_table, header_name, Duration_Rules[0], "Is A Number",
                                 False, 1900, curr_year, "int")
            self.unknow_number_dependancy(file_name, header_name, data_table, Duration_Rules[0], ["N/A"])

    def compare_dates_to_curr(self, sheet_name, data_table, header_name, unit_name, year_name, curr_date):
        self.Rule_Count = self.Rule_Count + 1
        curr_year = curr_date.year
        curr_month = curr_date.month
        test_data = data_table[data_table[year_name].apply(lambda x: isinstance(x, (int, float)))]
        if unit_name in data_table.columns:
            year_data = test_data.query("{0} == 'Year' or {0} == 'year'".format(unit_name))
            month_data = test_data.query("{0} == 'Month' or {0} == 'month'".format(unit_name))
            day_data = test_data.query("{0} == 'Day' or {0} == 'day'".format(unit_name))

            bad_month = month_data[month_data[header_name] + month_data[year_name]*12 > (curr_year*12 + curr_month)]
            bad_year = year_data[year_data[header_name] + year_data[year_name] > curr_year]
            day_dur = day_data[year_name].apply(lambda x: (curr_date - (datetime.date(int(x), 1, 1))).days)
            bad_day = day_data[day_data[header_name] > day_dur]
            bad_data = pd.concat([bad_day, bad_month, bad_year])
        else:
            unit_name = "days"
            day_dur = test_data[year_name].apply(lambda x: (curr_date - (datetime.date(int(x), 1, 1))).days)
            bad_data = test_data[test_data[header_name] > day_dur]
        for iterZ in bad_data.index:
            error_msg = header_name + " Exists in the Future, not valid combination, Check Duration Units"
            if unit_name == "days":
                error_unit = "Days"
            else:
                error_unit = bad_data.loc[iterZ][unit_name]
            error_val = (error_unit + ": " + str(bad_data.loc[iterZ][header_name]) +
                         ", Year: " + str(bad_data.loc[iterZ][year_name]))
            self.add_error_values("Error", sheet_name, iterZ+2, header_name, error_val, error_msg)

    def compare_total_to_live(self, sheet_name, data_table, header_name):
        self.Rule_Count = self.Rule_Count + 1
        second_col = header_name.replace('Total_Cells', 'Live_Cells')
        data_table, error_str = self.check_for_dependancy(data_table, header_name, "Is A Number", sheet_name, header_name)
        data_table, error_str = self.check_for_dependancy(data_table, second_col, "Is A Number", sheet_name, header_name)
        if len(data_table) == 0:
            return
        error_data = data_table.query("{0} > {1}".format(second_col, header_name))
        for iterZ in error_data.index:
            error_msg = "Total Cell Count must be greater then Live Cell Count (" + str(error_data[second_col][iterZ]) + ")"
            self.add_error_values("Error", sheet_name, iterZ+2, header_name, error_data.loc[iterZ][header_name], error_msg)

    def compare_viability(self, sheet_name, data_table, header_name):
        self.Rule_Count = self.Rule_Count + 1
        live_col = header_name.replace('Viability', 'Live_Cells')
        total_col = header_name.replace('Viability', 'Total_Cells')
        data_table, error_str = self.check_for_dependancy(data_table, header_name, "Is A Number", sheet_name, header_name)
        data_table, error_str = self.check_for_dependancy(data_table, live_col, "Is A Number", sheet_name, header_name)
        data_table, error_str = self.check_for_dependancy(data_table, total_col, "Is A Number", sheet_name, header_name)
        if len(data_table) == 0:
            return

        error_data = data_table[data_table.apply(lambda x: x[total_col] == 0 and x[header_name] not in ['N/A'], axis=1)]
        error_msg = "Total Count is 0, Viability_Count should be N/A"
        self.update_error_table("Warning", error_data, sheet_name, header_name, error_msg)

        data_table = data_table[data_table.apply(lambda x: x[total_col] > 0, axis=1)]
        error_data = data_table[data_table.apply(lambda x: round((x[live_col]/x[total_col])*100, 1) != x[header_name],
                                                 axis=1)]

        for iterZ in error_data.index:
            via_pct = round((error_data[live_col][iterZ] / error_data[total_col][iterZ])*100, 1)
            error_msg = "Viability Count must be (" + str(via_pct) + ") which is (Live_Count / Total_Count) * 100"
            self.add_error_values("Error", sheet_name, iterZ+2, header_name, error_data.loc[iterZ][header_name], error_msg)

    def check_if_string(self, sheet_name, data_table, header_name, depend_col, depend_val, na_allowed):
        self.Rule_Count = self.Rule_Count + 1
        data_table, error_str = self.check_for_dependancy(data_table, depend_col, depend_val, sheet_name, header_name)
        if len(data_table) > 0:
            if depend_col == "None":
                error_msg = "Value must be a string and NOT N/A "
            else:
                error_msg = error_str + ".  Value must be a string and NOT N/A"
            # value can be a string but can not be a string of spaces
            good_logic = data_table[header_name].apply(lambda x: (isinstance(x, (int, float, str)) or x in [''] or
                                                                  len(str(x).strip()) > 0) and (x not in ['N/A']))
            if na_allowed is True:
                error_msg.replace("and NOT N/A", "OR N/A")
                good_logic = data_table[header_name].apply(lambda x: (isinstance(x, (int, float, str)) or x in [''] or
                                                           len(str(x).strip()) > 0) or (x not in ['N/A']))
            error_data = data_table[[not x for x in good_logic]]
            if header_name in ["Comments"]:
                error_msg = "Value must be a non empty string and NOT N/A ('  ') not allowed"
                self.update_error_table("Warning", error_data, sheet_name, header_name, error_msg)
            else:
                self.update_error_table("Error", error_data, sheet_name, header_name, error_msg)

    def check_icd10(self, sheet_name, data_table, header_name):
        self.Rule_Count = self.Rule_Count + 1
        number_data = data_table[data_table[header_name].apply(lambda x: not isinstance(x, str))]
        data_table = data_table[data_table[header_name].apply(lambda x: isinstance(x, str))]
        error_data = data_table[data_table[header_name].apply(lambda x: not (icd10.exists(x) or x in ["N/A"]))]
        Error_Message = "Invalid or unknown ICD10 code, Value must be Valid ICD10 code or N/A"
        self.update_error_table("Error", error_data, sheet_name, header_name, Error_Message)
        self.update_error_table("Error", number_data, sheet_name, header_name, Error_Message)

    def add_warning_msg(self, neg_values, neg_msg, neg_error_msg, pos_values, pos_msg, pos_error_msg,
                        sheet_name, header_name):
        self.update_error_table(neg_msg, neg_values, sheet_name, header_name, neg_error_msg, True)
        self.update_error_table(pos_msg, pos_values, sheet_name, header_name, pos_error_msg, True)

    def get_missing_values(self, sheet_name, data_table, header_name, Required_column):
        if header_name in ["Comments"]:  # comments can be left blank, no need to warn
            return
        try:
            missing_data = data_table.query("{0} == '' ".format(header_name))
        except Exception:
            missing_data = data_table[data_table[header_name].apply(lambda x: x == '')]

        if len(missing_data) > 0:
            if Required_column == "Yes":
                error_msg = "Missing Values are not allowed for this column.  Please recheck data"
                self.update_error_table("Error", missing_data, sheet_name, header_name, error_msg, True)
            elif Required_column == "No":
                error_msg = "Missing Values where found, this is a warning.  Please recheck data"
                self.update_error_table("Warning", missing_data, sheet_name, header_name, error_msg, True)

            elif 'SARS_CoV_2_PCR_Test_Result' not in missing_data.columns.to_list():
                error_msg = "Patient SARS_CoV-2 is missing, unable to validate this column.  Please recheck data"
                self.update_error_table("Error", missing_data, sheet_name, header_name, error_msg, True)
            elif Required_column in ["Yes: SARS-Positive", "Yes: SARS-Negative"]:
                neg_values = missing_data.query("SARS_CoV_2_PCR_Test_Result == 'Negative'")
                pos_values = missing_data.query("SARS_CoV_2_PCR_Test_Result == 'Positive'")
                warn_msg = "Missing Values where found, this is a warning.  Please recheck data"
                if Required_column == "Yes: SARS-Positive":
                    error_msg = ("This column is requred for Sars Positive Patients, " +
                                 " missing values are not allowed.  Please recheck data")
                    self.add_warning_msg(neg_values, 'Warning', warn_msg, pos_values, 'Error',
                                         error_msg, sheet_name, header_name)
                else:
                    error_msg = ("This column is requred for Sars Negative Patients," +
                                 " missing values are not allowed.  Please recheck data")
                    self.add_warning_msg(neg_values, 'Error', error_msg, pos_values, 'Warning',
                                         warn_msg, sheet_name, header_name)

    def get_all_unique_ids(self, re):
        all_part_ids = []
        all_bio_ids = []
        for iterF in self.Data_Object_Table:
            if iterF not in ['submission.csv', 'shipping_manifest.csv']:
                header_list = self.Data_Object_Table[iterF]["Data_Table"].columns.tolist()
                if "Research_Participant_ID" in header_list:
                    curr_ids = self.Data_Object_Table[iterF]["Data_Table"]["Research_Participant_ID"].tolist()
                    all_part_ids = all_part_ids + curr_ids
                if "Biospecimen_ID" in header_list:
                    curr_ids = self.Data_Object_Table[iterF]["Data_Table"]["Biospecimen_ID"].tolist()
                    all_bio_ids = all_bio_ids + curr_ids

        self.All_part_ids = list(set(all_part_ids))
        self.All_bio_ids = list(set(all_bio_ids))

        self.All_part_ids = [i for i in self.All_part_ids if (re.compile('^' + str(self.CBC_ID) +
                                                                         '[_]{1}[0-9]{6}$').match(i) is not None)]
        self.All_bio_ids = [i for i in self.All_bio_ids if (re.compile('^' + str(self.CBC_ID)
                                                                       + '[_]{1}[0-9]{6}[_]{1}[0-9]{3}$').match(i)
                                                            is not None)]

        self.All_part_ids = pd.DataFrame(self.All_part_ids, columns=["Research_Participant_ID"])
        self.All_bio_ids = pd.DataFrame(self.All_bio_ids, columns=["Biospecimen_ID"])

    def get_passing_part_ids(self):
        self.Rule_Count = self.Rule_Count + 1
        if (int(self.Submit_Participant_IDs) != len(self.All_part_ids)):
            error_msg = "After validation only " + str(len(self.All_part_ids)) + " Participat IDS are valid"
            self.add_error_values("Error", "submission.csv", -5, "submit_Participant_IDs",
                                  self.Submit_Participant_IDs, error_msg)
        elif (int(self.Submit_Biospecimen_IDs) != len(self.All_bio_ids)):
            error_msg = "After validation only " + str(len(self.All_bio_ids)) + " Biospecimen IDS are valid"
            self.add_error_values("Error", "submission.csv", -5, "submit_Biospecimen_IDs",
                                  self.Submit_Biospecimen_IDs, error_msg)
        else:
            error_msg = "ID match, do not do anything"

    def make_error_queries(self, all_merge, field_name):
        self.Rule_Count = self.Rule_Count + 1
        if field_name == "Biospecimen":
            col_name = "Biospecimen_ID"
        if field_name == "Demographic":
            col_name = "Age"
        if field_name == "Confimatory_Clinical_Test":
            col_name = "Assay_ID"
        error_msg = "Participant is SARS_Cov2 Negative, however missing " + field_name + " Data"
        self.part_ids_errors(all_merge, error_msg, ("{0} != {0} and SARS_CoV_2_PCR_Test_Result == 'Negative'"),
                             col_name)
        error_msg = "Participant is SARS_Cov2 Positive, however missing " + field_name + " Data"
        self.part_ids_errors(all_merge, error_msg, ("{0} != {0} and SARS_CoV_2_PCR_Test_Result == 'Positive'"), col_name)
        error_msg = "Participant has " + field_name + " Data, however has missing/unknown SARS_Cov2 test"
        self.part_ids_errors(all_merge, error_msg, ("{0} == {0} and SARS_CoV_2_PCR_Test_Result not in " +
                                                    "['Positive','Negative']"), col_name)

    def part_ids_errors(self, all_merge, error_msg, querry_str, test_field):
        self.Rule_Count = self.Rule_Count + 1
        error_data = all_merge.query(querry_str.format(test_field))
        error_data.drop_duplicates("Research_Participant_ID", inplace=True)
        self.update_error_table("Error", error_data, "Cross_Participant_ID.csv", "Research_Participant_ID", error_msg)

    def get_cross_sheet_ID(self, re, field_name, pattern_str, sheet_name):
        self.Rule_Count = self.Rule_Count + 1
        if field_name == "Biospecimen_ID":
            file_list = self.Bio_List
        elif field_name == "Research_Participant_ID":
            file_list = self.Part_List
        if len(file_list) == 0:
            return

        all_merge = []
        for iterF in file_list:
            curr_col = self.Data_Object_Table[iterF]["Key_Cols"]
            if len(all_merge) == 0:
                all_merge = self.Data_Object_Table[iterF]["Data_Table"][curr_col]
            else:
                all_merge = all_merge.merge(self.Data_Object_Table[iterF]["Data_Table"][curr_col],
                                            on=field_name, how="outer")
        pattern_str = '^' + str(self.CBC_ID) + pattern_str
        all_merge = all_merge[all_merge[field_name].apply(lambda x: re.compile(pattern_str).match(x) is not None)]

        if field_name == "Research_Participant_ID":
            if ("Age" in all_merge.columns) and ("SARS_CoV_2_PCR_Test_Result" in all_merge.columns):
                self.make_error_queries(all_merge, "Demographic")
            if ("Biospecimen_Type" in all_merge.columns) and ("SARS_CoV_2_PCR_Test_Result" in all_merge.columns):
                self.make_error_queries(all_merge, "Biospecimen")
            if ("Assay_ID" in all_merge.columns) and ("SARS_CoV_2_PCR_Test_Result" in all_merge.columns):
                self.make_error_queries(all_merge, "Confimatory_Clinical_Test")
        if field_name == "Biospecimen_ID":
            if "Biospecimen_Type" not in all_merge.columns:
                print("Biospecimen.csv was not provided, not able to validte Biospecimen_ID for cross sheet rules\n")
            elif "aliquot.csv" in file_list:
                error_data = all_merge.query("{0} != {0} and {1} == {1}".format("Biospecimen_Type", "Aliquot_ID"))
                error_msg = "ID is found in Aliquot.csv, however no coresponding ID found in biospecimen.csv"
                self.update_error_table("Error", error_data, "Cross_Biospecimen_ID.csv", "Biospecimen_ID", error_msg)

                error_data = all_merge.query("{0} == {0} and {1} != {1}".format("Biospecimen_Type", "Aliquot_ID"))
                error_msg = "ID is found in biospecimen.csv, however no coresponding ID found in aliquot.csv"
                self.update_error_table("Error", error_data, "Cross_Biospecimen_ID.csv", "Biospecimen_ID", error_msg)
            else:
                for iterF in file_list:
                    if iterF in ["equipment.csv"]:
                        col_name = "Equipment_ID"
                    if iterF in ["reagent.csv"]:
                        col_name = "Reagent_Name"
                    if iterF in ["consumable.csv"]:
                        col_name = "Consumable_Name"
                    if iterF in ["aliquot.csv", "biospecimen.csv"]:
                        continue
                    try:
                        error_data = all_merge.query("{0} != {0} and {1} == {1}".format("Biospecimen_Type", col_name))
                        error_msg = "ID is found in " + iterF + ", however no coresponding ID found in biospecimen.csv"
                        self.update_error_table("Error", error_data, "Cross_Biospecimen_ID.csv", "Biospecimen_ID", error_msg)
                        error_data = all_merge.query(("{0} == {0} and Biospecimen_Type == 'PBMC'" +
                                                      "and {1} != {1}").format("Biospecimen_Type", col_name))
                        error_msg = ("ID is found in biospecimen.csv and has a Biospecimen type of PBMC," +
                                     " however no coresponding ID found in " + iterF)
                        self.update_error_table("Error", error_data, "Cross_Biospecimen_ID.csv",
                                                "Biospecimen_ID", error_msg)
                        error_data = all_merge.query(("{0} == {0} and Biospecimen_Type != 'PBMC'" +
                                                     " and {1} == {1}").format("Biospecimen_Type", col_name))
                        error_msg = ("ID is found in both biospecimen.csv and "+iterF +
                                     ", however has a Biospecimen type that is not PBMC")
                        self.update_error_table("Warning", error_data, "Cross_Biospecimen_ID.csv",
                                                "Biospecimen_ID", error_msg)
                    except Exception as e:
                        print(e)

    def check_map_ids(self, column_test):
        ref_id_data = self.Data_Object_Table["reference_panel.csv"]["Data_Table"]
        bio_id_data = self.Data_Object_Table["biorepository_id_map.csv"]["Data_Table"]
        if column_test == 'Parent_Biorepository__ID':
            merge_data = ref_id_data.merge(bio_id_data, left_on=['Parent_Biorepository__ID'],
                                           right_on=['Biorepository_ID'], how='outer', indicator=True)
        elif column_test == 'Subaliquot_ID':
            merge_data = ref_id_data.merge(bio_id_data, on='Subaliquot_ID', how='outer', indicator=True)
        else:
            return

        error_msg = "Id is found in the refrence panel, however ID is missing from the biorepository_id_map"
        col_names = merge_data.columns
        merge_data = pd.DataFrame([convert_data_type(c) for c in l] for l in merge_data.values)
        merge_data.columns = col_names

        for curr_col in merge_data.columns:
            if "_x" in curr_col or "_y" in curr_col:
                merge_data.drop(curr_col, inplace=True, axis=1)

        for field in ['Biorepository_ID', 'Parent_Biorepository__ID', 'Subaliquot_ID']:
            if field in merge_data:
                error_id_list = self.Error_list.query("Column_Name == @field")["Column_Value"].tolist()
                merge_data = merge_data.query("{} not in @error_id_list".format(field))
        merge_data = merge_data.query("_merge == 'left_only'")
        self.update_error_table("Error", merge_data, "Cross_Sheet_ID.csv", column_test, error_msg)
        pass

    def write_col_errors(self, Error_Path):
        self.Column_error_count.to_csv(Error_Path + "All_Column_Errors_Found.csv", index=False)

    def write_error_file(self, Error_path):
        uni_name = list(set(self.Error_list["CSV_Sheet_Name"]))
        if len(uni_name) == 0:
            print(colored("No Errors were found in this submission", 'green'))
        for iterU in uni_name:
            curr_table = self.Error_list.query("CSV_Sheet_Name == @iterU")
            curr_name = iterU.replace('.csv', '_Errors.csv')
            if uni_name in ["Cross_Participant_ID.csv", "Cross_Biospecimen_ID.csv", "submission.csv"]:
                curr_table = curr_table.sort_index()
            else:
                curr_table = curr_table.sort_values('Row_Index')
            curr_name = curr_name.replace(".xlsx", ".csv")
            curr_table.to_csv(Error_path + curr_name, index=False)
            print(colored(iterU + " has " + str(len(curr_table)) + " Errors", 'red'))

    def validate_serology(self, file_name, serology_data, assay_data, assay_target, serology_id):
        self.update_object(serology_data, file_name)
        self.update_object(assay_data, "assay.csv")
        self.update_object(assay_target, "assay_target.csv")
        data_table, drop_list = self.correct_var_types(file_name)
        self.CBC_ID = serology_id
        return data_table, drop_list
