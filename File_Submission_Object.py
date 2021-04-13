import icd10
import pandas as pd
from dateutil.parser import parse
from termcolor import colored
######################################################################################################################
def clean_up_column_names(header_name):
    header_name = header_name.replace(" (cells/mL)","")
    header_name = header_name.replace(" (mL)","")
    header_name = header_name.replace(" (Years)","")
    header_name = header_name.replace(" (Days)","")
    header_name = header_name.replace(" (min)","")
    header_name = header_name.replace(" (hrs)","")
    header_name = header_name.replace("°C","")
    header_name = header_name.replace("-80","80")
    header_name = header_name.replace("-","_")
    return header_name
def convert_data_type(v):
    if str(v).find('_') > 0:
        return v
    else:
        try:
            return float(v)
        except ValueError:
            try:
                return parse(v)
            except ValueError:
                return v
def check_multi_rule(data_table,depend_col,depend_val):
    if depend_col not in data_table.columns.to_list():
        error_str = depend_col + " is not found, unable to validate Data "
        data_table = -1
        return data_table,error_str
    if depend_val == "Is A Number":             #dependant column must be a number
        data_table = data_table[data_table[depend_col].apply(lambda x: isinstance(x,(float,int)))]
        error_str = depend_col + " is a Number "
    elif depend_val == "Is A Date":             #dependant column must be a Date
        data_table = data_table[data_table[depend_col].apply(lambda x: isinstance(x,pd.Timestamp))]
        error_str = depend_col + " is a Date "
    else:                                       #dependant column must be a list or fixed value
        data_table = data_table[data_table[depend_col].apply(lambda x: x in depend_val)]
        error_str = depend_col + " is in " +  str(depend_val) + " "
    return data_table,error_str
######################################################################################################################
class Submission_Object:
    def __init__(self,file_name):                  #initalizes the Object
        """An Object that contains information for each Submitted File that Passed File_Validation."""   
        self.File_Name = file_name
        self.Data_Object_Table = {}
        self.Part_List = []
        self.Bio_List = []
        self.Column_error_count = pd.DataFrame(columns = ["Message_Type","CSV_Sheet_Name","Column_Name","Error_Message"])
        self.Curr_col_errors = pd.DataFrame(columns = ["Message_Type","CSV_Sheet_Name","Column_Name","Error_Message"])
        self.Error_list = pd.DataFrame(columns = ["Message_Type","CSV_Sheet_Name","Row_Index","Column_Name","Column_Value","Error_Message"])
######################################################################################################################
    def get_data_tables(self,file_name,file_path):
        self.Data_Object_Table[file_name] = {"Data_Table":[],"Column_List":[]}
        Data_Table = pd.read_csv(file_path,na_filter=False)
        self.Data_Object_Table[file_name]["Data_Table"].append(Data_Table)
        if file_name not in ["submission.csv","shipping_manifest.csv"]:
            self.cleanup_table(file_name)
######################################################################################################################
    def cleanup_table(self,file_name):          
        curr_table = self.Data_Object_Table[file_name]["Data_Table"][0]
        curr_table.dropna(axis=0, how="all", thresh=None, subset=None, inplace=True)        #if a row is completely blank remove it
        missing_logic = curr_table.eq(curr_table.iloc[:, 0], axis=0).all(axis=1)
        curr_table = curr_table[[i is not True for i in missing_logic]]
        
        curr_table = curr_table .loc[:,~ curr_table .columns.str.startswith('Unnamed')]     #if a column is completely blank, remove it
        self.Data_Object_Table[file_name]["File_Size"] = len(curr_table)                            #number of rows in the file
        self.Data_Object_Table[file_name]["Data_Table"] = curr_table
##########################################################################################################################
    def column_validation(self,file_name,Support_Files):
        if file_name in ["submission.csv","shipping_manifest.csv"]:
            return
        header_list = self.Data_Object_Table[file_name]['Data_Table'].columns.tolist()
        check_file = [i for i in Support_Files if file_name.replace('csv','xlsx') in i]
        header_list = [clean_up_column_names(i) for i in header_list]
        self.Data_Object_Table[file_name]['Data_Table'].columns = header_list
        
        if len(check_file) == 0:            #file was not included, nothing to check for
            return
        check_file = pd.read_excel(check_file[0])
        col_list = check_file.columns.tolist()
        col_list = [clean_up_column_names(i) for i in col_list]
        
        in_csv_not_excel = [i for i in header_list if i not in col_list]
        in_excel_not_csv = [i for i in col_list if i not in header_list]

        csv_errors = ["Column Found in CSV is not Expected"] * len(in_csv_not_excel)
        excel_errors = ["This Column is Expected and is missing from CSV File"] * len(in_excel_not_csv)
        name_list  = [file_name] * (len(in_csv_not_excel) + len(in_excel_not_csv))
        
        if len(name_list) > 0:
            self.Curr_col_errors["Message_Type"] = ["Error"]*len(name_list)
            self.Curr_col_errors["CSV_Sheet_Name"] = name_list
            self.Curr_col_errors["Column_Name"] = (in_csv_not_excel + in_excel_not_csv)
            self.Curr_col_errors["Error_Message"] = (csv_errors+excel_errors)
            self.Column_error_count = self.Column_error_count.append(self.Curr_col_errors)
            self.Curr_col_errors.drop(labels = range(0,len(name_list)),axis = 0, inplace = True) 
    def get_submission_metadata(self,Support_Files):
        if "submission.csv" not in self.Data_Object_Table:
            print("Submission File was not included in the list of files to validate")
        else:
            try:
                submit_table = self.Data_Object_Table['submission.csv']['Data_Table'][0]
                id_list =  [i for i in Support_Files if "SeroNet_Org_IDs.xlsx" in i]
                id_conv = pd.read_excel(id_list[0])
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
        if self.CBC_ID > 0:
            print("The CBC Code for " + self.Submitted_Name + " Is: " + str(self.CBC_ID) + "\n")
        else:
            print("The Submitted CBC name: " + self.Submitted_Name + " does NOT exist in the Database")
##########################################################################################################################
    def correct_var_types(self,file_name):    
        data_table = self.Data_Object_Table[file_name]['Data_Table']
        data_table,drop_list = self.merge_tables(file_name,data_table)
        col_names = data_table.columns
        data_table = pd.DataFrame([convert_data_type(c) for c in l] for l in data_table.values)
        data_table.columns = col_names
        return data_table,drop_list
    def merge_tables(self,file_name,data_table):
        self.Data_Object_Table[file_name]["Column_List"] = data_table.columns
        if file_name == "prior_clinical_test.csv":
            data_table = self.check_merge(data_table,"demographic.csv","Research_Participant_ID")
        elif file_name == "demographic.csv":
            data_table = self.check_merge(data_table,"prior_clinical_test.csv","Research_Participant_ID")
        elif file_name == "biospecimen.csv":
            data_table = self.check_merge(data_table,"prior_clinical_test.csv","Research_Participant_ID")
            data_table = self.check_merge(data_table,"demographic.csv","Research_Participant_ID")
        elif file_name in ["aliquot.csv","equipment.csv","reagent.csv","consumable.csv"]:
            data_table = self.check_merge(data_table,"biospecimen.csv","Biospecimen_ID")
        elif file_name in ["assay_target.csv"]:
            data_table = self.check_merge(data_table,"assay.csv","Assay_ID")
        elif file_name in ["confirmatory_clinical_test.csv"]:
             data_table = self.check_merge(data_table,"assay.csv","Assay_ID")
             data_table = self.check_merge(data_table,"assay_target.csv",["Assay_ID","Assay_Target","Assay_Target_Sub_Region"])            
        drop_list = [i for i in data_table.columns if i not in  self.Data_Object_Table[file_name]["Column_List"]]
        return data_table,drop_list
    def check_merge(self,data_table,table_name,merge_field):
        if table_name in self.Data_Object_Table:
            data_table = data_table.merge(self.Data_Object_Table[table_name]["Data_Table"],how='left',on=merge_field)
        return data_table
##########################################################################################################################
    def add_error_values(self,msg_type,sheet_name,row_index,col_name,col_value,error_msg):
        new_row = {"Message_Type":msg_type,"CSV_Sheet_Name":sheet_name,"Row_Index":row_index,"Column_Name":col_name,"Column_Value":col_value,"Error_Message":error_msg}
        self.Error_list = self.Error_list.append(new_row, ignore_index=True)
    def sort_and_drop(self,header_name,keep_blank = False):
        self.Error_list.drop_duplicates(["Row_Index","Column_Name","Column_Value"],inplace = True)
        if keep_blank == False:
            drop_idx = self.Error_list.query("Column_Name == @header_name and Column_Value == ''").index
            self.Error_list.drop(drop_idx , inplace=True)
    def update_error_table(self,msg_type,error_data,sheet_name,header_name,error_msg,keep_blank = False):
        for i in error_data.index:
            self.add_error_values(msg_type,sheet_name,i+2,header_name,error_data.loc[i][header_name],error_msg)
        self.sort_and_drop(header_name,keep_blank)
###########################################################################################################################
    def check_for_dependancy(self,data_table,depend_col,depend_val,sheet_name,header_name):
        error_str = "Unexpected Value. "
        if depend_col != "None":          #rule has a dependancy on another column
            data_table,error_str = check_multi_rule(data_table,depend_col,depend_val)
        if isinstance(data_table,(int,float)):
            self.add_error_values("Error",sheet_name,0,header_name,"Entire Column",error_str)
            data_table = []
        return data_table,error_str    

    def check_assay_special(self,data_table,header_name,file_name,field_name):
        error_data = data_table.query("{0} != {0}".format(field_name))
        error_msg = header_name + " is not found in the table of valid " + header_name +"s in databse or submitted file"
        self.update_error_table("Error",error_data,file_name,header_name,error_msg,keep_blank = False)
    def check_id_field(self,sheet_name,data_table,re,field_name,pattern_str,cbc_id,pattern_error):        
        single_invalid = data_table[data_table[field_name].apply(lambda x : re.compile('^[0-9]{2}' + pattern_str).match(str(x)) is None)]
        wrong_cbc_id   = data_table[data_table[field_name].apply(lambda x : (re.compile('^' + cbc_id + pattern_str).match(str(x)) is None))]
        
        for i in single_invalid.index:
            if single_invalid[field_name][i] != '':
                error_msg = "ID is Not Valid Format, Expecting " + pattern_error
                self.add_error_values("Error",sheet_name,i+2,field_name,single_invalid[field_name][i],error_msg)
        for i in wrong_cbc_id.index:
            if int(cbc_id) == 0:
                error_msg = "ID is Valid however submission file is missing, unable to validate CBC code"
            else:
                error_msg = "ID is Valid however has wrong CBC code. Expecting CBC Code (" + str(cbc_id) + ")"
            self.add_error_values("Error",sheet_name,i+2,field_name,wrong_cbc_id[field_name][i],error_msg)
        self.sort_and_drop(field_name)
    def check_for_dup_ids(self,sheet_name,field_name):
        if sheet_name in self.Data_Object_Table:
            data_table = self.Data_Object_Table[sheet_name]['Data_Table']
            table_counts = data_table[field_name].value_counts(dropna=False).to_frame()
            dup_id_count = table_counts[table_counts[field_name] >  1]
            for i in dup_id_count.index:
                error_msg = "Id is repeated " + str(dup_id_count[field_name][i]) + " times, Multiple repeats are not allowed"
                self.add_error_values("Error",sheet_name,-3,field_name,i,error_msg)
    def check_if_substr(self,data_table,id_1,id_2,file_name,header_name):
        id_compare = data_table[data_table.apply(lambda x: x[id_1] not in x[id_2],axis = 1)]
        Error_Message = id_1 + " is not a substring of " + id_2 +".  Data is not Valid, please check data"
        self.update_error_table("Error",id_compare,file_name,header_name,Error_Message)
##########################################################################################################################
    def check_in_list(self,sheet_name,data_table,header_name,depend_col,depend_val,list_values):
        data_table,error_str = self.check_for_dependancy(data_table,depend_col,depend_val,sheet_name,header_name)
        if len(data_table) == 0:
            return{}
        error_msg = error_str +  "Value must be one of the following: " + str(list_values)
        
        if list_values == ["N/A"]:
            passing_values = data_table[data_table[header_name].apply(lambda x: x == "N/A")]
        else:
            query_str = "{0} in @list_values or {0} in ['']".format(header_name)
            passing_values = data_table.query(query_str)
            
        row_index = [iterI for iterI in data_table.index if (iterI not in passing_values.index)]
        error_data = data_table.loc[row_index]
        
        self.update_error_table("Error",error_data,sheet_name,header_name,error_msg)
##########################################################################################################################
    def check_date(self,datetime,sheet_name,data_table,header_name,depend_col,depend_val,na_allowed,time_check,lower_lim = 0,upper_lim = 24):
        data_table,error_str = self.check_for_dependancy(data_table,depend_col,depend_val,sheet_name,header_name)
        if len(data_table) == 0:
            return{}
        
        date_only = data_table[header_name].apply(lambda x: isinstance(x,datetime.datetime))
        good_date = data_table[date_only]
          
        if time_check == "Date":
            error_msg = error_str + "Value must be a Valid Date MM/DD/YYYY"
        else:
            error_msg = error_str + "Value must be a Valid Time HH:MM:SS"
        if na_allowed == False:
            date_logic = data_table[header_name].apply(lambda x: isinstance(x,datetime.datetime) or x in [''])
        else:
            date_logic = data_table[header_name].apply(lambda x: isinstance(x,datetime.datetime) or x in ['N/A',''])
            error_msg =  error_msg + " Or N/A"
        error_data = data_table[[not x for x in date_logic]]
        self.update_error_table("Error",error_data,sheet_name,header_name,error_msg)
      
        if time_check == "Date":
            to_early = good_date[header_name].apply(lambda x: x.date() < lower_lim)
            to_late  = good_date[header_name].apply(lambda x: x.date() > upper_lim)
            if "Expiration_Date" in header_name:
                error_msg = "Expiration Date has already passed, check to make sure date is correct"
                self.update_error_table("Warning",good_date[to_early],sheet_name,header_name,error_msg)
            elif "Calibration_Due_Date" in header_name:
                error_msg = "Calibration Date has already passed, check to make sure date is correct"
                self.update_error_table("Warning",good_date[to_early],sheet_name,header_name,error_msg)
            else:
                error_msg = "Date is valid however must be between " + str(lower_lim) + " and " + str(upper_lim)
                self.update_error_table("Error",good_date[to_early],sheet_name,header_name,error_msg)
            error_msg = "Date is valid however must be between " + str(lower_lim) + " and " + str(upper_lim)
            self.update_error_table("Error",good_date[to_late],sheet_name,header_name,error_msg)
##########################################################################################################################
    def check_if_number(self,sheet_name,data_table,header_name,depend_col,depend_val,na_allowed,lower_lim,upper_lim,num_type):
        data_table,error_str = self.check_for_dependancy(data_table,depend_col,depend_val,sheet_name,header_name)

        if len(data_table) == 0:
            return{}
        error_msg = error_str + "Value must be a number between " + str(lower_lim) + " and " + str(upper_lim)
        
        data_list = data_table[header_name].tolist()
        for iterD in enumerate(data_list):
            if isinstance(iterD[1],pd.Timestamp):                       #if storage_time comes in as HH:MM convert
                time_conv = iterD[1].hour + (iterD[1].minute)/60        #into a hour + min/60 (into a float value)
                data_table.at[iterD[0],header_name] = time_conv
        
        number_only = data_table[header_name].apply(lambda x: isinstance(x,(int,float)))        #if float allowed then so are intigers
        good_data = data_table[number_only]
        
        good_logic = data_table[header_name].apply(lambda x: isinstance(x,(int,float)) or x in [''])
        to_low  = good_data[header_name].apply(lambda x: x < lower_lim)
        to_high = good_data[header_name].apply(lambda x: x > upper_lim)
        if num_type == "int":
             is_float = good_data[header_name].apply(lambda x: x.is_integer() == False)
             error_msg = "Value must be an interger between " + str(lower_lim) + " and " + str(upper_lim) + ", decimal values are not allowed"
             self.update_error_table("Error",good_data[is_float],sheet_name,header_name,error_msg)
        if na_allowed == True:
             good_logic = data_table[header_name].apply(lambda x: isinstance(x,(int,float)) or x in ['N/A',''])
             
        error_data = data_table[[not x for x in good_logic]]
        self.update_error_table("Error",error_data,sheet_name,header_name,error_msg)
        self.update_error_table("Error",good_data[to_low],sheet_name,header_name,error_msg)
        self.update_error_table("Error",good_data[to_high],sheet_name,header_name,error_msg)
##########################################################################################################################
    def compare_total_to_live(self,sheet_name,data_table,header_name):
        second_col = header_name.replace('Total_Cells','Live_Cells')
        data_table,error_str = self.check_for_dependancy(data_table,header_name,"Is A Number",sheet_name,header_name)
        data_table,error_str = self.check_for_dependancy(data_table,second_col,"Is A Number",sheet_name,header_name)
        error_data = data_table.query("{0} > {1}".format(second_col,header_name))
        for iterZ in error_data.index:
            error_msg = "Total Cell Count must be greater then Live Cell Count (" + str(error_data[second_col][iterZ]) + ")" 
            self.add_error_values("Error",sheet_name,iterZ+2,header_name,error_data.loc[iterZ][header_name],error_msg)
    def compare_viability(self,sheet_name,data_table,header_name):
        live_col = header_name.replace('Viability','Live_Cells')
        total_col = header_name.replace('Viability','Total_Cells')
        data_table,error_str = self.check_for_dependancy(data_table,header_name,"Is A Number",sheet_name,header_name)
        data_table,error_str = self.check_for_dependancy(data_table,live_col,"Is A Number",sheet_name,header_name)
        data_table,error_str = self.check_for_dependancy(data_table,total_col,"Is A Number",sheet_name,header_name)
    
        error_data = data_table[data_table.apply(lambda x: x[total_col] == 0 and x[header_name] not in ['N/A'],axis = 1)]
        error_msg = "Total Count is 0, Viability_Count should be N/A"
        self.update_error_table("Warning",error_data,sheet_name,header_name,error_msg)
        
        data_table = data_table[data_table.apply(lambda x: x[total_col] > 0,axis = 1)]        
        error_data = data_table[data_table.apply(lambda x: round((x[live_col]/x[total_col])*100,1) != x[header_name],axis = 1)]
        
        for iterZ in error_data.index:
            via_pct = round((error_data[live_col][iterZ] / error_data[total_col][iterZ])*100,1)
            error_msg = "Viability Count must be ("+ str(via_pct) +") which is (Live_Count / Total_Count) * 100"
            self.add_error_values("Error",sheet_name,iterZ+2,header_name,error_data.loc[iterZ][header_name],error_msg)
##########################################################################################################################
    def check_if_string(self,sheet_name,data_table,header_name,depend_col,depend_val,na_allowed):
        data_table,error_str = self.check_for_dependancy(data_table,depend_col,depend_val,sheet_name,header_name)
        if len(data_table) == 0:
            return{}
        if depend_col == "None":
            error_msg = "Value must be a string and NOT N/A"  
        else:
            error_msg = error_str + ".  Value must be a string and NOT N/A"        
        good_logic = data_table[header_name].apply(lambda x: (isinstance(x,(int,float,str)) or x in ['']) and (x not in ['N/A']))      
        if na_allowed == True:
            error_msg.replace("and NOT N/A","OR N/A")
            good_logic = data_table[header_name].apply(lambda x: isinstance(x,(int,float,str)) or x in ['N/A',''])
             
        error_data = data_table[[not x for x in good_logic]]
        self.update_error_table("Error",error_data,sheet_name,header_name,error_msg)
##########################################################################################################################
    def check_icd10(self,sheet_name,data_table,header_name):
        number_data = data_table[data_table[header_name].apply(lambda x: not isinstance(x,str))]
        data_table = data_table[data_table[header_name].apply(lambda x: isinstance(x,str))]
        error_data = data_table[data_table[header_name].apply(lambda x: not (icd10.exists(x) or x in ["N/A"]))]
        Error_Message = "Invalid or unknown ICD10 code, Value must be Valid ICD10 code or N/A"
        self.update_error_table("Error",error_data,sheet_name,header_name,Error_Message)
        self.update_error_table("Error",number_data,sheet_name,header_name,Error_Message)
##########################################################################################################################
    def add_warning_msg(self,neg_values,neg_msg,neg_error_msg,pos_values,pos_msg,pos_error_msg,sheet_name,header_name):
         self.update_error_table(neg_msg,neg_values,sheet_name,header_name,neg_error_msg,True)
         self.update_error_table(pos_msg,pos_values,sheet_name,header_name,pos_error_msg,True)
    def get_missing_values(self,sheet_name,data_table,header_name,Required_column):
        try:
             missing_data = data_table.query("{0} == '' ".format(header_name))
        except Exception:
            missing_data = data_table[data_table[header_name].apply(lambda x: x == '')]
           
        if len(missing_data) > 0:
            
            if Required_column == "Yes":
                error_msg = "Missing Values are not allowed for this column.  Please recheck data"
                self.update_error_table("Error",missing_data,sheet_name,header_name,error_msg,True)
            elif Required_column == "No":
                error_msg = "Missing Values where found, this is a warning.  Please recheck data"
                self.update_error_table("Warning",missing_data,sheet_name,header_name,error_msg,True)
                
            elif 'SARS_CoV_2_PCR_Test_Result' not in missing_data.columns.to_list():
                error_msg = "Patient SARS_CoV-2 is missing, unable to validate this column.  Please recheck data"
                self.update_error_table("Error",missing_data,sheet_name,header_name,error_msg,True)
            elif Required_column in[ "Yes: SARS-Positive","Yes: SARS-Negative"]:
                neg_values = missing_data.query("SARS_CoV_2_PCR_Test_Result == 'Negative'")
                pos_values = missing_data.query("SARS_CoV_2_PCR_Test_Result == 'Positive'")
                warn_msg = "Missing Values where found, this is a warning.  Please recheck data"
                if Required_column ==  "Yes: SARS-Positive":
                    error_msg = "This column is requred for Sars Positive Patients, missing values are not allowed.  Please recheck data"
                    self.add_warning_msg(neg_values,'Warning',warn_msg,pos_values,'Error',error_msg,sheet_name,header_name)
                else:
                    error_msg = "This column is requred for Sars Negative Patients, missing values are not allowed.  Please recheck data"
                    self.add_warning_msg(neg_values,'Error',error_msg,pos_values,'Warning',warn_msg,sheet_name,header_name)
##########################################################################################################################
    def get_all_unique_ids(self,re):
        all_part_ids = []
        all_bio_ids = []
        for iterF in self.Data_Object_Table:
            if iterF not in ['submission.csv','shipping_manifest.csv']:
                header_list = self.Data_Object_Table[iterF]["Data_Table"].columns.tolist()
                if "Research_Participant_ID" in header_list:
                    all_part_ids = all_part_ids + self.Data_Object_Table[iterF]["Data_Table"]["Research_Participant_ID"].tolist()
                if "Biospecimen_ID" in header_list:
                    all_bio_ids = all_bio_ids + self.Data_Object_Table[iterF]["Data_Table"]["Biospecimen_ID"].tolist()
            
        self.All_part_ids = list(set(all_part_ids))
        self.All_bio_ids  = list(set(all_bio_ids))

        self.All_part_ids  = [i for i in self.All_part_ids if (re.compile('^' + str(self.CBC_ID) + '[_]{1}[0-9]{6}$').match(i) is not None)]
        self.All_bio_ids  = [i for i in self.All_bio_ids if (re.compile('^' + str(self.CBC_ID) + '[_]{1}[0-9]{6}[_]{1}[0-9]{3}$').match(i) is not None)]
    def get_passing_part_ids(self):
        if (int(self.Submit_Participant_IDs) != len(self.All_part_ids)):
            error_msg = "After validation only " + str(len(self.All_part_ids)) + " Participat IDS are valid"
            self.add_error_values("Error","submission.csv",-5,"submit_Participant_IDs",self.Submit_Participant_IDs,error_msg)
        elif (int(self.Submit_Biospecimen_IDs) != len(self.All_bio_ids)):
            error_msg = "After validation only " + str(len(self.All_bio_ids)) + " Biospecimen IDS are valid"
            self.add_error_values("Error","submission.csv",-5,"submit_Biospecimen_IDs",self.Submit_Biospecimen_IDs,error_msg)     
##########################################################################################################################



    def write_cross_sheet_id_error(self,merged_data,query_str,error_msg,field_name):
        check_id_only = merged_data.query(query_str.format("SARS_CoV_2_PCR_Test_Result","Age","Biospecimen_ID")) 
        for iterZ in range(len(check_id_only)):
            self.add_error_values("Error","Cross_Participant_ID.csv",-10,field_name,check_id_only.iloc[iterZ][field_name],error_msg)
        self.sort_and_drop(field_name,True)
##########################################################################################################################
    def write_cross_bio_errors(self,merged_data,table_name,sheet_name):
        error_data = merged_data.query("Biospecimen_Type != Biospecimen_Type and {0} == {0}".format(table_name))
        error_msg = "ID is found in " + sheet_name + ", however ID is missing from Biospecimen.csv"
        self.update_error_table("Error",error_data,"Cross_Biospecimen_ID.csv","Biospecimen_ID",error_msg)          
        if table_name in ["Aliquot_ID"]:
            error_data = merged_data.query("Biospecimen_Type == Biospecimen_Type and {0} != {0}".format(table_name))
            error_msg = "ID is found in Biospecimen.csv, however is missing from " + sheet_name
            self.update_error_table("Error",error_data,"Cross_Biospecimen_ID.csv","Biospecimen_ID",error_msg) 
        else:   
            error_data = merged_data.query("Biospecimen_Type != 'PBMC' and Biospecimen_Type == Biospecimen_Type and {0} == {0}".format(table_name))
            error_msg = "ID is found in " + sheet_name + ", and ID is found in Biospecimen.csv however has Biospecimen_Type NOT PBMC"
            self.update_error_table("Error",error_data,"Cross_Biospecimen_ID.csv","Biospecimen_ID",error_msg)
            error_data = merged_data.query("Biospecimen_Type == 'PBMC' and Biospecimen_Type == Biospecimen_Type and {0} != {0}".format(table_name))
            error_msg = "ID is found in Biospecimen.csv and has Biospecimen_Type of PBMC, however ID is missing from " + sheet_name
            self.update_error_table("Error",error_data,"Cross_Biospecimen_ID.csv","Biospecimen_ID",error_msg)
    def get_submitted_ids(self,file_list,col_name,merged_data):
        all_pass= []
        for iterF in self.Data_Object_Table:
            if iterF in file_list:
                all_pass = all_pass + self.Data_Object_Table[iterF]['Data_Table'][col_name].tolist()
        
        if len(all_pass) == 0:
            return all_pass
        else:
            all_pass = pd.Series(all_pass, name = col_name)
            merged_data.merge(all_pass,on = col_name, how = "right")
            return merged_data
    def get_cross_sheet_Biospecimen_ID(self,re,field_name):
        merged_data = self.all_bio_ids[self.all_bio_ids.isna().any(axis=1)]
        file_list = ['biospecimen.csv','aliquot.csv','equipment.csv','reagent.csv','consumable.csv']
        merged_data = merged_data[merged_data[field_name].apply(lambda x : (re.compile('^' + self.CBC_ID + '[_]{1}[0-9]{6}[_]{1}[0-9]{3}$').match(x) is not None))]
        merged_data  = self.get_submitted_ids(file_list,'Biospecimen_ID',merged_data)
        if len(merged_data) > 0:
            self.write_cross_bio_errors(merged_data,"Aliquot_ID","Aliquot.csv")
            self.write_cross_bio_errors(merged_data,"Equipment_ID","Equipment.csv")
            self.write_cross_bio_errors(merged_data,"Reagent_Name","Reagent.csv")
            self.write_cross_bio_errors(merged_data,"Consumable_Name","Consumable.csv")
    def get_cross_sheet_Participant_ID(self,re,field_name):
        merged_data = self.all_part_ids[self.all_part_ids.isna().any(axis=1)]
        if len(merged_data) > 0:                #if there are unmatcehd IDS then remove bad IDS and filter to submitted list
            file_list = ['prior_clinical_test.csv','demographic.csv','biospecimen.csv','confirmatory_clinical_test.csv']
            merged_data = merged_data[merged_data[field_name].apply(lambda x : (re.compile('^' + self.CBC_ID + '[_]{1}[0-9]{6}$').match(x) is not None))]
            merged_data  = self.get_submitted_ids(file_list,'Research_Participant_ID',merged_data)
        if len(merged_data) > 0:                #only checks for errors if there are IDs left after the filtering
            error_msg = "ID is found in Prior_Clinical_Test, but is missing from Demographic and Biospecimen"
            self.write_cross_sheet_id_error(merged_data,"{0} == {0} and {1} != {1} and {2} != {2}",error_msg,field_name)
            error_msg = "ID is found in Demographic, but is missing from Prior_Clinical_Test and Biospecimen"
            self.write_cross_sheet_id_error(merged_data,"{0} != {0} and {1} == {1} and {2} != {2}",error_msg,field_name)
            error_msg = "ID is found in Biospecimen, but is missing from Prior_Clinical_Test and Demographic"
            self.write_cross_sheet_id_error(merged_data,"{0} != {0} and {1} != {1} and {2} == {2}",error_msg,field_name)
            error_msg = "ID is found in Prior_Clinical_Test and Demographic but is missing from Biospecimen"
            self.write_cross_sheet_id_error(merged_data,"{0} == {0} and {1} == {1} and {2} != {2}",error_msg,field_name)
            error_msg = "ID is found in Prior_Clinical_Test and Biospecimen but is missing from Demographic"
            self.write_cross_sheet_id_error(merged_data,"{0} == {0} and {1} != {1} and {2} == {2}",error_msg,field_name)
            error_msg = "ID is found in Demographic and Biospecimen but is missing from Prior_Clinical_Test"
            self.write_cross_sheet_id_error(merged_data,"{0} != {0} and {1} == {1} and {2} == {2}",error_msg,field_name)
##########################################################################################################################
    def write_col_errors(self,Error_Path):
        self.Column_error_count.to_csv(Error_Path + "All_Column_Errors_Found.csv",index=False)
    def write_error_file(self,Error_path):
        uni_name = list(set(self.Error_list["CSV_Sheet_Name"]))
        if len(uni_name) == 0:
            print(colored("No Errors were found in this submission",'green'))
        for iterU in uni_name:
            curr_table = self.Error_list.query("CSV_Sheet_Name == @iterU")
            curr_name = iterU.replace('.csv','_Errors.csv')
            if uni_name in ["Cross_Participant_ID.csv","Cross_Biospecimen_ID.csv","submission.csv"]:
                curr_table = curr_table.sort_index()
            else:
                curr_table = curr_table.sort_values('Row_Index')
                
            curr_table.to_csv(Error_path + curr_name,index=False)
            print(iterU +  " has " + colored(str(len(curr_table)) + " Errors",'red'))
##########################################################################################################################