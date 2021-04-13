import datetime
import dateutil.tz
import pathlib
import shutil
import re
import os
import pandas as pd

import File_Submission_Object
from Validation_Rules import Validation_Rules
from Validation_Rules import check_ID_Cross_Sheet

file_sep = os.path.sep
eastern = dateutil.tz.gettz("US/Eastern")
validation_date = datetime.datetime.now(tz = eastern).strftime("%Y-%m-%d")
#############################################################################################################  
def Data_Validation_Main():
    root_dir = r"C:\Seronet_Data_Validation"
    passing_msg = ("File is a valid Zipfile. No errors were found in submission. " + 
                   "Files are good to proceed to Data Validation")
#############################################################################################
    summary_path = root_dir + file_sep + "Summary_of_all_Submissions.xlsx"
    summary_file = []
    if os.path.exists(summary_path):
        xls = pd.ExcelFile(summary_path)
        for iterZ in xls.sheet_names:
            if len(summary_file) == 0:
                summary_file = pd.read_excel(summary_path,sheet_name = iterZ)
            else:
                new_file = pd.read_excel(summary_path,sheet_name = iterZ)
                summary_file = pd.concat([summary_file,new_file])
        summary_file.reset_index(inplace = True)
        summary_file.drop(["index"],inplace = True,axis = 1)
    else:
        summary_file = pd.DataFrame(columns = ["Submission_Status","Date_Of_Last_Status","Folder_Location","CBC_Num","Date_Timestamp","Submission_Name","Validation_Status","JIRA_Ticket","Ticket_Status"])
#############################################################################################
    Support_Files = get_subfolder(root_dir,file_sep,"Support_Files")
    
    create_sub_folders(root_dir,file_sep,"00_Uploaded_Submissions")
    create_sub_folders(root_dir,file_sep,"01_Failed_File_Validation")
    create_sub_folders(root_dir,file_sep,"02_Data_Validation_No_Errors")
    create_sub_folders(root_dir,file_sep,"03_Data_Validation_Column_Errors")
    create_sub_folders(root_dir,file_sep,"04_Data_Validation_Data_Errors")
    
    summary_file = check_for_typo(summary_file)
    summary_file = move_updated(summary_file,root_dir) #if status us updated, moves file back into queue to process
    summary_file = move_folder_to_uploaded(summary_file,root_dir) #if status is uploaded, moves file to correct bucket and updates excel sheet

    CBC_Folders = get_subfolder(root_dir,file_sep,"Files_To_Validate")
    if len(CBC_Folders) == 0:
        print("\nThe Files_To_Validate Folder is empty, no Submissions Downloaded to Process\n")
#############################################################################################    
    for iterT in CBC_Folders:                       #loops over the list of the 4 CBC folders created during SOP       
        date_folders = os.listdir(iterT)
        cbc_name = pathlib.PurePath(iterT).name
        if len(date_folders) == 0:                  #if the CBC folder is empty, skip and move to next folder
            print("There are no submitted files for " + cbc_name)
            continue
        
        for iterD in date_folders:                  #Each submission is proceeded by a timestamp when was submitted         
            Date_path = iterT + file_sep + iterD
            Submissions_Names = os.listdir(Date_path)
        
            for iterS in Submissions_Names:         #If more then one submission within a date folder, checks each seperately
                file_str = (Date_path + file_sep + iterS)
                if os.path.isfile(file_str):
                    os.remove(file_str)
                    file_str = file_str.replace((root_dir + file_sep + "Files_To_Validate"),"")
                    print("\n##    File Validation has NOT been run for " + file_str + "    ##")
                    continue
                current_sub_object = File_Submission_Object.Submission_Object(iterS[15:])
                curr_dict = populate_dict(validation_date,cbc_name,iterD,current_sub_object)
                
                file_check = summary_file.query("CBC_Num == @cbc_name and Date_Timestamp == @iterD and Submission_Name ==@current_sub_object.File_Name")
                if len(file_check) > 0:
                    curr_status = file_check['Submission_Status'].tolist()[0]
                    if curr_status in ["Updated"]:
                         curr_dict = file_check.to_dict('records')
                         curr_dict = curr_dict[0]
                         curr_dict["Submission_Status"] = "Pending Review"
                    else:
                        if curr_status in ["Downloaded"]:
                            print("File Previously Downloaded - Pending Manual Review of Files")
                        elif "Uploaded" in file_check['Submission_Status'].tolist()[0]:
                            print("Submission Previously uploaded to coresponding S3 Bucket")
                        elif curr_status:
                            print("Submission Previously Updated and Reprocssed - Pending Manual Review of changes")
                        elif curr_status not in "Unknown":
                            print("Submission Status (" + curr_status + ") is Unknown. Possible mistyped")
                            print("Defaulting Status to Unknowm and skipping File")
                            
                        shutil.rmtree(Date_path + file_sep + iterS)
                        continue
                
                orgional_path = Date_path + file_sep + iterS
                print("\n## Starting the Data Validation Proccess for " + current_sub_object.File_Name + " ##")
                list_of_folders = os.listdir(Date_path + file_sep + iterS)
                result_message = get_result_message(list_of_folders,root_dir,Date_path,orgional_path,current_sub_object,iterS)
                if (result_message == ''):
                    continue
                elif result_message != passing_msg:
                    print("Submitted File FAILED the File-Validation Process")
                    print("With Error Message: " + result_message + "\n")
                    error_str =  "Submission Failed File Validation"
                    move_file_and_update(orgional_path,root_dir,current_sub_object,curr_dict,"01_Failed_File_Validation",error_str)
                    curr_dict["Validation_Status"] = result_message
                    summary_file = summary_file.append(curr_dict,ignore_index=True)
                    summary_file.drop_duplicates(["CBC_Num","Date_Timestamp","Submission_Name"], keep='last', inplace=True)
                    continue
#######################################################################################################################################               
                list_of_files = []
                if "UnZipped_Files" in list_of_folders:
                    list_of_files = os.listdir(Date_path + file_sep + iterS + file_sep + "Unzipped_Files")
                if len(list_of_files) == 0:
                    print("There are no files found within this submission to process")
                    continue 
                Subpath = Date_path + file_sep + iterS
                current_sub_object = populate_object(current_sub_object, Subpath,list_of_files,file_sep,Support_Files)
                    
                col_err_count = len(current_sub_object.Column_error_count)
                if col_err_count > 0:
                    print("There are (" + str(col_err_count) + ") Column Names in the submission that are wrong/missing")
                    print("Not able to process this submission, please correct and resubmit \n")
                    current_sub_object.write_col_errors((Subpath + file_sep))                    
                    error_str =  "Submission has Column Errors, Data Validation NOT Preformed"
                    move_file_and_update(orgional_path,root_dir,current_sub_object,curr_dict,"03_Data_Validation_Column_Errors",error_str)
                    summary_file = summary_file.append(curr_dict,ignore_index=True)
                    continue
                valid_cbc_ids = str(current_sub_object.CBC_ID)
                for file_name in current_sub_object.Data_Object_Table:
                    if file_name not in ["submission.csv","shipping_manifest.csv"]:
                        if "Data_Table" in current_sub_object.Data_Object_Table[file_name]:
                            try:
                                data_table = current_sub_object.Data_Object_Table[file_name]['Data_Table']
                                data_table,drop_list = current_sub_object.correct_var_types(file_name)
                                current_sub_object = Validation_Rules(re,datetime,current_sub_object,data_table,file_name,valid_cbc_ids,drop_list)
                            except Exception as e:
                                display_error_line(e)
                        else:
                            print (file_name + " was not included in the submission")
##########################################################################################################################################
                check_ID_Cross_Sheet(current_sub_object,re)
                try:
                    create_sub_folders(Date_path,file_sep,iterS + file_sep + "Data_Validation_Results",True)
                    Data_Validation_Path = Date_path + file_sep + iterS + file_sep + "Data_Validation_Results"  
                    current_sub_object.write_error_file(Data_Validation_Path + file_sep)
                    if len(current_sub_object.Error_list) == 0:
                        error_str =  "No Errors were Found during Data Validation"
                        move_file_and_update(orgional_path,root_dir,current_sub_object,curr_dict,"02_Data_Validation_No_Errors",error_str)
                    else:
                        error_str =  "Data Validation found " + str(len(current_sub_object.Error_list)) + " errors in the submitted files"  
                        move_file_and_update(orgional_path,root_dir,current_sub_object,curr_dict,"04_Data_Validation_Data_Errors",error_str)
                except Exception as err:
                    print("An Error Occured when trying to write output file")
                    display_error_line(err)
                print("Validation for this File is complete")
                summary_file = summary_file.append(curr_dict,ignore_index=True)
                summary_file.drop_duplicates(["CBC_Num","Date_Timestamp","Submission_Name"], keep='last', inplace=True)
                
            if len(os.listdir(Date_path)) == 0:
                shutil.rmtree(Date_path)        #if all files have been procesed, remove folder
        print("End of Current CBC Folder (" + cbc_name +"), moving to next CBC Folder")
        if  len(os.listdir(iterT)) == 0:    #once all date folders have been processed, remove CBC folder
             shutil.rmtree(iterT)           #this will avoid confusion when next download happens
    print("ALl folders have been checked")
    print("Closing Validation Program")
    
    summary_file.sort_values(by=['CBC_Num', 'Date_Of_Last_Status','Date_Timestamp'],inplace = True)  
    writer = pd.ExcelWriter(summary_path, engine='xlsxwriter')
    writer = write_excel_sheets(writer,summary_file,"01_Failed_File_Validation","Failed_File_Validation")
    writer = write_excel_sheets(writer,summary_file,"03_Data_Validation_Column_Errors","Column_Errors_Found")
    writer = write_excel_sheets(writer,summary_file,"04_Data_Validation_Data_Errors","Failed_Data_Validation")
    writer = write_excel_sheets(writer,summary_file,"02_Data_Validation_No_Errors","Passed_Data_Validation")
    writer = write_excel_sheets(writer,summary_file,"00_Uploaded_Submissions","Uploaded_Submissions")
    
    writer.save()
    
    clear_empty_folders(root_dir)
##########################################################################################################################################
def display_error_line(ex):
    trace = []
    tb = ex.__traceback__
    while tb is not None:
        trace.append({"filename": tb.tb_frame.f_code.co_filename,"name": tb.tb_frame.f_code.co_name,"lineno": tb.tb_lineno})
        tb = tb.tb_next
    print(str({'type': type(ex).__name__,'message': str(ex),'trace': trace}))
##########################################################################################################################################
def populate_dict(curr_date,cbc_name,iterD,current_sub_object):
    curr_dict = { "Submission_Status":"Downloaded","Date_Of_Last_Status":curr_date,"CBC_Num":cbc_name,
                 "Date_Timestamp":iterD,"Submission_Name":current_sub_object.File_Name, "JIRA_Ticket":" ","Ticket_Status":"In_Progress"}
    
    return curr_dict
def get_subfolder(root_dir,file_sep,folder_name):
    file_path = root_dir + file_sep + folder_name
    file_dir = os.listdir(file_path)
    file_dir = [file_path + file_sep + i for i in file_dir]
    return file_dir
def create_sub_folders(root_dir,file_sep,folder_name,data_folder = False):
    folder_path = root_dir + file_sep + "Files_Processed" + file_sep + folder_name
    if data_folder == True:
        folder_path = root_dir + file_sep + folder_name
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
def move_target_folder(orgional_path,error_path,file_sep,file_name):
    orgional_path = orgional_path + file_sep
    target_path = orgional_path.replace("C:\Seronet_Data_Validation\Files_To_Validate",error_path)
    move_func(orgional_path,target_path)
def move_func(orgional_path,target_path):
    try:
        shutil.move(orgional_path,target_path)  #if path does not exist will create
    except Exception:
        shutil.rmtree(target_path)              #if path alredy exists, remove it
        shutil.move(orgional_path,target_path)  #recreate path with new files
def move_failed_sub(orgional_path,failed_dir,file_sep,dest_path,current_sub_object):
     error_path = failed_dir + file_sep + dest_path
     move_target_folder(orgional_path,error_path,file_sep,current_sub_object.File_Name)  
def move_updated(summary_file,root_dir):
    print("\n#####   Checking for Submissions flagged as Updated   #####\n")
    samp_file = summary_file.query("Submission_Status in ['Updated']")
    move_count = 0
    for iterZ in samp_file.index:
#        curr_file = samp_file["CBC_Num"] + file_sep + samp_file["Date_Timestamp"] + file_sep + samp_file["Submission_Name"]
        curr_loc = (samp_file["Folder_Location"][iterZ] + file_sep + samp_file["CBC_Num"][iterZ] + 
                   file_sep + samp_file["Date_Timestamp"][iterZ])
        
        new_path = (root_dir + file_sep + "Files_To_Validate" + file_sep + samp_file["CBC_Num"][iterZ] + 
                   file_sep + samp_file["Date_Timestamp"][iterZ])
        move_func(curr_loc,new_path)
        move_count = move_count + 1
    if move_count == 0:
        print("No Submissions were updated since last time program was run")
    else:
        print("There are " + str(move_count) + " Submissions Found that had Updates (Changes)")
        print("These submissions have been moved back into the Files_To_Validate Folder to be reproccesed")
    return summary_file
def move_folder_to_uploaded(summary_file,root_dir):
    print("\n#####   Checking for Submissions that have been uploaded to the S3 Bucket   #####\n")
    samp_file = summary_file.query("Submission_Status in ['Uploaded_to_Failed_S3_Bucket','Uploaded_to_Passed_S3_Bucket']")
    move_count = 0
    for iterZ in samp_file.index:
        if "00_Uploaded_Submissions" in samp_file["Folder_Location"][iterZ]:
            continue  #file has already been uploaded, nothing to check
        curr_loc = (samp_file["Folder_Location"][iterZ] + file_sep + samp_file["CBC_Num"][iterZ] + 
                   file_sep + samp_file["Date_Timestamp"][iterZ])
        
        new_path = (root_dir + file_sep + "Files_Processed" + file_sep + "00_Uploaded_Submissions" + file_sep + samp_file["CBC_Num"][iterZ] + 
                   file_sep + samp_file["Date_Timestamp"][iterZ])
        
        new_location = root_dir + file_sep + "Files_Processed" + file_sep + "00_Uploaded_Submissions"
        move_func(curr_loc,new_path)

        move_count = move_count + 1
        summary_file["Folder_Location"][iterZ] = new_location
    if move_count == 0:
        print("No Submissions were flagged as uploaded since last time program was run")
    else:
        print("There are " + str(move_count) + " Submissions Found that were uploaded to an S3 Bucket")
        print("These submissions have been moved to the Uploaded_Submissions Folder")
    return summary_file
def get_result_message(list_of_folders,root_dir,Date_path,orgional_path,current_sub_object,iterS):
    result_message = ''
    if "File_Validation_Results" not in list_of_folders:
        print("File-Validation has not been run on this submission\n")
        shutil.rmtree(orgional_path)
    else:
       result_file = Date_path + file_sep + iterS + file_sep + "File_Validation_Results" + file_sep + "Result_Message.txt"
       result_message = open(result_file, "r").read()
    return result_message
def populate_object(current_sub_object, Subpath,list_of_files,file_sep,Support_Files):
    for iterF in list_of_files:
        file_path = Subpath + file_sep + "Unzipped_Files" + file_sep + iterF
        current_sub_object.get_data_tables(iterF,file_path)
        current_sub_object.column_validation(iterF,Support_Files)
    current_sub_object.get_submission_metadata(Support_Files)
    return current_sub_object
def move_file_and_update(orgional_path,root_dir,current_sub_object,curr_dict,folder_str,error_str):
    root_dir = root_dir + file_sep + "Files_Processed"
    move_failed_sub(orgional_path,root_dir,file_sep,folder_str,current_sub_object)
    curr_dict["Validation_Status"] = error_str
    curr_dict["Folder_Location"] = root_dir  + file_sep + folder_str
def write_excel_sheets(writer,summary_file,file_path,new_sheet_name):
    df1 = summary_file[summary_file["Folder_Location"].apply(lambda x: file_path in x)]
    df1.to_excel(writer, sheet_name = new_sheet_name, index = False)
    return writer
def clear_empty_folders(root_dir):
    process_path = root_dir + file_sep + "Files_Processed"
    processed_folders = os.listdir(process_path)
    for iterF in processed_folders:
        cbc_folders = os.listdir(process_path + file_sep + iterF)
        for iterC in cbc_folders:
            curr_cbc =  os.listdir(process_path + file_sep + iterF + file_sep + iterC)
            if len(curr_cbc) == 0:      #no submissions in this fodler
               shutil.rmtree(process_path + file_sep + iterF + file_sep+ iterC)
def check_for_typo(summary_file):
    print("#####   Checking for Errors/Typos in the Submission Status Field  #####")
    print("Valid Submission Status options are Downloaded, Updated, Uploaded_to_Passed, Uploaded_to_Failed \n")
    error_count = 0
    for iterZ in summary_file.index:
        curr_status = summary_file["Submission_Status"][iterZ]
        curr_status = curr_status.lower()
        if curr_status in  ["updated","update","fixed"]:
            summary_file["Submission_Status"][iterZ] = "Updated"
        elif "upload" in curr_status or "uploaded" in curr_status:
            if "pass" in curr_status or "passed" in curr_status:
                summary_file["Submission_Status"][iterZ] = "Uploaded_to_Passed_S3_Bucket"
            elif "fail" in curr_status or "faileded" in curr_status:
                summary_file["Submission_Status"][iterZ] = "Uploaded_to_Failed_S3_Bucket"
            else:
                summary_file["Submission_Status"][iterZ] = "Unknown"
        elif curr_status not in  ["downloaded","pending review"]:
            summary_file["Submission_Status"][iterZ] = "Unknown"
        if summary_file["Submission_Status"][iterZ] == "Unknown":
            error_count = error_count + 1
            print(curr_status + " is not a valid Submission Status Option. Defaulting to Unknown")
    if error_count == 0:
        print("No Submission Status Errors were found")
    return summary_file
##########################################################################################################################################
Data_Validation_Main()