import datetime
import dateutil.tz
import pathlib
import shutil
import re
import os
import pandas as pd
from colorama import init
from termcolor import colored
import time
from pandas_aws import s3 as pd_s3
import boto3
import aws_creds_prod

import File_Submission_Object
from Validation_Rules import Validation_Rules
from Validation_Rules import check_ID_Cross_Sheet
from Validation_Rules import compare_tests

import get_assay_data_from_box
from get_data_to_check import get_summary_file

import warnings
warnings.simplefilter("ignore")

file_sep = os.path.sep
eastern = dateutil.tz.gettz("US/Eastern")
validation_date = datetime.datetime.now(tz=eastern).strftime("%Y-%m-%d")

user_dir = os.getcwd()
box_dir = "C:" + file_sep + "Users" + file_sep + os.getlogin() + file_sep + "Box"

s3_client = boto3.client('s3', aws_access_key_id=aws_creds_prod.aws_access_id, aws_secret_access_key=aws_creds_prod.aws_secret_key,
                         region_name='us-east-1')
s3_resource = boto3.resource('s3', aws_access_key_id=aws_creds_prod.aws_access_id, aws_secret_access_key=aws_creds_prod.aws_secret_key,
                             region_name='us-east-1')


def get_box_data(box_dir, box_path):
    file_path = []
    cur_path = box_dir + file_sep + box_path
    for r, d, f in os.walk(cur_path):  # r=root, d=directories, f = files
        for file in f:
            if (file.endswith(".xlsx")):
                file_path.append(os.path.join(r, file))
    return file_path


templates = get_box_data(box_dir, "CBC Data Submission Documents" + file_sep + "Data Submission Templates")
cbc_codes = get_box_data(box_dir, "SeroNet DMS" + file_sep + "12 SeroNet Data Submitter Information")

Support_Files = templates + cbc_codes
#############################################################################################


def Data_Validation_Main():
    init()
    tic = time.perf_counter()
    root_dir = "C:\\Seronet_Data_Validation"
    passing_msg = ("File is a valid Zipfile. No errors were found in submission. " +
                   "Files are good to proceed to Data Validation")
#############################################################################################
    summary_path = root_dir + file_sep + "Summary_of_all_Submissions.xlsx"
    summary_file = get_summary_file(os, pd, root_dir, file_sep, s3_client, summary_path)
#############################################################################################
    if "Submission_Status" not in summary_file.columns:
        print("Excel file Exists but is empty, will cause errors during validation")
        print("Delete File and re-run program.  Terminating Data Validation")
        clear_empty_folders(root_dir)
        input("\n\nPress Enter to close window...")
        return{}

    assay_data, assay_target, all_qc_data, converion_file = get_assay_data_from_box.get_assay_data()

    create_sub_folders(root_dir, "00_Uploaded_Submissions")
    create_sub_folders(root_dir, "01_Failed_File_Validation")
    create_sub_folders(root_dir, "02_Data_Validation_No_Errors")
    create_sub_folders(root_dir, "03_Data_Validation_Column_Errors")
    create_sub_folders(root_dir, "04_Data_Validation_Data_Errors")
    create_sub_folders(root_dir, "05_Data_Validation_Major_Errors")
    create_sub_folders(root_dir, "06_Data_Validation_Minor_Errors")

    summary_file = check_for_typo(summary_file)
    summary_file = move_minor_errors(summary_file, root_dir)
    summary_file = move_major_errors(summary_file, root_dir)
    summary_file = move_updated(summary_file, root_dir)
    summary_file = move_folder_to_uploaded(summary_file, root_dir)
    CBC_Folders = get_subfolder(root_dir, "Files_To_Validate")

############################################################################################
# validate serology confirm data
    bucket = "nci-cbiit-seronet-submissions-passed"
    key = "serology/serology_confirmation_test_result/"
    file_name = "serology_confirmation_test_results.csv"
    serology_code = 12
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
    for curr_serology in resp["Contents"]:
        if ("Test_Results_Passed" in curr_serology["Key"]) or ("Test_Results_Failed" in curr_serology["Key"]):
            pass
        elif ".xlsx" in curr_serology["Key"]:
            current_sub_object = File_Submission_Object.Submission_Object("Serology")
            serology_data = pd_s3.get_df_from_keys(s3_client, bucket, prefix=curr_serology["Key"], suffix="xlsx",
                                                   format="xlsx", na_filter=False, output_type="pandas")

            data_table, drop_list = current_sub_object.validate_serology(file_name, serology_data,
                                                                         assay_data, assay_target, serology_code)
            current_sub_object = Validation_Rules(re, datetime, current_sub_object, data_table,
                                                  file_name, [serology_code], drop_list)
            error_list = current_sub_object.Error_list
            if len(error_list) == 0:
                print("\n## Serology File has No Errors, Submission is Valid\n")

    if len(CBC_Folders) == 0:
        print("\nThe Files_To_Validate Folder is empty, no Submissions Downloaded to Process\n")
#############################################################################################
    rename_CBC_folders(root_dir, CBC_Folders)
    CBC_Folders = get_subfolder(root_dir, "Files_To_Validate")

    if len(CBC_Folders) > 1:
        sort_order = [int(i[-2:]) for i in CBC_Folders]
        sort_list = sorted(range(len(sort_order)), key=lambda k: sort_order[k])
        CBC_Folders = [CBC_Folders[i] for i in sort_list]

    all_res = []
    time_list = []
    cbc_name_list = []
    file_name_list = []
    date_name_list = []
    toc = time.perf_counter()
    print(f"Folder Creation, and summary file in {toc - tic:0.4f} seconds")

    for iterT in CBC_Folders:
        date_folders = os.listdir(iterT)
        cbc_name = pathlib.PurePath(iterT).name
        res = [cbc_name, 0, 0]
        if len(date_folders) == 0:
            print("There are no submitted files for " + cbc_name)
            clear_dir(iterT)
            all_res.append(res)
            continue

        for iterD in date_folders:
            Date_path = iterT + file_sep + iterD
            Submissions_Names = os.listdir(Date_path)
            rem_data = 0
            rem_data = check_test_data(Date_path)
            if rem_data == 1:  # if submission is a test no need to check it
                date_name_list.append(iterD)
                file_name_list.append("File not run")
                time_list.append(f"{0.0000} seconds")
                continue

            file_count = 0
            for iterS in Submissions_Names:
                cbc_name_list.append(res[0])
                date_name_list.append(iterD)
                file_name_list.append(iterS)
                tic = time.perf_counter()
                file_str = (Date_path + file_sep + iterS)
                if os.path.isfile(file_str):
                    os.remove(file_str)
                    file_str = file_str.replace((root_dir + file_sep + "Files_To_Validate"), "")
                    print(colored("\n##    File Validation has NOT been run for " + file_str + "    ##", 'yellow'))
                    toc = time.perf_counter()
                    time_list.append(f"{toc - tic:0.4f} seconds")
                    continue
                current_sub_object = File_Submission_Object.Submission_Object(iterS[15:])
                curr_dict = populate_dict(validation_date, cbc_name, iterD, current_sub_object)

                file_check = summary_file.query("CBC_Num == @cbc_name and Date_Timestamp == @iterD and " +
                                                " Submission_Name ==@current_sub_object.File_Name")
                if len(file_check) > 0:
                    curr_status = file_check['Submission_Status'].tolist()[0]
                    if curr_status in ["Updated"]:
                        curr_dict = file_check.to_dict('records')
                        curr_dict = curr_dict[0]
                        curr_dict["Submission_Status"] = "Pending Review"
                    else:
                        if curr_status in ["Downloaded"]:
                            pass
                        elif "Uploaded" in curr_status:
                            pass
                        elif curr_status in ["Pending Review"]:
                            print("Submission Previously Updated and Reprocssed" +
                                  " - Pending Manual Review of changes")
                        elif curr_status not in "Unknown":
                            print("Submission Status (" + curr_status + ") is Unknown." +
                                  " Possible mistyped")
                            print("Defaulting Status to Unknowm and skipping File")

                        shutil.rmtree(Date_path + file_sep + iterS)
                        continue

                orgional_path = Date_path + file_sep + iterS
                print("\n## Starting the Data Validation Proccess for " + current_sub_object.File_Name + " ##")
                file_count = file_count + 1
                list_of_folders = os.listdir(Date_path + file_sep + iterS)
                result_message = get_result_message(list_of_folders, Date_path, orgional_path, iterS)
                if (result_message == ''):
                    summary_file = summary_file.append(curr_dict, ignore_index=True)
                    toc = time.perf_counter()
                    time_list.append(f"{toc - tic:0.4f} seconds")
                    continue
                if result_message != passing_msg:
                    print("Submitted File FAILED the File-Validation Process")
                    print("With Error Message: " + result_message + "\n")
                    error_str = "Submission Failed File Validation"
                    move_file_and_update(orgional_path, root_dir, current_sub_object, curr_dict,
                                         "01_Failed_File_Validation", error_str)
                    curr_dict["Validation_Status"] = result_message
                    toc = time.perf_counter()
                    time_list.append(f"{toc - tic:0.4f} seconds")
                    summary_file = summary_file.append(curr_dict, ignore_index=True)
#                    summary_file.drop_duplicates(["CBC_Num", "Date_Timestamp", "Submission_Name"], keep='last', inplace=True)
                    continue
#############################################################################################################################
                list_of_files = []
                if "UnZipped_Files" in list_of_folders:
                    list_of_files = os.listdir(Date_path + file_sep + iterS + file_sep + "Unzipped_Files")
                if len(list_of_files) == 0:
                    print("There are no files found within this submission to process")
                    continue
                Subpath = Date_path + file_sep + iterS
                try:
                    current_sub_object = populate_object(current_sub_object, Subpath, list_of_files, Support_Files)
                except Exception as e:
                    print(e)
                current_sub_object.update_object(assay_data, "assay.csv")
                current_sub_object.update_object(assay_target, "assay_target.csv")
#############################################################################################################################
                col_err_count = len(current_sub_object.Column_error_count)
#                col_err_count = 0
                if col_err_count > 0:
                    print(colored("There are (" + str(col_err_count) +
                                  ") Column Names in the submission that are wrong/missing", 'red'))
                    print(colored("Not able to process this submission, please correct and resubmit \n", 'red'))
                    current_sub_object.write_col_errors((Subpath + file_sep))
                    error_str = "Submission has Column Errors, Data Validation NOT Preformed"
                    move_file_and_update(orgional_path, root_dir, current_sub_object, curr_dict,
                                         "03_Data_Validation_Column_Errors", error_str)
                    summary_file = summary_file.append(curr_dict, ignore_index=True)
                    toc = time.perf_counter()
                    time_list.append(f"{toc - tic:0.4f} seconds")
                    continue
                valid_cbc_ids = str(current_sub_object.CBC_ID)
                for file_name in current_sub_object.Data_Object_Table:
                    if file_name not in ["submission.csv", "shipping_manifest.csv", "assay.csv", "assay_target.csv"]:
                        if "Data_Table" in current_sub_object.Data_Object_Table[file_name]:
                            try:
                                data_table = current_sub_object.Data_Object_Table[file_name]['Data_Table']
                                data_table, drop_list = current_sub_object.correct_var_types(file_name)
                                current_sub_object = Validation_Rules(re, datetime, current_sub_object, data_table,
                                                                      file_name, valid_cbc_ids, drop_list)
                            except Exception as e:
                                display_error_line(e)
                        else:
                            print(file_name + " was not included in the submission")
#############################################################################################################################
                check_ID_Cross_Sheet(current_sub_object, re)
                compare_tests(current_sub_object)

                if (("shipping_manifest.csv" in current_sub_object.Data_Object_Table) and
                   ("aliquot.csv" in current_sub_object.Data_Object_Table)):
                    shipping_table = current_sub_object.Data_Object_Table["shipping_manifest.csv"]["Data_Table"][0]
                    aliquot_table = current_sub_object.Data_Object_Table["aliquot.csv"]["Data_Table"]
                    compare_tables = shipping_table.merge(aliquot_table, left_on="Current Label", right_on="Aliquot_ID", indicator=True, how="outer")
                    compare_tables = compare_tables.query("_merge not in ['both']")
                try:
                    create_sub_folders(Date_path, iterS + file_sep + "Data_Validation_Results", True)
                    Data_Validation_Path = Date_path + file_sep + iterS + file_sep + "Data_Validation_Results"
                    current_sub_object.write_error_file(Data_Validation_Path + file_sep)
                    if len(current_sub_object.Error_list) == 0:
                        error_str = "No Errors were Found during Data Validation"
                        move_file_and_update(orgional_path, root_dir, current_sub_object, curr_dict,
                                             "02_Data_Validation_No_Errors", error_str)
                    else:
                        error_str = ("Data Validation found " + str(len(current_sub_object.Error_list)) +
                                     " errors in the submitted files")
                        move_file_and_update(orgional_path, root_dir, current_sub_object, curr_dict,
                                             "04_Data_Validation_Data_Errors", error_str)
                except Exception as err:
                    print("An Error Occured when trying to write output file")
                    display_error_line(err)
                print("Validation for this File is complete")
                summary_file = summary_file.append(curr_dict, ignore_index=True)
#                summary_file.drop_duplicates(["CBC_Num", "Date_Timestamp", "Submission_Name"], keep='last', inplace=True)

                toc = time.perf_counter()
                time_list.append(f"{toc - tic:0.4f} seconds")

            if file_count > 0:
                res[1] = res[1] + 1
                res[2] = res[2] + file_count
            if len(os.listdir(Date_path)) == 0:
                shutil.rmtree(Date_path)

        print(colored("\nEnd of Current CBC Folder (" + cbc_name + "), moving to next CBC Folder", 'blue'))
        all_res.append(res)
        clear_dir(iterT)

    print("\nALl folders have been checked")
    print("Closing Validation Program")

    summary_file.sort_values(by=['CBC_Num', 'Date_Of_Last_Status', 'Date_Timestamp'], inplace=True)
    writer = pd.ExcelWriter(summary_path, engine='xlsxwriter')
    writer = write_excel_sheets(writer, summary_file, "01_Failed_File_Validation", "Failed_File_Validation")
    writer = write_excel_sheets(writer, summary_file, "03_Data_Validation_Column_Errors", "Column_Errors_Found")
    writer = write_excel_sheets(writer, summary_file, "04_Data_Validation_Data_Errors", "Failed_Data_Validation")
    writer = write_excel_sheets(writer, summary_file, "06_Data_Validation_Minor_Errors", "Pending_Feedback")
    writer = write_excel_sheets(writer, summary_file, "05_Data_Validation_Major_Errors", "Major_Errors_Found")
    writer = write_excel_sheets(writer, summary_file, "02_Data_Validation_No_Errors", "Passed_Data_Validation")
    writer = write_excel_sheets(writer, summary_file, "00_Uploaded_Submissions", "Uploaded_Submissions")

    writer.save()
    writer.close()
    writer.handles = None

    print("\n#### Validation Summary ####\n")
    for iterZ in range(len(all_res)):
        print("CBC :: " + all_res[iterZ][0] + ", Had " + str(all_res[iterZ][1]) + " date folders checked." +
              "  And processed " + str(all_res[iterZ][2]) + " unique Submissions")

    clear_empty_folders(root_dir)
    input("\n\nPress Enter to close window...")


def clear_dir(file_path):
    if len(os.listdir(file_path)) == 0:
        shutil.rmtree(file_path)


def display_error_line(ex):
    trace = []
    tb = ex.__traceback__
    while tb is not None:
        trace.append({"filename": tb.tb_frame.f_code.co_filename,
                      "name": tb.tb_frame.f_code.co_name,
                      "lineno": tb.tb_lineno})
        tb = tb.tb_next
    print(str({'type': type(ex).__name__, 'message': str(ex), 'trace': trace}))


def rename_CBC_folders(root_dir, CBC_Folders):
    os.chdir(root_dir + file_sep + "Files_To_Validate" + file_sep)
    for cur_folder in CBC_Folders:
        if "cbc01" in cur_folder:
            os.rename("cbc01", "Feinstein_CBC01")
        if "cbc02" in cur_folder:
            os.rename("cbc02", "UMN_CBC02")
        if "cbc03" in cur_folder:
            os.rename("cbc03", "ASU_CBC03")
        if "cbc04" in cur_folder:
            os.rename("cbc04", "Mt_Sinai_CBC04")


def populate_dict(curr_date, cbc_name, iterD, current_sub_object):
    curr_dict = {"Submission_Status": "Downloaded", "Date_Of_Last_Status": curr_date, "CBC_Num": cbc_name,
                 "Date_Timestamp": iterD, "Submission_Name": current_sub_object.File_Name,
                 "JIRA_Ticket": " ", "Ticket_Status": "In_Progress"}
    return curr_dict


def get_subfolder(root_dir, folder_name):
    file_path = root_dir + file_sep + folder_name
    file_dir = os.listdir(file_path)
    file_dir = [file_path + file_sep + i for i in file_dir]
    return file_dir


def create_sub_folders(root_dir, folder_name, data_folder=False):
    folder_path = root_dir + file_sep + "Files_Processed" + file_sep + folder_name
    if data_folder is True:
        folder_path = root_dir + file_sep + folder_name
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)


def move_target_folder(orgional_path, error_path, file_name):
    # orgional_path = orgional_path + file_sep + file_name
    target_path = orgional_path.replace("C:\\Seronet_Data_Validation\\Files_To_Validate", error_path)
    move_func(orgional_path, target_path)


def move_func(orgional_path, target_path):
    try:
        shutil.move(orgional_path, target_path)
    except Exception:
        shutil.rmtree(target_path)
        shutil.move(orgional_path, target_path)


def move_failed_sub(orgional_path, failed_dir, dest_path, current_sub_object):
    error_path = failed_dir + file_sep + dest_path
    move_target_folder(orgional_path, error_path, current_sub_object.File_Name)


def check_multi_sub(curr_loc, new_path, samp_file, iterZ):
    if len(os.listdir(curr_loc)) > 1:
        file_list = os.listdir(curr_loc)
        if not os.path.exists(new_path):
            os.makedirs(new_path)

        curr_submission = [i for i in file_list if samp_file["Submission_Name"][iterZ] in i][0]
        curr_loc = curr_loc + file_sep + curr_submission
        new_path = new_path + file_sep + curr_submission
    return curr_loc, new_path


def move_updated(summary_file, root_dir):
    print("\n#####   Checking for Submissions flagged as Updated   #####\n")
    samp_file = summary_file.query("Submission_Status in ['Updated']")
    move_count = 0
    for iterZ in samp_file.index:
        curr_loc = (samp_file["Folder_Location"][iterZ] + file_sep + samp_file["CBC_Num"][iterZ] +
                    file_sep + samp_file["Date_Timestamp"][iterZ])
        new_path = (root_dir + file_sep + "Files_To_Validate" + file_sep + samp_file["CBC_Num"][iterZ] +
                    file_sep + samp_file["Date_Timestamp"][iterZ])

        curr_loc, new_path = check_multi_sub(curr_loc, new_path, samp_file, iterZ)
        move_func(curr_loc, new_path)
        move_count = move_count + 1
    if move_count == 0:
        print("No Submissions were updated since last time program was run")
    else:
        print("There are " + str(move_count) + " Submissions Found that had Updates (Changes)")
        print("These submissions have been moved back into the Files_To_Validate Folder to be reproccesed")
    return summary_file


def move_major_errors(summary_file, root_dir):
    print("\n#####   Checking for Submissions flagged as Major_Errors_Found   #####\n")
    samp_file = summary_file[summary_file.apply(lambda x: x['Submission_Status'] in ['Major_Errors_Found'] and
                                                '05_Data_Validation_Major_Errors' not in x['Folder_Location'], axis=1)]
    move_count = 0
    for iterZ in samp_file.index:
        curr_loc = (samp_file["Folder_Location"][iterZ] + file_sep + samp_file["CBC_Num"][iterZ] +
                    file_sep + samp_file["Date_Timestamp"][iterZ])

        new_path = (root_dir + file_sep + "Files_Processed" + file_sep + "05_Data_Validation_Major_Errors" + file_sep +
                    samp_file["CBC_Num"][iterZ] + file_sep + samp_file["Date_Timestamp"][iterZ])

        curr_loc, new_path = check_multi_sub(curr_loc, new_path, samp_file, iterZ)
        move_func(curr_loc, new_path)
        move_count = move_count + 1
        new_location = root_dir + file_sep + "Files_Processed" + file_sep + "05_Data_Validation_Major_Errors"
        summary_file["Folder_Location"][iterZ] = new_location

    if move_count == 0:
        print("No Submissions were flagged as major errors since last time program was run")
    else:
        print("There are " + str(move_count) + " Submissions Found that had Major Errors")
        print("These submissions have been moved into 05_Data_Validation_Major_Errors")
    return summary_file


def move_minor_errors(summary_file, root_dir):
    print("\n#####   Checking for Submissions flagged as Pending_Feedback   #####\n")
    samp_file = summary_file[summary_file.apply(lambda x: x['Submission_Status'] in ['Pending_Feedback'] and
                                                '06_Data_Validation_Minor_Errors' not in
                                                x['Folder_Location'], axis=1)]
    move_count = 0
    for iterZ in samp_file.index:
        curr_loc = (samp_file["Folder_Location"][iterZ] + file_sep + samp_file["CBC_Num"][iterZ] +
                    file_sep + samp_file["Date_Timestamp"][iterZ])

        new_path = (root_dir + file_sep + "Files_Processed" + file_sep + "06_Data_Validation_Minor_Errors" + file_sep +
                    samp_file["CBC_Num"][iterZ] + file_sep + samp_file["Date_Timestamp"][iterZ])

        curr_loc, new_path = check_multi_sub(curr_loc, new_path, samp_file, iterZ)
        move_func(curr_loc, new_path)
        move_count = move_count + 1

        new_location = root_dir + file_sep + "Files_Processed" + file_sep + "06_Data_Validation_Minor_Errors"
        summary_file["Folder_Location"][iterZ] = new_location
    if move_count == 0:
        print("No Submissions were flagged as pending feedback since last time program was run")
    else:
        print("There are " + str(move_count) + " Submissions Found that had Minor Errors")
        print("These submissions have been moved into 06_Data_Validation_Minor_Errors")
    return summary_file


def move_folder_to_uploaded(summary_file, root_dir):
    print("\n#####   Checking for Submissions that have been uploaded to the S3 Bucket   #####\n")
    samp_file = summary_file.query("Submission_Status in ['Uploaded_to_Failed_S3_Bucket', " +
                                   "'Uploaded_to_Passed_S3_Bucket']")
    move_count = 0
    for iterZ in samp_file.index:
        if "00_Uploaded_Submissions" in samp_file["Folder_Location"][iterZ]:
            continue
        curr_loc = (samp_file["Folder_Location"][iterZ] + file_sep + samp_file["CBC_Num"][iterZ] +
                    file_sep + samp_file["Date_Timestamp"][iterZ])

        new_path = (root_dir + file_sep + "Files_Processed" + file_sep + "00_Uploaded_Submissions" +
                    file_sep + samp_file["CBC_Num"][iterZ] +
                    file_sep + samp_file["Date_Timestamp"][iterZ])

        curr_loc, new_path = check_multi_sub(curr_loc, new_path, samp_file, iterZ)
        new_loc = root_dir + file_sep + "Files_Processed" + file_sep + "00_Uploaded_Submissions"
        move_func(curr_loc, new_path)

        move_count = move_count + 1
        summary_file["Folder_Location"][iterZ] = new_loc
    if move_count == 0:
        print("No Submissions were flagged as uploaded since last time program was run")
    else:
        print("There are " + str(move_count) + " Submissions Found " +
              " that were uploaded to an S3 Bucket")
        print("These submissions have been moved to the Uploaded_Submissions Folder")
    return summary_file


def get_result_message(list_of_folders, Date_path, orgional_path, iterS):
    result_message = ''
    if "File_Validation_Results" not in list_of_folders:
        print(colored("File-Validation has not been run on this submission\n", "yellow"))
        shutil.rmtree(orgional_path)
    else:
        curr_file = os.listdir(Date_path + file_sep + iterS + file_sep + "File_Validation_Results")
        if "Result_Message.txt" in curr_file:
            result_file = (Date_path + file_sep + iterS + file_sep + "File_Validation_Results" +
                           file_sep + "Result_Message.txt")
            result_message = open(result_file, "r").read()
        else:
            result_message = "Result File Is Missing"
    return result_message


def populate_object(current_sub_object, Subpath, list_of_files, Support_Files):
    for iterF in list_of_files:
        file_path = Subpath + file_sep + "Unzipped_Files" + file_sep + iterF
        current_sub_object.get_data_tables(iterF, file_path)
        current_sub_object.column_validation(iterF, Support_Files)
    current_sub_object.get_submission_metadata(Support_Files)
    return current_sub_object


def move_file_and_update(orgional_path, root_dir, current_sub_object,
                         curr_dict, folder_str, error_str):
    root_dir = root_dir + file_sep + "Files_Processed"
    move_failed_sub(orgional_path, root_dir, folder_str, current_sub_object)
    curr_dict["Validation_Status"] = error_str
    curr_dict["Folder_Location"] = root_dir + file_sep + folder_str


def write_excel_sheets(writer, summary_file, file_path, new_sheet_name):
    df1 = summary_file[summary_file["Folder_Location"].apply(lambda x: file_path in x)]
    df1.to_excel(writer, sheet_name=new_sheet_name, index=False)
    return writer


def clear_empty_folders(root_dir):
    process_path = root_dir + file_sep + "Files_Processed"
    processed_folders = os.listdir(process_path)
    for iterF in processed_folders:
        cbc_folders = os.listdir(process_path + file_sep + iterF)
        for iterC in cbc_folders:
            curr_cbc = os.listdir(process_path + file_sep + iterF + file_sep + iterC)
            if len(curr_cbc) == 0:
                shutil.rmtree(process_path + file_sep + iterF + file_sep + iterC)


def check_for_typo(summary_file):
    print("#####   Checking for Errors/Typos in the Submission Status Field  #####")
    print("Valid Submission Status options are:\n Downloaded, Updated, Uploaded_to_Passed," +
          "Uploaded_to_Failed, Major_Errors_Found, Pending_Feedback \n")
    error_count = 0
    for iterZ in summary_file.index:
        curr_status = summary_file["Submission_Status"][iterZ]
        curr_status = curr_status.lower()
        if curr_status in ["updated", "update", "fixed"]:
            summary_file["Submission_Status"][iterZ] = "Updated"
        elif ("upload" in curr_status) or ("uploaded" in curr_status):
            if ("pass" in curr_status) or ("passed" in curr_status):
                summary_file["Submission_Status"][iterZ] = "Uploaded_to_Passed_S3_Bucket"
            elif ("fail" in curr_status) or ("faileded" in curr_status):
                summary_file["Submission_Status"][iterZ] = "Uploaded_to_Failed_S3_Bucket"
            else:
                summary_file["Submission_Status"][iterZ] = "Unknown"
        elif ("major" in curr_status) or ("errors" in curr_status):
            summary_file["Submission_Status"][iterZ] = "Major_Errors_Found"
        elif ("pending" in curr_status) or ("feedback" in curr_status):
            summary_file["Submission_Status"][iterZ] = "Pending_Feedback"
        elif curr_status not in ["downloaded", "pending review"]:
            summary_file["Submission_Status"][iterZ] = "Unknown"
        if summary_file["Submission_Status"][iterZ] == "Unknown":
            error_count = error_count + 1
            print(curr_status + " is not a valid Submission Status Option. Defaulting to Unknown")
    if error_count == 0:
        print("No Submission Status Errors were found")
    return summary_file


def check_test_data(Date_path):
    rem_file = 0
    if(("Feinstein_CBC01" + file_sep + "09-13-36-06-22-2021" in Date_path) or
       ("Feinstein_CBC01" + file_sep + "14-58-48-03-26-2021" in Date_path) or
       ("Feinstein_CBC01" + file_sep + "16-13-43-04-01-2021" in Date_path)):
        rem_file = 1
    if(("ASU_CBC03" + file_sep + "10-03-08-06-11-2021" in Date_path) or
       ("ASU_CBC03" + file_sep + "15-43-32-06-10-2021" in Date_path)):
        rem_file = 1
    if(("UMN_CBC02" + file_sep + "12-37-48-04-12-2021" in Date_path)):
        rem_file = 1
    if(("Mt_Sinai_CBC04" + file_sep + "15-54-47-05-06-2021" in Date_path) or
       ("Mt_Sinai_CBC04" + file_sep + "12-11-00-06-11-2021" in Date_path) or
       ("Mt_Sinai_CBC04" + file_sep + "15-45-38-05-06-2021" in Date_path) or
       ("Mt_Sinai_CBC04" + file_sep + "15-48-31-05-06-2021" in Date_path) or
       ("Mt_Sinai_CBC04" + file_sep + "15-51-30-05-06-2021" in Date_path)):
        rem_file = 1
    if rem_file == 1:
        shutil.rmtree(Date_path)
    return rem_file


Data_Validation_Main()
