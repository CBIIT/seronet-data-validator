from import_loader_cbc import *

import File_Submission_Object
from Validation_Rules import Validation_Rules
from Validation_Rules import check_ID_Cross_Sheet
from Validation_Rules import compare_tests
#############################################################################################
file_sep, Support_Files, validation_date = set_up_function()

def Data_Validation_Main():
    root_dir = "C:\\Data_Validation_CBC\Data_Folder"
    name_of_file = "CBC_Testing_Submission"
    create_sub_folders(root_dir + file_sep + "Data_Validation_Results")
    Error_path = root_dir + file_sep + "Data_Validation_Results"
    assay_data, assay_target, all_qc_data, converion_file = get_assay_data_from_box.get_assay_data()
############################################################################################
    try:
        list_of_files = os.listdir(root_dir)
        list_of_files = [i for i in list_of_files if ".csv" in i]
        if len(list_of_files) == 0:
            print("There are no files found within this submission to process")
            return
        try:
            current_sub_object = File_Submission_Object.Submission_Object(name_of_file)
            current_sub_object = populate_object(current_sub_object, root_dir, list_of_files, Support_Files)
            for file_name in current_sub_object.Data_Object_Table:
                current_sub_object.set_key_cols(file_name)
        except Exception as e:
            print(e)
        current_sub_object.update_object(assay_data, "assay.csv")
        current_sub_object.update_object(assay_target, "assay_target.csv")
        col_err_count = len(current_sub_object.Column_error_count)
        if col_err_count > 0:
            print("There are (" + str(col_err_count) + ") Column Names in the submission that are wrong/missing")
            print("Not able to process this submission, please correct and resubmit \n")
            current_sub_object.write_col_errors((Error_path + file_sep))
            return

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
        check_ID_Cross_Sheet(current_sub_object, re)
        compare_tests(current_sub_object)
        if (("shipping_manifest.csv" in current_sub_object.Data_Object_Table) and
           ("aliquot.csv" in current_sub_object.Data_Object_Table)):
            shipping_table = current_sub_object.Data_Object_Table["shipping_manifest.csv"]["Data_Table"][0]
            aliquot_table = current_sub_object.Data_Object_Table["aliquot.csv"]["Data_Table"]
            compare_tables = shipping_table.merge(aliquot_table, left_on="Current Label",
                                                  right_on="Aliquot_ID", indicator=True, how="outer")
            compare_tables = compare_tables.query("_merge not in ['both']")
            current_sub_object.write_error_file(Error_path + file_sep)
        print("Validation for this File is complete")
    except Exception as e:
        print(e)
        display_error_line(e)

    print("\nALl folders have been checked")
    print("Closing Validation Program")


def display_error_line(ex):
    trace = []
    tb = ex.__traceback__
    while tb is not None:
        trace.append({"filename": tb.tb_frame.f_code.co_filename,
                      "name": tb.tb_frame.f_code.co_name,
                      "lineno": tb.tb_lineno})
        tb = tb.tb_next
    print(str({'type': type(ex).__name__, 'message': str(ex), 'trace': trace}))


def create_sub_folders(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)


def populate_object(current_sub_object, Root_path, list_of_files, Support_Files):
    for iterF in list_of_files:
        file_path = Root_path + file_sep + iterF
        current_sub_object.get_data_tables(iterF, file_path)
        current_sub_object.column_validation(iterF, Support_Files)
    current_sub_object.get_submission_metadata(Support_Files)
    return current_sub_object


Data_Validation_Main()
