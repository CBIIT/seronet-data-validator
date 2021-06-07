# content of conftest.py
import pytest
from Validation_Rules import Validation_Rules
from Validation_Rules import check_ID_Cross_Sheet
from Validation_Rules import compare_tests

import os
import pandas
import re
import datetime

import File_Submission_Object
file_sep = os.path.sep
#############################################################################################


@pytest.fixture
def Good_Test_Data():
    root_dir = "C:\\Py_Testing_Folder\\Good_Submission"
    sub_obj = Create_Object(root_dir)
    sub_obj = Run_Validation(sub_obj)
    return sub_obj


@pytest.fixture
def Bad_Test_Data():
    root_dir = "C:\\Py_Testing_Folder\\Bad_Submission"
    sub_obj = Create_Object(root_dir)
    sub_obj = Run_Validation(sub_obj)
    return sub_obj


def Create_Object(file_path_dir):
    Support_Files = get_subfolder("C:\\Seronet_Data_Validation", "Support_Files")
    assay_folders = [i for i in Support_Files if os.path.isdir(i)]
    assay_data, assay_target = populate_assay_data(assay_folders)
    current_sub_object = File_Submission_Object.Submission_Object("Test_Submission")

    list_of_files = os.listdir(file_path_dir)
    current_sub_object = populate_object(current_sub_object, file_path_dir, list_of_files, Support_Files)
    current_sub_object.update_object(assay_data, "assay.csv")
    current_sub_object.update_object(assay_target, "assay_target.csv")
    return current_sub_object


def Run_Validation(current_sub_object):
    valid_cbc_ids = str(current_sub_object.CBC_ID)
    for file_name in current_sub_object.Data_Object_Table:
        if file_name not in ["submission.csv", "shipping_manifest.csv", "assay.csv", "assay_target.csv"]:
            if "Data_Table" in current_sub_object.Data_Object_Table[file_name]:
                data_table = current_sub_object.Data_Object_Table[file_name]['Data_Table']
                data_table, drop_list = current_sub_object.correct_var_types(file_name)
                current_sub_object = Validation_Rules(re, datetime, current_sub_object, data_table,
                                                      file_name, valid_cbc_ids, drop_list)
    check_ID_Cross_Sheet(current_sub_object, re)
    compare_tests(current_sub_object)
    return current_sub_object
#############################################################################################


def populate_object(current_sub_object, Currpath, list_of_files, Support_Files):
    for iterF in list_of_files:
        file_path = Currpath + file_sep + iterF
        current_sub_object.get_data_tables(iterF, file_path)
        current_sub_object.column_validation(iterF, Support_Files)
    current_sub_object.get_submission_metadata(Support_Files)
    return current_sub_object


def get_subfolder(root_dir, folder_name):
    file_path = root_dir + file_sep + folder_name
    file_dir = os.listdir(file_path)
    file_dir = [file_path + file_sep + i for i in file_dir]
    return file_dir


def get_assay_data(file_path, file_names, curr_file):
    if curr_file + ".xlsx" in file_names:
        curr_assay = pandas.read_excel(file_path + file_sep + curr_file +
                                   ".xlsx", na_filter=False, engine='openpyxl')
    elif curr_file + ".csv" in file_names:
        curr_assay = pandas.read_csv(file_path + file_sep + curr_file + ".csv", na_filter=False)
    return curr_assay


def populate_assay_data(assay_folders):
    assay_data = []
    assay_target = []
    for iterZ in assay_folders:
        file_names = os.listdir(iterZ)
        if len(file_names) == 0:
            continue
        if len(file_names) > 0:
            curr_assay = get_assay_data(iterZ, file_names, "assay")
            curr_target = get_assay_data(iterZ, file_names, "assay_target_antigen")
            if len(assay_data) == 0:
                assay_data = curr_assay
            else:
                assay_data = pandas.concat([assay_data, curr_assay])
            if len(assay_target) == 0:
                assay_target = curr_target
            else:
                assay_target = pandas.concat([assay_target, curr_target])
    if len(assay_data) > 0:
        assay_data.reset_index(inplace=True)
        assay_target.reset_index(inplace=True)
        if "Assay_Target_Antigen" in assay_target.columns:
            assay_target = assay_target.rename(columns={"Assay_Target_Antigen": "Assay_Target"})
        assay_data = clean_up_tables(assay_data)
        assay_target = clean_up_tables(assay_target)

    return assay_data, assay_target


def clean_up_tables(curr_table):
    curr_table.dropna(axis=0, how="all", thresh=None, subset=None, inplace=True)
    if len(curr_table) > 0:
        missing_logic = curr_table.eq(curr_table.iloc[:, 0], axis=0).all(axis=1)
        curr_table = curr_table[[i is not True for i in missing_logic]]
        curr_table = curr_table.loc[:, ~curr_table .columns.str.startswith('Unnamed')]

        for iterC in curr_table.columns:
            try:
                curr_table[iterC] = curr_table[iterC].apply(lambda x: x.replace('–', '-'))
            except Exception:
                pass
    return curr_table
