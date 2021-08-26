import os
import pandas as pd
import re
import warnings
warnings.simplefilter("ignore")


def get_assay_data():
    file_sep = os.path.sep
    box_dir = "C:" + file_sep + "Users" + file_sep + os.getlogin() + file_sep + "Box"
    assay_dir = box_dir + file_sep + "CBC_Folders"
    assay_paths = []

    path_dir = pd.DataFrame(columns=["Dir_Path", "File_Name", "Date_Created", "Date_Modified"])
    for r, d, f in os.walk(assay_dir):  # r=root, d=directories, f = files
        for file in f:
            if (file.endswith(".xlsx")) and ("20210402" not in r):
                file_path = os.path.join(r, file)
                assay_paths.append(file_path)
                created = os.path.getctime(file_path)
                modified = os.path.getmtime(file_path)
                path_dir.loc[len(path_dir.index)] = [r, file, created, modified]

    all_assay_data = pd.DataFrame()
    all_target_data = pd.DataFrame()
    all_qc_data = pd.DataFrame()
    converion_file = pd.DataFrame()

    uni_path = list(set(path_dir["Dir_Path"]))
    for curr_path in uni_path:
        curr_folder = path_dir.query("Dir_Path == @curr_path")
        assay_file = curr_folder[curr_folder["File_Name"].apply(lambda x: 'assay' in x and "assay_qc" not in x
                                                                and "assay_target" not in x)]
        all_assay_data = populate_df(all_assay_data, assay_file)

        assay_file = curr_folder[curr_folder["File_Name"].apply(lambda x: "assay_qc" in x)]
        all_qc_data = populate_df(all_qc_data, assay_file)

        assay_file = curr_folder[curr_folder["File_Name"].apply(lambda x: "assay_target_antigen" in x)]
        all_target_data = populate_df(all_target_data, assay_file)

        assay_file = curr_folder[curr_folder["File_Name"].apply(lambda x: "Assay_Target_Organism_Conversion.xlsx" in x)]
        converion_file = populate_df(converion_file, assay_file)

    all_assay_data = clean_up_tables(all_assay_data, '[0-9]{2}[_]{1}[0-9]{3}$')
    all_target_data = clean_up_tables(all_target_data, '[0-9]{2}[_]{1}[0-9]{3}$')
    all_qc_data = clean_up_tables(all_qc_data, '[0-9]{2}[_]{1}[0-9]{3}$')

    return all_assay_data, all_target_data, all_qc_data, converion_file


def populate_df(curr_assay, assay_file):
    if len(assay_file):
        curr_data = assay_file[assay_file["Date_Modified"] == max(assay_file["Date_Modified"])]
        file_path = os.path.join(curr_data["Dir_Path"].tolist()[0], curr_data["File_Name"].tolist()[0])
        curr_data = pd.read_excel(file_path, na_filter=False, engine='openpyxl')
        curr_assay = pd.concat([curr_assay, curr_data])
    return curr_assay


def clean_up_tables(curr_table, ptrn_str):
    curr_table = curr_table[curr_table["Assay_ID"].apply(lambda x: re.compile(ptrn_str).match(str(x)) is not None)]
    curr_table = curr_table.dropna(axis=0, how="all", thresh=None, subset=None)
    if len(curr_table) > 0:
        missing_logic = curr_table.eq(curr_table.iloc[:, 0], axis=0).all(axis=1)
        curr_table = curr_table[[i is not True for i in missing_logic]]
        curr_table = curr_table.loc[:, ~curr_table .columns.str.startswith('Unnamed')]
        curr_table = curr_table.replace('–', '-')
        curr_table.columns = [i.replace("Assay_Target_Antigen", "Assay_Target") for i in curr_table.columns]
        curr_table.columns = [i.replace("lavage", "Lavage") for i in curr_table.columns]
    return curr_table
