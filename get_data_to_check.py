# -*- coding: utf-8 -*-


def get_summary_file(os, pd, root_dir, file_sep, s3_client, summary_path):
    summary_file = []
    if os.path.exists(summary_path):
        xls = pd.ExcelFile(summary_path, engine='openpyxl')
        for iterZ in xls.sheet_names:
            if len(summary_file) == 0:
                summary_file = pd.read_excel(summary_path, sheet_name=iterZ, engine='openpyxl')
            else:
                new_file = pd.read_excel(summary_path, sheet_name=iterZ, engine='openpyxl')
                summary_file = pd.concat([summary_file, new_file])
        summary_file.reset_index(inplace=True)
        summary_file.drop(["index"], inplace=True, axis=1)
    else:
        summary_file = pd.DataFrame(columns=["Submission_Status", "Date_Of_Last_Status", "Folder_Location",
                                             "CBC_Num", "Date_Timestamp", "Submission_Name",
                                             "Validation_Status", "JIRA_Ticket", "Ticket_Status"])

    get_submissions_to_check(os, s3_client, pd, summary_file, root_dir)
    return summary_file


def get_submissions_to_check(os, s3_client, pd, summary_file, root_dir):
    bucket_name = "nci-seronet-cbc-destination"
    done_list = summary_file["Date_Timestamp"].tolist()
    test_list = ["09-13-36-06-22-2021", "14-58-48-03-26-2021", "16-13-43-04-01-2021", "10-03-08-06-11-2021",
                 "15-43-32-06-10-2021", "12-37-48-04-12-2021", "15-54-47-05-06-2021", "12-11-00-06-11-2021",
                 "15-45-38-05-06-2021", "15-48-31-05-06-2021", "15-51-30-05-06-2021"]

    folders_to_process = get_s3_folders(s3_client, pd, bucket_name, done_list, prefix='', suffix='.zip')
    folder_list = folders_to_process[folders_to_process[1].apply(lambda x: x not in done_list and x not in test_list)]
    folder_list.columns = ["CBC_Name", "Date", "Sub_Name", "File_Name"]

    curr_dir = os.getcwd()
    for index in folder_list.values:
        resp = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=(index[0] + "/" + index[1]))
        for curr_file in resp["Contents"]:
            os.chdir(root_dir + os.path.sep + "Files_To_Validate")
            path, name = os.path.split(curr_file["Key"])
            create_sub_folders(os, path)
            os.chdir(root_dir + os.path.sep + "Files_To_Validate" + os.path.sep + path)
            s3_client.download_file(bucket_name, curr_file["Key"], name)
    os.chdir(curr_dir)


def get_s3_folders(s3, pd, bucket, done_list, prefix, suffix):
    key_list = []
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in resp['Contents']:
        key = obj['Key']
        if key.endswith(suffix):
            key_list.append(key)
    new_list = [i.split("/") for i in key_list]
    return pd.DataFrame(new_list)


def create_sub_folders(os, folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name, mode=0o666)
