def test_submission_file_count(Bad_Test_Data):
    """ py test to ensure that all 9 files are loaded in the dictionary
        plus the 2 assay data files from Support Folder"""
    dict_size = len(Bad_Test_Data.Data_Object_Table)
    assert dict_size == (9+2)


def test_col_errors(Bad_Test_Data):
    """ py test to ensure that there are 0 column errors in data"""
    exp_col_err_count = 0   # number of column errors expected to find
    found_col_err_count = len(Bad_Test_Data.Column_error_count)
    assert found_col_err_count == exp_col_err_count


def test_demographic(Bad_Test_Data):
    res = make_col_dict(Bad_Test_Data, "demographic.csv")
    check_if_exist(res, "demographic.csv")
    Exp_Error_Dict = {"Research_Participant_ID": 0}
    check_dict(res, Exp_Error_Dict)


def test_prior_clinical(Bad_Test_Data):
    res = make_col_dict(Bad_Test_Data, "prior_clinical_test.csv")
    check_if_exist(res, "prior_clinical_test.csv")
    Exp_Error_Dict = {"Research_Participant_ID": 0}
    check_dict(res, Exp_Error_Dict)


def test_Biospecimen(Bad_Test_Data):
    res = make_col_dict(Bad_Test_Data, "biospecimen.csv")
    check_if_exist(res, "biospecimen.csv")
    Exp_Error_Dict = {"Research_Participant_ID": 0}
    check_dict(res, Exp_Error_Dict)


def test_aliquot(Bad_Test_Data):
    res = make_col_dict(Bad_Test_Data, "aliquot.csv")
    check_if_exist(res, "aliquot.csv")
    Exp_Error_Dict = {"Biospecimen_ID": 6}
    check_dict(res, Exp_Error_Dict)


def test_Confirmatory_clinical_test(Bad_Test_Data):
    res = make_col_dict(Bad_Test_Data, "confirmatory_clinical_test.csv")
    check_if_exist(res, "confirmatory_clinical_test.csv")
    Exp_Error_Dict = {"Research_Participant_ID": 0}
    check_dict(res, Exp_Error_Dict)


def test_Equipment(Bad_Test_Data):
    res = make_col_dict(Bad_Test_Data, "equipment.csv")
    check_if_exist(res, "equipment.csv")
    Exp_Error_Dict = {"Biospecimen_ID": 3, "Equipment_Type": 4}
    check_dict(res, Exp_Error_Dict)


def test_Reagents(Bad_Test_Data):
    res = make_col_dict(Bad_Test_Data, "reagent.csv")
    check_if_exist(res, "reagent.csv")
    Exp_Error_Dict = {"Biospecimen_ID": 0}
    check_dict(res, Exp_Error_Dict)


def test_Consumables(Bad_Test_Data):
    res = make_col_dict(Bad_Test_Data, "consumable.csv")
    check_if_exist(res, "consumable.csv")
    Exp_Error_Dict = {"Biospecimen_ID": 0}
    check_dict(res, Exp_Error_Dict)


def test_Cross_Sheet_Participant_IDs(Bad_Test_Data):
    res = make_col_dict(Bad_Test_Data, "Cross_Participant_ID.csv")
    check_if_exist(res, "Cross_Participant_ID.csv")
    Exp_Error_Dict = {"Research_Participant_ID": 0}
    check_dict(res, Exp_Error_Dict)
    

def test_Cross_Sheet_Bio_IDs(Bad_Test_Data):
    res = make_col_dict(Bad_Test_Data, "Cross_Biospecimen_ID.csv")
    check_if_exist(res, "Cross_Biospecimen_ID.csv")
    Exp_Error_Dict = {"Biospecimen_ID": 0}
    check_dict(res, Exp_Error_Dict)


def test_Prior_Vs_Clinical_Tests(Bad_Test_Data):
    res = make_col_dict(Bad_Test_Data, "Prior_Vs_Confirm_Test.csv")
    check_if_exist(res,"Prior_Vs_Confirm_Test.csv")
    Exp_Error_Dict = {"Research_Participant_ID": 0}
    check_dict(res, Exp_Error_Dict)


#######################################################################################
def check_if_exist(res,file_name):
    if len(res) == 0:
        assert False, file_name + " was not found, test not run"


def make_col_dict(Bad_Test_Data, sheet_name):
    if sheet_name not in Bad_Test_Data.Data_Object_Table:
        res = []
    else:
        data_table = Bad_Test_Data.Data_Object_Table[sheet_name]["Data_Table"]
        data_table = data_table.query("Message_Type == 'Error'")
        col_names = data_table.columns
        col_err_count = [Bad_Test_Data.Error_list["Column_Name"].tolist().count(i) for i in col_names]
        res = {col_names[i]: col_err_count[i] for i in range(len(col_names))}
    return res


def check_dict(res, Exp_Error_Dict):
    for curr_col in res:
        exp_error = 0
        if curr_col in Exp_Error_Dict:
            exp_error = Exp_Error_Dict[curr_col]
        assert res[curr_col] == exp_error, ("Column_Name: " + curr_col + " expected " + str(exp_error) + " Errors. " +
                                            str(res[curr_col]) + " errors where found")
