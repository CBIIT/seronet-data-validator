# -*- coding: utf-8 -*-
"""
Created on Thu Oct 14 16:47:28 2021
imports all utilities for the validation process
@author: breadsp2
"""
import re
import os

import datetime
import dateutil.tz
import pathlib
import shutil
from dateutil.parser import parse

import pandas as pd
import numpy as np
import icd10

import get_assay_data_from_box
import warnings

def set_up_function():
    warnings.simplefilter("ignore")
    
    file_sep = os.path.sep
    eastern = dateutil.tz.gettz("US/Eastern")
    validation_date = datetime.datetime.now(tz=eastern).strftime("%Y-%m-%d")
    pd.options.mode.chained_assignment = None  # default='warn'
    
#    box_dir = "C:" + file_sep + "Users" + file_sep + os.getlogin() + file_sep + "Box"
    
#    templates = get_box_data(box_dir, "CBC Data Submission Documents" + file_sep + "Data Submission Templates", file_sep)
#    cbc_codes = get_box_data(box_dir, "SeroNet DMS" + file_sep + "12 SeroNet Data Submitter Information", file_sep)

#    Support_Files = templates + cbc_codes
    Support_Files = get_box_data("C:\\Data_Validation_CBC", "Template_Folder", file_sep)
    return file_sep, Support_Files, validation_date

def get_box_data(box_dir, box_path, file_sep):
    file_path = []
    cur_path = box_dir + file_sep + box_path
    for r, d, f in os.walk(cur_path):  # r=root, d=directories, f = files
        for file in f:
            if (file.endswith(".xlsx")):
                file_path.append(os.path.join(r, file))
    return file_path
