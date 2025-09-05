import ast
import json
import pandas as pd
from typing import *
import re
from loguru import logger
import polars as pl
from .extract_comment_from_variable import get_the_comment_from_program, get_the_comment_from_function
from .ladder_utils import regex_pattern_check, clean_rung_number


# ============================ Rule 62: Definitions, Content, and Configuration Details ============================

condition_section_name = 'condition'
hmi_out_section_name = 'hmi_out'

rule_62_check_item = "Rule of Home Position"

home_comment = "原"
pos_comment = "位置"
auto_comment = "自動"   
start_comment = "起動"
condition_comment = "条件"
end_comment = "端"

check_detail_content = {"cc1":"If ① exists but ③ does not, it is NG.",
                        "cc2":"Check that “all contacts included in the outcoil condition detected in ③” satisfy the following. If not, NG.",
                        "cc3":"Check that the A contact of the same variable as the outcoil detected in step ③ is used in one or more locations within the same section as when it was detected. If not, it is NG.",
                        "cc4":"If ④ exists but ⑥ does not, it is NG.",
                        "cc5":"Check that only one normally open A contact containing the variable comment “原(home)” + “位置(pos)” is connected to the outcoil condition detected in step ⑥.",
                        "cc6":" Find one outcoil with the same variable as the A contact detected in ❺ within the same task. Then check that only one A contact connected to it has a variable comment containing “原(home)” + “位置(pos)”.",
                        "cc7":" Find one outcoil with the same variable as the A contact detected in ❻ within the same task. Then, for all A contacts existing under that condition, perform the following checks (❼.1~❼.2) to verify that one of the outcoils detected in ③ is connected. *However, if the variable name is GSB000, no check is required.",
                        }

ng_content =  {"cc1":"各タスクの起動条件に原位置の回路が標準通りに作成されていないためNG",
               "cc2" : "各タスクの原位置の条件に端確認や位置信号以外の接点が含まれているためNG",
               "cc3":"原位置信号が起動条件に含まれていないためNG",
               "cc4":"Mainタスクにて自動起動条件に原位置信号のコイルがないためNG",
               "cc5":"Mainタスクにて自動起動条件に原位置信号が含まれていないためNG",
               "cc6":"Mainタスクにて自動起動条件に原位置信号の条件がないためNG",
               "cc7":"各タスクの原位置信号がMainタスクに接続されていないためNG"}

def is_allowed_task(name: str) -> bool:
    """
    Returns True if task name is allowed,
    False if it starts with P000-P101 (case-insensitive).
    """
    pattern = re.compile(r"^P(\d+)", re.IGNORECASE)
    match = pattern.match(name)
    if match:
        num = int(match.group(1))
        return not (0 <= num <= 101)  # False if in range
    return True  # No match means allowed


# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================

def detection_range_programwise(condition_section_df:pd.DataFrame, program:str, program_comment_data:dict, home_comment:str, pos_comment:str):

    # print("condition_section_df",condition_section_df)
    coil_df = condition_section_df[condition_section_df['OBJECT_TYPE_LIST'].str.lower() == 'coil']
    # print("coil_df",coil_df)
    match_outcoil = []
    if not coil_df.empty:
        for _, coil_row in coil_df.iterrows():
            attr = ast.literal_eval(coil_row['ATTRIBUTES'])
            coil_operand = attr.get('operand')
            if isinstance(coil_operand, str) and coil_operand:
                coil_comment = get_the_comment_from_program(coil_operand, program, program_comment_data)
                # print("coil_comment", coil_comment, coil_operand)
                if isinstance(coil_comment, list) and coil_comment:
                    if regex_pattern_check(home_comment, coil_comment) and regex_pattern_check(pos_comment, coil_comment):
                        match_outcoil.append([coil_operand, coil_row['RUNG']])
                        break
    
    return match_outcoil

def check_detail_1_programwise(detection_result:dict, program:str):
     
    status = "NG"
    rung_number = -1
    target_coil = ""
    if detection_result:
        status = "OK"
        rung_number = detection_result[0][1]
        target_coil = detection_result[0][0]
    
    return {
        "status" : status,
        "cc" : "cc1",
        "rung_number" : rung_number,
        "target_coil" : target_coil,
        "check_number": 1
    }


def check_detail_2_programwise(condition_section_df:pd.DataFrame, detection_result:dict, program:str, program_comment_data:str, end_comment:str, pos_comment:str):

    status = "NG"
    match_rung_number = -1
    target_coil = ""

    if detection_result:
        match_rung_number = detection_result[0][1]
        target_coil = detection_result[0][0]
        if match_rung_number!=-1:
            match_rung_df = condition_section_df[condition_section_df['RUNG'] == match_rung_number]
            if not match_rung_df.empty:
                contact_df = match_rung_df[match_rung_df['OBJECT_TYPE_LIST'].str.lower() == 'contact']
                if not contact_df.empty:
                    for _, contact_row in contact_df.iterrows():
                        attr = ast.literal_eval(contact_row['ATTRIBUTES'])
                        contact_operand = attr.get('operand')
                        negated_operand = attr.get('negated')

                        if isinstance(contact_operand, str) and contact_operand and negated_operand == 'false':
                            contact_comment = get_the_comment_from_program(contact_operand, program, program_comment_data)
                            if isinstance(contact_comment, list) and contact_comment:
                                if regex_pattern_check(end_comment, contact_comment) or regex_pattern_check(pos_comment, contact_comment) or regex_pattern_check("POS", contact_comment):
                                    status = "OK"
                                else:
                                    status = "NG"
                                    break
    return {
        "status" : status,
        "cc" : "cc2",
        "rung_number" : match_rung_number,
        "target_coil" : target_coil,
        "check_number": 2
    }


def check_detail_3_programwise(condition_section_df:pd.DataFrame, detection_result:dict):
    
    status = "NG"
    rung_number = -1
    target_coil = ""

    if detection_result:

        detected_3_outcoil = detection_result[0][0]
        rung_number = detection_result[0][1]
        target_coil = detection_result[0][0]

        contact_df = condition_section_df[condition_section_df['OBJECT_TYPE_LIST'].str.lower() == 'contact']
        for _, contact_row in contact_df.iterrows():
            attr = ast.literal_eval(contact_row['ATTRIBUTES'])
            contact_operand = attr.get('operand')
            negated_operand = attr.get('negated')

            if isinstance(contact_operand, str) and contact_operand and negated_operand == 'false':
                if contact_operand == detected_3_outcoil:
                    status = "OK"
                    break
    
    return {
        "status" : status,
        "cc" : "cc3",
        "rung_number" : rung_number,
        "target_coil" : target_coil,
        "check_number": 3
    }

def check_detail_4_programwise(hmi_out_main_program_df: pd.DataFrame, auto_comment:str, start_comment:str, condition_comment:str, home_comment:str, pos_comment:str, program:str, program_comment_data:dict):
    status = "NG"
    rung_number = -1
    target_coil = ""

    if not hmi_out_main_program_df.empty:
        coil_df = hmi_out_main_program_df[hmi_out_main_program_df['OBJECT_TYPE_LIST'].str.lower() == 'coil']
        if not coil_df.empty:
            for _, coil_row in coil_df.iterrows():
                attr = ast.literal_eval(coil_row['ATTRIBUTES'])
                coil_operand = attr.get('operand')
                if isinstance(coil_operand, str) and coil_operand:
                    coil_comment = get_the_comment_from_program(coil_operand, program, program_comment_data)
                    # print("coil_comment", coil_comment, coil_operand)
                    if isinstance(coil_comment, list) and coil_comment:
                        if regex_pattern_check(auto_comment, coil_comment) and regex_pattern_check(start_comment, coil_comment) and regex_pattern_check(condition_comment, coil_comment) and regex_pattern_check(home_comment, coil_comment) and regex_pattern_check(pos_comment, coil_comment):
                            status = "OK"
                            rung_number = coil_row['RUNG']
                            target_coil = coil_operand
                            break

    return {
        "status" : status,
        "cc" : "cc4",
        "rung_number" : rung_number,
        "target_coil" : target_coil,
        "check_number": 4
    }                           

 
def check_detail_5_programwise(hmi_out_main_program_df:pd.DataFrame, cc4_result:dict):

    status = "NG"
    rung_number = cc4_result.get('rung_number')
    target_contact = ""

    if cc4_result and cc4_result.get('status') == "OK":
        rung_number = cc4_result.get('rung_number')
        if rung_number != -1:
            match_rung_df = hmi_out_main_program_df[hmi_out_main_program_df['RUNG'] == rung_number]
            if not match_rung_df.empty:
                contact_df = match_rung_df[match_rung_df['OBJECT_TYPE_LIST'].str.lower() == 'contact']
                if len(contact_df) == 1:
                    for _, contact_row in contact_df.iterrows():
                        attr = ast.literal_eval(contact_row['ATTRIBUTES'])
                        contact_operand = attr.get('operand')
                        negated_operand = attr.get('negated')
                        if isinstance(contact_operand, str) and contact_operand and negated_operand == 'false':
                            status = "OK"
                            target_contact = contact_operand
                            rung_number = contact_row['RUNG']
                            break
    return {
        "status" : status,
        "cc" : "cc5",
        "rung_number" : rung_number,
        "target_coil" : target_contact,
        "check_number": 5
    }


# rung_number = cc4_result.get('rung_number')
# if rung_number != -1:
#     match_rung_df = hmi_out_main_program_df[hmi_out_main_program_df['RUNG'] == rung_number]
#     if not match_rung_df.empty:
#         contact_df = match_rung_df[match_rung_df['BODY'].str.lower() == 'contact']
#         if len(contact_df) == 1:
#             for _, contact_row in contact_df.iterrows():
#                 attr = ast.literal_eval(contact_row['ATTRIBUTES'])
#                 contact_operand = attr.get('operand')
#                 negated_operand = attr.get('negated')
#                 if isinstance(contact_operand, str) and contact_operand and negated_operand == 'false':
#                     status = "OK"
#                     target_contact = contact_operand
#                     rung_number = contact_row['RUNG']
#                     break


def check_detail_6_programwise(main_program_df:pd.DataFrame, cc5_result:dict, program:str, program_comment_data:dict):

    status = "NG"
    target_contact = ""
    rung_number = -1

    # print("main_program_df",main_program_df)

    if cc5_result and cc5_result.get('status') == "OK":
        target_contact = cc5_result.get('target_coil')
        if target_contact:
            coil_df = main_program_df[main_program_df['OBJECT_TYPE_LIST'].str.lower() == 'coil']
            if not coil_df.empty:
                for _, coil_row in coil_df.iterrows():
                    coil_attr = ast.literal_eval(coil_row['ATTRIBUTES'])
                    coil_operand = coil_attr.get('operand')
                    # print("coil_operand",coil_row['BODY'], coil_operand, target_contact)
                    if isinstance(coil_operand, str) and coil_operand and coil_operand == target_contact:
                        rung_number = coil_row['RUNG']
                        match_rung_df = main_program_df[(main_program_df['BODY'] == coil_row['BODY']) &
                                                        (main_program_df['RUNG'] == rung_number)]
                        if not match_rung_df.empty:
                            contact_df = match_rung_df[match_rung_df['OBJECT_TYPE_LIST'].str.lower() == 'contact']
                            if len(contact_df) == 1:
                                for _, contact_row in contact_df.iterrows():
                                    contact_attr = ast.literal_eval(contact_row['ATTRIBUTES'])
                                    contact_operand = contact_attr.get('operand')
                                    negated_operand = contact_attr.get('negated')
                                    if isinstance(contact_operand, str) and contact_operand and negated_operand == 'false':
                                        contact_comment = get_the_comment_from_program(contact_operand, coil_row['PROGRAM'], program_comment_data)
                                        if isinstance(contact_comment, list) and contact_comment:
                                            if regex_pattern_check(home_comment, contact_comment) and regex_pattern_check(pos_comment, contact_comment):
                                                status = "OK"
                                                target_contact = contact_operand
                                                rung_number = contact_row['RUNG']
                                                break

                            if status == "OK":
                                break

        
    return {
        "status" : status,
        "cc" : "cc6",
        "rung_number" : rung_number,
        "target_coil" : target_contact,
        "check_number": 6
    }

def check_detail_7_programwise(program_df:pd.DataFrame, main_program_df:pd.DataFrame, detection_result:dict, cc6_result:dict, program:str, program_comment_data:dict):
    
    status = "NG"
    final_rung_number = -1
    final_target_contact = ""

    if cc6_result and cc6_result.get('status') == "OK":
        target_contact = cc6_result.get('target_coil')

        print("target_contact", target_contact)
        if target_contact:
            coil_df = main_program_df[main_program_df['OBJECT_TYPE_LIST'].str.lower() == 'coil']
            if not coil_df.empty:
                for _, coil_row in coil_df.iterrows():
                    coil_attr = ast.literal_eval(coil_row['ATTRIBUTES'])
                    coil_operand = coil_attr.get('operand')
                    if isinstance(coil_operand, str) and coil_operand and coil_operand == target_contact:
                        rung_number = coil_row['RUNG']
                        match_rung_df = main_program_df[(main_program_df['BODY'] == coil_row['BODY']) &
                                                        (main_program_df['RUNG'] == rung_number)]
                        if not match_rung_df.empty:
                            contact_df = match_rung_df[match_rung_df['OBJECT_TYPE_LIST'].str.lower() == 'contact']
                            current_section_df = main_program_df[main_program_df['BODY'].str.lower() == coil_row['BODY'].lower()]
                            match_coil_df = current_section_df[current_section_df['OBJECT_TYPE_LIST'].str.lower() == 'coil']


                            """
                            this code is for getting all contact which are in the same rung where coil is detected in step 6
                            """
                            detection_7_1_operand = ""
                            all_match_contact = []
                            for _, contact_row in contact_df.iterrows():
                                contact_attr = ast.literal_eval(contact_row['ATTRIBUTES'])
                                contact_operand = contact_attr.get('operand')
                                negated_operand = contact_attr.get('negated')
                                if isinstance(contact_operand, str) and contact_operand and negated_operand == 'false':
                                    all_match_contact.append(contact_operand)
                        

                            """
                            this code is for getting contact which is connected to the coil detected in step 6
                            """

                            print("all_match_contact", all_match_contact)
                            get_final_match_contact = []
                            if all_match_contact:
                                for _, match_coil_row in match_coil_df.iterrows():
                                    match_coil_attr = ast.literal_eval(match_coil_row['ATTRIBUTES'])
                                    match_coil_operand = match_coil_attr.get('operand')
                                    if isinstance(match_coil_operand, str) and match_coil_operand and match_coil_operand in all_match_contact:
                                        current_match_contact_df = main_program_df[(main_program_df['BODY'] == coil_row['BODY']) &
                                                                                    (main_program_df['RUNG'] == match_coil_row['RUNG'])]
                                        for _, current_contact_row in current_match_contact_df.iterrows():
                                            current_contact_attr = ast.literal_eval(current_contact_row['ATTRIBUTES'])
                                            current_contact_operand = current_contact_attr.get('operand')
                                            negated_operand = current_contact_attr.get('negated')
                                            if isinstance(current_contact_operand, str) and current_contact_operand and negated_operand == 'false':
                                                if current_contact_operand.lower() != "gsb000":
                                                    get_final_match_contact.append(current_contact_operand)
                            

                            """ this is for checking 7.2 condition"""

                            print("get_final_match_contact", get_final_match_contact)
                            if get_final_match_contact and detection_result:
                                detected_3_outcoil = detection_result[0][0]
                                coil_df = program_df[program_df['OBJECT_TYPE_LIST'].str.lower() == 'coil']
                                for _, coil_row in coil_df.iterrows():
                                    coil_attr = ast.literal_eval(coil_row['ATTRIBUTES'])
                                    coil_operand = coil_attr.get('operand')
                                    if isinstance(coil_operand, str) and coil_operand and coil_operand in get_final_match_contact:
                                        rung_number = coil_row['RUNG']
                                        match_section_rung_df = program_df[(program_df['PROGRAM'] == coil_row['PROGRAM']) & 
                                                                           (program_df['BODY'] == coil_row['BODY']) & 
                                                                            (program_df['RUNG'] == rung_number)]
                                        
                                        match_contact_df = match_section_rung_df[match_section_rung_df['OBJECT_TYPE_LIST'].str.lower() == 'contact']
                                        if len(match_contact_df) == 1:
                                            for _, match_contact_row in match_contact_df.iterrows():
                                                match_contact_attr = ast.literal_eval(match_contact_row['ATTRIBUTES'])
                                                match_contact_operand = match_contact_attr.get('operand')
                                                if isinstance(match_contact_operand, str) and match_contact_operand and match_contact_operand == detected_3_outcoil:
                                                    status = "OK"
                                                    final_rung_number = match_contact_row['RUNG']
                                                    final_target_contact = match_contact_operand
                                                    break
                                    
                                            if status == "OK":
                                                break

    return {
        "status" : status,
        "cc" : "cc7",
        "rung_number" : final_rung_number,
        "target_coil" : final_target_contact,
        "check_number": 7
    }                                    





# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_62_programwise(input_program_file:str, 
                                program_comment_file:str) -> pd.DataFrame:

    logger.info("Rule 62 Start executing rule 1 program wise")

    output_rows = []
    
    program_df = pd.read_csv(input_program_file)
    with open(program_comment_file, 'r', encoding="utf-8") as file:
        program_comment_data = json.load(file)
    
    unique_program_values = program_df["PROGRAM"].unique()

    main_programs = [p for p in unique_program_values if "main" in str(p).lower()]

    main_program_df = program_df[program_df['PROGRAM'].isin(main_programs)]
    hmi_out_main_program_df = main_program_df[main_program_df['BODY'].str.lower() == hmi_out_section_name]

    # print("hmi_out_main_program_df",hmi_out_main_program_df)

    for program in unique_program_values:

        logger.info(f"Executing Rule 62 in program {program}")

        is_task_name_in_range = is_allowed_task(program)

        # if is_task_name_in_range:
        if program == 'P112_sample':
            
            current_program_df = program_df[program_df['PROGRAM'] == program]
            condition_section_df = current_program_df[current_program_df['BODY'].str.lower() == condition_section_name]

            detection_result = detection_range_programwise(condition_section_df=condition_section_df,
                                        program=program,
                                        program_comment_data=program_comment_data,
                                        home_comment=home_comment,
                                        pos_comment=pos_comment)
            
            cc1_result = check_detail_1_programwise(detection_result=detection_result, 
                                                    program=program)
            
            cc2_result = check_detail_2_programwise(condition_section_df=condition_section_df,
                                                    detection_result=detection_result, 
                                                    program=program,
                                                    program_comment_data=program_comment_data,
                                                    end_comment=end_comment,
                                                    pos_comment=pos_comment)
            
            cc3_result = check_detail_3_programwise(condition_section_df=condition_section_df,
                                                    detection_result=detection_result)     

            cc4_result = check_detail_4_programwise(hmi_out_main_program_df=hmi_out_main_program_df,
                                                    auto_comment=auto_comment,
                                                    start_comment=start_comment,
                                                    condition_comment=condition_comment,
                                                    home_comment=home_comment,
                                                    pos_comment=pos_comment,
                                                    program=program,
                                                    program_comment_data=program_comment_data)     

            cc5_result = check_detail_5_programwise(hmi_out_main_program_df=hmi_out_main_program_df,
                                                    cc4_result=cc4_result,
                                                    )  

            cc6_result = check_detail_6_programwise(main_program_df=main_program_df,
                                                    cc5_result=cc5_result,
                                                    program=program,
                                                    program_comment_data=program_comment_data)      

            cc7_result = check_detail_7_programwise(program_df=program_df,
                                                    main_program_df=main_program_df,
                                                    detection_result=detection_result,
                                                    cc6_result=cc6_result,
                                                    program=program,
                                                    program_comment_data=program_comment_data)         

            # print(detection_result)
            print(cc1_result)
            print(cc2_result)
            print(cc3_result)
            print(cc4_result)
            print(cc5_result)
            print(cc6_result)
            print(cc7_result)
