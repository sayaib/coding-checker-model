import ast
import json
import pandas as pd
from typing import *
import re
from loguru import logger
import polars as pl
from .extract_comment_from_variable import get_the_comment_from_function, get_the_comment_from_program
from .rule_47_ladder_utils import check_self_holding, get_series_contacts
from .ladder_utils import regex_pattern_check, clean_rung_number

# ============================================ Comments referenced in Rule 25 processing ============================================
# memory feed Complete （記憶送り完了）/memory feed timing （記憶送りタイミング）/ memory shift timing （記憶シフトタイミング）/ memory shift Complete （記憶シフト完了）

fuse_full_width_comment = "ヒューズ"
fuse_half_width_comment = "ﾋｭｰｽﾞ"
normal_comment = "正常"
disconnection_comment = "断"
poweron_comment = "電源入り" 
delay_comment = "遅延"

# ============================ Rule 46: Definitions, Content, and Configuration Details ============================
rule_content_48 = "Display 「Fuse blown,」 buzzer output and emergency stop when fuse blown is detected."
rule_48_check_item = "Rule of Fuse Trip Detection Circuit"

check_detail_content = {"cc1":" If ① but not ③ or not ⑤, it is assumed to be NG.",
                        "cc2":" Checks that all of the out coil condition detected in ③ are connected in series (AND) only with the A contact that contains ”ヒューズ(fuse)"+"正常(normal)” in the variable comment. *NG if other contacts exist.", 
                        "cc3":"Check that contact A includes the variable comment of ”電源入り(power on)"+"遅延(delay)” in the out coil condition detected in ③.", 
                        "cc4":"Check that contact B includes the variable comment of ”ヒューズ(fuse)"+"正常(normal)”+でない(not) in the out coil condition detected in ③.", 
                        "cc5":" Check that the out coil is connected in series (AND) with only the contacts of ❷and❸ within the self-holding of the out coil detected by ③."}

ng_content = {"cc1":"Fault回路が標準通りに作られていない(異常回路の抜け・漏れ)可能性あり(Fault circuit may not be built to standard (abnormal circuit missing or leaking))", 
              "cc2":" ヒューズ正常コイルの条件に”ヒューズ正常”以外の接点が存在しているためNG(NG because a contact other than “fuse normal” exists in the fuse normal coil condition.)", 
              "cc3":" ヒューズ断ALの条件に”電源入り遅延”のA接点が存在しない(There is no A contact for “power-on delay” in the fuse disconnection AL conditions.)", 
              "cc4":"ヒューズ断ALの条件に”ヒューズ正常”のB接点が存在しない(There is no B contact for “fuse normal” in the fuse disconnection AL conditions.)", 
              "cc5":"ヒューズ断の異常回路が標準通りに構成されていない(Abnormal circuit for fuse disconnection not configured as per standard.)"}

devicein_section_name = "devicein"
fault_section_name = "fault"

# ============================ Helper Functions for Program-Wise Operations ============================
def  check_memory_feed_outcoil_from_program(row, program_name:str,  memory_feed_comment:str, timing_comment:str,  program_comment_data:dict):
    try:
        attr = ast.literal_eval(row['ATTRIBUTES'])
        if isinstance(attr, dict) and  'operand' in attr and 'latch' in attr:
            if attr.get('latch') == 'set' and isinstance(attr.get('operand'), str):
                comment = get_the_comment_from_program(attr.get('operand'), program_name, program_comment_data)
                if isinstance(comment, list):
                    if regex_pattern_check(memory_feed_comment, comment) and regex_pattern_check(timing_comment, comment):
                        return attr.get('operand')
    except Exception:
        return None
    return None


# ============================ Helper Functions for Function-Wise Operations ============================
def check_memory_feed_outcoil_from_function(row, function_name:str, memory_feed_comment:str, timing_comment:str, function_comment_data:dict):
    try:
        attr = ast.literal_eval(row['ATTRIBUTES'])
        if isinstance(attr, dict) and 'operand' in attr and 'latch' in attr:
            if attr.get('latch') == 'set' and isinstance(attr.get('operand'), str):
                comment = get_the_comment_from_function(attr.get('operand'), function_name, function_comment_data)
                if isinstance(comment, list):
                    if regex_pattern_check(memory_feed_comment, comment) and regex_pattern_check(timing_comment, comment):
                        return attr.get('operand')
    except Exception:
        return None
    return None


# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================

def detection_range_programwise(devicein_section_df:pd.DataFrame, fault_section_df: pd.DataFrame, fuse_full_width_comment:str, fuse_half_width_comment:str,  program_name:str, program_comment_data:dict) -> dict:
    
    logger.info(f"Executing rule 48 detection range on program {program_name}")
    
    devicein_coil_df = devicein_section_df[devicein_section_df['OBJECT_TYPE_LIST'] == 'Coil'].copy()
    fault_coil_df = fault_section_df[fault_section_df['OBJECT_TYPE_LIST'] == 'Coil'].copy()

    detection_3_operand = ''
    detection_5_operand = ''
    detection_3_operand_rung_number = -1
    detection_5_operand_rung_number = -1

    for _, devicein_coil_row in devicein_coil_df.iterrows():
        attr = ast.literal_eval(devicein_coil_row['ATTRIBUTES'])
        devicein_coil_operand = attr.get('operand')
        if devicein_coil_operand and isinstance(devicein_coil_operand, str):
            devicein_coil_comment = get_the_comment_from_program(devicein_coil_operand, program_name, program_comment_data)
        else:
            devicein_coil_comment = []

        if isinstance(devicein_coil_comment, list):
            if (regex_pattern_check(fuse_full_width_comment, devicein_coil_comment) or regex_pattern_check(fuse_half_width_comment, devicein_coil_comment)) and regex_pattern_check(normal_comment, devicein_coil_comment):
                detection_3_operand = devicein_coil_operand
                detection_3_operand_rung_number = devicein_coil_row['RUNG']
                break
                
    for _, fault_coil_row in fault_coil_df.iterrows():
        attr = ast.literal_eval(fault_coil_row['ATTRIBUTES'])
        fault_coil_operand = attr.get('operand')
        if 'AL' in fault_coil_operand:
            fault_coil_comment = get_the_comment_from_program(fault_coil_operand, program_name, program_comment_data)
            if isinstance(fault_coil_comment, list):
                if (regex_pattern_check(fuse_full_width_comment, fault_coil_comment) or regex_pattern_check(fuse_half_width_comment, fault_coil_comment)) and regex_pattern_check(disconnection_comment, fault_coil_comment):
                    detection_5_operand = fault_coil_operand
                    detection_5_operand_rung_number = fault_coil_row['RUNG']
                    break
                

    return {
        "detection_3_details" : [detection_3_operand, detection_3_operand_rung_number],
        "detection_5_details" : [detection_5_operand, detection_5_operand_rung_number]
    }


def check_detail_1_programwise(detection_3_details:dict, detection_5_details:dict):
    cc1_result = {}

    cc1_result['status'] = "OK" if (detection_3_details[1]!=-1 and detection_5_details[-1]!=-1) else "NG" 
    cc1_result['cc'] = "cc1"
    cc1_result['check_number'] = 1
    cc1_result['target_coil'] = ""
    cc1_result['rung_number'] = -1

    return cc1_result


def check_detail_2_programwise(devicein_section_df:pd.DataFrame, detection_3_details:List, fuse_full_width_comment:str, fuse_half_width_comment:str, program_name:str, program_comment_data:str) -> dict:

    logger.info(f"Executing rule no 48 check detail 1 in program {program_name}")

    status = "OK"
    cc1_result = {}

    rung_number = detection_3_details[1]
    not_fuse_norma_comment_for_NG = ''

    if rung_number != -1:
        detection_coil_rung_df = devicein_section_df[devicein_section_df['RUNG']==rung_number]
        contact_df = detection_coil_rung_df[detection_coil_rung_df['OBJECT_TYPE_LIST'] == 'Contact'].copy()

        for _, contact_row in contact_df.iterrows():
            attr = ast.literal_eval(contact_row['ATTRIBUTES'])
            contact_operand = attr.get('operand')
            negated_operand = attr.get('negated')
            if contact_operand and isinstance(contact_operand, str):
                contact_comment = get_the_comment_from_program(contact_operand, program_name, program_comment_data)
            else:
                contact_comment = []

            if isinstance(contact_comment, list):
                if not ((regex_pattern_check(fuse_full_width_comment, contact_comment) or regex_pattern_check(fuse_half_width_comment, contact_comment)) and regex_pattern_check(normal_comment, contact_comment) and negated_operand=='false'):
                    status = "NG"
                    rung_number = -1
                    not_fuse_norma_comment_for_NG = contact_operand
                    break

    if rung_number == -1:
        status = 'NG'
            

    cc1_result['status'] = status
    cc1_result['cc'] = "cc2"
    cc1_result['target_coil'] = not_fuse_norma_comment_for_NG
    cc1_result['check_number'] = 2
    cc1_result['rung_number'] = rung_number

    return cc1_result

def check_detail_3_programwise(fault_section_df:pd.DataFrame, detection_5_details:int, program_name:str, program_comment_data:str) -> dict:

    logger.info(f"Executing rule no 48 check detail 2 in program {program_name}")

    status = "NG"
    match_contact_operand = ''
    cc3_result = {}
    rung_number = detection_5_details[1]

    fault_detection_coil_rung_df = fault_section_df[fault_section_df['RUNG']==rung_number]
    contact_df = fault_detection_coil_rung_df[fault_detection_coil_rung_df['OBJECT_TYPE_LIST'] == 'Contact'].copy()

    if rung_number != -1:
        for _, contact_row in contact_df.iterrows():
            attr = ast.literal_eval(contact_row['ATTRIBUTES'])
            contact_operand = attr.get('operand')
            negated_operand = attr.get('negated')
            if contact_operand and isinstance(contact_operand, str):
                contact_comment = get_the_comment_from_program(contact_operand, program_name, program_comment_data)
            else:
                contact_comment = []

            if isinstance(contact_comment, list) and contact_comment:
                if regex_pattern_check(poweron_comment, contact_comment) and regex_pattern_check(delay_comment, contact_comment) and negated_operand=='false':
                    status = "OK"
                    match_contact_operand = contact_operand
                    break
        

    cc3_result['status'] = status
    cc3_result['cc'] = "cc3"
    cc3_result['target_coil'] = match_contact_operand
    cc3_result['check_number'] = 3
    cc3_result['rung_number'] = rung_number

    return cc3_result

def check_detail_4_programwise(fault_section_df:pd.DataFrame, detection_5_details:int, fuse_full_width_comment:str, fuse_half_width_comment:str, program_name:str, program_comment_data:str) -> dict:

    logger.info(f"Executing rule no 48 check detail 3 in program {program_name}")

    status = "NG"
    match_contact_operand = ''
    cc4_result = {}
    rung_number = detection_5_details[1]

    detection_coil_rung_df = fault_section_df[fault_section_df['RUNG']==rung_number]
    contact_df = detection_coil_rung_df[detection_coil_rung_df['OBJECT_TYPE_LIST'] == 'Contact'].copy()

    if rung_number != -1:
        for _, contact_row in contact_df.iterrows():
            attr = ast.literal_eval(contact_row['ATTRIBUTES'])
            contact_operand = attr.get('operand')
            negated_operand = attr.get('negated')
            if contact_operand and isinstance(contact_operand, str):
                contact_comment = get_the_comment_from_program(contact_operand, program_name, program_comment_data)
            else:
                contact_comment = []

            if isinstance(contact_comment, list):
                if (regex_pattern_check(fuse_full_width_comment, contact_comment) or regex_pattern_check(fuse_half_width_comment, contact_comment)) and regex_pattern_check(normal_comment, contact_comment) and negated_operand=='true':
                    status = "OK"
                    match_contact_operand = contact_operand
                    break
    

    cc4_result['status'] = status
    cc4_result['cc'] = "cc4"
    cc4_result['target_coil'] = match_contact_operand
    cc4_result['rung_number'] = rung_number
    cc4_result['check_number'] = 4
    # cc4_result['target_coil'] = "FUSE_GOOD"
    # cc4_result['rung_number'] = rung_number

    return cc4_result

def check_detail_5_programwise(fault_section_df:pd.DataFrame, detection_coil:str, cc3_contact:str, cc4_contact:str, rung_number:int, program_name:str) -> dict:

    logger.info(f"Executing rule no 48 check detail 4 in program {program_name}")

    status = "NG"
    match_rung_number = -1
    both_contact_operand_in_series = False
    cc5_result = {}

    if cc3_contact and cc4_contact:
        detection_coil_rung_df = fault_section_df[fault_section_df['RUNG']==rung_number]
        contact_df = detection_coil_rung_df[detection_coil_rung_df['OBJECT_TYPE_LIST'] == 'Contact'].copy()

        all_self_holding_coil = check_self_holding(detection_coil_rung_df)
        get_series_connect_data = get_series_contacts(pl.from_pandas(detection_coil_rung_df))

        series_contact_operands_only = [[item.get('operand') for item in sublist] for sublist in get_series_connect_data]
        for series_contact in series_contact_operands_only:
            if cc3_contact in series_contact and cc4_contact in series_contact:
                both_contact_operand_in_series = True
                break

        """
        this is function is for checking if both contact should be in under self holding
        logic is if both contact outcoil is less than self holding outcoil then it is under slef holding
        """
        both_contact_outlist = []
        self_holding_outlist = []
        both_contact_inside_self_holding = True
        for _, contact_row in contact_df.iterrows():
            attr = ast.literal_eval(contact_row['ATTRIBUTES'])
            contact_operand = attr.get('operand')
            if contact_operand == cc3_contact or contact_operand == cc4_contact:
                both_contact_outlist.append(attr.get('out_list'))
            if contact_operand == detection_coil:
                self_holding_outlist.append(attr.get('out_list'))
        
        for both_contact_out_val in both_contact_outlist:
            if any(both_contact_out_val > x for x in self_holding_outlist):
                both_contact_inside_self_holding =False
                break
        if detection_coil in all_self_holding_coil and both_contact_operand_in_series and len(contact_df)==3 and both_contact_inside_self_holding:
            status = "OK"
            match_rung_number = rung_number
        else:
            status = "NG"
            match_rung_number = -1

    cc5_result['status'] = status
    cc5_result['cc'] = "cc5"
    cc5_result['target_coil'] = ""
    cc5_result['check_number'] = 5
    cc5_result['rung_number'] = match_rung_number

    return cc5_result


# ============================== Function-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================

def detection_range_functionwise(devicein_section_df:pd.DataFrame, fault_section_df: pd.DataFrame, fuse_full_width_comment:str, fuse_half_width_comment:str,  function_name:str, function_comment_data:dict) -> dict:
    
    logger.info(f"Executing rule 48 detection range on function {function_name}")
    
    devicein_coil_df = devicein_section_df[devicein_section_df['OBJECT_TYPE_LIST'] == 'Coil'].copy()
    fault_coil_df = fault_section_df[fault_section_df['OBJECT_TYPE_LIST'] == 'Coil'].copy()

    detection_3_operand = ''
    detection_5_operand = ''
    detection_3_operand_rung_number = -1
    detection_5_operand_rung_number = -1

    for _, devicein_coil_row in devicein_coil_df.iterrows():
        attr = ast.literal_eval(devicein_coil_row['ATTRIBUTES'])
        devicein_coil_operand = attr.get('operand')
        if devicein_coil_operand and isinstance(devicein_coil_operand,str):
            devicein_coil_comment = get_the_comment_from_function(devicein_coil_operand, function_name, function_comment_data)

        if isinstance(devicein_coil_comment, list):
            if (regex_pattern_check(fuse_full_width_comment, devicein_coil_comment) or regex_pattern_check(fuse_half_width_comment, devicein_coil_comment)) and regex_pattern_check(normal_comment, devicein_coil_comment):
                detection_3_operand = devicein_coil_operand
                detection_3_operand_rung_number = devicein_coil_row['RUNG']
                break
                
    for _, fault_coil_row in fault_coil_df.iterrows():
        attr = ast.literal_eval(fault_coil_row['ATTRIBUTES'])
        fault_coil_operand = attr.get('operand')
        if 'AL' in fault_coil_operand:
            fault_coil_comment = get_the_comment_from_function(fault_coil_operand, function_name, function_comment_data)
            if isinstance(fault_coil_comment, list):
                if (regex_pattern_check(fuse_full_width_comment, fault_coil_comment) or regex_pattern_check(fuse_half_width_comment, fault_coil_comment)) and regex_pattern_check(disconnection_comment, fault_coil_comment):
                    detection_5_operand = fault_coil_operand
                    detection_5_operand_rung_number = fault_coil_row['RUNG']
                    break
                

    return {
        "detection_3_details" : [detection_3_operand, detection_3_operand_rung_number],
        "detection_5_details" : [detection_5_operand, detection_5_operand_rung_number]
    }


def check_detail_1_functionwise(detection_3_details:dict, detection_5_details:dict):
    cc1_result = {}

    cc1_result['status'] = "OK" if (detection_3_details[0]!=-1 and detection_5_details[0]!=-1) else "NG" 
    cc1_result['cc'] = "cc1"
    cc1_result['check_number'] = 1
    cc1_result['target_coil'] = ""
    cc1_result['rung_number'] = -1

    return cc1_result


def check_detail_2_functionwise(devicein_section_df:pd.DataFrame, detection_3_details:List, fuse_full_width_comment:str, fuse_half_width_comment:str, function_name:str, function_comment_data:str) -> dict:

    logger.info(f"Executing rule no 48 check detail 1 in function {function_name}")

    status = "OK"
    cc1_result = {}

    rung_number = detection_3_details[1]
    not_fuse_norma_comment_for_NG = ''

    if rung_number != -1:
        detection_coil_rung_df = devicein_section_df[devicein_section_df['RUNG']==rung_number]
        contact_df = detection_coil_rung_df[detection_coil_rung_df['OBJECT_TYPE_LIST'] == 'Contact'].copy()

        for _, contact_row in contact_df.iterrows():
            attr = ast.literal_eval(contact_row['ATTRIBUTES'])
            contact_operand = attr.get('operand')
            negated_operand = attr.get('negated')
            if contact_operand and isinstance(contact_operand, str):
                contact_comment = get_the_comment_from_function(contact_operand, function_name, function_comment_data)
            else:
                contact_comment = []

            if isinstance(contact_comment, list):
                if not (regex_pattern_check(fuse_full_width_comment, contact_comment) or regex_pattern_check(fuse_half_width_comment, contact_comment)) and regex_pattern_check(normal_comment, contact_comment and negated_operand=='false'):
                    status = "NG"
                    rung_number = -1
                    not_fuse_norma_comment_for_NG = contact_operand
                    break
            

    cc1_result['status'] = status
    cc1_result['cc'] = "cc2"
    cc1_result['target_coil'] = not_fuse_norma_comment_for_NG
    cc1_result['check_number'] = 2
    cc1_result['rung_number'] = rung_number

    return cc1_result

def check_detail_3_functionwise(fault_section_df:pd.DataFrame, detection_5_details:int, function_name:str, function_comment_data:str) -> dict:

    logger.info(f"Executing rule no 48 check detail 2 in function {function_name}")

    status = "NG"
    match_contact_operand = ''
    cc3_result = {}
    rung_number = detection_5_details[1]

    fault_detection_coil_rung_df = fault_section_df[fault_section_df['RUNG']==rung_number]
    contact_df = fault_detection_coil_rung_df[fault_detection_coil_rung_df['OBJECT_TYPE_LIST'] == 'Contact'].copy()

    if rung_number != -1:
        for _, contact_row in contact_df.iterrows():
            attr = ast.literal_eval(contact_row['ATTRIBUTES'])
            contact_operand = attr.get('operand')
            negated_operand = attr.get('negated')

            if contact_operand and isinstance(contact_operand, str):
                contact_comment = get_the_comment_from_function(contact_operand, function_name, function_comment_data)
            else:
                contact_comment = []

            if isinstance(contact_comment, list):
                if regex_pattern_check(poweron_comment, contact_comment) and regex_pattern_check(delay_comment, contact_comment) and negated_operand=='false':
                    status = "OK"
                    match_contact_operand = contact_operand
                    break
        

    cc3_result['status'] = status
    cc3_result['cc'] = "cc3"
    cc3_result['target_coil'] = match_contact_operand
    cc3_result['check_number'] = 3
    cc3_result['rung_number'] = rung_number

    return cc3_result

def check_detail_4_functionwise(fault_section_df:pd.DataFrame, detection_5_details:int, fuse_full_width_comment:str, fuse_half_width_comment:str, function_name:str, function_comment_data:str) -> dict:

    logger.info(f"Executing rule no 48 check detail 3 in function {function_name}")

    status = "NG"
    match_contact_operand = ''
    cc4_result = {}
    rung_number = detection_5_details[1]

    detection_coil_rung_df = fault_section_df[fault_section_df['RUNG']==rung_number]
    contact_df = detection_coil_rung_df[detection_coil_rung_df['OBJECT_TYPE_LIST'] == 'Contact'].copy()

    if rung_number != -1:
        for _, contact_row in contact_df.iterrows():
            attr = ast.literal_eval(contact_row['ATTRIBUTES'])
            contact_operand = attr.get('operand')
            negated_operand = attr.get('negated')

            if contact_operand and isinstance(contact_operand, str):
                contact_comment = get_the_comment_from_function(contact_operand, function_name, function_comment_data)
            else:
                contact_comment = []

            if isinstance(contact_comment, list):
                if (regex_pattern_check(fuse_full_width_comment, contact_comment) or regex_pattern_check(fuse_half_width_comment, contact_comment)) and regex_pattern_check(normal_comment, contact_comment) and negated_operand=='true':
                    status = "OK"
                    match_contact_operand = contact_operand
                    break
    

    cc4_result['status'] = status
    cc4_result['cc'] = "cc4"
    cc4_result['target_coil'] = match_contact_operand
    cc4_result['rung_number'] = rung_number
    cc4_result['check_number'] = 4
    # cc4_result['target_coil'] = "FUSE_GOOD"
    # cc4_result['rung_number'] = rung_number

    return cc4_result

def check_detail_5_functionwise(fault_section_df:pd.DataFrame, detection_coil:str, cc3_contact:str, cc4_contact:str, rung_number:int, function_name:str) -> dict:

    logger.info(f"Executing rule no 48 check detail 4 in function {function_name}")

    status = "NG"
    match_rung_number = -1
    both_contact_operand_in_series = False
    cc5_result = {}

    if cc3_contact and cc4_contact:
        detection_coil_rung_df = fault_section_df[fault_section_df['RUNG']==rung_number]
        contact_df = detection_coil_rung_df[detection_coil_rung_df['OBJECT_TYPE_LIST'] == 'Contact'].copy()

        all_self_holding_coil = check_self_holding(detection_coil_rung_df)
        get_series_connect_data = get_series_contacts(pl.from_pandas(detection_coil_rung_df))

        series_contact_operands_only = [[item.get('operand') for item in sublist] for sublist in get_series_connect_data]
        for series_contact in series_contact_operands_only:
            if cc3_contact in series_contact and cc4_contact in series_contact:
                both_contact_operand_in_series = True
                break

        """
        this is function is for checking if both contact should be in under self holding
        logic is if both contact outcoil is less than self holding outcoil then it is under slef holding
        """
        both_contact_outlist = []
        self_holding_outlist = []
        both_contact_inside_self_holding = True
        for _, contact_row in contact_df.iterrows():
            attr = ast.literal_eval(contact_row['ATTRIBUTES'])
            contact_operand = attr.get('operand')
            if contact_operand == cc3_contact or contact_operand == cc4_contact:
                both_contact_outlist.append(attr.get('out_list'))
            if contact_operand == detection_coil:
                self_holding_outlist.append(attr.get('out_list'))
        
        for both_contact_out_val in both_contact_outlist:
            if any(both_contact_out_val > x for x in self_holding_outlist):
                both_contact_inside_self_holding =False
                break
        if detection_coil in all_self_holding_coil and both_contact_operand_in_series and len(contact_df)==3 and both_contact_inside_self_holding:
            status = "OK"
            match_rung_number = rung_number
        else:
            status = "NG"
            match_rung_number = -1

    cc5_result['status'] = status
    cc5_result['cc'] = "cc5"
    cc5_result['target_coil'] = ""
    cc5_result['check_number'] = 5
    cc5_result['rung_number'] = match_rung_number

    return cc5_result

# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_48_programwise(input_program_file:str, 
                                input_program_comment_file:str) -> pd.DataFrame:

    logger.info("Starting execution of Rule 48")

    try:

        program_df = pd.read_csv(input_program_file)
        with open(input_program_comment_file, 'r', encoding="utf-8") as file:
            program_comment_data = json.load(file)

        unique_program_values = program_df["PROGRAM"].unique()
        output_rows = []

        for program_name in unique_program_values:
            logger.info(f"Executing rule 46 in Program {program_name}")

            if 'main' in program_name.lower():

                current_program_df = program_df[program_df['PROGRAM']==program_name]
                devicein_section_df = current_program_df[current_program_df['BODY'].str.lower()==devicein_section_name]
                fault_section_df = current_program_df[current_program_df['BODY'].str.lower()==fault_section_name]

                # Run detection range logic as per Rule 24
                detection_result = detection_range_programwise(
                    devicein_section_df=devicein_section_df,
                    fault_section_df=fault_section_df,
                    fuse_full_width_comment = fuse_full_width_comment,
                    fuse_half_width_comment = fuse_half_width_comment,
                    program_name = program_name,
                    program_comment_data=program_comment_data
                )

                print("program_name",program_name)
                print("detection_result",detection_result)
                detection_3_details = detection_result['detection_3_details']
                detection_5_details = detection_result['detection_5_details']

                cc1_result = check_detail_1_programwise(detection_3_details=detection_3_details, detection_5_details=detection_5_details)
                cc2_result = check_detail_2_programwise(devicein_section_df=devicein_section_df, detection_3_details=detection_3_details, fuse_full_width_comment = fuse_full_width_comment , fuse_half_width_comment = fuse_half_width_comment, program_name=program_name, program_comment_data=program_comment_data)
                cc3_result = check_detail_3_programwise(fault_section_df=fault_section_df, detection_5_details=detection_5_details, program_name=program_name, program_comment_data=program_comment_data)
                cc4_result = check_detail_4_programwise(fault_section_df=fault_section_df, detection_5_details=detection_5_details, fuse_full_width_comment = fuse_full_width_comment , fuse_half_width_comment = fuse_half_width_comment, program_name=program_name, program_comment_data=program_comment_data)
                        
                cc3_contact = cc3_result.get('target_coil', '')
                cc4_contact = cc4_result.get('target_coil', '')
                detection_5_coil = detection_5_details[0]
                detection_5_rung_number = detection_5_details[1]
                
                cc5_result = check_detail_5_programwise(fault_section_df=fault_section_df, detection_coil=detection_5_coil, cc3_contact=cc3_contact, cc4_contact=cc4_contact, rung_number=detection_5_rung_number, program_name=program_name)

                all_cc_result = [cc1_result, cc2_result, cc3_result, cc4_result, cc5_result]

                for cc_result in all_cc_result:
                    ng_name = ng_content.get(cc_result.get('cc', '')) if cc_result.get('status') == "NG" else ""
                    rung_number = cc_result.get('rung_number')-1 if cc_result.get('rung_number')!=-1 else -1
                    target_outcoil = cc_result.get('target_coil') if cc_result.get('target_coil') else ""
                    check_number = cc_result.get("check_number")

                    output_rows.append({
                        "Result": cc_result.get('status'),
                        "Task": program_name,
                        "Section": fault_section_name,
                        "RungNo": rung_number,
                        "Target": target_outcoil,
                        "CheckItem": rule_48_check_item,
                        "Detail": ng_name,
                        "Status" : ""
                    })

                    # output_rows.append({
                    #     "TASK_NAME": program_name,
                    #     "SECTION_NAME": fault_section_name,
                    #     "RULE_NUMBER": "48",
                    #     "CHECK_NUMBER": check_number,
                    #     "RUNG_NUMBER": -1 if rung_number < 0 else rung_number-1,
                    #     "RULE_CONTENT": rule_content_48,
                    #     "CHECK_CONTENT": check_detail_content.get(cc_result.get('cc')),
                    #     "STATUS": cc_result.get('status'),
                    #     "Target_outcoil" : target_outcoil,
                    #     "NG_EXPLANATION": ng_name
                    # })

        final_output_df = pd.DataFrame(output_rows)
        if not final_output_df.empty:
            if 'RungNo' in final_output_df.columns:
                final_output_df['RungNo'] = final_output_df['RungNo'].apply(clean_rung_number)
        else:
            final_output_df = pd.DataFrame(columns=["Result","Task","Section","RungNo","Target","CheckItem","Detail","Status"])

        return {"status":"OK", "output_df":final_output_df}

    except Exception as e:
        logger.error(f"Rule 47 Error : {e}")

        return {"status":"NOT OK", "error":str(e)}





# ============================== Function-Wise Execution Starts Here ===============================
def execute_rule_48_functionwise(input_function_file:str, 
                                 input_function_comment_file:str) -> pd.DataFrame:

    logger.info("Starting execution of Rule 48")

    try:

        function_df = pd.read_csv(input_function_file)
        with open(input_function_comment_file, 'r', encoding="utf-8") as file:
            function_comment_data = json.load(file)

        unique_function_values = function_df["FUNCTION_BLOCK"].unique()
        output_rows = []

        for function_name in unique_function_values:
            logger.info(f"Executing rule 46 in function {function_name}")

            if 'main' in function_name.lower():

                current_function_df = function_df[function_df['FUNCTION_BLOCK']==function_name]
                devicein_section_df = current_function_df[current_function_df['BODY_TYPE'].str.lower()==devicein_section_name]
                fault_section_df = current_function_df[current_function_df['BODY_TYPE'].str.lower()==fault_section_name]

                # Run detection range logic as per Rule 24
                detection_result = detection_range_functionwise(
                    devicein_section_df=devicein_section_df,
                    fault_section_df=fault_section_df,
                    fuse_full_width_comment = fuse_full_width_comment,
                    fuse_half_width_comment = fuse_half_width_comment,
                    function_name = function_name,
                    function_comment_data=function_comment_data
                )

                detection_3_details = detection_result['detection_3_details']
                detection_5_details = detection_result['detection_5_details']

                cc1_result = check_detail_1_functionwise(detection_3_details=detection_3_details, detection_5_details=detection_5_details)
                cc2_result = check_detail_2_functionwise(devicein_section_df=devicein_section_df, detection_3_details=detection_3_details, fuse_full_width_comment = fuse_full_width_comment , fuse_half_width_comment = fuse_half_width_comment, function_name=function_name, function_comment_data=function_comment_data)
                cc3_result = check_detail_3_functionwise(fault_section_df=fault_section_df, detection_5_details=detection_5_details, function_name=function_name, function_comment_data=function_comment_data)
                cc4_result = check_detail_4_functionwise(fault_section_df=fault_section_df, detection_5_details=detection_5_details, fuse_full_width_comment = fuse_full_width_comment , fuse_half_width_comment = fuse_half_width_comment, function_name=function_name, function_comment_data=function_comment_data)
                        
                cc3_contact = cc3_result.get('target_coil', '')
                cc4_contact = cc4_result.get('target_coil', '')
                detection_5_coil = detection_5_details[0]
                detection_5_rung_number = detection_5_details[1]
                
                cc5_result = check_detail_5_functionwise(fault_section_df=fault_section_df, detection_coil=detection_5_coil, cc3_contact=cc3_contact, cc4_contact=cc4_contact, rung_number=detection_5_rung_number, function_name=function_name)

                all_cc_result = [cc1_result, cc2_result, cc3_result, cc4_result, cc5_result]

                for cc_result in all_cc_result:
                    ng_name = ng_content.get(cc_result.get('cc', '')) if cc_result.get('status') == "NG" else ""
                    rung_number = cc_result.get('rung_number') if cc_result.get('rung_number') else -1
                    target_outcoil = cc_result.get('target_coil') if cc_result.get('target_coil') else ""
                    check_number = cc_result.get("check_number")

                    output_rows.append({
                        "Result": cc_result.get('status'),
                        "Task": function_name,
                        "Section": fault_section_name,
                        "RungNo": rung_number,
                        "Target": target_outcoil,
                        "CheckItem": rule_48_check_item,
                        "Detail": ng_name,
                        "Status" : ""
                    })

                    # output_rows.append({
                    #     "TASK_NAME": function_name,
                    #     "SECTION_NAME": fault_section_name,
                    #     "RULE_NUMBER": "48",
                    #     "CHECK_NUMBER": check_number,
                    #     "RUNG_NUMBER": -1 if rung_number < 0 else rung_number-1,
                    #     "RULE_CONTENT": rule_content_48,
                    #     "CHECK_CONTENT": check_detail_content.get(cc_result.get('cc')),
                    #     "STATUS": cc_result.get('status'),
                    #     "Target_outcoil" : target_outcoil,
                    #     "NG_EXPLANATION": ng_name
                    # })


        final_output_df = pd.DataFrame(output_rows)
        if not final_output_df.empty:
            if 'RungNo' in final_output_df.columns:
                final_output_df['RungNo'] = final_output_df['RungNo'].apply(clean_rung_number)
        else:
            final_output_df = pd.DataFrame(columns=["Result","Task","Section","RungNo","Target","CheckItem","Detail","Status"])

        return {"status":"OK", "output_df":final_output_df}

    except Exception as e:
        logger.error(f"Rule 47 Error : {e}")

        return {"status":"NOT OK", "error":str(e)}



# if __name__=='__main__':

#     input_program_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/GithubCode/rules_personal/data_modelling/data_model_Rule_46_56_NG_v2/data_model_Rule_46_56_NG_v2_programwise.csv"
#     input_program_comment_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/GithubCode/rules_personal/data_modelling/data_model_Rule_46_56_NG_v2/data_model_Rule_46_56_NG_v2_programwise.json"
#     input_function_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/GithubCode/rules_personal/data_modelling/data_model_Rule_46_56_NG_v2/data_model_Rule_46_56_NG_v2_functionwise.csv"
#     input_function_comment_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/GithubCode/rules_personal/data_modelling/data_model_Rule_46_56_NG_v2/data_model_Rule_46_56_NG_v2_functionwise.json"
#     output_folder_path = 'C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Rules_implementation/pythoncode/output_csv/'
#     program_output_file = 'Rule_48_programwise_NG_v2.csv'
#     function_output_file = 'Rule_48_functionwise_NG_v2.csv'


#     final_csv = execute_rule_48_programwise(input_program_file=input_program_file, input_program_comment_file=input_program_comment_file)
#     final_csv.to_csv(f"{output_folder_path}/{program_output_file}", index=False, encoding='utf-8-sig')

#     final_csv = execute_rule_48_functionwise(input_function_file=input_function_file, input_function_comment_file=input_function_comment_file)
#     final_csv.to_csv(f"{output_folder_path}/{function_output_file}", index=False, encoding='utf-8-sig')
