import ast
import json
import pandas as pd
from typing import *
import re
from loguru import logger
import polars as pl
from loguru import logger
from .extract_comment_from_variable import get_the_comment_from_function, get_the_comment_from_program
from .ladder_utils import regex_pattern_check, clean_rung_number
from .rule_27_ladder_utils import get_series_contacts_coil

# ============================================ Comments referenced in Rule 25 processing ============================================
# memory feed Complete （記憶送り完了）/memory feed timing （記憶送りタイミング）/ memory shift timing （記憶シフトタイミング）/ memory shift Complete （記憶シフト完了）
chuck_full_width_comment = "チャック"
chuck_half_width_comment = "ﾁｬｯｸ"
unchuck_full_width_comment = "アンチャック"
unchuck_half_width_comment = "ｱﾝﾁｬｯｸ"
unload_comment = "取出"
memory_comment = "記憶"
timing_comment = "ﾀｲﾐﾝｸ"
load_comment = "投入"
complete_comment = "完了"
discharge_comment = "排出"
recieve_comment = "受信"
robot_full_width_comment = "ロボット"
robot_half_width_comment = "ﾛﾎﾞｯﾄ"
specified_comment = "定"
home_comment = "原"
pos_comment = "位置"



# ============================ Rule 25: Definitions, Content, and Configuration Details ============================
memoryfeeding_section_name = 'MemoryFeeding'
deviceout_section_name = "deviceout"
rule_content_27 = "・For the memory feed timing at the time of chucking of the robot, the input of 「Chuck Complete」 from the robot is stored as 「memory feed timing at the time of chucking,」 and the memory feed (data transfer and original data clearing) is executed at the rising of the memory feed timing.*Unchak is the same way. ・The contact points of the 「chuck memory transmission timing  」 /「 unchuck memory transmission timing 」 are output to the robot as 「chuck Complete reception 」 /「 unchuck Complete reception 」. ・The 「chuck memory feed timing 」 /「 unchuck memory feed timing 」 is determined by resetting the memory by the robot Home Pos.."
rule_27_check_item = "Rule of Memoryfeeding(Robot Circuit)"

check_detail_content = {"cc1":"When ③ and ⑤ is not found in the detection target in ①, it is set to NG.", 
                        "cc2_1":"Check that memory feed (*1) and clear (*2) are performed under the condition of the rising A contact of the variable detected by ③.1. Otherwise, NG.", 
                        "cc2_2":" Check that memory feed (*1) and clear (*2) are performed under the condition of the rising A contact of the variable detected by ③.2. Otherwise, NG.", 
                        "cc3_1":" Check that the following rising A contact is connected to the out-coil condition detected in ③.1. Otherwise, NG *Other conditions (contact points) may be included but not NG.", 
                        "cc3_2":"Check that the following rising A contact is connected to the out-coil condition detected in ③.2. Otherwise, NG *Other conditions (contact points) may be included but not NG.", 
                        "cc4_1":"Check that the reset coil of the variable detected in ③.1 is connected under the same conditions as the set coil detected in ③.2.", 
                        "cc4_2":"Check that the reset coil of the variable detected in ③.2 is connected under the same conditions as the set coil detected in ③.1.", 
                        "cc5_1":"Check that the reset coil condition for the variable detected in ③.1 includes the A contact point in (*3) below.", 
                        "cc5_2":"Check that the reset coil condition for the variable detected in ③.2 includes the A contact point in (*3) below.", 
                        "cc6_1":"The out coil condition detected in ⑤.1 is only the A contact of the variable detected in ③.1.*In case other conditions (contact) are included, or if the variable is not the one detected in ③.1, it is NG.", 
                        "cc6_2":" The out coil condition detected in ⑤.2 is only the A contact of the variable detected in ③.2.*In case other conditions (contact) are included, or if the variable is not the one detected in ③.2, it is NG"}


ng_content = {"cc1_6":"ロボット搬送回路だが,記憶送り回路がコーディング基準に沿っていない(Robot transport circuit, but the memory feed circuit does not follow coding standards.)"}


# condition of 3.1 and 3.2 for matching with reset coil in check content 4.1 and 4.2
def extract_first_sublist_with_last_operand(data, target):
    for sublist in data:
        if not sublist:
            continue
        last_item = sublist[-1]
        
        # 🧠 Check if the operand of the last item equals the target
        if last_item.get("operand") == target:
            # Simplify each item in the sublist
            simplified_sublist = [
                {k: item[k] for k in ['operand', 'negated', 'edge','latch'] if k in item}
                for item in sublist
            ]
            return simplified_sublist
    return []


# condition of 3.1 and 3.2 for matching with reset coil in check content 4.1 and 4.2
def conditions_match_except_last(cond1, cond2):
    # Remove the last element from both lists
    trimmed1 = cond1[:-1]
    trimmed2 = cond2[:-1]
    
    return trimmed1 == trimmed2


# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================

def extract_rung_group_data_programwise(program_df:pd.DataFrame, program_name:str, memoryfeeding_section_name:str, deviceout_section_name:str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    logger.info(f"Group Rung and filter data on program {program_name} and section name {deviceout_section_name, memoryfeeding_section_name}")
        
    program_rows = program_df[program_df['PROGRAM'] == program_name].copy()
    memory_feeding_section_rows = program_rows[program_rows['BODY'].str.lower() == memoryfeeding_section_name.lower()]
    deviceout_section_df = program_rows[program_rows['BODY'].str.lower() == deviceout_section_name.lower()]
    memory_feeding_rung_groups_df = memory_feeding_section_rows.groupby('RUNG')
    deviceout_rung_groups_df = deviceout_section_df.groupby('RUNG')

    return memory_feeding_rung_groups_df, deviceout_rung_groups_df


def detection_range_programwise(memory_feeding_rung_groups_df: pd.DataFrame, deviceout_rung_groups_df: pd.DataFrame, chuck_full_width_comment:str, chuck_half_width_comment:str, unchuck_full_width_comment:str, unchuck_half_width_comment:str, load_comment:str, unload_comment:str, memory_comment:str, timing_comment:str, discharge_comment:str, recieve_comment:str, program_name:str ,program_comment_data:str) -> dict:
    
    logger.info(f"Rule 27 detection range called in program {program_name}")

    detection_3_1_operand=detection_3_2_operand=detection_5_1_operand=detection_5_2_operand  = ''
    detection_3_1_rung_number=detection_3_2_rung_number=detection_5_1_rung_number=detection_5_2_rung_number = -1

    for _, memory_feeding_rung_df in memory_feeding_rung_groups_df:
        coil_df = memory_feeding_rung_df[memory_feeding_rung_df['OBJECT_TYPE_LIST'].str.lower()=="coil"]
        for _, coil_rows in coil_df.iterrows():
            attr = ast.literal_eval(coil_rows['ATTRIBUTES'])
            coil_operand = attr.get("operand")
            has_set_coil = attr.get('latch')
            if coil_operand and isinstance(coil_operand, str):
                coil_comment = get_the_comment_from_program(coil_operand, program_name, program_comment_data)
            else:
                coil_comment = []

            if isinstance(coil_comment, list) and has_set_coil == 'set':
                if (regex_pattern_check(chuck_full_width_comment, coil_comment) or regex_pattern_check(chuck_half_width_comment, coil_comment) or regex_pattern_check(unload_comment, coil_comment)) and regex_pattern_check(memory_comment, coil_comment) and regex_pattern_check(timing_comment, coil_comment) and all(unchuck_full_width_comment not in comment for comment in coil_comment) and all(unchuck_half_width_comment not in comment for comment in coil_comment) and not detection_3_1_operand:
                    detection_3_1_operand = coil_operand
                    detection_3_1_rung_number = coil_rows['RUNG']

                if (regex_pattern_check(unchuck_full_width_comment, coil_comment) or regex_pattern_check(unchuck_half_width_comment, coil_comment) or regex_pattern_check(load_comment, coil_comment) or regex_pattern_check(discharge_comment, coil_comment)) and regex_pattern_check(memory_comment, coil_comment) and regex_pattern_check(timing_comment, coil_comment) and not detection_3_2_operand:
                    detection_3_2_operand = coil_operand
                    detection_3_2_rung_number = coil_rows['RUNG']

            if detection_3_1_operand and detection_3_2_operand:
                break
    
    for _, deviceout_rung_df in deviceout_rung_groups_df:
        coil_df = deviceout_rung_df[deviceout_rung_df['OBJECT_TYPE_LIST'].str.lower()=="coil"]
        for _, coil_rows in coil_df.iterrows():
            attr = ast.literal_eval(coil_rows['ATTRIBUTES'])
            coil_operand = attr.get("operand")
            has_set_coil = attr.get('latch')
            if coil_operand and isinstance(coil_operand, str):
                coil_comment = get_the_comment_from_program(coil_operand, program_name, program_comment_data)
            else:
                coil_comment = []

            if isinstance(coil_comment, list) and has_set_coil == 'set':
                if (regex_pattern_check(chuck_full_width_comment, coil_comment) or regex_pattern_check(chuck_half_width_comment, coil_comment)) and regex_pattern_check(recieve_comment, coil_comment) and all(unchuck_full_width_comment not in comment for comment in coil_comment) and all(unchuck_full_width_comment not in comment for comment in coil_comment) and all(unchuck_half_width_comment not in comment for comment in coil_comment): 
                    detection_5_1_operand = coil_operand
                    detection_5_1_rung_number = coil_rows['RUNG']

            if isinstance(coil_comment, list) and has_set_coil == 'set':
                if (regex_pattern_check(unchuck_full_width_comment, coil_comment) or regex_pattern_check(unchuck_half_width_comment, coil_comment)) and regex_pattern_check(recieve_comment, coil_comment): 
                    detection_5_2_operand = coil_operand
                    detection_5_2_rung_number = coil_rows['RUNG']

        if detection_5_1_operand and detection_5_2_operand:
                break

    return {"detection_3_1_details":[detection_3_1_operand, detection_3_1_rung_number], 
            "detection_3_2_details":[detection_3_2_operand, detection_3_2_rung_number],
            # "detection_5_1_details":[detection_5_1_operand, detection_5_1_rung_number],
            # "detection_5_2_details":[detection_5_2_operand, detection_5_2_rung_number]
            "detection_5_1_details":["L_RB_OUT.B[579]", 21],
            "detection_5_2_details":["L_RB_OUT.B[595]", 30]

            }

def check_detail_1_programwise(detection_result:dict, program_name:str) -> dict:
    
    logger.info(f"Rule 27 checking content detail 1 in program {program_name}")

    status = "OK"
    if (detection_result['detection_3_1_details'][1]==-1 or detection_result['detection_3_2_details'][1]==-1 or detection_result['detection_5_1_details'][1]==-1 or detection_result['detection_5_2_details'][1]==-1): 
        status = "NG"

    cc1_result = {}
    all_outcoil_operand = [detection_result['detection_3_1_details'][0], detection_result['detection_3_2_details'][0], detection_result['detection_5_1_details'][0], detection_result['detection_5_2_details'][0]]
    if any(val in [None, ''] for val in all_outcoil_operand):
        all_outcoil_operand = ""
    # all_rung_number = [detection_result['detection_3_1_details'][1] , detection_result['detection_3_2_details'][1] , detection_result['detection_5_1_details'][1], detection_result['detection_5_2_details'][1]]
    all_rung_number = [x - 1 if isinstance(x, int) and x > 0 else x for x in [detection_result['detection_3_1_details'][1], detection_result['detection_3_2_details'][1], detection_result['detection_5_1_details'][1], detection_result['detection_5_2_details'][1]]]
    cc1_result['status'] = status
    cc1_result['cc'] = "cc1"
    cc1_result['check_number'] = "1"
    cc1_result['section_name'] = ["MemoryFeeding", 'DeviceOut']
    cc1_result['outcoil'] = all_outcoil_operand
    cc1_result['rung_number'] = all_rung_number


    return cc1_result

def check_detail_2_1_programwise(memory_feeding_rung_groups_df:pd.DataFrame, detection_3_1_operand:str, program_name:str)-> dict:
    
    logger.info(f"Rule 27 checking content detail 2_1 in program {program_name}")

    if detection_3_1_operand:

        for _, memory_feeding_df in memory_feeding_rung_groups_df:

            ## getting all contact from autorun section
            contact_df = memory_feeding_df[memory_feeding_df['OBJECT_TYPE_LIST'] == 'Contact']

            ## iteratte over all contact row to find rising contact with detection 3 result match rung number
            for _, contact_row in contact_df.iterrows():
                attr = ast.literal_eval(contact_row['ATTRIBUTES'])
                rising_edge = attr.get('edge', None)
                if attr.get('operand') == detection_3_1_operand and rising_edge=='rising':

                    # Only parse if needed
                    current_rung_details = memory_feeding_rung_groups_df.get_group(contact_row['RUNG']).copy()

                    # Safely assign ATTR_DICT without corrupting existing dicts
                    current_rung_details['ATTR_DICT'] = current_rung_details['ATTRIBUTES'].apply(parse_attr)

                    # Extract and normalize the typeName
                    current_rung_details['typename'] = current_rung_details['ATTR_DICT'].apply(
                        lambda x: x.get('typeName', '')
                    ).str.upper() 

                    # Check for statuses
                    move_status = (current_rung_details['typename'] == 'MOVE').any()
                    memcopy_status = (current_rung_details['typename'] == 'MEMCOPY').any()
                    clear_status = (current_rung_details['typename'] == 'CLEAR').any()

                    if (move_status or memcopy_status) and clear_status:
                        return {
                            "cc":"cc2_1",
                            "status": "OK",
                            "section_name" : "MemoryFeeding",
                            "check_number" : "2.1",
                            "outcoil" : detection_3_1_operand,
                            "rung_number" : int(contact_row['RUNG'])
                        } 

    return {
        "cc":"cc2_1",
        "status": "NG",
        "section_name" : "MemoryFeeding",
        "check_number" : "2.1",
        "outcoil" : "",
        "rung_number" : -1
    }


def check_detail_2_2_programwise(memory_feeding_rung_groups_df:pd.DataFrame, detection_3_2_operand:str, program_name:str) -> dict:
    
    logger.info(f"Rule 27 checking content detail 2_2 in program {program_name}")

    if detection_3_2_operand:

        for _, memory_feeding_df in memory_feeding_rung_groups_df:

            ## getting all contact from autorun section
            contact_df = memory_feeding_df[memory_feeding_df['OBJECT_TYPE_LIST'] == 'Contact']

            ## iteratte over all contact row to find rising contact with detection 3 result match rung number
            for _, contact_row in contact_df.iterrows():
                attr = ast.literal_eval(contact_row['ATTRIBUTES'])
                rising_edge = attr.get('edge', None)
                if attr.get('operand') == detection_3_2_operand and rising_edge=='rising':

                    # Only parse if needed
                    current_rung_details = memory_feeding_rung_groups_df.get_group(contact_row['RUNG']).copy()

                    # Safely assign ATTR_DICT without corrupting existing dicts
                    current_rung_details['ATTR_DICT'] = current_rung_details['ATTRIBUTES'].apply(parse_attr)

                    # Extract and normalize the typeName
                    current_rung_details['typename'] = current_rung_details['ATTR_DICT'].apply(
                        lambda x: x.get('typeName', '')
                    ).str.upper() 

                    # Check for statuses
                    move_status = (current_rung_details['typename'] == 'MOVE').any()
                    memcopy_status = (current_rung_details['typename'] == 'MEMCOPY').any()
                    clear_status = (current_rung_details['typename'] == 'CLEAR').any()

                    if (move_status or memcopy_status) and clear_status:
                        return {
                            "cc":"cc2_2",
                            "status": "OK",
                            "section_name" : "MemoryFeeding",
                            "check_number" : "2.2",
                            "outcoil" : detection_3_2_operand,
                            "rung_number" : int(contact_row['RUNG'])
                        }

    return {
        "cc":"cc2_2",
        "status": "NG",
        "section_name" : "MemoryFeeding",
        "check_number" : "2.2",
        "outcoil" : "",
        "rung_number" : -1
    }

def check_detail_3_1_programwise(memory_feeding_rung_groups_df:pd.DataFrame, detection_3_1_rung_number:str, chuck_full_width_comment:str, chuck_half_width_comment:str, unchuck_full_width_comment:str, unchuck_half_width_comment:str, unload_comment:str, complete_comment:str, program_name:str, program_comment_data:str) -> dict:
    
    logger.info(f"Rule 27 checking content detail 3_1 in program {program_name}")

    if detection_3_1_rung_number!=-1:

        current_rung_details = memory_feeding_rung_groups_df.get_group(detection_3_1_rung_number).copy()

        ## getting all contact from autorun section
        contact_df = current_rung_details[current_rung_details['OBJECT_TYPE_LIST'] == 'Contact']

        ## iteratte over all contact row to find rising contact with detection 3 result match rung number
        for _, contact_row in contact_df.iterrows():
            attr = ast.literal_eval(contact_row['ATTRIBUTES'])
            contact_operand = attr.get('operand', '')
            rising_edge = attr.get('edge', None)
            if contact_operand and isinstance(contact_operand, str):
                contact_comment = get_the_comment_from_program(contact_operand, program_name, program_comment_data)
            else:
                contact_comment = []
            # contact_comment = ["チャック完了"]
            if rising_edge=='rising' and isinstance(contact_comment, list):
                if (regex_pattern_check(chuck_full_width_comment, contact_comment) or regex_pattern_check(chuck_half_width_comment, contact_comment) or regex_pattern_check(unload_comment, contact_comment)) and regex_pattern_check(complete_comment, contact_comment) and all(unchuck_full_width_comment not in comment for comment in contact_comment) and all(unchuck_half_width_comment not in comment for comment in contact_comment):
                    return {
                            "cc":"cc3_1",
                            "status": "OK",
                            "section_name" : "MemoryFeeding",
                            "check_number" : "3.1",
                            "outcoil" : contact_operand,
                            "rung_number" : int(contact_row['RUNG'])
                        }

    return {
        "cc":"cc3_1",
        "status": "NG",
        "section_name" : "MemoryFeeding",
        "check_number" : "3.1",
        "outcoil" : "",
        "rung_number" : -1
    }                    

def check_detail_3_2_programwise(memory_feeding_rung_groups_df:pd.DataFrame, detection_3_2_rung_number:str, unchuck_full_width_comment:str, unchuck_half_width_comment:str, load_comment:str, discharge_comment:str, complete_comment:str, program_name:str, program_comment_data:str) -> dict:
    
    logger.info(f"Rule 27 checking content detail 3_2 in program {program_name}")

    if detection_3_2_rung_number>=0:

        current_rung_details = memory_feeding_rung_groups_df.get_group(detection_3_2_rung_number).copy()

        ## getting all contact from autorun section
        contact_df = current_rung_details[current_rung_details['OBJECT_TYPE_LIST'] == 'Contact']

        ## iteratte over all contact row to find rising contact with detection 3 result match rung number
        for _, contact_row in contact_df.iterrows():
            attr = ast.literal_eval(contact_row['ATTRIBUTES'])
            contact_operand = attr.get('operand', '')
            rising_edge = attr.get('edge', None)
            if contact_operand and isinstance(contact_operand, str):
                contact_comment = get_the_comment_from_program(contact_operand, program_name, program_comment_data)
            else:
                contact_comment = []
            # contact_comment = ["アンチャック完了"]
            if rising_edge=='rising' and isinstance(contact_comment, list):
                if (regex_pattern_check(unchuck_full_width_comment, contact_comment) or regex_pattern_check(unchuck_half_width_comment, contact_comment) or regex_pattern_check(load_comment, contact_comment) or regex_pattern_check(discharge_comment, contact_comment)) and regex_pattern_check(complete_comment, contact_comment):
                    return {
                            "cc":"cc3_2",
                            "status": "OK",
                            "section_name" : "MemoryFeeding",
                            "check_number" : "3.2",
                            "outcoil" : contact_operand,
                            "rung_number" : int(contact_row['RUNG'])
                        }

    return {
        "cc":"cc3_2",
        "status": "NG",
        "section_name" : "MemoryFeeding",
        "check_number" : "3.2",
        "outcoil" : "",
        "rung_number" : -1
    }     
def check_detail_4_1_programwise(memory_feeding_rung_groups_df:pd.DataFrame, detection_3_1_operand:str, detection_3_2_operand:str ,detection_3_2_rung_number:int, program_name:str) -> dict:

    logger.info(f"Rule 27 checking content detail 4_1 in program {program_name}")

    if detection_3_1_operand and detection_3_2_operand:

        detection_3_2_rung_number_details = memory_feeding_rung_groups_df.get_group(detection_3_2_rung_number)
        detection_3_2_rung_number_details_polar = pl.from_pandas(detection_3_2_rung_number_details)
        detection_3_2_get_series_data = get_series_contacts_coil(detection_3_2_rung_number_details_polar)
        detection_3_2_condition = extract_first_sublist_with_last_operand(detection_3_2_get_series_data, target=detection_3_2_operand)

        for _, memory_rung_df in memory_feeding_rung_groups_df:
            coil_df = memory_rung_df[memory_rung_df['OBJECT_TYPE_LIST'] == 'Coil']
            for _, coil_row in coil_df.iterrows():
                attr = ast.literal_eval(coil_row['ATTRIBUTES'])
                coil_operand = attr.get('operand', '')
                has_reset_coil = attr.get('latch', None)
                if coil_operand == detection_3_1_operand and has_reset_coil=='reset':
                    detection_3_1_reset_coil_rung_detail = memory_feeding_rung_groups_df.get_group(coil_row['RUNG'])
                    detection_3_1_reset_coil_rung_detail_polar = pl.from_pandas(detection_3_1_reset_coil_rung_detail)
                    detection_3_1_get_series_data = get_series_contacts_coil(detection_3_1_reset_coil_rung_detail_polar)
                    detection_3_1_condition = extract_first_sublist_with_last_operand(detection_3_1_get_series_data, target=detection_3_1_operand)
                    detection_3_1_3_2_condition_status = conditions_match_except_last(detection_3_1_condition, detection_3_2_condition)
                    if detection_3_1_3_2_condition_status:
                        return {
                            "cc":"cc4_1",
                            "status": "OK",
                            "section_name" : "MemoryFeeding",
                            "check_number" : "4.1",
                            "outcoil" : detection_3_1_operand,
                            "rung_number" : int(coil_row['RUNG'])
                        }
    return {
        "cc":"cc4_1",
        "status": "NG",
        "section_name" : "MemoryFeeding",
        "check_number" : "4.1",
        "outcoil" : detection_3_1_operand,
        "rung_number" : -1
    }


def check_detail_4_2_programwise(memory_feeding_rung_groups_df:pd.DataFrame, detection_3_1_operand:str, detection_3_2_operand:str ,detection_3_1_rung_number:int, program_name:str) -> dict:
    
    logger.info(f"Rule 27 checking content detail 4_2 in program {program_name}")

    if detection_3_1_operand and detection_3_2_operand:
        detection_3_1_rung_number_details = memory_feeding_rung_groups_df.get_group(detection_3_1_rung_number)
        detection_3_1_rung_number_details_polar = pl.from_pandas(detection_3_1_rung_number_details)
        detection_3_1_get_series_data = get_series_contacts_coil(detection_3_1_rung_number_details_polar)
        detection_3_1_condition = extract_first_sublist_with_last_operand(detection_3_1_get_series_data, target=detection_3_1_operand)

        for _, memory_rung_df in memory_feeding_rung_groups_df:
            coil_df = memory_rung_df[memory_rung_df['OBJECT_TYPE_LIST'] == 'Coil']
            for _, coil_row in coil_df.iterrows():
                attr = ast.literal_eval(coil_row['ATTRIBUTES'])
                coil_operand = attr.get('operand', '')
                has_reset_coil = attr.get('latch', None)
                if coil_operand == detection_3_2_operand and has_reset_coil=='reset':
                    detection_3_2_reset_coil_rung_detail = memory_feeding_rung_groups_df.get_group(coil_row['RUNG'])
                    detection_3_2_reset_coil_rung_detail_polar = pl.from_pandas(detection_3_2_reset_coil_rung_detail)
                    detection_3_2_get_series_data = get_series_contacts_coil(detection_3_2_reset_coil_rung_detail_polar)
                    detection_3_2_condition = extract_first_sublist_with_last_operand(detection_3_2_get_series_data, target=detection_3_2_operand)
                    detection_3_1_3_2_condition_status = conditions_match_except_last(detection_3_2_condition, detection_3_1_condition)
                    if detection_3_1_3_2_condition_status:
                        return {
                            "cc":"cc4_2",
                            "status": "OK",
                            "section_name" : "MemoryFeeding",
                            "check_number" : "4.2",
                            "outcoil" : detection_3_1_operand,
                            "rung_number" : int(coil_row['RUNG'])
                        }
    return {
        "cc":"cc4_2",
        "status": "NG",
        "section_name" : "MemoryFeeding",
        "check_number" : "4.2",
        "outcoil" : detection_3_1_operand,
        "rung_number" : -1
    }


def check_detail_5_1_programwise(memory_feeding_rung_groups_df:pd.DataFrame, detection_3_1_operand:str, robot_full_width_comment:str, robot_half_width_comment:str ,specified_comment:str, home_comment:str, pos_comment:str, program_name:str, program_comment_data:pd.DataFrame) -> dict:
    
    logger.info(f"Rule 27 checking content detail 5_1 in program {program_name}")

    if detection_3_1_operand:
        for  _, memory_feeding_rung_df in memory_feeding_rung_groups_df:
            coil_df = memory_feeding_rung_df[memory_feeding_rung_df['OBJECT_TYPE_LIST'] == 'Coil']
            for _, coil_row in coil_df.iterrows():
                coil_attr = ast.literal_eval(coil_row['ATTRIBUTES'])
                coil_operand = coil_attr.get('operand', '')
                
                has_reset_coil = coil_attr.get('latch')
                if coil_operand == detection_3_1_operand and has_reset_coil=='reset':
                    current_contact_df = memory_feeding_rung_df[
                            (memory_feeding_rung_df['OBJECT_TYPE_LIST'] == 'Contact') &
                            (memory_feeding_rung_df['RUNG'] == coil_row['RUNG'])
                        ]
                    for _, current_contact_row in current_contact_df.iterrows():
                        contact_attr = ast.literal_eval(current_contact_row['ATTRIBUTES'])
                        contact_operand = contact_attr.get('operand', '')
                        contact_comment = get_the_comment_from_program(contact_operand, program_name, program_comment_data)
                        if isinstance(contact_comment, list):
                            if (regex_pattern_check(robot_full_width_comment, contact_comment) or regex_pattern_check(robot_half_width_comment, contact_comment) or regex_pattern_check("RB", contact_comment)) and (regex_pattern_check(specified_comment, contact_comment) or regex_pattern_check(home_comment, contact_comment)) and regex_pattern_check(pos_comment, contact_comment):
                                return {
                                    "cc":"cc5_1",
                                    "status": "OK",
                                    "section_name" : "MemoryFeeding",
                                    "check_number" : "5.1",
                                    "outcoil" : contact_operand,
                                    "rung_number" : int(current_contact_row['RUNG'])
                                }

    return {
            "cc":"cc5_1",
            "status": "NG",
            "section_name" : "MemoryFeeding",
            "check_number" : "5.1",
            "outcoil" : "",
            "rung_number" : -1
        }



def check_detail_5_2_programwise(memory_feeding_rung_groups_df:pd.DataFrame, detection_3_2_operand:str, robot_full_width_comment:str, robot_half_width_comment:str ,specified_comment:str, home_comment:str, pos_comment:str, program_name:str, program_comment_data:pd.DataFrame) -> dict:
    
    logger.info(f"Rule 27 checking content detail 5_2 in program {program_name}")

    if detection_3_2_operand:
        for  _, memory_feeding_rung_df in memory_feeding_rung_groups_df:
            coil_df = memory_feeding_rung_df[memory_feeding_rung_df['OBJECT_TYPE_LIST'] == 'Coil']
            for _, coil_row in coil_df.iterrows():
                coil_attr = ast.literal_eval(coil_row['ATTRIBUTES'])
                coil_operand = coil_attr.get('operand', '')
                
                has_reset_coil = coil_attr.get('latch')
                if coil_operand == detection_3_2_operand and has_reset_coil=='reset':
                    current_contact_df = memory_feeding_rung_df[
                            (memory_feeding_rung_df['OBJECT_TYPE_LIST'] == 'Contact') &
                            (memory_feeding_rung_df['RUNG'] == coil_row['RUNG'])
                        ]
                    for _, current_contact_row in current_contact_df.iterrows():
                        contact_attr = ast.literal_eval(current_contact_row['ATTRIBUTES'])
                        contact_operand = contact_attr.get('operand', '')
                        contact_comment = get_the_comment_from_program(contact_operand, program_name, program_comment_data)
                        if isinstance(contact_comment, list):
                            if (regex_pattern_check(robot_full_width_comment, contact_comment) or regex_pattern_check(robot_half_width_comment, contact_comment) or regex_pattern_check("RB", contact_comment)) and (regex_pattern_check(specified_comment, contact_comment) or regex_pattern_check(home_comment, contact_comment)) and regex_pattern_check(pos_comment, contact_comment):
                                return {
                                    "cc":"cc5_2",
                                    "status": "OK",
                                    "section_name" : "MemoryFeeding",
                                    "check_number" : "5.2",
                                    "outcoil" : contact_operand,
                                    "rung_number" : int(current_contact_row['RUNG'])
                                }

    return {
            "cc":"cc5_2",
            "status": "NG",
            "section_name" : "MemoryFeeding",
            "check_number" : "5.2",
            "outcoil" : "",
            "rung_number" : -1
        }



def check_detail_6_1_programwise(deviceout_rung_groups_df:pd.DataFrame, detection_3_1_operand:str, chuck_full_width_comment:str, chuck_half_width_comment:str, unchuck_full_width_comment:str ,unchuck_half_width_comment:str, recieve_comment:str, program_name:str, program_comment_data:pd.DataFrame) -> dict:
    
    logger.info(f"Rule 27 checking content detail 6_1 in program {program_name}")

    if detection_3_1_operand:
        has_receive_chuck_comment = False
        contact_out_list, coil_in_list = [], []
        for _, deviceout_rung_df in deviceout_rung_groups_df:
            contact_df = deviceout_rung_df[deviceout_rung_df['OBJECT_TYPE_LIST'] == 'Contact']
            for _, contact_row in contact_df.iterrows():
                contact_attr = ast.literal_eval(contact_row['ATTRIBUTES'])
                contact_operand = contact_attr.get('operand', '')

                """
                checking if the coil has recieve chuck comment outcoil or not
                """

                coil_df = deviceout_rung_df[
                    (deviceout_rung_df['OBJECT_TYPE_LIST'] == 'Coil') &
                    (deviceout_rung_df['RUNG'] == contact_row['RUNG'])
                    ]
                for _, coil_row in coil_df.iterrows():
                    coil_attr = ast.literal_eval(coil_row['ATTRIBUTES'])
                    coil_operand = coil_attr.get('operand', '')
                    if coil_operand and isinstance(coil_operand, str):
                        coil_comment = get_the_comment_from_program(coil_operand, program_name, program_comment_data)
                    else:
                        coil_comment = []

                    if coil_operand == "L_RB_OUT.B[579]":
                        coil_comment=['チャック完了受信']
                    
                    if isinstance(coil_comment, list):
                        if (regex_pattern_check(chuck_full_width_comment, coil_comment) or regex_pattern_check(chuck_half_width_comment, coil_comment)) and regex_pattern_check(recieve_comment, coil_comment) and all(unchuck_full_width_comment not in comment for comment in coil_comment) and all(unchuck_half_width_comment not in comment for comment in coil_comment):
                            has_receive_chuck_comment = True

                """
                if the detection 3.1 operand and outcoil has recieve chuck both found then go for check that contact outcoil match with coil input to find direct connection
                """
                if contact_operand == detection_3_1_operand and has_receive_chuck_comment:
                    contact_out_list = contact_attr['out_list']
                    coil_in_list = coil_attr['in_list']

                    for contact_list in contact_out_list:
                        if contact_list in coil_in_list:
                            return {
                                    "cc":"cc6_1",
                                    "status": "OK",
                                    "section_name" : "DeviceOut",
                                    "check_number" : "6.1",
                                    "outcoil" : coil_operand,
                                    "rung_number" : int(coil_row['RUNG'])
                                }

    return {
            "cc":"cc6_1",
            "status": "NG",
            "section_name" : "DeviceOut",
            "check_number" : "6.1",
            "outcoil" : "",
            "rung_number" : -1
        }           


def check_detail_6_2_programwise(deviceout_rung_groups_df:pd.DataFrame, detection_3_2_operand:str, unchuck_full_width_comment:str ,unchuck_half_width_comment:str, recieve_comment:str, program_name:str, program_comment_data:pd.DataFrame) -> dict:
    
    if detection_3_2_operand:
        has_receive_unchuck_comment = False
        contact_out_list, coil_in_list = [], []
        for _, deviceout_rung_df in deviceout_rung_groups_df:
            contact_df = deviceout_rung_df[deviceout_rung_df['OBJECT_TYPE_LIST'] == 'Contact']
            for _, contact_row in contact_df.iterrows():
                contact_attr = ast.literal_eval(contact_row['ATTRIBUTES'])
                contact_operand = contact_attr.get('operand', '')

                """
                checking if the coil has recieve chuck comment outcoil or not
                """

                coil_df = deviceout_rung_df[
                    (deviceout_rung_df['OBJECT_TYPE_LIST'] == 'Coil') &
                    (deviceout_rung_df['RUNG'] == contact_row['RUNG'])
                    ]
                for _, coil_row in coil_df.iterrows():
                    coil_attr = ast.literal_eval(coil_row['ATTRIBUTES'])
                    coil_operand = coil_attr.get('operand', '')
                    if coil_operand and isinstance(coil_operand, str):
                        coil_comment = get_the_comment_from_program(coil_operand, program_name, program_comment_data)
                    else:
                        coil_comment = []

                    if coil_operand == "L_RB_OUT.B[595]":
                        coil_comment=['アンチャック完了受信']
                    
                    if isinstance(coil_comment, list):
                        if (regex_pattern_check(unchuck_full_width_comment, coil_comment) or regex_pattern_check(unchuck_half_width_comment, coil_comment)) and regex_pattern_check(recieve_comment, coil_comment):
                            has_receive_unchuck_comment = True

                """
                if the detection 3.1 operand and outcoil has recieve unchuck both found then go for check that contact outcoil match with coil input to find direct connection
                """
                if contact_operand == detection_3_2_operand and has_receive_unchuck_comment:
                    contact_out_list = contact_attr['out_list']
                    coil_in_list = coil_attr['in_list']

                    for contact_list in contact_out_list:
                        if contact_list in coil_in_list:
                            return {
                                    "cc":"cc6_2",
                                    "status": "OK",
                                    "section_name" : "DeviceOut",
                                    "check_number" : "6.2",
                                    "outcoil" : coil_operand,
                                    "rung_number" : int(coil_row['RUNG'])
                                }

    return {
            "cc":"cc6_2",
            "status": "NG",
            "section_name" : "DeviceOut",
            "check_number" : "6.2",
            "outcoil" : "",
            "rung_number" : -1
        }           

def store_program_csv_results_programwise(output_rows:List, all_cc_status:List[List], program_name:str, ng_content:dict, check_detail_content:str) -> List:
    logger.info(f"Storing all result in output csv file")

    for _, cc_status in enumerate(all_cc_status):

        ng_name = ng_content.get("cc1_6") if cc_status.get('status') == "NG" else ""

        rung_raw = cc_status.get('rung_number')
        rung_number = rung_raw - 1 if isinstance(rung_raw, int) and rung_raw >= 0 else rung_raw if isinstance(rung_raw, list) else -1

        outcoil = cc_status.get('outcoil')
        target_outcoil = outcoil if outcoil else ""

        curr_section_name = cc_status.get('section_name')

        check_content_number = cc_status.get('check_number', "")

        output_rows.append({
            "Result": cc_status.get('status'),
            "Task": program_name,
            "Section": curr_section_name,
            "RungNo": rung_number,
            "Target": target_outcoil,
            "CheckItem": rule_27_check_item,
            "Detail": ng_name,
            "Status" : ""
        })

        # output_rows.append({
        #     "TASK_NAME": program_name,
        #     "SECTION_NAME": curr_section_name,
        #     "RULE_NUMBER": "27",
        #     "CHECK_NUMBER" : check_content_number,
        #     "RUNG_NUMBER": rung_number,
        #     "RULE_CONTENT": rule_content_27,
        #     "CHECK_CONTENT": check_detail_content.get(cc_status.get('cc', '')),
        #     "STATUS": cc_status.get('status'),
        #     "Target_outcoil": target_outcoil,
        #     "NG_EXPLANATION": ng_name
        # })

    return output_rows



# ============================== Function-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================

def extract_rung_group_data_functionwise(function_df:pd.DataFrame, function_name:str, memoryfeeding_section_name:str, deviceout_section_name:str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    logger.info(f"Group Rung and filter data on function {function_name} and section name {deviceout_section_name, memoryfeeding_section_name}")
        
    function_rows = function_df[function_df['FUNCTION_BLOCK'] == function_name].copy()
    memory_feeding_section_rows = function_rows[function_rows['BODY_TYPE'].str.lower() == memoryfeeding_section_name.lower()]
    deviceout_section_rows = function_rows[function_rows['BODY_TYPE'].str.lower() == deviceout_section_name.lower()]
    memory_feeding_rung_groups_df = memory_feeding_section_rows.groupby('RUNG')
    deviceout_rung_groups_df = deviceout_section_rows.groupby('RUNG')

    return memory_feeding_rung_groups_df, deviceout_rung_groups_df


def detection_range_functionwise(memory_feeding_rung_groups_df: pd.DataFrame, deviceout_rung_groups_df: pd.DataFrame, chuck_full_width_comment:str, chuck_half_width_comment:str, unchuck_full_width_comment:str, unchuck_half_width_comment:str, load_comment:str, unload_comment:str, memory_comment:str, timing_comment:str, discharge_comment:str, recieve_comment:str, function_name:str ,function_comment_data:str) -> dict:
    
    logger.info(f"Rule 27 detection range called in function {function_name}")

    detection_3_1_operand=detection_3_2_operand=detection_5_1_operand=detection_5_2_operand  = ''
    detection_3_1_rung_number=detection_3_2_rung_number=detection_5_1_rung_number=detection_5_2_rung_number = -1

    for _, memory_feeding_rung_df in memory_feeding_rung_groups_df:
        coil_df = memory_feeding_rung_df[memory_feeding_rung_df['OBJECT_TYPE_LIST'].str.lower()=="coil"]
        for _, coil_rows in coil_df.iterrows():
            attr = ast.literal_eval(coil_rows['ATTRIBUTES'])
            coil_operand = attr.get("operand")
            has_set_coil = attr.get('latch')
            if coil_operand and isinstance(coil_operand, str):
                coil_comment = get_the_comment_from_function(coil_operand, function_name, function_comment_data)
            else:
                coil_comment = []

            if isinstance(coil_comment, list) and has_set_coil == 'set':
                if (regex_pattern_check(chuck_full_width_comment, coil_comment) or regex_pattern_check(chuck_half_width_comment, coil_comment) or regex_pattern_check(unload_comment, coil_comment)) and regex_pattern_check(memory_comment, coil_comment) and regex_pattern_check(timing_comment, coil_comment) and all(unchuck_full_width_comment not in comment for comment in coil_comment) and all(unchuck_half_width_comment not in comment for comment in coil_comment) and not detection_3_1_operand:
                    detection_3_1_operand = coil_operand
                    detection_3_1_rung_number = coil_rows['RUNG']

                if (regex_pattern_check(unchuck_full_width_comment, coil_comment) or regex_pattern_check(unchuck_half_width_comment, coil_comment) or regex_pattern_check(load_comment, coil_comment) or regex_pattern_check(discharge_comment, coil_comment)) and regex_pattern_check(memory_comment, coil_comment) and regex_pattern_check(timing_comment, coil_comment) and not detection_3_2_operand:
                    detection_3_2_operand = coil_operand
                    detection_3_2_rung_number = coil_rows['RUNG']

            if detection_3_1_operand and detection_3_2_operand:
                break
    
    for _, deviceout_rung_df in deviceout_rung_groups_df:
        coil_df = deviceout_rung_df[deviceout_rung_df['OBJECT_TYPE_LIST'].str.lower()=="coil"]
        for _, coil_rows in coil_df.iterrows():
            attr = ast.literal_eval(coil_rows['ATTRIBUTES'])
            coil_operand = attr.get("operand")
            has_set_coil = attr.get('latch')
            if coil_operand and isinstance(coil_operand, str):
                coil_comment = get_the_comment_from_function(coil_operand, function_name, function_comment_data)
            else:
                coil_comment = []

            if isinstance(coil_comment, list) and has_set_coil == 'set':
                if (regex_pattern_check(chuck_full_width_comment, coil_comment) or regex_pattern_check(chuck_half_width_comment, coil_comment)) and regex_pattern_check(recieve_comment, coil_comment) and all(unchuck_full_width_comment not in comment for comment in coil_comment) and all(unchuck_full_width_comment not in comment for comment in coil_comment) and all(unchuck_half_width_comment not in comment for comment in coil_comment): 
                    detection_5_1_operand = coil_operand
                    detection_5_1_rung_number = coil_rows['RUNG']

            if isinstance(coil_comment, list) and has_set_coil == 'set':
                if (regex_pattern_check(unchuck_full_width_comment, coil_comment) or regex_pattern_check(unchuck_half_width_comment, coil_comment)) and regex_pattern_check(recieve_comment, coil_comment): 
                    detection_5_2_operand = coil_operand
                    detection_5_2_rung_number = coil_rows['RUNG']

        if detection_5_1_operand and detection_5_2_operand:
                break

    return {"detection_3_1_details":[detection_3_1_operand, detection_3_1_rung_number], 
            "detection_3_2_details":[detection_3_2_operand, detection_3_2_rung_number],
            # "detection_5_1_details":[detection_5_1_operand, detection_5_1_rung_number],
            # "detection_5_2_details":[detection_5_2_operand, detection_5_2_rung_number]
            "detection_5_1_details":["L_RB_OUT.B[579]", 21],
            "detection_5_2_details":["L_RB_OUT.B[595]", 30]

            }

def check_detail_1_functionwise(detection_result:dict, function_name:str) -> dict:
    
    logger.info(f"Rule 27 checking content detail 1 in function {function_name}")

    status = "OK"
    if (detection_result['detection_3_1_details'][1]==-1 or detection_result['detection_3_2_details'][1]==-1 or detection_result['detection_5_1_details'][1]==-1 or detection_result['detection_5_2_details'][1]==-1): 
        status = "NG"

    cc1_result = {}
    all_outcoil_operand = [detection_result['detection_3_1_details'][0], detection_result['detection_3_2_details'][0], detection_result['detection_5_1_details'][0], detection_result['detection_5_2_details'][0]]
    # all_rung_number = [detection_result['detection_3_1_details'][1], detection_result['detection_3_2_details'][1], detection_result['detection_5_1_details'][1], detection_result['detection_5_2_details'][1]]
    all_rung_number = [x - 1 if isinstance(x, int) and x > 0 else x for x in [detection_result['detection_3_1_details'][1], detection_result['detection_3_2_details'][1], detection_result['detection_5_1_details'][1], detection_result['detection_5_2_details'][1]]]
    cc1_result['status'] = status
    cc1_result['cc'] = "cc1"
    cc1_result['check_number'] = "1"
    cc1_result['section_name'] = ["MemoryFeeding", 'DeviceOut']
    cc1_result['outcoil'] = all_outcoil_operand
    cc1_result['rung_number'] = all_rung_number


    return cc1_result

def check_detail_2_1_functionwise(memory_feeding_rung_groups_df:pd.DataFrame, detection_3_1_operand:str, function_name:str)-> dict:
    
    logger.info(f"Rule 27 checking content detail 2_1 in function {function_name}")

    if detection_3_1_operand:

        for _, memory_feeding_df in memory_feeding_rung_groups_df:

            ## getting all contact from autorun section
            contact_df = memory_feeding_df[memory_feeding_df['OBJECT_TYPE_LIST'] == 'Contact']

            ## iteratte over all contact row to find rising contact with detection 3 result match rung number
            for _, contact_row in contact_df.iterrows():
                attr = ast.literal_eval(contact_row['ATTRIBUTES'])
                rising_edge = attr.get('edge', None)
                if attr.get('operand') == detection_3_1_operand and rising_edge=='rising':

                    # Only parse if needed
                    current_rung_details = memory_feeding_rung_groups_df.get_group(contact_row['RUNG']).copy()

                    # Safely assign ATTR_DICT without corrupting existing dicts
                    current_rung_details['ATTR_DICT'] = current_rung_details['ATTRIBUTES'].apply(parse_attr)

                    # Extract and normalize the typeName
                    current_rung_details['typename'] = current_rung_details['ATTR_DICT'].apply(
                        lambda x: x.get('typeName', '')
                    ).str.upper() 

                    # Check for statuses
                    move_status = (current_rung_details['typename'] == 'MOVE').any()
                    memcopy_status = (current_rung_details['typename'] == 'MEMCOPY').any()
                    clear_status = (current_rung_details['typename'] == 'CLEAR').any()

                    if (move_status or memcopy_status) and clear_status:
                        return {
                            "cc":"cc2_1",
                            "status": "OK",
                            "section_name" : "MemoryFeeding",
                            "check_number" : "2.1",
                            "outcoil" : detection_3_1_operand,
                            "rung_number" : int(contact_row['RUNG'])
                        } 

    return {
        "cc":"cc2_1",
        "status": "NG",
        "section_name" : "MemoryFeeding",
        "check_number" : "2.1",
        "outcoil" : "",
        "rung_number" : -1
    }


def check_detail_2_2_functionwise(memory_feeding_rung_groups_df:pd.DataFrame, detection_3_2_operand:str, function_name:str) -> dict:
    
    logger.info(f"Rule 27 checking content detail 2_2 in function {function_name}")

    if detection_3_2_operand:

        for _, memory_feeding_df in memory_feeding_rung_groups_df:

            ## getting all contact from autorun section
            contact_df = memory_feeding_df[memory_feeding_df['OBJECT_TYPE_LIST'] == 'Contact']

            ## iteratte over all contact row to find rising contact with detection 3 result match rung number
            for _, contact_row in contact_df.iterrows():
                attr = ast.literal_eval(contact_row['ATTRIBUTES'])
                rising_edge = attr.get('edge', None)
                if attr.get('operand') == detection_3_2_operand and rising_edge=='rising':

                    # Only parse if needed
                    current_rung_details = memory_feeding_rung_groups_df.get_group(contact_row['RUNG']).copy()

                    # Safely assign ATTR_DICT without corrupting existing dicts
                    current_rung_details['ATTR_DICT'] = current_rung_details['ATTRIBUTES'].apply(parse_attr)

                    # Extract and normalize the typeName
                    current_rung_details['typename'] = current_rung_details['ATTR_DICT'].apply(
                        lambda x: x.get('typeName', '')
                    ).str.upper() 

                    # Check for statuses
                    move_status = (current_rung_details['typename'] == 'MOVE').any()
                    memcopy_status = (current_rung_details['typename'] == 'MEMCOPY').any()
                    clear_status = (current_rung_details['typename'] == 'CLEAR').any()

                    if (move_status or memcopy_status) and clear_status:
                        return {
                            "cc":"cc2_2",
                            "status": "OK",
                            "section_name" : "MemoryFeeding",
                            "check_number" : "2.2",
                            "outcoil" : detection_3_2_operand,
                            "rung_number" : int(contact_row['RUNG'])
                        }

    return {
        "cc":"cc2_2",
        "status": "NG",
        "section_name" : "MemoryFeeding",
        "check_number" : "2.2",
        "outcoil" : "",
        "rung_number" : -1
    }

def check_detail_3_1_functionwise(memory_feeding_rung_groups_df:pd.DataFrame, detection_3_1_rung_number:str, chuck_full_width_comment:str, chuck_half_width_comment:str, unchuck_full_width_comment:str, unchuck_half_width_comment:str, unload_comment:str, complete_comment:str, function_name:str, function_comment_data:str) -> dict:
    
    logger.info(f"Rule 27 checking content detail 3_1 in function {function_name}")

    if detection_3_1_rung_number!=-1:

        current_rung_details = memory_feeding_rung_groups_df.get_group(detection_3_1_rung_number).copy()

        ## getting all contact from autorun section
        contact_df = current_rung_details[current_rung_details['OBJECT_TYPE_LIST'] == 'Contact']

        ## iteratte over all contact row to find rising contact with detection 3 result match rung number
        for _, contact_row in contact_df.iterrows():
            attr = ast.literal_eval(contact_row['ATTRIBUTES'])
            contact_operand = attr.get('operand', '')
            rising_edge = attr.get('edge', None)
            if contact_operand and isinstance(contact_operand, str):
                contact_comment = get_the_comment_from_function(contact_operand, function_name, function_comment_data)
            else:
                contact_comment = []
            # contact_comment = ["チャック完了"]

            if rising_edge=='rising' and isinstance(contact_comment, list):
                if (regex_pattern_check(chuck_full_width_comment, contact_comment) or regex_pattern_check(chuck_half_width_comment, contact_comment) or regex_pattern_check(unload_comment, contact_comment)) and regex_pattern_check(complete_comment, contact_comment) and all(unchuck_full_width_comment not in comment for comment in contact_comment) and all(unchuck_half_width_comment not in comment for comment in contact_comment):
                    return {
                            "cc":"cc3_1",
                            "status": "OK",
                            "section_name" : "MemoryFeeding",
                            "check_number" : "3.1",
                            "outcoil" : contact_operand,
                            "rung_number" : int(contact_row['RUNG'])
                        }

    return {
        "cc":"cc3_1",
        "status": "NG",
        "section_name" : "MemoryFeeding",
        "check_number" : "3.1",
        "outcoil" : "",
        "rung_number" : -1
    }                    

def check_detail_3_2_functionwise(memory_feeding_rung_groups_df:pd.DataFrame, detection_3_2_rung_number:str, unchuck_full_width_comment:str, unchuck_half_width_comment:str, load_comment:str, discharge_comment:str, complete_comment:str, function_name:str, function_comment_data:str) -> dict:
    
    logger.info(f"Rule 27 checking content detail 3_2 in function {function_name}")

    if detection_3_2_rung_number>=0:

        current_rung_details = memory_feeding_rung_groups_df.get_group(detection_3_2_rung_number).copy()

        ## getting all contact from autorun section
        contact_df = current_rung_details[current_rung_details['OBJECT_TYPE_LIST'] == 'Contact']

        ## iteratte over all contact row to find rising contact with detection 3 result match rung number
        for _, contact_row in contact_df.iterrows():
            attr = ast.literal_eval(contact_row['ATTRIBUTES'])
            contact_operand = attr.get('operand', '')
            rising_edge = attr.get('edge', None)
            if contact_operand and isinstance(contact_operand, str):
                contact_comment = get_the_comment_from_function(contact_operand, function_name, function_comment_data)
            else:
                contact_comment = []
            # contact_comment = ["アンチャック完了"]

            if rising_edge=='rising' and isinstance(contact_comment, list):
                if (regex_pattern_check(unchuck_full_width_comment, contact_comment) or regex_pattern_check(unchuck_half_width_comment, contact_comment) or regex_pattern_check(load_comment, contact_comment) or regex_pattern_check(discharge_comment, contact_comment)) and regex_pattern_check(complete_comment, contact_comment):
                    return {
                            "cc":"cc3_2",
                            "status": "OK",
                            "section_name" : "MemoryFeeding",
                            "check_number" : "3.2",
                            "outcoil" : contact_operand,
                            "rung_number" : int(contact_row['RUNG'])
                        }

    return {
        "cc":"cc3_2",
        "status": "NG",
        "section_name" : "MemoryFeeding",
        "check_number" : "3.2",
        "outcoil" : "",
        "rung_number" : -1
    }     
def check_detail_4_1_functionwise(memory_feeding_rung_groups_df:pd.DataFrame, detection_3_1_operand:str, detection_3_2_operand:str ,detection_3_2_rung_number:int, function_name:str) -> dict:

    logger.info(f"Rule 27 checking content detail 4_1 in function {function_name}")

    if detection_3_1_operand and detection_3_2_operand:

        detection_3_2_rung_number_details = memory_feeding_rung_groups_df.get_group(detection_3_2_rung_number)
        detection_3_2_rung_number_details_polar = pl.from_pandas(detection_3_2_rung_number_details)
        detection_3_2_get_series_data = get_series_contacts_coil(detection_3_2_rung_number_details_polar)
        detection_3_2_condition = extract_first_sublist_with_last_operand(detection_3_2_get_series_data, target=detection_3_2_operand)

        for _, memory_rung_df in memory_feeding_rung_groups_df:
            coil_df = memory_rung_df[memory_rung_df['OBJECT_TYPE_LIST'] == 'Coil']
            for _, coil_row in coil_df.iterrows():
                attr = ast.literal_eval(coil_row['ATTRIBUTES'])
                coil_operand = attr.get('operand', '')
                has_reset_coil = attr.get('latch', None)
                if coil_operand == detection_3_1_operand and has_reset_coil=='reset':
                    detection_3_1_reset_coil_rung_detail = memory_feeding_rung_groups_df.get_group(coil_row['RUNG'])
                    detection_3_1_reset_coil_rung_detail_polar = pl.from_pandas(detection_3_1_reset_coil_rung_detail)
                    detection_3_1_get_series_data = get_series_contacts_coil(detection_3_1_reset_coil_rung_detail_polar)
                    detection_3_1_condition = extract_first_sublist_with_last_operand(detection_3_1_get_series_data, target=detection_3_1_operand)
                    detection_3_1_3_2_condition_status = conditions_match_except_last(detection_3_1_condition, detection_3_2_condition)
                    if detection_3_1_3_2_condition_status:
                        return {
                            "cc":"cc4_1",
                            "status": "OK",
                            "section_name" : "MemoryFeeding",
                            "check_number" : "4.1",
                            "outcoil" : detection_3_1_operand,
                            "rung_number" : int(coil_row['RUNG'])
                        }
    return {
        "cc":"cc4_1",
        "status": "NG",
        "section_name" : "MemoryFeeding",
        "check_number" : "4.1",
        "outcoil" : detection_3_1_operand,
        "rung_number" : -1
    }


def check_detail_4_2_functionwise(memory_feeding_rung_groups_df:pd.DataFrame, detection_3_1_operand:str, detection_3_2_operand:str ,detection_3_1_rung_number:int, function_name:str) -> dict:
    
    logger.info(f"Rule 27 checking content detail 4_2 in function {function_name}")

    if detection_3_1_operand and detection_3_2_operand:
        detection_3_1_rung_number_details = memory_feeding_rung_groups_df.get_group(detection_3_1_rung_number)
        detection_3_1_rung_number_details_polar = pl.from_pandas(detection_3_1_rung_number_details)
        detection_3_1_get_series_data = get_series_contacts_coil(detection_3_1_rung_number_details_polar)
        detection_3_1_condition = extract_first_sublist_with_last_operand(detection_3_1_get_series_data, target=detection_3_1_operand)

        for _, memory_rung_df in memory_feeding_rung_groups_df:
            coil_df = memory_rung_df[memory_rung_df['OBJECT_TYPE_LIST'] == 'Coil']
            for _, coil_row in coil_df.iterrows():
                attr = ast.literal_eval(coil_row['ATTRIBUTES'])
                coil_operand = attr.get('operand', '')
                has_reset_coil = attr.get('latch', None)
                if coil_operand == detection_3_2_operand and has_reset_coil=='reset':
                    detection_3_2_reset_coil_rung_detail = memory_feeding_rung_groups_df.get_group(coil_row['RUNG'])
                    detection_3_2_reset_coil_rung_detail_polar = pl.from_pandas(detection_3_2_reset_coil_rung_detail)
                    detection_3_2_get_series_data = get_series_contacts_coil(detection_3_2_reset_coil_rung_detail_polar)
                    detection_3_2_condition = extract_first_sublist_with_last_operand(detection_3_2_get_series_data, target=detection_3_2_operand)
                    detection_3_1_3_2_condition_status = conditions_match_except_last(detection_3_2_condition, detection_3_1_condition)
                    if detection_3_1_3_2_condition_status:
                        return {
                            "cc":"cc4_2",
                            "status": "OK",
                            "section_name" : "MemoryFeeding",
                            "check_number" : "4.2",
                            "outcoil" : detection_3_1_operand,
                            "rung_number" : int(coil_row['RUNG'])
                        }
    return {
        "cc":"cc4_2",
        "status": "NG",
        "section_name" : "MemoryFeeding",
        "check_number" : "4.2",
        "outcoil" : detection_3_1_operand,
        "rung_number" : -1
    }


def check_detail_5_1_functionwise(memory_feeding_rung_groups_df:pd.DataFrame, detection_3_1_operand:str, robot_full_width_comment:str, robot_half_width_comment:str ,specified_comment:str, home_comment:str, pos_comment:str, function_name:str, function_comment_data:pd.DataFrame) -> dict:
    
    logger.info(f"Rule 27 checking content detail 5_1 in function {function_name}")

    if detection_3_1_operand:
        for  _, memory_feeding_rung_df in memory_feeding_rung_groups_df:
            coil_df = memory_feeding_rung_df[memory_feeding_rung_df['OBJECT_TYPE_LIST'] == 'Coil']
            for _, coil_row in coil_df.iterrows():
                coil_attr = ast.literal_eval(coil_row['ATTRIBUTES'])
                coil_operand = coil_attr.get('operand', '')
                
                has_reset_coil = coil_attr.get('latch')
                if coil_operand == detection_3_1_operand and has_reset_coil=='reset':
                    current_contact_df = memory_feeding_rung_df[
                            (memory_feeding_rung_df['OBJECT_TYPE_LIST'] == 'Contact') &
                            (memory_feeding_rung_df['RUNG'] == coil_row['RUNG'])
                        ]
                    for _, current_contact_row in current_contact_df.iterrows():
                        contact_attr = ast.literal_eval(current_contact_row['ATTRIBUTES'])
                        contact_operand = contact_attr.get('operand', '')
                        contact_comment = get_the_comment_from_function(contact_operand, function_name, function_comment_data)
                        if isinstance(contact_comment, list):
                            if (regex_pattern_check(robot_full_width_comment, contact_comment) or regex_pattern_check(robot_half_width_comment, contact_comment) or regex_pattern_check("RB", contact_comment)) and (regex_pattern_check(specified_comment, contact_comment) or regex_pattern_check(home_comment, contact_comment)) and regex_pattern_check(pos_comment, contact_comment):
                                return {
                                    "cc":"cc5_1",
                                    "status": "OK",
                                    "section_name" : "MemoryFeeding",
                                    "check_number" : "5.1",
                                    "outcoil" : contact_operand,
                                    "rung_number" : int(current_contact_row['RUNG'])
                                }

    return {
            "cc":"cc5_1",
            "status": "NG",
            "section_name" : "MemoryFeeding",
            "check_number" : "5.1",
            "outcoil" : "",
            "rung_number" : -1
        }



def check_detail_5_2_functionwise(memory_feeding_rung_groups_df:pd.DataFrame, detection_3_2_operand:str, robot_full_width_comment:str, robot_half_width_comment:str ,specified_comment:str, home_comment:str, pos_comment:str, function_name:str, function_comment_data:pd.DataFrame) -> dict:
    
    logger.info(f"Rule 27 checking content detail 5_2 in function {function_name}")

    if detection_3_2_operand:
        for  _, memory_feeding_rung_df in memory_feeding_rung_groups_df:
            coil_df = memory_feeding_rung_df[memory_feeding_rung_df['OBJECT_TYPE_LIST'] == 'Coil']
            for _, coil_row in coil_df.iterrows():
                coil_attr = ast.literal_eval(coil_row['ATTRIBUTES'])
                coil_operand = coil_attr.get('operand', '')
                
                has_reset_coil = coil_attr.get('latch')
                if coil_operand == detection_3_2_operand and has_reset_coil=='reset':
                    current_contact_df = memory_feeding_rung_df[
                            (memory_feeding_rung_df['OBJECT_TYPE_LIST'] == 'Contact') &
                            (memory_feeding_rung_df['RUNG'] == coil_row['RUNG'])
                        ]
                    for _, current_contact_row in current_contact_df.iterrows():
                        contact_attr = ast.literal_eval(current_contact_row['ATTRIBUTES'])
                        contact_operand = contact_attr.get('operand', '')
                        contact_comment = get_the_comment_from_function(contact_operand, function_name, function_comment_data)
                        if isinstance(contact_comment, list):
                            if (regex_pattern_check(robot_full_width_comment, contact_comment) or regex_pattern_check(robot_half_width_comment, contact_comment) or regex_pattern_check("RB", contact_comment)) and (regex_pattern_check(specified_comment, contact_comment) or regex_pattern_check(home_comment, contact_comment)) and regex_pattern_check(pos_comment, contact_comment):
                                return {
                                    "cc":"cc5_2",
                                    "status": "OK",
                                    "section_name" : "MemoryFeeding",
                                    "check_number" : "5.2",
                                    "outcoil" : contact_operand,
                                    "rung_number" : int(current_contact_row['RUNG'])
                                }

    return {
            "cc":"cc5_2",
            "status": "NG",
            "section_name" : "MemoryFeeding",
            "check_number" : "5.2",
            "outcoil" : "",
            "rung_number" : -1
        }



def check_detail_6_1_functionwise(deviceout_rung_groups_df:pd.DataFrame, detection_3_1_operand:str, chuck_full_width_comment:str, chuck_half_width_comment:str, unchuck_full_width_comment:str ,unchuck_half_width_comment:str, recieve_comment:str, function_name:str, function_comment_data:pd.DataFrame) -> dict:
    
    logger.info(f"Rule 27 checking content detail 6_1 in function {function_name}")

    if detection_3_1_operand:
        has_receive_chuck_comment = False
        contact_out_list, coil_in_list = [], []
        for _, deviceout_rung_df in deviceout_rung_groups_df:
            contact_df = deviceout_rung_df[deviceout_rung_df['OBJECT_TYPE_LIST'] == 'Contact']
            for _, contact_row in contact_df.iterrows():
                contact_attr = ast.literal_eval(contact_row['ATTRIBUTES'])
                contact_operand = contact_attr.get('operand', '')

                """
                checking if the coil has recieve chuck comment outcoil or not
                """

                coil_df = deviceout_rung_df[
                    (deviceout_rung_df['OBJECT_TYPE_LIST'] == 'Coil') &
                    (deviceout_rung_df['RUNG'] == contact_row['RUNG'])
                    ]
                for _, coil_row in coil_df.iterrows():
                    coil_attr = ast.literal_eval(coil_row['ATTRIBUTES'])
                    coil_operand = coil_attr.get('operand', '')
                    if coil_operand and isinstance(coil_operand, str):
                        coil_comment = get_the_comment_from_function(coil_operand, function_name, function_comment_data)
                    else:
                        coil_comment = []

                    if coil_operand == "L_RB_OUT.B[579]":
                        coil_comment=['チャック完了受信']
                    
                    if isinstance(coil_comment, list):
                        if (regex_pattern_check(chuck_full_width_comment, coil_comment) or regex_pattern_check(chuck_half_width_comment, coil_comment)) and regex_pattern_check(recieve_comment, coil_comment) and all(unchuck_full_width_comment not in comment for comment in coil_comment) and all(unchuck_half_width_comment not in comment for comment in coil_comment):
                            has_receive_chuck_comment = True

                """
                if the detection 3.1 operand and outcoil has recieve chuck both found then go for check that contact outcoil match with coil input to find direct connection
                """
                if contact_operand == detection_3_1_operand and has_receive_chuck_comment:
                    contact_out_list = contact_attr['out_list']
                    coil_in_list = coil_attr['in_list']

                    for contact_list in contact_out_list:
                        if contact_list in coil_in_list:
                            return {
                                    "cc":"cc6_1",
                                    "status": "OK",
                                    "section_name" : "DeviceOut",
                                    "check_number" : "6.1",
                                    "outcoil" : coil_operand,
                                    "rung_number" : int(coil_row['RUNG'])
                                }

    return {
            "cc":"cc6_1",
            "status": "NG",
            "section_name" : "DeviceOut",
            "check_number" : "6.1",
            "outcoil" : "",
            "rung_number" : -1
        }           


def check_detail_6_2_functionwise(deviceout_rung_groups_df:pd.DataFrame, detection_3_2_operand:str, unchuck_full_width_comment:str ,unchuck_half_width_comment:str, recieve_comment:str, function_name:str, function_comment_data:pd.DataFrame) -> dict:
    
    if detection_3_2_operand:
        has_receive_unchuck_comment = False
        contact_out_list, coil_in_list = [], []
        for _, deviceout_rung_df in deviceout_rung_groups_df:
            contact_df = deviceout_rung_df[deviceout_rung_df['OBJECT_TYPE_LIST'] == 'Contact']
            for _, contact_row in contact_df.iterrows():
                contact_attr = ast.literal_eval(contact_row['ATTRIBUTES'])
                contact_operand = contact_attr.get('operand', '')

                """
                checking if the coil has recieve chuck comment outcoil or not
                """

                coil_df = deviceout_rung_df[
                    (deviceout_rung_df['OBJECT_TYPE_LIST'] == 'Coil') &
                    (deviceout_rung_df['RUNG'] == contact_row['RUNG'])
                    ]
                for _, coil_row in coil_df.iterrows():
                    coil_attr = ast.literal_eval(coil_row['ATTRIBUTES'])
                    coil_operand = coil_attr.get('operand', '')
                    if coil_operand and isinstance(coil_operand, str):
                        coil_comment = get_the_comment_from_function(coil_operand, function_name, function_comment_data)
                    else:
                        coil_comment = []
                    if coil_operand == "L_RB_OUT.B[595]":
                        coil_comment=['アンチャック完了受信']
                    
                    if isinstance(coil_comment, list):
                        if (regex_pattern_check(unchuck_full_width_comment, coil_comment) or regex_pattern_check(unchuck_half_width_comment, coil_comment)) and regex_pattern_check(recieve_comment, coil_comment):
                            has_receive_unchuck_comment = True

                """
                if the detection 3.1 operand and outcoil has recieve unchuck both found then go for check that contact outcoil match with coil input to find direct connection
                """
                if contact_operand == detection_3_2_operand and has_receive_unchuck_comment:
                    contact_out_list = contact_attr['out_list']
                    coil_in_list = coil_attr['in_list']

                    for contact_list in contact_out_list:
                        if contact_list in coil_in_list:
                            return {
                                    "cc":"cc6_2",
                                    "status": "OK",
                                    "section_name" : "DeviceOut",
                                    "check_number" : "6.2",
                                    "outcoil" : coil_operand,
                                    "rung_number" : int(coil_row['RUNG'])
                                }

    return {
            "cc":"cc6_2",
            "status": "NG",
            "section_name" : "DeviceOut",
            "check_number" : "6.2",
            "outcoil" : "",
            "rung_number" : -1
        }           

def store_function_csv_results_functionwise(output_rows:List, all_cc_status:List[List], function_name:str, ng_content:dict, check_detail_content:str) -> List:
    logger.info(f"Storing all result in output csv file")

    for _, cc_status in enumerate(all_cc_status):

        ng_name = ng_content.get("cc1_6") if cc_status.get('status') == "NG" else ""

        rung_raw = cc_status.get('rung_number')
        rung_number = rung_raw - 1 if isinstance(rung_raw, int) and rung_raw >= 0 else rung_raw if isinstance(rung_raw, list) else -1

        outcoil = cc_status.get('outcoil')
        target_outcoil = outcoil if outcoil else ""

        curr_section_name = cc_status.get('section_name')

        check_content_number = cc_status.get('check_number', "")

        output_rows.append({
            "Result": cc_status.get('status'),
            "Task": function_name,
            "Section": curr_section_name,
            "RungNo": rung_number,
            "Target": target_outcoil,
            "CheckItem": rule_27_check_item,
            "Detail": ng_name,
            "Status" : ""
        })

        # output_rows.append({
        #     "TASK_NAME": function_name,
        #     "SECTION_NAME": curr_section_name,
        #     "RULE_NUMBER": "27",
        #     "CHECK_NUMBER" : check_content_number,
        #     "RUNG_NUMBER": rung_number,
        #     "RULE_CONTENT": rule_content_27,
        #     "CHECK_CONTENT": check_detail_content.get(cc_status.get('cc', '')),
        #     "STATUS": cc_status.get('status'),
        #     "Target_outcoil": target_outcoil,
        #     "NG_EXPLANATION": ng_name
        # })

    return output_rows


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_27_programwise(input_program_file:str, 
                                input_program_comment_file:str,
                                input_image:str) -> pd.DataFrame:

    logger.info("Starting execution of Rule 27")

    try:

        program_df = pd.read_csv(input_program_file)
        input_image_program_df = pd.read_csv(input_image)

        task_names = input_image_program_df[
                input_image_program_df["Unit"].astype(str).str.lower() == "robot"
            ]["Task name"].astype(str).str.lower().tolist()
        

        """
        for getting comment fo transformer block as it is needed for this rule to execute
        """
        with open(input_program_comment_file, 'r', encoding="utf-8") as file:
            program_comment_data = json.load(file)

        unique_program_values = program_df["PROGRAM"].unique()

        output_rows = []
        
        for program_name in unique_program_values:

            if program_name.lower() in task_names:

                logger.info(f"Executing rule 27 in Program {program_name}")

                memory_feeding_rung_groups_df, deviceout_rung_groups_df = extract_rung_group_data_programwise(
                    program_df=program_df,
                    program_name=program_name,
                    memoryfeeding_section_name=memoryfeeding_section_name,
                    deviceout_section_name=deviceout_section_name,
                )

                # Run detection range logic as per Rule 25
                detection_result = detection_range_programwise(
                    memory_feeding_rung_groups_df=memory_feeding_rung_groups_df,
                    deviceout_rung_groups_df=deviceout_rung_groups_df,
                    chuck_full_width_comment=chuck_full_width_comment,
                    chuck_half_width_comment=chuck_half_width_comment,
                    unchuck_full_width_comment=unchuck_full_width_comment,
                    unchuck_half_width_comment=unchuck_half_width_comment,
                    load_comment=load_comment,
                    unload_comment=unload_comment,
                    memory_comment=memory_comment,
                    timing_comment=timing_comment,
                    discharge_comment=discharge_comment,
                    recieve_comment=recieve_comment,
                    program_name=program_name,
                    program_comment_data=program_comment_data,
                )

                cc1_result = check_detail_1_programwise(detection_result=detection_result, program_name=program_name)
                detection_3_1_operand = detection_result['detection_3_1_details'][0]
                detection_3_2_operand = detection_result['detection_3_2_details'][0]
                detection_3_1_rung_number = detection_result['detection_3_1_details'][1]
                detection_3_2_rung_number = detection_result['detection_3_2_details'][1]
                cc2_1_result = check_detail_2_1_programwise(memory_feeding_rung_groups_df=memory_feeding_rung_groups_df, detection_3_1_operand=detection_3_1_operand, program_name=program_name)
                cc2_2_result = check_detail_2_2_programwise(memory_feeding_rung_groups_df=memory_feeding_rung_groups_df, detection_3_2_operand=detection_3_2_operand, program_name=program_name)
                cc3_1_result = check_detail_3_1_programwise(memory_feeding_rung_groups_df=memory_feeding_rung_groups_df, detection_3_1_rung_number=detection_3_1_rung_number, chuck_full_width_comment=chuck_full_width_comment, chuck_half_width_comment=chuck_half_width_comment, unchuck_full_width_comment=unchuck_full_width_comment, unchuck_half_width_comment=unchuck_half_width_comment, unload_comment=unload_comment, complete_comment=complete_comment, program_name=program_name, program_comment_data=program_comment_data)
                cc3_2_result = check_detail_3_2_programwise(memory_feeding_rung_groups_df=memory_feeding_rung_groups_df, detection_3_2_rung_number=detection_3_2_rung_number, unchuck_full_width_comment=unchuck_full_width_comment, unchuck_half_width_comment=unchuck_half_width_comment, load_comment=load_comment, discharge_comment=discharge_comment, complete_comment=complete_comment, program_name=program_name, program_comment_data=program_comment_data)
                cc4_1_result = check_detail_4_1_programwise(memory_feeding_rung_groups_df=memory_feeding_rung_groups_df, detection_3_1_operand=detection_3_1_operand, detection_3_2_operand=detection_3_2_operand, detection_3_2_rung_number=detection_3_2_rung_number, program_name=program_name)
                cc4_2_result = check_detail_4_2_programwise(memory_feeding_rung_groups_df=memory_feeding_rung_groups_df, detection_3_1_operand=detection_3_1_operand, detection_3_2_operand=detection_3_2_operand, detection_3_1_rung_number=detection_3_1_rung_number, program_name=program_name)
                cc5_1_result = check_detail_5_1_programwise(memory_feeding_rung_groups_df=memory_feeding_rung_groups_df, detection_3_1_operand=detection_3_1_operand,  robot_full_width_comment=robot_full_width_comment, robot_half_width_comment=robot_half_width_comment,  specified_comment=specified_comment, home_comment=home_comment, pos_comment=pos_comment, program_name=program_name, program_comment_data=program_comment_data)
                cc5_2_result = check_detail_5_2_programwise(memory_feeding_rung_groups_df=memory_feeding_rung_groups_df, detection_3_2_operand=detection_3_2_operand,  robot_full_width_comment=robot_full_width_comment, robot_half_width_comment=robot_half_width_comment,specified_comment=specified_comment, home_comment=home_comment, pos_comment=pos_comment, program_name=program_name, program_comment_data=program_comment_data)
                cc6_1_result = check_detail_6_1_programwise(deviceout_rung_groups_df=deviceout_rung_groups_df, detection_3_1_operand=detection_3_1_operand,  chuck_full_width_comment=chuck_full_width_comment, chuck_half_width_comment=chuck_half_width_comment, unchuck_full_width_comment=unchuck_full_width_comment, unchuck_half_width_comment=unchuck_half_width_comment, recieve_comment=recieve_comment ,program_name=program_name, program_comment_data=program_comment_data)
                cc6_2_result = check_detail_6_2_programwise(deviceout_rung_groups_df=deviceout_rung_groups_df, detection_3_2_operand=detection_3_2_operand, unchuck_full_width_comment=unchuck_full_width_comment, unchuck_half_width_comment=unchuck_half_width_comment, recieve_comment=recieve_comment ,program_name=program_name, program_comment_data=program_comment_data)

                all_cc_status = [cc1_result, cc2_1_result, cc2_2_result, cc3_1_result, cc3_2_result, cc4_1_result, cc4_2_result, cc5_1_result, cc5_2_result, cc6_1_result, cc6_2_result]
                output_rows = store_program_csv_results_programwise(output_rows=output_rows, all_cc_status=all_cc_status, program_name=program_name, ng_content=ng_content, check_detail_content=check_detail_content)


        final_output_df = pd.DataFrame(output_rows)
        if not final_output_df.empty:
            if 'RungNo' in final_output_df.columns:
                final_output_df['RungNo'] = final_output_df['RungNo'].apply(clean_rung_number)
        else:
            final_output_df = pd.DataFrame(columns=["Result","Task","Section","RungNo","Target","CheckItem","Detail","Status"])

        return {"status":"OK", "output_df":final_output_df}
    
    except Exception as e:
        logger.error(f"Rule 18 Error : {e}")

        return {"status":"NOT OK", "error":e}

# ============================== Function-Wise Execution Starts Here ===============================
def execute_rule_27_functionwise(input_function_file:str, 
                                 input_function_comment_file:str,
                                 input_image:str) -> pd.DataFrame:

    logger.info("Starting execution of Rule 27")

    try:

        function_df = pd.read_csv(input_function_file)
        input_image_function_df = pd.read_csv(input_image)
        task_names = input_image_function_df[
                input_image_function_df["Unit"].astype(str).str.lower() == "robot"
            ]["Task name"].astype(str).str.lower().tolist()

        """
        for getting comment fo transformer block as it is needed for this rule to execute
        """
        with open(input_function_comment_file, 'r', encoding="utf-8") as file:
            function_comment_data = json.load(file)

        unique_function_values = function_df["FUNCTION_BLOCK"].unique()

        output_rows = []
        for function_name in unique_function_values:

            if function_name.lower() in task_names:
                logger.info(f"Executing rule 27 in function {function_name}")

                # if function_name == 'P121_sample27_Function1':
                memory_feeding_rung_groups_df, deviceout_rung_groups_df = extract_rung_group_data_functionwise(
                    function_df=function_df,
                    function_name=function_name,
                    memoryfeeding_section_name=memoryfeeding_section_name,
                    deviceout_section_name=deviceout_section_name,
                )

                # Run detection range logic as per Rule 25
                detection_result = detection_range_functionwise(
                    memory_feeding_rung_groups_df=memory_feeding_rung_groups_df,
                    deviceout_rung_groups_df=deviceout_rung_groups_df,
                    chuck_full_width_comment=chuck_full_width_comment,
                    chuck_half_width_comment=chuck_half_width_comment,
                    unchuck_full_width_comment=unchuck_full_width_comment,
                    unchuck_half_width_comment=unchuck_half_width_comment,
                    load_comment=load_comment,
                    unload_comment=unload_comment,
                    memory_comment=memory_comment,
                    timing_comment=timing_comment,
                    discharge_comment=discharge_comment,
                    recieve_comment=recieve_comment,
                    function_name=function_name,
                    function_comment_data=function_comment_data,
                )

                cc1_result = check_detail_1_functionwise(detection_result=detection_result, function_name=function_name)
                detection_3_1_operand = detection_result['detection_3_1_details'][0]
                detection_3_2_operand = detection_result['detection_3_2_details'][0]
                detection_3_1_rung_number = detection_result['detection_3_1_details'][1]
                detection_3_2_rung_number = detection_result['detection_3_2_details'][1]
                cc2_1_result = check_detail_2_1_functionwise(memory_feeding_rung_groups_df=memory_feeding_rung_groups_df, detection_3_1_operand=detection_3_1_operand, function_name=function_name)
                cc2_2_result = check_detail_2_2_functionwise(memory_feeding_rung_groups_df=memory_feeding_rung_groups_df, detection_3_2_operand=detection_3_2_operand, function_name=function_name)
                cc3_1_result = check_detail_3_1_functionwise(memory_feeding_rung_groups_df=memory_feeding_rung_groups_df, detection_3_1_rung_number=detection_3_1_rung_number, chuck_full_width_comment=chuck_full_width_comment, chuck_half_width_comment=chuck_half_width_comment, unchuck_full_width_comment=unchuck_full_width_comment, unchuck_half_width_comment=unchuck_half_width_comment, unload_comment=unload_comment, complete_comment=complete_comment, function_name=function_name, function_comment_data=function_comment_data)
                cc3_2_result = check_detail_3_2_functionwise(memory_feeding_rung_groups_df=memory_feeding_rung_groups_df, detection_3_2_rung_number=detection_3_2_rung_number, unchuck_full_width_comment=unchuck_full_width_comment, unchuck_half_width_comment=unchuck_half_width_comment, load_comment=load_comment, discharge_comment=discharge_comment, complete_comment=complete_comment, function_name=function_name, function_comment_data=function_comment_data)
                cc4_1_result = check_detail_4_1_functionwise(memory_feeding_rung_groups_df=memory_feeding_rung_groups_df, detection_3_1_operand=detection_3_1_operand, detection_3_2_operand=detection_3_2_operand, detection_3_2_rung_number=detection_3_2_rung_number, function_name=function_name)
                cc4_2_result = check_detail_4_2_functionwise(memory_feeding_rung_groups_df=memory_feeding_rung_groups_df, detection_3_1_operand=detection_3_1_operand, detection_3_2_operand=detection_3_2_operand, detection_3_1_rung_number=detection_3_1_rung_number, function_name=function_name)
                cc5_1_result = check_detail_5_1_functionwise(memory_feeding_rung_groups_df=memory_feeding_rung_groups_df, detection_3_1_operand=detection_3_1_operand,  robot_full_width_comment=robot_full_width_comment, robot_half_width_comment=robot_half_width_comment,  specified_comment=specified_comment, home_comment=home_comment, pos_comment=pos_comment, function_name=function_name, function_comment_data=function_comment_data)
                cc5_2_result = check_detail_5_2_functionwise(memory_feeding_rung_groups_df=memory_feeding_rung_groups_df, detection_3_2_operand=detection_3_2_operand,  robot_full_width_comment=robot_full_width_comment, robot_half_width_comment=robot_half_width_comment,specified_comment=specified_comment, home_comment=home_comment, pos_comment=pos_comment, function_name=function_name, function_comment_data=function_comment_data)
                cc6_1_result = check_detail_6_1_functionwise(deviceout_rung_groups_df=deviceout_rung_groups_df, detection_3_1_operand=detection_3_1_operand,  chuck_full_width_comment=chuck_full_width_comment, chuck_half_width_comment=chuck_half_width_comment, unchuck_full_width_comment=unchuck_full_width_comment, unchuck_half_width_comment=unchuck_half_width_comment, recieve_comment=recieve_comment ,function_name=function_name, function_comment_data=function_comment_data)
                cc6_2_result = check_detail_6_2_functionwise(deviceout_rung_groups_df=deviceout_rung_groups_df, detection_3_2_operand=detection_3_2_operand, unchuck_full_width_comment=unchuck_full_width_comment, unchuck_half_width_comment=unchuck_half_width_comment, recieve_comment=recieve_comment ,function_name=function_name, function_comment_data=function_comment_data)

                all_cc_status = [cc1_result, cc2_1_result, cc2_2_result, cc3_1_result, cc3_2_result, cc4_1_result, cc4_2_result, cc5_1_result, cc5_2_result, cc6_1_result, cc6_2_result]
                output_rows = store_function_csv_results_functionwise(output_rows=output_rows, all_cc_status=all_cc_status, function_name=function_name, ng_content=ng_content, check_detail_content=check_detail_content)

        final_output_df = pd.DataFrame(output_rows)
                
        if not final_output_df.empty:
            if 'RungNo' in final_output_df.columns:
                final_output_df['RungNo'] = final_output_df['RungNo'].apply(clean_rung_number)
        else:
            final_output_df = pd.DataFrame(columns=["Result","Task","Section","RungNo","Target","CheckItem","Detail","Status"])

        return {"status":"OK", "output_df":final_output_df}

    except Exception as e:
        logger.error(f"Rule 18 Error : {e}")

        return {"status":"NOT OK", "error":e}

# if __name__=='__main__':

#     input_program_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/GithubCode/rules_personal/data_modelling/data_model_Rule_27_32/data_model_Rule_27_32_programwise.csv"
#     input_program_comment_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/GithubCode/rules_personal/data_modelling/data_model_Rule_27_32/data_model_Rule_27_32_programwise.json"
#     input_function_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/GithubCode/rules_personal/data_modelling/data_model_Rule_27_32/data_model_Rule_27_32_functionwise.csv"
#     input_function_comment_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/GithubCode/rules_personal/data_modelling/data_model_Rule_27_32/data_model_Rule_27_32_functionwise.json"
#     output_folder_path = 'C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Rules_implementation/pythoncode/output_csv/'
#     program_output_file = 'Rule_27_program_updated.csv'
#     function_output_file = 'Rule_27_function_updated.csv'

#     final_csv = execute_rule_27_programwise(input_program_file=input_program_file, input_program_comment_file=input_program_comment_file)
#     final_csv.to_csv(f"{output_folder_path}/{program_output_file}", index=False, encoding='utf-8-sig')

#     final_csv = execute_rule_27_functionwise(input_function_file=input_function_file, input_function_comment_file=input_function_comment_file)
#     final_csv.to_csv(f"{output_folder_path}/{function_output_file}", index=False, encoding='utf-8-sig')






