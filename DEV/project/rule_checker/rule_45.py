import ast
import json
import pandas as pd
from typing import *
import re
from ...main import logger
import polars as pl
import pprint
from rich import print as rprint
from rich.pretty import Pretty
from .extract_comment_from_variable import get_the_comment_from_function, get_the_comment_from_program
from .ladder_utils import regex_pattern_check, clean_rung_number
from .rule_45_self_holding import check_self_holding, get_series_contacts
# from rule_27_ladder_utils import get_block_connections, get_comments_from_datasource, get_series_contacts_coil

# ============================================ Comments referenced in Rule 25 processing ============================================
total_comment = 'まとめ'
emergency_stop_comment = '緊急停止'
independent_device_comment = '独立制御機器'
automatic_stop_comment = '自動停止'
cycle_stop_comment = "ｻｲｸﾙ停止" 
fault_stop_comment = '異常停止'
warning_comment = '警報'
operation_fault_comment = '動作異常'
autorun_comment = "自動運転"
condition_comment = "条件"

# ============================ Rule 25: Definitions, Content, and Configuration Details ============================
fault_section_name = 'fault'
deviceout_section_name = 'deviceout'
automain_section_name = 'automain'
mdout_section_name = 'md_out'

rule_content_45 = "・Combine anomalies into one relay for each equipment outage category. Relays that are combined into one should be “OFF” and abnormal."

check_detail_content = {
    "cc1":"Fault' section without any of ②,④,⑤,⑥, or ⑦ is NG.", 
    "cc2":"Check that there is no outcoil including “AL ” in the variable name between rung number of rung 0 to rung ② in the section of ①.", 
    "cc3":"Check that there is no outcoil with “AL ” in the variable name between rung ⑩ and last rung of section in section ①.", 
    "cc4":"Check that the rung numbers from ② to ⑨ are in decreasing numerical order (②<③<④<⑤<⑥<⑦<⑧<⑨).", 
    "cc5":"Fault' section without any of ⑩,⑪,⑫,⑬, or ⑭ is NG.", 
    "cc6":" Check that the rung numbers from ⑩ to ⑭ are in decreasing numerical order (⑩<⑪<⑫<⑬<⑭).", 
    "cc7":"Check that all out coils that exist within the range of ②~⑤ among the coils with “AL” in the variable name detected in STEP1 satisfy the following two points.", 
    "cc8":"Check that all the out coils in the range of ⑥~⑨ among the coils with 'AL' in the variable name detected in STEP1 are not self-holding.", 
    "cc9":"Check whether the coil whose variable name contains “AL” detected in STEP 1 is connected in series with the B contact at the correct location (*1).", 
    "cc10":"For the out-coil detected in STEP 2, check whether the “TOTAL COIL” condition detected in step ⑯ is connected in series with the A contact of any other detected out-coil. If only a “TOTAL COIL” exists, the condition is unconditionally OK.",
    "cc11":" Check if all the “TOTAL COIL” A contacts detected in step ⑭ are connected in series to the out coil detected in step ⑮. However, the  ⑭“警報(warning)” contact is OK even if it is not present.",
    "cc12":"Check if all variables (MD_Out COlL) detected in STEP3 are connected in series with A contact to the correct part (*2) of the task containing “Main”.",
    "cc13":"Among the “TOTAL COIL” extracted in A, check that the 'TOTAL COIL' that fall under ⑩(”緊急停止(Emergency stop)“) and do not include circuit comments for ”独立制御機器(Independent device) satisfy all of the following conditions. ",
    }

ng_content = {
    "cc1":"Fault回路が標準通りに作られていない(異常回路の抜け・漏れ)可能性あり(Fault circuit may not be built to standard (abnormal circuit missing or leaking))",
    "cc2":"アラームが停止区分の外にあるため,正しく処理されない可能性有(Alarm may not be handled correctly because it is outside of the stop category.)",
    "cc3":"アラームが停止区分の外にあるため,正しく処理されない可能性有(Alarm may not be handled correctly because it is outside of the stop category.)",
    "cc4":"プログラムが異常の優先度が高い順に組まれていない可能性有(Program may not be organized in order of priority of anomalies.)",
    "cc5":"Faultのまとめ回路が標準通りに作られていない(異常回路の抜け・漏れ)可能性あり(Fault summary circuit may not be built to standard (missing or omitted abnormal circuits))",
    "cc6":"異常まとめの回路が優先度が高い順に組まれていない可能性有(Fault summary circuit may not be organized in order of priority of anomalies.)",
    "cc7":"緊急停止、自動停止、サイクル停止のいずれかに該当するALが自己保持されていない(AL not self-holding for emergency stop, automatic stop, or cycle stop)",
    "cc8":"警報、異常停止のいずれかに該当するALが自己保持されている(AL is self-holding for either alarm or abnormal stop)",
    "cc9":"アラームが正しい箇所(まとめコイル)にB接点で直列に接続されている必要があるが、満たしていない。(Alarm must be connected in series with B contact to the correct location (TOTAL COIL), but does not meet)",
    "cc10":"各異常をまとめているコイルに抜けがある可能性有(There is a possibility that the coil that summarizes each anomaly may be missing.)",
    "cc11":"全ての異常をまとめているコイルに抜けがある可能性有(Possible omission in the coil that summarizes all anomalies.)",
    "cc12":"Not given",            
    "cc13":"Not given"                    
    }

# def fetch_matched_rung_no_without_total(fault_section_rung_group_df:pd.DataFrame, last_rung_no:int, all_comment:List, total_comment:str):
#     # pass
#     all_comment_detail_output = {}
#     for _, fault_df in fault_section_rung_group_df:
#         rung_name_str = str(fault_df['RUNG_NAME'].iloc[0])
#         if regex_pattern_check(rung_name_str, all_comment) and not regex_pattern_check(rung_name_str, [total_comment]):
#             pass
#         if regex_pattern_check(rung_name_str, [emergency_stop_comment]) and not regex_pattern_check(rung_name_str, [independent_device_comment]) and not regex_pattern_check(rung_name_str, [total_comment]):
#             pass

#         if regex_pattern_check(rung_name_str, [emergency_stop_comment]) and regex_pattern_check(rung_name_str, [independent_device_comment]) and not regex_pattern_check(rung_name_str, [total_comment]):
#             pass


# def execute_rule_45_programwise(input_program_file:str, input_program_comment_file:str) -> pd.DataFrame:

#     program_df = pd.read_csv(input_program_file)
#     all_comment = [automatic_stop_comment, cycle_stop_comment, fault_stop_comment, warning_comment, operation_fault_comment]


#     unique_program_values = program_df["PROGRAM"].unique()

#     for program_name in unique_program_values:

#         if program_name=="P111_XXXPRS_Function1":
#             current_program_df = program_df[program_df['PROGRAM'] == program_name]
#             fault_section_df = current_program_df[current_program_df['BODY'].str.lower() == fault_section_name]
#             fault_section_df = fault_section_df.sort_values(by="RUNG")
#             if not fault_section_df.empty:
#                 last_rung_no = int(fault_section_df.iloc[-1]["RUNG"])
#                 fault_section_rung_group_df = fault_section_df.groupby("RUNG_NAME", sort=False)
#                 all_rung_comment_filled_rung_no_without_total = fetch_matched_rung_no_without_total(fault_section_rung_group_df=fault_section_rung_group_df, all_comment=all_comment, total_comment=total_comment,  last_rung_no=last_rung_no)


# # ============================== Program-Wise Function Definitions ===============================
# # These functions perform operations specific to each program, supporting rule validations and logic checks.
# # ===============================================================================================

def extract_rung_group_data_programwise(program_df:pd.DataFrame, program_name:str, autorun_section_name:str, autorun_section_name_with_star:str, memoryfeeding_section_name:str, MD_Out_section_name:str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    logger.info(f"Group Rung and filter data on program {program_name} and section name {autorun_section_name, memoryfeeding_section_name, MD_Out_section_name}")
    
    """
    Make three different section Autorun, MemoryFeeding, MD_Out as this all is going to use in check content 2,3,4,5 in rul 25
    ALso I use autorun_section_name_with_star because there are two autorun, one is AutoRun, other one is AutoRun★
    """
    
    program_rows = program_df[program_df['PROGRAM'] == program_name].copy()
    autorun_section_rows = program_rows[program_rows['BODY'].str.lower().isin([autorun_section_name.lower(), autorun_section_name_with_star.lower()])]
    memory_feeding_section_rows = program_rows[program_rows['BODY'].str.lower() == memoryfeeding_section_name.lower()]
    MD_Out_section_rows = program_rows[program_rows['BODY'].str.lower() == MD_Out_section_name.lower()]
    autorun_rung_groups_df = autorun_section_rows.groupby('RUNG')
    memory_feeding_rung_groups_df = memory_feeding_section_rows.groupby('RUNG')
    MD_out_rung_groups_df = MD_Out_section_rows.groupby('RUNG')

    return autorun_rung_groups_df, memory_feeding_rung_groups_df, MD_out_rung_groups_df


def check_detail_1_programwise(all_rung_comment_filled_details_without_total:dict) -> List:
    stop_keys = ["emergency_stop", "automatic_stop", "cycle_stop", "fault_stop", "warning"]

    has_missing_rung = any(
        all_rung_comment_filled_details_without_total.get(key, {}).get("rung_no") == -1
        for key in stop_keys
    )

    return {
        "status": "NG" if has_missing_rung else "OK",
        "check_number": 1,
        "cc_number" : "cc1",
        "rung_no": -1,
        "target_outcoil": "",
    }

def check_detail_2_programwise(fault_section_df:pd.DataFrame, all_rung_comment_filled_details_without_total:dict):
    
    status = "OK"
    coil_operand = ""
    rung_number = -1

    first_valid_rung = next(
        (v.get('rung_no') for k, v in all_rung_comment_filled_details_without_total.items() if v.get('rung_no') != -1),
        -1  # default if no match found
    )

    if first_valid_rung !=-1:
        coil_df = fault_section_df[
            (fault_section_df['RUNG'] > 0) &
            (fault_section_df['RUNG'] < first_valid_rung) &
            (fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'coil')
        ]

        for _, coil_row in coil_df.iterrows():
            attr = ast.literal_eval(coil_row['ATTRIBUTES'])
            coil_operand = attr.get('operand')
            if isinstance(coil_operand, str) and coil_operand.startswith('AL'):
                status = "NG"
                rung_number = coil_row['RUNG']
                break
    else:
        status = 'NG'

    return {
        "status" : status,
        "check_number": 2,
        "cc_number" : "cc2",
        "rung_no": rung_number,
        "target_outcoil": coil_operand,
    }

def check_detail_3_programwise(fault_section_df:pd.DataFrame, all_rung_comment_filled_details_without_total:dict, all_rung_comment_filled_details_with_total:dict, last_rung_no:int):
    status = "OK"
    coil_operand = ""
    rung_number = -1


    """
    code is for reversed order 10->9->8....
    """
    # first_valid_rung = next(
    #     (v.get('rung_no') for k, v in reversed(all_rung_comment_filled_details_without_total.items()) if v.get('rung_no') != -1),
    #     -1  # default if no match found
    # )

    """
    code is for 10->11->12....
    """
        # Get the last key-value pair from the dictionary
    last_key, last_value = list(all_rung_comment_filled_details_without_total.items())[-1]
    # Extract rung_no if it's valid, else default to -1
    first_valid_rung = last_value.get('rung_no', -1) if last_value.get('rung_no') != -1 else -1

    if first_valid_rung == -1:
        first_valid_rung = next(
        (v.get('rung_no') for k, v in all_rung_comment_filled_details_with_total.items() if v.get('rung_no') != -1),
        -1  # default if no match found
        )
        

    print("last_rung_no", first_valid_rung, last_rung_no)
    if first_valid_rung !=-1:
        coil_df = fault_section_df[
            (fault_section_df['RUNG'] > first_valid_rung) &
            (fault_section_df['RUNG'] <= last_rung_no) &
            (fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'coil')
        ]

        for _, coil_row in coil_df.iterrows():
            attr = ast.literal_eval(coil_row['ATTRIBUTES'])
            coil_operand = attr.get('operand')
            if isinstance(coil_operand, str) and coil_operand.startswith('AL'):
                status = "NG"
                rung_number = coil_row['RUNG']
                break
    else:
        status = 'NG'
    
    return {
        "status" : status,
        "check_number": 3,
        "cc_number" : "cc3",
        "rung_no": rung_number,
        "target_outcoil": "" if status == "OK" else coil_operand,
    }


def check_detail_4_programwise(all_rung_comment_filled_details_without_total:dict):
    check_all_rung_in_sequence = [
        v.get('rung_no')
        for v in all_rung_comment_filled_details_without_total.values()
        if isinstance(v.get('rung_no'), int) and v.get('rung_no') != -1
    ]

    # Case 1: All rung_no values are -1
    if not check_all_rung_in_sequence:
        return {
            "status": "NG",
            "check_number": 4,
            "cc_number" : "cc4",
            "rung_no": -1,
            "target_outcoil": "",
        }

    # Case 2: Valid rung_nos are in ascending order
    elif check_all_rung_in_sequence == sorted(check_all_rung_in_sequence):
        return {
            "status": "OK",
            "check_number": 4,
            "cc_number" : "cc4",
            "rung_no": -1,
            "target_outcoil": "",
        }

    # Optional: Else block (if you want to handle the case when order is not correct)
    return {
        "status": "NG",
        "check_number": 4,
        "cc_number" : "cc4",
        "rung_no": -1,
        "target_outcoil": "",
    }


def check_detail_5_programwise(all_rung_comment_filled_details_with_total:dict) -> List:
    stop_keys = ["emergency_stop_total", "automatic_stop_total", "cycle_stop_total", "fault_stop_total", "warning_total"]

    has_missing_rung = any(
        all_rung_comment_filled_details_with_total.get(key, {}).get("rung_no") == -1
        for key in stop_keys
    )

    return {
        "status": "NG" if has_missing_rung else "OK",
        "check_number": 5,
        "cc_number" : "cc5",
        "rung_no": -1,
        "target_outcoil": "",
    }

def check_detail_6_programwise(all_rung_comment_filled_details_with_total:dict):
    check_all_rung_in_sequence = [
        v.get('rung_no')
        for v in all_rung_comment_filled_details_with_total.values()
        if isinstance(v.get('rung_no'), int) and v.get('rung_no') != -1
    ]

    # Case 1: All rung_no values are -1
    if not check_all_rung_in_sequence:
        return {
            "status": "NG",
            "check_number": 6,
            "cc_number" : "cc6",
            "rung_no": -1,
            "target_outcoil": "",
        }

    # Case 2: Valid rung_nos are in ascending order
    elif check_all_rung_in_sequence == sorted(check_all_rung_in_sequence):
        return {
            "status": "OK",
            "check_number": 6,
            "cc_number" : "cc6",
            "rung_no": -1,
            "target_outcoil": "",
        }

    # Optional: Else block (if you want to handle the case when order is not correct)
    return {
        "status": "NG",
        "check_number": 6,
        "cc_number" : "cc6",
        "rung_no": -1,
        "target_outcoil": "",
    }


def check_detail_7_programwise(fault_section_df:pd.DataFrame, all_rung_comment_filled_details_without_total:dict):
    stop_list = ["emergency_stop", "emergency_stop_with_independence_device", "automatic_stop", "cycle_stop"]
    not_match_coil = {}

    all_coil_list = [
        v.get('outcoil')
        for k, v in all_rung_comment_filled_details_without_total.items()
        if k in stop_list
    ]

    coil_df = fault_section_df[fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'coil']

    print("*"*10)
    print("all_coil_list",all_coil_list)
    flat_all_coil_list = [item for sublist in all_coil_list for item in sublist]
    for coil in flat_all_coil_list:
        print("$"*10)
        print("coil",coil)

        rule_7_1 = False
        rule_7_2 = False

        current_coil_rung_number = -1
        for _, coil_row in coil_df.iterrows():
            attr = ast.literal_eval(coil_row['ATTRIBUTES'])
            coil_operand = attr.get('operand')
            if isinstance(coil_operand, str) and coil==coil_operand:
                current_coil_rung_number = coil_row['RUNG']
                break
        
        """
        we have got rung number of that coil
        Now check the coil is self holding and there should not be any contact between self holding
        """

        contact_operand_outlist = []
        coil_operand_inlist = []

        print("current_coil_rung_number",current_coil_rung_number)

        if current_coil_rung_number !=-1:
            current_coil_df = fault_section_df[fault_section_df['RUNG']==current_coil_rung_number]
            all_self_holding_operand = check_self_holding(current_coil_df)
            print("all_self_holding_operand",all_self_holding_operand, coil)
            if coil in all_self_holding_operand:
                rule_7_1 = True
                

            # print("current_coil_df",current_coil_df)

            # print("****", current_coil_df.head(5))

            for _, current_coil_row in current_coil_df.iterrows():
                attr = ast.literal_eval(current_coil_row['ATTRIBUTES'])
                operand = attr.get('operand')
                if current_coil_row['OBJECT_TYPE_LIST'].lower() == 'contact' and isinstance(operand, str) and operand==coil:
                    contact_operand_outlist = attr.get('out_list')
                
                if current_coil_row['OBJECT_TYPE_LIST'].lower() == 'coil' and isinstance(operand, str) and operand==coil:
                    coil_operand_inlist =  attr.get('in_list')

                print(contact_operand_outlist, coil_operand_inlist)
                for in_list_val in coil_operand_inlist:
                    if in_list_val in contact_operand_outlist:
                        rule_7_2 = True
                        break
            
                if rule_7_2:
                    break
            if not (rule_7_1 and rule_7_2):
                not_match_coil[coil] = current_coil_rung_number
        else:
            not_match_coil[coil] = current_coil_rung_number
    
    if not_match_coil:
        return {
            "status": "NG",
            "check_number": 7,
            "cc_number" : "cc7",
            "rung_no": -1,
            "target_outcoil": not_match_coil,
        }
    else:
        return {
            "status": "OK",
            "check_number": 7,
            "cc_number" : "cc7",
            "rung_no": -1,
            "target_outcoil": "",
        }


def check_detail_8_programwise(fault_section_df:pd.DataFrame, all_rung_comment_filled_details_without_total:dict):
    total_stop_list = ["fault_stop", "warning", "warning_with_independence_device", "operation_fault"]
    not_match_coil = {}

    all_coil_list = [
        v.get('outcoil')
        for k, v in all_rung_comment_filled_details_without_total.items()
        if k in total_stop_list
    ]

    print("all_coil_list",all_coil_list)
    coil_df = fault_section_df[fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'coil']

    for coil in all_coil_list:
        if coil:
            coil = coil[0]

            # rule_8_1 = True

            current_coil_rung_number = -1
            for _, coil_row in coil_df.iterrows():
                attr = ast.literal_eval(coil_row['ATTRIBUTES'])
                coil_operand = attr.get('operand')
                if isinstance(coil_operand, str) and coil==coil_operand:
                    current_coil_rung_number = coil_row['RUNG']
                    break
            
            """
            we have got rung number of that coil
            Now check the coil should not be self holding
            """

            print("current_coil_rung_number",current_coil_rung_number)

            if current_coil_rung_number !=-1:
                current_coil_df = fault_section_df[fault_section_df['RUNG']==current_coil_rung_number]
                all_self_holding_operand = check_self_holding(current_coil_df)
                print("all_self_holding_operand",all_self_holding_operand, coil)
                if coil in all_self_holding_operand:
                    # rule_8_1 = False
                    not_match_coil[coil] = current_coil_rung_number

    if not_match_coil:
        return {
            "status": "NG",
            "check_number": 8,
            "cc_number" : "cc8",
            "rung_no": -1,
            "target_outcoil": not_match_coil,
        }
    else:
        return {
            "status": "OK",
            "check_number": 8,
            "cc_number" : "cc8",
            "rung_no": -1,
            "target_outcoil": "",
        }

def check_detail_9_programwise(fault_section_df:pd.DataFrame, all_rung_comment_filled_details_without_total:dict, all_rung_comment_filled_details_with_total:dict):
    # emergency_stop_contact = all_rung_comment_filled_details_without_total.get('emergency_stop').get('outcoil')
    # emergency_stop_with_independence_device_contact = all_rung_comment_filled_details_without_total.get('emergency_stop_with_independence_device').get('outcoil')
    # automatic_stop_contact = all_rung_comment_filled_details_without_total.get('automatic_stop').get('outcoil')
    # cycle_stop_contact = all_rung_comment_filled_details_without_total.get('cycle_stop').get('outcoil')
    # fault_stop_contact = all_rung_comment_filled_details_without_total.get('fault_stop').get('outcoil')
    # warning_contact = all_rung_comment_filled_details_without_total.get('warning').get('outcoil')
    # warning_with_independence_device_contact = all_rung_comment_filled_details_without_total.get('warning_with_independence_device').get('outcoil')
    # operation_fault_contact = all_rung_comment_filled_details_without_total.get('operation_fault').get('outcoil')

    # emergency_stop_total_rung_number = [-1, -1]
    # if all_rung_comment_filled_details_with_total.get("emergency_stop_total").get("rung_no") !=-1:
    #     start_rung_number = all_rung_comment_filled_details_with_total.get("emergency_stop_total").get("rung_no")
    #     end_rung_number = all_rung_comment_filled_details_with_total.get("emergency_stop_total").get("next_rung")
    #     emergency_stop_total_rung_number = [start_rung_number, end_rung_number]

    without_total_keys_to_check = [
            "emergency_stop",
            "emergency_stop_with_independence_device",
            "automatic_stop",
            "cycle_stop",
            "fault_stop",
            "warning",
            "warning_with_independence_device",
            "operation_fault"
        ]

    contacts = {}

    for key in without_total_keys_to_check:
        data = all_rung_comment_filled_details_without_total.get(key) or {}
        contacts[key] = data.get("outcoil")


    with_total_keys_to_check = [
            "emergency_stop_total",
            "automatic_stop_total",
            "cycle_stop_total",
            "fault_stop_total",
            "warning_total"
        ]

    rung_numbers = {}

    for key in with_total_keys_to_check:
        data = all_rung_comment_filled_details_with_total.get(key, {})
        start_rung_no = data.get("rung_no", -1)
        end_rung_no = data.get("next_rung", -1)
        rung_numbers[key] = [start_rung_no, end_rung_no] if start_rung_no != -1 else [-1, -1]

    """
    checking content 9 The out coil detected in ② or ③ must be present in series as a B contact within the detection range of ⑩
    i have all contact of 2 and 3 and have to check in 10 
    """
    check_9_1 = False
    check_9_2 = False
    check_9_3 = False
    check_9_4 = False
    check_9_5 = False

    detection_map = {
        "check_9_1": ["emergency_stop", "emergency_stop_with_independence_device"],
        "check_9_2": ["automatic_stop"],
        "check_9_3": ["cycle_stop"],
        "check_9_4": ["fault_stop", "operation_fault"],
        "check_9_5": ["warning", "warning_with_independence_device"],
    }

    # Initialize all detection flags to False
    check_flags = {key: False for key in detection_map.keys()}

    # Rung keys to check in 'rung_numbers'
    rung_keys = [
        "emergency_stop_total",
        "automatic_stop_total",
        "cycle_stop_total",
        "fault_stop_total",
        "warning_total"
    ]

    for rung_key in rung_keys:
        start_rung_no, end_rung_no = rung_numbers.get(rung_key, [-1, -1])
        if start_rung_no == -1:
            continue

        for curr_rung_no in range(start_rung_no, end_rung_no):
            contact_coil_df = fault_section_df[fault_section_df['RUNG'] == curr_rung_no]

            # Loop through each detection mapping
            for check_flag, contact_keys in detection_map.items():
                for contact_key in contact_keys:
                    contact_list = contacts.get(contact_key)
                    if not contact_list:
                        continue

                    total_contact_operands = len(contact_list)
                    found_count = 0

                    # Count matching negated contacts
                    for contact_operand in contact_list:
                        for _, row in contact_coil_df.iterrows():
                            attr = ast.literal_eval(row['ATTRIBUTES'])
                            if contact_operand == attr.get('operand') and attr.get('negated') == 'true':
                                found_count += 1

                        if found_count == total_contact_operands:
                            # Check if all contacts appear together in any series
                            all_contact_in_series = get_series_contacts(pl.from_pandas(contact_coil_df))
                            series_contact_operands_only = [
                                [item.get('operand') for item in sublist] for sublist in all_contact_in_series
                            ]
                            if any(all(elem in series for elem in contact_list) for series in series_contact_operands_only):
                                check_flags[check_flag] = True
                                break

                # If already true for this detection, skip checking more keys
                if check_flags[check_flag]:
                    continue

    # Final combined check
    if all(check_flags.values()):
        return {
            "status": "OK",
            "check_number": 9,
            "cc_number" : "cc9",
            "rung_no": -1,
            "target_outcoil": "",
        }
    else:
        return {
            "status": "NG",
            "check_number": 9,
            "cc_number" : "cc9",
            "rung_no": -1,
            "target_outcoil": check_flags,
        }


    #  ================= first approach =========================   

    # if rung_numbers.get('emergency_stop_total')[0] != -1:
    #     start_rung_no = rung_numbers.get('emergency_stop_total')[0]
    #     end_rung_no = rung_numbers.get('emergency_stop_total')[1]
    #     # contact_coil_df = fault_section_df[(fault_section_df['RUNG'] >= start_rung_no) & (fault_section_df['RUNG'] >= end_rung_no)]
    #     for curr_rung_no in range(start_rung_no, end_rung_no):
    #         print("*"*100)
    #         print("curr_rung_no", curr_rung_no)
    #         contact_coil_df = fault_section_df[fault_section_df['RUNG'] == curr_rung_no]
    #         """ 
    #         checking here in detection number 2 with 10
    #         """
    #         # print("contacts.get('emergency_stop')", contacts.get('emergency_stop'), type(contacts.get('emergency_stop')))
    #         if contacts.get('emergency_stop'):
    #             emergency_stop_contact = contacts.get('emergency_stop')
    #             total_emergency_stop_contact_operand = len(emergency_stop_contact)
    #             emergency_stop_contact_found_count = 0

    #             print("emergency_stop_contact",emergency_stop_contact)

    #             for emergency_stop_contact_operand in emergency_stop_contact:
    #                 for _, contact_coil_row in contact_coil_df.iterrows():
    #                     attr = ast.literal_eval(contact_coil_row['ATTRIBUTES'])
    #                     contact_operand = attr.get('operand')
    #                     negated_operand = attr.get('negated')

    #                     print("attr",attr)

    #                     if emergency_stop_contact_operand == contact_operand and negated_operand == 'true':
    #                         emergency_stop_contact_found_count+=1
                        
    #                     print("emergency_stop_contact_found_count", total_emergency_stop_contact_operand, total_emergency_stop_contact_operand)
    #                     if emergency_stop_contact_found_count == total_emergency_stop_contact_operand:
    #                         """
    #                             checking series connection if all found then in detection 2
    #                         """
    #                         all_contact_in_series = get_series_contacts(pl.from_pandas(contact_coil_df))
    #                         series_contact_operands_only = [[item.get('operand') for item in sublist] for sublist in all_contact_in_series]
    #                         print("series contact", series_contact_operands_only)
    #                         if any(all(elem in series for elem in emergency_stop_contact) for series in series_contact_operands_only):
    #                             check_9_1 = True
    #                             break

    #                 if check_9_1 is True:
    #                     break
                    

    #         """ 
    #         or checking here in detection number 3 with 10
    #         """
    #         if contacts.get('emergency_stop_with_independence_device') and isinstance(contacts, List):
    #             emergency_stop_with_independence_device_contact = contacts.get('emergency_stop_with_independence_device')
    #             total_emergency_stop_with_independence_device_contact_operand = len(emergency_stop_with_independence_device_contact)
    #             emergency_stop_with_independence_device_contact_found_count = 0

    #             for emergency_stop_with_independence_device_contact_operand in emergency_stop_with_independence_device_contact:
    #                 for _, contact_coil_row in contact_coil_df.iterrows():
    #                     attr = ast.literal_eval(contact_coil_row['ATTRIBUTES'])
    #                     contact_operand = attr.get('operand')
    #                     negated_operand = attr.get('negated')

    #                     if emergency_stop_with_independence_device_contact_operand == contact_operand and negated_operand == 'true':
    #                         emergency_stop_with_independence_device_contact_found_count+=1
                        
    #                     if emergency_stop_with_independence_device_contact_found_count == total_emergency_stop_with_independence_device_contact_operand:
    #                         """
    #                             checking series connection if all found then in detection 2
    #                         """
    #                         all_contact_in_series = get_series_contacts(pl.from_pandas(contact_coil_df))
    #                         series_contact_operands_only = [[item.get('operand') for item in sublist] for sublist in all_contact_in_series]
    #                         if any(all(elem in series for elem in emergency_stop_with_independence_device_contact) for series in series_contact_operands_only):
    #                             check_9_1 = True
    #                             break

    #                 if check_9_1 is True:
    #                     break

    # print("check_9_1",check_9_1)

def check_detail_10_programwise(fault_section_df:pd.DataFrame, all_rung_comment_details_with_total:dict):

    # all_result = {}
    # for circuit_name, circuit_details in list(all_rung_comment_details_with_total.items())[:-1]:
    #     all_operand = circuit_details.get('outcoil')
    #     current_rung_number = -1
    #     if len(all_operand) >=1:
    #         last_operand = all_operand[-1]
    #         other_than_last_contact = all_operand[:-1]
    #         coil_df = fault_section_df[fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'coil']
    #         for _, coil_row in coil_df.iterrows():
    #             attr = ast.literal_eval(coil_row['ATTRIBUTES'])
    #             coil_operand = attr.get('operand')
    #             if coil_operand == last_operand:
    #                 current_rung_number = coil_row['RUNG']
    #                 break

    #         if current_rung_number!=-1:
    #             contact_df = fault_section_df[fault_section_df['RUNG'] == current_rung_number]
    #             all_contact_in_series = get_series_contacts(pl.from_pandas(contact_df))
    #             series_contact_operands_only = [
    #                 [{'operand': item.get('operand'), 'negated': item.get('negated')} for item in sublist]
    #                 for sublist in all_contact_in_series
    #             ]

    #             for all_contact_negated in series_contact_operands_only:
    #                 result = all(
    #                         any(d['operand'] == op and d['negated'] == 'false' for d in all_contact_negated)
    #                         for op in other_than_last_contact
    #                     )
                    
    #                 if result:
    #                     break

                
    #             if result:
    #                 break

    #         else:
    #             result = False
    #     else:
    #         result = True

    #     all_result[circuit_name] = result

    all_result = {}

    # Pre-filter coil_df once instead of inside the loop
    coil_df = fault_section_df[fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'coil']

    # Convert attributes to dict once for coil_df to speed up lookups
    coil_df['ATTR_DICT'] = coil_df['ATTRIBUTES'].apply(ast.literal_eval)

    # Iterate over all but the last circuit
    for circuit_name, circuit_details in list(all_rung_comment_details_with_total.items())[:-1]:
        all_operand = circuit_details.get('outcoil', [])

        if len(all_operand) < 1:
            all_result[circuit_name] = True
            continue

        last_operand = all_operand[-1]
        other_than_last_contact = all_operand[:-1]

        # Find rung number of last operand (coil)
        rung_matches = coil_df[coil_df['ATTR_DICT'].apply(lambda attr: attr.get('operand') == last_operand)]
        if rung_matches.empty:
            all_result[circuit_name] = False
            continue

        current_rung_number = rung_matches.iloc[0]['RUNG']

        # Get all contacts in the rung
        contact_df = fault_section_df[fault_section_df['RUNG'] == current_rung_number]
        all_contact_in_series = get_series_contacts(pl.from_pandas(contact_df))
        
        # Extract operand + negated info
        series_contact_operands_only = [
            [{'operand': item.get('operand'), 'negated': item.get('negated')} for item in sublist]
            for sublist in all_contact_in_series
        ]

        # Check if all other_than_last_contact are present with negated == 'false' in any one series
        result = any(
            all(
                any(d['operand'] == op and d['negated'] == 'false' for d in series)
                for op in other_than_last_contact
            )
            for series in series_contact_operands_only
        )

        all_result[circuit_name] = result
        
    if all(all_result.values()):
        return {
            "status": "OK",
            "check_number": 10,
            "cc_number" : "cc10",
            "rung_no": -1,
            "target_outcoil": "",
        }
    else:
        return {
            "status": "NG",
            "check_number": 10,
            "cc_number" : "cc10",
            "rung_no": -1,
            "target_outcoil": all_result,
        }

def check_detail_11_programwise(fault_section_df:pd.DataFrame, all_rung_comment_details_with_total:dict):

    # all_contact_total = []
    # result = False

    
    # """
    # collecting all total contact which is last of each circuit total 
    # """
    # last_key = list(all_rung_comment_details_with_total.keys())[-1]
    # last_coil = all_rung_comment_details_with_total[last_key].get('outcoil')
    # if last_coil:
    #     last_coil = last_coil[0]

    # print("last_coil",last_coil)
    # for circuit_name, circuit_details in list(all_rung_comment_details_with_total.items())[:-1]:
    #     outcoil =  circuit_details.get('outcoil')
    #     if outcoil:
    #         all_contact_total.append(outcoil[-1])

    # coil_df = fault_section_df[fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'coil']

    # print("all_contact_total",all_contact_total)
    
    # current_rung_number = -1
    # for _, coil_row in coil_df.iterrows():
    #     attr = ast.literal_eval(coil_row['ATTRIBUTES'])
    #     coil_operand = attr.get('operand')
    #     print("coil_operand", coil_operand, "last_coil",last_coil)
    #     if isinstance(last_coil, str) and coil_operand == last_coil:
    #         current_rung_number = coil_row['RUNG']
    #         break

    # print("current_rung_number",current_rung_number)
    # if current_rung_number!=-1:
    #     contact_df = fault_section_df[fault_section_df['RUNG'] == current_rung_number]
    #     all_contact_in_series = get_series_contacts(pl.from_pandas(contact_df))
    #     series_contact_operands_only = [
    #         [{'operand': item.get('operand'), 'negated': item.get('negated')} for item in sublist]
    #         for sublist in all_contact_in_series
    #     ]

    #     for all_contact_negated in series_contact_operands_only:
    #         result = all(
    #                 any(d['operand'] == op and d['negated'] == 'false' for d in all_contact_negated)
    #                 for op in all_contact_total
    #             )
            
    #         if result:
    #             break

    all_contact_total = []
    result = False

    # ---- 1. Get last coil from the last circuit ----
    last_key = next(reversed(all_rung_comment_details_with_total))
    last_coil_list = all_rung_comment_details_with_total.get(last_key, {}).get('outcoil', [])
    last_coil = last_coil_list[0] if last_coil_list else None

    print("last_coil", last_coil)

    # ---- 2. Collect last outcoil from each circuit except the last ----
    all_contact_total = [
        details['outcoil'][-1]
        for name, details in list(all_rung_comment_details_with_total.items())[:-1]
        if details.get('outcoil')
    ]

    print("all_contact_total", all_contact_total)

    # ---- 3. Pre-filter coil_df and parse ATTRIBUTES once ----
    coil_df = fault_section_df[fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'coil'].copy()
    coil_df['ATTR_DICT'] = coil_df['ATTRIBUTES'].apply(ast.literal_eval)

    # ---- 4. Find rung number for last_coil ----
    if isinstance(last_coil, str):
        match = coil_df[coil_df['ATTR_DICT'].apply(lambda a: a.get('operand') == last_coil)]
        if not match.empty:
            current_rung_number = match.iloc[0]['RUNG']
        else:
            current_rung_number = -1
    else:
        current_rung_number = -1

    print("current_rung_number", current_rung_number)

    # ---- 5. If found, check contact conditions ----
    if current_rung_number != -1:
        contact_df = fault_section_df[fault_section_df['RUNG'] == current_rung_number]
        all_contact_in_series = get_series_contacts(pl.from_pandas(contact_df))

        series_contact_operands_only = [
            [{'operand': item.get('operand'), 'negated': item.get('negated')} for item in sublist]
            for sublist in all_contact_in_series
        ]

        result = any(
            all(any(c['operand'] == op and c['negated'] == 'false' for c in series) for op in all_contact_total)
            for series in series_contact_operands_only
        )

    print("result", result)
        
    if result:
        return {
            "status": "OK",
            "check_number": 11,
            "cc_number" : "cc11",
            "rung_no": -1,
            "target_outcoil": "",
        }
    else:
        return {
            "status": "NG",
            "check_number": 11,
            "cc_number" : "cc11",
            "rung_no": -1,
            "target_outcoil": "",
        }


"""
below code if emergency stop is there and not independence device check in code 
"""
# def check_detail_13_programwise(fault_section_rung_group_df:pd.DataFrame, fault_section_df:pd.DataFrame, deviceout_section_df:pd.DataFrame, automain_section_df:pd.DataFrame, program_name:str, program_comment_data:dict, emergency_stop_comment:str, total_comment:str, independent_device_comment:str, autorun_comment:str, condition_comment:str):

#     rule_13_1 = False
#     rule_13_2 = False

#     emergency_stop_without_independent_device_exist = False
#     emergency_stop_without_independent_device_exist_rung_number = -1
#     for _, fault_df in fault_section_rung_group_df:
#         rung_name_str = str(fault_df['RUNG_NAME'].iloc[0])

#         if regex_pattern_check(emergency_stop_comment, [rung_name_str]) and regex_pattern_check(total_comment, [rung_name_str]) and not regex_pattern_check(independent_device_comment, [rung_name_str]):
#             emergency_stop_without_independent_device_exist = True
#             emergency_stop_without_independent_device_exist_rung_number = int(fault_df['RUNG'].iloc[0])
#             break

#     if emergency_stop_without_independent_device_exist:
#         """
#         checking in deviceout section
#         """
#         emergency_stop_coil_df = fault_section_df[(fault_section_df['RUNG']==emergency_stop_without_independent_device_exist_rung_number) &
#                                                   (fault_section_df['OBJECT_TYPE_LIST'].str.lower()=='coil') ]

#         if not emergency_stop_coil_df.empty:
#             first_row_attr = ast.literal_eval(emergency_stop_coil_df.iloc[0]['ATTRIBUTES'])
#             emergency_stop_without_independent_device_outcoil = first_row_attr.get('operand')


#         if emergency_stop_without_independent_device_outcoil:
#             devicein_emergency_stop_comment_present = False
#             devicein_emergency_stop_comment_present_rung_number = -1
#             coil_df = deviceout_section_df[deviceout_section_df['OBJECT_TYPE_LIST'].str.lower() == 'coil']
#             for _, coil_row in coil_df.iterrows():
#                 attr = ast.literal_eval(coil_row['ATTRIBUTES'])
#                 coil_operand = attr.get('operand')
#                 if coil_operand and isinstance(coil_operand, str):
#                     coil_comment = get_the_comment_from_program(coil_operand, program_name, program_comment_data)
#                     if regex_pattern_check(emergency_stop_comment, coil_comment):
#                         devicein_emergency_stop_comment_present = True
#                         devicein_emergency_stop_comment_present_rung_number = coil_df['RUNG']
#                         break

#                 if devicein_emergency_stop_comment_present:
#                     contact_df = deviceout_section_df[deviceout_section_df['RUNG']==devicein_emergency_stop_comment_present_rung_number]
#                     for _, contact_row in contact_df.iterrows():
#                         attr = ast.literal_eval(contact_row['ATTRIBUTES'])
#                         contact_operand = attr.get('operand')
#                         negated_operand = attr.get('negated')
#                         if contact_operand and isinstance(contact_operand,str) and contact_operand == emergency_stop_without_independent_device_outcoil and negated_operand == 'false':
#                             rule_13_1 = True
#                             break

#                 if rule_13_1:
#                     break


#             devicein_autorun_condition_comment_present = False
#             devicein_autorun_condition_comment_present_rung_number = -1
#             for _, coil_row in coil_df.iterrows():
#                 attr = ast.literal_eval(coil_row['ATTRIBUTES'])
#                 coil_operand = attr.get('operand')
#                 if coil_operand and isinstance(coil_operand, str):
#                     coil_comment = get_the_comment_from_program(coil_operand, program_name, program_comment_data)
#                     if regex_pattern_check(autorun_comment, coil_comment) and regex_pattern_check(condition_comment, coil_comment):
#                         devicein_autorun_condition_comment_present = True
#                         devicein_autorun_condition_comment_present_rung_number = coil_df['RUNG']
#                         break

#                 if devicein_autorun_condition_comment_present:
#                     contact_df = deviceout_section_df[deviceout_section_df['RUNG']==devicein_autorun_condition_comment_present_rung_number]
#                     for _, contact_row in contact_df.iterrows():
#                         attr = ast.literal_eval(contact_row['ATTRIBUTES'])
#                         contact_operand = attr.get('operand')
#                         negated_operand = attr.get('negated')
#                         if contact_operand and isinstance(contact_operand,str) and contact_operand == emergency_stop_without_independent_device_outcoil and negated_operand == 'false':
#                             rule_13_2 = True
#                             break

#                 if rule_13_2:
#                     break

#     if rule_13_1 and rule_13_2:
#         return {
#             "status": "OK",
#             "check_number": 13,
#             "cc_number" : "cc13",
#             "rung_no": -1,
#             "target_outcoil": "",
#         }
#     else:
#         return {
#             "status": "NG",
#             "check_number": 13,
#             "cc_number" : "cc13",
#             "rung_no": -1,
#             "target_outcoil": "",
#         }

def check_detail_12_programwise(program_df:pd.DataFrame, mdout_section_df:pd.DataFrame, all_rung_comment_filled_details_with_total:dict):
    print("check_detail_12_programwise")
    """
    Applying step 3 here for getting outcoil of each total stop last outcoil value
    """
    last_element_of_each_total = {}
    for k, v in list(all_rung_comment_filled_details_with_total.items())[:-1]:
        if v.get('outcoil'):
            last_value = v.get('outcoil', [""])
            last_element_of_each_total[k] = last_value[-1]
    
    # print(last_element_of_each_total)

    md_out_contact_df = mdout_section_df[mdout_section_df['OBJECT_TYPE_LIST'].str.lower() == 'contact']
    # for _, md_out_row in mdout_section_df.iterrows():

    print("last_element_of_each_total",last_element_of_each_total)
    md_outcoil_list = {}
    for k,v in last_element_of_each_total.items():
        if v:

            match_v_found = False
            match_v_rung_number = -1
            for _, md_out_contact_row in md_out_contact_df.iterrows():
                attr = ast.literal_eval(md_out_contact_row['ATTRIBUTES'])
                contact_operand = attr.get('operand')
                # print("contact_operand",contact_operand, "v", v)
                if isinstance(contact_operand, str) and contact_operand == v:
                    match_v_found = True
                    match_v_rung_number = md_out_contact_row.get("RUNG")
                    break
            
            # print("match_v_rung_number",match_v_rung_number)
            if match_v_found and match_v_rung_number!=-1:
                match_coil_df = mdout_section_df[(mdout_section_df['RUNG'] == match_v_rung_number) &
                                                  (mdout_section_df['OBJECT_TYPE_LIST'].str.lower() == 'coil')]
                
                # print("match_coil_df",match_coil_df)
                if not match_coil_df.empty:
                    match_coil_value = match_coil_df['ATTRIBUTES'].iloc[0] 
                    attr = ast.literal_eval(match_coil_value)
                    contact_operand = attr.get('operand')
                    md_outcoil_list[k] = contact_operand
    
                else:
                    md_outcoil_list[k] = ""
            else:
                md_outcoil_list[k] = ""
        else:
            md_outcoil_list[k] = ""

    print("md_outcoil_list",md_outcoil_list)


def check_detail_13_programwise(fault_section_df:pd.DataFrame, deviceout_section_df:pd.DataFrame, automain_section_df:pd.DataFrame, program_name:str, program_comment_data:dict, emergency_stop_comment:str, total_comment:str, independent_device_comment:str, autorun_comment:str, condition_comment:str, emergency_stop_total_outcoil_data:List):
    
    rule_13_1 = False
    rule_13_2 = False
    
    print("emergency_stop_total_outcoil_data last data",emergency_stop_total_outcoil_data[-1])
    if emergency_stop_total_outcoil_data:
        devicein_emergency_stop_comment_present = False
        devicein_emergency_stop_comment_present_rung_number = -1
        deviceout_coil_df = deviceout_section_df[deviceout_section_df['OBJECT_TYPE_LIST'].str.lower() == 'coil']
        for _, coil_row in deviceout_coil_df.iterrows():
            attr = ast.literal_eval(coil_row['ATTRIBUTES'])
            coil_operand = attr.get('operand')
            if coil_operand and isinstance(coil_operand, str):
                coil_comment = get_the_comment_from_program(coil_operand, program_name, program_comment_data)
                # print("coil_comment",coil_operand, coil_comment, emergency_stop_comment)
                if regex_pattern_check(emergency_stop_comment, coil_comment):
                    devicein_emergency_stop_comment_present = True
                    devicein_emergency_stop_comment_present_rung_number = deviceout_coil_df['RUNG']
                    break
            print("devicein_emergency_stop_comment_present",devicein_emergency_stop_comment_present,devicein_emergency_stop_comment_present_rung_number)
            if devicein_emergency_stop_comment_present:
                contact_df = deviceout_section_df[deviceout_section_df['RUNG']==devicein_emergency_stop_comment_present_rung_number]
                for _, contact_row in contact_df.iterrows():
                    attr = ast.literal_eval(contact_row['ATTRIBUTES'])
                    contact_operand = attr.get('operand')
                    negated_operand = attr.get('negated')
                    if contact_operand and isinstance(contact_operand,str) and contact_operand == emergency_stop_total_outcoil_data[-1] and negated_operand == 'false':
                        rule_13_1 = True
                        break
            
            if rule_13_1:
                break


        
        automain_coil_df = automain_section_df[automain_section_df['OBJECT_TYPE_LIST'].str.lower() == 'coil']

        for _, coil_row in automain_coil_df.iterrows():
            automain_condition_comment_present = False
            automain_condition_comment_present_rung_number = -1
            attr = ast.literal_eval(coil_row['ATTRIBUTES'])
            coil_operand = attr.get('operand')
            if coil_operand and isinstance(coil_operand, str):
                coil_comment = get_the_comment_from_program(coil_operand, program_name, program_comment_data)
                print("coil_comment",coil_operand, coil_comment, autorun_comment, condition_comment)
                if regex_pattern_check(autorun_comment, coil_comment) and regex_pattern_check(condition_comment, coil_comment):
                    automain_condition_comment_present = True
                    automain_condition_comment_present_rung_number = coil_row['RUNG']
                    # break
            print("automain_condition_comment_present",automain_condition_comment_present, automain_condition_comment_present_rung_number)
            if automain_condition_comment_present:
                contact_df = automain_section_df[automain_section_df['RUNG']==automain_condition_comment_present_rung_number]
                for _, contact_row in contact_df.iterrows():
                    attr = ast.literal_eval(contact_row['ATTRIBUTES'])
                    contact_operand = attr.get('operand')
                    negated_operand = attr.get('negated')
                    if contact_operand and isinstance(contact_operand,str) and contact_operand == emergency_stop_total_outcoil_data[-1] and negated_operand == 'false':
                        rule_13_2 = True
                        break

            if rule_13_2:
                break

    if rule_13_1 and rule_13_2:
        return {
            "status": "OK",
            "check_number": 13,
            "cc_number" : "cc13",
            "rung_no": -1,
            "target_outcoil": "",
        }
    else:
        return {
            "status": "NG",
            "check_number": 13,
            "cc_number" : "cc13",
            "rung_no": -1,
            "target_outcoil": "",
        }


def fetch_matched_rung_no_without_total(fault_section_rung_group_df:pd.DataFrame, all_rung_comment_details_without_total:dict) -> dict:

    for _, fault_df in fault_section_rung_group_df:
        rung_name_str = str(fault_df['RUNG_NAME'].iloc[0])
        # if  all_rung_comment_details_without_total['emergency_stop']['rung_no']==-1 and regex_pattern_check(emergency_stop_comment, [rung_name_str]) and not regex_pattern_check(independent_device_comment, [rung_name_str]) and not regex_pattern_check(total_comment, [rung_name_str]):
        if  regex_pattern_check(emergency_stop_comment, [rung_name_str]) and not regex_pattern_check(independent_device_comment, [rung_name_str]) and not regex_pattern_check(total_comment, [rung_name_str]):
            all_rung_comment_details_without_total['emergency_stop']['rung_no'] = int(fault_df['RUNG'].iloc[0])
        
        # if  all_rung_comment_details_without_total['emergency_stop_with_independence_device']['rung_no']==-1 and regex_pattern_check(emergency_stop_comment, [rung_name_str]) and regex_pattern_check(independent_device_comment, [rung_name_str]) and not regex_pattern_check(total_comment, [rung_name_str]):
        if  regex_pattern_check(emergency_stop_comment, [rung_name_str]) and regex_pattern_check(independent_device_comment, [rung_name_str]) and not regex_pattern_check(total_comment, [rung_name_str]):
            all_rung_comment_details_without_total['emergency_stop_with_independence_device']['rung_no'] = int(fault_df['RUNG'].iloc[0])

        # if  all_rung_comment_details_without_total['automatic_stop']['rung_no']==-1 and regex_pattern_check(automatic_stop_comment, [rung_name_str]) and not regex_pattern_check(total_comment, [rung_name_str]):
        if  regex_pattern_check(automatic_stop_comment, [rung_name_str]) and not regex_pattern_check(total_comment, [rung_name_str]):
            all_rung_comment_details_without_total['automatic_stop']['rung_no'] = int(fault_df['RUNG'].iloc[0])

        # if  all_rung_comment_details_without_total['cycle_stop']['rung_no']==-1 and (regex_pattern_check(cycle_stop_comment, [rung_name_str])) and not regex_pattern_check(total_comment, [rung_name_str]):
        if  regex_pattern_check(cycle_stop_comment, [rung_name_str]) and not regex_pattern_check(total_comment, [rung_name_str]):
            all_rung_comment_details_without_total['cycle_stop']['rung_no'] = int(fault_df['RUNG'].iloc[0])

        # if  all_rung_comment_details_without_total['fault_stop']['rung_no']==-1 and regex_pattern_check(fault_stop_comment, [rung_name_str]) and not regex_pattern_check(total_comment, [rung_name_str]):
        if  regex_pattern_check(fault_stop_comment, [rung_name_str]) and not regex_pattern_check(total_comment, [rung_name_str]):
            all_rung_comment_details_without_total['fault_stop']['rung_no'] = int(fault_df['RUNG'].iloc[0])

        # if  all_rung_comment_details_without_total['warning']['rung_no']==-1 and regex_pattern_check(warning_comment, [rung_name_str]) and not regex_pattern_check(independent_device_comment, [rung_name_str]) and not regex_pattern_check(total_comment, [rung_name_str]):
        if  regex_pattern_check(warning_comment, [rung_name_str]) and not regex_pattern_check(independent_device_comment, [rung_name_str]) and not regex_pattern_check(total_comment, [rung_name_str]):
            all_rung_comment_details_without_total['warning']['rung_no'] = int(fault_df['RUNG'].iloc[0])

        # if  all_rung_comment_details_without_total['warning_with_independence_device']['rung_no']==-1 and regex_pattern_check(warning_comment, [rung_name_str]) and regex_pattern_check(independent_device_comment, [rung_name_str]) and not regex_pattern_check(total_comment, [rung_name_str]):
        if  regex_pattern_check(warning_comment, [rung_name_str]) and regex_pattern_check(independent_device_comment, [rung_name_str]) and not regex_pattern_check(total_comment, [rung_name_str]):
            all_rung_comment_details_without_total['warning_with_independence_device']['rung_no'] = int(fault_df['RUNG'].iloc[0])

        # if  all_rung_comment_details_without_total['operation_fault']['rung_no']==-1 and regex_pattern_check(operation_fault_comment, [rung_name_str]) and not regex_pattern_check(total_comment, [rung_name_str]):
        if  regex_pattern_check(operation_fault_comment, [rung_name_str]) and not regex_pattern_check(total_comment, [rung_name_str]):
            all_rung_comment_details_without_total['operation_fault']['rung_no'] = int(fault_df['RUNG'].iloc[0])

    return all_rung_comment_details_without_total

def fetch_next_rung_and_range_without_total(all_rung_comment_filled_rung_no_without_total:dict, last_rung_no:int):
    # Extract all valid rung numbers
    all_valid_rungs = sorted([
        entry["rung_no"]
        for entry in all_rung_comment_filled_rung_no_without_total.values()
        if entry["rung_no"] != -1
    ])

    for key, current in all_rung_comment_filled_rung_no_without_total.items():
        curr_rung = current["rung_no"]

        # Skip if current rung is -1
        if curr_rung == -1:
            continue

        # Default to last_rung_no
        next_rung = last_rung_no

        # Find the smallest valid rung number greater than curr_rung
        for r in all_valid_rungs:
            if r > curr_rung:
                next_rung = r
                break

        # Update fields
        current["next_rung"] = next_rung
        current["range"] = f"{curr_rung}-{next_rung}"

    return all_rung_comment_filled_rung_no_without_total

def filled_outcoil_detail_without_total(fault_section_df, all_rung_comment_filled_rung_no_next_rung):
    for comment_key, comment_value in all_rung_comment_filled_rung_no_next_rung.items():
        rung_no = comment_value.get('rung_no')
        next_rung = comment_value.get('next_rung')
        outcoil = comment_value.get('outcoil')

        if rung_no >= 0 and next_rung >= 0:
            for curr_rung_no in range(rung_no, next_rung):
                current_rung_df = fault_section_df[fault_section_df['RUNG']==curr_rung_no].copy()
                current_coil_df = current_rung_df[current_rung_df['OBJECT_TYPE_LIST'] == 'Coil']

                for _, curr_coil_row in current_coil_df.iterrows():
                    attr_raw = curr_coil_row['ATTRIBUTES']
                    
                    # Safe parsing of attribute dict
                    if isinstance(attr_raw, str):
                        try:
                            attr = ast.literal_eval(attr_raw)
                        except (ValueError, SyntaxError):
                            attr = {}
                    elif isinstance(attr_raw, dict):
                        attr = attr_raw
                    else:
                        attr = {}
                    curr_coil_operand = attr.get('operand', '')
                    if curr_coil_operand:
                        outcoil.append(curr_coil_operand)

        # Save back the updated coil list
        all_rung_comment_filled_rung_no_next_rung[comment_key]['outcoil'] = outcoil

    return all_rung_comment_filled_rung_no_next_rung


def fetch_matched_rung_no_with_total(fault_section_rung_group_df:pd.DataFrame, all_rung_comment_details_with_total:dict) -> dict:

    for _, fault_df in fault_section_rung_group_df:
        rung_name_str = str(fault_df['RUNG_NAME'].iloc[0])

        # if  all_rung_comment_details_with_total['emergency_stop_total']['rung_no']==-1 and regex_pattern_check(emergency_stop_comment, [rung_name_str]) and regex_pattern_check(total_comment, [rung_name_str]):
       
        if  regex_pattern_check(emergency_stop_comment, [rung_name_str]) and regex_pattern_check(total_comment, [rung_name_str]) and not regex_pattern_check(independent_device_comment, [rung_name_str]):
            all_rung_comment_details_with_total['emergency_stop_total']['rung_no'] = int(fault_df['RUNG'].iloc[0])

        # if  all_rung_comment_details_with_total['automatic_stop_total']['rung_no']==-1 and regex_pattern_check(automatic_stop_comment, [rung_name_str]) and regex_pattern_check(total_comment, [rung_name_str]):
        if  regex_pattern_check(automatic_stop_comment, [rung_name_str]) and regex_pattern_check(total_comment, [rung_name_str]):
            all_rung_comment_details_with_total['automatic_stop_total']['rung_no'] = int(fault_df['RUNG'].iloc[0])

        # if  all_rung_comment_details_with_total['cycle_stop_total']['rung_no']==-1 and regex_pattern_check(cycle_stop_comment, [rung_name_str]) and regex_pattern_check(total_comment, [rung_name_str]):
        if  regex_pattern_check(cycle_stop_comment, [rung_name_str]) and regex_pattern_check(total_comment, [rung_name_str]):
            all_rung_comment_details_with_total['cycle_stop_total']['rung_no'] = int(fault_df['RUNG'].iloc[0])

        # if  all_rung_comment_details_with_total['fault_stop_total']['rung_no']==-1 and regex_pattern_check(fault_stop_comment, [rung_name_str]) and regex_pattern_check(total_comment, [rung_name_str]):
        if  regex_pattern_check(fault_stop_comment, [rung_name_str]) and regex_pattern_check(total_comment, [rung_name_str]):
            all_rung_comment_details_with_total['fault_stop_total']['rung_no'] = int(fault_df['RUNG'].iloc[0])

        # if  all_rung_comment_details_with_total['warning_total']['rung_no']==-1 and regex_pattern_check(warning_comment, [rung_name_str]) and regex_pattern_check(total_comment, [rung_name_str]):
        if  regex_pattern_check(warning_comment, [rung_name_str]) and regex_pattern_check(total_comment, [rung_name_str]):
            all_rung_comment_details_with_total['warning_total']['rung_no'] = int(fault_df['RUNG'].iloc[0])

    return all_rung_comment_details_with_total

def fetch_next_rung_and_range_with_total(all_rung_comment_filled_rung_no_with_total:dict, last_rung_no:int):
    # Extract all valid rung numbers
    all_valid_rungs = sorted([
        entry["rung_no"]
        for entry in all_rung_comment_filled_rung_no_with_total.values()
        if entry["rung_no"] != -1
    ])

    for key, current in all_rung_comment_filled_rung_no_with_total.items():
        curr_rung = current["rung_no"]

        # Skip if current rung is -1
        if curr_rung == -1:
            continue

        # Default to last_rung_no
        next_rung = last_rung_no

        # Find the smallest valid rung number greater than curr_rung
        for r in all_valid_rungs:
            if r > curr_rung:
                next_rung = r
                break

        # Update fields
        current["next_rung"] = next_rung
        current["range"] = f"{curr_rung}-{next_rung}"

    return all_rung_comment_filled_rung_no_with_total

def filled_outcoil_detail_with_total(fault_section_df, all_rung_comment_filled_rung_no_next_rung_with_total, last_rung_no:int):
    for comment_key, comment_value in all_rung_comment_filled_rung_no_next_rung_with_total.items():
        rung_no = comment_value['rung_no']
        next_rung = comment_value['next_rung']
        outcoil = comment_value['outcoil']
        is_curr_next_rung_no_exist = False

        if comment_key == 'final_summary':
            rung_no = last_rung_no
            next_rung = last_rung_no+1
            is_curr_next_rung_no_exist = True

        if rung_no >= 0 and next_rung >= 0:
            is_curr_next_rung_no_exist = True

        if is_curr_next_rung_no_exist:
            for curr_rung_no in range(rung_no, next_rung):
                current_rung_df = fault_section_df[fault_section_df['RUNG']==curr_rung_no].copy()
                current_coil_df = current_rung_df[current_rung_df['OBJECT_TYPE_LIST'] == 'Coil']

                for _, curr_coil_row in current_coil_df.iterrows():
                    attr_raw = curr_coil_row['ATTRIBUTES']
                    
                    # Safe parsing of attribute dict
                    if isinstance(attr_raw, str):
                        try:
                            attr = ast.literal_eval(attr_raw)
                        except (ValueError, SyntaxError):
                            attr = {}
                    elif isinstance(attr_raw, dict):
                        attr = attr_raw
                    else:
                        attr = {}
                    curr_coil_operand = attr.get('operand', '')
                    if curr_coil_operand:
                        outcoil.append(curr_coil_operand)

        # Save back the updated coil list
        all_rung_comment_filled_rung_no_next_rung_with_total[comment_key]['outcoil'] = outcoil

    return all_rung_comment_filled_rung_no_next_rung_with_total

##---------------------------- Execution Program main code here -----------------------------------##
def execute_rule_45_programwise(input_program_file:str, input_program_comment_file:str) -> pd.DataFrame:

    program_df = pd.read_csv(input_program_file)
    with open(input_program_comment_file, 'r', encoding="utf-8") as file:
        program_comment_data = json.load(file)

    # print("program_df",program_df)
    unique_program_values = program_df["PROGRAM"].unique()

    for program_name in unique_program_values:

        print("_"*100)
        print("program_name",program_name)
        unique_section_name = program_df[program_df['PROGRAM'] == program_name]['BODY'].str.lower().unique()

        if 'fault' in unique_section_name:

            all_rung_comment_details_without_total = {
                "emergency_stop" : { "rung_no": -1, "next_rung" : -1, "range":"", "outcoil":[]},
                "emergency_stop_with_independence_device" : { "rung_no": -1, "next_rung" : -1, "range":"", "outcoil":[]},
                "automatic_stop" : { "rung_no": -1, "next_rung" : -1, "range":"", "outcoil":[]},
                "cycle_stop" : { "rung_no": -1, "next_rung" : -1, "range":"", "outcoil":[]},
                "fault_stop" : { "rung_no": -1, "next_rung" : -1, "range":"", "outcoil":[]},
                "warning" : { "rung_no": -1, "next_rung" : -1, "range":"", "outcoil":[]},
                "warning_with_independence_device" : { "rung_no": -1, "next_rung" : -1, "range":"", "outcoil":[]},
                "operation_fault" : { "rung_no": -1, "next_rung" : -1, "range":"", "outcoil":[]}
            }

            all_rung_comment_details_with_total = {
                "emergency_stop_total" : { "rung_no": -1, "next_rung" : -1, "range":"", "outcoil":[]},
                "automatic_stop_total" : { "rung_no": -1, "next_rung" : -1, "range":"", "outcoil":[]},
                "cycle_stop_total" : { "rung_no": -1, "next_rung" : -1, "range":"", "outcoil":[]},
                "fault_stop_total" : { "rung_no": -1, "next_rung" : -1, "range":"", "outcoil":[]},
                "warning_total" : {"rung_no": -1, "next_rung" : -1, "range":"", "outcoil":[]},
                "final_summary" : {"rung_no": -1, "next_rung" : -1, "range":"", "outcoil":[]}
            }

            # if program_name=="P111_XXXPRS_Function1":
            current_program_df = program_df[program_df['PROGRAM'] == program_name]
            fault_section_df = current_program_df[current_program_df['BODY'].str.lower() == fault_section_name]
            deviceout_section_df = current_program_df[current_program_df['BODY'].str.lower() == deviceout_section_name]
            automain_section_df = current_program_df[current_program_df['BODY'].str.lower() == automain_section_name]
            mdout_section_df = current_program_df[current_program_df['BODY'].str.lower() == mdout_section_name]
            fault_section_df = fault_section_df.sort_values(by="RUNG")

            if not fault_section_df.empty:
                """
                Code is for finding comment from rung name which contain no total and all
                """
                last_rung_no = int(fault_section_df.iloc[-1]["RUNG"])
                fault_section_rung_group_df = fault_section_df.groupby("RUNG_NAME", sort=False)
                all_rung_comment_filled_rung_no_without_total = fetch_matched_rung_no_without_total(fault_section_rung_group_df=fault_section_rung_group_df, all_rung_comment_details_without_total=all_rung_comment_details_without_total)
                all_rung_comment_filled_rung_no_next_rung_without_total = fetch_next_rung_and_range_without_total(all_rung_comment_filled_rung_no_without_total=all_rung_comment_filled_rung_no_without_total, last_rung_no=last_rung_no)                                 
                all_rung_comment_filled_details_without_total = filled_outcoil_detail_without_total(fault_section_df=fault_section_df, all_rung_comment_filled_rung_no_next_rung=all_rung_comment_filled_rung_no_next_rung_without_total)
                rprint(Pretty(all_rung_comment_filled_details_without_total))

                """
                Code is for finding comment which include total comment in output comment
                """
                all_rung_comment_filled_rung_no_with_total = fetch_matched_rung_no_with_total(fault_section_rung_group_df=fault_section_rung_group_df, all_rung_comment_details_with_total=all_rung_comment_details_with_total)
                all_rung_comment_filled_rung_no_next_rung_with_total = fetch_next_rung_and_range_with_total(all_rung_comment_filled_rung_no_with_total=all_rung_comment_filled_rung_no_with_total, last_rung_no=last_rung_no)                                 
                all_rung_comment_filled_details_with_total = filled_outcoil_detail_with_total(fault_section_df=fault_section_df, all_rung_comment_filled_rung_no_next_rung_with_total=all_rung_comment_filled_rung_no_next_rung_with_total,last_rung_no=last_rung_no)
                rprint(Pretty(all_rung_comment_filled_details_with_total))

                cc1_results = check_detail_1_programwise(all_rung_comment_filled_details_without_total=all_rung_comment_filled_details_without_total)
                cc2_results = check_detail_2_programwise(fault_section_df=fault_section_df, all_rung_comment_filled_details_without_total=all_rung_comment_filled_details_without_total)
                cc3_results = check_detail_3_programwise(fault_section_df=fault_section_df, all_rung_comment_filled_details_without_total=all_rung_comment_filled_details_without_total, all_rung_comment_filled_details_with_total=all_rung_comment_filled_details_with_total, last_rung_no=last_rung_no)
                cc4_results = check_detail_4_programwise(all_rung_comment_filled_details_without_total=all_rung_comment_filled_details_without_total)
                cc5_results = check_detail_5_programwise(all_rung_comment_filled_details_with_total=all_rung_comment_filled_details_with_total)
                cc6_results = check_detail_6_programwise(all_rung_comment_filled_details_with_total=all_rung_comment_filled_details_with_total)
                cc7_results = check_detail_7_programwise(fault_section_df=fault_section_df, all_rung_comment_filled_details_without_total=all_rung_comment_filled_details_without_total)
                cc8_results = check_detail_8_programwise(fault_section_df=fault_section_df, all_rung_comment_filled_details_without_total=all_rung_comment_filled_details_without_total)
                cc9_results = check_detail_9_programwise(fault_section_df=fault_section_df, all_rung_comment_filled_details_without_total=all_rung_comment_filled_details_without_total, all_rung_comment_filled_details_with_total=all_rung_comment_filled_details_with_total)
                cc10_results = check_detail_10_programwise(fault_section_df=fault_section_df, all_rung_comment_details_with_total=all_rung_comment_details_with_total)
                cc11_results = check_detail_11_programwise(fault_section_df=fault_section_df, all_rung_comment_details_with_total=all_rung_comment_details_with_total)
                if 'main' in program_name.lower():
                    emergency_stop_total_outcoil_data = all_rung_comment_filled_details_with_total.get("emergency_stop_total").get("outcoil")
                    cc13_results = check_detail_13_programwise(fault_section_df=fault_section_df, deviceout_section_df=deviceout_section_df, automain_section_df=automain_section_df, program_name=program_name, program_comment_data=program_comment_data ,emergency_stop_comment=emergency_stop_comment, total_comment=total_comment, independent_device_comment=independent_device_comment, autorun_comment=autorun_comment, condition_comment=condition_comment, emergency_stop_total_outcoil_data=emergency_stop_total_outcoil_data)
                else:
                    cc12_results = check_detail_12_programwise(program_df=program_df, mdout_section_df=mdout_section_df, all_rung_comment_filled_details_with_total=all_rung_comment_filled_details_with_total)

                
                rprint(Pretty(cc1_results))
                rprint(Pretty(cc2_results))
                rprint(Pretty(cc3_results))
                rprint(Pretty(cc4_results))
                rprint(Pretty(cc5_results))
                rprint(Pretty(cc6_results))
                rprint(Pretty(cc7_results))
                rprint(Pretty(cc8_results))
                rprint(Pretty(cc9_results))
                rprint(Pretty(cc10_results))
                rprint(Pretty(cc11_results))
                if 'main' in program_name.lower():
                    rprint(Pretty(cc13_results))
                else:
                    rprint(Pretty(cc12_results))


    return {}
# if __name__=='__main__':

#     input_program_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Data_Modelling/version3/data_model_Rule_44_56/data_model_Rule_44_56_programwise.csv"
#     input_program_comment_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Data_Modelling/version3/data_model_Rule_44_56/data_model_Rule_44_56_programwise.json"
#     input_function_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Data_Modelling/version3/data_model_Rule_44_56/data_model_Rule_44_56_functionwise.csv"
#     input_function_comment_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Data_Modelling/version3/data_model_Rule_44_56/data_model_Rule_44_56_functionwise.json"
#     output_folder_path = 'C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Rules_implementation/pythoncode/output_csv/'
#     program_output_file = 'Rule_45_programwise.csv'
#     function_output_file = 'Rule_45_functionwise.csv'

#     final_csv = execute_rule_45_programwise(input_program_file=input_program_file, input_program_comment_file=input_program_comment_file)
#     # final_csv.to_csv(f"{output_folder_path}/{program_output_file}", index=False, encoding='utf-8-sig')

#     # final_csv = execute_rule_45_functionwise(input_function_file=input_function_file, input_function_comment_file=input_function_comment_file)
#     # final_csv.to_csv(f"{output_folder_path}/{function_output_file}", index=False, encoding='utf-8-sig')
