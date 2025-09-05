import ast
import json
import pandas as pd
from typing import *
import re
from loguru import logger
import polars as pl
from .extract_comment_from_variable import get_the_comment_from_program, get_the_comment_from_function
from .rule_4_3_ladder_utils import get_parallel_contacts, get_format_parellel_contact_detail
from .ladder_utils import regex_pattern_check, clean_rung_number
from .japanese_half_full_width_mapping import full_to_half_conversion

# ============================================ Comments referenced in Rule 4.3 processing ============================================
rule_content_cc = {"cc1": "If the target is to be detected in ① but not in ③, NG is assumed.", 
                   "cc2_1":"If the out coil detected in ③ is “chuck”, check to see if there is an A contact in series that includes “unchuck” and  'end' in the condition. If not, it is NG.", 
                   "cc2_2": "If the out coil detected in ③ is “unchuck”, check to see if there is an A contact in series that includes “chuck” and  'end' in the condition. If not, it is NG."}

ng_content = {"cc1": "Not given", 
                "cc2_1":"P&P搬送の記憶送りだが逆戻り防止のための条件が抜けている可能性有(ﾁｬｯｸ)(P&P transfer memory feed circuit may be missing a condition to prevent backward movement.(chuck))", 
                "cc2_2": "P&P搬送の記憶送りだが逆戻り防止のための条件が抜けている可能性有(ｱﾝﾁｬｯｸ)(P&P transfer memory feed circuit may be missing a condition to prevent backward movement.(unchuck))"}

chuck_comment = "チャック"
unchuck_comment = "アンチャック"
operation_comment = "動作"
condition_comment = "条件"
end_comment = "端"

rule_4_3_check_item = "Rule of Memoryfeeding(P&P)"

# ============================ Rule 4.3: Definitions, Content, and Configuration Details ============================
section_name = "Preparation"
rule_content = "In order to prevent backward movement at P&P, it is necessary to put “unchuck end” in the chuck operation start condition and “chuck end” in the unchuck start condition."

# ============================ Helper Functions for Program-Wise Operations ============================
def check_chuck_operation_condition_from_program(row, chuck_comment:str, 
                                                 unchuck_comment:str, 
                                                 operation_comment:str, 
                                                 condition_comment:str, 
                                                 program_name:str, 
                                                 program_comment_data:dict):
    try:
        attr = ast.literal_eval(row['ATTRIBUTES'])
        if isinstance(attr, dict) and 'operand' in attr:
                comment = get_the_comment_from_program(attr.get('operand'), program_name, program_comment_data)
                if isinstance(comment, list):

                    unchuck_comment = ''.join(full_to_half_conversion.get(char, char) for char in unchuck_comment)
                    comment = [
                                ''.join(full_to_half_conversion.get(char, char) for char in c)
                                for c in comment
                            ]
                    """
                    check unchuck should not be there 
                    """
                    if not any(unchuck_comment in c for c in comment):
                        unchuck_not_present_status = True
                    else:
                        unchuck_not_present_status = False

                    if regex_pattern_check(chuck_comment, comment) and regex_pattern_check(operation_comment, comment) and regex_pattern_check(condition_comment, comment) and unchuck_not_present_status:
                        return attr.get('operand')
    except Exception:
        return None
    return None

def check_unchuck_operation_condition_from_program(row, 
                                                   chuck_comment:str, 
                                                   unchuck_comment:str, 
                                                   operation_comment:str, 
                                                   condition_comment:str,  
                                                   program_name:str, 
                                                   program_comment_data:dict):
    try:
        attr = ast.literal_eval(row['ATTRIBUTES'])
        if isinstance(attr, dict) and 'operand' in attr:
                if isinstance(attr.get('operand'), str):
                    comment = get_the_comment_from_program(attr.get('operand'), program_name, program_comment_data)
                    if isinstance(comment, list):
                        if regex_pattern_check(unchuck_comment, comment) and regex_pattern_check(operation_comment, comment) and regex_pattern_check(condition_comment, comment):
                            return attr.get('operand')
    except Exception:
        return None
    return None

# ============================ Helper Functions for Function-Wise Operations ============================
def check_chuck_operation_condition_from_function(row, 
                                                  chuck_comment:str, 
                                                  unchuck_comment:str, 
                                                  operation_comment:str, 
                                                  condition_comment:str,  
                                                  function_name:str, 
                                                  function_comment_data:dict):
    try:
        attr = ast.literal_eval(row['ATTRIBUTES'])
        if isinstance(attr, dict) and 'operand' in attr:
                comment = get_the_comment_from_function(attr.get('operand'), function_name, function_comment_data)
                if isinstance(comment, list):

                    unchuck_comment = ''.join(full_to_half_conversion.get(char, char) for char in unchuck_comment)
                    comment = [
                                ''.join(full_to_half_conversion.get(char, char) for char in c)
                                for c in comment
                            ]
                    """
                    check unchuck should not be there 
                    """
                    if not any(unchuck_comment in c for c in comment):
                        unchuck_not_present_status = True
                    else:
                        unchuck_not_present_status = False

                    if regex_pattern_check(chuck_comment, comment) and regex_pattern_check(operation_comment, comment) and regex_pattern_check(condition_comment, comment) and unchuck_not_present_status:
                        return attr.get('operand')
    except Exception:
        return None
    return None

def check_unchuck_operation_condition_from_function(row, 
                                                    chuck_comment:str, 
                                                    unchuck_comment:str, 
                                                    operation_comment:str, 
                                                    condition_comment:str,  
                                                    function_name:str, 
                                                    function_comment_data:dict):
    try:
        attr = ast.literal_eval(row['ATTRIBUTES'])
        if isinstance(attr, dict) and 'operand' in attr:
                if isinstance(attr.get('operand'), str):
                    comment = get_the_comment_from_function(attr.get('operand'), function_name, function_comment_data)
                    if isinstance(comment, list):
                        if regex_pattern_check(unchuck_comment, comment) and regex_pattern_check(operation_comment, comment) and regex_pattern_check(condition_comment, comment):
                            return attr.get('operand')
    except Exception:
        return None
    return None


# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================

# ============================== Detection Range function for program ==============================
def detection_range_programwise(memory_feeding_rung_groups, 
                                chuck_comment:str, 
                                unchuck_comment:str,  
                                operation_comment:str, 
                                condition_comment:str, 
                                program_name:str, 
                                program_comment_data : dict) -> dict:
    
    logger.info(f"Detection Range function for program: {program_name}, section: {section_name}")
    
    chuck_operation_condition_operand = None
    unchuck_operation_condition_operand = None
    chuck_operation_condition_rung_number = -1
    unchuck_operation_condition_rung_number = -1

    for _, rung_df in memory_feeding_rung_groups:

        rung_df['chuck_operation_condition'] = rung_df.apply(
            lambda row: check_chuck_operation_condition_from_program(row=row, chuck_comment=chuck_comment, unchuck_comment=unchuck_comment,  operation_comment=operation_comment, condition_comment=condition_comment, program_name=program_name, program_comment_data=program_comment_data) if row['OBJECT_TYPE_LIST'] == 'Coil' else None,
            axis=1
        )

        chuck_operation_condition_match_outcoil = rung_df[rung_df['chuck_operation_condition'].notna()]
        if (not chuck_operation_condition_match_outcoil.empty) and chuck_operation_condition_operand is None:
            chuck_operation_condition_operand = chuck_operation_condition_match_outcoil.iloc[0]['chuck_operation_condition']  # First match
            chuck_operation_condition_rung_number = chuck_operation_condition_match_outcoil.iloc[0]['RUNG']

        rung_df['unchuck_operation_condition'] = rung_df.apply(
            lambda row: check_unchuck_operation_condition_from_program(row=row, chuck_comment=chuck_comment, unchuck_comment=unchuck_comment,  operation_comment=operation_comment, condition_comment=condition_comment, program_name=program_name, program_comment_data=program_comment_data) if row['OBJECT_TYPE_LIST'] == 'Coil' else None,
            axis=1
        )

        unchuck_operation_condition_match_outcoil = rung_df[rung_df['unchuck_operation_condition'].notna()]
        if (not unchuck_operation_condition_match_outcoil.empty) and unchuck_operation_condition_operand is None:
            unchuck_operation_condition_operand = unchuck_operation_condition_match_outcoil.iloc[0]['unchuck_operation_condition']  # First match
            unchuck_operation_condition_rung_number = unchuck_operation_condition_match_outcoil.iloc[0]['RUNG']
    
    return {
        "chuck_unchuck_outcoil_operand" : [chuck_operation_condition_operand, unchuck_operation_condition_operand],
        "chuck_unchuck_outcoil_rung_number" : [int(chuck_operation_condition_rung_number)-1, int(unchuck_operation_condition_rung_number)-1],
        "chuck_unchuck_type" : ["chuck", "unchuck"],
    }

#============================== Check Detail 1 program ==============================
def check_detail_1_programwise(detection_result : dict, 
                               program_name:str) -> dict:
    logger.info(f"Executing program {program_name} check detail 1")

    cc1_result = []
    chuck_unchuck_type = detection_result.get('chuck_unchuck_type')
    chuck_unchuck_operands = detection_result.get('chuck_unchuck_outcoil_operand')
    chuck_unchuck_rung_number = detection_result.get('chuck_unchuck_outcoil_rung_number')

    for chuck_type, operand, rung_number in zip(chuck_unchuck_type, chuck_unchuck_operands, chuck_unchuck_rung_number):
        cc1_result.append({
            'cc': 'cc1',
            'status': "NG" if operand is None else "OK",
            "chuck_type" : chuck_type,
            "check_number" : 1,
            'rung': -1 if operand is None else rung_number,
            'target_coil': operand
        })

    cc1_result.append({
        'cc': 'cc1',
        'status': "NG" if not chuck_unchuck_operands or any(x is None for x in chuck_unchuck_operands) else "OK",
        "chuck_type" : None,
        "check_number" : 1,
        'rung': -1,
        'target_coil': chuck_unchuck_operands
    })

    return cc1_result
    

#============================== Check Detail 2 program ==============================
def check_detail_2_programwise(memory_feeding_rung_groups, 
                               detection_result:str, 
                               chuck_comment:str, 
                               unchuck_comment:str, 
                               end_comment:str, 
                               program_name:str, 
                               program_comment_data:dict) -> dict:
    
    logger.info(f"Executing program {program_name} check detail 2")

    chuck_unchuck_type = detection_result.get('chuck_unchuck_type')
    chuck_unchuck_operands = detection_result.get('chuck_unchuck_outcoil_operand',[])
    chuck_unchuck_rung_number = detection_result.get('chuck_unchuck_outcoil_rung_number',[])

    cc2_result = []

    for chuck_type, chuck_unchuck_operand, rung_number in zip(chuck_unchuck_type, chuck_unchuck_operands, chuck_unchuck_rung_number):
        chuck_contact_operand_found = False
        if chuck_type=='chuck':
            if chuck_unchuck_operand is not None:
                current_memory_feeding_rung_df = memory_feeding_rung_groups.get_group(rung_number+1).copy()
                for _, row in current_memory_feeding_rung_df.iterrows():
                    if row['OBJECT_TYPE_LIST'] == 'Contact':
                        attr = ast.literal_eval(row['ATTRIBUTES'])
                        contact_operand = attr.get('operand')
                        
                        contact_comment = get_the_comment_from_program(contact_operand, program_name, program_comment_data)
                        if isinstance(contact_comment, list):
                            if regex_pattern_check(unchuck_comment, contact_comment) and regex_pattern_check(end_comment, contact_comment):
                                chuck_contact_operand_found = True
                                
                                """
                                for checking if the match contact is not in parellel contact if it is parellel contact then it is NG
                                """
                                current_rung_parellel_check_df = current_memory_feeding_rung_df[current_memory_feeding_rung_df['RUNG'] == row['RUNG']]
                                all_parellel_contact = get_parallel_contacts(pl.from_pandas(current_rung_parellel_check_df))
                                if all_parellel_contact:
                                    structured_parellel_contact = get_format_parellel_contact_detail(all_parellel_contact)
                                else:
                                    structured_parellel_contact = {}

                                if contact_operand not in structured_parellel_contact.keys():
                                    cc2_result.append({
                                        'cc': 'cc2_1',
                                        'status': "OK",
                                        "chuck_type": chuck_type,
                                        "check_number" : 2.1,
                                        'rung': rung_number,
                                        'target_coil': contact_operand
                                    })
                                else:
                                    cc2_result.append({
                                        'cc': 'cc2_1',
                                        'status': "NG",
                                        "chuck_type": chuck_type,
                                        "check_number" : 2.1,
                                        'rung': -1,
                                        'target_coil': ""
                                    })
                                break

            if not chuck_contact_operand_found:
                cc2_result.append({
                    'cc': 'cc2_1',
                    'status': "NG",
                    "chuck_type": chuck_type,
                    "check_number" : 2.1,
                    'rung': -1,
                    'target_coil': ""
                })

        if chuck_type=='unchuck':
            unchuck_contact_operand_found = False
            if chuck_unchuck_operand is not None:
                # current_memory_feeding_rung_df = memory_feeding_rung_groups[memory_feeding_rung_groups['RUNG'] == rung_number].copy()
                current_memory_feeding_rung_df = memory_feeding_rung_groups.get_group(rung_number+1).copy()
                for _, row in current_memory_feeding_rung_df.iterrows():
                    if row['OBJECT_TYPE_LIST'] == 'Contact':
                        attr = ast.literal_eval(row['ATTRIBUTES'])
                        contact_operand = attr.get('operand')
                        contact_comment = get_the_comment_from_program(contact_operand, program_name, program_comment_data)
                        if isinstance(contact_comment, list):
                            unchuck_comment = ''.join(full_to_half_conversion.get(char, char) for char in unchuck_comment)

                            contact_comment = [
                                        ''.join(full_to_half_conversion.get(char, char) for char in c)
                                        for c in contact_comment
                                    ]
                            """
                            check unchuck should not be there 
                            """
                            if not any(unchuck_comment in c for c in contact_comment):
                                unchuck_not_present_status = True
                            else:
                                unchuck_not_present_status = False

                            if regex_pattern_check(chuck_comment, contact_comment) and regex_pattern_check(end_comment, contact_comment) and unchuck_not_present_status:
                                unchuck_contact_operand_found = True

                                current_rung_parellel_check_df = current_memory_feeding_rung_df[current_memory_feeding_rung_df['RUNG'] == row['RUNG']]
                                all_parellel_contact = get_parallel_contacts(pl.from_pandas(current_rung_parellel_check_df))
                                if all_parellel_contact:
                                    structured_parellel_contact = get_format_parellel_contact_detail(all_parellel_contact)
                                else:
                                    structured_parellel_contact = {}

                                if contact_operand not in structured_parellel_contact.keys():
                                    cc2_result.append({
                                        'cc': 'cc2_2',
                                        'status': "OK",
                                        "chuck_type": chuck_type,
                                        "check_number" : 2.2,
                                        'rung': rung_number,
                                        'target_coil': contact_operand
                                    })
                                else:
                                    cc2_result.append({
                                        'cc': 'cc2_1',
                                        'status': "NG",
                                        "chuck_type": chuck_type,
                                        "check_number" : 2.2,
                                        'rung': -1,
                                        'target_coil': ""
                                    })
                                    break

            if not unchuck_contact_operand_found:
                cc2_result.append({
                    'cc': 'cc2_2',
                    'status': "NG",
                    "chuck_type": chuck_type,
                    "check_number" : 2.2,
                    'rung': -1,
                    'target_coil': ""
                })

    return cc2_result


#============================== calling check_detail which check all 4 check detail ==============================
def check_detail_programwise(memory_feeding_rung_groups, 
                            detection_result:dict, 
                            chuck_comment:str, 
                            unchuck_comment:str, 
                            end_comment:str, 
                            program_name:str, 
                            section_name:str, 
                            program_comment_data:dict) -> List:

    logger.info(f"Checking details for program: {program_name}, section: {section_name}")

    cc1_results = check_detail_1_programwise(detection_result=detection_result, program_name=program_name)
    cc2_results = check_detail_2_programwise(memory_feeding_rung_groups=memory_feeding_rung_groups, detection_result=detection_result, chuck_comment=chuck_comment, unchuck_comment=unchuck_comment, end_comment=end_comment, program_name=program_name, program_comment_data=program_comment_data)

    return [cc1_results, cc2_results]


#============================== Make filter Program, section, and group rung ==============================

def extract_rung_group_data_programwise(program_df, 
                                        program_name:str, 
                                        section_name:str):
    
    logger.info(f"Filter program and memory feeding section and group rung")
    program_rows = program_df[program_df['PROGRAM'] == program_name].copy()
    memory_feeding_section_rows = program_rows[program_rows['BODY'] == section_name]
    memory_feeding_rung_groups = memory_feeding_section_rows.groupby('RUNG')
    return memory_feeding_rung_groups

#============================== Store all data to CSV ==============================
def store_program_csv_results(output_rows:List, 
                              all_cc_status:List, 
                              program_name:str, 
                              section_name:str, 
                              ng_content:dict, 
                              rule_content:str) -> List:
    
    logger.info(f"Storing all result in output csv file")

    for _, cc_results in enumerate(all_cc_status):
        for cc_result in cc_results:
            ng_name = ng_content.get(cc_result.get('cc', '')) if cc_result.get('status') == "NG" else ""
            rung_number = cc_result.get('rung')-1 if cc_result.get('rung')!=-1 else -1
            outcoil = cc_result.get('target_coil') if cc_result.get('target_coil') else ""
            output_rows.append({
                "Result": cc_result.get('status'),
                "Task": program_name,
                "Section": section_name,
                "RungNo": rung_number,
                "Target": outcoil,
                "CheckItem": rule_4_3_check_item,
                "Detail": ng_name,
                "Status" : ""
            })
            # output_rows.append({
            #     "TASK_NAME": program_name,
            #     "SECTION_NAME": section_name,
            #     "RULE_NUMBER": "4.3",
            #     "CHECK_NUMBER": cc_result.get('check_number'),
            #     "RUNG_NUMBER": cc_result.get('rung'),
            #     "RULE_CONTENT": rule_content,
            #     "CHECK_CONTENT": rule_content_cc.get(cc_result.get('cc', '')),
            #     "TARGET_OUTCOIL" : cc_result.get('target_coil'),
            #     "CHUCK_UNCHUCK" : cc_result.get('chuck_type', ''),
            #     "STATUS": cc_result.get('status'),
            #     "NG_EXPLANATION": ng_name
            # })

    return output_rows


# ============================== Function-Wise Function Definitions ===============================
# These functions perform operations specific to each function, supporting rule validations and logic checks.
# ===============================================================================================

#===================================== Detection Range function for program =====================================
def detection_range_functionwise(memory_feeding_rung_groups, 
                                chuck_comment:str, 
                                unchuck_comment:str,  
                                operation_comment:str, 
                                condition_comment:str, 
                                function_name:str, 
                                function_comment_data : dict) -> dict:
    
    logger.info(f"Detection Range function for function: {function_name}, section: {section_name}")
    
    chuck_operation_condition_operand = None
    unchuck_operation_condition_operand = None
    chuck_operation_condition_rung_number = -1
    unchuck_operation_condition_rung_number = -1

    for _, rung_df in memory_feeding_rung_groups:

        rung_df['chuck_operation_condition'] = rung_df.apply(
            lambda row: check_chuck_operation_condition_from_function(row=row, chuck_comment=chuck_comment, unchuck_comment=unchuck_comment,  operation_comment=operation_comment, condition_comment=condition_comment, function_name=function_name, function_comment_data=function_comment_data) if row['OBJECT_TYPE_LIST'] == 'Coil' else None,
            axis=1
        )

        chuck_operation_condition_match_outcoil = rung_df[rung_df['chuck_operation_condition'].notna()]
        if (not chuck_operation_condition_match_outcoil.empty) and chuck_operation_condition_operand is None:
            chuck_operation_condition_operand = chuck_operation_condition_match_outcoil.iloc[0]['chuck_operation_condition']  # First match
            chuck_operation_condition_rung_number = chuck_operation_condition_match_outcoil.iloc[0]['RUNG']

        rung_df['unchuck_operation_condition'] = rung_df.apply(
            lambda row: check_unchuck_operation_condition_from_function(row=row, chuck_comment=chuck_comment, unchuck_comment=unchuck_comment,  operation_comment=operation_comment, condition_comment=condition_comment, function_name=function_name, function_comment_data=function_comment_data) if row['OBJECT_TYPE_LIST'] == 'Coil' else None,
            axis=1
        )

        unchuck_operation_condition_match_outcoil = rung_df[rung_df['unchuck_operation_condition'].notna()]
        if (not unchuck_operation_condition_match_outcoil.empty) and unchuck_operation_condition_operand is None:
            unchuck_operation_condition_operand = unchuck_operation_condition_match_outcoil.iloc[0]['unchuck_operation_condition']  # First match
            unchuck_operation_condition_rung_number = unchuck_operation_condition_match_outcoil.iloc[0]['RUNG']
    
    return {
        "chuck_unchuck_outcoil_operand" : [chuck_operation_condition_operand, unchuck_operation_condition_operand],
        "chuck_unchuck_outcoil_rung_number" : [int(chuck_operation_condition_rung_number)-1, int(unchuck_operation_condition_rung_number)-1],
        "chuck_unchuck_type" : ["chuck", "unchuck"],
    }

#============================== Check Detail 1 function ==============================
def check_detail_1_functionwise(detection_result : dict, 
                                function_name:str) -> dict:
    logger.info(f"Executing function {function_name} check detail 1")

    cc1_result = []
    chuck_unchuck_type = detection_result.get('chuck_unchuck_type')
    chuck_unchuck_operands = detection_result.get('chuck_unchuck_outcoil_operand')
    chuck_unchuck_rung_number = detection_result.get('chuck_unchuck_outcoil_rung_number')

    for chuck_type, operand, rung_number in zip(chuck_unchuck_type, chuck_unchuck_operands, chuck_unchuck_rung_number):
        cc1_result.append({
            'cc': 'cc1',
            'status': "NG" if operand is None else "OK",
            "chuck_type" : chuck_type,
            "check_number" : 1,
            'rung': -1 if operand is None else rung_number,
            'target_coil': operand
        })

    cc1_result.append({
        'cc': 'cc1',
        'status': "NG" if not chuck_unchuck_operands or any(x is None for x in chuck_unchuck_operands) else "OK",
        "chuck_type" : None,
        "check_number" : 1,
        'rung': -1,
        'target_coil': chuck_unchuck_operands
    })

    return cc1_result
    

#============================== Check Detail 2 function ==============================
def check_detail_2_functionwise(memory_feeding_rung_groups, detection_result:str, chuck_comment:str, unchuck_comment:str, end_comment:str, function_name:str, function_comment_data:dict) -> dict:
    logger.info(f"Executing function {function_name} check detail 2")

    chuck_unchuck_type = detection_result.get('chuck_unchuck_type')
    chuck_unchuck_operands = detection_result.get('chuck_unchuck_outcoil_operand',[])
    chuck_unchuck_rung_number = detection_result.get('chuck_unchuck_outcoil_rung_number',[])

    cc2_result = []
    for chuck_type, chuck_unchuck_operand, rung_number in zip(chuck_unchuck_type, chuck_unchuck_operands, chuck_unchuck_rung_number):
        chuck_contact_operand_found = False
        if chuck_type=='chuck':
            if chuck_unchuck_operand is not None:
                current_memory_feeding_rung_df = memory_feeding_rung_groups.get_group(rung_number+1).copy()
                for _, row in current_memory_feeding_rung_df.iterrows():
                    if row['OBJECT_TYPE_LIST'] == 'Contact':
                        attr = ast.literal_eval(row['ATTRIBUTES'])
                        contact_operand = attr.get('operand')
                        
                        contact_comment = get_the_comment_from_function(contact_operand, function_name, function_comment_data)
                        if isinstance(contact_comment, list):
                            if regex_pattern_check(unchuck_comment, contact_comment) and regex_pattern_check(end_comment, contact_comment):
                                chuck_contact_operand_found = True
                                
                                """
                                for checking if the match contact is not in parellel contact if it is parellel contact then it is NG
                                """
                                current_rung_parellel_check_df = current_memory_feeding_rung_df[current_memory_feeding_rung_df['RUNG'] == row['RUNG']]
                                all_parellel_contact = get_parallel_contacts(pl.from_pandas(current_rung_parellel_check_df))
                                if all_parellel_contact:
                                    structured_parellel_contact = get_format_parellel_contact_detail(all_parellel_contact)
                                else:
                                    structured_parellel_contact = {}

                                if contact_operand not in structured_parellel_contact.keys():
                                    cc2_result.append({
                                        'cc': 'cc2_1',
                                        'status': "OK",
                                        "chuck_type": chuck_type,
                                        "check_number" : 2.1,
                                        'rung': rung_number,
                                        'target_coil': contact_operand
                                    })
                                else:
                                    cc2_result.append({
                                        'cc': 'cc2_1',
                                        'status': "NG",
                                        "chuck_type": chuck_type,
                                        "check_number" : 2.1,
                                        'rung': -1,
                                        'target_coil': ""
                                    })
                                break

            if not chuck_contact_operand_found:
                cc2_result.append({
                    'cc': 'cc2_1',
                    'status': "NG",
                    "chuck_type": chuck_type,
                    "check_number" : 2.1,
                    'rung': -1,
                    'target_coil': ""
                })

        if chuck_type=='unchuck':
            unchuck_contact_operand_found = False
            if chuck_unchuck_operand is not None:
                # current_memory_feeding_rung_df = memory_feeding_rung_groups[memory_feeding_rung_groups['RUNG'] == rung_number].copy()
                current_memory_feeding_rung_df = memory_feeding_rung_groups.get_group(rung_number+1).copy()
                for _, row in current_memory_feeding_rung_df.iterrows():
                    if row['OBJECT_TYPE_LIST'] == 'Contact':
                        attr = ast.literal_eval(row['ATTRIBUTES'])
                        contact_operand = attr.get('operand')
                        contact_comment = get_the_comment_from_function(contact_operand, function_name, function_comment_data)
                        if isinstance(contact_comment, list):
                            unchuck_comment = ''.join(full_to_half_conversion.get(char, char) for char in unchuck_comment)

                            contact_comment = [
                                        ''.join(full_to_half_conversion.get(char, char) for char in c)
                                        for c in contact_comment
                                    ]
                            """
                            check unchuck should not be there 
                            """
                            if not any(unchuck_comment in c for c in contact_comment):
                                unchuck_not_present_status = True
                            else:
                                unchuck_not_present_status = False

                            if regex_pattern_check(chuck_comment, contact_comment) and regex_pattern_check(end_comment, contact_comment) and unchuck_not_present_status:
                                unchuck_contact_operand_found = True

                                current_rung_parellel_check_df = current_memory_feeding_rung_df[current_memory_feeding_rung_df['RUNG'] == row['RUNG']]
                                all_parellel_contact = get_parallel_contacts(pl.from_pandas(current_rung_parellel_check_df))
                                if all_parellel_contact:
                                    structured_parellel_contact = get_format_parellel_contact_detail(all_parellel_contact)
                                else:
                                    structured_parellel_contact = {}

                                if contact_operand not in structured_parellel_contact.keys():
                                    cc2_result.append({
                                        'cc': 'cc2_2',
                                        'status': "OK",
                                        "chuck_type": chuck_type,
                                        "check_number" : 2.2,
                                        'rung': rung_number,
                                        'target_coil': contact_operand
                                    })
                                else:
                                    cc2_result.append({
                                        'cc': 'cc2_1',
                                        'status': "NG",
                                        "chuck_type": chuck_type,
                                        "check_number" : 2.2,
                                        'rung': -1,
                                        'target_coil': ""
                                    })
                                    break

            if not unchuck_contact_operand_found:
                cc2_result.append({
                    'cc': 'cc2_2',
                    'status': "NG",
                    "chuck_type": chuck_type,
                    "check_number" : 2.2,
                    'rung': -1,
                    'target_coil': ""
                })

    return cc2_result

#============================== calling check_detail which check all 4 check detail ==============================
def check_detail_functionwise(memory_feeding_rung_groups, detection_result:dict, chuck_comment:str, unchuck_comment:str, end_comment:str, function_name:str, section_name:str, function_comment_data:dict) -> List:

    logger.info(f"Checking details for function: {function_name}, section: {section_name}")

    cc1_results = check_detail_1_functionwise(detection_result=detection_result, function_name=function_name)
    cc2_results = check_detail_2_functionwise(memory_feeding_rung_groups=memory_feeding_rung_groups, detection_result=detection_result, chuck_comment=chuck_comment, unchuck_comment=unchuck_comment, end_comment=end_comment, function_name=function_name, function_comment_data=function_comment_data)

    return [cc1_results, cc2_results]

#============================== Make filter function, section, and group rung ==============================

def extract_rung_group_data_functionwise(function_df, function_name:str, section_name:str):
    logger.info(f"Filter function and memory feeding section and group rung")
    function_rows = function_df[function_df['FUNCTION_BLOCK'] == function_name].copy()
    memory_feeding_section_rows = function_rows[function_rows['BODY_TYPE'] == section_name]
    memory_feeding_rung_groups = memory_feeding_section_rows.groupby('RUNG')
    return memory_feeding_rung_groups


#============================== Store all data to CSV ==============================
def store_function_csv_results(output_rows:List, all_cc_status:List, function_name:str, section_name:str, ng_content:dict, rule_content:str) -> List:
    logger.info(f"Storing all result in output csv file")

    for _, cc_results in enumerate(all_cc_status):
        for cc_result in cc_results:
            ng_name = ng_content.get(cc_result.get('cc', '')) if cc_result.get('status') == "NG" else ""
            
            rung_number = cc_result.get('rung')-1 if cc_result.get('rung')!=-1 else -1
            outcoil = cc_result.get('target_coil') if cc_result.get('target_coil') else ""
            output_rows.append({
                "Result": cc_result.get('status'),
                "Task": function_name,
                "Section": section_name,
                "RungNo": rung_number,
                "Target": outcoil,
                "CheckItem": rule_4_3_check_item,
                "Detail": ng_name,
                "Status" : ""
            })
            # output_rows.append({
            #     "TASK_NAME": function_name,
            #     "SECTION_NAME": section_name,
            #     "RULE_NUMBER": "4.3",
            #     "CHECK_NUMBER": cc_result.get('check_number'),
            #     "RUNG_NUMBER": cc_result.get('rung'),
            #     "RULE_CONTENT": rule_content,
            #     "CHECK_CONTENT": rule_content_cc.get(cc_result.get('cc', '')),
            #     "TARGET_OUTCOIL" : cc_result.get('target_coil'),
            #     "CHUCK_UNCHUCK" : cc_result.get('chuck_type', ''),
            #     "STATUS": cc_result.get('status'),
            #     "NG_EXPLANATION": ng_name,
            # })

    return output_rows


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_4_3_program_wise(input_file:str, 
                                  program_comment_file:str, 
                                  input_image:str):

    logger.info("Executing Rule 4.3 program wise")

    try:
        program_df = pd.read_csv(input_file)
        input_image_program_df = pd.read_csv(input_image)
        task_names = input_image_program_df[
                input_image_program_df["Unit"].astype(str).str.lower() == "p&p"
            ]["Task name"].astype(str).str.lower().tolist()
        
        with open(program_comment_file, 'r', encoding="utf-8") as file:
            program_comment_data = json.load(file)
        

        output_rows = []
        unique_program_values = program_df["PROGRAM"].unique()
        for program in unique_program_values:

            if program.lower() in task_names:
                logger.info(f"Executing Rule 4.3 in program {program}")

                try:
                    logger.info(f"Filter rung based on program name {program} and section name {section_name}")
                    memory_feeding_rung_groups = extract_rung_group_data_programwise(program_df=program_df, program_name=program, section_name=section_name)

                    if len(memory_feeding_rung_groups) != 0:
                        detection_result  = detection_range_programwise(memory_feeding_rung_groups=memory_feeding_rung_groups, chuck_comment=chuck_comment, unchuck_comment=unchuck_comment, operation_comment=operation_comment, condition_comment=condition_comment, program_name=program, program_comment_data=program_comment_data)
                        all_cc_status = check_detail_programwise(memory_feeding_rung_groups=memory_feeding_rung_groups, detection_result=detection_result, chuck_comment=chuck_comment, unchuck_comment=unchuck_comment, end_comment=end_comment, program_name=program, section_name=section_name, program_comment_data=program_comment_data)
                        output_rows = store_program_csv_results(output_rows=output_rows, all_cc_status=all_cc_status, program_name=program, section_name=section_name, ng_content=ng_content, rule_content=rule_content)
                    
                except Exception as e:
                    logger.error(str(e))

        final_output_df = pd.DataFrame(output_rows)
        if not final_output_df.empty:
            if 'RungNo' in final_output_df.columns:
                final_output_df['RungNo'] = final_output_df['RungNo'].apply(clean_rung_number)
        else:
            final_output_df = pd.DataFrame(columns=["Result","Task","Section","RungNo","Target","CheckItem","Detail","Status"])

        return {"status":"OK", "output_df":final_output_df}
            
    except Exception as e:
        logger.error(f"Rule 4.3 Error : {e}")

        return {"status":"NOT OK", "error":str(e)}
    

# ============================== Function-Wise Execution Starts Here ===============================
def execute_rule_4_3_function_wise(input_function_file:str, 
                                   function_comment_file:str, 
                                   input_image:str):

    logger.info("Executing Rule 4.3 function wise")

    try:

        function_df = pd.read_csv(input_function_file)
        input_image_function_df = pd.read_csv(input_image)
        task_names = input_image_function_df[
                input_image_function_df["Unit"].astype(str).str.lower() == "p&p"
            ]["Task name"].astype(str).str.lower().tolist()
        
        with open(function_comment_file, 'r', encoding="utf-8") as file:
            function_comment_data = json.load(file)
        

        output_rows = []
        unique_function_values = function_df["FUNCTION_BLOCK"].unique()
        for function in unique_function_values:

            if function.lower() in task_names:
                logger.info(f"Executing Rule 4.3 in function {function}")

                try:
                    logger.info(f"Filter rung based on function name {function} and section name {section_name}")
                    memory_feeding_rung_groups = extract_rung_group_data_functionwise(function_df=function_df, function_name=function, section_name=section_name)

                    if len(memory_feeding_rung_groups) != 0:
                        detection_result  = detection_range_functionwise(memory_feeding_rung_groups=memory_feeding_rung_groups, chuck_comment=chuck_comment, unchuck_comment=unchuck_comment, operation_comment=operation_comment, condition_comment=condition_comment, function_name=function, function_comment_data=function_comment_data)
                        
                        all_cc_status = check_detail_functionwise(memory_feeding_rung_groups=memory_feeding_rung_groups, detection_result=detection_result, chuck_comment=chuck_comment, unchuck_comment=unchuck_comment, end_comment=end_comment, function_name=function, section_name=section_name, function_comment_data=function_comment_data)
                        # output_data = store_function_csv_results(output_rows=output_rows, all_cc_status=all_cc_status, function_name=function_name, section_name=section_name, chuck_unchuck_var=chuck_unchuck_var, ng_content=ng_content, rule_content=rule_content)
                        output_rows = store_function_csv_results(output_rows=output_rows, all_cc_status=all_cc_status, function_name=function, section_name=section_name, ng_content=ng_content, rule_content=rule_content)
                    
                except Exception as e:
                    logger.error(str(e))

        final_output_df = pd.DataFrame(output_rows)
        if not final_output_df.empty:
            if 'RungNo' in final_output_df.columns:
                final_output_df['RungNo'] = final_output_df['RungNo'].apply(clean_rung_number)
        else:
            final_output_df = pd.DataFrame(columns=["Result","Task","Section","RungNo","Target","CheckItem","Detail","Status"])

        return {"status":"OK", "output_df":final_output_df}
        
    except Exception as e:
        logger.error(f"Rule 4.3 Error : {e}")

        return {"status":"NOT OK", "error":str(e)}
