import ast
import json
import pandas as pd
from typing import *
import re
from loguru import logger
import polars as pl
import pprint
from .extract_comment_from_variable import get_the_comment_from_program, get_the_comment_from_function
from .ladder_utils import regex_pattern_check, clean_rung_number
from .rule_71_ladder_utils import get_series_contacts


# ============================ Rule 71: Definitions, Content, and Configuration Details ============================
workpiece_comment = "ﾜｰｸ"
unmatch_comment_1 = "不一致"
unmatch_comment_2 = "アンマッチ"
match_comment = "照合"
state_comment = "状態"
with_comment_1 = "あり"
with_comment_2 = "有"
confirm_comment = "確認"
without_comment_1 = "なし"
without_comment_2 = "無"

devicein_section_name = "devicein"
rule_35_check_item = "Rule of Parts Memory Misalignment Detection"
check_detail_content = {"cc1":"If ① but not ③, it is assumed to be NG. If even one exists, check item 1 is OK.",
                        "cc2":"Check that  A contact  includes the variable comment of ”ﾜｰｸ(workpiece)”＋(”あり(with)” or '有(with)' or '確認(confirm)') in the out coil condition detected in ③.",
                        "cc3" : "Check that the ❷ and “=” function is connected in series (AND) in the out-coil condition detected by ③.",
                        "cc4" : " Check that  「A contact  includes the variable comment of ”ﾜｰｸ(workpiece)”＋(”なし(without)” or ”無(without)”)」 or 「B contact of the variable detected in ❷」 in the out coil condition detected in ③.",
                        "cc5" : "Check that the ❹ and “=” function is connected in series (AND) in the out-coil condition detected by ③.",
                        "cc6" : "Check that ❸ and ❺ are connected in parallel (OR) in the out-coil condition detected by ③."}

ng_content =  {"cc1":"ワーク確認センサとワーク有無情報の不一致を検出できない可能性有",
               "cc2" : "センサとワーク情報の照合回路にて、「ワーク確認センサのA接点」が存在しないためNG",
               "cc3" : "センサとワーク情報の照合回路にて、「ワーク確認センサのA接点」と比較するためのワーク情報が存在しないためNG", 
               "cc4" : "センサとワーク情報の照合回路にて、「ワーク確認センサのB接点」もしくは「ワークなしセンサのA接点」が存在しないためNG",
               "cc5" : "センサとワーク情報の照合回路にて、「ワーク確認センサのB接点」と比較するためのワーク情報が存在しないためNG", 
               "cc6" : "センサとワーク情報の照合回路にて、ワーク有状態の照合と無状態の照合が並列で接続されていないためNG"}


# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================
def detection_range_programwise(fault_section_df:pd.DataFrame, program:str, program_comment_data:dict):

    match_outcoil = {}
    index = 0
    coil_df = fault_section_df[fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'coil']
    for _, coil_row in coil_df.iterrows():
        attr = ast.literal_eval(coil_row['ATTRIBUTES'])
        coil_operand = attr.get('operand')
        if isinstance(coil_operand, str) and coil_operand and "AL" in coil_operand:
            coil_comment = get_the_comment_from_program(coil_operand, program, program_comment_data)
            if isinstance(coil_comment, list) and coil_comment:
                if regex_pattern_check(workpiece_comment, coil_comment) and (regex_pattern_check(unmatch_comment_1, coil_comment) or regex_pattern_check(unmatch_comment_2, coil_comment) or regex_pattern_check(match_comment, coil_comment) or regex_pattern_check(state_comment, coil_comment)):
                    match_outcoil[index] = {"coil" : coil_operand, "rung_number" : coil_row['RUNG']}
                    index+=1

    return match_outcoil

def check_detail_1_programwise(detection_result:dict):
    status = "NG"
    if detection_result:
        status = "OK"

    return {
        "status" : status,
        "rung_number " : -1,
        "check_number" : "cc1",
        "target_coil" : ""
    }

def check_detail_2_programwise(fault_section_df:pd.DataFrame, detection_result:dict, program:str, program_comment_data:dict):
    status = "NG"
    cc2_contact = ""
    if detection_result:
        rung_number = detection_result[0]['rung_number']
        contact_match_rung_df = fault_section_df[(fault_section_df['RUNG'] == rung_number) &
                                                (fault_section_df['OBJECT_TYPE_LIST'].str.lower() == "contact")
                                                ]
        
        # print("contact_match_rung_df",contact_match_rung_df)
        for _, contact_row in contact_match_rung_df.iterrows():
            attr = ast.literal_eval(contact_row['ATTRIBUTES'])
            contact_operand = attr.get('operand')
            negated_operand = attr.get('negated')

            if isinstance(contact_operand, str) and contact_operand:
                contact_comment = get_the_comment_from_program(contact_operand, program, program_comment_data)
                if regex_pattern_check(workpiece_comment, contact_comment) and (regex_pattern_check(with_comment_1, contact_comment) or regex_pattern_check(with_comment_2, contact_comment) or regex_pattern_check(confirm_comment, contact_comment)) and negated_operand == 'false':
                    status = "OK"
                    cc2_contact = contact_operand
                    break
    
    return {
        "status" : status,
        "rung_number " : -1,
        "check_number" : "cc2",
        "target_coil" : cc2_contact
    }


def check_detail_3_programwise(fault_section_df:pd.DataFrame, detection_result:dict, program:str, program_comment_data:dict, cc2_contact:str):
    
    status = "NG"
    if detection_result and cc2_contact:
        rung_number = detection_result[0]['rung_number']
        block_contact_df = fault_section_df[(fault_section_df["RUNG"] == rung_number) &
                                            (fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'block') |
                                            (fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'contact')]
        
        equal_function_block_outlists = []
        contact_inlists = []
        for _, block_contact_row in block_contact_df.iterrows():
            attr = ast.literal_eval(block_contact_row['ATTRIBUTES'])
            operand = attr.get('operand')
            attr_typename = attr.get("typeName")
            if block_contact_row['OBJECT_TYPE_LIST'].lower() == 'block' and attr_typename == '=':
                equal_function_block_outlists.append(attr.get('_outVar_out_list'))
        
            if block_contact_row['OBJECT_TYPE_LIST'].lower() == 'contact' and operand == cc2_contact:
                contact_inlists.append(attr.get('in_list'))

        for contact_inlist in contact_inlists:
            for equal_block_outlist in equal_function_block_outlists:
                if any(item in equal_block_outlist for item in contact_inlist):
                    status = "OK"
                    break
            if status == "OK":
                break

    return {
        "status" : status,
        "rung_number " : -1,
        "check_number" : "cc3",
        "target_coil" : ""
    }


def check_detail_4_programwise(fault_section_df:pd.DataFrame, detection_result:dict, program:str, program_comment_data:dict, cc2_contact:str):
    status = "NG"
    cc4_contact = ""
    if detection_result:
        rung_number = detection_result[0]['rung_number']
        contact_match_rung_df = fault_section_df[(fault_section_df['RUNG'] == rung_number) &
                                                (fault_section_df['OBJECT_TYPE_LIST'].str.lower() == "contact")
                                                ]
        
        for _, contact_row in contact_match_rung_df.iterrows():
            attr = ast.literal_eval(contact_row['ATTRIBUTES'])
            contact_operand = attr.get('operand')
            negated_operand = attr.get('negated')

            if isinstance(contact_operand, str) and contact_operand:
                contact_comment = get_the_comment_from_program(contact_operand, program, program_comment_data)
                if regex_pattern_check(workpiece_comment, contact_comment) and (regex_pattern_check(without_comment_1, contact_comment) or regex_pattern_check(without_comment_2, contact_comment)) and negated_operand == 'false':
                    status = "OK"
                    cc4_contact = contact_operand
                    break
            
            if cc2_contact:
                if contact_operand == cc2_contact and negated_operand == 'true':
                    status = "OK"
                    cc4_contact = cc2_contact
                    break
    
    return {
        "status" : status,
        "rung_number " : -1,
        "check_number" : "cc4",
        "target_coil" : cc4_contact
    }


def check_detail_5_programwise(fault_section_df:pd.DataFrame, detection_result:dict, program:str, program_comment_data:dict, cc4_contact:str):
    
    status = "NG"
    if detection_result and cc4_contact:
        rung_number = detection_result[0]['rung_number']
        block_contact_df = fault_section_df[(fault_section_df["RUNG"] == rung_number) &
                                            (fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'block') |
                                            (fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'contact')]
        
        equal_function_block_outlists = []
        contact_inlists = []
        for _, block_contact_row in block_contact_df.iterrows():
            attr = ast.literal_eval(block_contact_row['ATTRIBUTES'])
            operand = attr.get('operand')
            attr_typename = attr.get("typeName")
            if block_contact_row['OBJECT_TYPE_LIST'].lower() == 'block' and attr_typename == '=':
                equal_function_block_outlists.append(attr.get('_outVar_out_list'))
        
            if block_contact_row['OBJECT_TYPE_LIST'].lower() == 'contact' and operand == cc4_contact:
                contact_inlists.append(attr.get('in_list'))

        print("equal_function_block_outlists", equal_function_block_outlists)
        print("contact_inlists", contact_inlists)
        for contact_inlist in contact_inlists:
            for equal_block_outlist in equal_function_block_outlists:
                if any(item in equal_block_outlist for item in contact_inlist):
                    status = "OK"
                    break
            if status == "OK":
                break

    return {
        "status" : status,
        "rung_number " : -1,
        "check_number" : "cc5",
        "target_coil" : ""
    }


def check_detail_6_programwise(fault_section_df:pd.DataFrame, detection_result:dict, cc1_results:dict, cc2_results:dict, cc3_results:dict, cc4_results:dict, cc5_results:dict):

    status = "NG"
    if all(cc.get("status") == "OK" for cc in [cc1_results, cc2_results, cc3_results, cc4_results, cc5_results]) and detection_result:
        """
        checking here that there should be more than 2 = block function
        """
        count_equal_block_present = 0
        rung_number = detection_result[0]['rung_number']
        block_df = fault_section_df[(fault_section_df["RUNG"] == rung_number) &
                                    (fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'block')]
        
        if not block_df.empty:
            for _, block_row in block_df.iterrows():
                attr = ast.literal_eval(block_row['ATTRIBUTES'])
                attr_typename = attr.get("typeName")
                if attr_typename == '=':
                    count_equal_block_present+=1
                
                if count_equal_block_present >=2:
                    break

        if count_equal_block_present >=2 :
            cc2_contact = cc2_results.get('target_coil')
            cc4_contact = cc4_results.get('target_coil')

            if cc2_contact and cc4_contact:
                status = "OK"
                current_rung_contact_df = fault_section_df[(fault_section_df["RUNG"] == rung_number) &
                                    (fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'contact')]
                get_series_connect_data = get_series_contacts(pl.from_pandas(current_rung_contact_df))
                series_contact_operands_only = [[item.get('operand') for item in sublist] for sublist in get_series_connect_data]
                for series_contact in series_contact_operands_only:
                    if cc2_contact in series_contact and cc4_contact in series_contact:
                        status = "NG"
                        break
    

    return {
        "status" : status,
        "rung_number " : -1,
        "check_number" : "cc6",
        "target_coil" : ""
    }     
            


# ============================== Function-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================
def detection_range_functionwise(fault_section_df:pd.DataFrame, function:str, function_comment_data:dict):

    match_outcoil = {}
    index = 0
    coil_df = fault_section_df[fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'coil']
    for _, coil_row in coil_df.iterrows():
        attr = ast.literal_eval(coil_row['ATTRIBUTES'])
        coil_operand = attr.get('operand')
        if isinstance(coil_operand, str) and coil_operand and "AL" in coil_operand:
            coil_comment = get_the_comment_from_function(coil_operand, function, function_comment_data)
            if isinstance(coil_comment, list) and coil_comment:
                if regex_pattern_check(workpiece_comment, coil_comment) and (regex_pattern_check(unmatch_comment_1, coil_comment) or regex_pattern_check(unmatch_comment_2, coil_comment) or regex_pattern_check(match_comment, coil_comment) or regex_pattern_check(state_comment, coil_comment)):
                    match_outcoil[index] = {"coil" : coil_operand, "rung_number" : coil_row['RUNG']}
                    index+=1

    return match_outcoil

def check_detail_1_functionwise(detection_result:dict):
    status = "NG"
    if detection_result:
        status = "OK"

    return {
        "status" : status,
        "rung_number " : -1,
        "check_number" : "cc1",
        "target_coil" : ""
    }

def check_detail_2_functionwise(fault_section_df:pd.DataFrame, detection_result:dict, function:str, function_comment_data:dict):
    status = "NG"
    cc2_contact = ""
    if detection_result:
        rung_number = detection_result[0]['rung_number']
        contact_match_rung_df = fault_section_df[(fault_section_df['RUNG'] == rung_number) &
                                                (fault_section_df['OBJECT_TYPE_LIST'].str.lower() == "contact")
                                                ]
        
        # print("contact_match_rung_df",contact_match_rung_df)
        for _, contact_row in contact_match_rung_df.iterrows():
            attr = ast.literal_eval(contact_row['ATTRIBUTES'])
            contact_operand = attr.get('operand')
            negated_operand = attr.get('negated')

            if isinstance(contact_operand, str) and contact_operand:
                contact_comment = get_the_comment_from_function(contact_operand, function, function_comment_data)
                if regex_pattern_check(workpiece_comment, contact_comment) and (regex_pattern_check(with_comment_1, contact_comment) or regex_pattern_check(with_comment_2, contact_comment) or regex_pattern_check(confirm_comment, contact_comment)) and negated_operand == 'false':
                    status = "OK"
                    cc2_contact = contact_operand
                    break
    
    return {
        "status" : status,
        "rung_number " : -1,
        "check_number" : "cc2",
        "target_coil" : cc2_contact
    }


def check_detail_3_functionwise(fault_section_df:pd.DataFrame, detection_result:dict, function:str, function_comment_data:dict, cc2_contact:str):
    
    status = "NG"
    if detection_result and cc2_contact:
        rung_number = detection_result[0]['rung_number']
        block_contact_df = fault_section_df[(fault_section_df["RUNG"] == rung_number) &
                                            (fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'block') |
                                            (fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'contact')]
        
        equal_function_block_outlists = []
        contact_inlists = []
        for _, block_contact_row in block_contact_df.iterrows():
            attr = ast.literal_eval(block_contact_row['ATTRIBUTES'])
            operand = attr.get('operand')
            attr_typename = attr.get("typeName")
            if block_contact_row['OBJECT_TYPE_LIST'].lower() == 'block' and attr_typename == '=':
                equal_function_block_outlists.append(attr.get('_outVar_out_list'))
        
            if block_contact_row['OBJECT_TYPE_LIST'].lower() == 'contact' and operand == cc2_contact:
                contact_inlists.append(attr.get('in_list'))

        for contact_inlist in contact_inlists:
            for equal_block_outlist in equal_function_block_outlists:
                if any(item in equal_block_outlist for item in contact_inlist):
                    status = "OK"
                    break
            if status == "OK":
                break

    return {
        "status" : status,
        "rung_number " : -1,
        "check_number" : "cc3",
        "target_coil" : ""
    }


def check_detail_4_functionwise(fault_section_df:pd.DataFrame, detection_result:dict, function:str, function_comment_data:dict, cc2_contact:str):
    status = "NG"
    cc4_contact = ""
    if detection_result:
        rung_number = detection_result[0]['rung_number']
        contact_match_rung_df = fault_section_df[(fault_section_df['RUNG'] == rung_number) &
                                                (fault_section_df['OBJECT_TYPE_LIST'].str.lower() == "contact")
                                                ]
        
        for _, contact_row in contact_match_rung_df.iterrows():
            attr = ast.literal_eval(contact_row['ATTRIBUTES'])
            contact_operand = attr.get('operand')
            negated_operand = attr.get('negated')

            if isinstance(contact_operand, str) and contact_operand:
                contact_comment = get_the_comment_from_function(contact_operand, function, function_comment_data)
                if regex_pattern_check(workpiece_comment, contact_comment) and (regex_pattern_check(without_comment_1, contact_comment) or regex_pattern_check(without_comment_2, contact_comment)) and negated_operand == 'false':
                    status = "OK"
                    cc4_contact = contact_operand
                    break
            
            if cc2_contact:
                if contact_operand == cc2_contact and negated_operand == 'true':
                    status = "OK"
                    cc4_contact = cc2_contact
                    break
    
    return {
        "status" : status,
        "rung_number " : -1,
        "check_number" : "cc4",
        "target_coil" : cc4_contact
    }


def check_detail_5_functionwise(fault_section_df:pd.DataFrame, detection_result:dict, function:str, function_comment_data:dict, cc4_contact:str):
    
    status = "NG"
    if detection_result and cc4_contact:
        rung_number = detection_result[0]['rung_number']
        block_contact_df = fault_section_df[(fault_section_df["RUNG"] == rung_number) &
                                            (fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'block') |
                                            (fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'contact')]
        
        equal_function_block_outlists = []
        contact_inlists = []
        for _, block_contact_row in block_contact_df.iterrows():
            attr = ast.literal_eval(block_contact_row['ATTRIBUTES'])
            operand = attr.get('operand')
            attr_typename = attr.get("typeName")
            if block_contact_row['OBJECT_TYPE_LIST'].lower() == 'block' and attr_typename == '=':
                equal_function_block_outlists.append(attr.get('_outVar_out_list'))
        
            if block_contact_row['OBJECT_TYPE_LIST'].lower() == 'contact' and operand == cc4_contact:
                contact_inlists.append(attr.get('in_list'))

        print("equal_function_block_outlists", equal_function_block_outlists)
        print("contact_inlists", contact_inlists)
        for contact_inlist in contact_inlists:
            for equal_block_outlist in equal_function_block_outlists:
                if any(item in equal_block_outlist for item in contact_inlist):
                    status = "OK"
                    break
            if status == "OK":
                break

    return {
        "status" : status,
        "rung_number " : -1,
        "check_number" : "cc5",
        "target_coil" : ""
    }


def check_detail_6_functionwise(fault_section_df:pd.DataFrame, detection_result:dict, cc1_results:dict, cc2_results:dict, cc3_results:dict, cc4_results:dict, cc5_results:dict):

    status = "NG"
    if all(cc.get("status") == "OK" for cc in [cc1_results, cc2_results, cc3_results, cc4_results, cc5_results]) and detection_result:
        """
        checking here that there should be more than 2 = block function
        """
        count_equal_block_present = 0
        rung_number = detection_result[0]['rung_number']
        block_df = fault_section_df[(fault_section_df["RUNG"] == rung_number) &
                                    (fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'block')]
        
        if not block_df.empty:
            for _, block_row in block_df.iterrows():
                attr = ast.literal_eval(block_row['ATTRIBUTES'])
                attr_typename = attr.get("typeName")
                if attr_typename == '=':
                    count_equal_block_present+=1
                
                if count_equal_block_present >=2:
                    break

        if count_equal_block_present >=2 :
            cc2_contact = cc2_results.get('target_coil')
            cc4_contact = cc4_results.get('target_coil')

            if cc2_contact and cc4_contact:
                status = "OK"
                current_rung_contact_df = fault_section_df[(fault_section_df["RUNG"] == rung_number) &
                                    (fault_section_df['OBJECT_TYPE_LIST'].str.lower() == 'contact')]
                get_series_connect_data = get_series_contacts(pl.from_pandas(current_rung_contact_df))
                series_contact_operands_only = [[item.get('operand') for item in sublist] for sublist in get_series_connect_data]
                for series_contact in series_contact_operands_only:
                    if cc2_contact in series_contact and cc4_contact in series_contact:
                        status = "NG"
                        break
    

    return {
        "status" : status,
        "rung_number " : -1,
        "check_number" : "cc6",
        "target_coil" : ""
    }     


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_35_programwise(input_program_file:str, 
                                program_comment_file:str
                                ) -> pd.DataFrame:

    logger.info("Rule 35 Start executing rule 1 program wise")
    
    output_rows = []

    try:
        program_df = pd.read_csv(input_program_file)
        with open(program_comment_file, 'r', encoding="utf-8") as file:
            program_comment_data = json.load(file)
        
        unique_program_values = program_df["PROGRAM"].unique()
        for program in unique_program_values:
            current_program_df = program_df[program_df['PROGRAM']==program]
            unique_section_values = current_program_df['BODY'].unique()
            is_memory_feeding_present = "memoryfeeding" in (val.lower() for val in unique_section_values)
            if is_memory_feeding_present:
                fault_section_df = current_program_df[current_program_df['BODY'].str.lower() == 'fault']
            
                if not fault_section_df.empty:
                    detection_result = detection_range_programwise(fault_section_df=fault_section_df, program=program, program_comment_data=program_comment_data)
                    print("Program", program)
                    print("detection_result",detection_result)
                    cc1_results = check_detail_1_programwise(detection_result=detection_result)
                    cc2_results = check_detail_2_programwise(fault_section_df=fault_section_df, detection_result=detection_result, program=program, program_comment_data=program_comment_data)
                    cc2_contact = cc2_results['target_coil']
                    cc3_results = check_detail_3_programwise(fault_section_df=fault_section_df, detection_result=detection_result, program=program, program_comment_data=program_comment_data, cc2_contact=cc2_contact)
                    cc4_results = check_detail_4_programwise(fault_section_df=fault_section_df, detection_result=detection_result, program=program, program_comment_data=program_comment_data, cc2_contact=cc2_contact)
                    cc4_contact = cc4_results['target_coil']
                    cc5_results = check_detail_5_programwise(fault_section_df=fault_section_df, detection_result=detection_result, program=program, program_comment_data=program_comment_data, cc4_contact=cc4_contact)
                    cc6_results = check_detail_6_programwise(fault_section_df=fault_section_df, detection_result=detection_result,  cc1_results=cc1_results, cc2_results=cc2_results, cc3_results=cc3_results, cc4_results=cc4_results, cc5_results=cc5_results)

                    all_cc_result = [cc1_results, cc2_results, cc3_results, cc4_results, cc5_results, cc6_results]

                    pprint.pprint(all_cc_result)

                    for cc_result in all_cc_result:
                        ng_name = ng_content.get(cc_result.get('cc', '')) if cc_result.get('status') == "NG" else ""
                        rung_number = detection_result[0]['rung_number']
                        target_outcoil = cc_result.get('target_coil') if cc_result.get('target_coil') else ""

                        output_rows.append({
                                "Result": cc_result.get('status'),
                                "Task": program,
                                "Section": "Fault",
                                "RungNo": rung_number,
                                "Target": target_outcoil,
                                "CheckItem": rule_35_check_item,
                                "Detail": ng_name,
                                "Status" : ""
                            })
                        
        final_output_df = pd.DataFrame(output_rows)
        if not final_output_df.empty:
            if 'RungNo' in final_output_df.columns:
                final_output_df['RungNo'] = final_output_df['RungNo'].apply(clean_rung_number)
        else:
            final_output_df = pd.DataFrame(columns=["Result","Task","Section","RungNo","Target","CheckItem","Detail","Status"])

        return {"status":"OK", "output_df":final_output_df}

    except Exception as e:
        logger.error(f"Rule 35 Error : {e}")

        return {"status":"NOT OK", "error":str(e)}


# ============================== Function-Wise Execution Starts Here ===============================
def execute_rule_35_functionwise(input_function_file:str, 
                                function_comment_file:str
                                ) -> pd.DataFrame:

    logger.info("Rule 35 Start executing rule 1 function wise")
    
    output_rows = []

    try:
        function_df = pd.read_csv(input_function_file)
        with open(function_comment_file, 'r', encoding="utf-8") as file:
            function_comment_data = json.load(file)
        
        unique_function_values = function_df["FUNCTION_BLOCK"].unique()
        for function in unique_function_values:
            current_function_df = function_df[function_df['FUNCTION_BLOCK']==function]
            unique_section_values = current_function_df['BODY_TYPE'].unique()
            is_memory_feeding_present = "memoryfeeding" in (val.lower() for val in unique_section_values)
            if is_memory_feeding_present:
                fault_section_df = current_function_df[current_function_df['BODY_TYPE'].str.lower() == 'fault']
            
                if not fault_section_df.empty:
                    detection_result = detection_range_functionwise(fault_section_df=fault_section_df, function=function, function_comment_data=function_comment_data)
                    print("function", function)
                    print("detection_result",detection_result)
                    cc1_results = check_detail_1_functionwise(detection_result=detection_result)
                    cc2_results = check_detail_2_functionwise(fault_section_df=fault_section_df, detection_result=detection_result, function=function, function_comment_data=function_comment_data)
                    cc2_contact = cc2_results['target_coil']
                    cc3_results = check_detail_3_functionwise(fault_section_df=fault_section_df, detection_result=detection_result, function=function, function_comment_data=function_comment_data, cc2_contact=cc2_contact)
                    cc4_results = check_detail_4_functionwise(fault_section_df=fault_section_df, detection_result=detection_result, function=function, function_comment_data=function_comment_data, cc2_contact=cc2_contact)
                    cc4_contact = cc4_results['target_coil']
                    cc5_results = check_detail_5_functionwise(fault_section_df=fault_section_df, detection_result=detection_result, function=function, function_comment_data=function_comment_data, cc4_contact=cc4_contact)
                    cc6_results = check_detail_6_functionwise(fault_section_df=fault_section_df, detection_result=detection_result,  cc1_results=cc1_results, cc2_results=cc2_results, cc3_results=cc3_results, cc4_results=cc4_results, cc5_results=cc5_results)

                    all_cc_result = [cc1_results, cc2_results, cc3_results, cc4_results, cc5_results, cc6_results]

                    pprint.pprint(all_cc_result)

                    for cc_result in all_cc_result:
                        ng_name = ng_content.get(cc_result.get('cc', '')) if cc_result.get('status') == "NG" else ""
                        rung_number = detection_result[0]['rung_number']
                        target_outcoil = cc_result.get('target_coil') if cc_result.get('target_coil') else ""

                        output_rows.append({
                                "Result": cc_result.get('status'),
                                "Task": function,
                                "Section": "Fault",
                                "RungNo": rung_number,
                                "Target": target_outcoil,
                                "CheckItem": rule_35_check_item,
                                "Detail": ng_name,
                                "Status" : ""
                            })
                        
        final_output_df = pd.DataFrame(output_rows)
        if not final_output_df.empty:
            if 'RungNo' in final_output_df.columns:
                final_output_df['RungNo'] = final_output_df['RungNo'].apply(clean_rung_number)
        else:
            final_output_df = pd.DataFrame(columns=["Result","Task","Section","RungNo","Target","CheckItem","Detail","Status"])

        return {"status":"OK", "output_df":final_output_df}

    except Exception as e:
        logger.error(f"Rule 35 Error : {e}")

        return {"status":"NOT OK", "error":str(e)}
