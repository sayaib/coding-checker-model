import ast
import json
import pandas as pd
from typing import *
import re
from loguru import logger
import polars as pl
from .extract_comment_from_variable import get_the_comment_from_function, get_the_comment_from_program
from .ladder_utils import regex_pattern_check, clean_rung_number

# ============================ Rule 46: Definitions, Content, and Configuration Details ============================
rule_content_46 = "Fault (variable name includes AL) contacts must not be used except in the Fault section."
rule_46_check_item = "Rule of Fault Program(Overall)"
check_detail_content = {"cc1":"If no contact is found that meets the detection condition in (1), it is assumed to be OK. If even one of the corresponding contacts is detected, the variable name is output as a judgment NG."}
ng_content = {"cc1":"異常(AL)の接点がFaultセクション(異常用回路)以外で使用されている (Fault (AL) contacts must not be used expect in the 'Fault section(for fault circuit)'.)"}

# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================

def detection_range_programwise(current_section_df: pd.DataFrame, 
                                program_name:str, 
                                section_name:str) -> dict:
    
    logger.info(f"Executing rule 46 detection range on program {program_name} and section name {section_name} on rule 46")

    # contact_df = current_section_df[current_section_df['OBJECT_TYPE_LIST'] == 'Contact']

    # all_operand_list = []
    # for _, contact_row in contact_df.iterrows():
    #     attr = ast.literal_eval(contact_row['ATTRIBUTES'])
    #     contact_operand = attr.get('operand', '')
    #     if contact_operand:
    #         all_operand_list.append((contact_operand, contact_row['RUNG']))


    # for operand_variable, rung_number in all_operand_list:
    #     if "AL" in operand_variable:
    #         return {
    #             "status" : "NG",
    #             "target_coil" : operand_variable,
    #             "rung_number" : rung_number
    #         }

    # return {
    #     "status" : "OK",
    #     "target_coil" : "",
    #     "rung_number" : -1
    # }

    
    # Filter only 'Contact' rows
    contact_df = current_section_df[current_section_df['OBJECT_TYPE_LIST'] == 'Contact'].copy()

    # Parse ATTRIBUTES column to dictionaries
    contact_df['ATTR_DICT'] = contact_df['ATTRIBUTES'].map(ast.literal_eval)

    # Build list of (operand, RUNG) where operand is not empty
    all_operand_list = [
        (attr.get('operand'), rung)
        for attr, rung in zip(contact_df['ATTR_DICT'], contact_df['RUNG'])
        if attr.get('operand')
        ]

    result = next(
        (
            {"status": "NG", "target_coil": op, "rung_number": rung}
            for op, rung in all_operand_list
            if "AL" in op
        ),

        {
            "status" : "OK",
            "target_coil" : "",
            "rung_number" : -1
        }
    )

    return result



def check_detail_1_programwise(detection_result:dict, 
                               program_name:str) -> dict:

    logger.info(f"Executing rule no 46 check detail 1 in program {program_name}")
    
    cc1_result = {}

    cc1_result['status'] = "OK" if detection_result['status'] == "OK" else "NG"
    cc1_result['cc'] = "cc1"
    cc1_result['outcoil'] = detection_result['target_coil']
    cc1_result['rung_number'] = detection_result['rung_number']

    return cc1_result


# ============================== Function-Wise Function Definitions ===============================
# These functions perform operations specific to each function, supporting rule validations and logic checks.
# ===============================================================================================

def detection_range_functionwise(current_section_df: pd.DataFrame, 
                                 function_name:str, 
                                 section_name:str) -> dict:
    
    logger.info(f"Executing detection range on function {function_name} and section name {section_name} on rule 46")

    # contact_df = current_section_df[current_section_df['OBJECT_TYPE_LIST'] == 'Contact']

    # all_operand_list = []
    # for _, contact_row in contact_df.iterrows():
    #     attr = ast.literal_eval(contact_row['ATTRIBUTES'])
    #     contact_operand = attr.get('operand', '')
    #     if contact_operand:
    #         all_operand_list.append((contact_operand, contact_row['RUNG']))

    # Filter only 'Contact' rows
    contact_df = current_section_df[current_section_df['OBJECT_TYPE_LIST'] == 'Contact'].copy()

    # Parse ATTRIBUTES column to dictionaries
    contact_df['ATTR_DICT'] = contact_df['ATTRIBUTES'].map(ast.literal_eval)

    # Build list of (operand, RUNG) where operand is not empty
    all_operand_list = [
        (attr.get('operand'), rung)
        for attr, rung in zip(contact_df['ATTR_DICT'], contact_df['RUNG'])
        if attr.get('operand')
        ]

    result = next(
        (
            {"status": "NG", "target_coil": op, "rung_number": rung}
            for op, rung in all_operand_list
            if "AL" in op
        ),

        {
            "status" : "OK",
            "target_coil" : "",
            "rung_number" : -1
        }
    )

    return result

    # for operand_variable, rung_number in all_operand_list:
    #     if "AL" in operand_variable:
    #         return {
    #             "status" : "NG",
    #             "target_coil" : operand_variable,
    #             "rung_number" : rung_number
    #         }

    # return {
    #     "status" : "OK",
    #     "target_coil" : "",
    #     "rung_number" : -1
    # }

def check_detail_1_functionwise(detection_result:dict, 
                                function_name:str) -> dict:

    logger.info(f"Executing rule no 46 check detail 1 in function {function_name}")
    
    cc1_result = {}

    cc1_result['status'] = "OK" if detection_result['status'] == "OK" else "NG"
    cc1_result['cc'] = "cc1"
    cc1_result['outcoil'] = detection_result['target_coil']
    cc1_result['rung_number'] = detection_result['rung_number']

    return cc1_result


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_46_programwise(input_program_file:str) -> pd.DataFrame:

    logger.info("Starting execution of Rule 46")

    try:

        program_df = pd.read_csv(input_program_file)

        unique_program_values = program_df["PROGRAM"].unique()

        output_rows = []
        for program_name in unique_program_values:
            logger.info(f"Executing rule 46 in Program {program_name}")

            current_program_df = program_df[program_df['PROGRAM']==program_name]

            unique_section_values = current_program_df['BODY'].unique()

            for section_name in unique_section_values:
                    
                if "fault" not in section_name.lower():

                    current_section_df = current_program_df[current_program_df['BODY']==section_name]

                    # Run detection range logic as per Rule 24
                    detection_result = detection_range_programwise(
                        current_section_df=current_section_df,
                        program_name = program_name,
                        section_name=section_name
                    )

                    cc1_result = check_detail_1_programwise(detection_result=detection_result, program_name=program_name)

                    ng_name = ng_content.get(cc1_result.get('cc', '')) if cc1_result.get('status') == "NG" else ""
                    rung_number = cc1_result.get('rung_number')-1 if cc1_result.get('rung_number')!=-1 else -1
                    target_outcoil = cc1_result.get('outcoil') if cc1_result.get('outcoil') else ""

                    output_rows.append({
                        "Result": cc1_result.get('status'),
                        "Task": program_name,
                        "Section": section_name,
                        "RungNo": rung_number,
                        "Target": target_outcoil,
                        "CheckItem": rule_46_check_item,
                        "Detail": ng_name,
                        "Status" : ""
                    })

                    # output_rows.append({
                    #     "TASK_NAME": program_name,
                    #     "SECTION_NAME": section_name,
                    #     "RULE_NUMBER": "46",
                    #     "CHECK_NUMBER": 1,
                    #     "RUNG_NUMBER": -1 if rung_number < 0 else rung_number-1,
                    #     "RULE_CONTENT": rule_content_46,
                    #     "CHECK_CONTENT": check_detail_content.get('cc1'),
                    #     "STATUS": cc1_result.get('status'),
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

def execute_rule_46_functionwise(input_function_file:str) -> pd.DataFrame:

    logger.info("Starting execution of Rule 46")

    try:

        function_df = pd.read_csv(input_function_file)

        unique_function_values = function_df["FUNCTION_BLOCK"].unique()

        output_rows = []
        for function_name in unique_function_values:
            logger.info(f"Executing rule 46 in function {function_name}")

            current_function_df = function_df[function_df['FUNCTION_BLOCK']==function_name]

            unique_section_values = current_function_df['BODY_TYPE'].unique()

            for section_name in unique_section_values:
                    
                if "fault" not in section_name.lower():

                    current_section_df = current_function_df[current_function_df['BODY_TYPE']==section_name]

                    # Run detection range logic as per Rule 24
                    detection_result = detection_range_functionwise(
                        current_section_df=current_section_df,
                        function_name = function_name,
                        section_name=section_name
                    )

                    cc1_result = check_detail_1_functionwise(detection_result=detection_result, function_name=function_name)

                    ng_name = ng_content.get(cc1_result.get('cc', '')) if cc1_result.get('status') == "NG" else ""
                    rung_number = cc1_result.get('rung_number')-1 if cc1_result.get('rung_number')!=-1 else -1
                    target_outcoil = cc1_result.get('outcoil') if cc1_result.get('outcoil') else ""

                    output_rows.append({
                        "Result": cc1_result.get('status'),
                        "Task": function_name,
                        "Section": section_name,
                        "RungNo": rung_number,
                        "Target": target_outcoil,
                        "CheckItem": rule_46_check_item,
                        "Detail": ng_name,
                        "Status" : ""
                    })

                    # output_rows.append({
                    #     "TASK_NAME": function_name,
                    #     "SECTION_NAME": section_name,
                    #     "RULE_NUMBER": "46",
                    #     "CHECK_NUMBER": 1,
                    #     "RUNG_NUMBER": -1 if rung_number < 0 else rung_number-1,
                    #     "RULE_CONTENT": rule_content_46,
                    #     "CHECK_CONTENT": check_detail_content.get('cc1'),
                    #     "STATUS": cc1_result.get('status'),
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
#     program_output_file = 'Rule_46_programwise_NG_v2.csv'
#     function_output_file = 'Rule_46_functionwise_NG_v2.csv'

#     final_csv = execute_rule_46_programwise(input_program_file=input_program_file)
#     final_csv.to_csv(f"{output_folder_path}/{program_output_file}", index=False, encoding='utf-8-sig')

#     final_csv = execute_rule_46_functionwise(input_function_file=input_function_file)
#     final_csv.to_csv(f"{output_folder_path}/{function_output_file}", index=False, encoding='utf-8-sig')
