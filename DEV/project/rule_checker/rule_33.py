import ast
import json
import pandas as pd
from typing import *
import re
from loguru import logger
import polars as pl
from .extract_comment_from_variable import get_the_comment_from_function, get_the_comment_from_program
from .ladder_utils import regex_pattern_check, clean_rung_number

# ============================ Rule 33: Definitions, Content, and Configuration Details ============================
autorun_section_name = 'autorun'
autorun_section_with_star_name = "autorun★"
preparation_section_name = "preparation"

rule_33_check_item = "Rule of Judgement Circuit"
check_detail_content = {"cc1":"Check for the existence of function block instructions with “ZDS” in the name. If it does not exist in the range, it is NG.",
                        "cc2":"Check for the existence of function block instructions with “ZFC” in the name. If it does not exist in the range, it is NG."
                        }

ng_content = {"cc1":" ”preparation”または”AutoRun”セクション内にZDSが使用されていないため,流動制御が成り立っていない可能性有",
              "cc2":"”preparation”または”AutoRun”セクション内にZFCが使用されていないため,流動制御が成り立っていない可能性有"
              }


# ============================== Program and Function-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================

def check_detail_1_programwise(autorun_preparation_section_df:pd.DataFrame, zds_keyword:str, body_type_key:str):
    # pass
    status = "NG"
    zds_rung_number = -1
    section_name = ""
    block_df = autorun_preparation_section_df[autorun_preparation_section_df['OBJECT_TYPE_LIST'].str.lower() == 'block']
    for _, block_row in block_df.iterrows():
        attr = ast.literal_eval(block_row['ATTRIBUTES'])
        attr_typename = attr.get("typeName")
        if all([attr_typename!=None, attr_typename==zds_keyword]):
            status = "OK"
            zds_rung_number = block_row['RUNG']
            section_name = block_row[body_type_key]
            break

    return {
        "status":status,
        "rung_number":zds_rung_number,
        "check_number" : "cc1",
        "section_name" : section_name,
        "target_coil" : zds_keyword
    }
    

def check_detail_2_programwise(autorun_preparation_section_df:pd.DataFrame, zfc_keyword:str, body_type_key:str):
    status = "NG"
    zfc_rung_number = -1
    section_name = ""
    block_df = autorun_preparation_section_df[autorun_preparation_section_df['OBJECT_TYPE_LIST'].str.lower() == 'block']
    for _, block_row in block_df.iterrows():
        attr = ast.literal_eval(block_row['ATTRIBUTES'])
        attr_typename = attr.get("typeName")
        if all([attr_typename!=None, attr_typename==zfc_keyword]):
            status = "OK"
            zfc_rung_number = block_row['RUNG']
            section_name = block_row[body_type_key]
            break

    return {
        "status":status,
        "rung_number":zfc_rung_number,
        "check_number" : "cc2",
        "section_name" : section_name,
        "target_coil" : zfc_keyword
    }

# ============================== Program and Function-Wise Execution Starts Here ===============================
def execute_rule_33(input_program_file:str, 
                    input_image:str,
                    program_key : str,
                    body_type_key : str
                    ) -> pd.DataFrame:

    logger.info("Rule 1 Start executing rule 1 program wise")
    output_rows = []

    try:
        program_df = pd.read_csv(input_program_file)

        input_image_program_df = pd.read_csv(input_image)

        task_names = input_image_program_df[
                input_image_program_df["Process type"].astype(str).str.lower().isin(["processing", "inspection"])
            ]["Task name"].astype(str).str.lower().tolist()
        
        unique_program_values = program_df[program_key].unique()
        for program in unique_program_values:
            if program.lower() in task_names:
                autorun_preparation_section_df = program_df[program_df[body_type_key].str.lower().isin([autorun_section_name, autorun_section_with_star_name, preparation_section_name])]
                cc1_results = check_detail_1_programwise(autorun_preparation_section_df=autorun_preparation_section_df, zds_keyword="FlowControlDataJudge_ZDS", body_type_key=body_type_key)
                cc2_results = check_detail_2_programwise(autorun_preparation_section_df=autorun_preparation_section_df, zfc_keyword = "FlowControlDataWrite_ZFC", body_type_key=body_type_key)

                all_cc_status = [cc1_results, cc2_results]
                for index, cc_status in enumerate(all_cc_status):
                    ng_name = ng_content.get(cc_status.get('check_number', '')) if cc_status.get('status') == "NG" else ""
                    rung_number = cc_status.get('rung_number')-1 if cc_status.get('rung_number')!=-1 else -1
 
                    output_rows.append({
                        "Result": cc_status.get('status'),
                        "Task": program,
                        "Section": cc_status.get("section_name"),
                        "RungNo": rung_number,
                        "Target": cc_status.get('target_coil'),
                        "CheckItem": rule_33_check_item,
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
        logger.error(f"Rule 33 Error : {e}")

        return {"status":"NOT OK", "error":str(e)}
