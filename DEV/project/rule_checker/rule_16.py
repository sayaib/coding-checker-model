import ast
import json
import pandas as pd
from typing import *
import re
from loguru import logger
import polars as pl
from .extract_comment_from_variable import get_the_comment_from_program, get_the_comment_from_function
from .ladder_utils import regex_pattern_check, clean_rung_number
from .rule_16_ladder_utils import get_series_contacts

# ============================ Rule 16: Definitions, Content, and Configuration Details ============================
autorun_section_name = 'autorun'
autorun_section_with_star_name = "autorun★"

rule_16_check_item = "Rule of Judgement Circuit"
check_detail_content = {"cc1":"Check that the outcoil conditions detected in ③.1 satisfy all of the following.",
                        "cc2":"Check that the B contact of the out-coil detected in ③,1 exists in the condition of the out-coil detected in ③.2."
                        }

ng_content = {"cc1":"判定回路において、正しいOK判定(OK信号のONとNG信号のOFFのAND回路)になっていないためNG",
              "cc2":"判定回路において、NG判定であるのにOK判定信号のB接点が存在しない(排他がとれていない)ためNG"
              }

# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================

def check_two_outcoil_exist_ok_ng_programwise(current_rung_df:pd.DataFrame, program_name:str, program_comment_data:dict):
    coil_df = current_rung_df[current_rung_df['OBJECT_TYPE_LIST'].str.lower() == 'coil']
    ok_status = False
    ng_status = False
    if not coil_df.empty and len(coil_df) >= 2:
        for _, coil_row in coil_df.iterrows():
            attr = ast.literal_eval(coil_row['ATTRIBUTES'])
            coil_operand = attr.get('operand')

            if isinstance(coil_operand,str) and coil_operand:
                coil_comment = get_the_comment_from_program(coil_operand, program_name, program_comment_data)
                if isinstance(coil_comment, list) and coil_comment:
                    if regex_pattern_check("OK", coil_comment) or regex_pattern_check("正常", coil_comment):
                        ok_status = True
                        ok_operand = coil_operand
                    
                    if regex_pattern_check("NG", coil_comment) or regex_pattern_check("異常", coil_comment):
                        ng_status = True
                        ng_operand = coil_operand
                
            if ok_status and ng_status:
                break
    
    if ok_status and ng_status:
        return True, {"OK":ok_operand, "NG":ng_operand}
    else:
        return False, {}
                                     

def check_detail_1_programwise(current_rung_df:pd.DataFrame, program_name:str, program_comment_data:dict, ok_operand:str):
    contact_df = current_rung_df[current_rung_df["OBJECT_TYPE_LIST"].str.lower() == 'contact']
    status = "NG"

    check_1_1 = check_1_2 = check_1_3 = False 
    all_match_operand = []
    all_match_operand_outlist = []

    """
    getting code for checking all three contact exist and store it outlist to compare that it should exist within self-holding 
    """
    for _, contact_row in contact_df.iterrows():
        attr = ast.literal_eval(contact_row['ATTRIBUTES'])
        contact_operand = attr.get('operand')
        negated_operand = attr.get('negated')

        if check_1_1 and check_1_2 and check_1_3:
            break

        if isinstance(contact_operand, str) and contact_operand:
            contact_comment = get_the_comment_from_program(contact_operand, program_name, program_comment_data)

            if isinstance(contact_comment, list) and contact_comment:
                if regex_pattern_check("OK", contact_comment) or regex_pattern_check("正常", contact_comment) and negated_operand=='false' and not check_1_1:
                    check_1_1 = True
                    all_match_operand.append(contact_operand)
                    all_match_operand_outlist.extend(attr.get("out_list"))
                    continue
                
                elif regex_pattern_check("NG", contact_comment) or regex_pattern_check("異常", contact_comment) and negated_operand=='true' and not check_1_2:
                    check_1_2 = True
                    all_match_operand.append(contact_operand)
                    all_match_operand_outlist.extend(attr.get("out_list"))
                    continue

                elif (regex_pattern_check("異常", contact_comment) and negated_operand=='true') or (regex_pattern_check("異常", contact_comment) and regex_pattern_check("でない", contact_comment) and negated_operand=='false') and not check_1_3:
                    check_1_3 = True
                    all_match_operand.append(contact_operand)
                    all_match_operand_outlist.extend(attr.get("out_list"))
                    continue
                
                else:
                    continue
        
    """
    getting self holding contact outlist
    """
    selfholding_outlist = []
    for _, contact_row in contact_df.iterrows():
        attr = ast.literal_eval(contact_row['ATTRIBUTES'])
        contact_operand = attr.get('operand')
        negated_operand = attr.get('negated')

        if isinstance(contact_operand, str) and contact_operand:
            if contact_operand == ok_operand:
                selfholding_outlist.extend(attr.get('out_list'))
                break

    if check_1_1 and check_1_2 and check_1_3:
        """ Here all values are converting into int"""
        all_match_operand_outlist = [int(x) for x in all_match_operand_outlist]
        selfholding_outlist = [int(x) for x in selfholding_outlist]

        print("all_match_operand_outlist",all_match_operand_outlist)
        print("selfholding_outlist",selfholding_outlist)

        """get maximum number from selfholding element"""
        max_self = -1
        if selfholding_outlist:
            max_self = max(selfholding_outlist)

        """
        checking all three cases 
        1. 3 operand should be there where all condition match
        2. all item should be in inside selfholding using outlist logic
        3. check all item in series using get_series_contact logic
        """
        if len(all_match_operand) == 3 and all(item <= max_self for item in all_match_operand_outlist):
            get_series_connect_data = get_series_contacts(pl.from_pandas(current_rung_df))
            series_contact_operands_only = [[item.get('operand') for item in sublist] for sublist in get_series_connect_data]
            for series_contact in series_contact_operands_only:
                all_series_present = all(item in series_contact for item in all_match_operand)
                if all_series_present:
                    status = "OK"
                    break
    
    return {
        "status" : status,
        "check_number" : "cc1"
    }
    

def check_detail_2_programwise(current_rung_df:pd.DataFrame, ok_operand:str, ng_operand:str):
    pass

    contact_match_found = False
    status = "NG"

    for _, curr_row in current_rung_df.iterrows():
        attr = ast.literal_eval(curr_row['ATTRIBUTES'])
        operand = attr.get('operand')
        negated_operand = attr.get('negated')

        if contact_match_found and curr_row['OBJECT_TYPE_LIST'].lower() == 'coil' and operand == ng_operand:
            status = "OK"
            break

        if curr_row['OBJECT_TYPE_LIST'].lower() == 'coil':
            contact_match_found = False
        
        if operand == ok_operand and negated_operand=='true':
            contact_match_found = True


    return {
        "status" : status,
        "check_number" : "cc2"
    }  



# ============================== Function-Wise Function Definitions ===============================
# These functions perform operations specific to each function, supporting rule validations and logic checks.
# ===============================================================================================

def check_two_outcoil_exist_ok_ng_functionwise(current_rung_df:pd.DataFrame, function_name:str, function_comment_data:dict):
    coil_df = current_rung_df[current_rung_df['OBJECT_TYPE_LIST'].str.lower() == 'coil']
    ok_status = False
    ng_status = False
    if not coil_df.empty and len(coil_df) >= 2:
        for _, coil_row in coil_df.iterrows():
            attr = ast.literal_eval(coil_row['ATTRIBUTES'])
            coil_operand = attr.get('operand')

            if isinstance(coil_operand,str) and coil_operand:
                coil_comment = get_the_comment_from_function(coil_operand, function_name, function_comment_data)
                if isinstance(coil_comment, list) and coil_comment:
                    if regex_pattern_check("OK", coil_comment) or regex_pattern_check("正常", coil_comment):
                        ok_status = True
                        ok_operand = coil_operand
                    
                    if regex_pattern_check("NG", coil_comment) or regex_pattern_check("異常", coil_comment):
                        ng_status = True
                        ng_operand = coil_operand
                
            if ok_status and ng_status:
                break
    
    if ok_status and ng_status:
        return True, {"OK":ok_operand, "NG":ng_operand}
    else:
        return False, {}
                                     

def check_detail_1_functionwise(current_rung_df:pd.DataFrame, function_name:str, function_comment_data:dict, ok_operand:str):
    contact_df = current_rung_df[current_rung_df["OBJECT_TYPE_LIST"].str.lower() == 'contact']
    status = "NG"

    check_1_1 = check_1_2 = check_1_3 = False 
    all_match_operand = []
    all_match_operand_outlist = []

    """
    getting code for checking all three contact exist and store it outlist to compare that it should exist within self-holding 
    """
    for _, contact_row in contact_df.iterrows():
        attr = ast.literal_eval(contact_row['ATTRIBUTES'])
        contact_operand = attr.get('operand')
        negated_operand = attr.get('negated')

        if check_1_1 and check_1_2 and check_1_3:
            break

        if isinstance(contact_operand, str) and contact_operand:
            contact_comment = get_the_comment_from_function(contact_operand, function_name, function_comment_data)

            if isinstance(contact_comment, list) and contact_comment:
                if regex_pattern_check("OK", contact_comment) or regex_pattern_check("正常", contact_comment) and negated_operand=='false' and not check_1_1:
                    check_1_1 = True
                    all_match_operand.append(contact_operand)
                    all_match_operand_outlist.extend(attr.get("out_list"))
                    continue
                
                elif regex_pattern_check("NG", contact_comment) or regex_pattern_check("異常", contact_comment) and negated_operand=='true' and not check_1_2:
                    check_1_2 = True
                    all_match_operand.append(contact_operand)
                    all_match_operand_outlist.extend(attr.get("out_list"))
                    continue

                elif (regex_pattern_check("異常", contact_comment) and negated_operand=='true') or (regex_pattern_check("異常", contact_comment) and regex_pattern_check("でない", contact_comment) and negated_operand=='false') and not check_1_3:
                    check_1_3 = True
                    all_match_operand.append(contact_operand)
                    all_match_operand_outlist.extend(attr.get("out_list"))
                    continue
                
                else:
                    continue
        
    """
    getting self holding contact outlist
    """
    selfholding_outlist = []
    for _, contact_row in contact_df.iterrows():
        attr = ast.literal_eval(contact_row['ATTRIBUTES'])
        contact_operand = attr.get('operand')
        negated_operand = attr.get('negated')

        if isinstance(contact_operand, str) and contact_operand:
            if contact_operand == ok_operand:
                selfholding_outlist.extend(attr.get('out_list'))
                break

    if check_1_1 and check_1_2 and check_1_3:
        """ Here all values are converting into int"""
        all_match_operand_outlist = [int(x) for x in all_match_operand_outlist]
        selfholding_outlist = [int(x) for x in selfholding_outlist]

        print("all_match_operand_outlist",all_match_operand_outlist)
        print("selfholding_outlist",selfholding_outlist)

        """get maximum number from selfholding element"""
        max_self = -1
        if selfholding_outlist:
            max_self = max(selfholding_outlist)

        """
        checking all three cases 
        1. 3 operand should be there where all condition match
        2. all item should be in inside selfholding using outlist logic
        3. check all item in series using get_series_contact logic
        """
        if len(all_match_operand) == 3 and all(item <= max_self for item in all_match_operand_outlist):
            get_series_connect_data = get_series_contacts(pl.from_pandas(current_rung_df))
            series_contact_operands_only = [[item.get('operand') for item in sublist] for sublist in get_series_connect_data]
            for series_contact in series_contact_operands_only:
                all_series_present = all(item in series_contact for item in all_match_operand)
                if all_series_present:
                    status = "OK"
                    break
    
    return {
        "status" : status,
        "check_number" : "cc1"
    }
    

def check_detail_2_functionwise(current_rung_df:pd.DataFrame, ok_operand:str, ng_operand:str):
    pass

    contact_match_found = False
    status = "NG"

    for _, curr_row in current_rung_df.iterrows():
        attr = ast.literal_eval(curr_row['ATTRIBUTES'])
        operand = attr.get('operand')
        negated_operand = attr.get('negated')

        if contact_match_found and curr_row['OBJECT_TYPE_LIST'].lower() == 'coil' and operand == ng_operand:
            status = "OK"
            break

        if curr_row['OBJECT_TYPE_LIST'].lower() == 'coil':
            contact_match_found = False
        
        if operand == ok_operand and negated_operand=='true':
            contact_match_found = True


    return {
        "status" : status,
        "check_number" : "cc2"
    }  


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_16_programwise(input_program_file:str, 
                                program_comment_file:str
                                ) -> pd.DataFrame:

    logger.info("Rule 37 Start executing rule 1 program wise")
    output_rows = []

    try:

        program_df = pd.read_csv(input_program_file)
        with open(program_comment_file, 'r', encoding="utf-8") as file:
            program_comment_data = json.load(file)
        
        unique_program_values = program_df["PROGRAM"].unique()
        for program in unique_program_values:
            curr_program_df = program_df[program_df['PROGRAM'] == program]
            autorun_section_df = curr_program_df[curr_program_df['BODY'].str.lower().isin([autorun_section_name, autorun_section_with_star_name])]
            unique_rung_number = sorted(autorun_section_df['RUNG'].unique())

            for curr_rung in unique_rung_number:
                current_rung_df = autorun_section_df[autorun_section_df['RUNG']==curr_rung]
                two_outcoil_exist_status, two_coil_details = check_two_outcoil_exist_ok_ng_programwise(current_rung_df=current_rung_df, program_name=program, program_comment_data=program_comment_data)
                if two_outcoil_exist_status:
                    ok_operand = two_coil_details.get("OK")
                    ng_operand = two_coil_details.get("NG")
                    cc1_result = check_detail_1_programwise(current_rung_df=current_rung_df, program_name=program, program_comment_data=program_comment_data, ok_operand=ok_operand)
                    cc2_result = check_detail_2_programwise(current_rung_df=current_rung_df, ok_operand=ok_operand, ng_operand=ng_operand)
                    print("*"*100)
                    print("curr_rung",curr_rung)
                    print("program",program)
                    print("two_coil_details",two_coil_details)
                    print("cc1_result",cc1_result)
                    print("cc2_result",cc2_result)

                    all_cc_status = [cc1_result, cc2_result]
                    for index, cc_status in enumerate(all_cc_status):
                        ng_name = ng_content.get(cc_status.get('check_number', '')) if cc_status.get('status') == "NG" else ""
                        target_coil = ok_operand if index == 0 else ng_operand
                        output_rows.append({
                            "Result": cc_status.get('status'),
                            "Task": program,
                            "Section": "AutoRun",
                            "RungNo": curr_rung-1,
                            "Target": target_coil,
                            "CheckItem": rule_16_check_item,
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
        logger.error(f"Rule 16 Error : {e}")

        return {"status":"NOT OK", "error":str(e)}




# ============================== Function-Wise Execution Starts Here ===============================
def execute_rule_16_functionwise(input_function_file:str, 
                                function_comment_file:str
                                ) -> pd.DataFrame:

    logger.info("Rule 37 Start executing rule 1 function wise")
    output_rows = []

    try:

        function_df = pd.read_csv(input_function_file)
        with open(function_comment_file, 'r', encoding="utf-8") as file:
            function_comment_data = json.load(file)
        
        unique_function_values = function_df["FUNCTION_BLOCK"].unique()
        for function in unique_function_values:
            curr_function_df = function_df[function_df['FUNCTION_BLOCK'] == function]
            autorun_section_df = curr_function_df[curr_function_df['BODY_TYPE'].str.lower().isin([autorun_section_name, autorun_section_with_star_name])]
            unique_rung_number = sorted(autorun_section_df['RUNG'].unique())

            for curr_rung in unique_rung_number:
                current_rung_df = autorun_section_df[autorun_section_df['RUNG']==curr_rung]
                two_outcoil_exist_status, two_coil_details = check_two_outcoil_exist_ok_ng_functionwise(current_rung_df=current_rung_df, function_name=function, function_comment_data=function_comment_data)
                if two_outcoil_exist_status:
                    ok_operand = two_coil_details.get("OK")
                    ng_operand = two_coil_details.get("NG")
                    cc1_result = check_detail_1_functionwise(current_rung_df=current_rung_df, function_name=function, function_comment_data=function_comment_data, ok_operand=ok_operand)
                    cc2_result = check_detail_2_functionwise(current_rung_df=current_rung_df, ok_operand=ok_operand, ng_operand=ng_operand)
                    print("*"*100)
                    print("curr_rung",curr_rung)
                    print("function",function)
                    print("two_coil_details",two_coil_details)
                    print("cc1_result",cc1_result)
                    print("cc2_result",cc2_result)

                    all_cc_status = [cc1_result, cc2_result]
                    for index, cc_status in enumerate(all_cc_status):
                        ng_name = ng_content.get(cc_status.get('check_number', '')) if cc_status.get('status') == "NG" else ""
                        target_coil = ok_operand if index == 0 else ng_operand
                        output_rows.append({
                            "Result": cc_status.get('status'),
                            "Task": function,
                            "Section": "AutoRun",
                            "RungNo": curr_rung-1,
                            "Target": target_coil,
                            "CheckItem": rule_16_check_item,
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
        logger.error(f"Rule 16 Error : {e}")

        return {"status":"NOT OK", "error":str(e)}


            
