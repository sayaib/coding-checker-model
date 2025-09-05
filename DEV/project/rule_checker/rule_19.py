import ast
import json
import pandas as pd
from typing import *
import re
from loguru import logger
import polars as pl
from loguru import logger
from .rule_19_ladder_utils import get_series_contacts
from .extract_comment_from_variable import get_the_comment_from_function, get_the_comment_from_program
from .ladder_utils import regex_pattern_check, clean_rung_number

# ============================================ Comments referenced in Rule 19 processing ============================================
# memory feed Complete （記憶送り完了）/memory feed timing （記憶送りタイミング）/ memory shift timing （記憶シフトタイミング）/ memory shift Complete （記憶シフト完了）
section_name = 'MemoryFeeding'
start_comment = "開始"
memory_comment = "記憶"
transport_comment = "搬送"
position_comment = "位置"
end_comment = "端"

# ============================ Rule 19: Definitions, Content, and Configuration Details ============================
rule_content_19 = "This 「transport start memory」 shall be reset by AND of 「gripper transport end」 and「position hold end.」"
rule_19_check_item = "Rule of Memoryfeeding(Gripper Transfer)"
check_detail_content = {"cc1":"When ③ is not found in the detection target in ①, it is set to NG.", 
                        "cc2":"Confirm that the out-coil condition detected in (3) consists of an AND circuit (series curcuit) with “A contact that contains the variable comments of ”搬送(transport)“ and one of the variables in ※1” and “A contact that contains the variable comments of ”位置(position)” and ”端(end)'. Confirm that the circuit is composed of a series curcuit. ※１　”位置(position)”,”POS”,”端(end)'"}
ng_content = {"cc1":"Gripper搬送回路だが,記憶送り回路がコーディング基準に沿っていない(開始記憶無し) Gripper transport circuit, but the memory feed circuit does not follow coding standards (no start memoryl).", 
              "cc2":"搬送回路だが,記憶送り回路がコーディング基準に沿っていない(搬送端と位置決め端なし)Gripper transport circuit, but the memory feed circuit does not follow the coding standard (no transport end and position end )."}

# ============================ Helper Functions for Program-Wise Operations ============================
def check_start_memory_outcoil_from_program(row, program_name:str, start_comment:str, memory_comment, program_comment_data:dict):
    try:
        attr = ast.literal_eval(row['ATTRIBUTES'])
        if 'operand' in attr and 'latch' in attr:
            if attr['latch'] == 'reset':
                comment = get_the_comment_from_program(attr['operand'], program_name, program_comment_data)
                if isinstance(comment, list):
                    if regex_pattern_check(start_comment, comment) and regex_pattern_check(memory_comment, comment):
                        return attr['operand']
    except Exception:
        return None
    return None


# ============================ Helper Functions for Function-Wise Operations ============================
def check_start_memory_outcoil_from_function(row, function_name:str, start_comment:str, memory_comment, function_comment_data:dict):
    try:
        attr = ast.literal_eval(row['ATTRIBUTES'])
        if 'operand' in attr and 'latch' in attr:
            if attr['latch'] == 'reset':
                comment = get_the_comment_from_function(attr['operand'], function_name, function_comment_data)
                if isinstance(comment, list):
                    if regex_pattern_check(start_comment, comment) and regex_pattern_check(memory_comment, comment):
                        return attr['operand']
    except Exception:
        return None
    return None


# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================

def extract_rung_group_data_programwise(program_df:pd.DataFrame, program_name:str, section_name:str) -> pd.DataFrame:
    logger.info(f"Group Rung and filter data on program {program_name} and section name {section_name}")
    program_rows = program_df[program_df['PROGRAM'] == program_name].copy()
    memory_feeding_section_rows = program_rows[program_rows['BODY'] == section_name]
    rung_groups_df = memory_feeding_section_rows.groupby('RUNG')
    return rung_groups_df

def detection_range_programwise(memory_feeding_rung_groups_df: pd.DataFrame, start_comment:str, memory_comment:str, program_name:str, program_comment_data:str, section_name:str) -> dict:
    
    logger.info(f"Executing detection range on program {program_name} and section name {section_name} on rule 19")

    memory_start_comment_operand = False
    memory_start_comment_rung_number = -1

    for _, rung_df in memory_feeding_rung_groups_df:

        rung_df['start_memory_reset_check_outcoil'] = rung_df.apply(
            lambda row: check_start_memory_outcoil_from_program(row=row, program_name=program_name, start_comment=start_comment, memory_comment=memory_comment, program_comment_data=program_comment_data) if row['OBJECT_TYPE_LIST'] == 'Coil' else None,
            axis=1
        )

        start_memory_match_outcoil = rung_df[rung_df['start_memory_reset_check_outcoil'].notna()]
        if not start_memory_match_outcoil.empty:
            memory_start_comment_operand = start_memory_match_outcoil.iloc[0]['start_memory_reset_check_outcoil']
            memory_start_comment_rung_number = start_memory_match_outcoil.iloc[0]['RUNG'] 

        if memory_start_comment_operand:
            return {
                        "start_memory":[memory_start_comment_operand, memory_start_comment_rung_number], 
                    }
        
    return {
            "start_memory":[memory_start_comment_operand, memory_start_comment_rung_number], 
        }   


def check_detail_1_programwise(detection_result:dict) -> dict:

    logger.info(f"Executing check detail 1 for rule no 19")
    
    cc1_result = {}
    outcoil = []

    cc1_result['status'] = "NG" if detection_result['start_memory'][1] == -1 else "OK"

    outcoil = detection_result['start_memory'][0]

    cc1_result['cc'] = "cc1"
    # cc1_result['start_memory'] = detection_result['start_memory']
    cc1_result['outcoil'] = outcoil
    cc1_result['rung_number'] = detection_result['start_memory'][1]
    

    return cc1_result

def check_detail_2_programwise(program_name:str, program_comment_data:str,  memory_feeding_section_df: pd.DataFrame, reset_start_memory_operand:str, reset_start_memory_rung_number:str):
    if reset_start_memory_rung_number != -1:

        transport_postion_end_contact_operand = None
        position_end_operand = None
        series_contact_status = False

        reset_start_memory_rung_df = memory_feeding_section_df[memory_feeding_section_df['RUNG']==reset_start_memory_rung_number]
        for _ , rung_row in reset_start_memory_rung_df.iterrows():
            attr = ast.literal_eval(rung_row['ATTRIBUTES'])
            contact_operand = attr.get('operand')
            contact_comment = get_the_comment_from_program(contact_operand, program_name, program_comment_data)

            if isinstance(contact_comment, list):
                if rung_row['OBJECT_TYPE_LIST'] == "Contact" and attr.get('negated')=='false' and regex_pattern_check(transport_comment, contact_comment) and (regex_pattern_check(position_comment, contact_comment) or regex_pattern_check(end_comment, contact_comment)):
                    transport_postion_end_contact_operand = contact_operand

                if rung_row['OBJECT_TYPE_LIST'] == "Contact" and attr.get('negated')=='false' and regex_pattern_check(position_comment, contact_comment) and regex_pattern_check(end_comment, contact_comment):
                    position_end_operand = contact_operand

        reset_start_memory_rung_df_polar = pl.from_pandas(reset_start_memory_rung_df)
        series_contact_data = get_series_contacts(reset_start_memory_rung_df_polar)
        series_contact_operands_only = [[item['operand'] for item in sublist] for sublist in series_contact_data]

        for series_contact in series_contact_operands_only:
            if transport_postion_end_contact_operand in series_contact and position_end_operand in series_contact:
                series_contact_status = True
                break

        if transport_postion_end_contact_operand and position_end_operand and series_contact_status:
            # return "OK", [transport_postion_end_contact_operand, position_end_operand]
            return {
                "cc": "cc2",
                "status": "OK",
                "outcoil": [transport_postion_end_contact_operand, position_end_operand],
                "rung_number" :reset_start_memory_rung_number
            }
    
    # return "NG", [None, None]
    return {
        "cc": "cc2",
        "status": "NG",
        "outcoil": [None, None],
        "rung_number" : -1
    }

       
def store_program_csv_results_programwise(output_rows:List, all_cc_status:List, program_name:str, section_name:str, ng_content:dict, check_detail_content:str) -> List:

    logger.info(f"Storing all result in output csv file")

    for index, cc_status in enumerate(all_cc_status):
        ng_name = ng_content.get(cc_status.get('cc', '')) if cc_status.get('status') == "NG" else ""
        rung_number = cc_status.get('rung_number')-1 if cc_status.get('rung_number') else -1
        target_outcoil = cc_status.get('outcoil') if cc_status.get('outcoil') else ""

        output_rows.append({
            "Result": cc_status.get('status'),
            "Task": program_name,
            "Section": section_name,
            "RungNo": rung_number,
            "Target": target_outcoil,
            "CheckItem": rule_19_check_item,
            "Detail": ng_name,
            "Status" : ""
        })
        # output_rows.append({
        #     "TASK_NAME": program_name,
        #     "SECTION_NAME": section_name,
        #     "RULE_NUMBER": "19",
        #     "CHECK_NUMBER": index + 1,
        #     "RUNG_NUMBER": -1 if rung_number < 0 else rung_number,
        #     "RULE_CONTENT": rule_content_19,
        #     "CHECK_CONTENT": check_detail_content.get(cc_status.get('cc', '')),
        #     "STATUS": cc_status.get('status'),
        #     "Target_outcoil" : target_outcoil,
        #     "NG_EXPLANATION": ng_name
        # })

    return output_rows


# ============================== Function-Wise Function Definitions ===============================
# These functions perform operations specific to each function, supporting rule validations and logic checks.
# ===============================================================================================

def extract_rung_group_data_functionwise(function_df:pd.DataFrame, function_name:str, section_name:str) -> pd.DataFrame:
    logger.info(f"Group Rung and filter data on function {function_name} and section name {section_name}")
    function_rows = function_df[function_df['FUNCTION_BLOCK'] == function_name].copy()
    memory_feeding_section_rows = function_rows[function_rows['BODY_TYPE'] == section_name]
    rung_groups_df = memory_feeding_section_rows.groupby('RUNG')
    return rung_groups_df

def detection_range_functionwise(memory_feeding_rung_groups_df: pd.DataFrame, start_comment:str, memory_comment:str, function_name:str, function_comment_data:str, section_name:str) -> dict:
    
    logger.info(f"Executing detection range on function {function_name} and section name {section_name} on rule 19")

    memory_start_comment_operand = False
    memory_start_comment_rung_number = -1

    for _, rung_df in memory_feeding_rung_groups_df:

        rung_df['start_memory_reset_check_outcoil'] = rung_df.apply(
            lambda row: check_start_memory_outcoil_from_function(row=row, function_name=function_name, start_comment=start_comment, memory_comment=memory_comment, function_comment_data=function_comment_data) if row['OBJECT_TYPE_LIST'] == 'Coil' else None,
            axis=1
        )

        start_memory_match_outcoil = rung_df[rung_df['start_memory_reset_check_outcoil'].notna()]
        if not start_memory_match_outcoil.empty:
            memory_start_comment_operand = start_memory_match_outcoil.iloc[0]['start_memory_reset_check_outcoil']
            memory_start_comment_rung_number = start_memory_match_outcoil.iloc[0]['RUNG'] 

        if memory_start_comment_operand:
            return {
                        "start_memory":[memory_start_comment_operand, memory_start_comment_rung_number], 
                    }
        
    return {
            "start_memory":[memory_start_comment_operand, memory_start_comment_rung_number], 
        }   


def check_detail_1_functionwise(detection_result:dict) -> dict:

    logger.info(f"Executing check detail 1 for rule no 19")
    
    cc1_result = {}
    outcoil = []

    cc1_result['status'] = "NG" if detection_result['start_memory'][1] == -1 else "OK"

    outcoil = detection_result['start_memory'][0]

    cc1_result['cc'] = "cc1"
    # cc1_result['start_memory'] = detection_result['start_memory']
    cc1_result['outcoil'] = outcoil
    cc1_result['rung_number'] = detection_result['start_memory'][1]
    

    return cc1_result

def check_detail_2_functionwise(function_name:str, function_comment_data:str,  memory_feeding_section_df: pd.DataFrame, reset_start_memory_operand:str, reset_start_memory_rung_number:str):
    if reset_start_memory_rung_number != -1:

        transport_postion_end_contact_operand = None
        position_end_operand = None

        reset_start_memory_rung_df = memory_feeding_section_df[memory_feeding_section_df['RUNG']==reset_start_memory_rung_number]
        for _ , rung_row in reset_start_memory_rung_df.iterrows():
            attr = ast.literal_eval(rung_row['ATTRIBUTES'])
            contact_operand = attr.get('operand')
            contact_comment = get_the_comment_from_program(contact_operand, function_name, function_comment_data)
            if isinstance(contact_comment, list):
                if rung_row['OBJECT_TYPE_LIST'] == "Contact" and attr.get('negated')=='false' and regex_pattern_check(transport_comment, contact_comment) and (regex_pattern_check(position_comment, contact_comment) or regex_pattern_check(end_comment, contact_comment)):
                    transport_postion_end_contact_operand = contact_operand

                if rung_row['OBJECT_TYPE_LIST'] == "Contact" and attr.get('negated')=='false' and regex_pattern_check(position_comment, contact_comment) and regex_pattern_check(end_comment, contact_comment):
                    position_end_operand = contact_operand

        reset_start_memory_rung_df_polar = pl.from_pandas(reset_start_memory_rung_df)
        series_contact_data = get_series_contacts(reset_start_memory_rung_df_polar)
        series_contact_operands_only = [[item['operand'] for item in sublist] for sublist in series_contact_data]

        for series_contact in series_contact_operands_only:
            if transport_postion_end_contact_operand in series_contact and position_end_operand in series_contact:
                series_contact_status = True
                break

        if transport_postion_end_contact_operand and position_end_operand and series_contact_status:
            # return "OK", [transport_postion_end_contact_operand, position_end_operand]
            return {
                "cc": "cc2",
                "status": "OK",
                "outcoil": [transport_postion_end_contact_operand, position_end_operand],
                "rung_number" :reset_start_memory_rung_number
            }
    # return "NG", [None, None]
    return {
        "cc": "cc2",
        "status": "NG",
        "outcoil": [None, None],
        "rung_number" : -1
    }

       
def store_function_csv_results_functionwise(output_rows:List, all_cc_status:List, function_name:str, section_name:str, ng_content:dict, check_detail_content:str) -> List:

    logger.info(f"Storing all result in output csv file")

    for index, cc_status in enumerate(all_cc_status):
        ng_name = ng_content.get(cc_status.get('cc', '')) if cc_status.get('status') == "NG" else ""
        rung_number = cc_status.get('rung_number')-1 if cc_status.get('rung_number') else -1
        target_outcoil = cc_status.get('outcoil') if cc_status.get('outcoil') else ""

        output_rows.append({
            "Result": cc_status.get('status'),
            "Task": function_name,
            "Section": section_name,
            "RungNo": rung_number,
            "Target": target_outcoil,
            "CheckItem": rule_19_check_item,
            "Detail": ng_name,
            "Status" : ""
        })
        # output_rows.append({
        #     "TASK_NAME": function_name,
        #     "SECTION_NAME": section_name,
        #     "RULE_NUMBER": "19",
        #     "CHECK_NUMBER": index + 1,
        #     "RUNG_NUMBER": -1 if rung_number < 0 else rung_number,
        #     "RULE_CONTENT": rule_content_19,
        #     "CHECK_CONTENT": check_detail_content.get(cc_status.get('cc', '')),
        #     "STATUS": cc_status.get('status'),
        #     "Target_outcoil" : target_outcoil,
        #     "NG_EXPLANATION": ng_name
        # })

    return output_rows


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_19_programwise(input_program_file:str, 
                                input_program_comment_file:str,
                                input_image:str) -> pd.DataFrame:

    logger.info("Starting execution of Rule 19")

    try:
        program_df = pd.read_csv(input_program_file)
        input_image_program_df = pd.read_csv(input_image)
        task_names = input_image_program_df[
                input_image_program_df["Unit"].astype(str).str.lower() == "gripper"
            ]["Task name"].astype(str).str.lower().tolist()
        
        with open(input_program_comment_file, 'r', encoding="utf-8") as file:
            program_comment_data = json.load(file)

        unique_program_values = program_df["PROGRAM"].unique()

        output_rows = []
        for program in unique_program_values:

            if program.lower() in task_names:

                logger.info(f"Executing in Program {program} and section {section_name}")
                program_rows = program_df[program_df['PROGRAM'] == program].copy()
                memory_feeding_section_df = program_rows[program_rows['BODY'] == section_name]

                # Extract rung group data filtered by program and section name, grouped by rung number
                memory_feeding_rung_groups_df = extract_rung_group_data_programwise(
                    program_df=program_df,
                    program_name=program,
                    section_name=section_name
                )
                # Run detection range logic as per Rule 19
                detection_result = detection_range_programwise(
                    memory_feeding_rung_groups_df=memory_feeding_rung_groups_df,
                    start_comment=start_comment,
                    memory_comment=memory_comment,
                    program_name=program,
                    program_comment_data=program_comment_data,
                    section_name=section_name
                )

                cc1_result = check_detail_1_programwise(detection_result=detection_result)

                reset_start_memory_operand = detection_result['start_memory'][0]
                reset_start_memory_rung_number = detection_result['start_memory'][1]

                cc2_result = check_detail_2_programwise(
                    program_name=program,
                    program_comment_data=program_comment_data,
                    memory_feeding_section_df=memory_feeding_section_df,
                    reset_start_memory_operand=reset_start_memory_operand,
                    reset_start_memory_rung_number = reset_start_memory_rung_number
                )
                # Store all CC results in a list
                all_cc_status = [cc1_result, cc2_result]

                # Save output to CSV
                output_rows = store_program_csv_results_programwise(
                    output_rows=output_rows,
                    all_cc_status=all_cc_status,
                    program_name=program,
                    section_name=section_name,
                    ng_content=ng_content,
                    check_detail_content=check_detail_content
                )

        final_output_df = pd.DataFrame(output_rows)

        if not final_output_df.empty:
            if 'RungNo' in final_output_df.columns:
                final_output_df['RungNo'] = final_output_df['RUNG_NUMBER'].apply(clean_rung_number)
        else:
            final_output_df = pd.DataFrame(columns=["Result","Task","Section","RungNo","Target","CheckItem","Detail","Status"])

        return {"status":"OK", "output_df":final_output_df}
        
    except Exception as e:
        logger.error(f"Rule 19 Error : {e}")

        return {"status":"NOT OK", "error":e}



# ============================== Function-Wise Execution Starts Here ===============================
def execute_rule_19_functionwise(input_function_file:str, 
                                 input_function_comment_file:str,
                                 input_image:str) -> pd.DataFrame:

    logger.info("Starting execution of Rule 19")

    try:

        function_df = pd.read_csv(input_function_file)
        input_image_function_df = pd.read_csv(input_image)
        task_names = input_image_function_df[
                input_image_function_df["Unit"].astype(str).str.lower() == "gripper"
            ]["Task name"].astype(str).str.lower().tolist()
        
        with open(input_function_comment_file, 'r', encoding="utf-8") as file:
            function_comment_data = json.load(file)

        unique_function_values = function_df["FUNCTION_BLOCK"].unique()

        output_rows = []
        for function in unique_function_values:

            if function.lower() in task_names:

                logger.info(f"Executing in function {function} and section {section_name}")
                function_rows = function_df[function_df['FUNCTION_BLOCK'] == function].copy()
                memory_feeding_section_df = function_rows[function_rows['BODY_TYPE'] == section_name]

                # Extract rung group data filtered by function and section name, grouped by rung number
                memory_feeding_rung_groups_df = extract_rung_group_data_functionwise(
                    function_df=function_df,
                    function_name=function,
                    section_name=section_name
                )
                # Run detection range logic as per Rule 19
                detection_result = detection_range_functionwise(
                    memory_feeding_rung_groups_df=memory_feeding_rung_groups_df,
                    start_comment=start_comment,
                    memory_comment=memory_comment,
                    function_name=function,
                    function_comment_data=function_comment_data,
                    section_name=section_name
                )

                cc1_result = check_detail_1_functionwise(detection_result=detection_result)

                reset_start_memory_operand = detection_result['start_memory'][0]
                reset_start_memory_rung_number = detection_result['start_memory'][1]

                cc2_result = check_detail_2_functionwise(
                    function_name=function,
                    function_comment_data=function_comment_data,
                    memory_feeding_section_df=memory_feeding_section_df,
                    reset_start_memory_operand=reset_start_memory_operand,
                    reset_start_memory_rung_number = reset_start_memory_rung_number
                )
                # Store all CC results in a list
                all_cc_status = [cc1_result, cc2_result]

                # Save output to CSV
                output_rows = store_function_csv_results_functionwise(
                    output_rows=output_rows,
                    all_cc_status=all_cc_status,
                    function_name=function,
                    section_name=section_name,
                    ng_content=ng_content,
                    check_detail_content=check_detail_content
                )
        
        final_output_df = pd.DataFrame(output_rows)

        if not final_output_df.empty:
            if 'RungNo' in final_output_df.columns:
                final_output_df['RungNo'] = final_output_df['RUNG_NUMBER'].apply(clean_rung_number)
        else:
            final_output_df = pd.DataFrame(columns=["Result","Task","Section","RungNo","Target","CheckItem","Detail","Status"])

        return {"status":"OK", "output_df":final_output_df}
        
    except Exception as e:
        logger.error(f"Rule 19 Error : {e}")

        return {"status":"NOT OK", "error":e}
    
# if __name__=='__main__':

#     input_program_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Data_Modelling/version3/data_model_Rule_19_NG/data_model_Rule_19_NG_programwise.csv"
#     input_program_comment_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Data_Modelling/version3/data_model_Rule_19_NG/data_model_Rule_19_NG_programwise.json"
#     input_function_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Data_Modelling/version3/data_model_Rule_19_NG/data_model_Rule_19_NG_functionwise.csv"
#     input_function_comment_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Data_Modelling/version3/data_model_Rule_19_NG/data_model_Rule_19_NG_functionwise.json"
#     output_folder_path = 'C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Rules_implementation/pythoncode/output_csv/'
#     program_output_file = 'Rule_19_NG_program.csv'
#     function_output_file = 'Rule_19_NG_function.csv'


#     final_csv = execute_rule_19_programwise(input_program_file=input_program_file, input_program_comment_file=input_program_comment_file)
#     final_csv.to_csv(f"{output_folder_path}/{program_output_file}", index=False, encoding='utf-8-sig')

#     final_csv = execute_rule_19_functionwise(input_function_file=input_function_file, input_function_comment_file=input_function_comment_file)
#     final_csv.to_csv(f"{output_folder_path}/{function_output_file}", index=False, encoding='utf-8-sig')
