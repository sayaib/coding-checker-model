import ast
import json
import pandas as pd
from typing import *
import re
from loguru import logger
import polars as pl
from .extract_comment_from_variable import get_the_comment_from_function, get_the_comment_from_program
from .ladder_utils import regex_pattern_check, clean_rung_number
from .rule_40_ladder_utils import *

# ============================ Rule 1: Definitions, Content, and Configuration Details ============================
detected_function_block = ["+","-","*", "**","/","ADD","SUB","MUL","DIV","MOD","ABS","SIN","COS","TAN","ASIN","ACOS","ATAN","SQRT","LN","LOG","EXP","EXPT","INC", "DEC", "RADTODEG", "DEGTORAG"]
undetected_function_block = ["MOVE", "@MOVE", "DINT_TO_REAL"]
merge_program_function_block = detected_function_block + undetected_function_block
cols_to_clean = ["input1", "input2", "input3", "output1", "output2"]
rule_content_40 = "・Make sure that the input data used in the calculation is within the intended range before the calculation."
rule_40_check_item = "Rule of Data Check"
check_detail_content = {"cc1":"The input variable to be checked by the program is used as an input variable of the comparison function (ZoneCmp,<,>,=).", 
                        "cc2":"The input variable to be checked by the program is an external variable (the variable name contains 'PD' or 'X')."}

ng_content = {"cc1":"演算に使用しているデータの演算前チェックができていない可能性有 (There is a possibility that the data used in the calculation is not checked before the calculation.)", 
              "cc2":"演算に使用しているデータが外部信号ではない可能性有 (There is a possibility that the data used for calculation is not an external signal.)"}

# ======================================== Helper function =====================================
def convert_block_data_to_custom_format(current_program_block_data, input_keys):
    result = {}

    for idx, (index, block) in enumerate(current_program_block_data, start=1):
        cleaned_block = {}

        for func_name, entries in block.items():
            collected_vars = []

            for entry in entries:
                for key, value in entry.items():
                    if key.lower() in input_keys:
                        collected_vars.extend(value)

            if collected_vars:
                cleaned_block[func_name] = collected_vars

        if cleaned_block:
            result[f"Index {idx}"] = cleaned_block

    return result

# ============================== Both Program-Wise and Function-Wise combine in one as per need Function Definitions ===============================
def merge_program_function_csv_data(program_df:pd.DataFrame, function_df:pd.DataFrame) -> pd.DataFrame:

    logger.info("Rule 40 Merge program and function dataframe into one")

    # Rename the different columns in function_df to match program_df
    function_df_renamed = function_df.rename(columns={
        'FUNCTION_BLOCK': 'PROGRAM',
        'BODY_TYPE': 'BODY'
    })
    
    # Ensure both DataFrames have the same column order
    function_df_renamed = function_df_renamed[program_df.columns]
    
    # Concatenate both DataFrames
    merged_program_function_df = pd.concat([program_df, function_df_renamed], ignore_index=True)
    
    return merged_program_function_df

##############  check detail 1 and 2 function defined here ##############################


def check_detail_programwise(merged_program_function_df:pd.DataFrame, process_data_detection_range_df:pd.DataFrame):

    unique_proecss_data_detection_range_program = process_data_detection_range_df["task_name"].unique()

    input_cols = ['input1', 'input2', 'input3']
    input_variable_comparison_function = ['in', 'in1', "in2", "in3", "mn", "mx"]
    complete_data_list = []

    """
    collecting all block program by program as mentioned that in entire program we have to check whether zonecmp,=,<,> occur.
    then each sectionwise get input variable and check in entire program
    """
    for process_data_program_name in unique_proecss_data_detection_range_program:
        current_program_block_data = []
        process_data_program_df = merged_program_function_df[merged_program_function_df['PROGRAM']==process_data_program_name]
        unique_section_block = process_data_program_df['BODY'].unique()
        for unique_section in unique_section_block:
            unique_section_df = process_data_program_df[process_data_program_df['BODY']==unique_section]
            unique_rung_block = unique_section_df['RUNG'].unique()
            for unique_rung in unique_rung_block:
                unique_rung_df = unique_section_df[unique_section_df['RUNG']==unique_rung]
                block_data = get_block_connections(unique_rung_df)
                for index, block in enumerate(block_data):
                        current_program_block_data.append([index, block])

        current_program_process_data_df = process_data_detection_range_df[process_data_detection_range_df['task_name']==process_data_program_name]
        # 1. Filter out rows where Function_name contains "_TO_"
        remove_to_filtered_df = current_program_process_data_df[~current_program_process_data_df['function_name'].astype(str).str.lower().str.contains('_to_|move|@move')]

        ## calling convert_block_data_to_custom_format to convert block data into strucutred format
        current_block_data = convert_block_data_to_custom_format(current_program_block_data, input_variable_comparison_function)
        unique_section_filter = remove_to_filtered_df["section"].unique()

        for unique_section in unique_section_filter:

            # Seen input define here for checking input data only one time if there are duplicate data then it check only one time
            seen_inputs = []
            section_wise_df = remove_to_filtered_df[remove_to_filtered_df["section"]==unique_section]
            for _, section_row in section_wise_df.iterrows():

                sectionwise_all_input_data = []

                """
                function defined for getting input data for current row having input1, input2, input3
                """
                for field in input_cols:
                    value = section_row.get(field)
                    if value:
                        sectionwise_all_input_data.append(value)
                

                if sectionwise_all_input_data:
                    for data in sectionwise_all_input_data:
                        # here check content 1
                        if data not in seen_inputs:
                            seen_inputs.append(data)

                            prev_index_smaller = -1
                            prev_index_greater = -1

                            for index, (_, current_block_value) in enumerate(current_block_data.items()):

                                # continuous_smaller_than_two_time_exist=False
                                # continuous_greater_than_two_time_exist = False
                                input_match_block_found=False
                                for block_data_key, block_data_values in current_block_value.items():

                                    if data in block_data_values:
                                        block_key_lower = block_data_key.lower()

                                        # Case 1: Immediate match if ZoneCmp or '='
                                        if block_key_lower == 'zonecmp' or block_data_key == '=':
                                            input_match_block_found = True
                                            match_block = 'zonecmp or = '
                                            break

                                        # Case 2: Two consecutive '<' or '<='
                                        if block_data_key in ('<', '<='):
                                            if index == prev_index_smaller+1:
                                                input_match_block_found = True
                                                match_block = '< and <='
                                                break
                                            else:
                                                # continuous_smaller_than_two_time_exist = True
                                                prev_index_smaller = index
                                                continue  # check next block

                                        # Reset if a different block breaks the continuity
                                        else:
                                            prev_index_smaller = -1

                                        # Case 3: Two consecutive '>' or '>='
                                        if block_data_key in ('>', '>='):
                                            if index == prev_index_greater+1:
                                                input_match_block_found = True
                                                match_block = '> and >='
                                                break
                                            else:
                                                # continuous_greater_than_two_time_exist = True
                                                prev_index_greater = index
                                                continue  # check next block

                                        # Reset if a different block breaks the continuity
                                        else:
                                            prev_index_greater = -1

                                if input_match_block_found:
                                    break    

                            if input_match_block_found:
                                status = "OK"
                                ng_name = ''
                            else:
                                match_block=''
                                status = "NG"
                                ng_name = ng_content.get('cc1')

                            complete_data_list.append(
                                    {
                                        "Result": status,
                                        "Task": process_data_program_name,
                                        "Section": unique_section,
                                        "RungNo": section_row['rung'],
                                        "Target": data,
                                        "CheckItem": rule_40_check_item,
                                        "Detail": ng_name,
                                        "Status" : ""
                                    }
                                    #     "TASK_NAME": process_data_program_name,
                                    #     "SECTION_NAME": unique_section,
                                    #     "RULE_NUMBER": "40",
                                    #     "CHECK_NUMBER": 1,
                                    #     "RUNG_NUMBER": section_row['rung'],
                                    #     "RULE_CONTENT": rule_content_40,
                                    #     "CHECK_CONTENT" : check_detail_content.get("cc1"),
                                    #     "TARGET_OUTCOIL": data,
                                    #     "STATUS": status,
                                    #     "NG_EXPLANATION": ng_name,
                                    #     # "match_block" : match_block
                                    # }
                            )
                                        

                            ### check content 2
                            if "PD" in data or "X" in data:
                                status = "OK"
                                ng_name = ''
                            else:
                                status = "NG"
                                ng_name = ng_content.get('cc2')
                            
                            complete_data_list.append(
                                    {
                                        "Result": status,
                                        "Task": process_data_program_name,
                                        "Section": unique_section,
                                        "RungNo": section_row['rung'],
                                        "Target": data,
                                        "CheckItem": rule_40_check_item,
                                        "Detail": ng_name,
                                        "Status" : ""
                                    }
                                        # "TASK_NAME": process_data_program_name,
                                        # "SECTION_NAME": unique_section,
                                        # "RULE_NUMBER": "40",
                                        # "CHECK_NUMBER": 2,
                                        # "RUNG_NUMBER": section_row['rung'],
                                        # "RULE_CONTENT": rule_content_40,
                                        # "CHECK_CONTENT" : check_detail_content.get("cc2"),
                                        # "TARGET_OUTCOIL": data,
                                        # "STATUS": status,
                                        # "NG_EXPLANATION": ng_name,
                                        # "match_block":""
                                    # }
                            )

    return complete_data_list

# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_40_program_functionwise(input_program_file:str, input_function_file:str):
    logger.info("Rule 40 Executing Rule 40 for program and function wise data")

    try:
        all_program_function_data = []

        program_df = pd.read_csv(input_program_file)
        function_df = pd.read_csv(input_function_file)

        unique_program_names = program_df["PROGRAM"].unique()
        unique_function_names = function_df["FUNCTION_BLOCK"].unique()

        merge_program_function_names = unique_program_names.tolist() + unique_function_names.tolist()
        merged_program_function_df = merge_program_function_csv_data(program_df=program_df, function_df=function_df)

        detected_no = 1
        process_data_detection_range_list = []
        for program_function_names in merge_program_function_names:
            all_program_function_data = []
            logger.info(f"Rule 40 processing program/function: {program_function_names}")

            # if program_function_names == "P121_sample40_41":

            """
            merge both program and function in one code to apply rule for whole pdf data
            """
            program_function_filtered_df=merged_program_function_df[merged_program_function_df['PROGRAM'] == program_function_names]
            

            """
            collect all data input which is mentioend in rules
            then apply detection range logic of removing duplicates,  and again check block which output to replace to input in mathced row
            final store all data after applying all data and get final detection logic
            """
            unique_section_names = program_function_filtered_df['BODY'].unique()
            
            if not program_function_filtered_df.empty:
                for section_name in unique_section_names:
                    logger.info(f"Processing section_name: {section_name}, unique section {unique_section_names}")
                    section_filtered_df=program_function_filtered_df[program_function_filtered_df['BODY'] == section_name]

                    unique_rung_number = section_filtered_df[section_filtered_df['BODY'] == section_name]['RUNG'].unique()
                    for rung_number in unique_rung_number:
                        logger.info(f"Rung Number: {rung_number}")
            
                        rung_filtered_df=section_filtered_df[section_filtered_df['RUNG'] == rung_number]
                        block_data = get_block_connections(rung_filtered_df)
                        
                        if not block_data:
                            logger.warning(f"No block data found for {program_function_names} in section {section_name} and rung {rung_number}")
                            continue

                        for block in block_data:
                            for block_name, params in block.items():
                                # if block_name.upper() in merge_program_function_block or (re.search('TO', block_name.upper()) and not re.search('_', block_name) ):  # Case-insensitive match
                                if (
                                            block_name.upper() in merge_program_function_block
                                            or (re.search('TO', block_name.upper()) and not re.search('_', block_name))
                                        ):
                                    logger.info(f"Block data exact found for {program_function_names} in section {section_name} and rung {rung_number}, block_data {block_data}")
                                    inputs = []
                                    outputs = []
                                    if block_name.upper() in ['INC', "DEC"]:
                                        index_inout = 0
                                        for param in params:
                                            for key, value in param.items():
                                                if key.lower().startswith('inout') and index_inout==0:
                                                    inputs.append(value)
                                                    index_inout+=1
                                                elif key.lower().startswith('inout') and index_inout==1:
                                                    outputs.append(value)
                                                    index_inout+=1
                                    else:
                                        for param in params:
                                            for key, value in param.items():
                                                if key.lower().startswith('in'):
                                                    inputs.extend(value)
                                                elif key.lower().startswith('out'):
                                                    outputs.extend(value)
                                                elif key == '' and ("RightPowerRail" not in value or "LeftPowerRail" not in value):  # Handle empty keys separately if needed
                                                    outputs.extend(value)

                                    row = {
                                        "detected_no": detected_no,
                                        "task_name": program_function_names,
                                        "section": section_name,
                                        "rung": rung_number,
                                        "function_name": block_name,
                                        "input1": inputs[0] if len(inputs) > 0 else None,
                                        "input2": inputs[1] if len(inputs) > 1 else None,
                                        "input3": inputs[2] if len(inputs) > 2 else None,
                                        "output1": outputs[0] if len(outputs) > 0 else None,
                                        "output2": outputs[1] if len(outputs) > 1 else None
                                    }
                                    all_program_function_data.append(row)
                                    detected_no += 1

                    if all_program_function_data:
                        current_program_data = pd.DataFrame(all_program_function_data)
                        current_program_data = flatten_singleton_lists(current_program_data, input_cols+output_cols)
                        for col in cols_to_clean:
                            current_program_data[col] = current_program_data[col].apply(lambda x: '' if isinstance(x, str) and '#' in x else x)

                        # current_program_data.to_csv("C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Rules_implementation/pythoncode/output_csv/intial_rule40.csv", index=False, encoding='utf-8-sig')
                        remove_duplicate_df = remove_cross_row_duplicates(current_program_data.copy())        # Step 1: Clean duplicates
                        replace_output_with_input_df = replace_to_outputs_with_inputs(remove_duplicate_df)     # Step 2: Replace using TO map
                        final_remove_duplicate_df = remove_cross_row_duplicates(replace_output_with_input_df)        # Step 3: Clean again after replacements
                        process_data_detection_range_list.append(final_remove_duplicate_df)

        """
        removing duplicated in there any occurs
        """
        process_data_detection_range_df = pd.concat(process_data_detection_range_list, ignore_index=True)
        process_data_detection_range_df = process_data_detection_range_df.drop_duplicates()
        """
        applying logic for check contect 1,2 as mentioned in rule after applying all filter processing
        """
        final_data_list = check_detail_programwise(merged_program_function_df=merged_program_function_df, process_data_detection_range_df=process_data_detection_range_df)
        
        final_output_df = pd.DataFrame(final_data_list)

        if not final_output_df.empty:
            if 'RungNo' in final_output_df.columns:
                final_output_df['RungNo'] = final_output_df['RungNo'].apply(clean_rung_number)
        else:
            final_output_df = pd.DataFrame(columns=["Result","Task","Section","RungNo","Target","CheckItem","Detail","Status"])

        return {"status":"OK", "output_df":final_output_df}
    
    except Exception as e:
        logger.error(f"Rule 40 Error : {e}")
        return {"status":"NOT OK", "error":e}
    
# if __name__=="__main__":

#     input_program_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Data_Modelling/version3/data_model_Rule_40_41_NG/data_model_Rule_40_41_NG_programwise.csv"
#     input_program_comment_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Data_Modelling/version3/data_model_Rule_40_41_NG/data_model_Rule_40_41_NG_programwise.json"
#     input_function_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Data_Modelling/version3/data_model_Rule_40_41_NG/data_model_Rule_40_41_NG_functionwise.csv"
#     input_function_comment_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Data_Modelling/version3/data_model_Rule_40_41_NG/data_model_Rule_40_41_NG_functionwise.json"
#     output_folder = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Rules_implementation/pythoncode/output_csv/"
#     program_output_file = 'Rule_40_NG_v3.csv'

#     program_result = execute_rule_40_program_functionwise(input_program_file=input_program_file, input_function_file=input_function_file, )
#     program_result.to_csv(f"{output_folder}/{program_output_file}", index=False, encoding='utf-8-sig')
