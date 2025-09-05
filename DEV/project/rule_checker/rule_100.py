import ast
import json
import pandas as pd
from typing import *
import re
from loguru import logger
import polars as pl
from .ladder_utils import regex_pattern_check, clean_rung_number


# ============================ Rule 18: Definitions, Content, and Configuration Details ============================
ng_content = "アラームが停止区分の外にあるため、正しく処理されない可能性有(Alarm may not be handled correctly because it is outside of the stop category."
rule_100_check_content_1 = "Make sure that the variables extracted in ③ do not exist in sections other than 'DeviceIn' and 'Fault.'"
rule_content_100 = "・The contact of the sensor shall not be used as a direct condition. (The ”DeviceIn” section rules to create an out coil with a timer condition AND) 　※To prevent abnormalities due to chattering."
rule_100_check_item = 'Rule of Sensor Circuit'
rule_number_100 = "100"
section_name = "devicein"


# ============================== Both Program-Wise and Function-Wise combine in one as per need Function Definitions ===============================
def extract_matching_operands(merged_program_function_df: pd.DataFrame)-> Tuple[List, List]:

    logger.info("Rule 100 - Identifying outcoils starting with 'X' and checking if any associated contact operands starting with 'T' exist.")
    
    unique_program_values = merged_program_function_df["PROGRAM"].unique()

    x_contacts_with_target_outcoil = []
    matching_operand_details = []
    
    for program in unique_program_values:

        logger.info(f"Rule 100 - Executing in program {program}")

        try:
            merged_program_function_filter_df = merged_program_function_df[merged_program_function_df['PROGRAM'] == program].copy()
            devicein_section_df = merged_program_function_filter_df[merged_program_function_filter_df['BODY'].str.lower() == section_name]
            devicein_rung_group_df = devicein_section_df.groupby('RUNG')

            for _, rung_df in devicein_rung_group_df:

                logger.info(f"Rule 100 - Executing in section {section_name} and rung number {rung_df["RUNG"].iloc[0]}")
        
                rung_df['ATTR_DICT'] = rung_df['ATTRIBUTES'].apply(lambda x: ast.literal_eval(x) if pd.notna(x) else {})
                coil_df = rung_df[rung_df['OBJECT_TYPE_LIST'] == 'Coil']

                for _, coil_row in coil_df.iterrows():
                    coil_operand = coil_row['ATTR_DICT'].get('operand', '')
                    
                    if coil_operand.startswith("T"):
                        contact_df = rung_df[rung_df['OBJECT_TYPE_LIST'] == 'Contact']
                        
                        for _, contact_row in contact_df.iterrows():
                            contact_operand = contact_row['ATTR_DICT'].get('operand', '')
                            
                            if contact_operand.startswith("X") and contact_operand not in x_contacts_with_target_outcoil:
                                x_contacts_with_target_outcoil.append(contact_operand)
                                matching_operand_details.append([
                                    contact_operand,
                                    coil_operand,
                                    contact_row.get('PROGRAM'),
                                    contact_row.get('BODY'),
                                    contact_row.get('RUNG'),
                                    "OK",
                                ])
        except Exception as e:
            logger.error(f"Rule 100 - Error processing program {program}: {e}")
            continue

    return x_contacts_with_target_outcoil, matching_operand_details


def check_detail(merged_program_function_df, x_contacts_with_target_outcoil:List, matching_operand_details:List) -> List:

    logger.info("Rule 100 - Checking for 'X'-type contacts found both outside and inside the 'DeviceIN' and 'Fault' sections.")

    exclude_section_name = ['devicein', 'fault']

    unique_program_values = merged_program_function_df["PROGRAM"].unique()

    for program in unique_program_values:
        logger.info(f"Rule 100 - Executing in program {program}")

        merged_program_function_filter_df = merged_program_function_df[merged_program_function_df['PROGRAM'] == program]
        non_device_fault_sections_df = merged_program_function_filter_df[~merged_program_function_filter_df['BODY'].str.lower().isin(exclude_section_name)]
        non_device_fault_rung_groups = non_device_fault_sections_df.groupby('RUNG')

        for _, rung_df in non_device_fault_rung_groups:
            
            try:

                current_section_name = rung_df["BODY"].iloc[0]
                current_rung_number = rung_df["RUNG"].iloc[0]
                
                logger.info(f"Rule 100 - Executing in section {current_section_name} and rung number {current_rung_number}")

                rung_df['ATTR_DICT'] = rung_df['ATTRIBUTES'].apply(
                    lambda x: ast.literal_eval(x) if pd.notna(x) else {}
                )
                contact_df = rung_df[rung_df['OBJECT_TYPE_LIST'] == 'Contact']
                for _, contact_row in contact_df.iterrows():
                    contact_operand = contact_row['ATTR_DICT'].get('operand', '')
                    # if contact_operand.startswith("X"):
                    if contact_operand in x_contacts_with_target_outcoil:
                        matching_operand_details.append([
                            contact_operand,
                            "",
                            contact_row.get('PROGRAM'),
                            contact_row.get('BODY'),
                            contact_row.get('RUNG'),
                            "NG"
                        ])
            except Exception as e:
                logger.error(f"Rule 100 - Error processing check detail 1 in program {program}: {e}")
                continue
    
    return matching_operand_details

def merge_program_function_csv_data(program_df, function_df):

    logger.info("Rule 100 - Merge program and function dataframe into one")

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


# ============================== Main Execution Starts Here ===============================
def execute_rule_100(input_program_file:str, 
                    input_function_file:str,
                    ) -> pd.DataFrame:

    logger.info("Rule 100 - Start executing rule 100 ")
    try:

        program_df = pd.read_csv(input_program_file)
        function_df = pd.read_csv(input_function_file)
        merged_program_function_df = merge_program_function_csv_data(program_df=program_df, function_df=function_df)
        x_contacts_with_target_outcoil, matching_operand_details = extract_matching_operands(merged_program_function_df=merged_program_function_df)
        check_detail_result = check_detail(merged_program_function_df=merged_program_function_df, x_contacts_with_target_outcoil=x_contacts_with_target_outcoil, matching_operand_details=matching_operand_details)
        
        output_rows = []
        for data in check_detail_result:
            try:
                contact_operand = data[0]
                coil_operand = data[1]
                program_name = data[2]
                section_name = data[3]
                rung_number = data[4]
                status = data[5]

                ng_name = ng_content if status == "NG" else ""

                output_rows.append({
                    "Result": status,
                    "Task": program_name,
                    "Section": section_name,
                    "RungNo": rung_number,
                    "Target": coil_operand if coil_operand else "",
                    "CheckItem": rule_100_check_item,
                    "Detail": ng_name,
                    "Status" : ""
                })
                #     "TASK_NAME": program_name,
                #     "SECTION_NAME": section_name,
                #     "RULE_NUMBER": rule_number_100,
                #     "CHECK_NUMBER": 1,
                #     "RUNG_NUMBER": rung_number,
                #     "RULE_CONTENT": rule_content_100,
                #     "CHECK_CONTENT": rule_100_check_content_1,
                #     "TARGET_OUTCOIL" : coil_operand if coil_operand else "",
                #     "STATUS": status,
                #     # "CONTACT_OPERAND" : contact_operand,
                #     # "COIL_OPERAND" : coil_operand,
                #     "NG_EXPLANATION": ng_name
                # })
            except Exception as e:
                logger.error(f"Rule 100 - Error processing data for output row: {e}")
                continue

        final_output_df = pd.DataFrame(output_rows)

        if not final_output_df.empty:
            if 'RungNo' in final_output_df.columns:
                final_output_df['RungNo'] = final_output_df['RungNo'].apply(clean_rung_number)
        else:
            final_output_df = pd.DataFrame(columns=["Result","Task","Section","RungNo","Target","CheckItem","Detail","Status"])

        return {"status":"OK", "output_df":final_output_df}

    except Exception as e:
        logger.error(f"Rule 100 Error : {e}")
        return {"status":"NOT OK", "error":e}

# if __name__=="__main__":
#     input_program_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Data_Modelling/version2/csv_json_output_file/data_model_Rule_100_NG_programwise.csv"
#     input_program_comment_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Data_Modelling/version2/csv_json_output_file/data_model_Rule_100_NG_programwise.json"
#     input_function_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Data_Modelling/version2/csv_json_output_file/data_model_Rule_100_NG_functionwise.csv"
#     input_function_comment_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Data_Modelling/version2/csv_json_output_file/data_model_Rule_100_NG_functionwise.json"
#     output_folder_path = 'C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Rules_implementation/pythoncode/output_csv/'
#     output_file = 'Rule_100_program_NG_update.csv'

#     logger.info("Rule 100 - Execute rule 100")
#     final_result = execute_rule_100(input_program_file=input_program_file, input_function_file=input_function_file)
#     final_result.to_csv(f"{output_folder_path}/{output_file}", index=False, encoding='utf-8-sig')
