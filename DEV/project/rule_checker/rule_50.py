import ast
import json
import pandas as pd
from typing import *
import re
from ...main import logger
import polars as pl
from .extract_comment_from_variable import *
from .ladder_utils import regex_pattern_check, clean_rung_number
from .rule_47_ladder_utils import *


#######################3 Function for execution ##################################
def execute_rule_50(
    input_file: str,
    input_program_comment_file: str,
    program_key: str,
    body_type_key: str,
) -> None:

    logger.info("Executing Rule 50")

    class BreakInner(Exception):
        pass

    target_program_pattern = r"Main"
    AL_var_pattern = r"AL"
    air_source_pattern = r"ｴｱｿｰｽ"
    drop_comment_pattern = r"低下"
    master_on_pattern = r"運転準備"
    complete_pattern = r"完了"
    safety_confirmation_pattern = r"安全確認"
    confirmation_pattern = r"確認"
    pt_value_to_check = "t#1.0s"

    rule_50_check_item = "Rule of Air Source Pressure Down Detection Circuit"

    try:
        with open(input_program_comment_file, "r", encoding="utf-8") as file:
            comment_data = json.load(file)

        # output_dict={'TASK_NAME':[], 'SECTION_NAME':[],   'RULE_NUMBER': [], 'CHECK_NUMBER':[], 'RULE_CONTENT':[], 'STATUS': [], 'DETAILS': [], 'NG_EXPLANATION':[]}
        output_dict = {
            "Result": [],
            "Task": [],
            "Section": [],
            "RungNo": [],
            "Target": [],
            "CheckItem": [],
            "Detail": [],
            "Status": [],
        }

        output_df = pd.DataFrame(output_dict)

        ladder_df = pl.read_csv(input_file)
        ladder_df_pd = pd.read_csv(input_file)

        unique_program_values = ladder_df[program_key].unique()
        program_range_dict = {}

        # Program Loop
        for program in unique_program_values:

            if "main" in program.lower():
                logger.info(f"Executing Rule 50 for program:{program}")

                ladder_program = ladder_df.filter(ladder_df[program_key] == program)
                ladder_fault = ladder_program.filter(
                    ladder_program[body_type_key] == "Fault"
                )

                # Pandas specific dataframe
                ladder_program_pd = ladder_df_pd[ladder_df_pd[program_key] == program]
                ladder_fault_pd = ladder_program_pd[
                    ladder_program_pd[body_type_key] == "Fault"
                ]

                # proceed ahead
                if len(ladder_fault) > 0:

                    out_coil_df = ladder_fault.filter(
                        ladder_fault["OBJECT_TYPE_LIST"] == "Coil"
                    )
                    outcoil_rung_list = list(out_coil_df["RUNG"])
                    outcoil_rung_name_list = list(out_coil_df["RUNG_NAME"])
                    outcoil_attributes_list = list(out_coil_df["ATTRIBUTES"])

                    AL_coil_found_flag = 0

                    # Outcoil Loop, this loop looks for the outcoil that is of interest
                    for rung_num, rung_name, coil_attribute in zip(
                        outcoil_rung_list,
                        outcoil_rung_name_list,
                        outcoil_attributes_list,
                    ):

                        # Define all the OK flags

                        check_1_flag = 0
                        check_2_flag = 0
                        check_3_flag = 0
                        check_4_flag = 0
                        check_5_1_flag = 0
                        check_5_2_flag = 0

                        # Define all the NG flags

                        check_1_NG_flag = 0
                        check_2_NG_flag = 0
                        check_3_NG_flag = 0
                        check_4_NG_flag = 0
                        check_5_1_NG_flag = 0
                        check_5_2_NG_flag = 0

                        rung_of_interest = 0

                        A_contact = ""
                        B_contact = ""
                        TON_in = ""
                        TON_out = ""

                        coil_attribute = eval(coil_attribute)
                        coil_operand = coil_attribute["operand"]
                        coil_comment_dict = get_the_comment_from_program(
                            variable=coil_operand,
                            program=program,
                            input_comment_data=comment_data,
                        )

                        # Range Detection,
                        if re.search(AL_var_pattern, coil_operand):
                            if regex_pattern_check(
                                air_source_pattern, coil_comment_dict
                            ) and regex_pattern_check(
                                drop_comment_pattern, coil_comment_dict
                            ):

                                check_1_flag = 1
                                AL_coil_found_flag = 1
                                rung_of_interest = rung_num
                                outcoil_of_interest = coil_operand

                                # Write the data to dataframe
                                detail_dict = {}
                                detail_dict["target_rung"] = rung_num
                                detail_dict["coil_operand"] = coil_operand

                                # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["50"],
                                #          'CHECK_NUMBER':["1"], 'RULE_CONTENT':['AL variable with air_source and drop'],
                                #             'STATUS': ['OK'], 'DETAILS': [detail_dict],
                                #             'NG_EXPLANATION':['NONE']}

                                sub_dict = {
                                    "Result": ["OK"],
                                    "Task": [program],
                                    "Section": ["Fault"],
                                    "RungNo": [rung_num],
                                    "Target": [detail_dict],
                                    "CheckItem": rule_50_check_item,
                                    "Detail": ["NONE"],
                                    "Status": [""],
                                }

                                sub_df = pd.DataFrame(sub_dict)

                                output_df = pd.concat(
                                    [output_df, sub_df], ignore_index=True
                                )

                                logger.warning(
                                    f"Outcoil: {coil_operand} detected in \n program:{program} \n Body : Fault \n Rung: {rung_num}"
                                )

                                series_operand_list = []
                                # Performing check 1
                                check_1_rung_df = ladder_fault.filter(
                                    ladder_fault["RUNG"] == rung_num
                                )
                                check_1_contacts_df = check_1_rung_df.filter(
                                    check_1_rung_df["OBJECT_TYPE_LIST"] == "Contact"
                                )

                                contact_rung_list = list(check_1_contacts_df["RUNG"])
                                contact_rung_name_list = list(
                                    check_1_contacts_df["RUNG_NAME"]
                                )

                                contact_attributes_list = list(
                                    check_1_contacts_df["ATTRIBUTES"]
                                )

                                for rung_num_co, rung_name_co, contact_attribute in zip(
                                    contact_rung_list,
                                    contact_rung_name_list,
                                    contact_attributes_list,
                                ):

                                    contact_attribute = eval(contact_attribute)

                                    contact_operand = contact_attribute["operand"]
                                    contact_comment_dict = get_the_comment_from_program(
                                        variable=contact_operand,
                                        program=program,
                                        input_comment_data=comment_data,
                                    )

                                    contact_type = contact_attribute.get("negated")
                                    contact_edge = contact_attribute.get("edge")

                                    # Check2
                                    if all([contact_type == "false"]):
                                        if regex_pattern_check(
                                            master_on_pattern, contact_comment_dict
                                        ) and regex_pattern_check(
                                            complete_pattern, contact_comment_dict
                                        ):
                                            series_operand_list.append(contact_operand)
                                            A_contact = contact_operand

                                            # Write the data to dataframe
                                            detail_dict = {}
                                            detail_dict["target_rung"] = (
                                                rung_of_interest
                                            )
                                            detail_dict["A_contact_operand"] = A_contact

                                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["50"],
                                            #         'CHECK_NUMBER':["2"], 'RULE_CONTENT':['A contact with master on and complete'],
                                            #             'STATUS': ['OK'], 'DETAILS': [detail_dict],
                                            #             'NG_EXPLANATION':['NONE']}

                                            sub_dict = {
                                                "Result": ["OK"],
                                                "Task": [program],
                                                "Section": ["Fault"],
                                                "RungNo": [rung_of_interest],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_50_check_item,
                                                "Detail": ["NONE"],
                                                "Status": [""],
                                            }

                                            sub_df = pd.DataFrame(sub_dict)

                                            output_df = pd.concat(
                                                [output_df, sub_df], ignore_index=True
                                            )

                                            logger.warning(
                                                f" A Contact: {contact_operand} detected in \n program:{program} \n Body : Fault \n Rung: {rung_num}"
                                            )

                                            check_2_flag = 1

                                    # check 3
                                    if contact_type == "true":
                                        if regex_pattern_check(
                                            confirmation_pattern, contact_comment_dict
                                        ) and regex_pattern_check(
                                            air_source_pattern, contact_comment_dict
                                        ):
                                            series_operand_list.append(contact_operand)
                                            B_contact = contact_operand

                                            # Write the data to dataframe
                                            detail_dict = {}
                                            detail_dict["target_rung"] = rung_num
                                            detail_dict["B_contact_operand"] = B_contact

                                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["50"],
                                            #         'CHECK_NUMBER':["3"], 'RULE_CONTENT':['B contact with conformation and air source'],
                                            #             'STATUS': ['OK'], 'DETAILS': [detail_dict],
                                            #             'NG_EXPLANATION':['NONE']}

                                            sub_dict = {
                                                "Result": ["OK"],
                                                "Task": [program],
                                                "Section": ["Fault"],
                                                "RungNo": [rung_num],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_50_check_item,
                                                "Detail": ["NONE"],
                                                "Status": [""],
                                            }

                                            sub_df = pd.DataFrame(sub_dict)

                                            output_df = pd.concat(
                                                [output_df, sub_df], ignore_index=True
                                            )

                                            logger.warning(
                                                f" B Contact : {contact_operand} detected in \n program:{program} \n Body : Fault \n Rung: {rung_num}"
                                            )
                                            # print(contact_comment_dict, contact_operand)
                                            check_3_flag = 1

                                # Check 4, This happens outside the contact loop
                                block_check_rung_df = ladder_fault.filter(
                                    ladder_fault["RUNG"] == rung_of_interest
                                )
                                block_df = block_check_rung_df.filter(
                                    block_check_rung_df["OBJECT_TYPE_LIST"] == "Block"
                                )
                                block_attributes_list = list(block_df["ATTRIBUTES"])

                                for block_attr in block_attributes_list:
                                    block_attr = eval(block_attr)

                                    if block_attr["typeName"] == "TON":
                                        block_conns = get_block_connections(
                                            block_check_rung_df
                                        )

                                        for outer_dict in block_conns:
                                            for key, nested_list in outer_dict.items():
                                                for inner_dict in nested_list:
                                                    for (
                                                        inner_key,
                                                        inner_value,
                                                    ) in inner_dict.items():
                                                        # print(f"  Inner Key: {inner_key}, Value: {inner_value}")

                                                        if inner_key == "In":
                                                            TON_in = inner_value

                                                        if inner_key == "Q":
                                                            TON_out = inner_value

                                                        if inner_key == "PT":

                                                            if (
                                                                pt_value_to_check
                                                                in inner_value
                                                            ):
                                                                check_4_flag = 1

                                                                # Write the data to dataframe
                                                                detail_dict = {}
                                                                detail_dict[
                                                                    "target_rung"
                                                                ] = rung_of_interest
                                                                detail_dict[
                                                                    "PT_value"
                                                                ] = inner_value

                                                                # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["50"],
                                                                #         'CHECK_NUMBER':["4"], 'RULE_CONTENT':['TON block exists and PT input is associated with t#1.0s'],
                                                                #             'STATUS': ['OK'], 'DETAILS': [detail_dict],
                                                                #             'NG_EXPLANATION':['NONE']}

                                                                sub_dict = {
                                                                    "Result": ["OK"],
                                                                    "Task": [program],
                                                                    "Section": [
                                                                        "Fault"
                                                                    ],
                                                                    "RungNo": [
                                                                        rung_of_interest
                                                                    ],
                                                                    "Target": [
                                                                        detail_dict
                                                                    ],
                                                                    "CheckItem": rule_50_check_item,
                                                                    "Detail": ["NONE"],
                                                                    "Status": [""],
                                                                }

                                                                sub_df = pd.DataFrame(
                                                                    sub_dict
                                                                )

                                                                output_df = pd.concat(
                                                                    [output_df, sub_df],
                                                                    ignore_index=True,
                                                                )

                                                                logger.warning(
                                                                    f" TON block with inner value {inner_value} detected in \n program:{program} \n Body : Fault \n Rung: {rung_of_interest}"
                                                                )

                                                                print(
                                                                    f"  Inner Key: {inner_key}, Value: {inner_value}"
                                                                )

                        # check 5.1, For Series connection and self holding
                        has_series_connection = False
                        if all(
                            [check_2_flag == 1, check_3_flag == 1, check_4_flag == 1]
                        ):

                            if (A_contact in TON_in) and (B_contact in TON_out):

                                check_5_1_flag = 1
                                # Write the data to dataframe
                                detail_dict = {}
                                detail_dict["target_rung"] = rung_of_interest
                                detail_dict["A_contact"] = A_contact
                                detail_dict["TON_in"] = TON_in

                                detail_dict["B_contact"] = B_contact
                                detail_dict["TON_out"] = TON_out

                                has_series_connection = True

                                # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["50"],
                                #         'CHECK_NUMBER':["5.1"], 'RULE_CONTENT':['A Contact, TON Block and B Contact are in series'],
                                #             'STATUS': ['OK'], 'DETAILS': [detail_dict],
                                #             'NG_EXPLANATION':['NONE']}

                                # sub_dict={'Result':["OK"], 'Task':[program], 'Section': ['Fault'],
                                #         'RungNo':[rung_of_interest], "Target":[detail_dict],
                                #         'CheckItem':rule_50_check_item,
                                #         'Detail': ['NONE'],  'Status': ['']}

                                # sub_df=pd.DataFrame(sub_dict)

                                # output_df=pd.concat([output_df, sub_df], ignore_index=True)

                                logger.warning(
                                    f"A contact :{A_contact}, TON Block and B contact:{B_contact} are in series connection in \n program:{program} \n Body : Fault \n Rung: {rung_num}"
                                )

                        # Check for Self holding, 5.2
                        if rung_of_interest != 0 and has_series_connection:
                            rung_df_pd = ladder_fault_pd[
                                ladder_fault_pd["RUNG"] == rung_of_interest
                            ]
                            self_holding_contacts = check_self_holding(rung_df_pd)
                            if outcoil_of_interest in self_holding_contacts:

                                check_5_2_flag = 1
                                # Write the data to dataframe
                                detail_dict = {}
                                detail_dict["target_rung"] = rung_of_interest
                                detail_dict["self_holding_contacts"] = (
                                    self_holding_contacts
                                )

                                # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["50"],
                                #         'CHECK_NUMBER':["5.2"], 'RULE_CONTENT':['Check for Self holding contact'],
                                #             'STATUS': ['OK'], 'DETAILS': [detail_dict],
                                #             'NG_EXPLANATION':['NONE']}

                                sub_dict = {
                                    "Result": ["OK"],
                                    "Task": [program],
                                    "Section": ["Fault"],
                                    "RungNo": [rung_of_interest],
                                    "Target": [detail_dict],
                                    "CheckItem": rule_50_check_item,
                                    "Detail": ["NONE"],
                                    "Status": [""],
                                }

                                sub_df = pd.DataFrame(sub_dict)

                                output_df = pd.concat(
                                    [output_df, sub_df], ignore_index=True
                                )

                                logger.warning(
                                    f" Outcoil {outcoil_of_interest} is self-holding in \n program:{program} \n Body : Fault \n Rung: {rung_num}"
                                )

                        ### Write the data for NG cases############

                        if all(
                            [check_1_flag == 1, check_2_flag == 0, check_2_NG_flag == 0]
                        ):

                            check_2_NG_flag = 1
                            detail_dict = {}
                            detail_dict["target_rung"] = rung_of_interest

                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["50"],
                            #         'CHECK_NUMBER':["2"], 'RULE_CONTENT':['A contact with master on and complete'],
                            #             'STATUS': ['NG'], 'DETAILS': [detail_dict],
                            #             'NG_EXPLANATION':['A contact with master on and complete not found']}

                            sub_dict = {
                                "Result": ["NG"],
                                "Task": [program],
                                "Section": ["Fault"],
                                "RungNo": [rung_of_interest],
                                "Target": [detail_dict],
                                "CheckItem": rule_50_check_item,
                                "Detail": [
                                    "A contact with master on and complete not found"
                                ],
                                "Status": [""],
                            }

                            sub_df = pd.DataFrame(sub_dict)

                            output_df = pd.concat(
                                [output_df, sub_df], ignore_index=True
                            )

                        if all(
                            [check_1_flag == 1, check_3_flag == 0, check_3_NG_flag == 0]
                        ):

                            check_3_NG_flag = 1
                            detail_dict = {}
                            detail_dict["target_rung"] = rung_of_interest

                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["50"],
                            #         'CHECK_NUMBER':["3"], 'RULE_CONTENT':['B contact with conformation and air source'],
                            #             'STATUS': ['NG'], 'DETAILS': [detail_dict],
                            #             'NG_EXPLANATION':['B contact with conformation and air source not found']}

                            sub_dict = {
                                "Result": ["NG"],
                                "Task": [program],
                                "Section": ["Fault"],
                                "RungNo": [rung_of_interest],
                                "Target": [detail_dict],
                                "CheckItem": rule_50_check_item,
                                "Detail": [
                                    "B contact with conformation and air source not found"
                                ],
                                "Status": [""],
                            }

                            sub_df = pd.DataFrame(sub_dict)

                            output_df = pd.concat(
                                [output_df, sub_df], ignore_index=True
                            )

                        if all(
                            [check_1_flag == 1, check_4_flag == 0, check_4_NG_flag == 0]
                        ):

                            check_4_NG_flag = 1
                            detail_dict = {}

                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["50"],
                            #         'CHECK_NUMBER':["4"], 'RULE_CONTENT':['TON block exists and PT input is associated with t#1.0s'],
                            #             'STATUS': ['NG'], 'DETAILS': [detail_dict],
                            #             'NG_EXPLANATION':['Either TON block not found or PT input is not associated with t#1.0s']}

                            sub_dict = {
                                "Result": ["NG"],
                                "Task": [program],
                                "Section": ["Fault"],
                                "RungNo": [rung_of_interest],
                                "Target": [detail_dict],
                                "CheckItem": rule_50_check_item,
                                "Detail": [
                                    "Either TON block not found or PT input is not associated with t#1.0s"
                                ],
                                "Status": [""],
                            }

                            sub_df = pd.DataFrame(sub_dict)

                            output_df = pd.concat(
                                [output_df, sub_df], ignore_index=True
                            )

                        if all(
                            [
                                check_1_flag == 1,
                                check_5_1_flag == 0,
                                check_5_1_NG_flag == 0,
                            ]
                        ):

                            check_5_1_NG_flag == 1
                            detail_dict = {}
                            detail_dict["target_rung"] = rung_of_interest
                            detail_dict["A_contact"] = A_contact
                            detail_dict["B_contact"] = B_contact

                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["50"],
                            #         'CHECK_NUMBER':["5.1"], 'RULE_CONTENT':['A Contact, TON Block and B Contact are in series'],
                            #             'STATUS': ['NG'], 'DETAILS': [detail_dict],
                            #             'NG_EXPLANATION':['A Contact, TON Block and B Contact are not in seriesE']}

                            sub_dict = {
                                "Result": ["NG"],
                                "Task": [program],
                                "Section": ["Fault"],
                                "RungNo": [rung_of_interest],
                                "Target": [detail_dict],
                                "CheckItem": rule_50_check_item,
                                "Detail": [
                                    "A Contact, TON Block and B Contact are not in seriesE"
                                ],
                                "Status": [""],
                            }

                            sub_df = pd.DataFrame(sub_dict)

                            output_df = pd.concat(
                                [output_df, sub_df], ignore_index=True
                            )

                        if all(
                            [
                                check_1_flag == 1,
                                check_5_2_flag == 0,
                                check_5_2_NG_flag == 0,
                            ]
                        ):

                            check_5_2_NG_flag = 1
                            detail_dict = {}
                            detail_dict["target_rung"] = rung_of_interest

                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["50"],
                            #         'CHECK_NUMBER':["5.2"], 'RULE_CONTENT':['Check for Self holding contact'],
                            #             'STATUS': ['NG'], 'DETAILS': [detail_dict],
                            #             'NG_EXPLANATION':['Self Holding Contact not found']}

                            sub_dict = {
                                "Result": ["NG"],
                                "Task": [program],
                                "Section": ["Fault"],
                                "RungNo": [rung_of_interest],
                                "Target": [detail_dict],
                                "CheckItem": rule_50_check_item,
                                "Detail": ["Self Holding Contact not found"],
                                "Status": [""],
                            }

                            sub_df = pd.DataFrame(sub_dict)

                            output_df = pd.concat(
                                [output_df, sub_df], ignore_index=True
                            )

                    ###############Checking for NG cases, this happens inside Program loop and inside fault section#############

                    if all([check_1_NG_flag == 0, AL_coil_found_flag == 0]):

                        check_1_NG_flag = 1
                        detail_dict = {}

                        # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["49"],
                        #         'CHECK_NUMBER':["1"], 'RULE_CONTENT':['AL variable with air_source and drop'],
                        #             'STATUS': ['NG'], 'DETAILS': [detail_dict],
                        #             'NG_EXPLANATION':['AL variable with air_source and drop not found']}

                        sub_dict = {
                            "Result": ["NG"],
                            "Task": [program],
                            "Section": ["Fault"],
                            "RungNo": [rung_of_interest],
                            "Target": [detail_dict],
                            "CheckItem": rule_50_check_item,
                            "Detail": [
                                "AL variable with air_source and drop not found"
                            ],
                            "Status": [""],
                        }

                        sub_df = pd.DataFrame(sub_dict)

                        output_df = pd.concat([output_df, sub_df], ignore_index=True)

        return {"status": "OK", "output_df": output_df}

    except Exception as e:
        logger.error(f"Rule 10 Error in Rule 10 execution: {e}")
        return {"status": "NOT OK", "error": e}


# if __name__ == "__main__":


#     input_file=r"C:\Users\Lenovo\OneDrive - OPTIMIZED SOLUTIONS LTD\Desktop\Evertything\DENSO_coding_checker\CodeBase\Repository\data_modelling\data_model_Rule_49\data_model_Rule_49_programwise.csv"
#     input_program_comment_file=r"C:\Users\Lenovo\OneDrive - OPTIMIZED SOLUTIONS LTD\Desktop\Evertything\DENSO_coding_checker\CodeBase\Repository\data_modelling\data_model_Rule_49\comments_rule_49_programwise.json"

#     execute_rule_50(input_file=input_file, input_program_comment_file=input_program_comment_file,
#                                 output_file_name="Rule_50_program.csv", program_name='', section_name='',
#                                 program_key='PROGRAM',body_type_key='BODY')
