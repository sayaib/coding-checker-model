import ast
import json
import pandas as pd
from typing import *
import re
from ...main import logger
import polars as pl
from .extract_comment_from_variable import (
    get_the_comment_from_function,
    get_the_comment_from_program,
)
from .rule_47_ladder_utils import *
from .ladder_utils import regex_pattern_check, clean_rung_number

#######################3 Function for execution ##################################

rule_49_check_item = "Rule of Sefety Cover Open Detection Circuit"


def execute_rule_49(
    input_file: str,
    input_program_comment_file: str,
    program_key: str,
    body_type_key: str,
) -> None:

    logger.info("Executing Rule 49")

    class BreakInner(Exception):
        pass

    target_program_pattern = r"Main"
    AL_var_pattern = r"AL"
    safety_comment_pattern = r"安全"
    open_comment_pattern = r"開"
    master_on_pattern = r"運転準備"
    complete_pattern = r"完了"
    safety_confirmation_pattern = r"安全確認"

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

        """
        
        
        
        """

        for program in unique_program_values:

            if "main" in program.lower():
                logger.info(f"Executing Rule 49 for program:{program}")

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

                    # Loop through the coils
                    for rung_num, rung_name, coil_attribute in zip(
                        outcoil_rung_list,
                        outcoil_rung_name_list,
                        outcoil_attributes_list,
                    ):

                        # defone all the OK flags
                        check_1_flag = 0
                        check_2_flag = 0
                        check_3_flag = 0
                        check_4_1_flag = 0
                        check_4_2_flag = 0

                        # define all the NG flags
                        check_1_NG_flag = 1
                        check_2_NG_flag = 0
                        check_3_NG_flag = 0
                        check_4_1_NG_flag = 0
                        check_4_2_NG_flag = 0

                        A_contact = ""
                        B_contact = ""
                        rung_of_interest = 0

                        coil_attribute = eval(coil_attribute)
                        coil_operand = coil_attribute["operand"]
                        coil_comment_dict = get_the_comment_from_program(
                            variable=coil_operand,
                            program=program,
                            input_comment_data=comment_data,
                        )

                        # Range Detection
                        if re.search(AL_var_pattern, coil_operand):
                            if regex_pattern_check(
                                open_comment_pattern, coil_comment_dict
                            ) and regex_pattern_check(
                                safety_comment_pattern, coil_comment_dict
                            ):

                                check_1_flag = 1
                                AL_coil_found_flag = 1
                                rung_of_interest = rung_num
                                outcoil_of_interest = coil_operand

                                # Write the data to dataframe
                                detail_dict = {}
                                detail_dict["target_rung"] = rung_of_interest
                                detail_dict["coil_operand"] = coil_operand

                                # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["49"],
                                #          'CHECK_NUMBER':["1"], 'RULE_CONTENT':['AL variable with open_comment and safety_comment'],
                                #             'STATUS': ['OK'], 'DETAILS': [detail_dict],
                                #             'NG_EXPLANATION':['NONE']}

                                sub_dict = {
                                    "Result": ["OK"],
                                    "Task": [program],
                                    "Section": ["Fault"],
                                    "RungNo": [rung_num],
                                    "Target": [detail_dict],
                                    "CheckItem": rule_49_check_item,
                                    "Detail": [""],
                                    "Status": [""],
                                }

                                sub_df = pd.DataFrame(sub_dict)

                                output_df = pd.concat(
                                    [output_df, sub_df], ignore_index=True
                                )
                                #####################################

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
                                    if all(
                                        [
                                            contact_type == "false",
                                            contact_edge == "falling",
                                        ]
                                    ):
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
                                            detail_dict["A_contact_operand"] = (
                                                contact_operand
                                            )

                                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["49"],
                                            #         'CHECK_NUMBER':["2"], 'RULE_CONTENT':['A contact falling edge, with master on and complete'],
                                            #             'STATUS': ['OK'], 'DETAILS': [detail_dict],
                                            #             'NG_EXPLANATION':['NONE']}
                                            sub_dict = {
                                                "Result": ["OK"],
                                                "Task": [program],
                                                "Section": ["Fault"],
                                                "RungNo": [rung_of_interest],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_49_check_item,
                                                "Detail": [""],
                                                "Status": [""],
                                            }

                                            sub_df = pd.DataFrame(sub_dict)

                                            output_df = pd.concat(
                                                [output_df, sub_df], ignore_index=True
                                            )

                                            ####################################
                                            logger.warning(
                                                f" A Contact with falling edge: {contact_operand} detected in \n program:{program} \n Body : Fault \n Rung: {rung_num}"
                                            )

                                            # print(contact_comment_dict, contact_operand)
                                            check_2_flag = 1

                                    # check 3
                                    if contact_type == "true":
                                        if regex_pattern_check(
                                            safety_confirmation_pattern,
                                            contact_comment_dict,
                                        ):
                                            series_operand_list.append(contact_operand)

                                            B_contact = contact_operand
                                            # Write the data to dataframe
                                            detail_dict = {}
                                            detail_dict["target_rung"] = (
                                                rung_of_interest
                                            )
                                            detail_dict["B_contact_operand"] = (
                                                contact_operand
                                            )

                                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["49"],
                                            #         'CHECK_NUMBER':["3"], 'RULE_CONTENT':['B contact with safety confirmation'],
                                            #             'STATUS': ['OK'], 'DETAILS': [detail_dict],
                                            #             'NG_EXPLANATION':['NONE']}

                                            sub_dict = {
                                                "Result": ["OK"],
                                                "Task": [program],
                                                "Section": ["Fault"],
                                                "RungNo": [rung_of_interest],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_49_check_item,
                                                "Detail": [""],
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

                        # check for 4_1
                        if all([check_2_flag == 1, check_3_flag == 1]):

                            rung_df = ladder_fault.filter(
                                ladder_fault["RUNG"] == rung_of_interest
                            )

                            series_contacts_list = get_series_contacts(rung_df)
                            series_contacts_operands = [
                                [entry.get("operand", "NONE") for entry in sublist]
                                for sublist in series_contacts_list
                            ]

                            series_contacts_operands_set_list = [
                                set(ele) for ele in series_contacts_operands
                            ]
                            series_operand_list = set(series_operand_list)

                            # Check for Series connections
                            has_series_connection = False
                            for ele_set in series_contacts_operands_set_list:
                                if ele_set == series_operand_list:

                                    check_4_1_flag = 1
                                    # Write the data to dataframe
                                    detail_dict = {}
                                    detail_dict["target_rung"] = rung_of_interest
                                    detail_dict["A_contact"] = A_contact
                                    detail_dict["B_contact"] = B_contact

                                    has_series_connection = True
                                    break
                                    # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["49"],
                                    #         'CHECK_NUMBER':["4.1"], 'RULE_CONTENT':['A Contact , B Contact are to be in Series connection'],
                                    #             'STATUS': ['OK'], 'DETAILS': [detail_dict],
                                    #             'NG_EXPLANATION':['NONE']}

                                    # sub_dict={'Result':["OK"], 'Task':[program], 'Section': ['Fault'],
                                    #                 'RungNo':[rung_of_interest], "Target":[detail_dict],
                                    #                 'CheckItem':rule_49_check_item,
                                    #                 'Detail': [''],  'Status': ['']}

                                    # sub_df=pd.DataFrame(sub_dict)

                                    # output_df=pd.concat([output_df, sub_df], ignore_index=True)

                                    # logger.warning(f" Contact chain {ele_set} is in series connection in \n program:{program} \n Body : Fault \n Rung: {rung_num}")
                                    # print("Series Confirmed")
                                    # break

                            # Check for Self holding, 4.2
                            if rung_of_interest != 0 and has_series_connection:
                                rung_df_pd = ladder_fault_pd[
                                    ladder_fault_pd["RUNG"] == rung_of_interest
                                ]
                                self_holding_contacts = check_self_holding(rung_df_pd)
                                if outcoil_of_interest in self_holding_contacts:
                                    check_4_2_flag = 1

                                    # Write the data to dataframe
                                    detail_dict = {}
                                    detail_dict["target_rung"] = rung_of_interest

                                    detail_dict["self_holding_contacts"] = (
                                        self_holding_contacts
                                    )

                                    # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["49"],
                                    #         'CHECK_NUMBER':["4.2"], 'RULE_CONTENT':['Checking for self Holding contacts'],
                                    #             'STATUS': ['OK'], 'DETAILS': [detail_dict],
                                    #             'NG_EXPLANATION':['NONE']}

                                    sub_dict = {
                                        "Result": ["OK"],
                                        "Task": [program],
                                        "Section": ["Fault"],
                                        "RungNo": [rung_of_interest],
                                        "Target": [detail_dict],
                                        "CheckItem": rule_49_check_item,
                                        "Detail": [""],
                                        "Status": [""],
                                    }

                                    sub_df = pd.DataFrame(sub_dict)

                                    output_df = pd.concat(
                                        [output_df, sub_df], ignore_index=True
                                    )

                                    logger.warning(
                                        f" Outcoil {outcoil_of_interest} is self-holding in \n program:{program} \n Body : Fault \n Rung: {rung_num}"
                                    )

                                    # print("self holding", self_holding_contacts)

                        ### Write the data for NG cases############
                        if all(
                            [check_1_flag == 1, check_2_flag == 0, check_2_NG_flag == 0]
                        ):

                            check_2_NG_flag = 1
                            detail_dict = {}
                            detail_dict["target_rung"] = rung_of_interest

                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["49"],
                            #         'CHECK_NUMBER':["2"], 'RULE_CONTENT':['A contact falling edge, with master on and complete'],
                            #             'STATUS': ['NG'], 'DETAILS': [detail_dict],
                            #             'NG_EXPLANATION':['A contact falling edge, with master on and complete not found']}

                            sub_dict = {
                                "Result": ["NG"],
                                "Task": [program],
                                "Section": ["Fault"],
                                "RungNo": [rung_of_interest],
                                "Target": [detail_dict],
                                "CheckItem": rule_49_check_item,
                                "Detail": [
                                    "A contact falling edge, with master on and complete not found"
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

                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["49"],
                            #         'CHECK_NUMBER':["3"], 'RULE_CONTENT':['B contact with safety confirmation'],
                            #             'STATUS': ['NG'], 'DETAILS': [detail_dict],
                            #             'NG_EXPLANATION':['B contact with safety confirmatione not found']}

                            sub_dict = {
                                "Result": ["NG"],
                                "Task": [program],
                                "Section": ["Fault"],
                                "RungNo": [rung_of_interest],
                                "Target": [detail_dict],
                                "CheckItem": rule_49_check_item,
                                "Detail": [
                                    "B contact with safety confirmatione not found"
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
                                check_4_1_flag == 0,
                                check_4_1_NG_flag == 0,
                            ]
                        ):

                            check_4_1_NG_flag = 1
                            detail_dict = {}
                            detail_dict["target_rung"] = rung_of_interest
                            detail_dict["A_contact"] = A_contact
                            detail_dict["B_contact"] = B_contact

                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["49"],
                            #         'CHECK_NUMBER':["4.1"], 'RULE_CONTENT':['A Contact and B Contact in series connection'],
                            #             'STATUS': ['NG'], 'DETAILS': [detail_dict],
                            #             'NG_EXPLANATION':['A Contact and B Contact not in series connection']}

                            sub_dict = {
                                "Result": ["NG"],
                                "Task": [program],
                                "Section": ["Fault"],
                                "RungNo": [rung_of_interest],
                                "Target": [detail_dict],
                                "CheckItem": rule_49_check_item,
                                "Detail": [
                                    "A Contact and B Contact not in series connection"
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
                                check_4_2_flag == 0,
                                check_4_2_NG_flag == 0,
                            ]
                        ):

                            check_4_2_NG_flag = 1
                            detail_dict = {}
                            detail_dict["target_rung"] = rung_of_interest

                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['Fault'], 'RULE_NUMBER': ["49"],
                            #         'CHECK_NUMBER':["4.2"], 'RULE_CONTENT':['Check for Self holding contact'],
                            #             'STATUS': ['NG'], 'DETAILS': [detail_dict],
                            #             'NG_EXPLANATION':['Self holding contact not found']}

                            sub_dict = {
                                "Result": ["NG"],
                                "Task": [program],
                                "Section": ["Fault"],
                                "RungNo": [rung_of_interest],
                                "Target": [detail_dict],
                                "CheckItem": rule_49_check_item,
                                "Detail": ["Self holding contact not found"],
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
                        #         'CHECK_NUMBER':["1"], 'RULE_CONTENT':['AL variable with open_comment and safety_comment'],
                        #             'STATUS': ['NG'], 'DETAILS': [detail_dict],
                        #             'NG_EXPLANATION':['AL variable with open_comment and safety_comment not found']}

                        sub_dict = {
                            "Result": ["NG"],
                            "Task": [program],
                            "Section": ["Fault"],
                            "RungNo": [""],
                            "Target": [detail_dict],
                            "CheckItem": rule_49_check_item,
                            "Detail": [
                                "AL variable with open_comment and safety_comment not found"
                            ],
                            "Status": [""],
                        }

                        sub_df = pd.DataFrame(sub_dict)

                        output_df = pd.concat([output_df, sub_df], ignore_index=True)

        return {"status": "OK", "output_df": output_df}

    except Exception as e:
        logger.error(f"Rule 10 Error in Rule 10 execution: {e}")
        return {"status": "NOT OK", "error": e}


######################################################################################3


###########################################################################################


# if __name__ == "__main__":


#     input_file=r"C:\Users\Lenovo\OneDrive - OPTIMIZED SOLUTIONS LTD\Desktop\Evertything\DENSO_coding_checker\CodeBase\Repository\data_modelling\data_model_Rule_49\data_model_Rule_49_programwise.csv"
#     input_program_comment_file=r"C:\Users\Lenovo\OneDrive - OPTIMIZED SOLUTIONS LTD\Desktop\Evertything\DENSO_coding_checker\CodeBase\Repository\data_modelling\data_model_Rule_49\comments_rule_49_programwise.json"

#     execute_rule_49(input_file=input_file, input_program_comment_file=input_program_comment_file,
#                                 output_file_name="Rule_49_program.csv", program_name='', section_name='',
#                                 program_key='PROGRAM',body_type_key='BODY')
