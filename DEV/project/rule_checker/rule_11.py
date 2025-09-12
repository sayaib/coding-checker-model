import ast
import json
import pandas as pd
from typing import *
import re
from ...main import logger
import polars as pl
from .rule_10_15_ladder_utils import *
from .extract_comment_from_variable import (
    get_the_comment_from_program,
    get_the_comment_from_function,
)
from .ladder_utils import regex_pattern_check, clean_rung_number

#########################

rule_11_check_item = "Rule of Process Complete / Good or Bad Result Set"


#######################3 Function for execution ##################################
def execute_rule_11(
    input_file: str, input_image: str=None, program_key: str="PROGRAM", body_type_key: str="BODY"
) -> None:

    logger.info("Executing Rule 11")

    class BreakInner(Exception):
        pass

    acceptable_body_list = ["AutoRun★", "AutoRun", "Preparation"]

    # output_dict={'TASK_NAME':[], 'SECTION_NAME':[],   'RULE_NUMBER': [], 'CHECK_NUMBER':[], "RUNG_NUMBER":[], 'RULE_CONTENT':[], 'CHECK_CONTENT':[], 'TARGET_OUTCOIL': [], 'STATUS': [], 'NG_EXPLANATION':[]}
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

    try:
        output_df = pd.DataFrame(output_dict)

        ladder_df = pl.read_csv(input_file)

        rule_11_look_up_df = pd.read_csv(input_image)
        rule_11_look_up_df = rule_11_look_up_df[["Task name", "Machine Number"]]

        rule_11_look_up_df["Machine Number"] = rule_11_look_up_df[
            "Machine Number"
        ].fillna(-1000)

        unique_program_values = ladder_df[program_key].unique()

        program_range_dict = {}

        """
        Look in each program,
        In each program look in to body Preparation and AutoRun
        In each of the above body names, check whetehr FlowControlDataJudge_ZDS block is present or not.
        If present look whether the terminal MachineNo is associated with a value or not. lets call  this machine_val
        If associated then findout the data-variable of the machine_val in the smc block
        
        """

        for program in unique_program_values:

            ladder_program = ladder_df.filter(ladder_df[program_key] == program)

            ladder_program_df = ladder_df.filter(pl.col(program_key) == program)
            unique_section_values = ladder_program_df.select(body_type_key).unique()
            lower_values = {
                val.lower() for val in unique_section_values[body_type_key].to_list()
            }
            if "autorun" in lower_values or "preparation" in lower_values:

                ladder_program = ladder_df.filter(pl.col(program_key) == program)

                ladder_body = ladder_program.filter(
                    pl.col(body_type_key)
                    .cast(pl.Utf8)
                    .str.to_lowercase()
                    .is_in(["autorun★", "autorun", "preparation"])
                )

                flow_block_flag = 0
                process_val_assigned_flag = 0
                machine_no_input = ""
                flow_block_rung = 0
                smc_block_rung = 0
                variable_lookup_match_flag = 0

                for row in ladder_body.iter_rows():

                    pgm, bdy, rg_order, rg_name, obj, obj_type, attrs = row
                    attrs = eval(attrs)
                    attr_type = attrs.get("typeName")

                    if all(
                        [attr_type != None, attr_type == "FlowControlDataJudge_ZDS"]
                    ):

                        ladder_rung = ladder_body.filter(
                            ladder_body["RUNG"] == rg_order
                        )
                        block_connections = get_block_connections(ladder_rung)
                        flow_block_flag = 1
                        flow_block_rung = rg_order

                        logger.info(
                            f"FlowControlDataJudge_ZDS found in \n program: {program} \n Body:{bdy} \n RUNG :{rg_order}  "
                        )

                        # Write the result to output for check 1
                        detail_dict = {}
                        detail_dict["RUNG"] = rg_order
                        # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':[bdy], 'RULE_NUMBER': ["11"],
                        #         'CHECK_NUMBER':["1"], 'RULE_CONTENT':["・For each process, check that all the processes before the own process are OK, and that all the facilities before the own facility are OK. If not, it is judged to be defective."],
                        #         'CHECK_CONTENT':'If the function block  in ② is not found in either of the following in the target task, NG is assumed.',
                        # 'STATUS': ['OK'], 'TARGET_OUTCOIL': [detail_dict],
                        #         'NG_EXPLANATION':['NONE']}

                        # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':[bdy], 'RULE_NUMBER': ["11"],
                        #         'CHECK_NUMBER':["1"],  "RUNG_NUMBER":[rg_order], 'RULE_CONTENT':["・For each process, check that all the processes before the own process are OK, and that all the facilities before the own facility are OK. If not, it is judged to be defective."],
                        #         'CHECK_CONTENT':'If the function block  in ② is not found in either of the following in the target task, NG is assumed.',
                        #         'TARGET_OUTCOIL': [detail_dict], 'STATUS': ['OK'],
                        #         'NG_EXPLANATION':['NONE']}

                        sub_dict = {
                            "Result": ["OK"],
                            "Task": [program],
                            "Section": [bdy],
                            "RungNo": [rg_order],
                            "Target": [detail_dict],
                            "CheckItem": rule_11_check_item,
                            "Detail": [""],
                            "Status": [""],
                        }

                        sub_df = pd.DataFrame(sub_dict)

                        output_df = pd.concat([output_df, sub_df], ignore_index=True)

                        # Loop for each block connections
                        for block in block_connections:
                            try:

                                machine_no_input = ""
                                for key, values in block.items():
                                    for val in values:
                                        val_machine_no = val.get("MachineNo")

                                        if val_machine_no != None:
                                            machine_no_input = val_machine_no[0]

                                            # smc_blocks=ladder_program.filter(ladder_program['OBJECT_TYPE_LIST'] == "smcext:InlineST")
                                            if (
                                                isinstance(machine_no_input, str)
                                                and machine_no_input
                                                and machine_no_input[0].isalpha()
                                                and not machine_no_input.startswith("G")
                                            ):
                                                smc_blocks = ladder_program.filter(
                                                    ladder_program["OBJECT_TYPE_LIST"]
                                                    == "smcext:InlineST"
                                                )
                                                smc_blocks_attrs = list(
                                                    smc_blocks["ATTRIBUTES"]
                                                )
                                                smc_rungs = list(smc_blocks["RUNG"])

                                            elif isinstance(
                                                machine_no_input, str
                                            ) and machine_no_input.startswith("G"):
                                                smc_blocks = ladder_df.filter(
                                                    ladder_df["OBJECT_TYPE_LIST"]
                                                    == "smcext:InlineST"
                                                )
                                                smc_blocks_attrs = list(
                                                    smc_blocks["ATTRIBUTES"]
                                                )
                                                smc_rungs = list(smc_blocks["RUNG"])

                                            else:
                                                smc_blocks = ladder_body.filter(
                                                    ladder_body["OBJECT_TYPE_LIST"]
                                                    == "smcext:InlineST"
                                                )
                                                smc_blocks_attrs = list(
                                                    smc_blocks["ATTRIBUTES"]
                                                )
                                                smc_rungs = list(smc_blocks["RUNG"])

                                            smc_blocks_attrs = list(
                                                smc_blocks["ATTRIBUTES"]
                                            )
                                            smc_rungs = list(smc_blocks["RUNG"])

                                            # Write the result to output for check 2 , process value properly assigned
                                            detail_dict = {}
                                            detail_dict["RUNG"] = rg_order
                                            detail_dict["machine_no_input"] = (
                                                machine_no_input
                                            )
                                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':[bdy], 'RULE_NUMBER': ["11"],
                                            #         'CHECK_NUMBER':["2"],
                                            #         'RULE_CONTENT':[f"・For each process, check that all the processes before the own process are OK, and that all the facilities before the own facility are OK. If not, it is judged to be defective."],
                                            #         'CHECK_CONTENT':'If a variable is not entered in the input variable in ③, it is assumed to be NG.', 'STATUS': ['OK'], 'TARGET_OUTCOIL': [detail_dict],
                                            #         'NG_EXPLANATION':['NONE']}

                                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':[bdy], 'RULE_NUMBER': ["11"],
                                            #     'CHECK_NUMBER':["2"],  "RUNG_NUMBER":[rg_order], 'RULE_CONTENT':["・For each process, check that all the processes before the own process are OK, and that all the facilities before the own facility are OK. If not, it is judged to be defective."],
                                            #     'CHECK_CONTENT':'If a variable is not entered in the input variable in ③, it is assumed to be NG.',
                                            #     'TARGET_OUTCOIL': [detail_dict], 'STATUS': ['OK'],
                                            #     'NG_EXPLANATION':['NONE']}

                                            sub_dict = {
                                                "Result": ["OK"],
                                                "Task": [program],
                                                "Section": [bdy],
                                                "RungNo": [rg_order],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_11_check_item,
                                                "Detail": [""],
                                                "Status": [""],
                                            }

                                            sub_df = pd.DataFrame(sub_dict)

                                            output_df = pd.concat(
                                                [output_df, sub_df], ignore_index=True
                                            )

                                            for smc_attr, smc_rung in zip(
                                                smc_blocks_attrs, smc_rungs
                                            ):

                                                smc_attr = eval(smc_attr)
                                                data_inputs = smc_attr.get(
                                                    "data_inputs"
                                                )
                                                smc_block_rung = smc_rung

                                                if data_inputs != None:

                                                    for data_ in data_inputs:

                                                        if re.search(
                                                            machine_no_input, data_
                                                        ):
                                                            if re.search(r";", data_):

                                                                data_var = data_.split(
                                                                    ";"
                                                                )[0]
                                                                data_var_value = (
                                                                    data_var.split("=")[
                                                                        1
                                                                    ].strip()
                                                                )
                                                                process_val_assigned_flag = (
                                                                    1
                                                                )

                                                                # Check3, Write to output

                                                                detail_dict = {}
                                                                detail_dict[
                                                                    "SMC_RUNG"
                                                                ] = smc_block_rung
                                                                detail_dict[
                                                                    "machine_no_input"
                                                                ] = machine_no_input
                                                                detail_dict[
                                                                    "process_val_assigned"
                                                                ] = data_var_value

                                                                # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':[bdy], 'RULE_NUMBER': ["11"],
                                                                #         'CHECK_NUMBER':["3"],
                                                                #         'RULE_CONTENT':[f"・For each process, check that all the processes before the own process are OK, and that all the facilities before the own facility are OK. If not, it is judged to be defective."],
                                                                #         'CHECK_CONTENT':'Check if a numerical value is assigned to the variable detected in ④. If not, NG is assumed.', 'STATUS': ['OK'], 'TARGET_OUTCOIL': [detail_dict],
                                                                #         'NG_EXPLANATION':['NONE']}

                                                                # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':[bdy], 'RULE_NUMBER': ["11"],
                                                                #     'CHECK_NUMBER':["3"],  "RUNG_NUMBER":[rg_order], 'RULE_CONTENT':["・For each process, check that all the processes before the own process are OK, and that all the facilities before the own facility are OK. If not, it is judged to be defective."],
                                                                #     'CHECK_CONTENT':'Check if a numerical value is assigned to the variable detected in ④. If not, NG is assumed.',
                                                                #     'TARGET_OUTCOIL': [detail_dict], 'STATUS': ['OK'],
                                                                #     'NG_EXPLANATION':['NONE']}

                                                                sub_dict = {
                                                                    "Result": ["OK"],
                                                                    "Task": [program],
                                                                    "Section": [bdy],
                                                                    "RungNo": [
                                                                        rg_order
                                                                    ],
                                                                    "Target": [
                                                                        detail_dict
                                                                    ],
                                                                    "CheckItem": rule_11_check_item,
                                                                    "Detail": [""],
                                                                    "Status": [""],
                                                                }

                                                                sub_df = pd.DataFrame(
                                                                    sub_dict
                                                                )

                                                                output_df = pd.concat(
                                                                    [output_df, sub_df],
                                                                    ignore_index=True,
                                                                )

                                                                """Check for looking into CSV file is remaininig """
                                                                logger.info(
                                                                    f"Variable {machine_no_input} found in \n program: {program} \n Body:{bdy} \n RUNG :{rg_order}  "
                                                                )

                                                                # Performing Check 4
                                                                rule_11_look_up_df_check = rule_11_look_up_df[
                                                                    rule_11_look_up_df[
                                                                        "Task name"
                                                                    ]
                                                                    == program
                                                                ]
                                                                rule_11_operation_number_list = list(
                                                                    rule_11_look_up_df_check[
                                                                        "Machine Number"
                                                                    ]
                                                                )

                                                                rule_11_operation_number_list = [
                                                                    str(int(ele))
                                                                    for ele in rule_11_operation_number_list
                                                                ]
                                                                # rule_11_operation_number_list = [
                                                                #         str(int(ele)) for ele in rule_11_operation_number_list
                                                                #         if isinstance(ele, int) or (isinstance(ele, str) and ele.isdigit())
                                                                #     ]

                                                                data_var_value_number = str(
                                                                    data_var_value.split(
                                                                        "#"
                                                                    )[
                                                                        1
                                                                    ]
                                                                )
                                                                print(
                                                                    f"data_var_value_number: {data_var_value_number} \n rule_11_operation_number_list: {rule_11_operation_number_list}"
                                                                )
                                                                if (
                                                                    data_var_value_number
                                                                    in rule_11_operation_number_list
                                                                ):

                                                                    # Dump in output file
                                                                    detail_dict = {}
                                                                    detail_dict[
                                                                        "FLOW_BLOCK_RUNG"
                                                                    ] = rg_order
                                                                    detail_dict[
                                                                        "SMC_BLOCK_RUNG"
                                                                    ] = smc_rung
                                                                    detail_dict[
                                                                        "Process_no_variable"
                                                                    ] = machine_no_input
                                                                    detail_dict[
                                                                        "Process_no_data"
                                                                    ] = data_var_value

                                                                    # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':[bdy], 'RULE_NUMBER': ["11"],
                                                                    #         'CHECK_NUMBER':["4"], 'RULE_CONTENT':["・For each process, check that all the processes before the own process are OK, and that all the facilities before the own facility are OK. If not, it is judged to be defective."],
                                                                    #         'CHECK_CONTENT':'The number assigned to the variable detected in ❸ is confirmed to be equal to the ｍachine number of the task being checked, which is INPUT in the system UI, and if it is not equal, it is assumed to be NG.', 'STATUS': ['OK'], 'TARGET_OUTCOIL': [detail_dict],
                                                                    #         'NG_EXPLANATION':["NONE"]}

                                                                    # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':[bdy], 'RULE_NUMBER': ["11"],
                                                                    #     'CHECK_NUMBER':["4"],  "RUNG_NUMBER":[rg_order], 'RULE_CONTENT':["・For each process, check that all the processes before the own process are OK, and that all the facilities before the own facility are OK. If not, it is judged to be defective."],
                                                                    #     'CHECK_CONTENT':'The number assigned to the variable detected in ❸ is confirmed to be equal to the ｍachine number of the task being checked, which is INPUT in the system UI, and if it is not equal, it is assumed to be NG.',
                                                                    #     'TARGET_OUTCOIL': [detail_dict], 'STATUS': ['OK'],
                                                                    #     'NG_EXPLANATION':['NONE']}

                                                                    sub_dict = {
                                                                        "Result": [
                                                                            "OK"
                                                                        ],
                                                                        "Task": [
                                                                            program
                                                                        ],
                                                                        "Section": [
                                                                            bdy
                                                                        ],
                                                                        "RungNo": [
                                                                            rg_order
                                                                        ],
                                                                        "Target": [
                                                                            detail_dict
                                                                        ],
                                                                        "CheckItem": rule_11_check_item,
                                                                        "Detail": [""],
                                                                        "Status": [""],
                                                                    }

                                                                    sub_df = (
                                                                        pd.DataFrame(
                                                                            sub_dict
                                                                        )
                                                                    )

                                                                    output_df = pd.concat(
                                                                        [
                                                                            output_df,
                                                                            sub_df,
                                                                        ],
                                                                        ignore_index=True,
                                                                    )
                                                                    raise BreakInner

                                                                else:

                                                                    detail_dict = {}
                                                                    detail_dict[
                                                                        "FLOW_BLOCK_RUNG"
                                                                    ] = rg_order
                                                                    detail_dict[
                                                                        "SMC_BLOCK_RUNG"
                                                                    ] = smc_rung
                                                                    detail_dict[
                                                                        "Process_no_variable"
                                                                    ] = machine_no_input
                                                                    detail_dict[
                                                                        "Process_no_data"
                                                                    ] = data_var_value
                                                                    ng_str = "ZDSに入力されている設備番号が,チェッカーの入力画面で入力した設備番号と異なっている。"

                                                                    # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':[bdy], 'RULE_NUMBER': ["11"],
                                                                    #         'CHECK_NUMBER':["4"], 'RULE_CONTENT':["・For each process, check that all the processes before the own process are OK, and that all the facilities before the own facility are OK. If not, it is judged to be defective."],
                                                                    #         'CHECK_CONTENT':'The number assigned to the variable detected in ❸ is confirmed to be equal to the ｍachine number of the task being checked, which is INPUT in the system UI, and if it is not equal, it is assumed to be NG.', 'STATUS': ['NG'], 'TARGET_OUTCOIL': [detail_dict],
                                                                    #         'NG_EXPLANATION':[ng_str]}
                                                                    # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':[bdy], 'RULE_NUMBER': ["11"],
                                                                    #     'CHECK_NUMBER':["4"],  "RUNG_NUMBER":[""], 'RULE_CONTENT':["・For each process, check that all the processes before the own process are OK, and that all the facilities before the own facility are OK. If not, it is judged to be defective."],
                                                                    #     'CHECK_CONTENT':'The number assigned to the variable detected in ❸ is confirmed to be equal to the ｍachine number of the task being checked, which is INPUT in the system UI, and if it is not equal, it is assumed to be NG.',
                                                                    #     'TARGET_OUTCOIL': [detail_dict], 'STATUS': ['NG'],
                                                                    #     'NG_EXPLANATION':[ng_str]}

                                                                    sub_dict = {
                                                                        "Result": [
                                                                            "NG"
                                                                        ],
                                                                        "Task": [
                                                                            program
                                                                        ],
                                                                        "Section": [
                                                                            bdy
                                                                        ],
                                                                        "RungNo": [
                                                                            rg_order
                                                                        ],
                                                                        "Target": [
                                                                            detail_dict
                                                                        ],
                                                                        "CheckItem": rule_11_check_item,
                                                                        "Detail": [
                                                                            ng_str
                                                                        ],
                                                                        "Status": [""],
                                                                    }

                                                                    sub_df = (
                                                                        pd.DataFrame(
                                                                            sub_dict
                                                                        )
                                                                    )

                                                                    output_df = pd.concat(
                                                                        [
                                                                            output_df,
                                                                            sub_df,
                                                                        ],
                                                                        ignore_index=True,
                                                                    )

                                                                    logger.info(
                                                                        f"Variable did not match with look up tale in \nProgram:{program} \n Body:{bdy} \n RUNG:{rg_order} for \nvariable {machine_no_input} \Process data:{data_var_value} "
                                                                    )

                                                                    raise BreakInner

                            except BreakInner:
                                pass

                ###################### Based on the flag values develop the report ###############3

                if flow_block_flag == 0:

                    detail_dict = {}
                    ng_str = " ”preparation”また”AutoRun”セクション内にZDSが使用されていないため,流動制御が成り立っていない可能性有"
                    # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':[bdy], 'RULE_NUMBER': ["11"],
                    #         'CHECK_NUMBER':["1"], 'RULE_CONTENT':["FlowControlDataJudge_ZDS not detected"], 'CHECK_CONTENT':'If the function block  in ② is not found in either of the following in the target task, NG is assumed.', 'STATUS': ['NG'], 'TARGET_OUTCOIL': [detail_dict],
                    #         'NG_EXPLANATION':[ng_str]}

                    # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':[bdy], 'RULE_NUMBER': ["11"],
                    #     'CHECK_NUMBER':["1"],  "RUNG_NUMBER":[""], 'RULE_CONTENT':["FlowControlDataJudge_ZDS not detected"],
                    #     'CHECK_CONTENT':'If the function block  in ② is not found in either of the following in the target task, NG is assumed.',
                    #     'TARGET_OUTCOIL': [detail_dict], 'STATUS': ['NG'],
                    #     'NG_EXPLANATION':[ng_str]}

                    sub_dict = {
                        "Result": ["NG"],
                        "Task": [program],
                        "Section": [""],
                        "RungNo": [""],
                        "Target": [detail_dict],
                        "CheckItem": rule_11_check_item,
                        "Detail": [ng_str],
                        "Status": [""],
                    }

                    sub_df = pd.DataFrame(sub_dict)

                    output_df = pd.concat([output_df, sub_df], ignore_index=True)

                if all([flow_block_flag == 1, machine_no_input == ""]):

                    detail_dict = {}
                    detail_dict["FLOW_BLOCK_RUNG"] = flow_block_rung

                    ng_str = 'ZDSの入力変数"MachineNo"に変数が接続されていないため,流動制御が成り立っていない可能性有'
                    # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':[bdy], 'RULE_NUMBER': ["11"],
                    #         'CHECK_NUMBER':["2"], 'RULE_CONTENT':["・For each process, check that all the processes before the own process are OK, and that all the facilities before the own facility are OK. If not, it is judged to be defective."],
                    #         'CHECK_CONTENT':'If a variable is not entered in the input variable in ③, it is assumed to be NG.','STATUS': ['NG'], 'TARGET_OUTCOIL': [detail_dict],
                    #         'NG_EXPLANATION':[ng_str]}

                    # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':[bdy], 'RULE_NUMBER': ["11"],
                    #     'CHECK_NUMBER':["2"],  "RUNG_NUMBER":[""], 'RULE_CONTENT':["・For each process, check that all the processes before the own process are OK, and that all the facilities before the own facility are OK. If not, it is judged to be defective."],
                    #     'CHECK_CONTENT':' If a variable is not entered in the input variable in ③, it is assumed to be NG.',
                    #     'TARGET_OUTCOIL': [detail_dict], 'STATUS': ['NG'],
                    #     'NG_EXPLANATION':[ng_str]}

                    sub_dict = {
                        "Result": ["NG"],
                        "Task": [program],
                        "Section": [""],
                        "RungNo": [""],
                        "Target": [detail_dict],
                        "CheckItem": rule_11_check_item,
                        "Detail": [ng_str],
                        "Status": [""],
                    }

                    sub_df = pd.DataFrame(sub_dict)

                    output_df = pd.concat([output_df, sub_df], ignore_index=True)

                if all(
                    [
                        flow_block_flag == 1,
                        machine_no_input != "",
                        process_val_assigned_flag == 0,
                    ]
                ):

                    detail_dict = {}
                    detail_dict["FLOW_BLOCK_RUNG"] = flow_block_rung
                    detail_dict["SMC_BLOCK_RUNG"] = smc_block_rung
                    ng_str = 'ZDSの入力変数"MachineNo"に変数に数値が入力されていないため,流動制御が成り立っていない可能性有'
                    # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':[bdy], 'RULE_NUMBER': ["11"],
                    #         'CHECK_NUMBER':["3"], 'RULE_CONTENT':["FlowControlDataJudge_ZDS  detected , but Process_no not assigned to a valid value"],
                    #         'CHECK_CONTENT':'Check if a numerical value is assigned to the variable detected in ④. If not, NG is assumed.', 'STATUS': ['NG'], 'TARGET_OUTCOIL': [detail_dict],
                    #         'NG_EXPLANATION':[ng_str]}

                    # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':[bdy], 'RULE_NUMBER': ["11"],
                    #     'CHECK_NUMBER':["3"],  "RUNG_NUMBER":[""], 'RULE_CONTENT':["・For each process, check that all the processes before the own process are OK, and that all the facilities before the own facility are OK. If not, it is judged to be defective."],
                    #     'CHECK_CONTENT':'Check if a numerical value is assigned to the variable detected in ④. If not, NG is assumed.',
                    #     'TARGET_OUTCOIL': [detail_dict], 'STATUS': ['NG'],
                    #     'NG_EXPLANATION':[ng_str]}

                    sub_dict = {
                        "Result": ["NG"],
                        "Task": [program],
                        "Section": [""],
                        "RungNo": [""],
                        "Target": [detail_dict],
                        "CheckItem": rule_11_check_item,
                        "Detail": [ng_str],
                        "Status": [""],
                    }

                    sub_df = pd.DataFrame(sub_dict)

                    output_df = pd.concat([output_df, sub_df], ignore_index=True)

        return {"status": "OK", "output_df": output_df}

    except Exception as e:
        logger.error(f"Rule 11 Error in Rule 11 execution: {e}")
        return {"status": "NOT OK", "error": e}


###########################################################################################


# if __name__ == "__main__":


#     rule_11_look_up_df=pd.read_csv(r"C:\Users\aniln\OneDrive - OPTIMIZED SOLUTIONS LTD\DENSO\Denso\Ladder_pdf_xml_rule\OK_coding_checker\250729_rule10-15sampledata_v2\Input_Rule10-23_33_250729_sampleNG.csv")
#     rule_11_look_up_df=rule_11_look_up_df[['Task name', 'Machine Number']]
#     rule_11_look_up_df["Machine Number"] = rule_11_look_up_df["Machine Number"].fillna(-1000)
#     input_file=r"C:\Users\aniln\OneDrive - OPTIMIZED SOLUTIONS LTD\DENSO\GithubCode\rules_personal\FASTAPI\input_files\Coding Checker_Rule10-23_33_250729\Coding Checker_Rule10-23_33_250729_programwise.csv"
#     input_program_comment_file=r"C:\Users\aniln\OneDrive - OPTIMIZED SOLUTIONS LTD\DENSO\GithubCode\rules_personal\FASTAPI\input_files\Coding Checker_Rule10-23_33_250729\Coding Checker_Rule10-23_33_250729_programwise.json"

#     output_dir = r'C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Rules_implementation/pythoncode/output_csv/'
#     execute_rule_11(input_file=input_file, input_program_comment_file=input_program_comment_file,
#                                 rule_11_look_up_df=rule_11_look_up_df,
#                                 output_dir = output_dir,
#                                 output_file_name="Rule_11_program_NG.csv", program_name='', section_name='',
#                                 program_key='PROGRAM',body_type_key='BODY')


#     input_file=r"C:\Users\aniln\OneDrive - OPTIMIZED SOLUTIONS LTD\DENSO\GithubCode\rules_personal\FASTAPI\input_files\Coding Checker_Rule10-23_33_250729\Coding Checker_Rule10-23_33_250729_functionwise.csv"
#     input_program_comment_file=r"C:\Users\aniln\OneDrive - OPTIMIZED SOLUTIONS LTD\DENSO\GithubCode\rules_personal\FASTAPI\input_files\Coding Checker_Rule10-23_33_250729\Coding Checker_Rule10-23_33_250729_functionwise.json"
#     execute_rule_11(input_file=input_file, input_program_comment_file=input_program_comment_file,
#                                 rule_11_look_up_df=rule_11_look_up_df,
#                                 output_dir = output_dir,
#                                 output_file_name="Rule_11_function_NG.csv", program_name='', section_name='',
#                                 program_key='FUNCTION_BLOCK',body_type_key='BODY_TYPE')
