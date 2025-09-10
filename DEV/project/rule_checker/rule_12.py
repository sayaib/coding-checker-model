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

# ============================================ Comments referenced in Rule 24 processing ============================================
# memory feed Complete （記憶送り完了）/memory feed timing （記憶送りタイミング）/ memory shift timing （記憶シフトタイミング）/ memory shift Complete （記憶シフト完了）
autorun_section_name = "AutoRun"
autorun_with_star_section_name = "AutoRun★"
preparation_section_name = "Preparation"

memory_comment = "記憶"

rule_12_check_item = "Rule of Process Complete / OK or NG Result Set"

# ============================ Rule 24: Definitions, Content, and Configuration Details ============================
rule_content_12 = "・For each process, the OKbit shall be turned ON only when the OK decision is made. *Okbit must not be turned ON when NG is selected."
check_detail_content = {
    "cc1": "If the function block  in ② is not found in either of the following in the target task, NG is assumed.",
    "cc2": " If a variable is not entered in the input variable in ③, it is assumed to be NG.",
    "cc3": "If any of ④,⑤,⑥ cannot be found even though ③ is present, it is assumed to be NG.",
    "cc4": "Check the existence of A contact whose variable comment includes “OK” + '記憶(memory)” in the out-coil condition detected in step ⑥. (It is OK even if there are other contacts.)",
    "cc5": "If a variable is not entered in the input variable in ⑦, it is assumed to be NG.",
    "cc6": "Check if a numerical value is assigned to the variable detected in ⑧. If not, NG is assumed.",
    "cc7": "Check if the value assigned in ⑧ is UINT#2 if it is the largest value (exit process) in the input information “process number”, and UINT#1 otherwise.",
    "cc8": "If any of ⑨,⑩,⑪ cannot be found even though ③ is present, it is assumed to be NG.",
    "cc9": "Check the existence of A contact whose variable comment includes “NG” + '記憶(memory)'”' in the out-coil condition detected in step ⑪. (It is OK even if there are other contacts.)",
}

ng_content = {
    "cc1": "”preparation”また”AutoRun”セクション内にZFCが使用されていないため,流動制御が成り立っていない可能性有",
    "cc2": " ZFCの入力変数'WP_Result' に変数が接続されていないため,流動制御が成り立っていない可能性有",
    "cc3": "ZFCの入力変数" "WP_Result'のOK条件が設定されていないためNG",
    "cc4": ":ZFCの入力変数" "WP_Result'のOK条件にOK記憶が存在しないためNG",
    "cc5": "ZFCの入力変数'SelectProcess'に変数が接続されていないため,流動制御が成り立っていない可能性有",
    "cc6": "ZFCの入力変数'SelectProcess'に変数に数値が入力されていないため,流動制御が成り立っていない可能性有",
    "cc7": "ZFCの入力変数'SelectProcess'に適した数値が入力されていないためNG",
    "cc8": "ZFCの入力変数" "WP_Result'のOK条件が設定されていないためNG",
    "cc9": "ZFCの入力変数'WP_Result'のNG条件にNG記憶が存在しないためNG",
}

# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================


def detection_range_programwise(
    program_df: pd.DataFrame,
    program_name: str,
    program_comment_data: dict,
    autorun_section_name: str,
    autorun_with_star_section_name: str,
    preparation_section_name: str,
) -> dict:

    logger.info(
        f"Executing detection range on program {program_name} and section name {autorun_section_name} and {preparation_section_name}  on rule 24"
    )

    autorun_preparation_section_df = program_df[
        program_df["BODY"].isin(
            [
                autorun_section_name,
                autorun_with_star_section_name,
                preparation_section_name,
            ]
        )
    ]
    block_df = autorun_preparation_section_df[
        autorun_preparation_section_df["OBJECT_TYPE_LIST"] == "Block"
    ]

    FlowControlDataWrite_ZFC_block_exist = False
    FlowControlDataWrite_ZFC_block_exist_rung_number = -1
    FlowControlDataWrite_ZFC_block_exist_section_name = ""
    wp_result_input = ""
    select_process_input = ""
    match_select_precess_wp_result_rung_number = -1
    match_select_precess_wp_result_section_name = ""
    select_process_exist = False
    wp_result_exist = False

    move_with_wp_result_unit1_exist = False
    move_with_wp_result_unit1_exist_rung_number = -1
    move_with_wp_result_unit1_exist_section_name = ""
    match_move_prev_contact_operand = ""
    match_move_current_contact_operand = ""
    match_move_current_contact_operand_comment = ""
    match_move_prev_contact_operand_comment = []

    match_contact_used_as_outcoil_exist = False
    match_contact_used_as_outcoil_section_name = ""
    match_contact_used_as_outcoil_rung_number = -1

    move_with_wp_result_unit1_exist = False
    move_with_wp_result_unit1_exist_rung_number = -1
    move_with_wp_result_unit1_exist_section_name = ""
    match_move_prev_contact_operand_uint1 = ""
    match_move_current_contact_operand_uint1 = ""
    match_move_prev_contact_operand_comment_uint1 = []

    move_with_wp_result_uint2_exist = False
    move_with_wp_result_uint2_exist_rung_number = -1
    move_with_wp_result_uint2_exist_section_name = ""
    match_move_prev_contact_operand_uint2 = ""
    match_move_current_contact_operand_uint2 = ""
    match_move_prev_contact_operand_comment_uint2 = []

    match_move_current_contact_operand_comment = ""

    match_contact_used_as_outcoil_uint1_exist = False
    match_contact_used_as_outcoil_section_name_uint1 = ""
    match_contact_used_as_outcoil_rung_number_uint1 = -1

    ok_memory_contact_found_uint1 = False
    ok_memory_contact_operand_uint1 = ""
    ok_memory_contact_rung_number_uint1 = -1

    match_contact_used_as_outcoil_uint2_exist = False
    match_contact_used_as_outcoil_section_name_uint2 = ""
    match_contact_used_as_outcoil_rung_number_uint2 = -1

    ok_memory_contact_found_uint2 = False
    ok_memory_contact_operand_uint2 = ""
    ok_memory_contact_rung_number_uint2 = -1

    for _, block_row in block_df.iterrows():
        attr = ast.literal_eval(block_row["ATTRIBUTES"])
        attr_typename = attr.get("typeName")

        """
        below code is written for checking if FlowControlDataWrite_ZFC exist
        If exist then get SelectProcess and WP_Result input save it details like rung number, section_name
        """
        if all([attr_typename != None, attr_typename == "FlowControlDataWrite_ZFC"]):
            FlowControlDataWrite_ZFC_block_exist = True
            FlowControlDataWrite_ZFC_block_exist_rung_number = block_row["RUNG"]
            FlowControlDataWrite_ZFC_block_exist_section_name = block_row["BODY"]

            ladder_rung = autorun_preparation_section_df[
                (autorun_preparation_section_df["RUNG"] == block_row["RUNG"])
                & (autorun_preparation_section_df["BODY"] == block_row["BODY"])
            ]

            block_connections = get_block_connections(pl.from_pandas(ladder_rung))

            for block in block_connections:
                try:
                    for key, values in block.items():
                        for val in values:
                            select_process_exist = val.get("SelectProcess")
                            if select_process_exist:
                                val_select_process = select_process_exist

                            wp_result_exist = val.get("WP_Result")
                            if wp_result_exist:
                                val_wp_result = wp_result_exist

                        if val_select_process and val_wp_result:
                            select_process_input = val_select_process[0]
                            wp_result_input = val_wp_result[0]
                            match_select_precess_wp_result_rung_number = block_row[
                                "RUNG"
                            ]
                            match_select_precess_wp_result_section_name = block_row[
                                "BODY"
                            ]
                            break
                except:
                    pass

        """
        below code is to find the move block having wp_result input and use as out in move block and find previous contact of it 
        if prevoius contacg exist then save it details and use it check content details
        """
        block_contact_df = autorun_preparation_section_df[
            (autorun_preparation_section_df["OBJECT_TYPE_LIST"] == "Block")
            | (autorun_preparation_section_df["OBJECT_TYPE_LIST"] == "Contact")
        ]

        print("wp_result_input", wp_result_input)
        if wp_result_input:

            print("block_contact_df", block_contact_df)

            for _, block_contact_row in block_contact_df.iterrows():

                if block_contact_row["OBJECT_TYPE_LIST"] == "Contact":
                    attr = ast.literal_eval(block_contact_row["ATTRIBUTES"])
                    if (
                        attr.get("operand")
                        and isinstance(attr.get("operand"), str)
                        and attr.get("negated") == "false"
                    ):
                        match_move_current_contact_operand = attr.get("operand")
                        if match_move_current_contact_operand and isinstance(
                            match_move_current_contact_operand, str
                        ):
                            match_move_current_contact_operand_comment = (
                                get_the_comment_from_program(
                                    match_move_current_contact_operand,
                                    program_name,
                                    program_comment_data,
                                )
                            )

                elif block_contact_row["OBJECT_TYPE_LIST"] == "Block":
                    attr = ast.literal_eval(block_contact_row["ATTRIBUTES"])
                    attr_typename = attr.get("typeName")

                    if attr_typename != None and (
                        attr_typename.lower() == "move"
                        or attr_typename.lower() == "@move"
                    ):
                        try:
                            ladder_rung = autorun_preparation_section_df[
                                (
                                    autorun_preparation_section_df["RUNG"]
                                    == block_contact_row["RUNG"]
                                )
                                & (
                                    autorun_preparation_section_df["BODY"]
                                    == block_contact_row["BODY"]
                                )
                            ]
                            block_connections_details = get_block_connections(
                                pl.from_pandas(ladder_rung)
                            )

                            for block_data in block_connections_details:
                                if (
                                    "MOVE" in block_data.keys()
                                    or "@MOVE" in block_data.keys()
                                ):
                                    move_block_output_operand = ""
                                    move_block_input_operand = ""

                                    if "MOVE" in block_data.keys():
                                        move_block_data = block_data["MOVE"]

                                    if "@MOVE" in block_data.keys():
                                        move_block_data = block_data["@MOVE"]

                                    for param in move_block_data:
                                        if "In" in param:
                                            move_block_input_operand = param["In"][0]
                                        if "Out" in param:
                                            move_block_output_operand = param["Out"][0]

                                    if (
                                        move_block_output_operand == wp_result_input
                                        and move_block_input_operand == "UINT#1"
                                        and not move_with_wp_result_unit1_exist
                                    ):
                                        move_with_wp_result_unit1_exist = True
                                        move_with_wp_result_unit1_exist_rung_number = (
                                            block_contact_row["RUNG"]
                                        )
                                        move_with_wp_result_unit1_exist_section_name = (
                                            block_contact_row["BODY"]
                                        )
                                        match_move_prev_contact_operand_uint1 = (
                                            match_move_current_contact_operand
                                        )
                                        match_move_prev_contact_operand_comment_uint1 = (
                                            match_move_current_contact_operand_comment
                                        )

                                    if (
                                        move_block_output_operand == wp_result_input
                                        and move_block_input_operand == "UINT#2"
                                        and not move_with_wp_result_uint2_exist
                                    ):
                                        move_with_wp_result_uint2_exist = True
                                        move_with_wp_result_uint2_exist_rung_number = (
                                            block_contact_row["RUNG"]
                                        )
                                        move_with_wp_result_uint2_exist_section_name = (
                                            block_contact_row["BODY"]
                                        )
                                        match_move_prev_contact_operand_uint2 = (
                                            match_move_current_contact_operand
                                        )
                                        match_move_prev_contact_operand_comment_uint2 = (
                                            match_move_current_contact_operand_comment
                                        )
                        except:
                            logger.info("Rule 12 detection range move block error")

                    else:
                        match_move_current_contact_operand = ""
                        match_move_current_contact_operand_comment = ""

                    print(
                        "match_move_current_contact_operand",
                        match_move_current_contact_operand,
                    )
                    print(
                        "block_contact_row['OBJECT_TYPE_LIST']",
                        block_contact_row["OBJECT_TYPE_LIST"],
                    )

                if move_with_wp_result_unit1_exist and move_with_wp_result_uint2_exist:
                    break

        """
        below code is used to check if the previous contact with move block used as outcoil in task name
        """

        """
        for check content 4 to find the coil having A contact attached to coil for uint1
        """
        print(
            "match_move_prev_contact_operand_uint1",
            match_move_prev_contact_operand_uint1,
        )
        print(
            "match_move_prev_contact_operand_uint2",
            match_move_prev_contact_operand_uint2,
        )

        if match_move_prev_contact_operand_uint1:
            coil_df = autorun_preparation_section_df[
                autorun_preparation_section_df["OBJECT_TYPE_LIST"] == "Coil"
            ]

            if not coil_df.empty:
                for _, coil_row in coil_df.iterrows():
                    attr = ast.literal_eval(coil_row["ATTRIBUTES"])
                    coil_operand = attr.get("operand")

                    if (
                        coil_operand
                        and isinstance(coil_operand, str)
                        and coil_operand == match_move_prev_contact_operand_uint1
                    ):
                        match_contact_used_as_outcoil_uint1_exist = True
                        match_contact_used_as_outcoil_section_name_uint1 = coil_row[
                            "BODY"
                        ]
                        match_contact_used_as_outcoil_rung_number_uint1 = coil_row[
                            "RUNG"
                        ]
                        break

            print(
                "match_contact_used_as_outcoil_rung_number_uint1",
                match_contact_used_as_outcoil_rung_number_uint1,
            )
            if match_contact_used_as_outcoil_rung_number_uint1 != -1:
                contact_df = autorun_preparation_section_df[
                    (autorun_preparation_section_df["OBJECT_TYPE_LIST"] == "Contact")
                    & (
                        autorun_preparation_section_df["RUNG"]
                        == match_contact_used_as_outcoil_rung_number_uint1
                    )
                ]

                if not contact_df.empty:
                    for _, contact_row in contact_df.iterrows():
                        attr = ast.literal_eval(contact_row["ATTRIBUTES"])
                        contact_operand = attr.get("operand")
                        negated_operand = attr.get("negated")
                        if (
                            contact_operand
                            and isinstance(contact_operand, str)
                            and negated_operand == "false"
                        ):
                            contact_comment = get_the_comment_from_program(
                                contact_operand, program_name, program_comment_data
                            )
                            if contact_comment and isinstance(contact_comment, list):
                                if regex_pattern_check(
                                    "OK", contact_comment
                                ) and regex_pattern_check(
                                    memory_comment, contact_comment
                                ):
                                    ok_memory_contact_found_uint1 = True
                                    ok_memory_contact_operand_uint1 = contact_operand
                                    ok_memory_contact_rung_number_uint1 = contact_row[
                                        "RUNG"
                                    ]
                                    break

        """
        for check content 4 to find the coil having contact attached to coil for uint2
        """

        if match_move_prev_contact_operand_uint2:
            coil_df = autorun_preparation_section_df[
                autorun_preparation_section_df["OBJECT_TYPE_LIST"] == "Coil"
            ]

            if not coil_df.empty:
                for _, coil_row in coil_df.iterrows():
                    attr = ast.literal_eval(coil_row["ATTRIBUTES"])
                    coil_operand = attr.get("operand")

                    if (
                        coil_operand
                        and isinstance(coil_operand, str)
                        and coil_operand == match_move_prev_contact_operand_uint2
                    ):
                        match_contact_used_as_outcoil_uint2_exist = True
                        match_contact_used_as_outcoil_section_name_uint2 = coil_row[
                            "BODY"
                        ]
                        match_contact_used_as_outcoil_rung_number_uint2 = coil_row[
                            "RUNG"
                        ]
                        break

            if match_contact_used_as_outcoil_rung_number_uint2 != -1:
                contact_df = autorun_preparation_section_df[
                    (autorun_preparation_section_df["OBJECT_TYPE_LIST"] == "Contact")
                    & (
                        autorun_preparation_section_df["RUNG"]
                        == match_contact_used_as_outcoil_rung_number_uint2
                    )
                ]

                if not contact_df.empty:
                    for _, contact_row in contact_df.iterrows():
                        attr = ast.literal_eval(contact_row["ATTRIBUTES"])
                        contact_operand = attr.get("operand")
                        if contact_operand and isinstance(contact_operand, str):
                            contact_comment = get_the_comment_from_program(
                                contact_operand, program_name, program_comment_data
                            )
                            if contact_comment and isinstance(contact_comment, list):
                                if regex_pattern_check(
                                    "NG", contact_comment
                                ) and regex_pattern_check(
                                    memory_comment, contact_comment
                                ):
                                    ok_memory_contact_found_uint2 = True
                                    ok_memory_contact_operand_uint2 = contact_operand
                                    ok_memory_contact_rung_number_uint2 = contact_row[
                                        "RUNG"
                                    ]
                                    break

    return {
        "FlowControlDataWrite_ZFC_block_exist_details": [
            FlowControlDataWrite_ZFC_block_exist,
            FlowControlDataWrite_ZFC_block_exist_rung_number,
            FlowControlDataWrite_ZFC_block_exist_section_name,
        ],
        "select_process_input_details": [
            select_process_input,
            match_select_precess_wp_result_rung_number,
            match_select_precess_wp_result_section_name,
        ],
        "wp_result_input_details": [
            wp_result_input,
            match_select_precess_wp_result_rung_number,
            match_select_precess_wp_result_section_name,
        ],
        "match_move_block_uint1_exist": [
            move_with_wp_result_unit1_exist,
            move_with_wp_result_unit1_exist_rung_number,
        ],
        "match_move_block_uint2_exist": [
            move_with_wp_result_uint2_exist,
            move_with_wp_result_uint2_exist_rung_number,
        ],
        "match_move_prev_contact_uint1": [
            match_move_prev_contact_operand_uint1,
            move_with_wp_result_unit1_exist_rung_number,
            match_move_prev_contact_operand_comment_uint1,
        ],
        "match_move_prev_contact_uint2": [
            match_move_prev_contact_operand_uint2,
            move_with_wp_result_uint2_exist_rung_number,
            match_move_prev_contact_operand_comment_uint2,
        ],
        "ok_memory_contact_details_uint1": [
            ok_memory_contact_found_uint1,
            ok_memory_contact_operand_uint1,
            ok_memory_contact_rung_number_uint1,
        ],
        "ok_memory_contact_details_uint2": [
            ok_memory_contact_found_uint2,
            ok_memory_contact_operand_uint2,
            ok_memory_contact_rung_number_uint2,
        ],
        "match_move_uint1_block_section_name": [
            move_with_wp_result_unit1_exist_section_name
        ],
        "match_contact_used_as_outcoil_uint1": [
            match_contact_used_as_outcoil_uint1_exist,
            match_contact_used_as_outcoil_section_name_uint1,
            match_contact_used_as_outcoil_rung_number_uint1,
        ],
        "match_contact_used_as_outcoil_uint2": [
            match_contact_used_as_outcoil_uint2_exist,
            match_contact_used_as_outcoil_section_name_uint2,
            match_contact_used_as_outcoil_rung_number_uint2,
        ],
    }


def check_detail_1_programwise(detection_result: dict, program_name: str) -> dict:

    logger.info("Rule 12 checking contect 1 program wise")

    status = "NG"
    section_name = ""
    rung_number = -1
    if detection_result["FlowControlDataWrite_ZFC_block_exist_details"][0] is True:
        status = "OK"
        section_name = (
            detection_result["FlowControlDataWrite_ZFC_block_exist_details"][2],
        )
        rung_number = detection_result["FlowControlDataWrite_ZFC_block_exist_details"][
            1
        ]

    return {
        "cc": "cc1",
        "status": status,
        "section_name": section_name,
        "check_number": "1",
        "rung_number": rung_number,
        "outcoil": "",
    }


def check_detail_2_programwise(detection_result: dict, program_name: str) -> dict:

    logger.info("Rule 12 checking contect 2 program wise")

    status = "NG"
    section_name = ""
    rung_number = -1

    if detection_result["wp_result_input_details"][0]:
        status = "OK"
        section_name = (detection_result["wp_result_input_details"][2],)
        rung_number = detection_result["wp_result_input_details"][1]

    return {
        "cc": "cc2",
        "status": status,
        "section_name": section_name,
        "check_number": "2",
        "rung_number": rung_number,
        "outcoil": "",
    }


def check_detail_3_programwise(detection_result: dict, program_name: str) -> dict:

    logger.info("Rule 12 checking contect 3 program wise")

    status = "NG"
    if (
        detection_result["match_move_block_uint1_exist"][0]
        and detection_result["match_move_prev_contact_uint1"][0]
        and detection_result["match_contact_used_as_outcoil_uint1"][0]
    ):
        status = "OK"

    return {
        "cc": "cc3",
        "status": status,
        "section_name": "",
        "check_number": "3",
        "rung_number": -1,
        "outcoil": "",
    }


def check_detail_4_programwise(
    detection_result: dict, memory_comment: str, program_name: str
) -> dict:

    logger.info("Rule 12 checking contect 4 program wise")

    status = "NG"
    rung_number = -1
    if detection_result["ok_memory_contact_details_uint1"][0]:
        status = "OK"
        rung_number = detection_result["ok_memory_contact_details_uint1"][2]

    return {
        "cc": "cc4",
        "status": status,
        "section_name": "",
        "check_number": "4",
        "rung_number": rung_number,
        "outcoil": "",
    }


def check_detail_5_programwise(detection_result: dict, program_name: str) -> dict:

    logger.info("Rule 12 checking contect 5 program wise")

    status = "NG"
    section_name = ("",)
    rung_number = -1
    if detection_result["select_process_input_details"][0]:
        status = "OK"
        section_name = (detection_result["select_process_input_details"][2],)
        rung_number = detection_result["select_process_input_details"][1]

    return {
        "cc": "cc5",
        "status": status,
        "section_name": section_name,
        "check_number": "5",
        "rung_number": rung_number,
        "outcoil": "",
    }


def check_detail_6_programwise(
    program_df: pd.DataFrame, detection_result: dict, program_name: str
) -> dict:

    logger.info("Rule 12 checking contect 6 program wise")

    status = "NG"
    section_name = ""
    rung_number = -1
    data_var_value = ""
    if detection_result["select_process_input_details"][0]:
        select_process_rung_number = detection_result["select_process_input_details"][1]
        select_process_sectionname = detection_result["select_process_input_details"][2]
        if select_process_rung_number and select_process_sectionname:
            match_process_section_df = program_df[program_df["PROGRAM"] == program_name]
            smc_blocks = match_process_section_df[
                match_process_section_df["OBJECT_TYPE_LIST"] == "smcext:InlineST"
            ]
            for _, smc_blocks_row in smc_blocks.iterrows():
                smc_blocks_attrs = ast.literal_eval(smc_blocks_row["ATTRIBUTES"])
                data_inputs = smc_blocks_attrs.get("data_inputs")
                for data_ in data_inputs:
                    if re.search(
                        re.escape(detection_result["select_process_input_details"][0]),
                        data_,
                    ):
                        status = "OK"
                        section_name = smc_blocks_row.get("BODY")
                        rung_number = smc_blocks_row.get("RUNG")

                        if re.search(r";", data_):
                            data_var = data_.split(";")[0]
                            data_var_value = data_var.split("=")[1].strip()

                            break
                if status == "OK":
                    break

    return {
        "cc": "cc6",
        "status": status,
        "section_name": section_name,
        "check_number": "6",
        "rung_number": rung_number,
        "outcoil": "",
        "assign_value": data_var_value,
    }


def check_detail_7_programwise(
    input_image_df: pd.DataFrame, cc6_results: dict, program_name: str
) -> dict:

    logger.info("Rule 12 checking contect 7 program wise")

    status = "NG"

    if cc6_results.get("assign_value"):
        input_image_df["Process No"] = pd.to_numeric(
            input_image_df["Process No"], errors="coerce"
        )
        max_op_number = input_image_df["Process No"].max()
        tasknames_with_max_op = input_image_df[
            input_image_df["Process No"] == max_op_number
        ]["Task name"].tolist()

        if program_name in tasknames_with_max_op:
            if cc6_results.get("assign_value") == "UINT#2":
                status = "OK"
        else:
            if cc6_results.get("assign_value") == "UINT#1":
                status = "OK"

    return {
        "cc": "cc7",
        "status": status,
        "section_name": "",
        "check_number": "7",
        "rung_number": -1,
        "outcoil": "",
    }


def check_detail_8_programwise(detection_result: dict) -> dict:

    logger.info("Rule 12 checking contect 8 program wise")

    status = "NG"
    if (
        detection_result["match_move_block_uint2_exist"][0]
        and detection_result["match_move_prev_contact_uint2"][0]
        and detection_result["match_contact_used_as_outcoil_uint2"][0]
    ):
        status = "OK"

    return {
        "cc": "cc8",
        "status": status,
        "section_name": "",
        "check_number": "8",
        "rung_number": -1,
        "outcoil": "",
    }


def check_detail_9_programwise(detection_result: dict) -> dict:

    logger.info("Rule 12 checking contect 9 program wise")
    status = "NG"
    rung_number = -1
    if detection_result["ok_memory_contact_details_uint2"][0]:
        status = "OK"
        rung_number = detection_result["ok_memory_contact_details_uint2"][2]

    return {
        "cc": "cc9",
        "status": status,
        "section_name": "",
        "check_number": "9",
        "rung_number": rung_number,
        "outcoil": "",
    }


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_12_programwise(
    input_program_file: str,
    input_program_comment_file: str,
    input_image_csv_file: str,
) -> pd.DataFrame:

    logger.info("Starting execution of Rule 12")

    try:

        all_program_df = pd.read_csv(input_program_file)
        input_image_df = pd.read_csv(input_image_csv_file)
        with open(input_program_comment_file, "r", encoding="utf-8") as file:
            program_comment_data = json.load(file)

        unique_program_values = all_program_df["PROGRAM"].unique()

        output_rows = []
        for program in unique_program_values:
            logger.info(f"Executing in Program {program}")

            program_df = all_program_df[all_program_df["PROGRAM"] == program]

            unique_section_values = program_df["BODY"].unique()

            lower_values = {val.lower() for val in unique_section_values}

            if "autorun" in lower_values and "preparation" in lower_values:

                # Run detection range logic as per Rule 24
                detection_result = detection_range_programwise(
                    program_df=program_df,
                    program_name=program,
                    program_comment_data=program_comment_data,
                    autorun_section_name=autorun_section_name,
                    autorun_with_star_section_name=autorun_with_star_section_name,
                    preparation_section_name=preparation_section_name,
                )

                cc1_results = check_detail_1_programwise(
                    detection_result=detection_result, program_name=program
                )
                cc2_results = check_detail_2_programwise(
                    detection_result=detection_result, program_name=program
                )
                cc3_results = check_detail_3_programwise(
                    detection_result=detection_result, program_name=program
                )
                cc4_results = check_detail_4_programwise(
                    detection_result=detection_result,
                    memory_comment=memory_comment,
                    program_name=program,
                )
                cc5_results = check_detail_5_programwise(
                    detection_result=detection_result, program_name=program
                )
                cc6_results = check_detail_6_programwise(
                    program_df=program_df,
                    detection_result=detection_result,
                    program_name=program,
                )
                cc7_results = check_detail_7_programwise(
                    input_image_df=input_image_df,
                    cc6_results=cc6_results,
                    program_name=program,
                )
                cc8_results = check_detail_8_programwise(detection_result)
                cc9_results = check_detail_9_programwise(detection_result)

                all_cc_result = [
                    cc1_results,
                    cc2_results,
                    cc3_results,
                    cc4_results,
                    cc5_results,
                    cc6_results,
                    cc7_results,
                    cc8_results,
                    cc9_results,
                ]

                for cc_result in all_cc_result:
                    ng_name = (
                        ng_content.get(cc_result.get("cc", ""))
                        if cc_result.get("status") == "NG"
                        else ""
                    )
                    rung_number = (
                        cc_result.get("rung_number") - 1
                        if cc_result.get("rung_number") != -1
                        else -1
                    )
                    target_outcoil = (
                        cc_result.get("outcoil") if cc_result.get("outcoil") else ""
                    )
                    check_number = cc_result.get("check_number")
                    section_name = cc_result.get("section_name")

                    output_rows.append(
                        {
                            "Result": cc_result.get("status"),
                            "Task": program,
                            "Section": section_name,
                            "RungNo": rung_number,
                            "Target": target_outcoil,
                            "CheckItem": rule_12_check_item,
                            "Detail": ng_name,
                            "Status": "",
                        }
                    )

        final_result_csv = pd.DataFrame(output_rows)
        if not final_result_csv.empty:
            if "RungNo" in final_result_csv.columns:
                final_result_csv["RungNo"] = final_result_csv["RungNo"].apply(
                    clean_rung_number
                )
        else:
            final_result_csv = pd.DataFrame(
                columns=[
                    "Result",
                    "Task",
                    "Section",
                    "RungNo",
                    "Target",
                    "CheckItem",
                    "Detail",
                    "Status",
                ]
            )

        return {"status": "OK", "output_df": final_result_csv}

    except Exception as e:
        logger.error(f"Rule 12 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}
