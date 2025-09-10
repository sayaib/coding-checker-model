import ast
import json
import pandas as pd
from typing import *
import re
from ...main import logger
import polars as pl
from collections import defaultdict
from .extract_comment_from_variable import (
    get_the_comment_from_function,
    get_the_comment_from_program,
)
from .ladder_utils import regex_pattern_check, clean_rung_number
from .rule_56_ladder_utils import (
    get_series_contacts_coil,
    get_parallel_contacts,
    get_format_parellel_contact_detail,
)

# ============================================ Comments referenced in Rule 25 processing ============================================
# memory feed Complete （記憶送り完了）/memory feed timing （記憶送りタイミング）/ memory shift timing （記憶シフトタイミング）/ memory shift Complete （記憶シフト完了）
st_r_comment = "ST_R"
st_y_comment = "ST_Y"
st_g_comment = "ST_G"
buzzer_full_width_comment = "ブザー"
autorun_comment = "自動運転"
master_on_comment = "運転準備"
complete_comment = "完了"
not_comment = "でない"
abnormal_comment = "異常"
warning_comment = "警報"
battery_comment = "ﾊﾞｯﾃﾘ"
replace_comment = "交換"
servo_comment = "ｻｰﾎﾞ"


# ============================ Rule 46: Definitions, Content, and Configuration Details ============================
rule_content_56 = "・Emergency stop, Auto stop, cycle stop, red for Abnormal stop, yellow for warning, green for normal operation. Also, flashing green on Master ON"
rule_56_check_item = "Rule of Tower Light Indication Circuit"

check_detail_content = {
    "cc1": " If ① but not ③ or ④ or ⑤, it is assumed to be NG.",
    "cc2": "Check that the B contact that contains the variable comment ”異常でない(not abnormal)” and the B contact that contains the variable comment ”ブザー(buzzer)”＋ '異常(abnormal)' exist in series (AND) in the out-coil condition detected in ③.",
    "cc3": "Check that the A contact that contains the variable comment 'PLC'+'ﾊﾞｯﾃﾘ(battery)'+'交換(replace)'  in the out-coil condition detected in ③.",
    "cc4": "Check that the A contact that contains the variable comment 'ｻｰﾎﾞ(servo)'+'ﾊﾞｯﾃﾘ(battery)'+'交換(replace)'  in the out-coil condition detected in ③.",
    "cc5": "Check that ❷, ❸, and ❹are in a parallel (OR) circuit in the out-coil conditions detected in ③.",
    "cc6": "Check that the B contact that contains the variable comment ,”警報でない(not warning)” and the B contact that contains the variable comment ”ブザー(buzzer)”＋ '警報(warning)' exist in series (AND) in the out-coil condition detected in ④.",
    "cc7": "Check that the A contact that contains the variable comment ,”自動運転(auto run)” and the B contact of the variable detected in ③ and the B contact of the variable detected in ④ exist in series (AND) in the out-coil condition detected in ⑤.",
    "cc8": " Check that the A contact that contains the variable comment ”運転準備(master on)”+'完了(complete)” and the A contact that contains the variable name 'ap' exist in series (AND) in the out-coil condition detected in ⑤.",
    "cc9": "Check that ❼and❽ are in a parallel (OR) circuit in the out-coil conditions detected in ⑤.",
}

ng_content = {
    "cc1": "ｼｸﾞﾅﾙﾀﾜｰ点灯ｺｲﾙなしのためNG(NG due to no signal tower lighting coil)",
    "cc2": "ｼｸﾞﾅﾙﾀﾜｰ赤を点灯するｺｲﾙの条件が異常でないのB接点とブザー消音のB接点での直列回路になっていないためNG (NG because the condition of the coil that lights the signal tower red is not in series circuit with the B contact that is not abnormal and the B contact that silences the buzzer.)",
    "cc3": "ｼｸﾞﾅﾙﾀﾜｰ赤を点灯するｺｲﾙの条件にPLCﾊﾞｯﾃﾘ交換のA接点が存在しないためNG (NG because there is no A contact for PLC battery replacement in the condition of the coil to light the signal tower red.)",
    "cc4": "ｼｸﾞﾅﾙﾀﾜｰ赤を点灯するｺｲﾙの条件にｻｰﾎﾞﾊﾞｯﾃﾘ交換のA接点が存在しないためNG (NG because there is no A contact for servo battery replacement in the condition of the coil to light the signal tower red.)",
    "cc5": "ｼｸﾞﾅﾙﾀﾜｰ赤を点灯するｺｲﾙの条件が標準通りになっていないためNG(NG because the condition of the coil to light the signal tower red is not up to standard.)",
    "cc6": "ｼｸﾞﾅﾙﾀﾜｰ黄を点灯するｺｲﾙの条件が警報でないのB接点とブザー消音のB接点での直列回路になっていないためNG (NG because the condition of the coil that lights the signal tower yellow is not in series circuit with the B contact that is not warning and the B contact that silences the buzzer.)",
    "cc7": "ｼｸﾞﾅﾙﾀﾜｰ緑を点灯するｺｲﾙの条件が'自動運転中ON&ｼｸﾞﾅﾙﾀﾜｰ赤点灯OFF&ｼｸﾞﾅﾙﾀﾜｰ黄点灯OFF'になっていないためNG (NG because the condition of the coil that turns on the signal tower green is not “ON during automatic operation & signal tower red light OFF & signal tower yellow light OFF”.)",
    "cc8": "ｼｸﾞﾅﾙﾀﾜｰ緑を点灯するｺｲﾙの条件が'運転準備完了ON&クロックパルスONになっていないためNG (NG because the coil condition to light the signal tower green is not “operation preparation complete ON & clock pulse ON”.)",
    "cc9": "ｼｸﾞﾅﾙﾀﾜｰ緑を点灯するｺｲﾙの条件が'標準通りになっていないためNG(NG because the condition of the coil to light the signal tower green is not up to standard.)",
}

fault_section_name = "fault"

# ============================ Helper Functions for both Program-Wise and Function-Wise Operations ============================


def get_first_contact_from_series(*contact_details):
    """
    Accepts 2 or 3 contact details, each as a list of dicts (usually with one dict inside).
    Returns the operand that comes first in the contact chain.
    """
    contacts = [c[0] for c in contact_details]  # Flatten inner dicts
    operands = [c.get("operand") for c in contacts]
    in_lists = [set(c.get("in_list", [])) for c in contacts]
    out_lists = [set(c.get("out_list", [])) for c in contacts]

    # Build edges: from_operand -> to_operand
    edges = []
    for i in range(len(contacts)):
        for j in range(len(contacts)):
            if i == j:
                continue
            if out_lists[i] & in_lists[j]:  # intersection is non-empty
                edges.append((operands[i], operands[j]))

    # Count incoming edges
    incoming_count = defaultdict(int)
    for src, dst in edges:
        incoming_count[dst] += 1
        if src not in incoming_count:
            incoming_count[src] = 0

    # Find operand with 0 incoming edges (first in chain)
    for operand, count in incoming_count.items():
        if count == 0:
            return operand

    # Fallback: return first contact's operand
    return operands[0]


# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================


def detection_range_programwise(
    fault_section_df: pd.DataFrame,
    st_r_comment: str,
    st_y_comment: str,
    st_g_comment: str,
    program_name: str,
    program_comment_data: str,
) -> dict:

    logger.info(f"Executing rule 56 detection range on program {program_name}")

    detection_3_coil = detection_4_coil = detection_5_coil = ""
    detection_3_rung_number = detection_4_rung_number = detection_5_rung_number = -1
    fault_check_st_r_twice = False
    fault_check_st_y_twice = False
    fault_check_st_g_twice = False

    if not fault_section_df.empty:

        fault_coil_df = fault_section_df[fault_section_df["OBJECT_TYPE_LIST"] == "Coil"]

        for _, fault_coil_row in fault_coil_df.iterrows():
            attr = ast.literal_eval(fault_coil_row["ATTRIBUTES"])
            fault_coil_operand = attr.get("operand")

            # print(":fault_coil_operand", fault_coil_operand)
            if fault_coil_operand and isinstance(fault_coil_operand, str):
                if st_r_comment in fault_coil_operand and not fault_check_st_r_twice:
                    fault_check_st_r_twice = True
                    detection_3_coil = fault_coil_operand
                    detection_3_rung_number = fault_coil_row["RUNG"]

                if st_y_comment in fault_coil_operand and not fault_check_st_y_twice:
                    fault_check_st_y_twice = True
                    detection_4_coil = fault_coil_operand
                    detection_4_rung_number = fault_coil_row["RUNG"]

                if st_g_comment in fault_coil_operand and not fault_check_st_g_twice:
                    fault_check_st_g_twice = True
                    detection_5_coil = fault_coil_operand
                    detection_5_rung_number = fault_coil_row["RUNG"]

    return {
        "detection_3_details": [detection_3_coil, detection_3_rung_number],
        "detection_4_details": [detection_4_coil, detection_4_rung_number],
        "detection_5_details": [detection_5_coil, detection_5_rung_number],
    }


def check_detail_1_programwise(detection_results: dict) -> dict:

    logger.info(f"Executing rule 56 and check detail 1")

    cc1_result = {}

    status = "OK"
    if (
        detection_results["detection_3_details"][1] == -1
        or detection_results["detection_4_details"][1] == -1
        or detection_results["detection_5_details"][1] == -1
    ):
        status = "NG"

    cc1_result["cc"] = "cc1"
    cc1_result["status"] = status
    cc1_result["check_number"] = 1
    cc1_result["target_coil"] = ""
    cc1_result["rung_number"] = -1

    return cc1_result


def check_detail_2_programwise(
    fault_section_df: pd.DataFrame,
    cc_detection_3_coil_operand: str,
    cc_detection_3_rung_number: int,
    not_comment: str,
    abnormal_comment: str,
    buzzer_full_width_comment: str,
    program_name: str,
    program_comment_data: dict,
) -> dict:

    logger.info(f"Executing rule no 56 check detail 2")

    not_abnormal_found = False
    buzzer_abnormal_found = False
    not_abnormal_operand = ""
    buzzer_abnormal_operand = ""

    contact_fault_df = fault_section_df[
        (fault_section_df["OBJECT_TYPE_LIST"] == "Contact")
        & (fault_section_df["RUNG"] == cc_detection_3_rung_number)
    ].copy()

    if (
        cc_detection_3_rung_number >= 0
        and cc_detection_3_coil_operand
        and not contact_fault_df.empty
    ):
        for _, contact_row in contact_fault_df.iterrows():
            attr = ast.literal_eval(contact_row["ATTRIBUTES"])
            contact_operand = attr.get("operand")
            negated_operand = attr.get("negated")
            if contact_operand and isinstance(contact_operand, str):

                contact_comment = get_the_comment_from_program(
                    contact_operand, program_name, program_comment_data
                )

                if isinstance(contact_comment, list) and contact_comment:
                    if (
                        regex_pattern_check(not_comment, contact_comment)
                        and regex_pattern_check(abnormal_comment, contact_comment)
                        and not not_abnormal_found
                        and negated_operand == "true"
                    ):
                        not_abnormal_found = True
                        not_abnormal_operand = contact_operand

                if isinstance(contact_comment, list) and contact_comment:
                    if (
                        regex_pattern_check(buzzer_full_width_comment, contact_comment)
                        and regex_pattern_check(abnormal_comment, contact_comment)
                        and not buzzer_abnormal_found
                        and negated_operand == "true"
                    ):
                        buzzer_abnormal_found = True
                        buzzer_abnormal_operand = contact_operand

        if not_abnormal_operand and buzzer_abnormal_operand:

            cc_detection_3_rung_number_details = fault_section_df[
                fault_section_df["RUNG"] == cc_detection_3_rung_number
            ]
            cc_detection_3_rung_number_details_polar = pl.from_pandas(
                cc_detection_3_rung_number_details
            )
            cc2_detection_3_get_series_data = get_series_contacts_coil(
                cc_detection_3_rung_number_details_polar
            )

            operand_list_per_rung = [
                [step["operand"] for step in rung]
                for rung in cc2_detection_3_get_series_data
            ]
            if operand_list_per_rung:
                for rung_operands in operand_list_per_rung:
                    if (
                        not_abnormal_operand in rung_operands
                        and buzzer_abnormal_operand in rung_operands
                    ):

                        return {
                            "cc": "cc2",
                            "status": "OK",
                            "check_number": 2,
                            "target_coil": "",
                            "rung_number": cc_detection_3_rung_number,
                            "not_abnormal_operand": not_abnormal_operand,
                            "buzzer_abnormal_operand": buzzer_abnormal_operand,
                        }

    return {
        "cc": "cc2",
        "status": "NG",
        "check_number": 2,
        "target_coil": "",
        "rung_number": -1,
        "not_abnormal_operand": "",
        "buzzer_abnormal_operand": "",
    }


def check_detail_3_programwise(
    fault_section_df: pd.DataFrame,
    cc_detection_3_coil_operand: str,
    cc_detection_3_rung_number: int,
    battery_comment: str,
    replace_comment: str,
    program_name: str,
    program_comment_data: dict,
) -> dict:

    logger.info(f"Executing rule no 56 check detail 3")

    contact_fault_df = fault_section_df[
        (fault_section_df["OBJECT_TYPE_LIST"] == "Contact")
        & (fault_section_df["RUNG"] == cc_detection_3_rung_number)
    ].copy()

    if cc_detection_3_rung_number >= 0 and not contact_fault_df.empty:
        for _, contact_row in contact_fault_df.iterrows():
            attr = ast.literal_eval(contact_row["ATTRIBUTES"])
            contact_operand = attr.get("operand")
            negated_operand = attr.get("negated")
            rising_edge = attr.get("edge")

            if contact_operand and isinstance(contact_operand, str):
                contact_comment = get_the_comment_from_program(
                    contact_operand, program_name, program_comment_data
                )

                if isinstance(contact_comment, list) and contact_comment:
                    if (
                        regex_pattern_check("PLC", contact_comment)
                        and regex_pattern_check(battery_comment, contact_comment)
                        and regex_pattern_check(replace_comment, contact_comment)
                        and negated_operand == "false"
                        and not rising_edge == "rising"
                        and not rising_edge == "falling"
                    ):
                        return {
                            "cc": "cc3",
                            "status": "OK",
                            "check_number": 3,
                            "target_coil": "",
                            "rung_number": cc_detection_3_rung_number,
                            "PLC_battery_replace": contact_operand,
                        }

    return {
        "cc": "cc3",
        "status": "NG",
        "check_number": 3,
        "target_coil": "",
        "rung_number": -1,
        "PLC_battery_replace": -1,
    }


def check_detail_4_programwise(
    fault_section_df: pd.DataFrame,
    cc_detection_3_rung_number: int,
    servo_comment: str,
    battery_comment: str,
    replace_comment: str,
    program_name: str,
    program_comment_data: dict,
) -> dict:

    logger.info(f"Executing rule no 56 check detail 4")

    contact_fault_df = fault_section_df[
        (fault_section_df["OBJECT_TYPE_LIST"] == "Contact")
        & (fault_section_df["RUNG"] == cc_detection_3_rung_number)
    ].copy()

    if cc_detection_3_rung_number >= 0 and not contact_fault_df.empty:
        for _, contact_row in contact_fault_df.iterrows():
            attr = ast.literal_eval(contact_row["ATTRIBUTES"])
            contact_operand = attr.get("operand")
            negated_operand = attr.get("negated")
            rising_edge = attr.get("edge")

            if contact_operand and isinstance(contact_operand, str):
                contact_comment = get_the_comment_from_program(
                    contact_operand, program_name, program_comment_data
                )

                if isinstance(contact_comment, list) and contact_comment:
                    if (
                        regex_pattern_check(servo_comment, contact_comment)
                        and regex_pattern_check(battery_comment, contact_comment)
                        and regex_pattern_check(replace_comment, contact_comment)
                        and negated_operand == "false"
                        and not rising_edge == "rising"
                        and not rising_edge == "falling"
                    ):
                        return {
                            "cc": "cc4",
                            "status": "OK",
                            "check_number": 4,
                            "target_coil": "",
                            "rung_number": cc_detection_3_rung_number,
                            "servo_battery_replace_operand": contact_operand,
                        }

    return {
        "cc": "cc4",
        "status": "NG",
        "check_number": 4,
        "target_coil": "",
        "rung_number": -1,
        "servo_battery_replace_operand": "",
    }


def check_detail_5_programwise(
    fault_section_df: pd.DataFrame,
    cc_detection_3_rung_number: int,
    cc2_result: dict,
    cc3_result: dict,
    cc4_result: dict,
    program_name: str,
    program_comment_data: dict,
) -> dict:

    logger.info(f"Executing rule no 56 check detail 5")

    if (
        cc2_result["status"] == "OK"
        and cc3_result["status"] == "OK"
        and cc4_result["status"] == "OK"
        and not fault_section_df.empty
        and cc_detection_3_rung_number >= 0
    ):

        try:
            rung_df = fault_section_df[
                fault_section_df["RUNG"] == cc_detection_3_rung_number
            ]
            parallel_contacts = get_parallel_contacts(pl.from_pandas(rung_df))
            formatted_parallel_contacts = get_format_parellel_contact_detail(
                parallel_contacts
            )
        except:
            formatted_parallel_contacts = {}

        # Extract operands
        cc2_op1 = cc2_result["not_abnormal_operand"]
        cc2_op2 = cc2_result["buzzer_abnormal_operand"]
        cc3_op = cc3_result["PLC_battery_replace"]
        cc4_op = cc4_result["servo_battery_replace_operand"]

        cc2_valid = False
        cc3_valid = False

        if formatted_parallel_contacts:
            # Primary check under cc4_op
            if cc4_op in formatted_parallel_contacts:
                for chain in formatted_parallel_contacts[cc4_op]:
                    if cc2_op1 in chain and cc2_op2 in chain:
                        cc2_valid = True
                    elif cc3_op in chain:
                        cc3_valid = True

                if cc2_valid and cc3_valid:
                    return {
                        "cc": "cc5",
                        "status": "OK",
                        "check_number": 5,
                        "target_coil": "",
                        "rung_number": cc_detection_3_rung_number,
                    }

        cc2_valid = False
        cc4_valid = False

        if formatted_parallel_contacts:
            # Primary check under cc4_op
            if cc3_op in formatted_parallel_contacts:
                for chain in formatted_parallel_contacts[cc3_op]:
                    if cc2_op1 in chain and cc2_op2 in chain:
                        cc2_valid = True
                    if cc4_op in chain:
                        cc4_valid = True

                if cc2_valid and cc4_valid:
                    return {
                        "cc": "cc5",
                        "status": "OK",
                        "check_number": 5,
                        "target_coil": "",
                        "rung_number": cc_detection_3_rung_number,
                    }

    # Fallback if any validation fails
    return {
        "cc": "cc5",
        "status": "NG",
        "check_number": 5,
        "target_coil": "",
        "rung_number": -1,
    }


def check_detail_6_programwise(
    fault_section_df: pd.DataFrame,
    cc_detection_4_coil_operand: str,
    cc_detection_4_rung_number: int,
    not_comment: str,
    warning_comment: str,
    buzzer_full_width_comment: str,
    program_name: str,
    program_comment_data: dict,
) -> dict:

    logger.info(f"Executing rule no 56 check detail 6")

    not_warning_found = False
    buzzer_warning_found = False
    not_warning_operand = ""
    buzzer_warning_operand = ""

    contact_fault_df = fault_section_df[
        (fault_section_df["OBJECT_TYPE_LIST"] == "Contact")
        & (fault_section_df["RUNG"] == cc_detection_4_rung_number)
    ].copy()

    if (
        cc_detection_4_rung_number >= 0
        and cc_detection_4_coil_operand
        and not contact_fault_df.empty
    ):
        for _, contact_row in contact_fault_df.iterrows():
            attr = ast.literal_eval(contact_row["ATTRIBUTES"])
            contact_operand = attr.get("operand")
            negated_operand = attr.get("negated")

            if contact_operand and isinstance(contact_operand, str):
                contact_comment = get_the_comment_from_program(
                    contact_operand, program_name, program_comment_data
                )

                if isinstance(contact_comment, list) and contact_comment:
                    if (
                        regex_pattern_check(not_comment, contact_comment)
                        and regex_pattern_check(warning_comment, contact_comment)
                        and not not_warning_found
                        and negated_operand == "true"
                    ):
                        not_warning_found = True
                        not_warning_operand = contact_operand

                if isinstance(contact_comment, list) and contact_comment:
                    if (
                        regex_pattern_check(buzzer_full_width_comment, contact_comment)
                        and regex_pattern_check(warning_comment, contact_comment)
                        and not buzzer_warning_found
                        and negated_operand == "true"
                    ):
                        buzzer_warning_found = True
                        buzzer_warning_operand = contact_operand

        if not_warning_operand and buzzer_warning_operand:

            cc_detection_4_rung_number_details = fault_section_df[
                fault_section_df["RUNG"] == cc_detection_4_rung_number
            ]
            cc_detection_4_rung_number_details_polar = pl.from_pandas(
                cc_detection_4_rung_number_details
            )
            cc2_detection_3_get_series_data = get_series_contacts_coil(
                cc_detection_4_rung_number_details_polar
            )

            operand_list_per_rung = [
                [step["operand"] for step in rung]
                for rung in cc2_detection_3_get_series_data
            ]
            if operand_list_per_rung:
                for rung_operands in operand_list_per_rung:
                    if (
                        not_warning_operand in rung_operands
                        and buzzer_warning_operand in rung_operands
                    ):
                        return {
                            "cc": "cc6",
                            "status": "OK",
                            "check_number": 6,
                            "target_coil": "",
                            "rung_number": cc_detection_4_rung_number,
                            "not_warning_operand": not_warning_operand,
                            "buzzer_warning_operand": buzzer_warning_operand,
                        }

    return {
        "cc": "cc6",
        "status": "NG",
        "check_number": 6,
        "target_coil": "",
        "rung_number": -1,
        "not_warning_operand": "",
        "buzzer_warning_operand": "",
    }


def check_detail_7_programwise(
    fault_section_df: pd.DataFrame,
    cc_detection_5_rung_number: int,
    cc_detection_3_coil_operand: str,
    cc_detection_4_coil_operand: str,
    autorun_comment: str,
    program_name: str,
    program_comment_data: dict,
) -> dict:

    logger.info(f"Executing rule no 56 check detail 7")

    if (
        cc_detection_3_coil_operand
        and cc_detection_4_coil_operand
        and not fault_section_df.empty
    ):

        autorun_comment_exist = False
        cc7_detection_3_operand_exist = False
        cc7_detection_4_operand_exist = False
        autorun_operand_details = []
        cc7_detection_3_operand_details = []
        cc7_detection_4_operand_details = []

        detection_5_rung_fault_section_df = fault_section_df[
            (fault_section_df["OBJECT_TYPE_LIST"] == "Contact")
            & (fault_section_df["RUNG"] == cc_detection_5_rung_number)
        ]

        if not detection_5_rung_fault_section_df.empty:
            for _, fault_section_row in detection_5_rung_fault_section_df.iterrows():

                attr = ast.literal_eval(fault_section_row["ATTRIBUTES"])
                contact_operand = attr.get("operand")
                negated_operand = attr.get("negated")

                if isinstance(contact_operand, str) and contact_operand:
                    contact_comment = get_the_comment_from_program(
                        contact_operand, program_name, program_comment_data
                    )

                    if isinstance(contact_comment, list) and contact_comment:
                        if (
                            regex_pattern_check(autorun_comment, contact_comment)
                            and not autorun_comment_exist
                            and negated_operand == "false"
                        ):
                            autorun_comment_exist = True
                            autorun_operand_details.append(attr)

                    if (
                        contact_operand == cc_detection_3_coil_operand
                        and not cc7_detection_3_operand_exist
                        and negated_operand == "true"
                    ):
                        cc7_detection_3_operand_exist = True
                        cc7_detection_3_operand_details.append(attr)

                    if (
                        contact_operand == cc_detection_4_coil_operand
                        and not cc7_detection_4_operand_exist
                        and negated_operand == "true"
                    ):
                        cc7_detection_4_operand_exist = True
                        cc7_detection_4_operand_details.append(attr)

                    if (
                        cc7_detection_3_operand_exist
                        and cc7_detection_4_operand_exist
                        and autorun_comment_exist
                    ):

                        """
                        get the first contact to used for finding parellel contact as given in rules
                        """
                        try:
                            first_contact_from_series = get_first_contact_from_series(
                                autorun_operand_details,
                                cc7_detection_3_operand_details,
                                cc7_detection_4_operand_details,
                            )
                        except:
                            first_contact_from_series = ""

                        return {
                            "cc": "cc7",
                            "status": "OK",
                            "check_number": 7,
                            "target_coil": "",
                            "rung_number": fault_section_row["RUNG"],
                            "first_contact_from_series": first_contact_from_series,
                        }

    return {
        "cc": "cc7",
        "status": "NG",
        "check_number": 7,
        "target_coil": "",
        "rung_number": -1,
        "first_contact_from_series": "",
    }


def check_detail_8_programwise(
    fault_section_df: pd.DataFrame,
    cc_detection_5_rung_number: int,
    master_on_comment: str,
    complete_comment: str,
    program_name: str,
    program_comment_data: dict,
) -> dict:

    logger.info(f"Executing rule no 56 check detail 8")

    if cc_detection_5_rung_number != -1 and not fault_section_df.empty:

        master_on_operand_exist = False
        ap_operand_exist = False
        master_on_operand_details = []
        ap_operand_details = []

        detection_5_rung_fault_section_df = fault_section_df[
            (fault_section_df["OBJECT_TYPE_LIST"] == "Contact")
            & (fault_section_df["RUNG"] == cc_detection_5_rung_number)
        ]

        if not detection_5_rung_fault_section_df.empty:
            for _, fault_section_row in detection_5_rung_fault_section_df.iterrows():

                attr = ast.literal_eval(fault_section_row["ATTRIBUTES"])
                contact_operand = attr.get("operand")
                negated_operand = attr.get("negated")

                if isinstance(contact_operand, str) and contact_operand:
                    contact_comment = get_the_comment_from_program(
                        contact_operand, program_name, program_comment_data
                    )

                    if isinstance(contact_comment, list) and contact_comment:
                        if (
                            regex_pattern_check(master_on_comment, contact_comment)
                            and regex_pattern_check(complete_comment, contact_comment)
                            and not master_on_operand_exist
                            and negated_operand == "false"
                        ):
                            master_on_operand_exist = True
                            master_on_operand_details.append(attr)

                        if (
                            "ap" in contact_operand.lower()
                            and not ap_operand_exist
                            and negated_operand == "false"
                        ):
                            ap_operand_exist = True
                            ap_operand_details.append(attr)

                    if master_on_operand_exist and ap_operand_exist:

                        """
                        get the first contact to used for finding parellel contact as given in rules
                        """

                        try:
                            first_contact_from_series = get_first_contact_from_series(
                                master_on_operand_details, ap_operand_details
                            )
                        except:
                            first_contact_from_series = ""

                        return {
                            "cc": "cc8",
                            "status": "OK",
                            "check_number": 8,
                            "target_coil": "",
                            "rung_number": fault_section_row["RUNG"],
                            "first_contact_from_series": first_contact_from_series,
                        }

    return {
        "cc": "cc8",
        "status": "NG",
        "check_number": 8,
        "target_coil": "",
        "rung_number": -1,
        "first_contact_from_series": "",
    }


def check_detail_9_programwise(
    fault_section_df: pd.DataFrame,
    cc_detection_5_rung_number: int,
    cc7_result: str,
    cc8_result: str,
    program_name: str,
    program_comment_data: dict,
) -> dict:

    logger.info(f"Executing rule no 56 check detail 9")

    if cc7_result["status"] == "OK" and cc8_result["status"] == "OK":

        cc7_first_operand = cc7_result["first_contact_from_series"]
        cc8_first_operand = cc8_result["first_contact_from_series"]
        cc7_in_list = []
        cc8_in_list = []

        cc9_current_fault_df = fault_section_df[
            (fault_section_df["RUNG"] == cc_detection_5_rung_number)
        ]

        if cc7_first_operand and cc8_first_operand and not cc9_current_fault_df.empty:
            print("in if")

            for _, current_row in cc9_current_fault_df.iterrows():

                attr = ast.literal_eval(current_row["ATTRIBUTES"])
                contact_operand = attr.get("operand")

                if (
                    contact_operand
                    and isinstance(contact_operand, str)
                    and contact_operand == cc7_first_operand
                ):
                    cc7_in_list.extend(attr["in_list"])

                if (
                    contact_operand
                    and isinstance(contact_operand, str)
                    and contact_operand == cc8_first_operand
                ):
                    cc8_in_list.extend(attr["in_list"])

            for _, current_row in cc9_current_fault_df.iterrows():
                attr = ast.literal_eval(current_row["ATTRIBUTES"])
                contact_operand = attr.get("operand")

                if (
                    contact_operand
                    and isinstance(contact_operand, str)
                    and contact_operand in [cc7_first_operand, cc8_first_operand]
                ):
                    return {
                        "cc": "cc5",
                        "status": "NG",
                        "check_number": 5,
                        "target_coil": "",
                        "rung_number": -1,
                    }

                curr_out_list = attr.get("out_list")

                if curr_out_list:
                    outlist_in_all_in_list = False
                    for curr_out_val in set(curr_out_list):
                        if curr_out_val in cc7_in_list:
                            outlist_in_all_in_list = True

                        if curr_out_val in cc8_in_list:
                            outlist_in_all_in_list = True

                        if outlist_in_all_in_list is False:
                            break

                    if outlist_in_all_in_list:
                        return {
                            "cc": "cc9",
                            "status": "OK",
                            "check_number": 9,
                            "target_coil": "",
                            "rung_number": cc_detection_5_rung_number,
                        }

    return {
        "cc": "cc9",
        "status": "NG",
        "check_number": 9,
        "target_coil": "",
        "rung_number": -1,
    }


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_56_programwise(
    input_program_file: str, input_program_comment_file: str
) -> pd.DataFrame:

    logger.info("Starting execution of Rule 56")

    try:
        program_df = pd.read_csv(input_program_file)
        with open(input_program_comment_file, "r", encoding="utf-8") as file:
            program_comment_data = json.load(file)

        unique_program_values = program_df["PROGRAM"].unique()
        output_rows = []

        for program_name in unique_program_values:
            logger.info(f"Executing rule 51 in Program {program_name}")
            if "main" in program_name.lower():

                current_program_df = program_df[program_df["PROGRAM"] == program_name]

                fault_section_df = current_program_df[
                    current_program_df["BODY"].str.lower() == fault_section_name
                ]

                if not fault_section_df.empty:
                    # Run detection range logic as per Rule 24
                    detection_results = detection_range_programwise(
                        fault_section_df=fault_section_df,
                        st_r_comment=st_r_comment,
                        st_y_comment=st_y_comment,
                        st_g_comment=st_g_comment,
                        program_name=program_name,
                        program_comment_data=program_comment_data,
                    )

                    print("detection details", detection_results)

                    cc1_result = check_detail_1_programwise(
                        detection_results=detection_results
                    )
                    cc_detection_3_coil_operand = detection_results[
                        "detection_3_details"
                    ][0]
                    cc_detection_3_rung_number = detection_results[
                        "detection_3_details"
                    ][1]
                    cc_detection_4_coil_operand = detection_results[
                        "detection_4_details"
                    ][0]
                    cc_detection_4_rung_number = detection_results[
                        "detection_4_details"
                    ][1]
                    cc_detection_5_rung_number = detection_results[
                        "detection_5_details"
                    ][1]
                    # cc_detection_5_coil_operand = detection_results['detection_5_details'][0]
                    # cc_detection_7_coil_operand = detection_results['detection_7_details'][0]

                    cc2_result = check_detail_2_programwise(
                        fault_section_df=fault_section_df,
                        cc_detection_3_coil_operand=cc_detection_3_coil_operand,
                        cc_detection_3_rung_number=cc_detection_3_rung_number,
                        not_comment=not_comment,
                        abnormal_comment=abnormal_comment,
                        buzzer_full_width_comment=buzzer_full_width_comment,
                        program_name=program_name,
                        program_comment_data=program_comment_data,
                    )

                    cc3_result = check_detail_3_programwise(
                        fault_section_df=fault_section_df,
                        cc_detection_3_coil_operand=cc_detection_3_coil_operand,
                        cc_detection_3_rung_number=cc_detection_3_rung_number,
                        battery_comment=battery_comment,
                        replace_comment=replace_comment,
                        program_name=program_name,
                        program_comment_data=program_comment_data,
                    )

                    cc4_result = check_detail_4_programwise(
                        fault_section_df=fault_section_df,
                        cc_detection_3_rung_number=cc_detection_3_rung_number,
                        servo_comment=servo_comment,
                        battery_comment=battery_comment,
                        replace_comment=replace_comment,
                        program_name=program_name,
                        program_comment_data=program_comment_data,
                    )

                    cc5_result = check_detail_5_programwise(
                        fault_section_df=fault_section_df,
                        cc_detection_3_rung_number=cc_detection_3_rung_number,
                        cc2_result=cc2_result,
                        cc3_result=cc3_result,
                        cc4_result=cc4_result,
                        program_name=program_name,
                        program_comment_data=program_comment_data,
                    )

                    cc6_result = check_detail_6_programwise(
                        fault_section_df=fault_section_df,
                        cc_detection_4_coil_operand=cc_detection_4_coil_operand,
                        cc_detection_4_rung_number=cc_detection_4_rung_number,
                        not_comment=not_comment,
                        warning_comment=warning_comment,
                        buzzer_full_width_comment=buzzer_full_width_comment,
                        program_name=program_name,
                        program_comment_data=program_comment_data,
                    )

                    cc7_result = check_detail_7_programwise(
                        fault_section_df=fault_section_df,
                        cc_detection_5_rung_number=cc_detection_5_rung_number,
                        cc_detection_3_coil_operand=cc_detection_3_coil_operand,
                        cc_detection_4_coil_operand=cc_detection_4_coil_operand,
                        autorun_comment=autorun_comment,
                        program_name=program_name,
                        program_comment_data=program_comment_data,
                    )

                    cc8_result = check_detail_8_programwise(
                        fault_section_df=fault_section_df,
                        cc_detection_5_rung_number=cc_detection_5_rung_number,
                        master_on_comment=master_on_comment,
                        complete_comment=complete_comment,
                        program_name=program_name,
                        program_comment_data=program_comment_data,
                    )

                    cc9_result = check_detail_9_programwise(
                        fault_section_df=fault_section_df,
                        cc_detection_5_rung_number=cc_detection_5_rung_number,
                        cc7_result=cc7_result,
                        cc8_result=cc8_result,
                        program_name=program_name,
                        program_comment_data=program_comment_data,
                    )

                    # print("cc1_result",cc1_result)
                    # print("cc2_result",cc2_result)
                    # print("cc3_result",cc3_result)
                    # print("cc4_result",cc4_result)
                    # print("cc5_result",cc5_result)
                    # print("cc6_result",cc6_result)
                    # print("cc7_result",cc7_result)
                    # print("cc8_result",cc8_result)
                    # print("cc9_result",cc9_result)
                    all_cc_result = [
                        cc1_result,
                        cc2_result,
                        cc3_result,
                        cc4_result,
                        cc5_result,
                        cc6_result,
                        cc7_result,
                        cc8_result,
                        cc9_result,
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
                            cc_result.get("target_coil")
                            if cc_result.get("target_coil")
                            else ""
                        )
                        check_number = cc_result.get("check_number", "")

                        output_rows.append(
                            {
                                "Result": cc_result.get("status"),
                                "Task": program_name,
                                "Section": fault_section_name,
                                "RungNo": rung_number,
                                "Target": target_outcoil,
                                "CheckItem": rule_56_check_item,
                                "Detail": ng_name,
                                "Status": "",
                            }
                        )

                        # output_rows.append({
                        #     "TASK_NAME": program_name,
                        #     "SECTION_NAME": fault_section_name,
                        #     "RULE_NUMBER": "56",
                        #     "CHECK_NUMBER": check_number,
                        #     "RUNG_NUMBER": -1 if rung_number < 0 else rung_number-1,
                        #     "RULE_CONTENT": rule_content_56,
                        #     "CHECK_CONTENT": check_detail_content.get(cc_result.get('cc')),
                        #     "STATUS": cc_result.get('status'),
                        #     "Target_outcoil" : target_outcoil,
                        #     "NG_EXPLANATION": ng_name
                        # })
            # else:
            #     output_rows.append({
            #         "TASK_NAME": program_name,
            #         "SECTION_NAME": fault_section_name,
            #         "RULE_NUMBER": "55",
            #         "CHECK_NUMBER": 1,
            #         "RUNG_NUMBER": -1,
            #         "RULE_CONTENT": rule_content_56,
            #         "CHECK_CONTENT": "",
            #         "STATUS": "NG",
            #         "Target_outcoil" : "",
            #         "NG_EXPLANATION": ""
            #     })

        final_output_df = pd.DataFrame(output_rows)
        if not final_output_df.empty:
            if "RungNo" in final_output_df.columns:
                final_output_df["RungNo"] = final_output_df["RungNo"].apply(
                    clean_rung_number
                )
        else:
            final_output_df = pd.DataFrame(
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

        return {"status": "OK", "output_df": final_output_df}

    except Exception as e:
        logger.error(f"Rule 47 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}
