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
from .ladder_utils import regex_pattern_check, clean_rung_number
from .rule_51_ladder_utils import (
    get_series_contacts_coil,
    check_self_holding,
    get_format_parellel_contact_detail,
    get_parallel_contacts,
)

# ============================================ Comments referenced in Rule 51 processing ============================================
BZ_variable_comment = "BZ"
buzzer_full_width_comment = "ブザー"
not_comment = "でない"
abnormal_comment = "異常"
warning_comment = "警報"
battery_comment = "ﾊﾞｯﾃﾘ"
replace_comment = "交換"
servo_comment = "ｻｰﾎﾞ"


# ============================ Rule 51: Definitions, Content, and Configuration Details ============================
rule_content_51 = "Rule of Buzzer ＆ Mute the Buzzer Circuit"
rule_51_check_item = "Rule of Buzzer ＆ Mute the Buzzer Circuit"

check_detail_content = {
    "cc1": " If ① but not ③ or ④ or ⑤ or ⑦, it is assumed to be NG.",
    "cc2": " Check that the B contact that contains the variable comment ”異常でない(not abnormal)” and the B contact of the variable detected in ④ exist in series (AND) in the out-coil condition detected in ③.",
    "cc3": " Check that the B contact that contains the variable comment ”警報でない(not warning)” and the B contact of the variable detected in ⑤ exist in series (AND) in the out-coil condition detected in ③.",
    "cc4": "Check that the A contact that contains the variable comment 'PLC'+'ﾊﾞｯﾃﾘ(battery)'+'交換(replace)'  in the out-coil condition detected in ③.",
    "cc5": "Check that the A contact that contains the variable comment 'ｻｰﾎﾞ(servo)'+'ﾊﾞｯﾃﾘ(battery)'+'交換(replace)'  in the out-coil condition detected in ③.",
    "cc6": "Check that the circuit checked in ❷to❺ exists in parallel (OR) with the out-coil condition detected in ③.",
    "cc7": "Check that the out-coil condition detected in ⑦ is only the A contact of the variable detected in ③. *If there are other contacts, NG.",
    "cc8": "Check that the outcoil condition detected by ④ satisfies all of the following.",
    "cc9": "Check that the outcoil condition detected by ⑤ satisfies all of the following. *Similar sections to ❽ are omitted.",
}

ng_content = {
    "cc1": "Fault回路が標準通りに作られていない(異常回路の抜け・漏れ)可能性あり(Fault circuit may not be built to standard (abnormal circuit missing or leaking))",
    "cc2": "異常時のブザーの条件が標準通りでない(Condition of buzzer in case of abnormality is not as standard.)",
    "cc3": "警報時のブザーの条件が標準通りでない(Condition of buzzer in case of warning is not as standard.)",
    "cc4": "ブザー条件にPLCﾊﾞｯﾃﾘ交換信号が含まれていないのでNG(NG because the buzzer condition does not include the PLC battery replacement signal.)",
    "cc5": "ブザー条件にｻｰﾎﾞﾊﾞｯﾃﾘ交換信号が含まれていないのでNG(NG because the buzzer condition does not include the servo battery replacement signal.)",
    "cc6": "ブザーの条件が標準通りでない(Condition of buzzer is not as standard.)",
    "cc7": "ブザーの出力信号の条件が標準通りでないためNG(NG due to non-standard buzzer output signal condition.)",
    "cc8": "ブザー消音(異常)信号の条件が標準通りでないためNG(NG due to non-standard conditions for buzzer mute (abnormal) signal.)",
    "cc9": "ブザー消音(警報)信号の条件が標準通りでないためNG(NG due to non-standard conditions for buzzer mute (warning) signal.)",
}

fault_section_name = "fault"
deviceout_section_name = "deviceout"


# ============================ Helper Functions for both Program-Wise and Function-Wise Operations ============================
def get_first_contact_from_series(first_contact_detail, second_contact_detail):
    first_contact_inlist = first_contact_detail[0].get("in_list")
    first_contact_outlist = first_contact_detail[0].get("out_list")
    second_contact_inlist = second_contact_detail[0].get("in_list")
    second_contact_outlist = second_contact_detail[0].get("out_list")

    for f_out in first_contact_outlist:
        if f_out in second_contact_inlist:
            return first_contact_detail[0].get("operand")

    for s_out in second_contact_outlist:
        if s_out in first_contact_inlist:
            return second_contact_detail[0].get("operand")

    return first_contact_detail[0].get("operand")


# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================


def detection_range_programwise(
    fault_section_df: pd.DataFrame,
    deviceout_section_df: pd.DataFrame,
    BZ_variable_comment: str,
    buzzer_full_width_comment: str,
    abnormal_comment: str,
    warning_comment: str,
    program_name: str,
    program_comment_data: dict,
) -> dict:

    logger.info(f"Executing rule 51 detection range on program {program_name}")

    fault_coil_df = fault_section_df[fault_section_df["OBJECT_TYPE_LIST"] == "Coil"]
    deviceout_coil_df = deviceout_section_df[
        deviceout_section_df["OBJECT_TYPE_LIST"] == "Coil"
    ]

    detection_3_coil = detection_4_coil = detection_5_coil = detection_7_coil = ""
    detection_3_rung_number = detection_4_rung_number = detection_5_rung_number = (
        detection_7_rung_number
    ) = -1
    fault_check_BZ_twice = False
    fault_check_buzzer_abnormal_twice = False
    fault_check_buzzer_warning_twice = False
    check_buzzer_twice_deviceout = False

    if not fault_coil_df.empty:
        for _, fault_coil_row in fault_coil_df.iterrows():
            attr = ast.literal_eval(fault_coil_row["ATTRIBUTES"])
            fault_coil_operand = attr.get("operand")

            if fault_coil_operand and isinstance(fault_coil_operand, str):
                fault_coil_comment = get_the_comment_from_program(
                    fault_coil_operand, program_name, program_comment_data
                )

                if (
                    BZ_variable_comment in fault_coil_operand
                    and not fault_check_BZ_twice
                ):
                    fault_check_BZ_twice = True
                    detection_3_coil = fault_coil_operand
                    detection_3_rung_number = fault_coil_row["RUNG"]

            if (
                isinstance(fault_coil_comment, list)
                and fault_coil_comment
                and not fault_check_buzzer_abnormal_twice
            ):
                if regex_pattern_check(
                    buzzer_full_width_comment, fault_coil_comment
                ) and regex_pattern_check(abnormal_comment, fault_coil_comment):
                    fault_check_buzzer_abnormal_twice = True
                    detection_4_coil = fault_coil_operand
                    detection_4_rung_number = fault_coil_row["RUNG"]

            if (
                isinstance(fault_coil_comment, list)
                and fault_coil_comment
                and not fault_check_buzzer_warning_twice
            ):
                if regex_pattern_check(
                    buzzer_full_width_comment, fault_coil_comment
                ) and regex_pattern_check(warning_comment, fault_coil_comment):
                    fault_check_buzzer_warning_twice = True
                    detection_5_coil = fault_coil_operand
                    detection_5_rung_number = fault_coil_row["RUNG"]

    if not deviceout_coil_df.empty:
        for _, deviceout_coil_row in deviceout_coil_df.iterrows():
            attr = ast.literal_eval(deviceout_coil_row["ATTRIBUTES"])
            deviceout_coil_operand = attr.get("operand")
            if isinstance(deviceout_coil_operand, str) and deviceout_coil_operand:
                deviceout_coil_comment = get_the_comment_from_program(
                    deviceout_coil_operand, program_name, program_comment_data
                )

                if isinstance(deviceout_coil_comment, list) and deviceout_coil_comment:
                    if (
                        regex_pattern_check(BZ_variable_comment, deviceout_coil_comment)
                        or regex_pattern_check(
                            buzzer_full_width_comment, fault_coil_comment
                        )
                    ) and not check_buzzer_twice_deviceout:
                        check_buzzer_twice_deviceout = True
                        detection_7_coil = deviceout_coil_operand
                        detection_7_rung_number = deviceout_coil_row["RUNG"]
                        break

    return {
        "detection_3_details": [detection_3_coil, detection_3_rung_number],
        "detection_4_details": [detection_4_coil, detection_4_rung_number],
        "detection_5_details": [detection_5_coil, detection_5_rung_number],
        "detection_7_details": [detection_7_coil, detection_7_rung_number],
    }


def check_detail_1_programwise(detection_results: dict) -> dict:

    logger.info(f"Executing rule 51 and check detail 1")

    cc1_result = {}

    status = "OK"
    if (
        detection_results["detection_3_details"][1] == -1
        or detection_results["detection_4_details"][1] == -1
        or detection_results["detection_5_details"][1] == -1
        or detection_results["detection_7_details"][1] == -1
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
    cc_detection_3_rung_number: int,
    cc_detection_4_coil_operand: str,
    cc_detection_5_coil_operand: str,
    not_comment: str,
    abnormal_comment: str,
    warning_comment: str,
    program_name: str,
    program_comment_data: dict,
) -> dict:

    logger.info(f"Executing rule no 51 check detail 2")

    not_abnormal_found = False
    not_warning_found = False
    not_abnormal_operand = ""
    not_warning_operand = ""
    cc2_detection_4_coil_operand_exist = False

    contact_fault_df = fault_section_df[
        (fault_section_df["OBJECT_TYPE_LIST"] == "Contact")
        & (fault_section_df["RUNG"] == cc_detection_3_rung_number)
    ].copy()

    if (
        cc_detection_3_rung_number >= 0
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
                        and regex_pattern_check(abnormal_comment, contact_comment)
                        and not not_abnormal_found
                        and negated_operand == "true"
                    ):
                        not_abnormal_found = True
                        not_abnormal_operand = contact_operand

                """
                Using this warning to check if there warning comment contact should not be there in series with both not_abnormal and detection_4 operand
                """
                if isinstance(contact_comment, list) and contact_comment:
                    if (
                        regex_pattern_check(not_comment, contact_comment)
                        and regex_pattern_check(warning_comment, contact_comment)
                        and not not_warning_found
                    ):
                        not_warning_found = True
                        not_warning_operand = contact_operand

                if (
                    contact_operand == cc_detection_4_coil_operand
                    and not cc2_detection_4_coil_operand_exist
                    and negated_operand == "true"
                ):
                    cc2_detection_4_coil_operand_exist = True

        if not_abnormal_operand and cc2_detection_4_coil_operand_exist:

            """
            Getting series data for current rung
            also check for both operand not_abnormal and detection_4 should be in series and no warning comment should be there
            """
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
                        and cc_detection_4_coil_operand in rung_operands
                        and not (
                            not_warning_operand in rung_operands
                            or cc_detection_5_coil_operand in rung_operands
                        )
                    ):

                        return {
                            "cc": "cc2",
                            "status": "OK",
                            "check_number": 2,
                            "target_coil": "",
                            "rung_number": cc_detection_3_rung_number,
                            "not_abnormal_operand": not_abnormal_operand,
                            "detection_4_coil": cc_detection_4_coil_operand,
                        }

    return {
        "cc": "cc2",
        "status": "NG",
        "check_number": 2,
        "target_coil": "",
        "rung_number": -1,
        "not_abnormal_operand": not_abnormal_operand,
        "detection_4_coil": cc_detection_4_coil_operand,
    }


def check_detail_3_programwise(
    fault_section_df: pd.DataFrame,
    cc_detection_3_rung_number: int,
    cc_detection_4_coil_operand: str,
    cc_detection_5_coil_operand: str,
    not_comment: str,
    abnormal_comment: str,
    warning_comment: str,
    program_name: str,
    program_comment_data: dict,
) -> dict:

    logger.info(f"Executing rule no 51 check detail 3")

    not_warning_found = False
    not_abnormal_found = False
    not_abnormal_operand = ""
    not_warning_operand = ""
    cc3_detection_5_coil_operand_exist = False

    contact_fault_df = fault_section_df[
        (fault_section_df["OBJECT_TYPE_LIST"] == "Contact")
        & (fault_section_df["RUNG"] == cc_detection_3_rung_number)
    ].copy()

    if (
        cc_detection_3_rung_number >= 0
        and cc_detection_5_coil_operand
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

                """
                Using this not_abnormal to check if there abnormal comment contact should not be there in series with both not_warning and detection_5_operand
                """
                if isinstance(contact_comment, list) and contact_comment:
                    if (
                        regex_pattern_check(not_comment, contact_comment)
                        and regex_pattern_check(abnormal_comment, contact_comment)
                        and not not_abnormal_found
                    ):
                        not_abnormal_found = True
                        not_abnormal_operand = contact_operand

            if (
                contact_operand == cc_detection_5_coil_operand
                and not cc3_detection_5_coil_operand_exist
                and negated_operand == "true"
            ):
                cc3_detection_5_coil_operand_exist = True

        if not_warning_operand and cc3_detection_5_coil_operand_exist:

            """
            Getting series data for current rung
            also check for both operand not_warning and detection_5_operand should be in series and no not_abnormal comment should be there
            """

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
                        not_warning_operand in rung_operands
                        and cc_detection_5_coil_operand in rung_operands
                        and not (
                            not_abnormal_operand in rung_operands
                            or cc_detection_4_coil_operand in rung_operands
                        )
                    ):

                        return {
                            "cc": "cc3",
                            "status": "OK",
                            "check_number": 3,
                            "target_coil": "",
                            "rung_number": cc_detection_3_rung_number,
                            "not_warning_operand": not_warning_operand,
                            "detection_5_coil": cc_detection_5_coil_operand,
                        }

    return {
        "cc": "cc3",
        "status": "NG",
        "check_number": 3,
        "target_coil": "",
        "rung_number": -1,
        "not_warning_operand": not_warning_operand,
        "detection_5_coil": cc_detection_5_coil_operand,
    }


def check_detail_4_programwise(
    fault_section_df: pd.DataFrame,
    cc_detection_3_rung_number: int,
    battery_comment: str,
    replace_comment: str,
    program_name: str,
    program_comment_data: dict,
) -> dict:

    logger.info(f"Executing rule no 51 check detail 4")

    contact_fault_df = fault_section_df[
        (fault_section_df["OBJECT_TYPE_LIST"] == "Contact")
        & (fault_section_df["RUNG"] == cc_detection_3_rung_number)
    ].copy()

    if cc_detection_3_rung_number >= 0 and not contact_fault_df.empty:
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
                        regex_pattern_check("PLC", contact_comment)
                        and regex_pattern_check(battery_comment, contact_comment)
                        and regex_pattern_check(replace_comment, contact_comment)
                        and negated_operand == "false"
                    ):
                        return {
                            "cc": "cc4",
                            "status": "OK",
                            "check_number": 4,
                            "target_coil": "",
                            "rung_number": cc_detection_3_rung_number,
                            "PLC_battery_operand": contact_operand,
                        }

    return {
        "cc": "cc4",
        "status": "NG",
        "check_number": 4,
        "target_coil": "",
        "rung_number": -1,
        "PLC_battery_operand": "",
    }


def check_detail_5_programwise(
    fault_section_df: pd.DataFrame,
    cc_detection_3_rung_number: int,
    servo_comment: str,
    battery_comment: str,
    replace_comment: str,
    program_name: str,
    program_comment_data: dict,
) -> dict:

    logger.info(f"Executing rule no 51 check detail 5")

    contact_fault_df = fault_section_df[
        (fault_section_df["OBJECT_TYPE_LIST"] == "Contact")
        & (fault_section_df["RUNG"] == cc_detection_3_rung_number)
    ].copy()

    if cc_detection_3_rung_number >= 0 and not contact_fault_df.empty:
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
                        regex_pattern_check(servo_comment, contact_comment)
                        and regex_pattern_check(battery_comment, contact_comment)
                        and regex_pattern_check(replace_comment, contact_comment)
                        and negated_operand == "false"
                    ):
                        return {
                            "cc": "cc5",
                            "status": "OK",
                            "check_number": 5,
                            "target_coil": "",
                            "rung_number": cc_detection_3_rung_number,
                            "servo_battery_replace_operand": contact_operand,
                        }

    return {
        "cc": "cc5",
        "status": "NG",
        "check_number": 5,
        "target_coil": "",
        "rung_number": -1,
        "servo_battery_replace_operand": "",
    }


def check_detail_6_programwise(
    fault_section_df: pd.DataFrame,
    cc_detection_3_rung_number: int,
    cc2_result: dict,
    cc3_result: dict,
    cc4_result: dict,
    cc5_result: str,
    program_name: str,
    program_comment_data: dict,
) -> dict:

    logger.info(f"Executing rule no 51 check detail 6")

    if (
        cc2_result["status"] == "OK"
        and cc3_result["status"] == "OK"
        and cc4_result["status"] == "OK"
        and cc5_result["status"] == "OK"
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
        cc2_op2 = cc2_result["detection_4_coil"]
        cc3_op1 = cc3_result["not_warning_operand"]
        cc3_op2 = cc3_result["detection_5_coil"]
        cc4_operand = cc4_result["PLC_battery_operand"]
        cc5_operand = cc5_result["servo_battery_replace_operand"]

        cc2_valid = False
        cc3_valid = False
        cc5_valid = False

        if formatted_parallel_contacts:
            # Primary check under cc4_operand
            if cc4_operand in formatted_parallel_contacts:
                for chain in formatted_parallel_contacts[cc4_operand]:
                    if cc2_op1 in chain and cc2_op2 in chain:
                        cc2_valid = True
                    elif cc3_op1 in chain and cc3_op2 in chain:
                        cc3_valid = True
                    elif cc5_operand in chain:
                        cc5_valid = True

                if cc2_valid and cc3_valid and cc5_valid:
                    return {
                        "cc": "cc6",
                        "status": "OK",
                        "check_number": 6,
                        "target_coil": "",
                        "rung_number": cc_detection_3_rung_number,
                    }

        cc2_valid = False
        cc3_valid = False
        cc4_valid = False

        if formatted_parallel_contacts:
            # Primary check under cc4_operand
            if cc5_operand in formatted_parallel_contacts:
                for chain in formatted_parallel_contacts[cc5_operand]:
                    if cc2_op1 in chain and cc2_op2 in chain:
                        cc2_valid = True
                    if cc3_op1 in chain and cc3_op2 in chain:
                        cc3_valid = True
                    if cc4_operand in chain:
                        cc4_valid = True

                if cc2_valid and cc3_valid and cc4_valid:
                    return {
                        "cc": "cc6",
                        "status": "OK",
                        "check_number": 6,
                        "target_coil": "",
                        "rung_number": cc_detection_3_rung_number,
                    }

    # Fallback if any validation fails
    return {
        "cc": "cc6",
        "status": "NG",
        "check_number": 6,
        "target_coil": "",
        "rung_number": -1,
    }


def check_detail_7_programwise(
    deviceout_section_df: pd.DataFrame,
    cc_detection_3_coil_operand: str,
    cc_detection_7_coil_operand: str,
) -> dict:

    logger.info(f"Executing rule no 51 check detail 7")

    if (
        cc_detection_3_coil_operand
        and cc_detection_7_coil_operand
        and not deviceout_section_df.empty
    ):

        deviceout_rung_groups_df = deviceout_section_df.groupby("RUNG")
        for _, device_rung_df in deviceout_rung_groups_df:

            cc7_detection_3_operand_exist = False
            cc7_detection_7_operand_exist = False

            contact_coil_fault_df = device_rung_df[
                (device_rung_df["OBJECT_TYPE_LIST"] == "Contact")
                | (device_rung_df["OBJECT_TYPE_LIST"] == "Coil")
            ].copy()

            if len(contact_coil_fault_df) == 2 and not contact_coil_fault_df.empty:
                for _, contact_coil_rung_row in contact_coil_fault_df.iterrows():
                    attr = ast.literal_eval(contact_coil_rung_row["ATTRIBUTES"])
                    operand = attr.get("operand")
                    negated_operand = attr.get("negated")
                    operand_type = contact_coil_rung_row["OBJECT_TYPE_LIST"]

                    if operand and isinstance(operand, str):
                        if (
                            operand_type == "Contact"
                            and operand == cc_detection_3_coil_operand
                            and negated_operand == "false"
                        ):
                            cc7_detection_3_operand_exist = True

                        if (
                            operand_type == "Coil"
                            and operand == cc_detection_7_coil_operand
                        ):
                            cc7_detection_7_operand_exist = True

                    if cc7_detection_3_operand_exist and cc7_detection_7_operand_exist:
                        return {
                            "cc": "cc7",
                            "status": "OK",
                            "check_number": 7,
                            "target_coil": "",
                            "rung_number": contact_coil_rung_row["RUNG"],
                        }

    return {
        "cc": "cc7",
        "status": "NG",
        "check_number": 7,
        "target_coil": "",
        "rung_number": -1,
    }


def check_detail_8_programwise(
    fault_section_df: pd.DataFrame,
    cc_detection_4_coil_operand: str,
    cc_detection_4_rung_number: int,
    program_name: str,
    program_comment_data: dict,
) -> dict:

    logger.info(f"Executing rule no 51 check detail 8")

    if (
        cc_detection_4_rung_number >= 0
        and cc_detection_4_coil_operand
        and not fault_section_df.empty
    ):

        is_check_8_1 = False
        is_check_8_2 = False
        is_check_8_3 = False

        detection_4_coil_rung_df = fault_section_df[
            fault_section_df["RUNG"] == cc_detection_4_rung_number
        ]
        contact_fault_df = detection_4_coil_rung_df[
            detection_4_coil_rung_df["OBJECT_TYPE_LIST"] == "Contact"
        ].copy()

        """
        checking condition 8.1 in which outcoil should be self holding
        """
        try:
            all_self_holding_contact = check_self_holding(detection_4_coil_rung_df)
        except:
            all_self_holding_contact = []

        if cc_detection_4_coil_operand in all_self_holding_contact:
            is_check_8_1 = True

        """
        checking condition 8.2 in which PL_AL_RST pattern should be inside self holding of detected outcoil 
        """
        PB_AL_RST = []
        is_PB_AL_RST_exist_inside = False
        self_holding_outlist_8_2 = []

        for _, contact_fault_row in contact_fault_df.iterrows():
            attr = ast.literal_eval(contact_fault_row["ATTRIBUTES"])
            contact_operand = attr.get("operand")
            negated_operand = attr.get("negated")

            if isinstance(contact_operand, str) and contact_operand:
                contact_comment = get_the_comment_from_program(
                    contact_operand, program_name, program_comment_data
                )

                if isinstance(contact_comment, list) and contact_comment:

                    if (
                        "pb" in contact_operand.lower()
                        and "al" in contact_operand.lower()
                        and (
                            "rst" in contact_operand.lower()
                            or "reset" in contact_operand.lower()
                        )
                        and negated_operand == "false"
                    ):
                        PB_AL_RST.extend(attr.get("out_list"))

                    if contact_operand == cc_detection_4_coil_operand:
                        self_holding_outlist_8_2.extend(attr.get("out_list"))

            """ 
            this is function is for checking if both contact should be in under self holding
            logic is if both contact outcoil is less than self holding outcoil then it is under slef holding
            """
            if PB_AL_RST and self_holding_outlist_8_2:
                for contact_out_val in PB_AL_RST:
                    if all(contact_out_val < x for x in self_holding_outlist_8_2):
                        is_PB_AL_RST_exist_inside = True
                        break

        if is_PB_AL_RST_exist_inside:
            is_check_8_2 = True

        ## checking 8.3
        """
        checking condition 8.3 in which not abnormal pattern should be outside self holding of detected outcoil 
        """
        not_abnormal_details = []
        self_holding_outlist_8_3 = []
        is_not_abnormal_exist_outside = False
        for _, contact_fault_row in contact_fault_df.iterrows():
            attr = ast.literal_eval(contact_fault_row["ATTRIBUTES"])
            contact_operand = attr.get("operand")
            negated_operand = attr.get("negated")

            if isinstance(contact_operand, str) and contact_operand:
                contact_comment = get_the_comment_from_program(
                    contact_operand, program_name, program_comment_data
                )

                if (
                    contact_operand
                    and isinstance(contact_comment, list)
                    and contact_comment
                ):

                    if (
                        regex_pattern_check(not_comment, contact_comment)
                        and regex_pattern_check(abnormal_comment, contact_comment)
                        and negated_operand == "true"
                    ):
                        not_abnormal_details.extend(attr.get("in_list"))

                    if contact_operand == cc_detection_4_coil_operand:
                        self_holding_outlist_8_3.extend(attr.get("out_list"))

            """ 
            this is function is for checking if both contact should be in under self holding
            logic is if both contact outcoil is less than self holding outcoil then it is under slef holding
            """
            if not_abnormal_details and self_holding_outlist_8_3:
                for not_abnormal_in_data in not_abnormal_details:
                    if any(not_abnormal_in_data >= x for x in self_holding_outlist_8_3):
                        is_not_abnormal_exist_outside = True
                        break

        if is_not_abnormal_exist_outside:
            is_check_8_3 = True

        if is_check_8_1 and is_check_8_2 and is_check_8_3:
            return {
                "cc": "cc8",
                "status": "OK",
                "check_number": 8,
                "target_coil": "",
                "rung_number": cc_detection_4_rung_number,
            }

    return {
        "cc": "cc8",
        "status": "NG",
        "check_number": 8,
        "target_coil": "",
        "rung_number": -1,
    }


def check_detail_9_programwise(
    fault_section_df: pd.DataFrame,
    cc_detection_5_coil_operand: str,
    cc_detection_5_rung_number: int,
    program_name: str,
    program_comment_data: dict,
) -> dict:

    logger.info(f"Executing rule no 51 check detail 9")

    if (
        cc_detection_5_rung_number >= 0
        and cc_detection_5_coil_operand
        and not fault_section_df.empty
    ):

        is_check_9_1 = False
        is_check_9_2 = False
        is_check_9_3 = False

        detection_5_coil_rung_df = fault_section_df[
            fault_section_df["RUNG"] == cc_detection_5_rung_number
        ]
        contact_fault_df = detection_5_coil_rung_df[
            detection_5_coil_rung_df["OBJECT_TYPE_LIST"] == "Contact"
        ].copy()

        """
        checking condition 9.1 in which outcoil should be self holding
        """
        try:
            all_self_holding_contact = check_self_holding(detection_5_coil_rung_df)
        except:
            all_self_holding_contact = []

        if cc_detection_5_coil_operand in all_self_holding_contact:
            is_check_9_1 = True

        """
        checking condition 9.2 in which not_abnormal comment operand should be oitside self holding of detected outcoil 
        """
        PB_AL_RST = []
        is_PB_AL_RST_exist_inside = False
        self_holding_outlist_9_2 = []

        for _, contact_fault_row in contact_fault_df.iterrows():
            attr = ast.literal_eval(contact_fault_row["ATTRIBUTES"])
            contact_operand = attr.get("operand")
            negated_operand = attr.get("negated")

            if isinstance(contact_operand, str) and contact_operand:
                contact_comment = get_the_comment_from_program(
                    contact_operand, program_name, program_comment_data
                )
                if (
                    contact_operand
                    and isinstance(contact_comment, list)
                    and contact_comment
                ):
                    if (
                        "pb" in contact_operand.lower()
                        and "al" in contact_operand.lower()
                        and (
                            "rst" in contact_operand.lower()
                            or "reset" in contact_comment.lower()
                        )
                        and negated_operand == "false"
                    ):
                        PB_AL_RST.append(attr.get("out_list"))

                    if contact_operand == cc_detection_5_coil_operand:
                        self_holding_outlist_9_2.append(attr.get("out_list"))

            """ 
            this is function is for checking if both contact should be in under self holding
            logic is if both contact outcoil is less than self holding outcoil then it is under slef holding
            """
            if PB_AL_RST and self_holding_outlist_9_2:
                for contact_out_val in PB_AL_RST:
                    if all(contact_out_val < x for x in self_holding_outlist_9_2):
                        is_PB_AL_RST_exist_inside = True
                        break

        if is_PB_AL_RST_exist_inside:
            is_check_9_2 = True

        """
        checking condition 9.3 in which not_abnormal comment operand should be oitside self holding of detected outcoil 
        """
        not_warning_details = []
        self_holding_outlist_9_3 = []
        is_not_warning_exist_outside = False
        for _, contact_fault_row in contact_fault_df.iterrows():
            attr = ast.literal_eval(contact_fault_row["ATTRIBUTES"])
            contact_operand = attr.get("operand")
            negated_operand = attr.get("negated")
            contact_comment = get_the_comment_from_program(
                contact_operand, program_name, program_comment_data
            )
            if (
                contact_operand
                and isinstance(contact_comment, list)
                and contact_comment
            ):
                if (
                    regex_pattern_check(not_comment, contact_comment)
                    and regex_pattern_check(warning_comment, contact_comment)
                    and negated_operand == "true"
                ):
                    not_warning_details.extend(attr.get("in_list"))

                if contact_operand == cc_detection_5_coil_operand:
                    self_holding_outlist_9_3.extend(attr.get("out_list"))

            """ 
            this is function is for checking if both contact should be in under self holding
            logic is if both contact outcoil is less than self holding outcoil then it is under slef holding
            """
            if not_warning_details and self_holding_outlist_9_3:
                for not_warning_in_data in not_warning_details:
                    if any(not_warning_in_data >= x for x in self_holding_outlist_9_3):
                        is_not_warning_exist_outside = True
                        break

        if is_not_warning_exist_outside:
            is_check_9_3 = True

        if is_check_9_1 and is_check_9_2 and is_check_9_3:
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
def execute_rule_51_programwise(
    input_program_file: str, input_program_comment_file: str
) -> pd.DataFrame:

    logger.info("Starting execution of Rule 51")

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
                deviceout_section_df = current_program_df[
                    current_program_df["BODY"].str.lower() == deviceout_section_name
                ]

                if not fault_section_df.empty:
                    # Run detection range logic as per Rule 24
                    detection_results = detection_range_programwise(
                        fault_section_df=fault_section_df,
                        deviceout_section_df=deviceout_section_df,
                        BZ_variable_comment=BZ_variable_comment,
                        buzzer_full_width_comment=buzzer_full_width_comment,
                        abnormal_comment=abnormal_comment,
                        warning_comment=warning_comment,
                        program_name=program_name,
                        program_comment_data=program_comment_data,
                    )

                    cc1_result = check_detail_1_programwise(
                        detection_results=detection_results
                    )

                    cc_detection_3_rung_number = detection_results[
                        "detection_3_details"
                    ][1]
                    cc_detection_4_rung_number = detection_results[
                        "detection_4_details"
                    ][1]
                    cc_detection_5_rung_number = detection_results[
                        "detection_5_details"
                    ][1]
                    cc_detection_4_coil_operand = detection_results[
                        "detection_4_details"
                    ][0]
                    cc_detection_5_coil_operand = detection_results[
                        "detection_5_details"
                    ][0]
                    cc_detection_3_coil_operand = detection_results[
                        "detection_3_details"
                    ][0]
                    cc_detection_7_coil_operand = detection_results[
                        "detection_7_details"
                    ][0]

                    cc2_result = check_detail_2_programwise(
                        fault_section_df=fault_section_df,
                        cc_detection_3_rung_number=cc_detection_3_rung_number,
                        cc_detection_4_coil_operand=cc_detection_4_coil_operand,
                        cc_detection_5_coil_operand=cc_detection_5_coil_operand,
                        not_comment=not_comment,
                        abnormal_comment=abnormal_comment,
                        warning_comment=warning_comment,
                        program_name=program_name,
                        program_comment_data=program_comment_data,
                    )

                    cc3_result = check_detail_3_programwise(
                        fault_section_df=fault_section_df,
                        cc_detection_3_rung_number=cc_detection_3_rung_number,
                        cc_detection_4_coil_operand=cc_detection_4_coil_operand,
                        cc_detection_5_coil_operand=cc_detection_5_coil_operand,
                        not_comment=not_comment,
                        abnormal_comment=abnormal_comment,
                        warning_comment=warning_comment,
                        program_name=program_name,
                        program_comment_data=program_comment_data,
                    )

                    cc4_result = check_detail_4_programwise(
                        fault_section_df=fault_section_df,
                        cc_detection_3_rung_number=cc_detection_3_rung_number,
                        battery_comment=battery_comment,
                        replace_comment=replace_comment,
                        program_name=program_name,
                        program_comment_data=program_comment_data,
                    )

                    cc5_result = check_detail_5_programwise(
                        fault_section_df=fault_section_df,
                        cc_detection_3_rung_number=cc_detection_3_rung_number,
                        servo_comment=servo_comment,
                        battery_comment=battery_comment,
                        replace_comment=replace_comment,
                        program_name=program_name,
                        program_comment_data=program_comment_data,
                    )

                    cc6_result = check_detail_6_programwise(
                        fault_section_df=fault_section_df,
                        cc_detection_3_rung_number=cc_detection_3_rung_number,
                        cc2_result=cc2_result,
                        cc3_result=cc3_result,
                        cc4_result=cc4_result,
                        cc5_result=cc5_result,
                        program_name=program_name,
                        program_comment_data=program_comment_data,
                    )

                    cc7_result = check_detail_7_programwise(
                        deviceout_section_df=deviceout_section_df,
                        cc_detection_3_coil_operand=cc_detection_3_coil_operand,
                        cc_detection_7_coil_operand=cc_detection_7_coil_operand,
                    )

                    cc8_result = check_detail_8_programwise(
                        fault_section_df=fault_section_df,
                        cc_detection_4_coil_operand=cc_detection_4_coil_operand,
                        cc_detection_4_rung_number=cc_detection_4_rung_number,
                        program_name=program_name,
                        program_comment_data=program_comment_data,
                    )

                    cc9_result = check_detail_9_programwise(
                        fault_section_df=fault_section_df,
                        cc_detection_5_coil_operand=cc_detection_5_coil_operand,
                        cc_detection_5_rung_number=cc_detection_5_rung_number,
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
                                "CheckItem": rule_51_check_item,
                                "Detail": ng_name,
                                "Status": "",
                            }
                        )

                        # output_rows.append({
                        #     "TASK_NAME": program_name,
                        #     "SECTION_NAME": fault_section_name,
                        #     "RULE_NUMBER": "51",
                        #     "CHECK_NUMBER": check_number,
                        #     "RUNG_NUMBER": -1 if rung_number < 0 else rung_number-1,
                        #     "RULE_CONTENT": rule_content_51,
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
            #         "RULE_CONTENT": rule_content_51,
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
