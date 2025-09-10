import ast
import json
import pandas as pd
from typing import *
import re
from ...main import logger
import polars as pl
from .extract_comment_from_variable import (
    get_the_comment_from_program,
    get_the_comment_from_function,
)
from .ladder_utils import regex_pattern_check, clean_rung_number
from .rule_67_ladder_utils import get_series_contacts

# ============================ Rule 67: Definitions, Content, and Configuration Details ============================
rule_content_67 = "・Indicator light, signal tower, buzzer and emergency stop interlock output are standard circuits."

rule_67_check_item = "Rule of Device Output Circuit"
deviceout_section_name = "deviceout"

PL_comment = "PL"
PLC_comment = "PLC"
master_on_comment = "運転準備"
auto_comment = "自動"
start_comment = "起動"
tower_comment = "タワー"
red_comment = "赤"
yellow_comment = "黄"
green_comment = "緑"
buzzer_comment = "ブザー"
detection_emergency_stop_comment = "非常停止"
checking_content_emergency_stop_comment = "緊急停止"
interlock_comment = "インターロック"
complete_comment = "完了"
run_comment = "運転"
during_comment = "中"
normal_comment = "正常"
machine_comment = "設備"
stop_comment = "停止"
not_comment = "でない"


check_detail_content = {
    "cc1": "If ① but not ③, ④, ⑤, ⑥, ⑦, ⑧, and ⑨, it is assumed to be NG.",
    "cc2": 'Check that only one A contact containing "運転準備(master on)”+"完了(complete)" in the variable comment is connected to the outcoil condition detected in ③.',
    "cc3": "Check that only one A contact containing ”自動(auto)"
    + "運転(run)"
    + "中(during)” in the variable comment is connected to the outcoil condition detected in ④.",
    "cc4": " Check that only one A contact containing ”タワー(tower)+'赤(red)” in the variable comment is connected to the outcoil condition detected in ⑤.",
    "cc5": " Check that only one A contact containing ”タワー(tower)+'黄(yellow)” in the variable comment is connected to the outcoil condition detected in ⑥.",
    "cc6": " Check that only one A contact containing ”タワー(tower)+'緑(green)” in the variable comment is connected to the outcoil condition detected in ⑦.",
    "cc7": "Check that only one A contact containing ”ブザー(buzzer)” in the variable comment is connected to the outcoil condition detected in ⑧.",
    "cc8": "Check that an A contact containing ”PLC'+正常(normal)” in the variable comment is connected to the outcoil condition detected in ⑨.'",
    "cc9": "Check that an A contact containing ”緊急停止(emergency stop)"
    + "でない(not)” in the variable comment is connected to the outcoil condition detected in ⑨.",
    "cc10": "Check that an B contact containing ”設備(machine)"
    + "停止(stop)” in the variable comment is connected to the outcoil condition detected in ⑨.",
    "cc11": " Check that ❽, ❾, ❿, and ⓫ are connected in series (AND) to the outcoil condition detected in ⑨.",
}


ng_content = {
    "cc1": "Mainタスクのdevice outセクションにて、機器出力の回路が存在しないためNG",
    "cc2": "運転準備ランプの条件に、運転準備完了のA接点が接続されていないのでNG",
    "cc3": "自動起動ランプの点灯する条件に、自動運転中のA接点が接続されていないのでNG",
    "cc4": "シグナルタワー赤の点灯する条件に、シグナルタワー赤のA接点が接続されていないのでNG",
    "cc5": "シグナルタワー黄の点灯する条件に、シグナルタワー黄のA接点が接続されていないのでNG",
    "cc6": "シグナルタワー緑の点灯する条件に、シグナルタワー緑のA接点が接続されていないのでNG",
    "cc7": "ブザーの出力条件に、ブザーのA接点が接続されていないのでNG",
    "cc8": "非常停止インターロックの出力条件に、PLC正常のA接点が接続されていないのでNG",
    "cc9": "非常停止インターロックの出力条件に、緊急停止でないのA接点が接続されていないのでNG",
    "cc10": "非常停止インターロックの出力条件に、設備停止運転完了のB接点が接続されていないのでNG",
    "cc11": "非常停止インターロックの出力条件が、直列になっていないのでNG",
}

# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================


def detection_range_programwise(
    deviceout_section_df: pd.DataFrame,
    program_name: str,
    program_comment_data: dict,
    PL_comment: str,
    master_on_comment: str,
    auto_comment: str,
    start_comment: str,
    tower_comment: str,
    red_comment: str,
    yellow_comment: str,
    green_comment: str,
    buzzer_comment: str,
    detection_emergency_stop_comment: str,
    interlock_comment: str,
) -> dict:

    logger.info(
        f"Executing rule 67 detection range on program {program_name} and section name {deviceout_section_name} on rule 67"
    )

    # Filter only 'Contact' rows
    coil_df = deviceout_section_df[
        deviceout_section_df["OBJECT_TYPE_LIST"].str.lower() == "coil"
    ].copy()

    match_coil = {
        "PL_masteron": [],
        "PL_auto": [],
        "tower_red": [],
        "tower_yellow": [],
        "tower_green": [],
        "buzzer": [],
        "emergency_stop_interlock": [],
    }

    if not coil_df.empty:
        for _, coil_row in coil_df.iterrows():
            attr = ast.literal_eval(coil_row["ATTRIBUTES"])
            coil_operand = attr.get("operand")

            if isinstance(coil_operand, str) and coil_operand:
                coil_comment = get_the_comment_from_program(
                    coil_operand, program_name, program_comment_data
                )
                print("coil_comment", coil_comment)

                if isinstance(coil_comment, list) and coil_comment:
                    if regex_pattern_check(
                        PL_comment, coil_comment
                    ) and regex_pattern_check(master_on_comment, coil_comment):
                        match_coil["PL_masteron"] = [coil_operand, coil_row["RUNG"]]

                    if (
                        regex_pattern_check(PL_comment, coil_comment)
                        and regex_pattern_check(auto_comment, coil_comment)
                        and regex_pattern_check(start_comment, coil_comment)
                    ):
                        match_coil["PL_auto"] = [coil_operand, coil_row["RUNG"]]

                    if regex_pattern_check(
                        tower_comment, coil_comment
                    ) and regex_pattern_check(red_comment, coil_comment):
                        match_coil["tower_red"] = [coil_operand, coil_row["RUNG"]]

                    if regex_pattern_check(
                        tower_comment, coil_comment
                    ) and regex_pattern_check(yellow_comment, coil_comment):
                        match_coil["tower_yellow"] = [coil_operand, coil_row["RUNG"]]

                    if regex_pattern_check(
                        tower_comment, coil_comment
                    ) and regex_pattern_check(green_comment, coil_comment):
                        match_coil["tower_green"] = [coil_operand, coil_row["RUNG"]]

                    if regex_pattern_check(buzzer_comment, coil_comment):
                        match_coil["buzzer"] = [coil_operand, coil_row["RUNG"]]

                    if regex_pattern_check(
                        detection_emergency_stop_comment, coil_comment
                    ) and regex_pattern_check(interlock_comment, coil_comment):
                        match_coil["emergency_stop_interlock"] = [
                            coil_operand,
                            coil_row["RUNG"],
                        ]

    return match_coil


def check_detail_1_programwise(detection_result: dict, program_name: str) -> dict:

    logger.info(f"Executing rule no 67 check detail 1 in program {program_name}")

    status = "NG"
    if all(len(v) > 0 for v in detection_result.values()):
        status = "OK"

    return {"status": status, "rung_number": -1, "target_coil": "", "cc": "cc1"}


def check_detail_2_programwise(
    deviceout_section_df: pd.DataFrame,
    detection_result: dict,
    program_name: str,
    program_comment_data: dict,
    master_on_comment: str,
    complete_comment: str,
) -> dict:

    logger.info(f"Executing rule no 67 check detail 2 in program {program_name}")

    status = "NG"
    rung_number = -1
    operand = ""
    if detection_result["PL_masteron"]:
        rung_number = detection_result.get("PL_masteron")[1]
        current_rung_number_df = deviceout_section_df[
            deviceout_section_df["RUNG"] == rung_number
        ]
        contact_df = current_rung_number_df[
            current_rung_number_df["OBJECT_TYPE_LIST"].str.lower() == "contact"
        ].copy()

        if len(contact_df) == 1:
            for _, contact_row in contact_df.iterrows():
                attr = ast.literal_eval(contact_row["ATTRIBUTES"])
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
                            and negated_operand == "false"
                        ):
                            status = "OK"
                            operand = contact_operand
                            break

    return {
        "status": status,
        "rung_number": rung_number,
        "target_coil": operand,
        "cc": "cc2",
    }


def check_detail_3_programwise(
    deviceout_section_df: pd.DataFrame,
    detection_result: dict,
    program_name: str,
    program_comment_data: dict,
    auto_comment: str,
    run_comment: str,
    during_comment: str,
) -> dict:

    logger.info(f"Executing rule no 67 check detail 3 in program {program_name}")

    status = "NG"
    rung_number = -1
    operand = ""
    if detection_result["PL_auto"]:
        rung_number = detection_result.get("PL_auto")[1]
        current_rung_number_df = deviceout_section_df[
            deviceout_section_df["RUNG"] == rung_number
        ]
        contact_df = current_rung_number_df[
            current_rung_number_df["OBJECT_TYPE_LIST"].str.lower() == "contact"
        ].copy()

        if len(contact_df) == 1:
            for _, contact_row in contact_df.iterrows():
                attr = ast.literal_eval(contact_row["ATTRIBUTES"])
                contact_operand = attr.get("operand")
                negated_operand = attr.get("negated")
                if isinstance(contact_operand, str) and contact_operand:
                    contact_comment = get_the_comment_from_program(
                        contact_operand, program_name, program_comment_data
                    )

                    if isinstance(contact_comment, list) and contact_comment:
                        if (
                            regex_pattern_check(auto_comment, contact_comment)
                            and regex_pattern_check(run_comment, contact_comment)
                            and regex_pattern_check(during_comment, contact_comment)
                            and negated_operand == "false"
                        ):
                            status = "OK"
                            operand = contact_operand
                            break

    return {
        "status": status,
        "rung_number": rung_number,
        "target_coil": operand,
        "cc": "cc3",
    }


def check_detail_4_programwise(
    deviceout_section_df: pd.DataFrame,
    detection_result: dict,
    program_name: str,
    program_comment_data: dict,
    tower_comment: str,
    red_comment: str,
) -> dict:

    logger.info(f"Executing rule no 67 check detail 4 in program {program_name}")

    status = "NG"
    rung_number = -1
    operand = ""
    if detection_result["tower_red"]:
        rung_number = detection_result.get("tower_red")[1]
        current_rung_number_df = deviceout_section_df[
            deviceout_section_df["RUNG"] == rung_number
        ]
        contact_df = current_rung_number_df[
            current_rung_number_df["OBJECT_TYPE_LIST"].str.lower() == "contact"
        ].copy()

        if len(contact_df) == 1:
            for _, contact_row in contact_df.iterrows():
                attr = ast.literal_eval(contact_row["ATTRIBUTES"])
                contact_operand = attr.get("operand")
                negated_operand = attr.get("negated")
                if isinstance(contact_operand, str) and contact_operand:
                    contact_comment = get_the_comment_from_program(
                        contact_operand, program_name, program_comment_data
                    )

                    if isinstance(contact_comment, list) and contact_comment:
                        if (
                            regex_pattern_check(tower_comment, contact_comment)
                            and regex_pattern_check(red_comment, contact_comment)
                            and negated_operand == "false"
                        ):
                            status = "OK"
                            operand = contact_operand
                            break

    return {
        "status": status,
        "rung_number": rung_number,
        "target_coil": operand,
        "cc": "cc4",
    }


def check_detail_5_programwise(
    deviceout_section_df: pd.DataFrame,
    detection_result: dict,
    program_name: str,
    program_comment_data: dict,
    tower_comment: str,
    yellow_comment: str,
) -> dict:

    logger.info(f"Executing rule no 67 check detail 5 in program {program_name}")

    status = "NG"
    rung_number = -1
    operand = ""
    if detection_result["tower_yellow"]:
        rung_number = detection_result.get("tower_yellow")[1]
        current_rung_number_df = deviceout_section_df[
            deviceout_section_df["RUNG"] == rung_number
        ]
        contact_df = current_rung_number_df[
            current_rung_number_df["OBJECT_TYPE_LIST"].str.lower() == "contact"
        ].copy()

        if len(contact_df) == 1:
            for _, contact_row in contact_df.iterrows():
                attr = ast.literal_eval(contact_row["ATTRIBUTES"])
                contact_operand = attr.get("operand")
                negated_operand = attr.get("negated")
                if isinstance(contact_operand, str) and contact_operand:
                    contact_comment = get_the_comment_from_program(
                        contact_operand, program_name, program_comment_data
                    )

                    if isinstance(contact_comment, list) and contact_comment:
                        if (
                            regex_pattern_check(tower_comment, contact_comment)
                            and regex_pattern_check(yellow_comment, contact_comment)
                            and negated_operand == "false"
                        ):
                            status = "OK"
                            operand = contact_operand
                            break

    return {
        "status": status,
        "rung_number": rung_number,
        "target_coil": operand,
        "cc": "cc5",
    }


def check_detail_6_programwise(
    deviceout_section_df: pd.DataFrame,
    detection_result: dict,
    program_name: str,
    program_comment_data: dict,
    tower_comment: str,
    green_comment: str,
) -> dict:

    logger.info(f"Executing rule no 67 check detail 6 in program {program_name}")

    status = "NG"
    rung_number = -1
    operand = ""
    if detection_result["tower_green"]:
        rung_number = detection_result.get("tower_green")[1]
        current_rung_number_df = deviceout_section_df[
            deviceout_section_df["RUNG"] == rung_number
        ]
        contact_df = current_rung_number_df[
            current_rung_number_df["OBJECT_TYPE_LIST"].str.lower() == "contact"
        ].copy()

        if len(contact_df) == 1:
            for _, contact_row in contact_df.iterrows():
                attr = ast.literal_eval(contact_row["ATTRIBUTES"])
                contact_operand = attr.get("operand")
                negated_operand = attr.get("negated")
                if isinstance(contact_operand, str) and contact_operand:
                    contact_comment = get_the_comment_from_program(
                        contact_operand, program_name, program_comment_data
                    )

                    if isinstance(contact_comment, list) and contact_comment:
                        if (
                            regex_pattern_check(tower_comment, contact_comment)
                            and regex_pattern_check(green_comment, contact_comment)
                            and negated_operand == "false"
                        ):
                            status = "OK"
                            operand = contact_operand
                            break

    return {
        "status": status,
        "rung_number": rung_number,
        "target_coil": operand,
        "cc": "cc6",
    }


def check_detail_7_programwise(
    deviceout_section_df: pd.DataFrame,
    detection_result: dict,
    program_name: str,
    program_comment_data: dict,
    buzzer_comment: str,
) -> dict:

    logger.info(f"Executing rule no 67 check detail 7 in program {program_name}")

    status = "NG"
    rung_number = -1
    operand = ""
    if detection_result["buzzer"]:
        rung_number = detection_result.get("buzzer")[1]
        current_rung_number_df = deviceout_section_df[
            deviceout_section_df["RUNG"] == rung_number
        ]
        contact_df = current_rung_number_df[
            current_rung_number_df["OBJECT_TYPE_LIST"].str.lower() == "contact"
        ].copy()

        if len(contact_df) == 1:
            for _, contact_row in contact_df.iterrows():
                attr = ast.literal_eval(contact_row["ATTRIBUTES"])
                contact_operand = attr.get("operand")
                negated_operand = attr.get("negated")
                if isinstance(contact_operand, str) and contact_operand:
                    contact_comment = get_the_comment_from_program(
                        contact_operand, program_name, program_comment_data
                    )

                    if isinstance(contact_comment, list) and contact_comment:
                        if (
                            regex_pattern_check(buzzer_comment, contact_comment)
                            and negated_operand == "false"
                        ):
                            status = "OK"
                            operand = contact_operand
                            break

    return {
        "status": status,
        "rung_number": rung_number,
        "target_coil": operand,
        "cc": "cc7",
    }


def check_detail_8_programwise(
    deviceout_section_df: pd.DataFrame,
    detection_result: dict,
    program_name: str,
    program_comment_data: dict,
    PLC_comment: str,
    normal_comment,
) -> dict:

    logger.info(f"Executing rule no 67 check detail 8 in program {program_name}")

    status = "NG"
    rung_number = -1
    operand = ""
    if detection_result["emergency_stop_interlock"]:
        rung_number = detection_result.get("emergency_stop_interlock")[1]
        current_rung_number_df = deviceout_section_df[
            deviceout_section_df["RUNG"] == rung_number
        ]
        contact_df = current_rung_number_df[
            current_rung_number_df["OBJECT_TYPE_LIST"].str.lower() == "contact"
        ].copy()

        for _, contact_row in contact_df.iterrows():
            attr = ast.literal_eval(contact_row["ATTRIBUTES"])
            contact_operand = attr.get("operand")
            negated_operand = attr.get("negated")
            if isinstance(contact_operand, str) and contact_operand:
                contact_comment = get_the_comment_from_program(
                    contact_operand, program_name, program_comment_data
                )

                if isinstance(contact_comment, list) and contact_comment:
                    if (
                        regex_pattern_check(PLC_comment, contact_comment)
                        and regex_pattern_check(normal_comment, contact_comment)
                        and negated_operand == "false"
                    ):
                        status = "OK"
                        operand = contact_operand
                        break

    return {
        "status": status,
        "rung_number": rung_number,
        "target_coil": operand,
        "cc": "cc8",
    }


def check_detail_9_programwise(
    deviceout_section_df: pd.DataFrame,
    detection_result: dict,
    program_name: str,
    program_comment_data: dict,
    checking_content_emergency_stop_comment: str,
    not_comment,
) -> dict:

    logger.info(f"Executing rule no 67 check detail 9 in program {program_name}")

    status = "NG"
    rung_number = -1
    operand = ""
    if detection_result["emergency_stop_interlock"]:
        rung_number = detection_result.get("emergency_stop_interlock")[1]
        current_rung_number_df = deviceout_section_df[
            deviceout_section_df["RUNG"] == rung_number
        ]
        contact_df = current_rung_number_df[
            current_rung_number_df["OBJECT_TYPE_LIST"].str.lower() == "contact"
        ].copy()

        for _, contact_row in contact_df.iterrows():
            attr = ast.literal_eval(contact_row["ATTRIBUTES"])
            contact_operand = attr.get("operand")
            negated_operand = attr.get("negated")
            if isinstance(contact_operand, str) and contact_operand:
                contact_comment = get_the_comment_from_program(
                    contact_operand, program_name, program_comment_data
                )

                if isinstance(contact_comment, list) and contact_comment:
                    if (
                        regex_pattern_check(
                            checking_content_emergency_stop_comment, contact_comment
                        )
                        and regex_pattern_check(not_comment, contact_comment)
                        and negated_operand == "false"
                    ):
                        status = "OK"
                        operand = contact_operand
                        break

    return {
        "status": status,
        "rung_number": rung_number,
        "target_coil": operand,
        "cc": "cc9",
    }


def check_detail_10_programwise(
    deviceout_section_df: pd.DataFrame,
    detection_result: dict,
    program_name: str,
    program_comment_data: dict,
    machine_comment: str,
    stop_comment,
) -> dict:

    logger.info(f"Executing rule no 67 check detail 10 in program {program_name}")

    status = "NG"
    rung_number = -1
    operand = ""
    if detection_result["emergency_stop_interlock"]:
        rung_number = detection_result.get("emergency_stop_interlock")[1]
        current_rung_number_df = deviceout_section_df[
            deviceout_section_df["RUNG"] == rung_number
        ]
        contact_df = current_rung_number_df[
            current_rung_number_df["OBJECT_TYPE_LIST"].str.lower() == "contact"
        ].copy()

        for _, contact_row in contact_df.iterrows():
            attr = ast.literal_eval(contact_row["ATTRIBUTES"])
            contact_operand = attr.get("operand")
            negated_operand = attr.get("negated")
            if isinstance(contact_operand, str) and contact_operand:
                contact_comment = get_the_comment_from_program(
                    contact_operand, program_name, program_comment_data
                )

                if isinstance(contact_comment, list) and contact_comment:
                    if (
                        regex_pattern_check(machine_comment, contact_comment)
                        and regex_pattern_check(stop_comment, contact_comment)
                        and negated_operand == "true"
                    ):
                        status = "OK"
                        operand = contact_operand
                        break

    return {
        "status": status,
        "rung_number": rung_number,
        "target_coil": operand,
        "cc": "cc10",
    }


def check_detail_11_programwise(
    deviceout_section_df: pd.DataFrame,
    program_name: str,
    cc8_result: str,
    cc9_result: str,
    cc10_result: str,
) -> dict:

    logger.info(f"Executing rule no 67 check detail 11 in program {program_name}")

    status = "NG"
    rung_number = -1

    if all(res.get("status") == "OK" for res in [cc8_result, cc9_result, cc10_result]):
        rung_number = cc8_result.get("rung_number")
        cc8_operand = cc8_result.get("target_coil")
        cc9_operand = cc9_result.get("target_coil")
        cc10_operand = cc10_result.get("target_coil")

        current_rung_df = deviceout_section_df[
            deviceout_section_df["RUNG"] == rung_number
        ]
        all_series_contact_data = get_series_contacts(pl.from_pandas(current_rung_df))
        series_contact_operand_list = [
            [step["operand"] for step in rung] for rung in all_series_contact_data
        ]
        if series_contact_operand_list:
            for each_series_contact in series_contact_operand_list:
                if all(
                    operand in each_series_contact
                    for operand in [cc8_operand, cc9_operand, cc10_operand]
                ):
                    status = "OK"
                    break

    return {
        "status": status,
        "rung_number": rung_number,
        "target_coil": "",
        "cc": "cc10",
    }


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_67_programwise(
    input_program_file: str, program_comment_file
) -> pd.DataFrame:

    logger.info("Starting execution of Rule 67")

    try:

        program_df = pd.read_csv(input_program_file)

        with open(program_comment_file, "r", encoding="utf-8") as file:
            program_comment_data = json.load(file)

        unique_program_values = program_df["PROGRAM"].unique()

        output_rows = []
        for program_name in unique_program_values:

            logger.info(f"Executing rule 67 in Program {program_name}")

            if "main" in program_name.lower():

                current_program_df = program_df[program_df["PROGRAM"] == program_name]
                deviceout_section_df = current_program_df[
                    current_program_df["BODY"].str.lower() == deviceout_section_name
                ]

                # Run detection range logic as per Rule 24
                detection_result = detection_range_programwise(
                    deviceout_section_df=deviceout_section_df,
                    program_name=program_name,
                    program_comment_data=program_comment_data,
                    PL_comment=PL_comment,
                    master_on_comment=master_on_comment,
                    auto_comment=auto_comment,
                    start_comment=start_comment,
                    tower_comment=tower_comment,
                    red_comment=red_comment,
                    yellow_comment=yellow_comment,
                    green_comment=green_comment,
                    buzzer_comment=buzzer_comment,
                    detection_emergency_stop_comment=detection_emergency_stop_comment,
                    interlock_comment=interlock_comment,
                )

                print("detection_result", detection_result)

                cc1_result = check_detail_1_programwise(
                    detection_result=detection_result, program_name=program_name
                )

                cc2_result = check_detail_2_programwise(
                    deviceout_section_df=deviceout_section_df,
                    detection_result=detection_result,
                    program_name=program_name,
                    program_comment_data=program_comment_data,
                    master_on_comment=master_on_comment,
                    complete_comment=complete_comment,
                )

                cc3_result = check_detail_3_programwise(
                    deviceout_section_df=deviceout_section_df,
                    detection_result=detection_result,
                    program_name=program_name,
                    program_comment_data=program_comment_data,
                    auto_comment=auto_comment,
                    run_comment=run_comment,
                    during_comment=during_comment,
                )

                cc4_result = check_detail_4_programwise(
                    deviceout_section_df=deviceout_section_df,
                    detection_result=detection_result,
                    program_name=program_name,
                    program_comment_data=program_comment_data,
                    tower_comment=tower_comment,
                    red_comment=red_comment,
                )

                cc5_result = check_detail_5_programwise(
                    deviceout_section_df=deviceout_section_df,
                    detection_result=detection_result,
                    program_name=program_name,
                    program_comment_data=program_comment_data,
                    tower_comment=tower_comment,
                    yellow_comment=yellow_comment,
                )

                cc6_result = check_detail_6_programwise(
                    deviceout_section_df=deviceout_section_df,
                    detection_result=detection_result,
                    program_name=program_name,
                    program_comment_data=program_comment_data,
                    tower_comment=tower_comment,
                    green_comment=green_comment,
                )

                cc7_result = check_detail_7_programwise(
                    deviceout_section_df=deviceout_section_df,
                    detection_result=detection_result,
                    program_name=program_name,
                    program_comment_data=program_comment_data,
                    buzzer_comment=buzzer_comment,
                )

                cc8_result = check_detail_8_programwise(
                    deviceout_section_df=deviceout_section_df,
                    detection_result=detection_result,
                    program_name=program_name,
                    program_comment_data=program_comment_data,
                    PLC_comment=PLC_comment,
                    normal_comment=normal_comment,
                )

                cc9_result = check_detail_9_programwise(
                    deviceout_section_df=deviceout_section_df,
                    detection_result=detection_result,
                    program_name=program_name,
                    program_comment_data=program_comment_data,
                    checking_content_emergency_stop_comment=checking_content_emergency_stop_comment,
                    not_comment=not_comment,
                )

                cc10_result = check_detail_10_programwise(
                    deviceout_section_df=deviceout_section_df,
                    detection_result=detection_result,
                    program_name=program_name,
                    program_comment_data=program_comment_data,
                    machine_comment=machine_comment,
                    stop_comment=stop_comment,
                )

                cc11_result = check_detail_11_programwise(
                    deviceout_section_df=deviceout_section_df,
                    program_name=program_name,
                    cc8_result=cc8_result,
                    cc9_result=cc9_result,
                    cc10_result=cc10_result,
                )

                for cc_result in [
                    cc1_result,
                    cc2_result,
                    cc3_result,
                    cc4_result,
                    cc5_result,
                    cc6_result,
                    cc7_result,
                    cc8_result,
                    cc9_result,
                    cc10_result,
                    cc11_result,
                ]:
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

                    output_rows.append(
                        {
                            "Result": cc_result.get("status"),
                            "Task": program_name,
                            "Section": "AutoMain",
                            "RungNo": rung_number,
                            "Target": target_outcoil,
                            "CheckItem": rule_67_check_item,
                            "Detail": ng_name,
                            "Status": "",
                        }
                    )

        final_output_df = pd.DataFrame(output_rows)
        print("final_output_df", final_output_df)
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
        logger.error(f"Rule 67 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}
