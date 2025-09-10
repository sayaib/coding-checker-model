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

# ============================ Rule 70: Definitions, Content, and Configuration Details ============================
workpiece_comment = "ワーク"
pallet_comment = "パレット"
tray_1_comment = "トレイ"
tray_2_comment = "トレー"
magazine_comment = "マガジン"
hoop_comment = "フープ"
stick_comment = "スティック"
skewer_comment = "串"
box_comment = "箱"

devicein_section_name = "devicein"
rule_70_check_item = "Rule of Sensor Delay Circuit"

check_detail_content = (
    "Check that the function name “TON” exists in the out coil condition detected by ③."
)

ng_content = "判定回路において、正しいOK判定(OK信号のONとNG信号のOFFのAND回路)になっていないためNG"


def is_allowed_task(name: str) -> bool:
    """
    Returns True if task name is allowed,
    False if it starts with P000–P101 (case-insensitive).
    """
    pattern = re.compile(r"^P(\d+)", re.IGNORECASE)
    match = pattern.match(name)
    if match:
        num = int(match.group(1))
        return not (0 <= num <= 101)  # False if in range
    return True  # No match means allowed


# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================


def detection_range_programwise(
    device_in_program_df: pd.DataFrame, program_name: str, program_comment_data: dict
):
    match_outcoil = []
    # print("device_in_program_df",device_in_program_df)
    if not device_in_program_df.empty:
        coil_df = device_in_program_df[
            device_in_program_df["OBJECT_TYPE_LIST"].str.lower() == "coil"
        ]
        if not coil_df.empty:
            for _, coil_row in coil_df.iterrows():
                attr = ast.literal_eval(coil_row["ATTRIBUTES"])
                coil_operand = attr.get("operand")
                if isinstance(coil_operand, str) and coil_operand:
                    coil_comment = get_the_comment_from_program(
                        coil_operand, program_name, program_comment_data
                    )
                    print("coil_comment", coil_comment, coil_operand)
                    if isinstance(coil_comment, list) and coil_comment:
                        if (
                            regex_pattern_check(workpiece_comment, coil_comment)
                            or regex_pattern_check(pallet_comment, coil_comment)
                            or regex_pattern_check(tray_1_comment, coil_comment)
                            or regex_pattern_check(tray_2_comment, coil_comment)
                            or regex_pattern_check(magazine_comment, coil_comment)
                            or regex_pattern_check(hoop_comment, coil_comment)
                            or regex_pattern_check(stick_comment, coil_comment)
                            or regex_pattern_check(skewer_comment, coil_comment)
                            or regex_pattern_check(box_comment, coil_comment)
                        ):
                            match_outcoil.append([coil_operand, coil_row["RUNG"]])

    print("match_outcoil", match_outcoil)
    return match_outcoil


# ============================== Function-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================


def detection_range_functionwise(
    device_in_function_df: pd.DataFrame, function_name: str, function_comment_data: dict
):
    match_outcoil = []
    # print("device_in_function_df",device_in_function_df)
    if not device_in_function_df.empty:
        coil_df = device_in_function_df[
            device_in_function_df["OBJECT_TYPE_LIST"].str.lower() == "coil"
        ]
        if not coil_df.empty:
            for _, coil_row in coil_df.iterrows():
                attr = ast.literal_eval(coil_row["ATTRIBUTES"])
                coil_operand = attr.get("operand")
                if isinstance(coil_operand, str) and coil_operand:
                    coil_comment = get_the_comment_from_function(
                        coil_operand, function_name, function_comment_data
                    )
                    if isinstance(coil_comment, list) and coil_comment:
                        if (
                            regex_pattern_check(workpiece_comment, coil_comment)
                            or regex_pattern_check(pallet_comment, coil_comment)
                            or regex_pattern_check(tray_1_comment, coil_comment)
                            or regex_pattern_check(tray_2_comment, coil_comment)
                            or regex_pattern_check(magazine_comment, coil_comment)
                            or regex_pattern_check(hoop_comment, coil_comment)
                            or regex_pattern_check(stick_comment, coil_comment)
                            or regex_pattern_check(skewer_comment, coil_comment)
                            or regex_pattern_check(box_comment, coil_comment)
                        ):
                            match_outcoil.append([coil_operand, coil_row["RUNG"]])

    return match_outcoil


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_70_programwise(
    input_program_file: str, program_comment_file: str
) -> pd.DataFrame:

    logger.info("Rule 70 Start executing rule 1 program wise")

    output_rows = []

    try:
        program_df = pd.read_csv(input_program_file)
        with open(program_comment_file, "r", encoding="utf-8") as file:
            program_comment_data = json.load(file)

        unique_program_values = program_df["PROGRAM"].unique()
        for program in unique_program_values:
            is_task_name_in_range = is_allowed_task(program)
            if is_task_name_in_range:
                curr_program_df = program_df[program_df["PROGRAM"] == program]
                device_in_program_df = curr_program_df[
                    curr_program_df["BODY"].str.lower() == devicein_section_name
                ]
                print("program name", program)
                detection_results = detection_range_programwise(
                    device_in_program_df=device_in_program_df,
                    program_name=program,
                    program_comment_data=program_comment_data,
                )
                print("detection_results", detection_results)
                if detection_results:
                    for coil_data in detection_results:
                        coil_rung_data_df = device_in_program_df[
                            device_in_program_df["RUNG"] == coil_data[1]
                        ]
                        status = "NG"
                        ng_name = ng_content
                        if not coil_rung_data_df.empty:
                            for _, coil_rung_row in coil_rung_data_df.iterrows():
                                attr = ast.literal_eval(coil_rung_row["ATTRIBUTES"])
                                operand = attr.get("operand")

                                if coil_rung_row["OBJECT_TYPE_LIST"].lower() == "coil":
                                    status = "NG"

                                if (
                                    operand == coil_data[0]
                                    and coil_rung_row["OBJECT_TYPE_LIST"].lower()
                                    == "coil"
                                ):
                                    break

                                if (
                                    coil_rung_row["OBJECT_TYPE_LIST"].lower() == "block"
                                    and attr.get("typeName") == "TON"
                                ):
                                    status = "OK"
                                    ng_name = ""
                                    break

                        output_rows.append(
                            {
                                "Result": status,
                                "Task": program,
                                "Section": "DeviceIn",
                                "RungNo": coil_data[1] - 1,
                                "Target": coil_data[0],
                                "CheckItem": rule_70_check_item,
                                "Detail": ng_name,
                                "Status": "",
                            }
                        )

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
        logger.error(f"Rule 70 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}


# ============================== Function-Wise Execution Starts Here ===============================
def execute_rule_70_functionwise(
    input_function_file: str, function_comment_file: str
) -> pd.DataFrame:

    logger.info("Rule 70 Start executing rule 1 function wise")

    output_rows = []

    try:
        function_df = pd.read_csv(input_function_file)
        with open(function_comment_file, "r", encoding="utf-8") as file:
            function_comment_data = json.load(file)

        unique_function_values = function_df["FUNCTION_BLOCK"].unique()
        for function in unique_function_values:
            is_task_name_in_range = is_allowed_task(function)
            if is_task_name_in_range:
                curr_function_df = function_df[
                    function_df["FUNCTION_BLOCK"] == function
                ]
                device_in_function_df = curr_function_df[
                    curr_function_df["BODY_TYPE"].str.lower() == devicein_section_name
                ]
                detection_results = detection_range_functionwise(
                    device_in_function_df=device_in_function_df,
                    function_name=function,
                    function_comment_data=function_comment_data,
                )
                print("detection_results", detection_results)
                if detection_results:
                    for coil_data in detection_results:
                        coil_rung_data_df = device_in_function_df[
                            device_in_function_df["RUNG"] == coil_data[1]
                        ]
                        status = "NG"
                        ng_name = ng_content
                        if not coil_rung_data_df.empty:
                            for _, coil_rung_row in coil_rung_data_df.iterrows():
                                attr = ast.literal_eval(coil_rung_row["ATTRIBUTES"])
                                operand = attr.get("operand")

                                if coil_rung_row["OBJECT_TYPE_LIST"].lower() == "coil":
                                    status = "NG"

                                if (
                                    operand == coil_data[0]
                                    and coil_rung_row["OBJECT_TYPE_LIST"].lower()
                                    == "coil"
                                ):
                                    break

                                if (
                                    coil_rung_row["OBJECT_TYPE_LIST"].lower() == "block"
                                    and attr.get("typeName") == "TON"
                                ):
                                    status = "OK"
                                    ng_name = ""
                                    break

                        output_rows.append(
                            {
                                "Result": status,
                                "Task": function,
                                "Section": "DeviceIn",
                                "RungNo": coil_data[1] - 1,
                                "Target": coil_data[0],
                                "CheckItem": rule_70_check_item,
                                "Detail": ng_name,
                                "Status": "",
                            }
                        )

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
        logger.error(f"Rule 70 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}
