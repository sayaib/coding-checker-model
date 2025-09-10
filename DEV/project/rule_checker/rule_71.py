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
from .rule_71_ladder_utils import get_series_contacts


# ============================ Rule 71: Definitions, Content, and Configuration Details ============================
communication_comment = "通信"
network_comment = "ネットワーク"
normal_comment = "正常"

devicein_section_name = "devicein"
rule_71_check_item = "Rule of Sensor Delay Circuit"
check_detail_content = " Check that one of the following is connected in series (AND) with the B contact to the out-coil condition detected in ④. Otherwise, NG."
ng_content = "センサ入力のB接点の回路に電源入り確認もしくは通信正常確認が入っていないためNG, NG because the circuit of B contact of the sensor input does not have power supply confirmation or communication normality confirmation."


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
    match_outcoil_dict = {}
    T_coil_found = False
    B_contact_found = False
    B_contact_operand = ""
    T_coil_operand = ""
    index = 0

    # print("device_in_program_df",device_in_program_df)
    if not device_in_program_df.empty:
        contact_coil_df = device_in_program_df[
            device_in_program_df["OBJECT_TYPE_LIST"]
            .str.lower()
            .isin(["contact", "coil"])
        ]
        if not contact_coil_df.empty:
            for _, contact_coil_row in contact_coil_df.iterrows():
                attr = ast.literal_eval(contact_coil_row["ATTRIBUTES"])
                contact_coil_operand = attr.get("operand")
                negated_operand = attr.get("negated")
                if isinstance(contact_coil_operand, str) and contact_coil_operand:
                    if (
                        contact_coil_row["OBJECT_TYPE_LIST"].lower() == "coil"
                        and not B_contact_found
                    ) or not contact_coil_operand.startswith("T"):
                        T_coil_found = False
                        B_contact_found = False

                    if contact_coil_row[
                        "OBJECT_TYPE_LIST"
                    ].lower() == "coil" and contact_coil_operand.startswith("T"):
                        T_coil_found = True
                        T_coil_operand = contact_coil_operand

                    if (
                        contact_coil_row["OBJECT_TYPE_LIST"].lower() == "contact"
                        and negated_operand == "true"
                    ):
                        B_contact_found = True
                        B_contact_operand = contact_coil_operand
                        rung_number = contact_coil_row["RUNG"]

                if T_coil_found and B_contact_found:
                    match_outcoil_dict[index] = {
                        "B_contact": B_contact_operand,
                        "T_coil": T_coil_operand,
                        "rung_number": rung_number,
                    }
                    index += 1

    return match_outcoil_dict


# ============================== Function-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================


def detection_range_functionwise(
    device_in_function_df: pd.DataFrame, function_name: str, function_comment_data: dict
):
    match_outcoil_dict = {}
    T_coil_found = False
    B_contact_found = False
    B_contact_operand = ""
    T_coil_operand = ""
    index = 0

    # print("device_in_function_df",device_in_function_df)
    if not device_in_function_df.empty:
        contact_coil_df = device_in_function_df[
            device_in_function_df["OBJECT_TYPE_LIST"]
            .str.lower()
            .isin(["contact", "coil"])
        ]
        if not contact_coil_df.empty:
            for _, contact_coil_row in contact_coil_df.iterrows():
                attr = ast.literal_eval(contact_coil_row["ATTRIBUTES"])
                contact_coil_operand = attr.get("operand")
                negated_operand = attr.get("negated")
                if isinstance(contact_coil_operand, str) and contact_coil_operand:
                    if (
                        contact_coil_row["OBJECT_TYPE_LIST"].lower() == "coil"
                        and not B_contact_found
                    ) or not contact_coil_operand.startswith("T"):
                        T_coil_found = False
                        B_contact_found = False

                    if contact_coil_row[
                        "OBJECT_TYPE_LIST"
                    ].lower() == "coil" and contact_coil_operand.startswith("T"):
                        T_coil_found = True
                        T_coil_operand = contact_coil_operand

                    if (
                        contact_coil_row["OBJECT_TYPE_LIST"].lower() == "contact"
                        and negated_operand == "true"
                    ):
                        B_contact_found = True
                        B_contact_operand = contact_coil_operand
                        rung_number = contact_coil_row["RUNG"]

                if T_coil_found and B_contact_found:
                    match_outcoil_dict[index] = {
                        "B_contact": B_contact_operand,
                        "T_coil": T_coil_operand,
                        "rung_number": rung_number,
                    }
                    index += 1

    return match_outcoil_dict


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_71_programwise(
    input_program_file: str, program_comment_file: str
) -> pd.DataFrame:

    logger.info("Rule 71 Start executing rule 1 program wise")

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
                detection_results = detection_range_programwise(
                    device_in_program_df=device_in_program_df,
                    program_name=program,
                    program_comment_data=program_comment_data,
                )
                if detection_results:
                    for k, v in detection_results.items():
                        T_coil = v["T_coil"]
                        B_contact = v["B_contact"]
                        status = "NG"
                        ng_name = ng_content
                        match_rung_df = device_in_program_df[
                            device_in_program_df["RUNG"] == v["rung_number"]
                        ]
                        get_series_connect_data = get_series_contacts(
                            pl.from_pandas(match_rung_df)
                        )
                        series_contact_operands_only = [
                            [item.get("operand") for item in sublist]
                            for sublist in get_series_connect_data
                        ]
                        for series_contact in series_contact_operands_only:
                            both_coil_contact_present = B_contact in series_contact
                            if both_coil_contact_present:
                                for series_contact_operand in series_contact:
                                    if series_contact_operand == B_contact:
                                        status = "NG"
                                        ng_name = ng_content
                                        break

                                    if series_contact_operand == "PWR_ON":
                                        status = "OK"
                                        ng_name = ""
                                        break

                                    if (
                                        isinstance(series_contact_operand, str)
                                        and series_contact_operand
                                    ):
                                        comment = get_the_comment_from_program(
                                            series_contact_operand,
                                            program,
                                            program_comment_data,
                                        )
                                        if (
                                            regex_pattern_check(
                                                communication_comment, comment
                                            )
                                            or regex_pattern_check(
                                                network_comment, comment
                                            )
                                        ) and regex_pattern_check(
                                            normal_comment, comment
                                        ):
                                            status = "OK"
                                            ng_name = ""
                                            break

                            if status == "OK":
                                break

                        rung_number = (
                            v.get("rung_number") - 1
                            if v.get("rung_number") != -1
                            else -1
                        )
                        output_rows.append(
                            {
                                "Result": status,
                                "Task": program,
                                "Section": "DeviceIn",
                                "RungNo": rung_number,
                                "Target": T_coil,
                                "CheckItem": rule_71_check_item,
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
        logger.error(f"Rule 71 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}


# ============================== Function-Wise Execution Starts Here ===============================
def execute_rule_71_functionwise(
    input_function_file: str, function_comment_file: str
) -> pd.DataFrame:

    logger.info("Rule 71 Start executing rule 1 function wise")

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
                if detection_results:
                    for k, v in detection_results.items():
                        T_coil = v["T_coil"]
                        B_contact = v["B_contact"]
                        status = "NG"
                        ng_name = ng_content
                        match_rung_df = device_in_function_df[
                            device_in_function_df["RUNG"] == v["rung_number"]
                        ]
                        get_series_connect_data = get_series_contacts(
                            pl.from_pandas(match_rung_df)
                        )
                        series_contact_operands_only = [
                            [item.get("operand") for item in sublist]
                            for sublist in get_series_connect_data
                        ]
                        for series_contact in series_contact_operands_only:
                            both_coil_contact_present = B_contact in series_contact
                            if both_coil_contact_present:
                                for series_contact_operand in series_contact:
                                    if series_contact_operand == B_contact:
                                        status = "NG"
                                        ng_name = ng_content
                                        break

                                    if series_contact_operand == "PWR_ON":
                                        status = "OK"
                                        ng_name = ""
                                        break

                                    if (
                                        isinstance(series_contact_operand, str)
                                        and series_contact_operand
                                    ):
                                        comment = get_the_comment_from_function(
                                            series_contact_operand,
                                            function,
                                            function_comment_data,
                                        )
                                        if (
                                            regex_pattern_check(
                                                communication_comment, comment
                                            )
                                            or regex_pattern_check(
                                                network_comment, comment
                                            )
                                        ) and regex_pattern_check(
                                            normal_comment, comment
                                        ):
                                            status = "OK"
                                            ng_name = ""
                                            break

                            if status == "OK":
                                break

                        rung_number = (
                            v.get("rung_number") - 1
                            if v.get("rung_number") != -1
                            else -1
                        )
                        output_rows.append(
                            {
                                "Result": status,
                                "Task": function,
                                "Section": "DeviceIn",
                                "RungNo": rung_number,
                                "Target": T_coil,
                                "CheckItem": rule_71_check_item,
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
        logger.error(f"Rule 71 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}
