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
from .rule_80_ladder_utils import get_series_contacts

# ============================ Rule 80: Definitions, Content, and Configuration Details ============================

condition_section_name = "condition"
rule_80_check_item = "Rule of Start Condition Circuit"

cycle_comment = "サイクル"
stop_comment = "停止"
start_comment = "起動"
condition_comment = "条件"
auxiliary_comment = "補助"

check_detail_content = {
    "cc1": "If ① exists but ③ does not, it is NG.",
    "cc2": " Check that the B contact detected in step ③ is connected in series to an outcoil that contains the variable comment ”起動(start)+'条件(condition)” and does not contain ”補助(auxiliary)”. If not, it is NG.",
}

ng_content = {
    "cc1": "各タスクの起動条件にサイクル停止のB接点が存在しないためNG",
    "cc2": "各タスクの起動条件にサイクル停止のB接点が接続されていないためNG",
}


def is_allowed_task(name: str) -> bool:
    """
    Returns True if task name is allowed,
    False if it starts with P000-P101 (case-insensitive).
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
    condition_section_df: pd.DataFrame,
    program: str,
    program_comment_data: dict,
    cycle_comment: str,
    stop_comment: str,
):

    match_outcoil = []
    if not condition_section_df.empty:
        contact_df = condition_section_df[
            condition_section_df["OBJECT_TYPE_LIST"].str.lower() == "contact"
        ]
        if not contact_df.empty:
            for _, contact_row in contact_df.iterrows():
                attr = ast.literal_eval(contact_row["ATTRIBUTES"])
                contact_operand = attr.get("operand")
                negated_operand = attr.get("negated")
                if isinstance(contact_operand, str) and contact_operand:
                    contact_comment = get_the_comment_from_program(
                        contact_operand, program, program_comment_data
                    )
                    if isinstance(contact_comment, list) and contact_comment:
                        if (
                            regex_pattern_check(cycle_comment, contact_comment)
                            and regex_pattern_check(stop_comment, contact_comment)
                            and negated_operand == "true"
                        ):
                            match_outcoil.append([contact_operand, contact_row["RUNG"]])

    return match_outcoil


def check_detail_1_programwise(detection_result: dict, program: str):

    status = "NG"
    rung_number = -1
    target_coil = ""
    if detection_result:
        status = "OK"
        rung_number = detection_result[0][1]
        target_coil = detection_result[0][0]

    return {
        "status": status,
        "cc": "cc1",
        "rung_number": rung_number,
        "target_coil": target_coil,
        "check_number": 1,
    }


def check_detail_2_programwise(
    condition_section_df: pd.DataFrame,
    detection_result: list,
    program: str,
    program_comment_data: dict,
    start_comment: str,
    condition_comment: str,
    auxiliary_comment: str,
):
    status = "NG"
    rung_number = -1
    target_coil = ""

    for detected_contact in detection_result:
        contact_operand = detected_contact[0]
        contact_rung = detected_contact[1]
        if not condition_section_df.empty:
            coil_current_rung_df = condition_section_df[
                (condition_section_df["OBJECT_TYPE_LIST"].str.lower() == "coil")
                & (condition_section_df["RUNG"] == contact_rung)
            ]
            current_rung_df = condition_section_df[
                condition_section_df["RUNG"] == contact_rung
            ]
            if not coil_current_rung_df.empty:
                for _, coil_row in coil_current_rung_df.iterrows():
                    attr = ast.literal_eval(coil_row["ATTRIBUTES"])
                    coil_operand = attr.get("operand")
                    if isinstance(coil_operand, str) and coil_operand:
                        coil_comment = get_the_comment_from_program(
                            coil_operand, program, program_comment_data
                        )
                        if isinstance(coil_comment, list) and coil_comment:
                            if (
                                regex_pattern_check(start_comment, coil_comment)
                                and regex_pattern_check(condition_comment, coil_comment)
                                and not regex_pattern_check(
                                    auxiliary_comment, coil_comment
                                )
                            ):
                                get_series_connect_data = get_series_contacts(
                                    pl.from_pandas(current_rung_df)
                                )
                                series_contact_operands_only = [
                                    [item.get("operand") for item in sublist]
                                    for sublist in get_series_connect_data
                                ]
                                for series_contact in series_contact_operands_only:
                                    if contact_operand in series_contact:
                                        status = "OK"
                                        rung_number = coil_row["RUNG"]
                                        target_coil = coil_operand
                                        break

                            if status == "OK":
                                break

            if status == "OK":
                break

    return {
        "status": status,
        "cc": "cc1",
        "rung_number": rung_number,
        "target_coil": target_coil,
        "check_number": 1,
    }


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_80_programwise(
    input_program_file: str,
    program_comment_file: str,
) -> pd.DataFrame:

    logger.info("Rule 80 Start executing rule 1 program wise")

    output_rows = []

    try:

        program_df = pd.read_csv(input_program_file)
        with open(program_comment_file, "r", encoding="utf-8") as file:
            program_comment_data = json.load(file)

        unique_program_values = program_df["PROGRAM"].unique()

        for program in unique_program_values:

            logger.info(f"Executing Rule 80 in program {program}")

            is_task_name_in_range = is_allowed_task(program)

            if is_task_name_in_range:
                current_program_df = program_df[program_df["PROGRAM"] == program]
                condition_section_df = current_program_df[
                    current_program_df["BODY"].str.lower() == condition_section_name
                ]

                detection_result = detection_range_programwise(
                    condition_section_df=condition_section_df,
                    program=program,
                    program_comment_data=program_comment_data,
                    cycle_comment=cycle_comment,
                    stop_comment=stop_comment,
                )

                cc1_result = check_detail_1_programwise(
                    detection_result=detection_result, program=program
                )

                cc2_result = check_detail_2_programwise(
                    condition_section_df=condition_section_df,
                    detection_result=detection_result,
                    program=program,
                    program_comment_data=program_comment_data,
                    start_comment=start_comment,
                    condition_comment=condition_comment,
                    auxiliary_comment=auxiliary_comment,
                )

                for cc_result in [cc1_result, cc2_result]:
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
                            "Task": program,
                            "Section": "Fault",
                            "RungNo": rung_number,
                            "Target": target_outcoil,
                            "CheckItem": rule_80_check_item,
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
        logger.error(f"Rule 16 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}
