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

# from .rule_76_ladder_utils import get_series_contacts


# ============================ Rule 76: Definitions, Content, and Configuration Details ============================
cycle_comment = "サイクル"
over_1_comment = "オーバ"
over_2_comment = "オーバー"
start_comment = "開始"

fault_section_name = "fault"
rule_76_check_item = "Rule of Fault Stop Detection Circuit"
check_detail_content = {
    "cc1": "If ① exists but ③ does not, it is NG.",
    "cc2": 'Check that the A contact connected to the outcoil condition detected in ③ includes the variables ”サイクル(cycle)”+"開始(start)" in its comment.',
    "cc3": "Check that the “TON” function is connected to the outcoil condition detected in ③.",
    "cc4": "Check that only components ❷ and ❸ are connected in series to the outcoil condition detected in ③.",
}


ng_content = {
    "cc1": "サイクルタイムオーバーのアラームが存在しないためNG",
    "cc2": "サイクルタイムオーバーのアラームの条件にサイクル開始が接続されていないためNG",
    "cc3": "サイクルタイムオーバーのアラームの条件にタイマが接続されていないためNG",
    "cc4": "サイクルタイムオーバーのアラームの条件がサイクル開始とタイマの直列回路で構成されていないためNG",
}


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
    fault_program_df: pd.DataFrame,
    cycle_comment: str,
    over_1_comment: str,
    over_2_comment: str,
    program_name: str,
    program_comment_data: dict,
):

    status = "NG"
    rung_number = -1
    target_coil = ""

    if not fault_program_df.empty:

        coil_df = fault_program_df[
            fault_program_df["OBJECT_TYPE_LIST"].str.lower() == "coil"
        ]

        if not coil_df.empty:
            for _, coil_row in coil_df.iterrows():
                attr = ast.literal_eval(coil_row["ATTRIBUTES"])
                coil_operand = attr.get("operand")

                if coil_operand and isinstance(coil_operand, str):
                    coil_comment = get_the_comment_from_program(
                        coil_operand, program_name, program_comment_data
                    )
                    if (
                        coil_comment
                        and isinstance(coil_comment, list)
                        and "AL" in coil_operand
                    ):

                        # print("coil_comment", coil_comment, coil_operand)
                        if regex_pattern_check(cycle_comment, coil_comment) and (
                            regex_pattern_check(over_1_comment, coil_comment)
                            or regex_pattern_check(over_2_comment, coil_comment)
                        ):
                            status = "OK"
                            rung_number = clean_rung_number(coil_row["RUNG"])
                            target_coil = coil_operand
                            break

    return {
        "status": status,
        "cc": "cc1",
        "check_number": 1,
        "rung_number": rung_number,
        "target_coil": target_coil,
    }


def check_detail_1_programwise(detection_result: dict, program_name: str) -> dict:

    logger.info(f"Executing rule no 76 check detail 1 in program {program_name}")

    status = "NG"
    if detection_result.get("status") == "OK":
        status = "OK"

    return {"status": status, "rung_number": -1, "target_coil": "", "cc": "cc1"}


def check_detail_2_programwise(
    fault_program_df: pd.DataFrame,
    detection_result: dict,
    cycle_comment: str,
    start_comment: str,
    program_name: str,
    program_comment_data: dict,
) -> dict:

    status = "NG"
    rung_number = -1
    target_coil = ""

    if detection_result.get("status") == "OK":
        rung_number = detection_result.get("rung_number")

        if isinstance(rung_number, (str, int)) and rung_number != -1:
            contact_rung_df = fault_program_df[
                (fault_program_df["OBJECT_TYPE_LIST"].str.lower() == "contact")
                & (fault_program_df["RUNG"] == rung_number)
            ]

            print("contact_rung_df", contact_rung_df)
            if not contact_rung_df.empty:
                for _, contact_rung_row in contact_rung_df.iterrows():
                    attr = ast.literal_eval(contact_rung_row["ATTRIBUTES"])
                    contact_operand = attr.get("operand")

                    if contact_operand and isinstance(contact_operand, str):
                        contact_comment = get_the_comment_from_program(
                            contact_operand, program_name, program_comment_data
                        )
                        print("contact_comment", contact_comment, contact_operand)
                        if contact_comment and isinstance(contact_comment, list):
                            if regex_pattern_check(
                                cycle_comment, contact_comment
                            ) and regex_pattern_check(start_comment, contact_comment):
                                status = "OK"
                                target_coil = contact_operand
                                break

    return {
        "status": status,
        "rung_number": rung_number,
        "target_coil": target_coil,
        "cc": "cc2",
    }


def check_detail_3_programwise(
    fault_program_df: pd.DataFrame, detection_result: dict
) -> dict:

    status = "NG"
    rung_number = -1
    target_coil = ""

    if detection_result.get("status") == "OK":
        rung_number = detection_result.get("rung_number")

        if isinstance(rung_number, (str, int)) and rung_number != -1:
            block_rung_df = fault_program_df[
                (fault_program_df["OBJECT_TYPE_LIST"].str.lower() == "block")
                & (fault_program_df["RUNG"] == rung_number)
            ]

            if not block_rung_df.empty:
                for _, block_rung_row in block_rung_df.iterrows():
                    attr = ast.literal_eval(block_rung_row["ATTRIBUTES"])

                    type_name = attr.get("typeName")

                    if type_name and isinstance(type_name, str):
                        if type_name.lower() == "ton":
                            status = "OK"
                            target_coil = type_name
                            break

    return {
        "status": status,
        "rung_number": rung_number,
        "target_coil": target_coil,
        "cc": "cc3",
    }


def check_detail_4_programwise(
    fault_program_df: pd.DataFrame, cc2_result: dict, cc3_result: dict
):

    status = "NG"
    rung_number = -1
    target_coil = ""

    if cc2_result.get("status") == "OK" and cc3_result.get("status") == "OK":

        contact_block_df = fault_program_df[
            (
                fault_program_df["OBJECT_TYPE_LIST"]
                .str.lower()
                .isin(["contact", "block"])
            )
            & (fault_program_df["RUNG"] == cc2_result.get("rung_number"))
        ]

        contact_outlist = []
        block_inlist = []
        check_contact_exist_first = False
        if len(contact_block_df) == 2:
            index = 0
            for _, contact_block_row in contact_block_df.iterrows():

                attr = ast.literal_eval(contact_block_row["ATTRIBUTES"])
                if index == 0:
                    check_contact_exist_first = (
                        contact_block_row.get("OBJECT_TYPE_LIST").lower() == "contact"
                    )
                    index += 1

                if check_contact_exist_first:
                    if contact_block_row.get("OBJECT_TYPE_LIST").lower() == "contact":
                        contact_outlist.append(attr["out_list"])
                    elif contact_block_row.get("OBJECT_TYPE_LIST").lower() == "block":
                        block_inlist.append(attr["In_inVar_in_list"])

            for contact_val in contact_outlist:
                if contact_val in block_inlist:
                    status = "OK"
                    rung_number = cc2_result.get("rung_number")
                    break
    return {
        "status": status,
        "rung_number": rung_number,
        "target_coil": target_coil,
        "cc": "cc4",
    }


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_76_programwise(
    input_program_file: str, program_comment_file: str
) -> pd.DataFrame:

    logger.info("Rule 76 Start executing rule 1 program wise")

    try:

        output_rows = []

        program_df = pd.read_csv(input_program_file)
        with open(program_comment_file, "r", encoding="utf-8") as file:
            program_comment_data = json.load(file)

        unique_program_values = program_df["PROGRAM"].unique()
        for program in unique_program_values:
            is_task_name_in_range = is_allowed_task(program)
            if is_task_name_in_range:
                curr_program_df = program_df[program_df["PROGRAM"] == program]
                fault_program_df = curr_program_df[
                    curr_program_df["BODY"].str.lower() == fault_section_name
                ]

                detection_results = detection_range_programwise(
                    fault_program_df=fault_program_df,
                    cycle_comment=cycle_comment,
                    over_1_comment=over_1_comment,
                    over_2_comment=over_2_comment,
                    program_name=program,
                    program_comment_data=program_comment_data,
                )

                cc1_result = check_detail_1_programwise(
                    detection_result=detection_results, program_name=program
                )

                cc2_result = check_detail_2_programwise(
                    fault_program_df=fault_program_df,
                    detection_result=detection_results,
                    cycle_comment=cycle_comment,
                    start_comment=start_comment,
                    program_name=program,
                    program_comment_data=program_comment_data,
                )

                cc3_result = check_detail_3_programwise(
                    fault_program_df=fault_program_df,
                    detection_result=detection_results,
                )

                cc4_result = check_detail_4_programwise(
                    fault_program_df=fault_program_df,
                    cc2_result=cc2_result,
                    cc3_result=cc3_result,
                )

                print(detection_results, cc1_result, cc2_result, cc3_result, cc4_result)

                for cc_result in [cc1_result, cc2_result, cc3_result, cc4_result]:
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
                            "CheckItem": rule_76_check_item,
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
        logger.error(f"Rule 76 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}
