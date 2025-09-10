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
from .rule_34_ladder_utils import check_self_holding

# ============================ Rule 34: Definitions, Content, and Configuration Details ============================

fault_section_name = "fault"
rule_34_check_item = "Rule of Continuous Defect Detection Circuit"

consecutive_comment = "連続"
fault_comment = "不良"

check_detail_content = {
    "cc1": " If ① but not ③, it is assumed to be NG.",
    "cc2": "Check that the “CTD” function is connected within the self-holding condition of the outcoil detected in ③.  (Pattern 1) If ‘CTD’ is not connected, check the condition of the A contact that exists within the condition, and if the “CTD” function is connected within that condition, it is OK. (Pattern 2)*Detection range is within the same task.",
}

ng_content = {
    "cc1": "加工や検査工程において連続不良の検出が実施できていないためNG",
    "cc2": "連続不良アラームの条件にカウンターが検出されなかっためNG",
}


# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================
def detection_range_programwise(
    fault_section_df: pd.DataFrame,
    program: str,
    program_comment_data: dict,
    consecutive_comment: str,
    fault_comment: str,
):

    status = "NG"
    rung_number = -1
    target_coil = ""
    if not fault_section_df.empty:
        coil_df = fault_section_df[
            fault_section_df["OBJECT_TYPE_LIST"].str.lower() == "coil"
        ]
        if not coil_df.empty:
            for _, coil_row in coil_df.iterrows():
                attr = ast.literal_eval(coil_row["ATTRIBUTES"])
                coil_operand = attr.get("operand")

                if (
                    isinstance(coil_operand, str)
                    and coil_operand
                    and "AL" in coil_operand
                ):
                    coil_comment = get_the_comment_from_program(
                        coil_operand, program, program_comment_data
                    )
                    if isinstance(coil_comment, list) and coil_comment:
                        if regex_pattern_check(consecutive_comment, coil_comment) and (
                            regex_pattern_check(fault_comment, coil_comment)
                            or regex_pattern_check("NG", coil_comment)
                        ):
                            status = "OK"
                            rung_number = coil_row["RUNG"]
                            target_coil = coil_operand
                            break

    return {"status": status, "rung_number": rung_number, "target_coil": target_coil}


def check_detail_1_programwise(detection_result: dict, program: str):

    logger.info(f"Executing rule no 34 check detail 1 in program {program}")

    status = "NG"
    rung_number = -1
    target_coil = ""
    if detection_result.get("status") == "OK":
        status = "OK"
        rung_number = detection_result.get("rung_number")
        target_coil = detection_result.get("target_coil")

    return {
        "status": status,
        "rung_number": rung_number,
        "target_coil": target_coil,
        "cc": "cc1",
    }


def checking_first_pattern(current_rung_df: pd.DataFrame, coil_operand: str):

    all_self_holding_contact = check_self_holding(current_rung_df)
    if coil_operand in all_self_holding_contact:
        block_df = current_rung_df[
            current_rung_df["OBJECT_TYPE_LIST"].str.lower() == "block"
        ]
        for _, block_row in block_df.iterrows():
            attr = ast.literal_eval(block_row["ATTRIBUTES"])
            block_operand = attr.get("typeName")

            if (
                isinstance(block_operand, str)
                and block_operand
                and "ctd" in block_operand.lower()
            ):
                return True, block_row["RUNG"]

    return False, -1


def checking_second_pattern(
    current_program_df: pd.DataFrame, current_rung_df: pd.DataFrame
):

    if not current_program_df.empty:
        all_contact = []
        contact_df = current_rung_df[
            current_rung_df["OBJECT_TYPE_LIST"].str.lower() == "contact"
        ]
        for _, contact_row in contact_df.iterrows():
            contact_attr = ast.literal_eval(contact_row["ATTRIBUTES"])
            contact_operand = contact_attr.get("operand")
            all_contact.append(contact_operand)

        coil_current_program_df = current_program_df[
            current_program_df["OBJECT_TYPE_LIST"].str.lower() == "coil"
        ]
        for _, coil_current_program_row in coil_current_program_df.iterrows():
            coil_current_attr = ast.literal_eval(contact_row["ATTRIBUTES"])
            coil_current_df_operand = coil_current_attr.get("operand")

            if (
                isinstance(coil_current_df_operand, str)
                and coil_current_df_operand
                and coil_current_df_operand in all_contact
            ):
                match_contact_as_coil_block_df = current_program_df[
                    (current_program_df["BODY"] == coil_current_program_row["BODY"])
                    & (current_program_df["RUNG"] == coil_current_program_row["RUNG"])
                    & (current_program_df["OBJECT_TYPE_LIST"].str.lower() == "block")
                ]

                if not match_contact_as_coil_block_df.empty:
                    for (
                        _,
                        match_contact_as_coil_block_row,
                    ) in match_contact_as_coil_block_df.iterrows():
                        attr = ast.literal_eval(
                            match_contact_as_coil_block_row["ATTRIBUTES"]
                        )
                        block_operand = attr.get("typeName")

                        if (
                            isinstance(block_operand, str)
                            and block_operand
                            and "ctd" in block_operand.lower()
                        ):
                            return True, match_contact_as_coil_block_row["RUNG"]

    return False, -1


def check_detail_2_programwise(
    current_program_df: pd.DataFrame,
    fault_section_df: pd.DataFrame,
    detection_result: dict,
    program: str,
    program_comment_data: dict,
    consecutive_comment: str,
    fault_comment: str,
):
    status = "NG"
    rung_number = -1
    target_coil = ""
    if not fault_section_df.empty and detection_result.get("status") == "OK":
        coil_df = fault_section_df[
            fault_section_df["OBJECT_TYPE_LIST"].str.lower() == "coil"
        ]
        if not coil_df.empty:
            for _, coil_row in coil_df.iterrows():
                attr = ast.literal_eval(coil_row["ATTRIBUTES"])
                coil_operand = attr.get("operand")

                if (
                    isinstance(coil_operand, str)
                    and coil_operand
                    and "AL" in coil_operand
                ):
                    coil_comment = get_the_comment_from_program(
                        coil_operand, program, program_comment_data
                    )
                    if isinstance(coil_comment, list) and coil_comment:
                        if regex_pattern_check(consecutive_comment, coil_comment) and (
                            regex_pattern_check(fault_comment, coil_comment)
                            or regex_pattern_check("NG", coil_comment)
                        ):
                            current_rung_df = fault_section_df[
                                fault_section_df["RUNG"] == coil_row["RUNG"]
                            ]

                            """
                            if first condition not met then we have to check second case
                            this code block is for checking first condition of selfholding and then CTD block
                            """
                            first_pattern_status, rung_number = checking_first_pattern(
                                current_rung_df=current_rung_df,
                                coil_operand=coil_operand,
                            )

                            second_pattern_status = False
                            if not first_pattern_status:
                                second_pattern_status, rung_number = (
                                    checking_second_pattern(
                                        current_program_df=current_program_df,
                                        current_rung_df=current_rung_df,
                                    )
                                )

                            if first_pattern_status or second_pattern_status:
                                return {
                                    "status": "OK",
                                    "rung_number": rung_number,
                                    "target_coil": coil_operand,
                                    "cc": "cc2",
                                }

    return {"status": "NG", "rung_number": -1, "target_coil": "", "cc": "cc2"}


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_34_programwise(
    input_program_file: str, program_comment_file: str, input_image: str
) -> pd.DataFrame:

    logger.info("Rule 34 Start executing rule 1 program wise")

    output_rows = []

    try:

        program_df = pd.read_csv(input_program_file)
        input_image_program_df = pd.read_csv(input_image)
        with open(program_comment_file, "r", encoding="utf-8") as file:
            program_comment_data = json.load(file)

        task_names = (
            input_image_program_df[
                input_image_program_df["Process type"]
                .astype(str)
                .str.lower()
                .isin(["processing", "inspection"])
            ]["Task name"]
            .astype(str)
            .str.lower()
            .tolist()
        )
        unique_program_values = program_df["PROGRAM"].unique()
        for program in unique_program_values:

            logger.info(f"Executing Rule 34 in program {program}")

            if program.lower() in task_names:
                current_program_df = program_df[program_df["PROGRAM"] == program]
                fault_section_df = current_program_df[
                    current_program_df["BODY"].str.lower() == fault_section_name
                ]

                detection_result = detection_range_programwise(
                    fault_section_df=fault_section_df,
                    program=program,
                    program_comment_data=program_comment_data,
                    consecutive_comment=consecutive_comment,
                    fault_comment=fault_comment,
                )

                print("detection_result", detection_result)
                cc1_result = check_detail_1_programwise(
                    detection_result=detection_result, program=program
                )

                cc2_result = check_detail_2_programwise(
                    current_program_df=current_program_df,
                    fault_section_df=fault_section_df,
                    detection_result=detection_result,
                    program=program,
                    program_comment_data=program_comment_data,
                    consecutive_comment=consecutive_comment,
                    fault_comment=fault_comment,
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
                            "CheckItem": rule_34_check_item,
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
        logger.error(f"Rule 16 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}
