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

# ============================ Rule 63: Definitions, Content, and Configuration Details ============================
rule_content_63 = (
    "・Abnormalities such as cycle stop or abnormal stop have not occurred."
)

rule_63_check_item = "Rule of Auto Start Condition Circuit"
hmi_out_section_name = "hmi_out"

auto_comment = "自動"
start_comment = "起動"
condition_comment = "条件"
abnormal_comment = "異常"
not_comment = "でない"
fault_comment = "異常"

check_detail_content = {
    "cc1": "If ① but not ③, it is assumed to be NG.",
    "cc2": 'Confirm that only one A contact containing ”異常(abnormal)”+"でない(not)" are connected to the outcoil conditions detected in ③.',
}

ng_content = {
    "cc1": "自動起動条件の1つである”異常でない”のコイルがないためNG",
    "cc2": "自動起動条件の1つである”異常でない”のコイルの条件がないためNG",
}

# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================


def detection_range_programwise(
    hmi_out_section_df: pd.DataFrame,
    program_name: str,
    program_comment_data: dict,
    auto_comment: str,
    start_comment: str,
    condition_comment: str,
    fault_comment: str,
    not_comment: str,
) -> dict:

    logger.info(
        f"Executing rule 63 detection range on program {program_name} and section name {hmi_out_section_name} on rule 63"
    )

    # Filter only 'Contact' rows
    coil_df = hmi_out_section_df[
        hmi_out_section_df["OBJECT_TYPE_LIST"].str.lower() == "coil"
    ].copy()

    match_coil = {}
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
                    if (
                        regex_pattern_check(auto_comment, coil_comment)
                        and regex_pattern_check(start_comment, coil_comment)
                        and regex_pattern_check(condition_comment, coil_comment)
                        and regex_pattern_check(fault_comment, coil_comment)
                        and regex_pattern_check(not_comment, coil_comment)
                    ):
                        match_coil["coil_data"] = [coil_operand, coil_row["RUNG"]]
                        break

    return match_coil


def check_detail_1_programwise(detection_result: dict, program_name: str) -> dict:

    logger.info(f"Executing rule no 63 check detail 1 in program {program_name}")

    status = "NG"
    rung_number = -1
    coil_operand = ""
    if detection_result:
        status = "OK"
        coil_operand = detection_result.get("coil_data")[0]
        rung_number = detection_result.get("coil_data")[1]

    return {
        "status": status,
        "rung_number": rung_number,
        "target_coil": coil_operand,
        "cc": "cc1",
    }


def check_detail_2_programwise(
    hmi_out_section_df: pd.DataFrame,
    detection_result: dict,
    program_name: str,
    program_comment_data: dict,
    abnormal_comment: str,
    not_comment: str,
) -> dict:

    logger.info(f"Executing rule no 63 check detail 1 in program {program_name}")

    status = "NG"
    rung_number = -1
    operand = ""
    if detection_result:
        rung_number = detection_result.get("coil_data")[1]
        current_rung_number_df = hmi_out_section_df[
            hmi_out_section_df["RUNG"] == rung_number
        ]
        contact_df = current_rung_number_df[
            current_rung_number_df["OBJECT_TYPE_LIST"].str.lower() == "contact"
        ].copy()

        print("contact_df", contact_df)
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
                            regex_pattern_check(abnormal_comment, contact_comment)
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
        "cc": "cc2",
    }


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_63_programwise(
    input_program_file: str, program_comment_file
) -> pd.DataFrame:

    logger.info("Starting execution of Rule 63")

    try:

        program_df = pd.read_csv(input_program_file)

        with open(program_comment_file, "r", encoding="utf-8") as file:
            program_comment_data = json.load(file)

        unique_program_values = program_df["PROGRAM"].unique()

        output_rows = []
        for program_name in unique_program_values:

            logger.info(f"Executing rule 63 in Program {program_name}")

            if "main" in program_name.lower():

                current_program_df = program_df[program_df["PROGRAM"] == program_name]
                hmi_out_section_df = current_program_df[
                    current_program_df["BODY"].str.lower() == hmi_out_section_name
                ]

                # Run detection range logic as per Rule 24
                detection_result = detection_range_programwise(
                    hmi_out_section_df=hmi_out_section_df,
                    program_name=program_name,
                    program_comment_data=program_comment_data,
                    auto_comment=auto_comment,
                    start_comment=start_comment,
                    condition_comment=condition_comment,
                    fault_comment=fault_comment,
                    not_comment=not_comment,
                )

                cc1_result = check_detail_1_programwise(
                    detection_result=detection_result, program_name=program_name
                )

                cc2_result = check_detail_2_programwise(
                    hmi_out_section_df=hmi_out_section_df,
                    detection_result=detection_result,
                    program_name=program_name,
                    program_comment_data=program_comment_data,
                    abnormal_comment=abnormal_comment,
                    not_comment=not_comment,
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
                            "Task": program_name,
                            "Section": "AutoMain",
                            "RungNo": rung_number,
                            "Target": target_outcoil,
                            "CheckItem": rule_63_check_item,
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
        logger.error(f"Rule 63 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}
