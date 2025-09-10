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

# ============================================ Comments referenced in Rule 20 processing ============================================
# memory feed Complete （記憶送り完了）/memory feed timing （記憶送りタイミング）/ memory shift timing （記憶シフトタイミング）/ memory shift Complete （記憶シフト完了）
section_name = "MemoryFeeding"
operation_comment = "動作"
complete_comment = "完了"
memory_comment = "記憶"
transport_comment = "搬送"
reset_full_comment = "リセット"
reset_half_comment = "ﾘｾｯﾄ"
timing_comment = "タイミング"
timing_half_comment = "ﾀｲﾐﾝｸﾞ"

# ============================ Rule 20: Definitions, Content, and Configuration Details ============================
rule_content_20 = "・「Operation Complete memory」 of each assembled process shall be reset by「Operation Complete reset timing」 from Conveying process."
rule_20_check_item = "Rule of Memoryfeeding"

check_detail_content = {
    "cc1": "When ③ is not found in the detection target in ①, it is set to NG.",
    "cc2": "Confirm that there is an A contact whose variable comments include ”搬送(transport) orリセット(reset)"
    and "タイミング(timing)” in the out-coil condition detected in ③.",
}

ng_content = {
    "cc1": "加工もしくは検査工程であるのに、動作完了記憶がない(No memory of operation completion even though it is a processing or inspection process.)",
    "cc2": "加工もしくは検査工程であるのに、動作完了記憶の条件が正しくない(Incorrect condition for operation completion memory, even though it is a processing or inspection process.)",
}


# ============================ Helper Functions for Program-Wise Operations ============================
def check_operation_complete_memory_outcoil_from_program(
    row,
    program_name: str,
    operation_comment: str,
    complete_comment: str,
    memory_comment: str,
    program_comment_data: dict,
):
    try:
        attr = ast.literal_eval(row["ATTRIBUTES"])
        if isinstance(attr, dict) and "operand" in attr and "latch" in attr:
            if attr.get("latch") == "reset":
                comment = get_the_comment_from_program(
                    attr.get("operand"), program_name, program_comment_data
                )
                if isinstance(comment, list):
                    if (
                        regex_pattern_check(operation_comment, comment)
                        and regex_pattern_check(complete_comment, comment)
                        and regex_pattern_check(memory_comment, comment)
                    ):
                        return attr.get("operand")
    except Exception:
        return None
    return None


# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================


def extract_rung_group_data_programwise(
    program_df: pd.DataFrame, program_name: str, section_name: str
) -> pd.DataFrame:
    logger.info(
        f"Group Rung and filter data on program {program_name} and section name {section_name}"
    )
    program_rows = program_df[program_df["PROGRAM"] == program_name].copy()
    memory_feeding_section_rows = program_rows[program_rows["BODY"] == section_name]
    rung_groups_df = memory_feeding_section_rows.groupby("RUNG")
    return rung_groups_df


def detection_range_programwise(
    memory_feeding_rung_groups_df: pd.DataFrame,
    operation_comment: str,
    complete_comment: str,
    memory_comment: str,
    program_name: str,
    program_comment_data: str,
    section_name: str,
) -> dict:

    logger.info(
        f"Executing detection range on program {program_name} and section name {section_name} on rule 20"
    )

    operation_complete_memory_comment_operand = ""
    operation_complete_memory_comment_rung_number = -1

    for _, rung_df in memory_feeding_rung_groups_df:

        rung_df["operation_complete_memory_reset_check_outcoil"] = rung_df.apply(
            lambda row: (
                check_operation_complete_memory_outcoil_from_program(
                    row=row,
                    program_name=program_name,
                    operation_comment=operation_comment,
                    complete_comment=complete_comment,
                    memory_comment=memory_comment,
                    program_comment_data=program_comment_data,
                )
                if row["OBJECT_TYPE_LIST"] == "Coil"
                else None
            ),
            axis=1,
        )

        operation_complete_memory_match_outcoil = rung_df[
            rung_df["operation_complete_memory_reset_check_outcoil"].notna()
        ]
        if not operation_complete_memory_match_outcoil.empty:
            operation_complete_memory_comment_operand = (
                operation_complete_memory_match_outcoil.iloc[0][
                    "operation_complete_memory_reset_check_outcoil"
                ]
            )
            operation_complete_memory_comment_rung_number = (
                operation_complete_memory_match_outcoil.iloc[0]["RUNG"]
            )

        if operation_complete_memory_comment_operand:
            return {
                "operation_complete_memory": [
                    operation_complete_memory_comment_operand,
                    operation_complete_memory_comment_rung_number,
                ],
            }

    return {
        "operation_complete_memory": [
            operation_complete_memory_comment_operand,
            operation_complete_memory_comment_rung_number,
        ],
    }


def check_detail_1_programwise(detection_result: dict) -> dict:

    logger.info(f"Executing check detail 1 for rule no 20")

    cc1_result = {}
    outcoil = []

    cc1_result["status"] = (
        "NG" if detection_result["operation_complete_memory"][1] == -1 else "OK"
    )

    outcoil = detection_result["operation_complete_memory"][0]

    cc1_result["cc"] = "cc1"
    cc1_result["outcoil"] = outcoil
    cc1_result["rung_number"] = detection_result["operation_complete_memory"][1]

    return cc1_result


def check_detail_2_programwise(
    program_name: str,
    program_comment_data: str,
    memory_feeding_section_df: pd.DataFrame,
    transport_comment: str,
    reset_full_comment: str,
    timing_comment: str,
    reset_operation_complete_memory_operand: str,
    reset_operation_complete_memory_rung_number: str,
):
    if reset_operation_complete_memory_rung_number != -1:

        transort_reset_timing_contact_operand = ""

        reset_operation_complete_memory_rung_df = memory_feeding_section_df[
            memory_feeding_section_df["RUNG"]
            == reset_operation_complete_memory_rung_number
        ]
        for _, rung_row in reset_operation_complete_memory_rung_df.iterrows():
            attr = ast.literal_eval(rung_row["ATTRIBUTES"])
            contact_operand = attr.get("operand")
            if contact_operand and isinstance(contact_operand, str):
                contact_comment = get_the_comment_from_program(
                    contact_operand, program_name, program_comment_data
                )
            else:
                contact_comment = []
            if isinstance(contact_comment, list) and contact_comment:
                if (
                    rung_row["OBJECT_TYPE_LIST"] == "Contact"
                    and attr.get("negated") == "false"
                    and (
                        regex_pattern_check(transport_comment, contact_comment)
                        or regex_pattern_check(reset_full_comment, contact_comment)
                    )
                    and regex_pattern_check(timing_comment, contact_comment)
                ):
                    transort_reset_timing_contact_operand = contact_operand
                    break

        if transort_reset_timing_contact_operand:
            return {
                "cc": "cc2",
                "status": "OK",
                "outcoil": transort_reset_timing_contact_operand,
                "rung_number": reset_operation_complete_memory_rung_number,
            }

    # return "NG", [None, None]
    return {"cc": "cc2", "status": "NG", "outcoil": None, "rung_number": -1}


def store_program_csv_results_programwise(
    output_rows: List,
    all_cc_status: List,
    program_name: str,
    section_name: str,
    ng_content: dict,
    check_detail_content: str,
) -> List:

    logger.info(f"Storing all result in output csv file")

    for index, cc_status in enumerate(all_cc_status):
        ng_name = (
            ng_content.get(cc_status.get("cc", ""))
            if cc_status.get("status") == "NG"
            else ""
        )
        rung_number = (
            cc_status.get("rung_number") - 1 if cc_status.get("rung_number") else -1
        )
        target_outcoil = cc_status.get("outcoil") if cc_status.get("outcoil") else ""

        output_rows.append(
            {
                "Result": cc_status.get("status"),
                "Task": program_name,
                "Section": section_name,
                "RungNo": rung_number,
                "Target": target_outcoil,
                "CheckItem": rule_20_check_item,
                "Detail": ng_name,
                "Status": "",
            }
        )

    return output_rows


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_20_programwise(
    input_program_file: str, input_program_comment_file: str, input_image: str
) -> pd.DataFrame:

    logger.info("Starting execution of Rule 20")

    try:

        program_df = pd.read_csv(input_program_file)
        input_image_program_df = pd.read_csv(input_image)

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

        with open(input_program_comment_file, "r", encoding="utf-8") as file:
            program_comment_data = json.load(file)

        unique_program_values = program_df["PROGRAM"].unique()

        output_rows = []
        for program in unique_program_values:

            if program.lower() in task_names:
                logger.info(
                    f"Executing in Program {program} and section {section_name}"
                )
                program_rows = program_df[program_df["PROGRAM"] == program].copy()
                memory_feeding_section_df = program_rows[
                    program_rows["BODY"] == section_name
                ]

                # Extract rung group data filtered by program and section name, grouped by rung number
                memory_feeding_rung_groups_df = extract_rung_group_data_programwise(
                    program_df=program_df,
                    program_name=program,
                    section_name=section_name,
                )

                # Run detection range logic as per Rule 20
                detection_result = detection_range_programwise(
                    memory_feeding_rung_groups_df=memory_feeding_rung_groups_df,
                    operation_comment=operation_comment,
                    complete_comment=complete_comment,
                    memory_comment=memory_comment,
                    program_name=program,
                    program_comment_data=program_comment_data,
                    section_name=section_name,
                )

                cc1_result = check_detail_1_programwise(
                    detection_result=detection_result
                )

                reset_operation_complete_memory_operand = detection_result[
                    "operation_complete_memory"
                ][0]
                reset_operation_complete_memory_rung_number = detection_result[
                    "operation_complete_memory"
                ][1]

                cc2_result = check_detail_2_programwise(
                    program_name=program,
                    program_comment_data=program_comment_data,
                    memory_feeding_section_df=memory_feeding_section_df,
                    transport_comment=transport_comment,
                    reset_full_comment=reset_full_comment,
                    timing_comment=timing_comment,
                    reset_operation_complete_memory_operand=reset_operation_complete_memory_operand,
                    reset_operation_complete_memory_rung_number=reset_operation_complete_memory_rung_number,
                )
                # Store all CC results in a list
                all_cc_status = [cc1_result, cc2_result]

                # Save output to CSV
                output_rows = store_program_csv_results_programwise(
                    output_rows=output_rows,
                    all_cc_status=all_cc_status,
                    program_name=program,
                    section_name=section_name,
                    ng_content=ng_content,
                    check_detail_content=check_detail_content,
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
        logger.error(f"Rule 18 Error : {e}")

        return {"status": "NOT OK", "error": e}
