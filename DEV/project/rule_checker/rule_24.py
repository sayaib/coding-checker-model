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

# ============================================ Comments referenced in Rule 24 processing ============================================
# memory feed Complete （記憶送り完了）/memory feed timing （記憶送りタイミング）/ memory shift timing （記憶シフトタイミング）/ memory shift Complete （記憶シフト完了）
section_name = "MemoryFeeding"
memory_feed_comment = "記憶送り"
timing_comment = "ﾀｲﾐﾝｸﾞ"

# ============================ Rule 24: Definitions, Content, and Configuration Details ============================
rule_content_24 = "・The memory feed shall be applied only to the forced feed mechanism and not to the free flow transport."
rule_24_check_item = "Rule of MemoryFeeding(Overall)"
check_detail_content = {
    "cc1": "If a task ② is detected, check the input information and confirm that the “使用機器(搬送)(Equipment used (transport))” item of the detected task is the following. If not, it is NG."
}
ng_content = {"cc1": "Nothing is given as of now"}


# ============================ Helper Functions for Program-Wise Operations ============================
def check_memory_feed_outcoil_from_program(
    row,
    program_name: str,
    memory_feed_comment: str,
    timing_comment: str,
    program_comment_data: dict,
):
    try:
        attr = ast.literal_eval(row["ATTRIBUTES"])
        if isinstance(attr, dict) and "operand" in attr and "latch" in attr:
            if attr.get("latch") == "set":
                comment = get_the_comment_from_program(
                    attr.get("operand"), program_name, program_comment_data
                )
                if isinstance(comment, list):
                    if regex_pattern_check(
                        memory_feed_comment, comment
                    ) and regex_pattern_check(timing_comment, comment):
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
    memory_feed_comment: str,
    timing_comment: str,
    program_name: str,
    program_comment_data: str,
    section_name: str,
) -> dict:

    logger.info(
        f"Executing detection range on program {program_name} and section name {section_name} on rule 24"
    )

    memory_feed_comment_operand = False
    memory_feed_comment_rung_number = -1

    for _, rung_df in memory_feeding_rung_groups_df:

        rung_df["memory_feed_set_check_outcoil"] = rung_df.apply(
            lambda row: (
                check_memory_feed_outcoil_from_program(
                    row=row,
                    program_name=program_name,
                    memory_feed_comment=memory_feed_comment,
                    timing_comment=timing_comment,
                    program_comment_data=program_comment_data,
                )
                if row["OBJECT_TYPE_LIST"] == "Coil"
                else None
            ),
            axis=1,
        )

        memory_feed_match_outcoil = rung_df[
            rung_df["memory_feed_set_check_outcoil"].notna()
        ]
        if not memory_feed_match_outcoil.empty:
            memory_feed_comment_operand = memory_feed_match_outcoil.iloc[0][
                "memory_feed_set_check_outcoil"
            ]
            memory_feed_comment_rung_number = memory_feed_match_outcoil.iloc[0]["RUNG"]

        if memory_feed_comment_operand:
            return {
                "memory_feed": [
                    memory_feed_comment_operand,
                    memory_feed_comment_rung_number,
                ],
            }

    return {
        "memory_feed": [memory_feed_comment_operand, memory_feed_comment_rung_number],
    }


def check_detail_1_programwise(
    detection_result: dict, input_image_data: pd.DataFrame, program_name: str
) -> dict:

    logger.info(f"Executing check detail 1 for rule no 24")

    equipment_use_keywords = ["Screw", "Index", "P&P", "RB", "Gripper"]
    equipment_use_pattern = "|".join(equipment_use_keywords)
    all_program_having_equipment_used = input_image_data[
        input_image_data["Unit"].str.contains(
            equipment_use_pattern, na=False, regex=True
        )
    ]["Task name"].tolist()

    cc1_result = {}
    outcoil = []

    cc1_result["status"] = (
        "OK" if program_name in all_program_having_equipment_used else "NG"
    )

    outcoil = detection_result["memory_feed"][0]

    cc1_result["cc"] = "cc1"
    cc1_result["outcoil"] = outcoil
    cc1_result["rung_number"] = detection_result["memory_feed"][1] - 1

    return cc1_result


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_24_programwise(
    input_program_file: str, input_program_comment_file: str, input_image: str
) -> pd.DataFrame:

    logger.info("Starting execution of Rule 24")

    try:
        program_df = pd.read_csv(input_program_file)
        input_image_data = pd.read_csv(input_image)
        with open(input_program_comment_file, "r", encoding="utf-8") as file:
            program_comment_data = json.load(file)

        unique_program_values = program_df["PROGRAM"].unique()

        output_rows = []
        for program in unique_program_values:
            unique_program_section_names = (
                program_df.loc[program_df["PROGRAM"] == program, "BODY"]
                .str.lower()
                .unique()
            )
            logger.info(f"Executing in Program {program} and section {section_name}")

            if "memoryfeeding" in unique_program_section_names:
                # Extract rung group data filtered by program and section name, grouped by rung number
                memory_feeding_rung_groups_df = extract_rung_group_data_programwise(
                    program_df=program_df,
                    program_name=program,
                    section_name=section_name,
                )

                # Run detection range logic as per Rule 24
                detection_result = detection_range_programwise(
                    memory_feeding_rung_groups_df=memory_feeding_rung_groups_df,
                    memory_feed_comment=memory_feed_comment,
                    timing_comment=timing_comment,
                    program_name=program,
                    program_comment_data=program_comment_data,
                    section_name=section_name,
                )

                cc1_result = check_detail_1_programwise(
                    detection_result=detection_result,
                    input_image_data=input_image_data,
                    program_name=program,
                )

                ng_name = (
                    ng_content.get(cc1_result.get("cc", ""))
                    if cc1_result.get("status") == "NG"
                    else ""
                )
                rung_number = (
                    cc1_result.get("rung_number")
                    if cc1_result.get("rung_number")
                    else -1
                )
                target_outcoil = (
                    cc1_result.get("outcoil") if cc1_result.get("outcoil") else ""
                )

                if target_outcoil:
                    output_rows.append(
                        {
                            "Result": cc1_result.get("status"),
                            "Task": program,
                            "Section": section_name,
                            "RungNo": rung_number,
                            "Target": target_outcoil,
                            "CheckItem": rule_24_check_item,
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
        logger.error(f"Rule 18 Error : {e}")

        return {"status": "NOT OK", "error": e}
