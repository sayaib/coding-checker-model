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
from .rule_47_ladder_utils import check_self_holding, get_series_contacts

# ============================================ Comments referenced in Rule 25 processing ============================================
# memory feed Complete （記憶送り完了）/memory feed timing （記憶送りタイミング）/ memory shift timing （記憶シフトタイミング）/ memory shift Complete （記憶シフト完了）
emergency_stop_comment = "非常停止"
operation_comment = "操作"
poweron_comment = "電源入り"
delay_comment = "遅延"
not_comment = "でない"

# ============================ Rule 46: Definitions, Content, and Configuration Details ============================

rule_47_check_item = "Rule of Emergency Stop Operation Detection Circuit"
rule_content_47 = "Display 「E-Stop operation,」 buzzer output and emergency stop when E-Stop PB of operation panel is detected to be pushed."
check_detail_content = {
    "cc1": " If ① but not ③, it is assumed to be NG.",
    "cc2": " Check that contact A includes the variable comment of ”電源入り(power on)"
    + "遅延(delay)” in the out coil condition detected in ③.",
    "cc3": "Check that contact B includes the variable comment of ”非常停止(emergency stop)"
    + "でない(not)” in the out coil condition detected in ③.",
    "cc4": "Check that the out coil is connected in series (AND) with only the contacts of ❶and❷ within the self-holding of the out coil detected by ③.",
}
ng_content = {
    "cc1": "Fault回路が標準通りに作られていない(異常回路の抜け・漏れ)可能性あり(Fault circuit may not be built to standard (abnormal circuit missing or leaking))",
    "cc2": "非常停止操作異常ALの条件に”電源入り遅延”のA接点が存在しない(There is no A contact for “power-on delay” in the emergency stop operation error AL conditions.)",
    "cc3": "非常停止操作異常ALの条件に”非常停止でない”のB接点が存在しない(There is no B contact for “not emergency stop” in the emergency stop operation error AL conditions.)",
    "cc4": "非常停止操作の異常回路が標準通りに構成されていない(Abnormal circuit for emergency stop operation not configured as per standard.)",
}
fault_section_name = "fault"


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
            if attr.get("latch") == "set" and isinstance(attr.get("operand"), str):
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


def detection_range_programwise(
    fault_section_df: pd.DataFrame, program_name: str, program_comment_data: dict
) -> dict:

    logger.info(f"Executing rule 47 detection range on program {program_name}")

    coil_df = fault_section_df[fault_section_df["OBJECT_TYPE_LIST"] == "Coil"].copy()

    for _, coil_row in coil_df.iterrows():
        attr = ast.literal_eval(coil_row["ATTRIBUTES"])
        coil_operand = attr.get("operand")
        if "AL" in coil_operand:
            coil_comment = get_the_comment_from_program(
                coil_operand, program_name, program_comment_data
            )
            if isinstance(coil_comment, list):
                if regex_pattern_check(
                    emergency_stop_comment, coil_comment
                ) and regex_pattern_check(operation_comment, coil_comment):
                    return {
                        "status": "OK",
                        "detection_coil": coil_operand,
                        "rung_number": coil_row["RUNG"],
                    }

    return {"status": "NG", "detection_coil": "", "rung_number": -1}


def check_detail_1_programwise(
    fault_section_df: pd.DataFrame, detection_result: dict
) -> dict:
    cc1_result = {}

    cc1_result["status"] = detection_result["status"]
    cc1_result["cc"] = "cc1"
    cc1_result["check_number"] = 1
    cc1_result["target_coil"] = detection_result["detection_coil"]
    cc1_result["rung_number"] = detection_result["rung_number"]

    return cc1_result


def check_detail_2_programwise(
    fault_section_df: pd.DataFrame,
    rung_number: int,
    program_name: str,
    program_comment_data: str,
) -> dict:

    logger.info(f"Executing rule no 47 check detail 1 in program {program_name}")

    status = "NG"
    match_contact_operand = ""
    cc2_result = {}

    detection_coil_rung_df = fault_section_df[fault_section_df["RUNG"] == rung_number]
    contact_df = detection_coil_rung_df[
        detection_coil_rung_df["OBJECT_TYPE_LIST"] == "Contact"
    ].copy()

    for _, contact_row in contact_df.iterrows():
        attr = ast.literal_eval(contact_row["ATTRIBUTES"])
        contact_operand = attr.get("operand")
        negated_operand = attr.get("negated")
        if contact_operand and isinstance(contact_operand, str):
            contact_comment = get_the_comment_from_program(
                contact_operand, program_name, program_comment_data
            )
        else:
            contact_comment = []

        if isinstance(contact_comment, list) and contact_comment:
            if (
                regex_pattern_check(poweron_comment, contact_comment)
                and regex_pattern_check(delay_comment, contact_comment)
                and negated_operand == "false"
            ):
                status = "OK"
                match_contact_operand = contact_operand
                break

    cc2_result["status"] = status
    cc2_result["cc"] = "cc2"
    cc2_result["check_number"] = 2
    cc2_result["target_coil"] = match_contact_operand
    cc2_result["rung_number"] = rung_number

    return cc2_result


def check_detail_3_programwise(
    fault_section_df: pd.DataFrame,
    rung_number: int,
    program_name: str,
    program_comment_data: str,
) -> dict:

    logger.info(f"Executing rule no 47 check detail 2 in program {program_name}")

    status = "NG"
    match_contact_operand = ""
    cc3_result = {}

    detection_coil_rung_df = fault_section_df[fault_section_df["RUNG"] == rung_number]
    contact_df = detection_coil_rung_df[
        detection_coil_rung_df["OBJECT_TYPE_LIST"] == "Contact"
    ].copy()

    for _, contact_row in contact_df.iterrows():
        attr = ast.literal_eval(contact_row["ATTRIBUTES"])
        contact_operand = attr.get("operand")
        negated_operand = attr.get("negated")
        if contact_operand and isinstance(contact_operand, str):
            contact_comment = get_the_comment_from_program(
                contact_operand, program_name, program_comment_data
            )
        else:
            contact_comment = []

        if isinstance(contact_comment, list) and contact_comment:
            if (
                regex_pattern_check(emergency_stop_comment, contact_comment)
                and regex_pattern_check(not_comment, contact_comment)
                and negated_operand == "true"
            ):
                status = "OK"
                match_contact_operand = contact_operand
                break

    cc3_result["status"] = status
    cc3_result["cc"] = "cc3"
    cc3_result["check_number"] = 3
    cc3_result["target_coil"] = match_contact_operand
    cc3_result["rung_number"] = rung_number

    return cc3_result


def check_detail_4_programwise(
    fault_section_df: pd.DataFrame,
    detection_coil: str,
    cc2_contact: str,
    cc3_contact: str,
    rung_number: int,
    program_name: str,
) -> dict:

    logger.info(f"Executing rule no 47 check detail 3 in program {program_name}")

    status = "NG"
    match_contact_operand = ""
    both_contact_operand_in_series = False
    cc4_result = {}

    detection_coil_rung_df = fault_section_df[fault_section_df["RUNG"] == rung_number]
    contact_df = detection_coil_rung_df[
        detection_coil_rung_df["OBJECT_TYPE_LIST"] == "Contact"
    ].copy()

    all_self_holding_coil = check_self_holding(detection_coil_rung_df)
    get_series_connect_data = get_series_contacts(
        pl.from_pandas(detection_coil_rung_df)
    )

    series_contact_operands_only = [
        [item.get("operand") for item in sublist] for sublist in get_series_connect_data
    ]
    for series_contact in series_contact_operands_only:
        if cc2_contact in series_contact and cc3_contact in series_contact:
            both_contact_operand_in_series = True
            break
    """
    this is function is for checking if both contact should be in under self holding
    logic is if both contact outcoil is less than self holding outcoil then it is under slef holding
    """
    both_contact_outlist = []
    self_holding_outlist = []
    both_contact_inside_self_holding = True
    for _, contact_row in contact_df.iterrows():
        attr = ast.literal_eval(contact_row["ATTRIBUTES"])
        contact_operand = attr.get("operand")
        if contact_operand == cc2_contact or contact_operand == cc3_contact:
            both_contact_outlist.append(attr.get("out_list"))
        if contact_operand == detection_coil:
            self_holding_outlist.append(attr.get("out_list"))

    for both_contact_out_val in both_contact_outlist:
        if any(both_contact_out_val > x for x in self_holding_outlist):
            both_contact_inside_self_holding = False
            break

    if (
        detection_coil in all_self_holding_coil
        and both_contact_operand_in_series
        and len(contact_df) == 3
        and both_contact_inside_self_holding
    ):
        status = "OK"
        match_rung_number = rung_number
    else:
        status = "NG"
        match_rung_number = -1

    cc4_result["status"] = status
    cc4_result["cc"] = "cc4"
    cc4_result["check_number"] = 4
    cc4_result["target_coil"] = ""
    cc4_result["rung_number"] = match_rung_number

    return cc4_result


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_47_programwise(
    input_program_file: str, input_program_comment_file: str
) -> pd.DataFrame:

    logger.info("Starting execution of Rule 46")

    try:
        program_df = pd.read_csv(input_program_file)
        with open(input_program_comment_file, "r", encoding="utf-8") as file:
            program_comment_data = json.load(file)

        unique_program_values = program_df["PROGRAM"].unique()
        output_rows = []

        for program_name in unique_program_values:
            logger.info(f"Executing rule 46 in Program {program_name}")

            if "main" in program_name.lower():

                current_program_df = program_df[program_df["PROGRAM"] == program_name]

                fault_section_df = current_program_df[
                    current_program_df["BODY"].str.lower() == fault_section_name
                ]

                # Run detection range logic as per Rule 24
                detection_result = detection_range_programwise(
                    fault_section_df=fault_section_df,
                    program_name=program_name,
                    program_comment_data=program_comment_data,
                )

                detection_status = detection_result["status"]
                detection_coil = detection_result["detection_coil"]
                rung_number = detection_result["rung_number"]

                # if status == "NG":
                cc1_result = check_detail_1_programwise(
                    fault_section_df=fault_section_df, detection_result=detection_result
                )
                cc2_result = check_detail_2_programwise(
                    fault_section_df=fault_section_df,
                    rung_number=rung_number,
                    program_name=program_name,
                    program_comment_data=program_comment_data,
                )
                cc3_result = check_detail_3_programwise(
                    fault_section_df=fault_section_df,
                    rung_number=rung_number,
                    program_name=program_name,
                    program_comment_data=program_comment_data,
                )

                cc2_contact = cc2_result.get("target_coil", "")
                cc3_contact = cc3_result.get("target_coil", "")
                cc4_result = check_detail_4_programwise(
                    fault_section_df=fault_section_df,
                    detection_coil=detection_coil,
                    cc2_contact=cc2_contact,
                    cc3_contact=cc3_contact,
                    rung_number=rung_number,
                    program_name=program_name,
                )

                all_cc_result = [cc1_result, cc2_result, cc3_result, cc4_result]

                if detection_status == "OK":
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
                                "Section": "Fault",
                                "RungNo": rung_number,
                                "Target": target_outcoil,
                                "CheckItem": rule_47_check_item,
                                "Detail": ng_name,
                                "Status": "",
                            }
                        )
                        # output_rows.append({
                        #     "TASK_NAME": program_name,
                        #     "SECTION_NAME": fault_section_name,
                        #     "RULE_NUMBER": "47",
                        #     "CHECK_NUMBER": check_number,
                        #     "RUNG_NUMBER": -1 if rung_number < 0 else rung_number-1,
                        #     "RULE_CONTENT": rule_content_47,
                        #     "CHECK_CONTENT": check_detail_content.get(cc_result.get('cc')),
                        #     "TARGET_OUTCOIL" : target_outcoil,
                        #     "STATUS": cc_result.get('status'),
                        #     "NG_EXPLANATION": ng_name
                        # })
                else:

                    output_rows.append(
                        {
                            "Result": "NG",
                            "Task": program_name,
                            "Section": "Fault",
                            "RungNo": -1 if rung_number < 0 else rung_number - 1,
                            "Target": "",
                            "CheckItem": rule_47_check_item,
                            "Detail": ng_name,
                            "Status": "",
                        }
                    )
                    #  output_rows.append({
                    #         "TASK_NAME": program_name,
                    #         "SECTION_NAME": fault_section_name,
                    #         "RULE_NUMBER": "47",
                    #         "CHECK_NUMBER": 1,
                    #         "RUNG_NUMBER": -1 if rung_number < 0 else rung_number-1,
                    #         "RULE_CONTENT": rule_content_47,
                    #         "CHECK_CONTENT": check_detail_content.get(cc_result.get('cc')),
                    #         "TARGET_OUTCOIL" : "",
                    #         "STATUS": "NG",
                    #         "NG_EXPLANATION": "Detection range coil not found"
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
