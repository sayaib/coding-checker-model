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


# ============================================ Comments referenced in Rule 4.2 processing ============================================
section_name = "memoryfeeding"
rule_content_4_2 = "In order to synchronize the transfer and the memory feed, it is necessary to implement the memory feed by ANDing the ON of the start memory and the ON of the end confirmation."
rule_4_2_check_item = "Rule of Memoryfeeding(P&P)"
chuck_comment = "ﾁｬｯｸ"
unchuck_comment = "ｱﾝﾁｬｯｸ"
memory_comment = "記憶"
timing_comment = "ﾀｲﾐﾝｸﾞ"
start_comment = "開始"

rule_content_cc = {
    "cc1": "If the target is to be detected in ① but not in ③, NG is assumed.",
    "cc2": "Memory feeding is performed by the start memory ON AND the end confirmation ON.",
    "cc3": "Memory feeding (move operation) is performed at the memory feed timing.",
    "cc4": "The chuck start memory is RESET (unlatched) at the rise (↑) of the memory feed timing.",
}

chuck_ng_content = {
    "cc1": "チャック確認中：チャック開始記憶が検出できなかった(Chuck checking: Chuck start memory could not could not be detected.)",
    "cc2": "記憶送りの条件が開始記憶のONと端確認のONのANDで構成されていないため、同期していない可能性有 (Possibly not synchronized because the condition of memory feed does not consist of the start memory ON AND the end confirmation ON.)",
    "cc3": "記憶送りタイミングで記憶送りをしていないため、同期していない可能性有 (Possibly  synchronized because the no memory feeding is performed at the memory feed timing) ",
    "cc4": "記憶送りタイミングの立ち上がりでチャック開始記憶をRESETしていないため、同期していない可能性有 (Possibly  synchronized because start memory is RESET (unlatched) at the rise of the memory feed timing)",
}

unchuck_ng_content = {
    "cc1": "アンチャック確認中：アンチャック開始記憶が検出できなかった(Unchuck checking: Unchuck start memory circuit could not be detected.)",
    "cc2": "記憶送りの条件が開始記憶のONと端確認のONのANDで構成されていないため、同期していない可能性有 (Possibly not synchronized because the condition of memory feed does not consist of the start memory ON AND the end confirmation ON.)",
    "cc3": "記憶送りタイミングで記憶送りをしていないため、同期していない可能性有 (Possibly  synchronized because the no memory feeding is performed at the memory feed timing) ",
    "cc4": "記憶送りタイミングの立ち上がりでチャック開始記憶をRESETしていないため、同期していない可能性有 (Possibly  synchronized because start memory is RESET (unlatched) at the rise of the memory feed timing)",
}


# ============================ Helper Functions for Program-Wise Operations ============================
def check_start_memory_outcoil_from_program(
    row,
    program_name: str,
    chuck_unchuck_comment: str,
    start_comment: str,
    memory_comment: str,
    program_comment_data: dict,
):
    try:
        attr = ast.literal_eval(row["ATTRIBUTES"])
        if "operand" in attr and "latch" in attr:
            if attr.get("latch") == "set":
                comment = get_the_comment_from_program(
                    attr.get("operand"), program_name, program_comment_data
                )
                if isinstance(comment, list) and comment:
                    if (
                        regex_pattern_check(chuck_unchuck_comment, comment)
                        and regex_pattern_check(start_comment, comment)
                        and regex_pattern_check(memory_comment, comment)
                    ):
                        return attr.get("operand")
    except Exception:
        return None
    return None


def extract_contact_in_list_from_program(x):
    try:
        return ast.literal_eval(x).get("in_list")
    except:
        return []


def extract_contact_outlist_from_program(x):
    try:
        return ast.literal_eval(x).get("out_list")
    except:
        return []


# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================
def detection_range_programwise(
    memory_feeding_rung_groups: pd.DataFrame,
    chuck_type: str,
    chuck_unchuck_comment: str,
    start_comment: str,
    memory_comment: str,
    program_name: str,
    program_comment_data: dict,
) -> dict:

    logger.info(
        f"Rule 4.2 Detection Range function for program: {program_name}, section: {section_name}"
    )

    results = []

    try:
        for _, rung_df in memory_feeding_rung_groups:
            start_memory_outcoil_operand = None
            has_chuck_unchuck_condition = False

            rung_df["start_memory_outcoil"] = rung_df.apply(
                lambda row: (
                    check_start_memory_outcoil_from_program(
                        row=row,
                        program_name=program_name,
                        chuck_unchuck_comment=chuck_unchuck_comment,
                        start_comment=start_comment,
                        memory_comment=memory_comment,
                        program_comment_data=program_comment_data,
                    )
                    if row["OBJECT_TYPE_LIST"].lower() == "coil"
                    else None
                ),
                axis=1,
            )

            start_memory_match_outcoil = rung_df[
                rung_df["start_memory_outcoil"].notna()
            ]
            if not start_memory_match_outcoil.empty:
                start_memory_outcoil_operand = start_memory_match_outcoil.iloc[0][
                    "start_memory_outcoil"
                ]  # First match

            rung_df["ATTR_DICT"] = rung_df["ATTRIBUTES"].apply(
                lambda x: ast.literal_eval(x) if pd.notna(x) else {}
            )

            chuck_unchuck_type = "ﾁｬｯｸ" if chuck_type == "chuck" else "ｱﾝﾁｬｯｸ"
            contact_df = rung_df[rung_df["OBJECT_TYPE_LIST"].str.lower() == "contact"]

            for _, contact_row in contact_df.iterrows():
                contact_operand = contact_row["ATTR_DICT"].get("operand")
                contact_comment = get_the_comment_from_program(
                    contact_operand, program_name, program_comment_data
                )
                try:
                    if isinstance(contact_comment, list):
                        if regex_pattern_check(
                            chuck_unchuck_type, contact_comment
                        ) and regex_pattern_check("条件", contact_comment):
                            if chuck_type == "chuck" and not regex_pattern_check(
                                "ｱﾝﾁｬｯｸ", contact_comment
                            ):
                                has_chuck_unchuck_condition = True
                            if chuck_type == "unchuck":
                                has_chuck_unchuck_condition = True
                except:
                    continue

            # Step 3: Save result if both match
            if start_memory_outcoil_operand and has_chuck_unchuck_condition:
                return {
                    "status": has_chuck_unchuck_condition,
                    "rung": int(rung_df["RUNG"].iloc[0]) - 1,
                    "target_coil": start_memory_outcoil_operand,
                }
    except:
        pass

    return {"status": False, "rung": -1, "target_coil": "None"}


# ============================== Check Detail 1 program ==============================
def check_detail_1_programwise(detection_result: dict, program_name: str) -> dict:
    logger.info(f"Rule 4.2 Executing program {program_name} check detail 1")
    detection_result["cc"] = "cc1"
    detection_result["status"] = "OK" if detection_result.get("status") else "NG"
    if detection_result["status"] == "NG":
        detection_result["rung"] = -1
    return detection_result


# ============================== Check Detail 2 program ==============================
def check_detail_2_programwise(
    memory_feeding_rung_groups,
    chuck_type: str,
    program_name: str,
    chuck_unchuck_comment: str,
    start_comment: str,
    memory_comment: str,
    timing_comment: str,
    program_comment_data: dict,
) -> dict:

    logger.info(f"Rule 4.2 Executing program {program_name} check detail 2")

    for _, rung_df in memory_feeding_rung_groups:
        # Check for required comments
        (
            is_chuck_start_memory_found,
            is_chuck_end_found,
            is_chuck_memory_timing_found,
        ) = (False, False, False)
        (
            start_memory_in_data,
            start_memory_out_data,
            end_memory_in_data,
            end_memory_out_data,
        ) = ([], [], [], [])

        rung_df["operand"] = rung_df["ATTRIBUTES"].apply(
            lambda x: ast.literal_eval(x).get("operand") if isinstance(x, str) else None
        )

        contacts_df = rung_df[rung_df["OBJECT_TYPE_LIST"].str.lower() == "contact"]

        for _, contact_row in contacts_df.iterrows():
            contact_operand = contact_row.get("operand")
            if contact_operand:
                try:
                    negated_contact = ast.literal_eval(contact_row["ATTRIBUTES"])
                    contact_comment = get_the_comment_from_program(
                        contact_operand, program_name, program_comment_data
                    )
                    if isinstance(contact_comment, list):
                        if (
                            regex_pattern_check(chuck_unchuck_comment, contact_comment)
                            and regex_pattern_check(start_comment, contact_comment)
                            and regex_pattern_check(memory_comment, contact_comment)
                            and negated_contact.get("negated") == "false"
                        ):
                            is_chuck_start_memory_found = True
                            start_memory_in_data.append(
                                extract_contact_in_list_from_program(
                                    contact_row["ATTRIBUTES"]
                                )
                            )
                            start_memory_out_data.append(
                                extract_contact_outlist_from_program(
                                    contact_row["ATTRIBUTES"]
                                )
                            )

                        elif (
                            regex_pattern_check("端", contact_comment)
                            and negated_contact.get("negated") == "false"
                        ):
                            is_chuck_end_found = True
                            end_memory_in_data.append(
                                extract_contact_in_list_from_program(
                                    contact_row["ATTRIBUTES"]
                                )
                            )
                            end_memory_out_data.append(
                                extract_contact_outlist_from_program(
                                    contact_row["ATTRIBUTES"]
                                )
                            )
                except:
                    continue

        # Get OUTCOIL operands
        coil_df = rung_df[rung_df["OBJECT_TYPE_LIST"].str.lower() == "coil"]
        for _, coil_row in coil_df.iterrows():
            coil_operand = coil_row.get("operand")
            coil_comment = get_the_comment_from_program(
                coil_operand, program_name, program_comment_data
            )
            try:
                if isinstance(coil_comment, list):
                    if (
                        regex_pattern_check(chuck_unchuck_comment, coil_comment)
                        and regex_pattern_check(memory_comment, coil_comment)
                        and regex_pattern_check(timing_comment, coil_comment)
                    ):
                        if chuck_type == "chuck" and not regex_pattern_check(
                            "ｱﾝ", coil_comment
                        ):
                            is_chuck_memory_timing_found = True
                        if chuck_type == "unchuck":
                            is_chuck_memory_timing_found = True
                        break
            except:
                continue
        if is_chuck_start_memory_found and is_chuck_end_found:
            if (
                start_memory_out_data[0] in end_memory_in_data
                and is_chuck_memory_timing_found
            ):
                results = {
                    "status": "OK",
                    "rung": int(rung_df["RUNG"].iloc[0]) - 1,
                    "cc": "cc2",
                    "target_coil": coil_operand,
                }
                return results

    return {"status": "NG", "rung": None, "cc": "cc2", "target_coil": None}


# ================================ Check Detail 3 program ==============================
def check_detail_3_programwise(
    memory_feeding_rung_groups,
    chuck_type: str,
    program_name: str,
    chuck_unchuck_comment: str,
    memory_comment: str,
    timing_comment: str,
    program_comment_data: dict,
) -> dict:

    logger.info(f"Rule 4.2 Executing program {program_name} check detail 3")

    for _, rung_df in memory_feeding_rung_groups:
        has_memory_timing_for_chuck_unchuck = False

        rung_df = rung_df.copy()

        rung_df["ATTR_DICT"] = rung_df["ATTRIBUTES"].apply(
            lambda x: (
                (lambda y: ast.literal_eval(y) if pd.notna(y) else {})(x)
                if pd.notna(x)
                else {}
            )
        )

        rung_df["operand"] = rung_df["ATTR_DICT"].apply(lambda x: x.get("operand"))
        rung_df["typename"] = (
            rung_df["ATTR_DICT"].apply(lambda x: x.get("typeName", "")).str.upper()
        )
        for _, rung_row in rung_df.iterrows():
            contact_operand = rung_row["ATTR_DICT"].get("operand")
            rising_contact = rung_row["ATTR_DICT"].get("edge")
            contact_comment = get_the_comment_from_program(
                contact_operand, program_name, program_comment_data
            )
            try:
                if isinstance(contact_comment, list):
                    if (
                        regex_pattern_check(chuck_unchuck_comment, contact_comment)
                        and regex_pattern_check(memory_comment, contact_comment)
                        and regex_pattern_check(timing_comment, contact_comment)
                        and rising_contact == "rising"
                    ):
                        if chuck_type == "chuck" and not regex_pattern_check(
                            "ｱﾝ", contact_comment
                        ):
                            has_memory_timing_for_chuck_unchuck = True
                            target_coil = contact_operand
                        if chuck_type == "unchuck":
                            has_memory_timing_for_chuck_unchuck = True
                            target_coil = contact_operand
                # break
            except:
                continue

        move_status = (rung_df["typename"].str.lower() == "move").any()

        # if has_memory_timing_for_chuck_unchuck and move_status and clear_status:
        if has_memory_timing_for_chuck_unchuck and move_status:

            matched_rung = rung_df["RUNG"].iloc[0]
            return {
                "status": "OK",
                "rung": int(matched_rung) - 1,
                "cc": "cc3",
                "target_coil": target_coil,
            }

    return {"status": "NG", "rung": None, "cc": "cc3", "target_coil": None}


# ============================== Check Detail 4 program ==============================


def check_detail_4_programwise(
    memory_feeding_rung_groups,
    chuck_type: str,
    program_name: str,
    chuck_unchuck_comment: str,
    start_comment: str,
    memory_comment: str,
    timing_comment: str,
    program_comment_data: dict,
) -> dict:

    logger.info(f"Rule 4.2 Executing program {program_name} check detail 4")

    for _, rung_df in memory_feeding_rung_groups:
        is_memory_timing_found = False
        is_reset_start_memory_found = False
        try:
            # Parse ATTRIBUTES into dict safely
            rung_df = rung_df.copy()
            rung_df["ATTR_DICT"] = rung_df["ATTRIBUTES"].apply(
                lambda x: ast.literal_eval(x) if pd.notna(x) else {}
            )

            contact_df = rung_df[rung_df["OBJECT_TYPE_LIST"].str.lower() == "contact"]
            for _, contact_row in contact_df.iterrows():
                contact_operand = contact_row["ATTR_DICT"].get("operand")
                contact_comment = get_the_comment_from_program(
                    contact_operand, program_name, program_comment_data
                )
                try:
                    if isinstance(contact_comment, list):
                        if (
                            regex_pattern_check(chuck_unchuck_comment, contact_comment)
                            and regex_pattern_check(memory_comment, contact_comment)
                            and regex_pattern_check(timing_comment, contact_comment)
                            and contact_row["ATTR_DICT"].get("edge") == "rising"
                        ):
                            is_memory_timing_found = True
                            break
                except:
                    continue

            coil_df = rung_df[rung_df["OBJECT_TYPE_LIST"].str.lower() == "coil"]
            for _, coil_row in coil_df.iterrows():
                coil_operand = coil_row["ATTR_DICT"].get("operand")
                coil_comment = get_the_comment_from_program(
                    coil_operand, program_name, program_comment_data
                )
                try:
                    if isinstance(coil_comment, list):
                        if (
                            regex_pattern_check(chuck_unchuck_comment, coil_comment)
                            and regex_pattern_check(start_comment, coil_comment)
                            and coil_row["ATTR_DICT"].get("latch") == "reset"
                        ):
                            if regex_pattern_check(
                                memory_comment, contact_comment
                            ) and regex_pattern_check(timing_comment, contact_comment):
                                # found_chuck_memory_timing = True
                                if chuck_type == "chuck" and not regex_pattern_check(
                                    "ｱﾝ", contact_comment
                                ):
                                    is_reset_start_memory_found = True
                                    target_outcoil = coil_operand
                                if chuck_type == "unchuck":
                                    is_reset_start_memory_found = True
                                    target_outcoil = coil_operand
                                break
                except:
                    continue

            if is_memory_timing_found and is_reset_start_memory_found:
                matched_rung = rung_df["RUNG"].iloc[0]
                return {
                    "status": "OK",
                    "rung": int(matched_rung) - 1,
                    "cc": "cc4",
                    "target_coil": target_outcoil,
                }

        except Exception:
            continue

    return {"status": "NG", "rung": None, "cc": "cc4", "target_coil": None}


# ============================== calling check_detail which check all 4 check detail ==============================
def check_detail_programwise(
    memory_feeding_rung_groups,
    chuck_type: str,
    detection_result: dict,
    chuck_unchuck_comment: str,
    start_comment: str,
    memory_comment: str,
    timing_comment: str,
    program_name: str,
    section_name: str,
    program_comment_data: dict,
) -> List:

    logger.info(
        f"Rule 4.2 Checking details for program: {program_name}, section: {section_name}"
    )

    cc1_result = check_detail_1_programwise(
        detection_result=detection_result, program_name=program_name
    )
    cc2_result = check_detail_2_programwise(
        memory_feeding_rung_groups=memory_feeding_rung_groups,
        chuck_type=chuck_type,
        program_name=program_name,
        chuck_unchuck_comment=chuck_unchuck_comment,
        start_comment=start_comment,
        memory_comment=memory_comment,
        timing_comment=timing_comment,
        program_comment_data=program_comment_data,
    )
    cc3_result = check_detail_3_programwise(
        memory_feeding_rung_groups=memory_feeding_rung_groups,
        chuck_type=chuck_type,
        program_name=program_name,
        chuck_unchuck_comment=chuck_unchuck_comment,
        memory_comment=memory_comment,
        timing_comment=timing_comment,
        program_comment_data=program_comment_data,
    )
    cc4_result = check_detail_4_programwise(
        memory_feeding_rung_groups=memory_feeding_rung_groups,
        chuck_type=chuck_type,
        program_name=program_name,
        chuck_unchuck_comment=chuck_unchuck_comment,
        start_comment=start_comment,
        memory_comment=memory_comment,
        timing_comment=timing_comment,
        program_comment_data=program_comment_data,
    )

    return [cc1_result, cc2_result, cc3_result, cc4_result]


# ============================== Make filter Program, section, and group rung ==============================


def extract_rung_group_data_programwise(
    program_df, program_name: str, section_name: str
):

    logger.info(f"Rule 4.2 Filter program and memory feeding section and group rung")
    program_rows = program_df[program_df["PROGRAM"] == program_name].copy()
    memory_feeding_section_rows = program_rows[
        program_rows["BODY"].str.lower() == section_name
    ]
    memory_feeding_rung_groups = memory_feeding_section_rows.groupby("RUNG")
    return memory_feeding_rung_groups


# ============================== Store all data to CSV ==============================
def store_program_csv_results(
    output_rows: List,
    all_cc_status: List,
    program_name: str,
    section_name: str,
    chuck_unchuck_var: str,
    ng_content: dict,
) -> List:

    logger.info(f"Rule 4.2 Storing all result in output csv file")

    for index, cc_status in enumerate(all_cc_status):
        try:
            ng_name = (
                ng_content.get(cc_status.get("cc", ""))
                if cc_status.get("status") == "NG"
                else ""
            )
        except Exception as e:
            logger.error(
                f"Rule 4.2 Error retrieving NG content for cc {cc_status.get('cc', '')}: {e}"
            )
            ng_name = ""

        output_rows.append(
            {
                "Result": cc_status.get("status"),
                "Task": program_name,
                "Section": section_name,
                "RungNo": cc_status.get("rung"),
                "Target": (
                    cc_status.get("target_coil")
                    if (cc_status.get("target_coil") != "None")
                    else ""
                ),
                "CheckItem": rule_4_2_check_item,
                "Detail": ng_name,
                "Status": "",
            }
        )

    return output_rows


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_4_2_program_wise(
    input_file: str, program_comment_file: str, input_image: str = None
):

    logger.info("Executing Rule 4.2 program wise")

    try:
        program_df = pd.read_csv(input_file)
        # Check if input_image is provided
        if input_image is None:
            # If Task-csv file is not provided, create an empty DataFrame with required columns
            input_image_program_df = pd.DataFrame(columns=["Unit", "Task name"])
        else:
            input_image_program_df = pd.read_csv(input_image)
        with open(program_comment_file, "r", encoding="utf-8") as file:
            program_comment_data = json.load(file)

        task_names = (
            input_image_program_df[
                input_image_program_df["Unit"].astype(str).str.lower() == "p&p"
            ]["Task name"]
            .astype(str)
            .str.lower()
            .tolist()
        )

        output_rows = []
        output_data = []
        unique_program_values = program_df["PROGRAM"].unique()
        for program in unique_program_values:

            logger.info(f"Rule 4.2 Executing Rule 4.2 in program {program}")

            if program.lower() in task_names:

                logger.info(
                    f"Rule 4.2 Filter rung based on program name {program} and section name {section_name}"
                )
                memory_feeding_rung_groups = extract_rung_group_data_programwise(
                    program_df=program_df,
                    program_name=program,
                    section_name=section_name,
                )

                if len(memory_feeding_rung_groups) != 0:
                    chuck_types = ["chuck", "unchuck"]

                    for chuck_type in chuck_types:

                        logger.info(
                            f"Rule 4.2 In program name {program} check for type {chuck_type}"
                        )
                        if chuck_type == "chuck":
                            chuck_unchuck_comment = chuck_comment
                            ng_content = chuck_ng_content

                        elif chuck_type == "unchuck":
                            chuck_unchuck_comment = unchuck_comment
                            ng_content = unchuck_ng_content

                        detection_result = detection_range_programwise(
                            memory_feeding_rung_groups=memory_feeding_rung_groups,
                            chuck_type=chuck_type,
                            chuck_unchuck_comment=chuck_unchuck_comment,
                            start_comment=start_comment,
                            memory_comment=memory_comment,
                            program_name=program,
                            program_comment_data=program_comment_data,
                        )

                        all_cc_status = check_detail_programwise(
                            memory_feeding_rung_groups=memory_feeding_rung_groups,
                            chuck_type=chuck_type,
                            detection_result=detection_result,
                            chuck_unchuck_comment=chuck_unchuck_comment,
                            start_comment=start_comment,
                            memory_comment=memory_comment,
                            timing_comment=timing_comment,
                            program_name=program,
                            section_name=section_name,
                            program_comment_data=program_comment_data,
                        )
                        # output_data = store_program_csv_results(output_rows=output_rows, all_cc_status=all_cc_status, program_name=program_name, section_name=section_name, chuck_unchuck_var=chuck_unchuck_var, ng_content=ng_content, rule_content=rule_content)
                        output_data = store_program_csv_results(
                            output_rows=output_rows,
                            all_cc_status=all_cc_status,
                            program_name=program,
                            section_name=section_name,
                            chuck_unchuck_var=chuck_type,
                            ng_content=ng_content,
                        )

        final_output_df = pd.DataFrame(output_data)
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
        logger.error(f"Rule 4.2 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}
