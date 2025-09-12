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

# ============================================ Comments referenced in Rule 18 processing ============================================
section_name = "memoryfeeding"
MD_OUT_section = "md_out"
transport_comment = "搬送"
start_comment = "開始"
memory_comment = "記憶"
memory_feed_full_complete_comment = "記憶送り完了"
memory_feed_full_timing_comment = "記憶送りタイミング"
memory_shift_full_timing_comment = "記憶シフトタイミング"
memory_shift_full_complete_comment = "記憶シフト完了"
memory_feed_half_complete_comment = "記憶送り完了"
memory_feed_half_timing_comment = "記憶送りﾀｲﾐﾝｸﾞ"
memory_shift_half_timing_comment = "記憶ｼﾌﾄﾀｲﾐﾝｸﾞ"
memory_shift_half_complete_comment = "記憶ｼﾌﾄ完了"


# ============================ Rule 18: Definitions, Content, and Configuration Details ============================
rule_content_18 = "・The memory feed timing of the gripper mechanism shall be such that the rising edge of the transport Auxiliary signal is memorized as 「ransport start memory,」 the memory feed (data transfer and original data clearing) is executed at the rising edge, and the contact is output to each process as 「operation Complete reset timing.」"
rule_18_check_item = "Rule of Memoryfeeding(Gripper Transfer)"
check_detail_content = {
    "cc1": "When ③ or ④ is not found in the detection target in ①, it is  NG.",
    "cc2": "Memory feed (*2) must be performed at the rising A contact with the same variable name as the out coil detected by ③. Otherwise, it shall be NG.",
    "cc3": "Check if clear (*3) is performed at the rising A contact with the same variable name as the out coil detected by ③.Otherwise, NG.",
    "cc4": "Confirm that only the rising A contact of the out-oil variable detected in (3) is connected to the outcoil condition detected in ④. Otherwise, NG",
    "cc5": "In the “MD_Out” section, in some of the variable names and comments, check that only the A contact with the same variable name as the outcoil detected in step ④ is connected to the condition of the outcoil whose variable name starts with “GB”. Otherwise, NG",
}

ng_content = {
    "cc1": "Gripper搬送回路だが,記憶送り回路がコーディング基準に沿っていない(開始記憶or記憶送り完了コイル無し) Gripper transport circuit, but the memory feed circuit does not follow coding standards (no start memory or memory feed complete coil).",
    "cc2": "Gripper搬送回路だが,記憶送り回路がコーディング基準に沿っていない(記憶送り条件不一致) Gripper transport circuit, but the memory feed circuit does not follow coding standards (Memory feed condition mismatch).",
    "cc3": "Gripper搬送回路だが,記憶送り回路がコーディング基準に沿っていない(記憶クリア条件不一致) Gripper transport circuit, but the memory feed circuit does not follow coding standards (Memory clear condition mismatch).",
    "cc4": "Gripper搬送回路だが,記憶送り回路がコーディング基準に沿っていない(記憶送り完了条件不一致) Gripper transport circuit, but the memory feed circuit does not follow coding standards (Memory feed complete mismatch).",
    "cc5": "Gripper搬送回路だが,記憶送り回路がコーディング基準に沿っていない(他工程への記憶送り完了信号が未出力) Gripper transport circuit, but the memory feed circuit does not follow coding standards (Memory feed complete signal to other processes not yet output).",
}


# ============================ Helper Functions for Program-Wise Operations ============================
def check_start_memory_outcoil_from_program(
    row,
    program_name: str,
    transport_comment: str,
    start_comment: str,
    memory_comment,
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
                    if (
                        regex_pattern_check(transport_comment, comment)
                        and regex_pattern_check(start_comment, comment)
                        and regex_pattern_check(memory_comment, comment)
                    ):
                        return attr.get("operand")
    except Exception:
        return None
    return None


def check_memory_feed_outcoil_from_program(
    row, program_name: str, memory_feed_comment_list: str, program_comment_data: dict
):
    try:
        attr = ast.literal_eval(row["ATTRIBUTES"])
        if isinstance(attr, dict) and "operand" in attr:
            comment = get_the_comment_from_program(
                attr.get("operand"), program_name, program_comment_data
            )
            # if regex_pattern_check(start_comment, comment) and regex_pattern_check(memory_comment, comment):
            if isinstance(comment, list):
                if (
                    regex_pattern_check(memory_feed_comment_list[0], comment)
                    or regex_pattern_check(memory_feed_comment_list[1], comment)
                    or regex_pattern_check(memory_feed_comment_list[2], comment)
                    or regex_pattern_check(memory_feed_comment_list[3], comment)
                    or regex_pattern_check(memory_feed_comment_list[4], comment)
                    or regex_pattern_check(memory_feed_comment_list[5], comment)
                    or regex_pattern_check(memory_feed_comment_list[6], comment)
                    or regex_pattern_check(memory_feed_comment_list[7], comment)
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
        f"Rule 18 Group Rung and filter data on program {program_name} and section name {section_name}"
    )
    program_rows = program_df[program_df["PROGRAM"] == program_name].copy()
    memory_feeding_section_rows = program_rows[
        program_rows["BODY"].str.lower() == section_name.lower()
    ]
    rung_groups_df = memory_feeding_section_rows.groupby("RUNG")
    return rung_groups_df


def detection_range_programwise(
    memory_feeding_rung_groups_df: pd.DataFrame,
    transport_comment: str,
    start_comment: str,
    memory_comment: str,
    memory_feed_comment_list: List,
    program_name: str,
    program_comment_data: str,
    section_name: str,
) -> dict:

    logger.info(
        f"Rule 18 Executing detection range on program {program_name} and section name {section_name}"
    )

    memory_start_comment_operand = ""
    memory_feed_comment_operand = ""
    memory_start_comment_rung_number = -1
    memory_feed_comment_rung_number = -1

    try:
        for _, rung_df in memory_feeding_rung_groups_df:

            rung_df["start_memory_check_outcoil"] = rung_df.apply(
                lambda row: (
                    check_start_memory_outcoil_from_program(
                        row=row,
                        program_name=program_name,
                        transport_comment=transport_comment,
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
                rung_df["start_memory_check_outcoil"].notna()
            ]
            if not start_memory_match_outcoil.empty:
                memory_start_comment_operand = start_memory_match_outcoil.iloc[0][
                    "start_memory_check_outcoil"
                ]
                memory_start_comment_rung_number = start_memory_match_outcoil.iloc[0][
                    "RUNG"
                ]

            rung_df["memory_feed_check_outcoil"] = rung_df.apply(
                lambda row: (
                    check_memory_feed_outcoil_from_program(
                        row=row,
                        program_name=program_name,
                        memory_feed_comment_list=memory_feed_comment_list,
                        program_comment_data=program_comment_data,
                    )
                    if row["OBJECT_TYPE_LIST"].lower() == "coil"
                    else None
                ),
                axis=1,
            )

            memory_feed_match_outcoil = rung_df[
                rung_df["memory_feed_check_outcoil"].notna()
            ]
            if not memory_feed_match_outcoil.empty:
                memory_feed_comment_operand = memory_feed_match_outcoil.iloc[0][
                    "memory_feed_check_outcoil"
                ]
                memory_feed_comment_rung_number = memory_feed_match_outcoil.iloc[0][
                    "RUNG"
                ]

            if memory_start_comment_operand and memory_feed_comment_operand:
                return {
                    "start_memory": [
                        memory_start_comment_operand,
                        memory_start_comment_rung_number,
                    ],
                    "memory_feed": [
                        memory_feed_comment_operand,
                        memory_feed_comment_rung_number,
                    ],
                }
    except Exception as e:
        logger.error(f"Error in detection_range_programwise: {e}")

    return {
        "start_memory": [
            memory_start_comment_operand,
            memory_start_comment_rung_number,
        ],
        "memory_feed": [memory_feed_comment_operand, memory_feed_comment_rung_number],
    }


def check_detail_1_programwise(detection_result: dict) -> dict:

    logger.info(f"Rule 18 Executing check detail 1 ")

    cc1_result = {}
    status = "OK"
    outcoil = []

    for _, detection_val in detection_result.items():
        if detection_val[1] == -1:
            status = "NG"

    outcoil = [
        operand
        for operand in [
            detection_result["start_memory"][0],
            detection_result["memory_feed"][0],
        ]
        if operand
    ]

    cc1_result["cc"] = "cc1"
    cc1_result["status"] = status
    cc1_result["rung_number"] = -1
    cc1_result["start_memory"] = detection_result["start_memory"]
    cc1_result["memory_feed"] = detection_result["memory_feed"]
    cc1_result["outcoil"] = outcoil

    return cc1_result


def check_detail_2_programwise(
    memory_feeding_rung_groups_df: pd.DataFrame, start_memory_operand: str
) -> dict:
    logger.info(f"Rule 18 Executing check detail 2 ")

    if start_memory_operand and isinstance(start_memory_operand, str):

        try:
            for _, rung_df in memory_feeding_rung_groups_df:
                rising_contact_operand_match_found = False
                rung_df["ATTR_DICT"] = rung_df["ATTRIBUTES"].apply(
                    lambda x: (
                        (lambda y: ast.literal_eval(y) if pd.notna(y) else {})(x)
                        if pd.notna(x)
                        else {}
                    )
                )
                rung_df["typename"] = (
                    rung_df["ATTR_DICT"]
                    .apply(lambda x: x.get("typeName", ""))
                    .str.upper()
                )

                for _, rung_row in rung_df.iterrows():
                    contact_operand = rung_row["ATTR_DICT"].get("operand")
                    rising_contact = rung_row["ATTR_DICT"].get("edge", False)
                    if (
                        start_memory_operand == contact_operand
                        and rising_contact == "rising"
                        and rung_row["OBJECT_TYPE_LIST"].lower() == "contact"
                    ):
                        rising_contact_operand_match_found = True
                        rung_number = rung_row["RUNG"]
                        break
                # move_status = rung_df['typename'].eq('MOVE').any() if 'Block' in rung_df['OBJECT_TYPE_LIST'] else False
                move_status = (rung_df["typename"].str.lower() == "move").any()
                memcopy_status = (rung_df["typename"].str.lower() == "memcopy").any()
                if rising_contact_operand_match_found and (
                    move_status or memcopy_status
                ):
                    return {
                        "cc": "cc2",
                        "status": "OK",
                        "rung_number": rung_number,
                        "outcoil": contact_operand,
                    }
        except Exception as e:
            logger.error(f"Error in check_detail_2_programwise: {e}")

    return {"cc": "cc2", "status": "NG", "rung_number": -1, "outcoil": ""}


def check_detail_3_programwise(
    memory_feeding_rung_groups_df: pd.DataFrame, start_memory_operand: str
) -> dict:

    logger.info(f"Rule 18 Executing check detail 3 ")

    if start_memory_operand:
        try:
            for _, rung_df in memory_feeding_rung_groups_df:
                rising_contact_operand_match_found = False
                rung_df["ATTR_DICT"] = rung_df["ATTRIBUTES"].apply(
                    lambda x: (
                        (lambda y: ast.literal_eval(y) if pd.notna(y) else {})(x)
                        if pd.notna(x)
                        else {}
                    )
                )
                rung_df["typename"] = (
                    rung_df["ATTR_DICT"]
                    .apply(lambda x: x.get("typeName", ""))
                    .str.upper()
                )

                for _, rung_row in rung_df.iterrows():
                    contact_operand = rung_row["ATTR_DICT"].get("operand")
                    rising_contact = rung_row["ATTR_DICT"].get("edge", False)
                    if (
                        start_memory_operand == contact_operand
                        and rising_contact == "rising"
                        and rung_row["OBJECT_TYPE_LIST"].lower() == "contact"
                    ):
                        rising_contact_operand_match_found = True
                        rung_number = rung_row["RUNG"]
                        break

                clear_status = (rung_df["typename"] == "CLEAR").any()
                if rising_contact_operand_match_found and clear_status:
                    return {
                        "cc": "cc3",
                        "status": "OK",
                        "rung_number": rung_number,
                        "outcoil": contact_operand,
                    }
        except Exception as e:
            logger.error(f"Error in check_detail_3_programwise: {e}")

    return {"cc": "cc3", "status": "NG", "rung_number": -1, "outcoil": ""}


def check_detail_4_programwise(
    memory_feeding_rung_groups_df: pd.DataFrame,
    start_memory_operand: str,
    memory_feed_operand: str,
) -> dict:

    logger.info(f"Rule 18 Executing check detail 4 ")

    if start_memory_operand:

        try:
            for _, rung_df in memory_feeding_rung_groups_df:

                rising_contact_operand_match_found = False
                memory_feed_operand_present = False
                rising_contact_operand_match_attr = {}
                memory_feed_operand_present_attr = {}

                rung_df["ATTR_DICT"] = rung_df["ATTRIBUTES"].apply(
                    lambda x: (
                        (lambda y: ast.literal_eval(y) if pd.notna(y) else {})(x)
                        if pd.notna(x)
                        else {}
                    )
                )
                for _, rung_row in rung_df.iterrows():
                    operand = rung_row["ATTR_DICT"].get("operand")
                    rising_contact = rung_row["ATTR_DICT"].get("edge", False)
                    if (
                        start_memory_operand == operand
                        and rising_contact == "rising"
                        and rung_row["OBJECT_TYPE_LIST"].lower() == "contact"
                    ):
                        rising_contact_operand_match_found = True
                        rising_contact_operand_match_attr = rung_row["ATTR_DICT"]
                    if (
                        memory_feed_operand == operand
                        and rung_row["OBJECT_TYPE_LIST"].lower() == "coil"
                    ):
                        memory_feed_operand_present = True
                        memory_feed_operand_present_attr = rung_row["ATTR_DICT"]

                if rising_contact_operand_match_found and memory_feed_operand_present:
                    rising_contact_outcoil_match_out_list = (
                        rising_contact_operand_match_attr["out_list"]
                    )
                    memory_feed_operand_present_in_list = (
                        memory_feed_operand_present_attr["in_list"]
                    )
                    for in_val in memory_feed_operand_present_in_list:
                        if in_val in rising_contact_outcoil_match_out_list:
                            return {
                                "cc": "cc4",
                                "status": "OK",
                                "rung_number": rung_row["RUNG"],
                                "outcoil": [start_memory_operand, memory_feed_operand],
                            }

        except Exception as e:
            logger.error(f"Error in check_detail_4_programwise: {e}")

    return {"cc": "cc4", "status": "NG", "rung_number": -1, "outcoil": []}


def check_detail_5_programwise(
    MD_Out_section_rung_group_df: pd.DataFrame, memory_feed_operand: str
) -> dict:

    logger.info(f"Rule 18 Executing check detail 5 ")

    try:
        for _, md_out_rung_df in MD_Out_section_rung_group_df:
            is_memory_feed_match_found_in_detection_4 = False

            md_out_rung_df["ATTR_DICT"] = md_out_rung_df["ATTRIBUTES"].apply(
                lambda x: (
                    (lambda y: ast.literal_eval(y) if pd.notna(y) else {})(x)
                    if pd.notna(x)
                    else {}
                )
            )
            for _, rung_row in md_out_rung_df.iterrows():

                if (
                    rung_row["OBJECT_TYPE_LIST"].lower() == "contact"
                    and rung_row["ATTR_DICT"].get("negated") == "false"
                ):
                    if rung_row["ATTR_DICT"].get("operand") == memory_feed_operand:
                        is_memory_feed_match_found_in_detection_4 = True

                if (
                    is_memory_feed_match_found_in_detection_4
                    and rung_row["OBJECT_TYPE_LIST"].lower() == "coil"
                ):
                    memory_feed_match_outcoil_in_MD = rung_row["ATTR_DICT"].get(
                        "operand"
                    )
                    if memory_feed_match_outcoil_in_MD.startswith("GB"):
                        return {
                            "cc": "cc5",
                            "status": "OK",
                            "rung_number": rung_row["RUNG"],
                            "outcoil": memory_feed_match_outcoil_in_MD,
                        }

                    else:
                        return {
                            "cc": "cc5",
                            "status": "NG",
                            "rung_number": rung_row["RUNG"],
                            "outcoil": memory_feed_match_outcoil_in_MD,
                        }

    except Exception as e:
        logger.error(f"Error in check_detail_5_programwise: {e}")

    return {
        "cc": "cc5",
        "status": "NG",
        "rung_number": -1,
        "outcoil": "No contact Present",
    }


def store_program_csv_results_programwise(
    output_rows: List,
    all_cc_status: List,
    program_name: str,
    section_name: str,
    ng_content: dict,
    check_detail_content: str,
) -> List:

    logger.info(f"Rule 18 Storing all result in output csv file")

    for index, cc_status in enumerate(all_cc_status):
        try:
            ng_name = (
                ng_content.get(cc_status.get("cc", ""))
                if cc_status.get("status") == "NG"
                else ""
            )
        except Exception as e:
            logger.error(f"Error in fetching ng_name: {e}")
            ng_name = ""

        try:
            rung_number = (
                rung_number - 1
                if isinstance((rung_number := cc_status.get("rung_number")), int)
                and rung_number >= 0
                else ""
            )
        except Exception as e:
            logger.error(f"Error in fetching rung_number: {e}")
            rung_number = -1

        try:
            target_outcoil = (
                cc_status.get("outcoil") if cc_status.get("outcoil") else ""
            )
        except Exception as e:
            logger.error(f"Error in fetching target_outcoil: {e}")
            target_outcoil = ""

        output_rows.append(
            {
                "Result": cc_status.get("status"),
                "Task": program_name,
                "Section": section_name,
                "RungNo": rung_number,
                "Target": target_outcoil,
                "CheckItem": rule_18_check_item,
                "Detail": ng_name,
                "Status": "",
            }
        )

    return output_rows


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_18_programwise(
    input_program_file: str, input_program_comment_file: str, input_image: str=None
) -> pd.DataFrame:

    logger.info("Starting execution of Rule 18")

    try:

        program_df = pd.read_csv(input_program_file)
        input_image_program_df = pd.read_csv(input_image)
        with open(input_program_comment_file, "r", encoding="utf-8") as file:
            program_comment_data = json.load(file)

        task_names = (
            input_image_program_df[
                input_image_program_df["Unit"].astype(str).str.lower() == "gripper"
            ]["Task name"]
            .astype(str)
            .str.lower()
            .tolist()
        )

        unique_program_values = program_df["PROGRAM"].unique()

        output_rows = []
        for program in unique_program_values:
            if program.lower() in task_names:

                logger.info(
                    f"Rule 18 Executing in Program {program} and section {section_name}"
                )

                # Extract rung group data filtered by program and section name, grouped by rung number
                memory_feeding_rung_groups_df = extract_rung_group_data_programwise(
                    program_df=program_df,
                    program_name=program,
                    section_name=section_name,
                )

                # Define list of memory feed-related comments for detection 4
                memory_feed_comment_list = [
                    memory_feed_full_complete_comment,
                    memory_feed_half_complete_comment,
                    memory_feed_full_timing_comment,
                    memory_feed_half_timing_comment,
                    memory_shift_full_timing_comment,
                    memory_shift_half_timing_comment,
                    memory_shift_full_complete_comment,
                    memory_shift_half_complete_comment,
                ]

                # Run detection range logic as per Rule 18
                detection_result = detection_range_programwise(
                    memory_feeding_rung_groups_df=memory_feeding_rung_groups_df,
                    transport_comment=transport_comment,
                    start_comment=start_comment,
                    memory_comment=memory_comment,
                    memory_feed_comment_list=memory_feed_comment_list,
                    program_name=program,
                    program_comment_data=program_comment_data,
                    section_name=section_name,
                )

                # Run individual check details (Rule 18 parts 1–4)
                cc1_result = check_detail_1_programwise(
                    detection_result=detection_result
                )

                start_memory_operand = detection_result["start_memory"][0]
                memory_feed_operand = detection_result["memory_feed"][0]

                cc2_result = check_detail_2_programwise(
                    memory_feeding_rung_groups_df=memory_feeding_rung_groups_df,
                    start_memory_operand=start_memory_operand,
                )

                cc3_result = check_detail_3_programwise(
                    memory_feeding_rung_groups_df=memory_feeding_rung_groups_df,
                    start_memory_operand=start_memory_operand,
                )

                cc4_result = check_detail_4_programwise(
                    memory_feeding_rung_groups_df=memory_feeding_rung_groups_df,
                    start_memory_operand=start_memory_operand,
                    memory_feed_operand=memory_feed_operand,
                )

                # MD_Out section checks
                MD_Out_section_rung_group_df = extract_rung_group_data_programwise(
                    program_df=program_df,
                    program_name=program,
                    section_name=MD_OUT_section,
                )

                cc5_result = check_detail_5_programwise(
                    MD_Out_section_rung_group_df=MD_Out_section_rung_group_df,
                    memory_feed_operand=memory_feed_operand,
                )

                # Store all CC results in a list
                all_cc_status = [
                    cc1_result,
                    cc2_result,
                    cc3_result,
                    cc4_result,
                    cc5_result,
                ]

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
