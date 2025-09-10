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
from .rule_25_26_ladder_utils import get_block_connections, get_comments_from_datasource
from .ladder_utils import regex_pattern_check, clean_rung_number

# ============================================ Comments referenced in Rule 25 processing ============================================
# memory feed Complete （記憶送り完了）/memory feed timing （記憶送りタイミング）/ memory shift timing （記憶シフトタイミング）/ memory shift Complete （記憶シフト完了）
index_comment = "インデックス"
table_comment = "テーブル"
start_comment = "起動"
command_comment = "指令"
memory_comment = "記憶"
feed_comment = "送り"
shift_comment = "シフト"
complete_comment = "完了"
timing_comment = "ﾀｲﾐﾝｸﾞ"
previous_comment = "前回"
present_comment = "現在"
angle_comment = "角度"
pos_comment = "位置"
command_angle_comment = "指令角度"
comment_360 = "#360"
transport_comment = "搬送"
reset_comment = "リセット"


# ============================ Rule 25: Definitions, Content, and Configuration Details ============================
autorun_section_name = "AutoRun"
autorun_section_name_with_star = "AutoRun★"
memoryfeeding_section_name = "Memoryfeeding"
MD_Out_section_name = "MD_Out"
rule_25_check_item = "Rule of Memoryfeeding(Index Transfer)"
rule_content_25 = "・The memory feed timing of the indexing mechanism shall be such that memory feed (data transfer and original data clearing) is performed at the rising A contact of the signal processed by angle calculation and ABSODEX Start in the same scan, and this contact shall be output to each process as「operation Complete reset timing.」"
check_detail_content = {
    "cc1": "When ③ is not found in the detection target in ①, it is set to NG.",
    "cc2": "The variable detected in ③ must be used as the rising A contact in the same AutoRun section. If even one of the above conditions is not met, the program shall be considered NG. ",
    "cc3": "Check that memory feed (*1) and clear (*2) are performed under the condition of the rising A contact of the variable detected by ③. Otherwise, NG.",
    "cc4": "Check that the out-coil condition detected in ⑤ is only the rising A contact of the variable detected in ②.",
    "cc5": "Check that at least one of the A contacts of the variable detected in ⑤ is connected to an out coil in the “MD_Out” section that meets the following conditions.",
}
ng_content = {
    "cc1": "Index搬送回路だが,記憶送り回路がコーディング基準に沿っていない(回転指示コイルもしくは記憶送り完了コイルなし)(Index transport circuit, but the memory feed circuit does not follow coding standards.(without rotation start coil or memory feed complete coil))",
    "cc2": "Index搬送回路だが動作回路がコーディング基準に沿っていない(Index transport circuit, but the operation circuit does not follow coding standards.)",
    "cc3": "Index搬送回路だが,記憶送り回路がコーディング基準に沿っていない(Index transport circuit, but the memory feed circuit does not follow coding standards.)",
    "cc4": "Index搬送回路だが,記憶送り回路がコーディング基準に沿っていない(Index transport circuit, but the memory feed circuit does not follow coding standards.)",
    "cc5": "Index搬送回路だが,記憶送り回路がコーディング基準に沿っていない(Index transport circuit, but the memory feed circuit does not follow coding standards.)",
}


def parse_attr(x):
    if pd.isna(x):
        return {}
    if isinstance(x, dict):
        return x
    try:
        return ast.literal_eval(x)
    except:
        return {}


def is_minus_connected_to_comparison(blocks):
    prev_cmp_index = None

    for i, block in enumerate(blocks):
        block_type = list(block.keys())[0]
        block_data = block[block_type]

        if block_type in (">=", "<="):
            prev_cmp_index = i  # Save the index of the comparison block

        elif block_type == "-":
            if prev_cmp_index is not None:
                prev_cmp_block = blocks[prev_cmp_index]
                prev_cmp_data = list(prev_cmp_block.values())[0]
                # Check: last element of comparison block is {} and first of '-' block is {}
                if prev_cmp_data[-1] == {} and block_data[0] == {}:
                    return True
            return False  # '-' came, but no valid comparison before

        else:
            # Any non-comparison block invalidates previous comparison
            prev_cmp_index = None

    return False  # No '-' found or not properly connected


def clean_rung_number(val):
    if isinstance(val, list):
        if -1 in val:
            return ""
        else:
            return val
    elif val == -1:
        return ""
    else:
        return val


# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================


def extract_rung_group_data_programwise(
    program_df: pd.DataFrame,
    program_name: str,
    autorun_section_name: str,
    autorun_section_name_with_star: str,
    memoryfeeding_section_name: str,
    MD_Out_section_name: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    logger.info(
        f"Group Rung and filter data on program {program_name} and section name {autorun_section_name, memoryfeeding_section_name, MD_Out_section_name}"
    )

    """
    Make three different section Autorun, MemoryFeeding, MD_Out as this all is going to use in check content 2,3,4,5 in rul 25
    ALso I use autorun_section_name_with_star because there are two autorun, one is AutoRun, other one is AutoRun★
    """

    program_rows = program_df[program_df["PROGRAM"] == program_name].copy()
    autorun_section_rows = program_rows[
        program_rows["BODY"]
        .str.lower()
        .isin([autorun_section_name.lower(), autorun_section_name_with_star.lower()])
    ]
    memory_feeding_section_rows = program_rows[
        program_rows["BODY"].str.lower() == memoryfeeding_section_name.lower()
    ]
    MD_Out_section_rows = program_rows[
        program_rows["BODY"].str.lower() == MD_Out_section_name.lower()
    ]
    autorun_rung_groups_df = autorun_section_rows.groupby("RUNG")
    memory_feeding_rung_groups_df = memory_feeding_section_rows.groupby("RUNG")
    MD_out_rung_groups_df = MD_Out_section_rows.groupby("RUNG")

    return autorun_rung_groups_df, memory_feeding_rung_groups_df, MD_out_rung_groups_df


def detection_range_programwise(
    autorun_rung_groups_df: pd.DataFrame,
    memory_feeding_rung_groups_df: pd.DataFrame,
    index_comment: str,
    table_comment: str,
    start_comment: str,
    command_comment: str,
    memory_comment: str,
    feed_comment: str,
    shift_comment: str,
    complete_comment: str,
    timing_comment: str,
    program_name: str,
    program_comment_data: str,
) -> dict:

    logger.info(f"Rule 25 detection range called in program {program_name}")

    """
    Autorun section checking there should be two or more outcoil in which 
    one outcoil contain index_start or index_command or table_start or table_command comment
    """
    index_table_start_command_operand = ""
    index_table_start_command_rung_number = -1
    for _, autorun_rung_df in autorun_rung_groups_df:
        has_two_or_more_outcoil = (
            autorun_rung_df["OBJECT_TYPE_LIST"].str.lower().eq("coil").sum() >= 2
        )
        if has_two_or_more_outcoil and not index_table_start_command_operand:
            coil_df = autorun_rung_df[
                autorun_rung_df["OBJECT_TYPE_LIST"].str.lower() == "coil"
            ]
            for _, coil_rows in coil_df.iterrows():
                attr = ast.literal_eval(coil_rows["ATTRIBUTES"])
                coil_operand = attr.get("operand")

                if coil_operand and isinstance(coil_operand, str):
                    coil_comment = get_the_comment_from_program(
                        coil_operand, program_name, program_comment_data
                    )
                else:
                    coil_comment = []

                if isinstance(coil_comment, list) and coil_comment:
                    if (
                        regex_pattern_check(index_comment, coil_comment)
                        or regex_pattern_check(table_comment, coil_comment)
                    ) and (
                        regex_pattern_check(start_comment, coil_comment)
                        or regex_pattern_check(command_comment, coil_comment)
                    ):
                        index_table_start_command_operand = coil_operand
                        index_table_start_command_rung_number = coil_rows["RUNG"]
                        break

        if index_table_start_command_operand:
            break

    """
    MemoryFeeding section checking there should be one outcoil in which 
    it contain (memory)" + ("送り(feed)” or ”シフト(shift)” ) + (”完了(complete)” or ”ﾀｲﾐﾝｸﾞ(timing)”
    """

    memory_feed_shift_complete_timing_operand = ""
    memory_feed_shift_complete_timing_rung_number = -1
    for _, memory_feeding_rung_df in memory_feeding_rung_groups_df:
        coil_df = memory_feeding_rung_df[
            memory_feeding_rung_df["OBJECT_TYPE_LIST"].str.lower() == "coil"
        ]
        for _, coil_rows in coil_df.iterrows():
            attr = ast.literal_eval(coil_rows["ATTRIBUTES"])
            coil_operand = attr.get("operand")
            if coil_operand and isinstance(coil_operand, str):
                coil_comment = get_the_comment_from_program(
                    coil_operand, program_name, program_comment_data
                )
            else:
                coil_comment = []

            if coil_comment and isinstance(coil_comment, list):
                if isinstance(coil_comment, list) and coil_comment:
                    if (
                        regex_pattern_check(memory_comment, coil_comment)
                        and (
                            regex_pattern_check(feed_comment, coil_comment)
                            or regex_pattern_check(shift_comment, coil_comment)
                        )
                        and (regex_pattern_check(complete_comment, coil_comment))
                    ):
                        memory_feed_shift_complete_timing_operand = coil_operand
                        memory_feed_shift_complete_timing_rung_number = coil_rows[
                            "RUNG"
                        ]
                    break
        if memory_feed_shift_complete_timing_operand:
            break

    return {
        "index_table_start_command_details": [
            index_table_start_command_operand,
            index_table_start_command_rung_number,
        ],
        "memory_feed_shift_complete_timing_details": [
            memory_feed_shift_complete_timing_operand,
            memory_feed_shift_complete_timing_rung_number,
        ],
    }


def check_detail_1_programwise(detection_result: dict, program_name: str) -> List:

    logger.info(f"Rule 25 checking content detail 1 in program {program_name}")
    cc1_results = []
    cc1_result = {}
    outcoil = detection_result["index_table_start_command_details"][0]
    cc1_result["status"] = (
        "OK" if detection_result["index_table_start_command_details"][0] else "NG"
    )
    cc1_result["cc"] = "cc1"
    cc1_result["check_number"] = "1"
    cc1_result["section_name"] = ["AutoRun", "MemoryFeeding"]
    cc1_result["outcoil"] = outcoil
    cc1_result["rung_number"] = (
        detection_result["index_table_start_command_details"][1] - 1
        if detection_result["index_table_start_command_details"][1] >= 0
        else -1
    )

    cc1_results.append(cc1_result)
    return cc1_results


def check_detail_2_programwise(
    autorun_rung_groups_df: pd.DataFrame,
    program_comment_data: pd.DataFrame,
    datasource_program_df: pd.DataFrame,
    detection_3_operand: str,
    previous_comment: str,
    present_comment: str,
    angle_comment: str,
    pos_comment: str,
    command_angle_comment: str,
    comment_360: str,
    program_name: str,
    section_name: str,
) -> List:
    logger.info(f"Rule 25 checking content detail 2 in program {program_name}")
    cc2_results = []
    rule_25_2_1 = rule_25_2_2 = rule_25_2_3 = False

    if detection_3_operand:
        for _, autorun_rung_df in autorun_rung_groups_df:

            ## getting all contact from autorun section
            contact_df = autorun_rung_df[
                autorun_rung_df["OBJECT_TYPE_LIST"] == "Contact"
            ]

            rule_25_2_1_1 = rule_25_2_1_2 = rule_25_2_2_1 = rule_25_2_2_2 = (
                rule_25_2_3_1
            ) = rule_25_2_3_2 = rule_25_2_3_3 = False
            rule_25_2_1 = rule_25_2_2 = rule_25_2_3 = False

            ## iteratte over all contact row to find rising contact with detection 3 result match rung number
            for _, contact_row in contact_df.iterrows():
                attr = ast.literal_eval(contact_row["ATTRIBUTES"])
                rising_edge = attr.get("edge", None)
                if (
                    detection_3_operand == attr.get("operand")
                    and rising_edge == "rising"
                ):
                    # if rising and detection 3 match found then going to check all 2.1, 2.2, 2.3 in matching rung
                    current_rung_data = autorun_rung_groups_df.get_group(
                        contact_row["RUNG"]
                    )
                    block_connection_details = get_block_connections(current_rung_data)

                    for block_data in block_connection_details:

                        ## check here 2.1 content given in rule
                        """
                        check here 2.1 content given in rule
                        2.1 A “+” function must be connected to the rising A contact of ③, and all of the following must be satisfied. Otherwise, it is NG.
                        2.1.1 One of the input variables for the “+” function must contain the variable comment (”前回(previous)" or ”現在(present)” + (”角度(angle)” or ”位置(pos)”) in the variable comment.
                        2.1.2 The output variable of the “+” function must contain ”指令角度(command angle)” in the variable comment.
                        """
                        if "+" in block_data.keys():
                            # Initialize default values
                            # Define keys
                            input_keys = ["In1", "In2", "In3"]
                            output_keys = ["Out1", "Out2", ""]

                            # Flatten the data
                            flat_dict = {
                                k: v[0] for d in block_data["+"] for k, v in d.items()
                            }

                            # Create input and output lists
                            block_plus_input_operand_list = [
                                flat_dict.get(k) for k in input_keys
                            ]
                            plus_block_output_operand_list = [
                                flat_dict.get(k) for k in output_keys
                            ]

                            block_plus_input_comments_list = []
                            block_plus_output_comments_list = []
                            if block_plus_input_operand_list:
                                for input_val in block_plus_input_operand_list:
                                    plus_block_input_comments = (
                                        get_the_comment_from_program(
                                            input_val,
                                            program_name,
                                            program_comment_data,
                                        )
                                    )
                                    plus_block_datasource_comments = (
                                        get_comments_from_datasource(
                                            input_variable=input_val,
                                            program_name=program_name,
                                            program_type="program",
                                            body_name=section_name,
                                            rung_order=contact_row["RUNG"],
                                            df=datasource_program_df,
                                        )
                                    )
                                    if (
                                        isinstance(plus_block_input_comments, list)
                                        and plus_block_input_comments
                                    ):
                                        block_plus_input_comments_list.extend(
                                            plus_block_input_comments
                                        )

                                    if (
                                        isinstance(plus_block_datasource_comments, list)
                                        and plus_block_datasource_comments
                                    ):
                                        block_plus_input_comments_list.extend(
                                            plus_block_datasource_comments
                                        )

                            if plus_block_output_operand_list:
                                for output_val in plus_block_output_operand_list:
                                    plus_block_output_comments = (
                                        get_the_comment_from_program(
                                            output_val,
                                            program_name,
                                            program_comment_data,
                                        )
                                    )
                                    plus_block_datasource_comments = (
                                        get_comments_from_datasource(
                                            input_variable=output_val,
                                            program_name=program_name,
                                            program_type="program",
                                            body_name=section_name,
                                            rung_order=contact_row["RUNG"],
                                            df=datasource_program_df,
                                        )
                                    )
                                    if (
                                        isinstance(plus_block_output_comments, list)
                                        and plus_block_output_comments
                                    ):
                                        block_plus_output_comments_list.extend(
                                            plus_block_output_comments
                                        )

                                    if (
                                        isinstance(plus_block_datasource_comments, list)
                                        and plus_block_datasource_comments
                                    ):
                                        block_plus_output_comments_list.extend(
                                            plus_block_datasource_comments
                                        )

                            ## check 2.1.1 here
                            if (
                                isinstance(block_plus_input_comments_list, list)
                                and block_plus_input_comments_list
                            ):
                                if (
                                    regex_pattern_check(
                                        previous_comment, block_plus_input_comments_list
                                    )
                                    or regex_pattern_check(
                                        present_comment, block_plus_input_comments_list
                                    )
                                ) and (
                                    regex_pattern_check(
                                        angle_comment, block_plus_input_comments_list
                                    )
                                    or regex_pattern_check(
                                        pos_comment, block_plus_input_comments_list
                                    )
                                ):
                                    rule_25_2_1_1 = True

                            if (
                                isinstance(block_plus_output_comments_list, list)
                                and block_plus_output_comments_list
                            ):
                                if (
                                    regex_pattern_check(
                                        command_angle_comment,
                                        block_plus_output_comments_list,
                                    )
                                    and not regex_pattern_check(
                                        present_comment, block_plus_output_comments_list
                                    )
                                    and not regex_pattern_check(
                                        previous_comment,
                                        block_plus_output_comments_list,
                                    )
                                ):
                                    rule_25_2_1_2 = True

                            if rule_25_2_1_1 and rule_25_2_1_2 and not rule_25_2_1:
                                rule_25_2_1 = True
                                cc2_results.append(
                                    {
                                        "cc": "cc2_1",
                                        "status": "OK",
                                        "section_name": "AutoRun",
                                        "check_number": "2.1",
                                        "outcoil": "",
                                        "rung_number": int(contact_row["RUNG"]),
                                    }
                                )
                        ## check here 2.2 content given in rule
                        """
                        check here 2.1 content given in rule
                        2.2 A “>=” or “<=” function must be connected to the rising A contact of ③, and either 2.2.1 or 2.2.2 below must be satisfied. Otherwise, it is NG.
                        2.2.1 For the function “>=”, the input variable In1 must be a variable containing the variable comment ”指令角度(command angle)”, and the input variable In2 must be set to a constant containing "#360".    *However, if ”前回(previous)" or ”現在(present)” is included in the variable comment, it is NG.
                        ❷.2.2 For the function "<=”, the input variable In2 must be a variable containing the variable comment”指令角度(command angle)”, and the input variable In1 must be set to a constant containing "#360".   
                        """

                        input1_operand_greater_than_block = []
                        input2_operand_greater_than_block = []
                        if ">=" in block_data.keys():
                            for param in block_data[">="]:
                                if "In1" in param:
                                    input1_operand_greater_than_block.append(
                                        param["In1"][0]
                                    )
                                elif "In2" in param:
                                    input2_operand_greater_than_block.append(
                                        param["In2"][0]
                                    )

                            in1_block_greaterthan_input_comments_list = []

                            if input1_operand_greater_than_block:
                                for input_val in input1_operand_greater_than_block:
                                    input1_greater_than_comments = (
                                        get_the_comment_from_program(
                                            input_val,
                                            program_name,
                                            program_comment_data,
                                        )
                                    )
                                    input1_greater_than_datasource_comments = (
                                        get_comments_from_datasource(
                                            input_variable=input_val,
                                            program_name=program_name,
                                            program_type="program",
                                            body_name=section_name,
                                            rung_order=contact_row["RUNG"],
                                            df=datasource_program_df,
                                        )
                                    )
                                    if (
                                        isinstance(input1_greater_than_comments, list)
                                        and input1_greater_than_comments
                                    ):
                                        in1_block_greaterthan_input_comments_list.extend(
                                            input1_greater_than_comments
                                        )

                                    if (
                                        isinstance(
                                            input1_greater_than_datasource_comments,
                                            list,
                                        )
                                        and input1_greater_than_datasource_comments
                                    ):
                                        in1_block_greaterthan_input_comments_list.extend(
                                            input1_greater_than_datasource_comments
                                        )

                            if (
                                isinstance(
                                    in1_block_greaterthan_input_comments_list, list
                                )
                                and in1_block_greaterthan_input_comments_list
                            ):
                                if (
                                    regex_pattern_check(
                                        command_angle_comment,
                                        in1_block_greaterthan_input_comments_list,
                                    )
                                    and regex_pattern_check(
                                        comment_360, input2_operand_greater_than_block
                                    )
                                    and not regex_pattern_check(
                                        present_comment,
                                        in1_block_greaterthan_input_comments_list,
                                    )
                                    and not regex_pattern_check(
                                        previous_comment,
                                        in1_block_greaterthan_input_comments_list,
                                    )
                                ):
                                    rule_25_2_2_1 = True

                        input1_operand_lesser_than_block = []
                        input2_operand_lesser_than_block = []
                        if "<=" in block_data.keys():
                            for param in block_data["<="]:
                                if "In1" in param:
                                    input1_operand_lesser_than_block.append(
                                        param["In1"][0]
                                    )
                                elif "In2" in param:
                                    input2_operand_lesser_than_block.append(
                                        param["In2"][0]
                                    )

                            in2_block_greaterthan_input_comments_list = []
                            if input2_operand_lesser_than_block:
                                for input_val in input2_operand_lesser_than_block:
                                    input2_lesser_than_comments = (
                                        get_the_comment_from_program(
                                            input_val,
                                            program_name,
                                            program_comment_data,
                                        )
                                    )
                                    input2_lesser_than_datasource_comments = (
                                        get_comments_from_datasource(
                                            input_variable=input_val,
                                            program_name=program_name,
                                            program_type="program",
                                            body_name=section_name,
                                            rung_order=contact_row["RUNG"],
                                            df=datasource_program_df,
                                        )
                                    )
                                    if (
                                        isinstance(input2_lesser_than_comments, list)
                                        and input2_lesser_than_comments
                                    ):
                                        in2_block_greaterthan_input_comments_list.extend(
                                            input2_lesser_than_comments
                                        )

                                    if (
                                        isinstance(
                                            input2_lesser_than_datasource_comments, list
                                        )
                                        and input2_lesser_than_datasource_comments
                                    ):
                                        in2_block_greaterthan_input_comments_list.extend(
                                            input2_lesser_than_datasource_comments
                                        )

                                if (
                                    regex_pattern_check(
                                        comment_360, input1_operand_lesser_than_block
                                    )
                                    and regex_pattern_check(
                                        command_angle_comment,
                                        in2_block_greaterthan_input_comments_list,
                                    )
                                    and not regex_pattern_check(
                                        present_comment,
                                        in2_block_greaterthan_input_comments_list,
                                    )
                                    and not regex_pattern_check(
                                        previous_comment,
                                        in2_block_greaterthan_input_comments_list,
                                    )
                                ):
                                    rule_25_2_2_2 = True

                        if (rule_25_2_2_1 or rule_25_2_2_2) and not rule_25_2_2:
                            rule_25_2_2 = True

                            cc2_results.append(
                                {
                                    "cc": "cc2_2",
                                    "status": "OK",
                                    "section_name": "AutoRun",
                                    "check_number": "2.2",
                                    "outcoil": "",
                                    "rung_number": int(contact_row["RUNG"]),
                                }
                            )

                        ## check here 2.3 content given in rule
                        """
                        2.3   A “-” function must be connected to the right side of 2.2, and all of the following must be satisfied. Otherwise, it is NG.
                        2.3.1 The input variable In1 for the “-” function must contain ”指令角度(command angle)” in the variable comment. 
                            *However, if ”前回(previous)" or ”現在(present)” is included in the variable comment, it is NG. 
                        2.3.2 The input variable In2 for the “-” function must be set to a constant that includes “#360” .
                        2.3.3 The output variable of the “-” function must contain ”指令角度(command angle)” in the variable comment.
                        """

                        ### check it to right side is nned to implement and remain to implement
                        minus_output_list = []
                        input2_operand = ""
                        if "-" in block_data.keys():
                            if is_minus_connected_to_comparison(
                                block_connection_details
                            ):
                                for param in block_data["-"]:
                                    if "In1" in param:
                                        input1_operand = param["In1"][0]
                                    elif "In2" in param:
                                        input2_operand = param["In2"][0]
                                    elif "Out1" in param:
                                        minus_output_list.append(param["Out1"][0])
                                    elif "Out2" in param:
                                        minus_output_list.append(param["Out2"][0])
                                    elif "" in param:
                                        minus_output_list.append(param[""][0])

                                in1_block_substract_input_comments_list = []
                                out_block_substract_input_comments_list = []

                                if input1_operand:
                                    in1_global_comments = get_the_comment_from_program(
                                        input1_operand,
                                        program_name,
                                        program_comment_data,
                                    )
                                    in1_datasource_comments = (
                                        get_comments_from_datasource(
                                            input_variable=input_val,
                                            program_name=program_name,
                                            program_type="program",
                                            body_name=section_name,
                                            rung_order=contact_row["RUNG"],
                                            df=datasource_program_df,
                                        )
                                    )
                                    if in1_global_comments:
                                        in1_block_substract_input_comments_list.extend(
                                            in1_global_comments
                                        )
                                    if in1_datasource_comments:
                                        in1_block_substract_input_comments_list.extend(
                                            in1_datasource_comments
                                        )

                                if minus_output_list:
                                    for output_val in minus_output_list:
                                        global_comments = get_the_comment_from_program(
                                            output_val,
                                            program_name,
                                            program_comment_data,
                                        )
                                        datasource_comments = (
                                            get_comments_from_datasource(
                                                input_variable=output_val,
                                                program_name=program_name,
                                                program_type="program",
                                                body_name=section_name,
                                                rung_order=contact_row["RUNG"],
                                                df=datasource_program_df,
                                            )
                                        )
                                        if (
                                            isinstance(global_comments, list)
                                            and global_comments
                                        ):
                                            out_block_substract_input_comments_list.extend(
                                                global_comments
                                            )
                                        if (
                                            isinstance(datasource_comments, list)
                                            and datasource_comments
                                        ):
                                            out_block_substract_input_comments_list.extend(
                                                datasource_comments
                                            )

                                if (
                                    regex_pattern_check(
                                        command_angle_comment,
                                        in1_block_substract_input_comments_list,
                                    )
                                    and not regex_pattern_check(
                                        present_comment,
                                        in1_block_substract_input_comments_list,
                                    )
                                    and not regex_pattern_check(
                                        previous_comment,
                                        in1_block_substract_input_comments_list,
                                    )
                                ):
                                    rule_25_2_3_1 = True

                                if re.search(comment_360, input2_operand):
                                    rule_25_2_3_2 = True

                                if (
                                    regex_pattern_check(
                                        command_angle_comment,
                                        out_block_substract_input_comments_list,
                                    )
                                    and not regex_pattern_check(
                                        present_comment,
                                        out_block_substract_input_comments_list,
                                    )
                                    and not regex_pattern_check(
                                        previous_comment,
                                        out_block_substract_input_comments_list,
                                    )
                                ):
                                    rule_25_2_3_3 = True

                        if (
                            rule_25_2_3_1
                            and rule_25_2_3_2
                            and rule_25_2_3_3
                            and not rule_25_2_3
                        ):
                            rule_25_2_3 = True

                            cc2_results.append(
                                {
                                    "cc": "cc2_3",
                                    "status": "OK",
                                    "section_name": "AutoRun",
                                    "check_number": "2.3",
                                    "outcoil": "",
                                    "rung_number": int(contact_row["RUNG"]),
                                }
                            )

                if rule_25_2_1 and rule_25_2_2 and rule_25_2_3:
                    break

            if rule_25_2_1 and rule_25_2_2 and rule_25_2_3:
                break

    """
    if some of them arenot getting OK at final step then it will make that check detail to NG
    """
    required_checks = {
        "cc2_1": 0,
        "cc2_2": 1,
        "cc2_3": 2,
    }
    if not (rule_25_2_1 and rule_25_2_2 and rule_25_2_3):
        for check, index in required_checks.items():
            if not any(entry.get("cc") == check for entry in cc2_results):
                cc2_results.insert(
                    index,
                    {
                        "cc": f"cc2_{check[-1]}",
                        "status": "NG",
                        "section_name": "AutoRun",
                        "check_number": check.replace("cc", "").replace("_", "."),
                        "outcoil": "",
                        "rung_number": -1,
                    },
                )

    print("cc2_results", cc2_results)
    if rule_25_2_1 and rule_25_2_2 and rule_25_2_3:
        cc2_results.append(
            {
                "cc": "cc2",
                "status": "OK",
                "section_name": "AutoRun",
                "check_number": "2",
                "outcoil": "",
                "rung_number": int(contact_row["RUNG"]),
            }
        )
        return cc2_results

    else:
        cc2_results.append(
            {
                "cc": "cc2",
                "status": "NG",
                "section_name": "AutoRun",
                "check_number": "2",
                "outcoil": "",
                "rung_number": "",
            }
        )
        return cc2_results


def check_detail_3_programwise(
    memory_feeding_rung_groups_df: pd.DataFrame,
    detection_3_operand: str,
    program_name: str,
) -> List:
    logger.info(f"Rule 25 checking contect detail 3 in program {program_name}")
    cc3_results = []

    if detection_3_operand:

        for _, memory_feeding_df in memory_feeding_rung_groups_df:

            ## getting all contact from autorun section
            contact_df = memory_feeding_df[
                memory_feeding_df["OBJECT_TYPE_LIST"] == "Contact"
            ]

            ## iteratte over all contact row to find rising contact with detection 3 result match rung number
            for _, contact_row in contact_df.iterrows():
                attr = ast.literal_eval(contact_row["ATTRIBUTES"])
                rising_edge = attr.get("edge", None)
                if (
                    detection_3_operand == attr.get("operand")
                    and rising_edge == "rising"
                ):

                    # Only parse if needed
                    current_rung_details = memory_feeding_rung_groups_df.get_group(
                        contact_row["RUNG"]
                    ).copy()

                    # Safely assign ATTR_DICT without corrupting existing dicts
                    current_rung_details["ATTR_DICT"] = current_rung_details[
                        "ATTRIBUTES"
                    ].apply(parse_attr)

                    # Extract and normalize the typeName
                    current_rung_details["typename"] = (
                        current_rung_details["ATTR_DICT"]
                        .apply(lambda x: x.get("typeName", ""))
                        .str.upper()
                    )

                    # Check for statuses
                    move_status = (current_rung_details["typename"] == "MOVE").any()
                    memcopy_status = (
                        current_rung_details["typename"] == "MEMCOPY"
                    ).any()
                    clear_status = (current_rung_details["typename"] == "CLEAR").any()

                    if (move_status or memcopy_status) and clear_status:
                        cc3_results.append(
                            {
                                "cc": "cc3",
                                "status": "OK",
                                "section_name": "MemoryFeeding",
                                "check_number": "3",
                                "outcoil": detection_3_operand,
                                "rung_number": int(contact_row["RUNG"]),
                            }
                        )

                        return cc3_results

    cc3_results.append(
        {
            "cc": "cc3",
            "status": "NG",
            "section_name": "MemoryFeeding",
            "check_number": "3",
            "outcoil": "",
            "rung_number": "",
        }
    )
    return cc3_results


def check_detail_4_programwise(
    memory_feeding_rung_groups_df: pd.DataFrame,
    detection_3_operand: str,
    detection_5_operand: str,
    program_name: str,
) -> List:
    logger.info(f"Rule 25 checking contect detail 4 in program {program_name}")
    cc4_results = []

    if detection_3_operand and detection_5_operand:
        for _, memory_feeding_rung_df in memory_feeding_rung_groups_df:

            detection_3_operand_in_memoryfeeding_found = False
            detection_3_operand_details = {}
            detection_5_operand_in_memoryfeeding_found = False
            detection_5_operand_details = {}

            contact_df = memory_feeding_rung_df[
                memory_feeding_rung_df["OBJECT_TYPE_LIST"] == "Contact"
            ]
            coil_df = memory_feeding_rung_df[
                memory_feeding_rung_df["OBJECT_TYPE_LIST"] == "Coil"
            ]

            for _, contact_row in contact_df.iterrows():
                attr = ast.literal_eval(contact_row["ATTRIBUTES"])
                rising_edge = attr.get("edge", None)
                if (
                    detection_3_operand == attr.get("operand")
                    and rising_edge == "rising"
                ):
                    detection_3_operand_in_memoryfeeding_found = True
                    detection_3_operand_details = attr
                    break

            for _, coil_row in coil_df.iterrows():
                attr = ast.literal_eval(coil_row["ATTRIBUTES"])
                if detection_5_operand == attr.get("operand"):
                    detection_5_operand_in_memoryfeeding_found = True
                    detection_5_operand_details = attr
                    break

            if (
                detection_3_operand_in_memoryfeeding_found
                and detection_5_operand_in_memoryfeeding_found
            ):

                for outlist_data in detection_5_operand_details["in_list"]:
                    if outlist_data in detection_3_operand_details["out_list"]:
                        cc4_results.append(
                            {
                                "cc": "cc4",
                                "status": "OK",
                                "section_name": "MemoryFeeding",
                                "check_number": "4",
                                "outcoil": [detection_3_operand, detection_5_operand],
                                "rung_number": int(coil_row["RUNG"]),
                            }
                        )
                        return cc4_results

    cc4_results.append(
        {
            "cc": "cc4",
            "status": "NG",
            "section_name": "MemoryFeeding",
            "check_number": "4",
            "outcoil": "",
            "rung_number": "",
        }
    )
    return cc4_results


def check_detail_5_programwise(
    MD_out_rung_groups_df: pd.DataFrame,
    detection_5_operand: str,
    program_name: str,
    program_comment_data: dict,
) -> List:
    logger.info(f"Rule 25 checking contect detail 5 in program {program_name}")
    cc5_results = []

    if detection_5_operand:

        for _, MD_Out_rung_df in MD_out_rung_groups_df:
            detection_5_operand_in_mdout_found = False
            detection_5_operand_details = {}
            md_out_contact_df = MD_Out_rung_df[
                MD_Out_rung_df["OBJECT_TYPE_LIST"] == "Contact"
            ]
            for _, md_out_contact_df in md_out_contact_df.iterrows():
                attr = ast.literal_eval(md_out_contact_df["ATTRIBUTES"])
                if (
                    attr.get("operand") == detection_5_operand
                    and attr.get("negated") == "false"
                ):
                    detection_5_operand_in_mdout_found = True
                    detection_5_operand_details = attr
                    break

            if detection_5_operand_in_mdout_found:
                md_out_coil = MD_Out_rung_df[
                    MD_Out_rung_df["OBJECT_TYPE_LIST"] == "Coil"
                ]
                for _, md_out_coil_df in md_out_coil.iterrows():

                    attr = ast.literal_eval(md_out_coil_df["ATTRIBUTES"])
                    contact_outlist_values = detection_5_operand_details.get(
                        "out_list", ""
                    )
                    coil_operand = attr.get("operand", "")
                    coil_inlist_values = attr.get("in_list", "")

                    coil_comment = get_the_comment_from_program(
                        coil_operand, program_name, program_comment_data
                    )

                    if isinstance(coil_comment, list):
                        for coil_inlist_value in coil_inlist_values:
                            if (
                                coil_inlist_value in contact_outlist_values
                                and "GB" in coil_operand.upper()
                                and (
                                    (
                                        regex_pattern_check(
                                            transport_comment, coil_comment
                                        )
                                        and regex_pattern_check(
                                            timing_comment, coil_comment
                                        )
                                    )
                                    or (
                                        regex_pattern_check(
                                            complete_comment, coil_comment
                                        )
                                        and regex_pattern_check(
                                            reset_comment, coil_comment
                                        )
                                        and regex_pattern_check(
                                            timing_comment, coil_comment
                                        )
                                    )
                                )
                            ):

                                cc5_results.append(
                                    {
                                        "cc": "cc5",
                                        "status": "OK",
                                        "section_name": "MD_Out",
                                        "check_number": "5",
                                        "outcoil": coil_operand,
                                        "rung_number": int(md_out_coil_df["RUNG"]),
                                    }
                                )
                                return cc5_results

    cc5_results.append(
        {
            "cc": "cc5",
            "status": "NG",
            "section_name": "MD_Out",
            "check_number": "5",
            "outcoil": "",
            "rung_number": "",
        }
    )
    return cc5_results


def store_program_csv_results_programwise(
    output_rows: List,
    all_cc_status: List[List],
    program_name: str,
    ng_content: dict,
    check_detail_content: str,
) -> List:
    logger.info(f"Storing all result in output csv file")
    # Flatten and loop through the inner items
    flat_cc_statuses = [cc for sublist in all_cc_status for cc in sublist]

    for _, cc_status in enumerate(flat_cc_statuses):

        ng_key = cc_status.get("cc", "").split("_")[0]
        ng_name = ng_content.get(ng_key) if cc_status.get("status") == "NG" else ""

        rung_raw = cc_status.get("rung_number")
        rung_number = (
            int(rung_raw) - 1 if isinstance(rung_raw, int) and rung_raw >= 0 else -1
        )

        outcoil = cc_status.get("outcoil")
        target_outcoil = outcoil if outcoil else ""

        curr_section_name = (
            cc_status.get("section_name") or cc_status.get("section", [None])[-1]
        )

        # check_content_number = cc_status.get('check_number', "")

        output_rows.append(
            {
                "Result": cc_status.get("status"),
                "Task": program_name,
                "Section": curr_section_name,
                "RungNo": rung_number,
                "Target": target_outcoil,
                "CheckItem": rule_25_check_item,
                "Detail": ng_name,
                "Status": "",
            }
        )

    return output_rows


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_25_programwise(
    input_program_file: str,
    input_program_comment_file: str,
    input_datasource_program_file: str,
    input_image: str,
) -> pd.DataFrame:

    logger.info("Starting execution of Rule 25")

    try:
        program_df = pd.read_csv(input_program_file)
        input_image_program_df = pd.read_csv(input_image)

        task_names = (
            input_image_program_df[
                input_image_program_df["Unit"].astype(str).str.lower() == "index"
            ]["Task name"]
            .astype(str)
            .str.lower()
            .tolist()
        )

        """
        for getting comment fo transformer block as it is needed for this rule to execute
        """
        datasource_program_df = pd.read_csv(input_datasource_program_file)

        with open(input_program_comment_file, "r", encoding="utf-8") as file:
            program_comment_data = json.load(file)

        unique_program_values = program_df["PROGRAM"].unique()

        output_rows = []
        for program_name in unique_program_values:

            if program_name.lower() in task_names:

                logger.info(f"Executing rule 25 in Program {program_name}")

                # Extract rung group data filtered by program and section name, grouped by rung number
                (
                    autorun_rung_groups_df,
                    memory_feeding_rung_groups_df,
                    MD_out_rung_groups_df,
                ) = extract_rung_group_data_programwise(
                    program_df=program_df,
                    program_name=program_name,
                    autorun_section_name=autorun_section_name,
                    autorun_section_name_with_star=autorun_section_name_with_star,
                    memoryfeeding_section_name=memoryfeeding_section_name,
                    MD_Out_section_name=MD_Out_section_name,
                )

                # Run detection range logic as per Rule 25
                detection_result = detection_range_programwise(
                    autorun_rung_groups_df=autorun_rung_groups_df,
                    memory_feeding_rung_groups_df=memory_feeding_rung_groups_df,
                    index_comment=index_comment,
                    table_comment=table_comment,
                    start_comment=start_comment,
                    command_comment=command_comment,
                    memory_comment=memory_comment,
                    feed_comment=feed_comment,
                    shift_comment=shift_comment,
                    complete_comment=complete_comment,
                    timing_comment=timing_comment,
                    program_name=program_name,
                    program_comment_data=program_comment_data,
                )

                cc1_result = check_detail_1_programwise(
                    detection_result=detection_result, program_name=program_name
                )
                detection_3_operand = detection_result[
                    "index_table_start_command_details"
                ][0]
                detection_5_operand = detection_result[
                    "memory_feed_shift_complete_timing_details"
                ][0]

                cc2_result = check_detail_2_programwise(
                    autorun_rung_groups_df=autorun_rung_groups_df,
                    program_comment_data=program_comment_data,
                    datasource_program_df=datasource_program_df,
                    detection_3_operand=detection_3_operand,
                    previous_comment=previous_comment,
                    present_comment=present_comment,
                    angle_comment=angle_comment,
                    pos_comment=pos_comment,
                    command_angle_comment=command_angle_comment,
                    comment_360=comment_360,
                    program_name=program_name,
                    section_name=autorun_section_name,
                )
                cc3_result = check_detail_3_programwise(
                    memory_feeding_rung_groups_df=memory_feeding_rung_groups_df,
                    detection_3_operand=detection_3_operand,
                    program_name=program_name,
                )
                cc4_result = check_detail_4_programwise(
                    memory_feeding_rung_groups_df=memory_feeding_rung_groups_df,
                    detection_3_operand=detection_3_operand,
                    detection_5_operand=detection_5_operand,
                    program_name=program_name,
                )
                cc5_result = check_detail_5_programwise(
                    MD_out_rung_groups_df=MD_out_rung_groups_df,
                    detection_5_operand=detection_5_operand,
                    program_name=program_name,
                    program_comment_data=program_comment_data,
                )

                all_cc_status = [
                    cc1_result,
                    cc2_result,
                    cc3_result,
                    cc4_result,
                    cc5_result,
                ]
                output_rows = store_program_csv_results_programwise(
                    output_rows=output_rows,
                    all_cc_status=all_cc_status,
                    program_name=program_name,
                    ng_content=ng_content,
                    check_detail_content=check_detail_content,
                )

        final_result_csv = pd.DataFrame(output_rows)

        if not final_result_csv.empty:
            if "RungNo" in final_result_csv.columns:
                final_result_csv["RungNo"] = final_result_csv["RungNo"].apply(
                    clean_rung_number
                )
        else:
            final_result_csv = pd.DataFrame(
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

        return {"status": "OK", "output_df": final_result_csv}

    except Exception as e:
        logger.error(f"Rule 25 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}
