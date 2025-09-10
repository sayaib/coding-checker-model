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
from .rule_37_ladder_utils import get_block_connections

# ============================ Rule 37: Definitions, Content, and Configuration Details ============================
rule_37_check_item = "Rule of Data Process"
check_detail_content = {
    "cc1": "Check if the variables in ③ are cleared. (If there is even one, OK; if there is none, NG)",
    "cc2": "Check if the variables in ④ are cleared. (If there is even one, OK; if there is none, NG)",
}

ng_content = {
    "cc1": "間接参照時の配列外のクリアがされていないためNG",
    "cc2": "間接参照時の配列内のクリアがされていないためNG",
}


# def is_array_of_array(value: str) -> bool:
#     """
#     Returns True if the string has a bracketed value that is not purely numeric.
#     Example: LB600[LB601], LB600[GSB400], etc.
#     """
#     match = re.match(r"^[A-Za-z]+\d*\[([A-Za-z0-9]+)\](?:\s*-\s*.*)?$", value.strip())
#     if match:
#         inside = match.group(1)
#         # Return True if inside is not purely digits
#         return not inside.isdigit()
#     return False
def is_valid_array_of_array(value: str) -> bool:
    """
    Returns True if:
      - Outer: letters followed by digits (e.g., AL000, LB500)
      - Inner: alphanumeric but not purely numeric
      - Format: Outer[Inner]
    """
    match = re.match(r"^([A-Za-z]+\d+)\[([A-Za-z0-9]+)\]$", value.strip())
    if match:
        outer, inner = match.group(1), match.group(2)
        return not inner.isdigit()
    return False


def get_outer_inner_from_array(value: str):
    """
    Returns (outer, inner) only if both exist.
    Otherwise returns None.
    """
    match = re.match(r"^([A-Za-z0-9]+)\[([A-Za-z0-9]+)\]$", value.strip())
    if match:
        return match.group(1), match.group(2)
    return None


def check_both_present(clear_data: dict, inner_outer_data: str) -> bool:
    """
    Checks if both occurrences (case-insensitive) of inner_outer_data exist
    across inputs and outputs for any block in clear_data.
    """
    for values in clear_data.values():
        # 'values' is [inputs, outputs]
        # Flatten to a single list
        flat_values = []
        for item in values:
            if isinstance(item, list):
                flat_values.extend(item)
            else:
                flat_values.append(item)

        # Normalize
        values_lower = [v.lower() for v in flat_values if isinstance(v, str)]

        # Check
        if values_lower.count(inner_outer_data.lower()) >= 2:
            return True

    return False


# ============================== Program and Function-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================


def detection_range_programwise(curr_program_df: pd.DataFrame, body_type_key: str):
    all_array_of_array_list = []
    all_array_of_array_details = []
    coil_df = curr_program_df[curr_program_df["OBJECT_TYPE_LIST"].str.lower() == "coil"]
    for _, coil_row in coil_df.iterrows():
        attr = ast.literal_eval(coil_row["ATTRIBUTES"])
        coil_operand = attr.get("operand")
        if isinstance(coil_operand, str) and coil_operand:
            is_array_of_array_outcoil = is_valid_array_of_array(coil_operand)
            if is_array_of_array_outcoil:
                all_array_of_array_list.append(coil_operand)
                all_array_of_array_details.append(
                    [coil_row["RUNG"], coil_row[body_type_key]]
                )

    return all_array_of_array_list, all_array_of_array_details


def get_clear_block(
    curr_program_df: pd.DataFrame, clear_keyword: str, body_type_key: str
):
    all_clear_block_data = {}
    block_df = curr_program_df[
        curr_program_df["OBJECT_TYPE_LIST"].str.lower() == "block"
    ]
    prev_rung = -1
    prev_section = ""
    clear_block_index = 1

    for _, block_row in block_df.iterrows():
        current_rung = block_row["RUNG"]
        curr_section = block_row[body_type_key]
        if prev_rung == current_rung and prev_section == curr_section:
            continue
        # print("current_rung",current_rung)
        current_rung_df = curr_program_df[
            curr_program_df[body_type_key] == curr_section
        ]
        current_rung_df = current_rung_df[current_rung_df["RUNG"] == current_rung]
        block_connection_details = get_block_connections(current_rung_df)
        for block_data in block_connection_details:
            for block_name, params in block_data.items():
                if block_name.lower() == clear_keyword:
                    # print("block_connection_details",block_connection_details)
                    # print("block_name",block_name,"params",params)
                    print("block_data", block_data)
                    inputs, outputs = "", ""
                    index_inout = 0

                    for param in params:
                        for key, value in param.items():
                            if key.lower().startswith("inout"):
                                if index_inout == 0:
                                    inputs = value
                                elif index_inout == 1:
                                    outputs = value
                                index_inout += 1
                                if index_inout > 1:  # stop once we have both
                                    break
                        if index_inout > 1:
                            break

                    # Only add if not already in values
                    if [inputs, outputs] not in all_clear_block_data.values():
                        all_clear_block_data[clear_block_index] = [inputs, outputs]
                        clear_block_index += 1

        prev_rung = current_rung
        prev_section = curr_section

    return all_clear_block_data


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_37(
    input_program_file: str, program_key: str, body_type_key: str
) -> pd.DataFrame:

    logger.info("Rule 37 Start executing rule 1 program wise")
    output_rows = []

    try:
        program_df = pd.read_csv(input_program_file)

        unique_program_values = program_df[program_key].unique()
        for program in unique_program_values:

            # if program == "P000_InitialSetting":
            print("*" * 100)
            print("program", program)
            curr_program_df = program_df[program_df[program_key] == program].copy()
            all_array_type_outcoil_list, all_array_of_array_details = (
                detection_range_programwise(
                    curr_program_df=curr_program_df, body_type_key=body_type_key
                )
            )
            print("all_array_type_outcoil_list", all_array_type_outcoil_list)
            get_all_clear_block_with_inout = get_clear_block(
                curr_program_df=curr_program_df,
                clear_keyword="clear",
                body_type_key=body_type_key,
            )
            print("get_all_clear_block_with_inout", get_all_clear_block_with_inout)
            for both_array_data, both_array_details in zip(
                all_array_type_outcoil_list, all_array_of_array_details
            ):
                inner_outer_array = get_outer_inner_from_array(both_array_data)
                if inner_outer_array:

                    outer_operand = inner_outer_array[0]
                    inner_operand = inner_outer_array[1]

                    if (
                        isinstance(inner_operand, str)
                        and isinstance(outer_operand, str)
                        and inner_operand
                        and outer_operand
                    ):
                        """
                        here check cc1
                        """
                        outer_exist_in_clear_block = check_both_present(
                            get_all_clear_block_with_inout, outer_operand
                        )

                        outer_status = "NG"
                        ng_name = ng_content.get("cc1")

                        if outer_exist_in_clear_block:
                            outer_status = "OK"
                            ng_name = ""

                        output_rows.append(
                            {
                                "Result": outer_status,
                                "Task": program,
                                "Section": both_array_details[1],
                                "RungNo": both_array_details[0] - 1,
                                "Target": outer_operand,
                                "CheckItem": rule_37_check_item,
                                "Detail": ng_name,
                                "Status": "",
                            }
                        )
                        """
                        here check cc2
                        """
                        inner_exist_in_clear_block = check_both_present(
                            get_all_clear_block_with_inout, inner_operand
                        )

                        inner_status = "NG"
                        ng_name = ng_content.get("cc1")

                        if inner_exist_in_clear_block:
                            inner_status = "OK"
                            ng_name = ""

                        output_rows.append(
                            {
                                "Result": inner_status,
                                "Task": program,
                                "Section": both_array_details[1],
                                "RungNo": both_array_details[0] - 1,
                                "Target": inner_operand,
                                "CheckItem": rule_37_check_item,
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
        logger.error(f"Rule 37 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}
