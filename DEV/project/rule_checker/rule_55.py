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
from .rule_55_ladder_utils import get_block_connections

# ============================================ Comments referenced in Rule 25 processing ============================================
# memory feed Complete （記憶送り完了）/memory feed timing （記憶送りタイミング）/ memory shift timing （記憶シフトタイミング）/ memory shift Complete （記憶シフト完了）
fault_reset_comment = "不良解除"
autorun_comment = "自動運転"

# ============================ Rule 46: Definitions, Content, and Configuration Details ============================
rule_content_55 = "・that Auto operation mode ON can't Fault reset"
rule_55_check_item = "Rule of Fault Reset Circuit"

check_detail_content = {
    "cc1": " If there is no ④ in the rung of rung No. 0, then NG.",
    "cc2": " Check that the variable including “AL” in the variable name is connected to “InOut” of the input/output condition of the function detected by ④.",
    "cc3": "A contact that contains 不良解除(fault reset)” as a variable comment and B contact that contains ”自動運転(auto run)” as a variable comment must exist in series(AND) circuit in the condition of the function detected by ④. And, there must be no other contacts. Otherwise, NG.",
}
ng_content = {
    "cc1": "Fault'セクションであるのに、異常リセット回路が存在しないのでNG(NG because 'Fault' section, but there is no fault reset circuit.)",
    "cc2": "異常リセット回路にて、クリアがされていないためNG(NG because the fault reset circuit has not been cleared.)",
    "cc3": "異常リセット回路にて、クリア条件が標準通りでないためNG(NG because non-standard clear conditions in the fault reset circuit.)",
}

fault_section_name = "fault"

# ============================ Helper Functions for both Program-Wise and Function-Wise Operations ============================


def is_valid_contact_pair_programwise(
    row_a,
    row_b,
    clear_row,
    fault_reset_comment,
    autorun_comment,
    program_name,
    program_comment_data,
):
    attr_a = ast.literal_eval(row_a["ATTRIBUTES"])
    attr_b = ast.literal_eval(row_b["ATTRIBUTES"])
    attr_clear = ast.literal_eval(clear_row["ATTRIBUTES"])

    # Get operand/negated/comment for both
    operand_a = attr_a.get("operand", "")
    operand_b = attr_b.get("operand", "")
    negated_a = attr_a.get("negated", "")
    negated_b = attr_b.get("negated", "")
    comment_a = get_the_comment_from_program(
        operand_a, program_name, program_comment_data
    )
    comment_b = get_the_comment_from_program(
        operand_b, program_name, program_comment_data
    )

    # Wiring check: output of row_a to input of row_b
    out_a = attr_a.get("out_list", [])
    in_b = attr_b.get("in_list", [])
    out_b = attr_b.get("out_list", [])
    en_clear = attr_clear.get("EN_inVar_in_list", [])

    output_to_input_match = any(val in out_a for val in in_b)
    clear_connection_match = any(val in out_b for val in en_clear)

    if not output_to_input_match or not clear_connection_match:
        return False

    # Check condition types
    is_a_fault_reset = (
        regex_pattern_check(fault_reset_comment, comment_a) and negated_a == "false"
    )
    is_b_autorun = (
        regex_pattern_check(autorun_comment, comment_b) and negated_b == "true"
    )

    return is_a_fault_reset and is_b_autorun


def is_valid_contact_pair_functionwise(
    row_a,
    row_b,
    clear_row,
    fault_reset_comment,
    autorun_comment,
    program_name,
    program_comment_data,
):
    attr_a = ast.literal_eval(row_a["ATTRIBUTES"])
    attr_b = ast.literal_eval(row_b["ATTRIBUTES"])
    attr_clear = ast.literal_eval(clear_row["ATTRIBUTES"])

    # Get operand/negated/comment for both
    operand_a = attr_a.get("operand", "")
    operand_b = attr_b.get("operand", "")
    negated_a = attr_a.get("negated", "")
    negated_b = attr_b.get("negated", "")
    comment_a = get_the_comment_from_program(
        operand_a, program_name, program_comment_data
    )
    comment_b = get_the_comment_from_program(
        operand_b, program_name, program_comment_data
    )

    # Wiring check: output of row_a to input of row_b
    out_a = attr_a.get("out_list", [])
    in_b = attr_b.get("in_list", [])
    out_b = attr_b.get("out_list", [])
    en_clear = attr_clear.get("EN_inVar_in_list", [])

    output_to_input_match = any(val in out_a for val in in_b)
    clear_connection_match = any(val in out_b for val in en_clear)

    if not output_to_input_match or not clear_connection_match:
        return False

    # Check condition types
    is_a_fault_reset = (
        regex_pattern_check(fault_reset_comment, comment_a) and negated_a == "false"
    )
    is_b_autorun = (
        regex_pattern_check(autorun_comment, comment_b) and negated_b == "true"
    )

    return is_a_fault_reset and is_b_autorun


# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================


def detection_range_programwise(
    fault_section_first_rung_df: pd.DataFrame, program_name: str
) -> dict:

    logger.info(f"Executing rule 55 detection range on program {program_name}")

    first_rung_fault_section_block_df = fault_section_first_rung_df[
        fault_section_first_rung_df["OBJECT_TYPE_LIST"] == "Block"
    ]
    if not first_rung_fault_section_block_df.empty:
        for _, first_rung_all_block_row in first_rung_fault_section_block_df.iterrows():
            attr = ast.literal_eval(first_rung_all_block_row["ATTRIBUTES"])
            block_type = attr.get("typeName", "")
            if (
                block_type
                and isinstance(block_type, str)
                and block_type.lower() == "clear"
            ):
                return {"status": "OK", "rung_number": first_rung_all_block_row["RUNG"]}
    return {"status": "NG", "rung_number": -1}


def check_detail_1_programwise(detection_result: dict) -> dict:

    logger.info(f"Executing rule 55 and check detail 1")

    cc1_result = {}

    cc1_result["cc"] = "cc1"
    cc1_result["status"] = detection_result["status"]
    cc1_result["check_number"] = 1
    cc1_result["target_coil"] = ""
    cc1_result["rung_number"] = detection_result["rung_number"]

    return cc1_result


def check_detail_2_programwise(fault_section_first_rung_df: pd.DataFrame) -> dict:

    logger.info(f"Executing rule no 55 check detail 2")

    clear_block_details = get_block_connections(fault_section_first_rung_df)
    first_clear_block = False

    input_val = ""
    output_val = ""
    inout_counter = 0
    status = "NG"
    cc2_result = {}

    if clear_block_details:
        for block_data in clear_block_details:
            if "Clear" in block_data and not first_clear_block:
                for param in block_data["Clear"]:
                    print("param:", param)
                    for key in param:
                        if key.lower() == "inout":  # using .lower() comparison
                            if inout_counter == 0:
                                input_val = param[key]
                            elif inout_counter == 1:
                                output_val = param[key]
                            inout_counter += 1
                first_clear_block = True

    if (
        isinstance(input_val, list)
        and input_val
        and isinstance(output_val, list)
        and output_val
    ):
        if "AL" in input_val[0] and "AL" in output_val[0]:
            status = "OK"

    cc2_result["cc"] = "cc2"
    cc2_result["status"] = status
    cc2_result["check_number"] = 2
    cc2_result["target_coil"] = ""
    cc2_result["rung_number"] = -1

    return cc2_result


def check_detail_3_programwise(
    fault_section_first_rung_df: pd.DataFrame,
    cc2_status: str,
    fault_reset_comment: str,
    autorun_comment: str,
    program_name: str,
    program_comment_data: str,
) -> dict:

    logger.info(f"Executing rule no 55 check detail 3 in")

    # contact_block_first_rung_df = fault_section_first_rung_df[fault_section_first_rung_df['OBJECT_TYPE_LIST'] == 'Contact'  & fault_section_first_rung_df['OBJECT_TYPE_LIST'] == 'Block']
    contact_block_first_rung_df = fault_section_first_rung_df[
        (fault_section_first_rung_df["OBJECT_TYPE_LIST"] == "Contact")
        | (fault_section_first_rung_df["OBJECT_TYPE_LIST"] == "Block")
    ]

    is_contact_block_series = False
    rung_number = -1

    try:
        if cc2_status == "OK" and not contact_block_first_rung_df.empty:
            row0, row1, row2 = (
                contact_block_first_rung_df.iloc[0],
                contact_block_first_rung_df.iloc[1],
                contact_block_first_rung_df.iloc[2],
            )

            if (
                row0["OBJECT_TYPE_LIST"] == "Contact"
                and row1["OBJECT_TYPE_LIST"] == "Contact"
                and row2["OBJECT_TYPE_LIST"] == "Block"
            ):
                attr_clear = ast.literal_eval(row2["ATTRIBUTES"])
                if attr_clear.get("typeName", "").lower() == "clear":
                    # Check both permutations of contacts
                    if is_valid_contact_pair_programwise(
                        row0,
                        row1,
                        row2,
                        fault_reset_comment,
                        autorun_comment,
                        program_name,
                        program_comment_data,
                    ) or is_valid_contact_pair_programwise(
                        row1,
                        row0,
                        row2,
                        fault_reset_comment,
                        autorun_comment,
                        program_name,
                        program_comment_data,
                    ):
                        is_contact_block_series = True
                        rung_number = row0["RUNG"]
                    else:
                        is_contact_block_series = False
                else:
                    is_contact_block_series = False
            else:
                is_contact_block_series = False
        else:
            is_contact_block_series = False
    except:
        is_contact_block_series = False

    if is_contact_block_series is True:
        status = "OK"
    else:
        status = "NG"

    cc3_result = {}

    cc3_result["cc"] = "cc3"
    cc3_result["status"] = status
    cc3_result["check_number"] = 3
    cc3_result["target_coil"] = ""
    cc3_result["rung_number"] = rung_number

    return cc3_result


# ============================== Function-Wise Function Definitions ===============================
# These functions perform operations specific to each function, supporting rule validations and logic checks.
# ===============================================================================================


def detection_range_functionwise(
    fault_section_first_rung_df: pd.DataFrame, function_name: str
) -> dict:

    logger.info(f"Executing rule 55 detection range on function {function_name}")

    first_rung_fault_section_block_df = fault_section_first_rung_df[
        fault_section_first_rung_df["OBJECT_TYPE_LIST"] == "Block"
    ]

    if not first_rung_fault_section_block_df.empty:
        for _, first_rung_all_block_row in first_rung_fault_section_block_df.iterrows():
            attr = ast.literal_eval(first_rung_all_block_row["ATTRIBUTES"])
            block_type = attr.get("typeName", "")
            if (
                block_type
                and isinstance(block_type, str)
                and block_type.lower() == "clear"
            ):
                return {"status": "OK", "rung_number": first_rung_all_block_row["RUNG"]}
    return {"status": "NG", "rung_number": -1}


def check_detail_1_functionwise(detection_result: dict) -> dict:

    logger.info(f"Executing rule 55 and check detail 1")

    cc1_result = {}

    cc1_result["cc"] = "cc1"
    cc1_result["status"] = detection_result["status"]
    cc1_result["check_number"] = 1
    cc1_result["target_coil"] = ""
    cc1_result["rung_number"] = detection_result["rung_number"]

    return cc1_result


def check_detail_2_functionwise(fault_section_first_rung_df: pd.DataFrame) -> dict:

    logger.info(f"Executing rule no 55 check detail 2")

    clear_block_details = get_block_connections(fault_section_first_rung_df)
    first_clear_block = False

    input_val = ""
    output_val = ""
    inout_counter = 0
    status = "NG"
    cc2_result = {}

    if clear_block_details:
        for block_data in clear_block_details:
            if "Clear" in block_data and not first_clear_block:
                for param in block_data["Clear"]:
                    print("param:", param)
                    for key in param:
                        if key.lower() == "inout":  # using .lower() comparison
                            if inout_counter == 0:
                                input_val = param[key]
                            elif inout_counter == 1:
                                output_val = param[key]
                            inout_counter += 1
                first_clear_block = True

    if (
        isinstance(input_val, list)
        and input_val
        and isinstance(output_val, list)
        and output_val
    ):
        if "AL" in input_val[0] and "AL" in output_val[0]:
            status = "OK"

    cc2_result["cc"] = "cc2"
    cc2_result["status"] = status
    cc2_result["check_number"] = 2
    cc2_result["target_coil"] = ""
    cc2_result["rung_number"] = -1

    return cc2_result


def check_detail_3_functionwise(
    fault_section_first_rung_df: pd.DataFrame,
    cc2_status: str,
    fault_reset_comment: str,
    autorun_comment: str,
    function_name: str,
    function_comment_data: str,
) -> dict:

    logger.info(f"Executing rule no 55 check detail 3 in")

    # contact_block_first_rung_df = fault_section_first_rung_df[fault_section_first_rung_df['OBJECT_TYPE_LIST'] == 'Contact'  & fault_section_first_rung_df['OBJECT_TYPE_LIST'] == 'Block']
    contact_block_first_rung_df = fault_section_first_rung_df[
        (fault_section_first_rung_df["OBJECT_TYPE_LIST"] == "Contact")
        | (fault_section_first_rung_df["OBJECT_TYPE_LIST"] == "Block")
    ]

    is_contact_block_series = False
    rung_number = -1

    try:
        if cc2_status == "OK" and not contact_block_first_rung_df.empty:
            row0, row1, row2 = (
                contact_block_first_rung_df.iloc[0],
                contact_block_first_rung_df.iloc[1],
                contact_block_first_rung_df.iloc[2],
            )

            if (
                row0["OBJECT_TYPE_LIST"] == "Contact"
                and row1["OBJECT_TYPE_LIST"] == "Contact"
                and row2["OBJECT_TYPE_LIST"] == "Block"
            ):
                attr_clear = ast.literal_eval(row2["ATTRIBUTES"])
                if attr_clear.get("typeName", "").lower() == "clear":
                    # Check both permutations of contacts
                    if is_valid_contact_pair_functionwise(
                        row0,
                        row1,
                        row2,
                        fault_reset_comment,
                        autorun_comment,
                        function_name,
                        function_comment_data,
                    ) or is_valid_contact_pair_functionwise(
                        row1,
                        row0,
                        row2,
                        fault_reset_comment,
                        autorun_comment,
                        function_name,
                        function_comment_data,
                    ):
                        is_contact_block_series = True
                        rung_number = row0["RUNG"]
                    else:
                        is_contact_block_series = False
                else:
                    is_contact_block_series = False
            else:
                is_contact_block_series = False
        else:
            is_contact_block_series = False
    except:
        is_contact_block_series = False

    if is_contact_block_series is True:
        status = "OK"
    else:
        status = "NG"

    cc3_result = {}

    cc3_result["cc"] = "cc3"
    cc3_result["status"] = status
    cc3_result["check_number"] = 3
    cc3_result["target_coil"] = ""
    cc3_result["rung_number"] = rung_number

    return cc3_result


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_55_programwise(
    input_program_file: str, input_program_comment_file: str
) -> pd.DataFrame:

    logger.info("Starting execution of Rule 55")

    try:
        program_df = pd.read_csv(input_program_file)
        with open(input_program_comment_file, "r", encoding="utf-8") as file:
            program_comment_data = json.load(file)

        unique_program_values = program_df["PROGRAM"].unique()
        output_rows = []

        for program_name in unique_program_values:
            logger.info(f"Executing rule 55 in Program {program_name}")

            current_program_df = program_df[program_df["PROGRAM"] == program_name]

            unique_section_name = [
                val.lower() for val in current_program_df["BODY"].unique()
            ]

            if "fault" in unique_section_name:

                fault_section_df = current_program_df[
                    current_program_df["BODY"].str.lower() == fault_section_name
                ]
                if not fault_section_df.empty:
                    fault_section_first_rung_df = fault_section_df[
                        fault_section_df["RUNG"] == 1
                    ]

                    # Run detection range logic as per Rule 24
                    detection_result = detection_range_programwise(
                        fault_section_first_rung_df=fault_section_first_rung_df,
                        program_name=program_name,
                    )

                    if detection_result["status"] == "OK":
                        cc1_result = check_detail_1_programwise(
                            detection_result=detection_result
                        )
                        cc2_result = check_detail_2_programwise(
                            fault_section_first_rung_df=fault_section_first_rung_df
                        )
                        cc2_status = cc2_result["status"]
                        cc3_result = check_detail_3_programwise(
                            fault_section_first_rung_df=fault_section_first_rung_df,
                            cc2_status=cc2_status,
                            fault_reset_comment=fault_reset_comment,
                            autorun_comment=autorun_comment,
                            program_name=program_name,
                            program_comment_data=program_comment_data,
                        )

                        all_cc_result = [cc1_result, cc2_result, cc3_result]
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
                                    "Section": fault_section_name,
                                    "RungNo": rung_number,
                                    "Target": target_outcoil,
                                    "CheckItem": rule_55_check_item,
                                    "Detail": ng_name,
                                    "Status": "",
                                }
                            )
                            # output_rows.append({
                            #     "TASK_NAME": program_name,
                            #     "SECTION_NAME": fault_section_name,
                            #     "RULE_NUMBER": "55",
                            #     "CHECK_NUMBER": check_number,
                            #     "RUNG_NUMBER": -1 if rung_number < 0 else rung_number-1,
                            #     "RULE_CONTENT": rule_content_55,
                            #     "CHECK_CONTENT": check_detail_content.get(cc_result.get('cc')),
                            #     "STATUS": cc_result.get('status'),
                            #     "Target_outcoil" : target_outcoil,
                            #     "NG_EXPLANATION": ng_name
                            # })
                    else:

                        output_rows.append(
                            {
                                "Result": "NG",
                                "Task": program_name,
                                "Section": fault_section_name,
                                "RungNo": -1,
                                "Target": "",
                                "CheckItem": rule_55_check_item,
                                "Detail": "Detection Range not found",
                                "Status": "",
                            }
                        )
                        # output_rows.append({
                        #     "TASK_NAME": program_name,
                        #     "SECTION_NAME": fault_section_name,
                        #     "RULE_NUMBER": "55",
                        #     "CHECK_NUMBER": 1,
                        #     "RUNG_NUMBER": -1 if rung_number < 0 else rung_number-1,
                        #     "RULE_CONTENT": rule_content_55,
                        #     "CHECK_CONTENT": "",
                        #     "STATUS": "NG",
                        #     "Target_outcoil" : "",
                        #     "NG_EXPLANATION": ""
                        # })
                else:
                    output_rows.append(
                        {
                            "Result": "NG",
                            "Task": program_name,
                            "Section": fault_section_name,
                            "RungNo": -1,
                            "Target": "",
                            "CheckItem": rule_55_check_item,
                            "Detail": "",
                            "Status": "",
                        }
                    )
                    # output_rows.append({
                    #     "TASK_NAME": program_name,
                    #     "SECTION_NAME": fault_section_name,
                    #     "RULE_NUMBER": "55",
                    #     "CHECK_NUMBER": 1,
                    #     "RUNG_NUMBER": "",
                    #     "RULE_CONTENT": rule_content_55,
                    #     "CHECK_CONTENT": "",
                    #     "STATUS": "NG",
                    #     "Target_outcoil" : "",
                    #     "NG_EXPLANATION": ""
                    # })

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


# ============================== Function-Wise Execution Starts Here ===============================
def execute_rule_55_functionwise(
    input_function_file: str, input_function_comment_file: str
) -> pd.DataFrame:

    logger.info("Starting execution of Rule 55")

    try:

        function_df = pd.read_csv(input_function_file)
        with open(input_function_comment_file, "r", encoding="utf-8") as file:
            function_comment_data = json.load(file)

        unique_function_values = function_df["FUNCTION_BLOCK"].unique()
        output_rows = []

        for function_name in unique_function_values:
            logger.info(f"Executing rule 55 in function {function_name}")

            current_function_df = function_df[
                function_df["FUNCTION_BLOCK"] == function_name
            ]
            unique_section_name = [
                val.lower() for val in current_function_df["BODY_TYPE"].unique()
            ]

            if "fault" in unique_section_name:

                fault_section_df = current_function_df[
                    current_function_df["BODY_TYPE"].str.lower() == fault_section_name
                ]
                if not fault_section_df.empty:
                    fault_section_first_rung_df = fault_section_df[
                        fault_section_df["RUNG"] == 1
                    ]

                    # Run detection range logic as per Rule 24
                    detection_result = detection_range_functionwise(
                        fault_section_first_rung_df=fault_section_first_rung_df,
                        function_name=function_name,
                    )

                    if detection_result["status"] == "OK":
                        cc1_result = check_detail_1_functionwise(
                            detection_result=detection_result
                        )
                        cc2_result = check_detail_2_functionwise(
                            fault_section_first_rung_df=fault_section_first_rung_df
                        )
                        cc2_status = cc2_result["status"]
                        cc3_result = check_detail_3_functionwise(
                            fault_section_first_rung_df=fault_section_first_rung_df,
                            cc2_status=cc2_status,
                            fault_reset_comment=fault_reset_comment,
                            autorun_comment=autorun_comment,
                            function_name=function_name,
                            function_comment_data=function_comment_data,
                        )

                        all_cc_result = [cc1_result, cc2_result, cc3_result]
                        for cc_result in all_cc_result:
                            ng_name = (
                                ng_content.get(cc_result.get("cc", ""))
                                if cc_result.get("status") == "NG"
                                else ""
                            )
                            rung_number = (
                                cc_result.get("rung_number")
                                if cc_result.get("rung_number")
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
                                    "Task": function_name,
                                    "Section": fault_section_name,
                                    "RungNo": rung_number,
                                    "Target": target_outcoil,
                                    "CheckItem": rule_55_check_item,
                                    "Detail": ng_name,
                                    "Status": "",
                                }
                            )

                            # output_rows.append({
                            #     "TASK_NAME": function_name,
                            #     "SECTION_NAME": fault_section_name,
                            #     "RULE_NUMBER": "55",
                            #     "CHECK_NUMBER": check_number,
                            #     "RUNG_NUMBER": -1 if rung_number < 0 else rung_number-1,
                            #     "RULE_CONTENT": rule_content_55,
                            #     "CHECK_CONTENT": check_detail_content.get(cc_result.get('cc')),
                            #     "STATUS": cc_result.get('status'),
                            #     "Target_outcoil" : target_outcoil,
                            #     "NG_EXPLANATION": ng_name
                            # })
                    else:
                        output_rows.append(
                            {
                                "Result": "NG",
                                "Task": function_name,
                                "Section": fault_section_name,
                                "RungNo": -1,
                                "Target": "",
                                "CheckItem": rule_55_check_item,
                                "Detail": "Detection Range not found",
                                "Status": "",
                            }
                        )
                        # output_rows.append({
                        #     "TASK_NAME": function_name,
                        #     "SECTION_NAME": fault_section_name,
                        #     "RULE_NUMBER": "55",
                        #     "CHECK_NUMBER": 1,
                        #     "RUNG_NUMBER": -1 if rung_number < 0 else rung_number-1,
                        #     "RULE_CONTENT": rule_content_55,
                        #     "CHECK_CONTENT": "",
                        #     "STATUS": "NG",
                        #     "Target_outcoil" : "",
                        #     "NG_EXPLANATION": ""
                        # })
                else:
                    output_rows.append(
                        {
                            "Result": "NG",
                            "Task": function_name,
                            "Section": fault_section_name,
                            "RungNo": -1,
                            "Target": "",
                            "CheckItem": rule_55_check_item,
                            "Detail": "",
                            "Status": "",
                        }
                    )
                    # ou
                    # output_rows.append({
                    #     "TASK_NAME": function_name,
                    #     "SECTION_NAME": fault_section_name,
                    #     "RULE_NUMBER": "55",
                    #     "CHECK_NUMBER": 1,
                    #     "RUNG_NUMBER": "",
                    #     "RULE_CONTENT": rule_content_55,
                    #     "CHECK_CONTENT": "",
                    #     "STATUS": "NG",
                    #     "Target_outcoil" : "",
                    #     "NG_EXPLANATION": ""
                    # })

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


# if __name__=='__main__':

#     input_program_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/GithubCode/rules_personal/data_modelling/data_model_Rule_46_56_NG_v2/data_model_Rule_46_56_NG_v2_programwise.csv"
#     input_program_comment_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/GithubCode/rules_personal/data_modelling/data_model_Rule_46_56_NG_v2/data_model_Rule_46_56_NG_v2_programwise.json"
#     input_function_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/GithubCode/rules_personal/data_modelling/data_model_Rule_46_56_NG_v2/data_model_Rule_46_56_NG_v2_functionwise.csv"
#     input_function_comment_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/GithubCode/rules_personal/data_modelling/data_model_Rule_46_56_NG_v2/data_model_Rule_46_56_NG_v2_functionwise.json"
#     output_folder_path = 'C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Rules_implementation/pythoncode/output_csv/'
#     program_output_file = 'Rule_55_programwise_NG_v2.csv'
#     function_output_file = 'Rule_55_functionwise_NG_v2.csv'

#     final_csv = execute_rule_55_programwise(input_program_file=input_program_file, input_program_comment_file=input_program_comment_file)
#     final_csv.to_csv(f"{output_folder_path}/{program_output_file}", index=False, encoding='utf-8-sig')

#     final_csv = execute_rule_55_functionwise(input_function_file=input_function_file, input_function_comment_file=input_function_comment_file)
#     final_csv.to_csv(f"{output_folder_path}/{function_output_file}", index=False, encoding='utf-8-sig')
