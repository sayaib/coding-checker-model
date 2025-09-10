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
from .rule_93_self_holding import check_self_holding

# ============================ Rule 34: Definitions, Content, and Configuration Details ============================

autorun_section_name = "autorun"
autorun_section_name_with_star = "autorun★"
preparation_section_name = "preparation"
fault_section_name = "fault"

rule_93_check_item = "Rule of Judgment Circuit"

normal_comment = "正常"
abnormal_comment = "異常"
inspection_comment = "検査"

check_detail_content = {
    "cc1": "Check that the variable detected in step ⑤ for the outcoil is connected at least once as an A contact inside the self-holding condition of an outcoil whose variable name starts with “AL” within the “Fault” section of the same task."
}

ng_content = {"cc1": "分岐・判定回路なのに排他がとられていないためNG"}


# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================


def detection_range_programwise(
    autorun_preparation_section_df: pd.DataFrame,
    program: str,
    program_comment_data: dict,
    normal_comment: str,
    abnormal_comment: str,
    inspection_comment: str,
):

    all_match_coil = []

    coil_df = autorun_preparation_section_df[
        autorun_preparation_section_df["OBJECT_TYPE_LIST"].str.lower() == "coil"
    ]
    if not coil_df.empty:
        # Group by BODY and RUNG, count occurrences
        grouped = coil_df.groupby(["BODY", "RUNG"]).size().reset_index(name="count")

        # Filter where count >= 2
        filtered = grouped[grouped["count"] >= 2]

        # For each BODY and RUNG where count >= 2, perform action
        for _, row in filtered.iterrows():
            body = row["BODY"]
            rung = row["RUNG"]
            count = row["count"]

            print(
                f"Program: {program}, BODY: {body}, RUNG: {rung}, Coil Count: {count}"
            )

            two_or_more_coil_df = coil_df[
                (coil_df["BODY"] == body) & (coil_df["RUNG"] == rung)
            ]
            ok_operand = ""
            ng_operand = ""
            for _, coil_row in two_or_more_coil_df.iterrows():
                attr = ast.literal_eval(coil_row["ATTRIBUTES"])
                coil_operand = attr.get("operand")
                if isinstance(coil_operand, str) and coil_operand:
                    coil_comment = get_the_comment_from_program(
                        coil_operand, program, program_comment_data
                    )
                    if isinstance(coil_comment, list) and coil_comment:
                        if regex_pattern_check(
                            "OK", coil_comment
                        ) or regex_pattern_check(normal_comment, coil_comment):
                            ok_operand = coil_operand
                        elif (
                            (
                                regex_pattern_check("NG", coil_comment)
                                or regex_pattern_check(abnormal_comment, coil_comment)
                            )
                            or (
                                regex_pattern_check("NG", coil_comment)
                                and (
                                    not regex_pattern_check(
                                        inspection_comment, coil_comment
                                    )
                                )
                            )
                            and ok_operand != coil_operand
                        ):
                            ng_operand = coil_operand

            if ok_operand and ng_operand:
                all_match_coil.append(ng_operand)

    return all_match_coil


def check_detail_1_programwise(fault_section_df: dict, detection_result: str):
    pass

    all_match_result = []
    if detection_result and not fault_section_df.empty:
        for detected_coil in detection_result:
            status = "NG"
            rung_number = -1

            fault_contact_df = fault_section_df[
                (fault_section_df["OBJECT_TYPE_LIST"].str.lower() == "contact")
            ]
            if not fault_contact_df.empty:
                for _, contact_row in fault_contact_df.iterrows():
                    attr = ast.literal_eval(contact_row["ATTRIBUTES"])
                    contact_fault_operand = attr.get("operand")
                    negated = attr.get("negated")
                    if (
                        isinstance(contact_fault_operand, str)
                        and contact_fault_operand
                        and negated == "false"
                    ):
                        if contact_fault_operand == detected_coil:
                            match_contact_coil = fault_section_df[
                                (
                                    fault_section_df["OBJECT_TYPE_LIST"].str.lower()
                                    == "coil"
                                )
                                & (fault_section_df["BODY"] == contact_row["BODY"])
                                & (fault_section_df["RUNG"] == contact_row["RUNG"])
                            ]

                            match_rung_df = fault_section_df[
                                (fault_section_df["BODY"] == contact_row["BODY"])
                                & (fault_section_df["RUNG"] == contact_row["RUNG"])
                            ]

                            coil_operand = (
                                match_contact_coil["ATTRIBUTES"]
                                .apply(
                                    lambda x: (
                                        ast.literal_eval(x).get("operand")
                                        if isinstance(ast.literal_eval(x), dict)
                                        else None
                                    )
                                )
                                .values[0]
                                if not match_contact_coil.empty
                                else None
                            )
                            if not match_contact_coil.empty:
                                self_holding_contact = check_self_holding(match_rung_df)
                                if coil_operand in self_holding_contact:
                                    status = "OK"
                                    rung_number = contact_row["RUNG"]
                                    break

            all_match_result.append(
                {
                    "status": status,
                    "cc": "cc1",
                    "rung_number": rung_number,
                    "target_coil": detected_coil,
                    "check_number": 1,
                }
            )

    return all_match_result


# ============================== Function-Wise Function Definitions ===============================
# These functions perform operations specific to each function, supporting rule validations and logic checks.
# ===============================================================================================


def detection_range_functionwise(
    autorun_preparation_section_df: pd.DataFrame,
    function: str,
    function_comment_data: dict,
    normal_comment: str,
    abnormal_comment: str,
    inspection_comment: str,
):

    all_match_coil = []

    coil_df = autorun_preparation_section_df[
        autorun_preparation_section_df["OBJECT_TYPE_LIST"].str.lower() == "coil"
    ]
    if not coil_df.empty:
        # Group by BODY and RUNG, count occurrences
        grouped = coil_df.groupby(["BODY", "RUNG"]).size().reset_index(name="count")

        # Filter where count >= 2
        filtered = grouped[grouped["count"] >= 2]

        # For each BODY and RUNG where count >= 2, perform action
        for _, row in filtered.iterrows():
            body = row["BODY"]
            rung = row["RUNG"]
            count = row["count"]

            print(
                f"function: {function}, BODY: {body}, RUNG: {rung}, Coil Count: {count}"
            )

            two_or_more_coil_df = coil_df[
                (coil_df["BODY"] == body) & (coil_df["RUNG"] == rung)
            ]
            ok_operand = ""
            ng_operand = ""
            for _, coil_row in two_or_more_coil_df.iterrows():
                attr = ast.literal_eval(coil_row["ATTRIBUTES"])
                coil_operand = attr.get("operand")
                if isinstance(coil_operand, str) and coil_operand:
                    coil_comment = get_the_comment_from_function(
                        coil_operand, function, function_comment_data
                    )
                    if isinstance(coil_comment, list) and coil_comment:
                        if regex_pattern_check(
                            "OK", coil_comment
                        ) or regex_pattern_check(normal_comment, coil_comment):
                            ok_operand = coil_operand
                        elif (
                            (
                                regex_pattern_check("NG", coil_comment)
                                or regex_pattern_check(abnormal_comment, coil_comment)
                            )
                            or (
                                regex_pattern_check("NG", coil_comment)
                                and (
                                    not regex_pattern_check(
                                        inspection_comment, coil_comment
                                    )
                                )
                            )
                            and ok_operand != coil_operand
                        ):
                            ng_operand = coil_operand

            if ok_operand and ng_operand:
                all_match_coil.append(ng_operand)

    return all_match_coil


def check_detail_1_functionwise(fault_section_df: dict, detection_result: str):
    pass

    all_match_result = []
    if detection_result and not fault_section_df.empty:
        for detected_coil in detection_result:
            status = "NG"
            rung_number = -1

            fault_contact_df = fault_section_df[
                (fault_section_df["OBJECT_TYPE_LIST"].str.lower() == "contact")
            ]
            if not fault_contact_df.empty:
                for _, contact_row in fault_contact_df.iterrows():
                    attr = ast.literal_eval(contact_row["ATTRIBUTES"])
                    contact_fault_operand = attr.get("operand")
                    negated = attr.get("negated")
                    if (
                        isinstance(contact_fault_operand, str)
                        and contact_fault_operand
                        and negated == "false"
                    ):
                        if contact_fault_operand == detected_coil:
                            match_contact_coil = fault_section_df[
                                (
                                    fault_section_df["OBJECT_TYPE_LIST"].str.lower()
                                    == "coil"
                                )
                                & (fault_section_df["BODY"] == contact_row["BODY"])
                                & (fault_section_df["RUNG"] == contact_row["RUNG"])
                            ]

                            match_rung_df = fault_section_df[
                                (fault_section_df["BODY"] == contact_row["BODY"])
                                & (fault_section_df["RUNG"] == contact_row["RUNG"])
                            ]

                            coil_operand = (
                                match_contact_coil["ATTRIBUTES"]
                                .apply(
                                    lambda x: (
                                        ast.literal_eval(x).get("operand")
                                        if isinstance(ast.literal_eval(x), dict)
                                        else None
                                    )
                                )
                                .values[0]
                                if not match_contact_coil.empty
                                else None
                            )
                            if not match_contact_coil.empty:
                                self_holding_contact = check_self_holding(match_rung_df)
                                if coil_operand in self_holding_contact:
                                    status = "OK"
                                    rung_number = contact_row["RUNG"]
                                    break

            all_match_result.append(
                {
                    "status": status,
                    "cc": "cc1",
                    "rung_number": rung_number,
                    "target_coil": detected_coil,
                    "check_number": 1,
                }
            )

    return all_match_result


# ============================== program-Wise Execution Starts Here ===============================
def execute_rule_93_programwise(
    input_program_file: str, program_comment_file: str
) -> pd.DataFrame:

    logger.info("Rule 35 Start executing rule 1 program wise")

    try:

        output_rows = []

        program_df = pd.read_csv(input_program_file)
        with open(program_comment_file, "r", encoding="utf-8") as file:
            program_comment_data = json.load(file)

        unique_program_values = program_df["PROGRAM"].unique()

        for program in unique_program_values:

            current_program_df = program_df[program_df["PROGRAM"] == program]
            autorun_preparation_section_df = current_program_df[
                current_program_df["BODY"]
                .str.lower()
                .isin(
                    [
                        autorun_section_name,
                        autorun_section_name_with_star,
                        preparation_section_name,
                    ]
                )
            ]
            fault_section_df = current_program_df[
                current_program_df["BODY"].str.lower() == fault_section_name
            ]

            detection_result = detection_range_programwise(
                autorun_preparation_section_df=autorun_preparation_section_df,
                program=program,
                program_comment_data=program_comment_data,
                normal_comment=normal_comment,
                abnormal_comment=abnormal_comment,
                inspection_comment=inspection_comment,
            )

            cc1_result = check_detail_1_programwise(
                fault_section_df=fault_section_df, detection_result=detection_result
            )

            for cc_result in cc1_result:
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
                    cc_result.get("target_coil") if cc_result.get("target_coil") else ""
                )

                output_rows.append(
                    {
                        "Result": cc_result.get("status"),
                        "Task": program,
                        "Section": "Fault",
                        "RungNo": rung_number,
                        "Target": target_outcoil,
                        "CheckItem": rule_93_check_item,
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
        logger.error(f"Rule 16 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}


# ============================== function-Wise Execution Starts Here ===============================
def execute_rule_93_functionwise(
    input_function_file: str, function_comment_file: str
) -> pd.DataFrame:

    logger.info("Rule 35 Start executing rule 1 function wise")

    try:

        output_rows = []

        function_df = pd.read_csv(input_function_file)
        with open(function_comment_file, "r", encoding="utf-8") as file:
            function_comment_data = json.load(file)

        unique_function_values = function_df["FUNCTION_BLOCK"].unique()

        for function in unique_function_values:

            current_function_df = function_df[function_df["FUNCTION_BLOCK"] == function]
            autorun_preparation_section_df = current_function_df[
                current_function_df["BODY_TYPE"]
                .str.lower()
                .isin(
                    [
                        autorun_section_name,
                        autorun_section_name_with_star,
                        preparation_section_name,
                    ]
                )
            ]
            fault_section_df = current_function_df[
                current_function_df["BODY_TYPE"].str.lower() == fault_section_name
            ]

            detection_result = detection_range_functionwise(
                autorun_preparation_section_df=autorun_preparation_section_df,
                function=function,
                function_comment_data=function_comment_data,
                normal_comment=normal_comment,
                abnormal_comment=abnormal_comment,
                inspection_comment=inspection_comment,
            )

            cc1_result = check_detail_1_functionwise(
                fault_section_df=fault_section_df, detection_result=detection_result
            )

            for cc_result in cc1_result:
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
                    cc_result.get("target_coil") if cc_result.get("target_coil") else ""
                )

                output_rows.append(
                    {
                        "Result": cc_result.get("status"),
                        "Task": function,
                        "Section": "Fault",
                        "RungNo": rung_number,
                        "Target": target_outcoil,
                        "CheckItem": rule_93_check_item,
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
        logger.error(f"Rule 16 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}
