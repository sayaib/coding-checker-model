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

# from .rule_93_self_holding import check_self_holding

# ============================ Rule 34: Definitions, Content, and Configuration Details ============================

autorun_section_name = "autorun"
autorun_section_name_with_star = "autorun★"
preparation_section_name = "preparation"

rule_94_check_item = "Rule of Branch Circuit (Action Judgment)"

normal_comment = "正常"
with_1_comment = "あり"
with_2_comment = "有"
confirm_comment = "確認"
abnormal_comment = "異常"
without_1_comment = "なし"
without_2_comment = "無"


check_detail_content = {
    "cc1": "Check that the B contact of the variable detected in ④.2 is connected to the outcoil condition detected in ④.1.",
    "cc2": "Check that the B contact of the variable detected in ④.1 is connected to the outcoil condition detected in ④.2.",
}

ng_content = {
    "cc1": "分岐・判定回路なのに排他がとられていないためNG",
    "cc2": "分岐・判定回路なのに排他がとられていないためNG",
}

# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================


def detection_range_programwise(
    autorun_preparation_section_df: pd.DataFrame,
    normal_comment: str,
    with_1_comment: str,
    with_2_comment: str,
    confirm_comment: str,
    abnormal_comment: str,
    without_1_comment: str,
    without_2_comment: str,
    program: str,
    program_comment_data: dict,
):
    pass

    coil_df = autorun_preparation_section_df[
        autorun_preparation_section_df["OBJECT_TYPE_LIST"].str.lower() == "coil"
    ]
    all_match_coil = []

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
                        if (
                            regex_pattern_check("OK", coil_comment)
                            or regex_pattern_check(normal_comment, coil_comment)
                            or regex_pattern_check(with_1_comment, coil_comment)
                            or regex_pattern_check(with_2_comment, coil_comment)
                            or regex_pattern_check(confirm_comment, coil_comment)
                        ):
                            ok_operand = coil_operand
                        elif (
                            regex_pattern_check("NG", coil_comment)
                            or regex_pattern_check(abnormal_comment, coil_comment)
                            or regex_pattern_check(without_1_comment, coil_comment)
                            or regex_pattern_check(without_2_comment, coil_comment)
                            or regex_pattern_check(confirm_comment, coil_comment)
                            and ok_operand != coil_operand
                        ):
                            ng_operand = coil_operand

            if ok_operand and ng_operand:
                all_match_coil.append([ok_operand, ng_operand, rung, body])

    return all_match_coil


def get_split_cc1_cc2_result_programwise(
    autorun_preparation_section_df: pd.DataFrame, detection_result: list
):

    all_data_result = []

    for detection_data in detection_result:
        ok_operand = detection_data[0]
        ng_operand = detection_data[1]
        rung_number = detection_data[2]
        section_name = detection_data[3]

        ok_data_df = pd.DataFrame()
        ng_data_df = pd.DataFrame()

        # Filter rows for the specific section and rung
        match_runf_df = autorun_preparation_section_df[
            (autorun_preparation_section_df["BODY"] == section_name)
            & (autorun_preparation_section_df["RUNG"] == rung_number)
        ]

        # Filter for coil entries
        coil_df = match_runf_df[match_runf_df["OBJECT_TYPE_LIST"].str.lower() == "coil"]

        print("detection_data", detection_data)
        if not coil_df.empty:

            start_index = 1
            for coil_index, coil_row in coil_df.iterrows():
                attr = ast.literal_eval(coil_row["ATTRIBUTES"])
                coil_operand = attr.get("operand")

                if coil_operand == ok_operand:
                    end_index = coil_index
                    ok_data_df = match_runf_df.loc[start_index:end_index]
                    print(
                        "ok start_index",
                        start_index,
                        "end_index",
                        end_index,
                        ok_data_df,
                    )
                    start_index = coil_index + 1

                elif coil_operand == ng_operand:
                    end_index = coil_index
                    ng_data_df = match_runf_df.loc[start_index:end_index]
                    print(
                        "ng start_index",
                        start_index,
                        "end_index",
                        end_index,
                        ng_data_df,
                    )
                    start_index = coil_index + 1

                else:
                    start_index = coil_index

                if not ok_data_df.empty and not ng_data_df.empty:
                    break

        cc1_status = "NG"
        cc2_status = "NG"

        if not ok_data_df.empty:
            contact_df = ok_data_df[
                ok_data_df["OBJECT_TYPE_LIST"].str.lower() == "contact"
            ]

            if not contact_df.empty:
                for _, contact_row in contact_df.iterrows():
                    attr = ast.literal_eval(contact_row["ATTRIBUTES"])
                    contact_operand = attr.get("operand")
                    negated_operand = attr.get("negated")

                    if (
                        contact_operand
                        and isinstance(contact_operand, str)
                        and negated_operand == "true"
                    ):
                        if contact_operand == ng_operand:
                            cc1_status = "OK"

        if not ng_data_df.empty:
            contact_df = ng_data_df[
                ng_data_df["OBJECT_TYPE_LIST"].str.lower() == "contact"
            ]

            if not contact_df.empty:
                for _, contact_row in contact_df.iterrows():
                    attr = ast.literal_eval(contact_row["ATTRIBUTES"])
                    contact_operand = attr.get("operand")
                    negated_operand = attr.get("negated")

                    if (
                        contact_operand
                        and isinstance(contact_operand, str)
                        and negated_operand == "true"
                    ):
                        if contact_operand == ok_operand:
                            cc2_status = "OK"

        all_data_result.append(
            [cc1_status, ok_operand, cc2_status, ng_operand, rung_number, section_name]
        )

    return all_data_result


# ============================== Function-Wise Function Definitions ===============================
# These functions perform operations specific to each function, supporting rule validations and logic checks.
# ===============================================================================================


def detection_range_functionwise(
    autorun_preparation_section_df: pd.DataFrame,
    normal_comment: str,
    with_1_comment: str,
    with_2_comment: str,
    confirm_comment: str,
    abnormal_comment: str,
    without_1_comment: str,
    without_2_comment: str,
    function: str,
    function_comment_data: dict,
):
    pass

    coil_df = autorun_preparation_section_df[
        autorun_preparation_section_df["OBJECT_TYPE_LIST"].str.lower() == "coil"
    ]
    all_match_coil = []

    if not coil_df.empty:
        # Group by BODY and RUNG, count occurrences
        grouped = (
            coil_df.groupby(["BODY_TYPE", "RUNG"]).size().reset_index(name="count")
        )

        # Filter where count >= 2
        filtered = grouped[grouped["count"] >= 2]

        # For each BODY and RUNG where count >= 2, perform action
        for _, row in filtered.iterrows():
            body = row["BODY_TYPE"]
            rung = row["RUNG"]
            count = row["count"]

            print(
                f"function: {function}, BODY: {body}, RUNG: {rung}, Coil Count: {count}"
            )

            two_or_more_coil_df = coil_df[
                (coil_df["BODY_TYPE"] == body) & (coil_df["RUNG"] == rung)
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
                        if (
                            regex_pattern_check("OK", coil_comment)
                            or regex_pattern_check(normal_comment, coil_comment)
                            or regex_pattern_check(with_1_comment, coil_comment)
                            or regex_pattern_check(with_2_comment, coil_comment)
                            or regex_pattern_check(confirm_comment, coil_comment)
                        ):
                            ok_operand = coil_operand
                        elif (
                            regex_pattern_check("NG", coil_comment)
                            or regex_pattern_check(abnormal_comment, coil_comment)
                            or regex_pattern_check(without_1_comment, coil_comment)
                            or regex_pattern_check(without_2_comment, coil_comment)
                            or regex_pattern_check(confirm_comment, coil_comment)
                            and ok_operand != coil_operand
                        ):
                            ng_operand = coil_operand

            if ok_operand and ng_operand:
                all_match_coil.append([ok_operand, ng_operand, rung, body])

    return all_match_coil


def get_split_cc1_cc2_result_functionwise(
    autorun_preparation_section_df: pd.DataFrame, detection_result: list
):

    all_data_result = []

    for detection_data in detection_result:
        ok_operand = detection_data[0]
        ng_operand = detection_data[1]
        rung_number = detection_data[2]
        section_name = detection_data[3]

        ok_data_df = pd.DataFrame()
        ng_data_df = pd.DataFrame()

        match_runf_df = autorun_preparation_section_df[
            (autorun_preparation_section_df["BODY_TYPE"] == section_name)
            & (autorun_preparation_section_df["RUNG"] == rung_number)
        ]

        coil_df = match_runf_df[match_runf_df["OBJECT_TYPE_LIST"].str.lower() == "coil"]

        # Initialize empty DataFrames to collect all the rows in the ranges
        ok_data_df_final = pd.DataFrame()
        ng_data_df_final = pd.DataFrame()

        # Filter rows for the specific section and rung
        match_runf_df = autorun_preparation_section_df[
            (autorun_preparation_section_df["BODY_TYPE"] == section_name)
            & (autorun_preparation_section_df["RUNG"] == rung_number)
        ]

        # Filter for coil entries
        coil_df = match_runf_df[match_runf_df["OBJECT_TYPE_LIST"].str.lower() == "coil"]

        print("detection_data", detection_data)
        if not coil_df.empty:

            start_index = 1
            for coil_index, coil_row in coil_df.iterrows():
                attr = ast.literal_eval(coil_row["ATTRIBUTES"])
                coil_operand = attr.get("operand")

                if coil_operand == ok_operand:
                    end_index = coil_index
                    ok_data_df = match_runf_df.loc[start_index:end_index]
                    print(
                        "ok start_index",
                        start_index,
                        "end_index",
                        end_index,
                        ok_data_df,
                    )
                    start_index = coil_index + 1

                elif coil_operand == ng_operand:
                    end_index = coil_index
                    ng_data_df = match_runf_df.loc[start_index:end_index]
                    print(
                        "ng start_index",
                        start_index,
                        "end_index",
                        end_index,
                        ng_data_df,
                    )
                    start_index = coil_index + 1

                else:
                    start_index = coil_index

                if not ok_data_df.empty and not ng_data_df.empty:
                    break

        cc1_status = "NG"
        cc2_status = "NG"

        if not ok_data_df.empty:
            contact_df = ok_data_df[
                ok_data_df["OBJECT_TYPE_LIST"].str.lower() == "contact"
            ]

            if not contact_df.empty:
                for _, contact_row in contact_df.iterrows():
                    attr = ast.literal_eval(contact_row["ATTRIBUTES"])
                    contact_operand = attr.get("operand")
                    negated_operand = attr.get("negated")

                    if (
                        contact_operand
                        and isinstance(contact_operand, str)
                        and negated_operand == "true"
                    ):
                        if contact_operand == ng_operand:
                            cc1_status = "OK"

        if not ng_data_df.empty:
            contact_df = ng_data_df[
                ng_data_df["OBJECT_TYPE_LIST"].str.lower() == "contact"
            ]

            if not contact_df.empty:
                for _, contact_row in contact_df.iterrows():
                    attr = ast.literal_eval(contact_row["ATTRIBUTES"])
                    contact_operand = attr.get("operand")
                    negated_operand = attr.get("negated")

                    if (
                        contact_operand
                        and isinstance(contact_operand, str)
                        and negated_operand == "true"
                    ):
                        if contact_operand == ok_operand:
                            cc2_status = "OK"

        all_data_result.append(
            [cc1_status, ok_operand, cc2_status, ng_operand, rung_number, section_name]
        )

    return all_data_result


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_94_programwise(
    input_program_file: str, program_comment_file: str
) -> pd.DataFrame:

    logger.info("Rule 94 Start executing rule 1 program wise")

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

            detection_result = detection_range_programwise(
                autorun_preparation_section_df=autorun_preparation_section_df,
                normal_comment=normal_comment,
                with_1_comment=with_1_comment,
                with_2_comment=with_2_comment,
                confirm_comment=confirm_comment,
                abnormal_comment=abnormal_comment,
                without_1_comment=without_1_comment,
                without_2_comment=without_2_comment,
                program=program,
                program_comment_data=program_comment_data,
            )

            print("program", program)
            print("detection_result", detection_result)

            if detection_result:

                cc_analysis_results = get_split_cc1_cc2_result_programwise(
                    autorun_preparation_section_df=autorun_preparation_section_df,
                    detection_result=detection_result,
                )

                for cc_result_data in cc_analysis_results:
                    rung_no = cc_result_data[4]
                    section = cc_result_data[5]

                    # List of control check results: (name, status, operand)
                    control_checks = [
                        ("cc1", cc_result_data[0], cc_result_data[1]),
                        ("cc2", cc_result_data[2], cc_result_data[3]),
                    ]

                    for cc_name, status, operand in control_checks:
                        ng_name = ng_content.get(cc_name) if status == "NG" else ""

                        output_rows.append(
                            {
                                "Result": status,
                                "Task": program,
                                "Section": section,
                                "RungNo": rung_no,
                                "Target": operand,
                                "CheckItem": rule_94_check_item,
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
        logger.error(f"Rule 94 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}


# ============================== program-Wise Execution Starts Here ===============================
def execute_rule_94_functionwise(
    input_function_file: str, function_comment_file: str
) -> pd.DataFrame:

    logger.info("Rule 94 Start executing rule 1 function wise")

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

            detection_result = detection_range_functionwise(
                autorun_preparation_section_df=autorun_preparation_section_df,
                normal_comment=normal_comment,
                with_1_comment=with_1_comment,
                with_2_comment=with_2_comment,
                confirm_comment=confirm_comment,
                abnormal_comment=abnormal_comment,
                without_1_comment=without_1_comment,
                without_2_comment=without_2_comment,
                function=function,
                function_comment_data=function_comment_data,
            )

            print("function", function)
            print("detection_result", detection_result)

            if detection_result:

                cc_analysis_results = get_split_cc1_cc2_result_functionwise(
                    autorun_preparation_section_df=autorun_preparation_section_df,
                    detection_result=detection_result,
                )

                for cc_result_data in cc_analysis_results:
                    rung_no = cc_result_data[4]
                    section = cc_result_data[5]

                    # List of control check results: (name, status, operand)
                    control_checks = [
                        ("cc1", cc_result_data[0], cc_result_data[1]),
                        ("cc2", cc_result_data[2], cc_result_data[3]),
                    ]

                    for cc_name, status, operand in control_checks:
                        ng_name = ng_content.get(cc_name) if status == "NG" else ""

                        output_rows.append(
                            {
                                "Result": status,
                                "Task": function,
                                "Section": section,
                                "RungNo": rung_no,
                                "Target": operand,
                                "CheckItem": rule_94_check_item,
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
        logger.error(f"Rule 94 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}
