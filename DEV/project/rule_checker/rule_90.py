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
from .rule_90_self_holding import check_self_holding

# ============================ Rule 34: Definitions, Content, and Configuration Details ============================

autorun_section_name = "autorun"
autorun_section_name_with_star = "autorun★"
preparation_section_name = "preparation"

rule_90_check_item = "Rule of Choice Branch Circuit"

move_comment = "動作"
run_comment = "運転"
select_comment = "選択"
cycle_comment = "サイクル"
start_comment = "起動"
complete_comment = "完了"

check_detail_content = {
    "cc1": "Self-holding must exist.",
    "cc2": "The condition for the target outcoil is 'that all B contacts of the same variable detected within the same ring and belonging to outcoils other than itself' are connected in series outside the self-holding circuit.",
}

ng_content = {
    "cc1": "選択分岐回路において、自己保持がされていないためNG",
    "cc2": "選択分岐回路において、自分以外の接点での排他ができいないためNG",
}


# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================
def detection_range_programwise(
    autorun_preparation_section_df: pd.DataFrame,
    move_comment: str,
    run_comment: str,
    select_comment: str,
    cycle_comment: str,
    start_comment: str,
    complete_comment: str,
    program: str,
    program_comment_data: dict,
):
    pass

    coil_df = autorun_preparation_section_df[
        autorun_preparation_section_df["OBJECT_TYPE_LIST"].str.lower() == "coil"
    ]

    all_match_coil = []

    """
    Getting rung and section where two or more outcoil is given and matching comment as rule is given.
    """
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

            two_or_more_coil_df = coil_df[
                (coil_df["BODY"] == body) & (coil_df["RUNG"] == rung)
            ]

            all_match_status = []
            current_rung_match_coil = []

            for _, coil_row in two_or_more_coil_df.iterrows():
                attr = ast.literal_eval(coil_row["ATTRIBUTES"])
                coil_operand = attr.get("operand")
                if isinstance(coil_operand, str) and coil_operand:
                    coil_comment = get_the_comment_from_program(
                        coil_operand, program, program_comment_data
                    )
                    if isinstance(coil_comment, list) and coil_comment:
                        if (
                            regex_pattern_check(move_comment, coil_comment)
                            or regex_pattern_check(run_comment, coil_comment)
                            or regex_pattern_check(select_comment, coil_comment)
                            or regex_pattern_check(cycle_comment, coil_comment)
                            or regex_pattern_check(start_comment, coil_comment)
                        ) and not regex_pattern_check(complete_comment, coil_comment):
                            all_match_status.append(True)
                            current_rung_match_coil.append(coil_operand)
                        else:
                            all_match_status.append(False)

            if all_match_status and all(all_match_status):
                current_rung_match_coil.extend([rung, body])
                all_match_coil.append(current_rung_match_coil)

    return all_match_coil


def get_split_cc1_cc2_result_programwise(
    autorun_preparation_section_df: pd.DataFrame, detection_result: list
):

    all_data_result = []

    """
    collecting all dataframe within same coil part so that we can check other opernad used as B contact
    """
    for detection_data in detection_result:
        rung_number = detection_data[-2]
        section_name = detection_data[-1]
        all_operand = detection_data[:-2]

        ok_data_df = pd.DataFrame()
        ng_data_df = pd.DataFrame()

        # Filter rows for the specific section and rung
        match_runf_df = autorun_preparation_section_df[
            (autorun_preparation_section_df["BODY"] == section_name)
            & (autorun_preparation_section_df["RUNG"] == rung_number)
        ]

        all_self_holding_operand = check_self_holding(match_runf_df)

        # Filter for coil entries
        coil_df = match_runf_df[match_runf_df["OBJECT_TYPE_LIST"].str.lower() == "coil"]

        operand_dfs = {}

        if not coil_df.empty:

            start_index = 1
            for coil_index, coil_row in coil_df.iterrows():
                attr = ast.literal_eval(coil_row["ATTRIBUTES"])
                coil_operand = attr.get("operand")
                if coil_operand and isinstance(coil_operand, str):
                    if coil_operand in all_operand:
                        end_index = coil_index
                        ok_data_df = match_runf_df.loc[start_index:end_index]

                        # Save the DataFrame in the dictionary with the operand as key
                        operand_dfs[coil_operand] = ok_data_df

                        start_index = coil_index + 1
                    else:
                        start_index = coil_index + 1

        cc1_status = "NG"
        cc2_status = "NG"

        """
        checking if all other operand is used as a B contact in current coil opernad
        """
        for operand, match_df in operand_dfs.items():
            if operand in all_self_holding_operand:
                cc1_status = "OK"

            remain_contact_operand = [k for k in operand_dfs.keys() if k != operand]

            for _, match_row in match_df.iterrows():
                attr = ast.literal_eval(match_row["ATTRIBUTES"])
                contact_operand = attr.get("operand")
                negated_operand = attr.get("negated")

                if (
                    contact_operand
                    and isinstance(contact_operand, str)
                    and negated_operand == "true"
                ):
                    if contact_operand in remain_contact_operand:
                        remain_contact_operand.remove(contact_operand)
                        continue

            if not remain_contact_operand:
                cc2_status = "OK"

            all_data_result.append(
                [cc1_status, cc2_status, operand, rung_number, section_name]
            )

    return all_data_result


# ============================== Function-Wise Function Definitions ===============================
# These functions perform operations specific to each function, supporting rule validations and logic checks.
# ===============================================================================================


def detection_range_functionwise(
    autorun_preparation_section_df: pd.DataFrame,
    move_comment: str,
    run_comment: str,
    select_comment: str,
    cycle_comment: str,
    start_comment: str,
    complete_comment: str,
    function: str,
    function_comment_data: dict,
):
    pass

    coil_df = autorun_preparation_section_df[
        autorun_preparation_section_df["OBJECT_TYPE_LIST"].str.lower() == "coil"
    ]

    all_match_coil = []

    """
    Getting rung and section where two or more outcoil is given and matching comment as rule is given.
    """
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

            two_or_more_coil_df = coil_df[
                (coil_df["BODY_TYPE"] == body) & (coil_df["RUNG"] == rung)
            ]

            all_match_status = []
            current_rung_match_coil = []

            for _, coil_row in two_or_more_coil_df.iterrows():
                attr = ast.literal_eval(coil_row["ATTRIBUTES"])
                coil_operand = attr.get("operand")
                if isinstance(coil_operand, str) and coil_operand:
                    coil_comment = get_the_comment_from_function(
                        coil_operand, function, function_comment_data
                    )
                    if isinstance(coil_comment, list) and coil_comment:
                        if (
                            regex_pattern_check(move_comment, coil_comment)
                            or regex_pattern_check(run_comment, coil_comment)
                            or regex_pattern_check(select_comment, coil_comment)
                            or regex_pattern_check(cycle_comment, coil_comment)
                            or regex_pattern_check(start_comment, coil_comment)
                        ) and not regex_pattern_check(complete_comment, coil_comment):
                            all_match_status.append(True)
                            current_rung_match_coil.append(coil_operand)
                        else:
                            all_match_status.append(False)

            if all_match_status and all(all_match_status):
                current_rung_match_coil.extend([rung, body])
                all_match_coil.append(current_rung_match_coil)

    return all_match_coil


def get_split_cc1_cc2_result_functionwise(
    autorun_preparation_section_df: pd.DataFrame, detection_result: list
):

    all_data_result = []

    """
    collecting all dataframe within same coil part so that we can check other opernad used as B contact
    """
    for detection_data in detection_result:
        rung_number = detection_data[-2]
        section_name = detection_data[-1]
        all_operand = detection_data[:-2]

        ok_data_df = pd.DataFrame()
        ng_data_df = pd.DataFrame()

        # Filter rows for the specific section and rung
        match_runf_df = autorun_preparation_section_df[
            (autorun_preparation_section_df["BODY_TYPE"] == section_name)
            & (autorun_preparation_section_df["RUNG"] == rung_number)
        ]

        all_self_holding_operand = check_self_holding(match_runf_df)

        # Filter for coil entries
        coil_df = match_runf_df[match_runf_df["OBJECT_TYPE_LIST"].str.lower() == "coil"]

        if not coil_df.empty:

            start_index = 1
            for coil_index, coil_row in coil_df.iterrows():
                attr = ast.literal_eval(coil_row["ATTRIBUTES"])
                coil_operand = attr.get("operand")

                operand_dfs = {}

                if coil_operand and isinstance(coil_operand, str):
                    if coil_operand in all_operand:
                        end_index = coil_index
                        ok_data_df = match_runf_df.loc[start_index:end_index]

                        # Save the DataFrame in the dictionary with the operand as key
                        operand_dfs[coil_operand] = ok_data_df

                        start_index = coil_index + 1
                    else:
                        start_index = coil_index

        cc1_status = "NG"
        cc2_status = "NG"

        """
        checking if all other operand is used as a B contact in current coil opernad
        """
        for operand, match_df in operand_dfs.items():
            if operand in all_self_holding_operand:
                cc1_status = "OK"

            remain_contact_operand = [k for k in operand_dfs.keys() if k != operand]

            for _, match_row in match_df.iterrows():
                attr = ast.literal_eval(match_row["ATTRIBUTES"])
                contact_operand = attr.get("operand")
                negated_operand = attr.get("negated")

                if (
                    contact_operand
                    and isinstance(contact_operand, str)
                    and negated_operand == "true"
                ):
                    if contact_operand in remain_contact_operand:
                        remain_contact_operand.remove(contact_operand)
                        continue

            if not remain_contact_operand:
                cc2_status = "OK"

            all_data_result.append(
                [cc1_status, cc2_status, operand, rung_number, section_name]
            )

    return all_data_result


# ==============================Program-Wise Execution Starts Here ===============================
def execute_rule_90_programwise(
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
                move_comment=move_comment,
                run_comment=run_comment,
                select_comment=select_comment,
                cycle_comment=cycle_comment,
                start_comment=start_comment,
                complete_comment=complete_comment,
                program=program,
                program_comment_data=program_comment_data,
            )

            if detection_result:

                cc_analysis_results = get_split_cc1_cc2_result_programwise(
                    autorun_preparation_section_df=autorun_preparation_section_df,
                    detection_result=detection_result,
                )

                for cc_result_data in cc_analysis_results:
                    rung_no = cc_result_data[3]
                    section = cc_result_data[4]

                    # List of control check results: (name, status, operand)
                    control_checks = [
                        ("cc1", cc_result_data[0], cc_result_data[2]),
                        ("cc2", cc_result_data[1], cc_result_data[2]),
                    ]

                    for cc_name, status, operand in control_checks:
                        ng_name = ng_content.get(cc_name) if status == "NG" else ""

                        output_rows.append(
                            {
                                "Result": status,
                                "Task": program,
                                "Section": section,
                                "RungNo": (
                                    rung_no - 1
                                    if isinstance(rung_no, int) and rung_no != -1
                                    else -1
                                ),
                                "Target": operand,
                                "CheckItem": rule_90_check_item,
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
        logger.error(f"Rule 94 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}


# ==============================Program-Wise Execution Starts Here ===============================
def execute_rule_90_functionwise(
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
                move_comment=move_comment,
                run_comment=run_comment,
                select_comment=select_comment,
                cycle_comment=cycle_comment,
                start_comment=start_comment,
                complete_comment=complete_comment,
                function=function,
                function_comment_data=function_comment_data,
            )

            if detection_result:

                cc_analysis_results = get_split_cc1_cc2_result_functionwise(
                    autorun_preparation_section_df=autorun_preparation_section_df,
                    detection_result=detection_result,
                )

                for cc_result_data in cc_analysis_results:
                    rung_no = cc_result_data[3]
                    section = cc_result_data[4]

                    # List of control check results: (name, status, operand)
                    control_checks = [
                        ("cc1", cc_result_data[0], cc_result_data[2]),
                        ("cc2", cc_result_data[1], cc_result_data[2]),
                    ]

                    for cc_name, status, operand in control_checks:
                        ng_name = ng_content.get(cc_name) if status == "NG" else ""

                        output_rows.append(
                            {
                                "Result": status,
                                "Task": function,
                                "Section": section,
                                "RungNo": (
                                    rung_no - 1
                                    if isinstance(rung_no, int) and rung_no != -1
                                    else -1
                                ),
                                "Target": operand,
                                "CheckItem": rule_90_check_item,
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
        logger.error(f"Rule 94 Error : {e}")

        return {"status": "NOT OK", "error": str(e)}
