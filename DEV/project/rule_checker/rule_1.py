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
from .rule_1_self_holding import check_self_holding
from .ladder_utils import regex_pattern_check, clean_rung_number

# ============================ Rule 1: Definitions, Content, and Configuration Details ============================
section_name = "autorun"
section_name_with_star = "autorun★"
rule_content_1 = "Programs in the “AutoRun” section require a sequence of actions from cycle start to cycle end."
rule_1_check_item = "1 cycle loop connection check"
check_detail_content = {
    "cc1": "Check the rungs in the range detected by ③ in order of decreasing number, and check all outcoils there one by one. If it is an AutoRun section, but ③ is not detected, it is assumed to be NG.",
    "cc2": "Check that the target outcoil of ❶ is not used as a contact point in the rung before the rung with the target outcoil. If it is used, it is assumed to be NG.",
    "cc3": "The range from after the rung where the target outcoil is located to the end of the cycle is checked, and if the target outcoil is used within that range, it is OK, otherwise it is NG. However, if any of the following is true, it is OK as an exception.",
    "cc4": "If the ❸ out-coil is self-holding, the check is completed as OK; otherwise, the check is completed as NG.",
}

ng_content = {
    "cc1": "AutoRunセクション内にサイクル終了コイルが無いため、1サイクルが成り立っていない可能性有 (There is no 1 cycle end outcoil in the AutoRun section, so a cycle may not be formed.)",
    "cc2": "対象のアウトコイルが、自分より前のラングで接点として使用されている (Target outcoil is used as a contact in the rung before the rung with it.)",
    "cc3": "対象のアウトコイルが接点として使用されていないので、途切れている可能性有 Possibly interrupted because the subject outcoil is not used as a contact",
    "cc4": "対象のアウトコイルが自己保持されていないため、サイクル中に信号が途切れる可能性有 (Possibility that the cycle is disconnected in during cycle because the target outcoil is not self-holding.)",
}


# ============================== Program-Wise Function Definitions ===============================
# These functions perform operations specific to each program, supporting rule validations and logic checks.
# ===============================================================================================


def extract_cycle_start_end_programwise(
    program_df: pd.DataFrame,
    section_name: str,
    section_name_with_star: str,
    program_comment_data: dict,
) -> dict:

    logger.info(
        "Rule 1 Checking if cycle start/end exists and retrieving corresponding rung number and operand information"
    )

    cycle_comment = "ｻｲｸﾙ"
    start_comment = "開始"
    end_comment = "終了"
    cycle_start_end_exist_rung_number_dict = {}
    cycle_start_end_operand_dict = {}

    if not program_df.empty:
        unique_program_values = program_df["PROGRAM"].unique()

        for program_name in unique_program_values:

            unique_section_df = program_df[program_df["PROGRAM"] == program_name]
            unique_section_values = unique_section_df["BODY"].str.lower().unique()

            if (
                "autorun" in unique_section_values
                or "autorun★" in unique_section_values
            ):

                logger.info(
                    f"Rule 1 Execute Program {program_name} and section {section_name}, {section_name}"
                )

                cycle_start_exist_rung_number = -1
                cycle_end_exist_rung_number = -1
                cycle_start_operand = ""
                cycle_end_operand = ""

                curr_program_df = program_df[
                    program_df["PROGRAM"] == program_name
                ].copy()

                if not curr_program_df.empty:

                    autorun_section_df = curr_program_df[
                        curr_program_df["BODY"]
                        .str.lower()
                        .isin([section_name, section_name_with_star])
                    ]

                    if not autorun_section_df.empty:
                        try:
                            coil_df = autorun_section_df[
                                autorun_section_df["OBJECT_TYPE_LIST"].str.lower()
                                == "coil"
                            ]

                            if not coil_df.empty:
                                for _, coil_row in coil_df.iterrows():
                                    attr = ast.literal_eval(coil_row["ATTRIBUTES"])
                                    coil_operand = attr.get("operand")

                                    if isinstance(coil_operand, str) and coil_operand:
                                        coil_comment = get_the_comment_from_program(
                                            coil_operand,
                                            program_name,
                                            program_comment_data,
                                        )
                                        if (
                                            isinstance(coil_comment, list)
                                            and coil_comment
                                        ):
                                            if (
                                                regex_pattern_check(
                                                    cycle_comment, coil_comment
                                                )
                                                and regex_pattern_check(
                                                    start_comment, coil_comment
                                                )
                                                and cycle_start_exist_rung_number == -1
                                            ):
                                                cycle_start_exist_rung_number = (
                                                    coil_row["RUNG"]
                                                )
                                                cycle_start_operand = attr.get(
                                                    "operand"
                                                )
                                            if (
                                                regex_pattern_check(
                                                    cycle_comment, coil_comment
                                                )
                                                and regex_pattern_check(
                                                    end_comment, coil_comment
                                                )
                                                and cycle_end_exist_rung_number == -1
                                            ):
                                                cycle_end_exist_rung_number = coil_row[
                                                    "RUNG"
                                                ]
                                                cycle_end_operand = attr.get("operand")

                        except:
                            logger.info(
                                f"Rule 1 Error processing program {program_name} for cycle start/end extraction."
                            )
                            continue

                cycle_start_end_exist_rung_number_dict[program_name] = [
                    cycle_start_exist_rung_number,
                    cycle_end_exist_rung_number,
                ]
                cycle_start_end_operand_dict[program_name] = [
                    cycle_start_operand,
                    cycle_end_operand,
                ]
    else:
        logger.info("Rule 1 Program DataFrame is empty, returning empty dictionaries.")

    return cycle_start_end_exist_rung_number_dict, cycle_start_end_operand_dict


# def rule_1_check_detail():
def check_detail_2_programwise(
    autorun_section_df: pd.DataFrame,
    outcoil_operand: str,
    start_rung: int,
    end_rung: int,
) -> Tuple[str, int]:

    logger.info(f"Rule 1 Executing check detail 2 for outcoil {outcoil_operand}")

    try:

        contact_coil_df = autorun_section_df[
            (autorun_section_df["RUNG"] >= start_rung)
            & (autorun_section_df["RUNG"] <= end_rung)
        ]
        if not contact_coil_df.empty:
            for _, contact_coil_row in contact_coil_df.iterrows():
                attr = ast.literal_eval(contact_coil_row["ATTRIBUTES"])
                if contact_coil_row["OBJECT_TYPE_LIST"].lower() in ["contact", "coil"]:
                    if attr["operand"] == outcoil_operand:
                        return "NG", 2
        return "OK", 2

    except Exception as e:
        logger.error(f"Rule 1 Error in check_detail_2_programwise: {e}")
        return "NG", 2


def check_detail_3_programwise(
    autorun_section_df: pd.DataFrame,
    outcoil_operand: str,
    current_rung: int,
    start_rung: int,
    end_rung: int,
) -> Tuple[str, int]:

    logger.info(f"Rule 1 Executing check detail 3 for outcoil {outcoil_operand}")

    ### Check rule 3 from next rung to end rung if coil variable is same varialbe contact is there from next to last rung it it is then return OK else NG
    try:

        if not autorun_section_df.empty:
            if start_rung >= 0 and end_rung >= 0:
                contact_next_rung_to_end_rung_df = autorun_section_df[
                    (autorun_section_df["OBJECT_TYPE_LIST"].str.lower() == "contact")
                    & (autorun_section_df["RUNG"] >= start_rung)
                    & (autorun_section_df["RUNG"] <= end_rung)
                ]
            else:
                contact_next_rung_to_end_rung_df = pd.DataFrame()

            if not contact_next_rung_to_end_rung_df.empty:
                for _, contact_row in contact_next_rung_to_end_rung_df.iterrows():
                    attr = ast.literal_eval(contact_row["ATTRIBUTES"])
                    if attr.get("operand") == outcoil_operand:
                        return "OK", 3

            if current_rung >= 0:
                contact_coil_current_df = autorun_section_df[
                    (
                        (
                            autorun_section_df["OBJECT_TYPE_LIST"].str.lower()
                            == "contact"
                        )
                        | (autorun_section_df["OBJECT_TYPE_LIST"].str.lower() == "coil")
                    )
                    & (autorun_section_df["RUNG"] == current_rung)
                ]
            else:
                contact_coil_current_df = pd.DataFrame()

    except Exception as e:
        logger.error(f"Rule 1 Error in check_detail_3_programwise: {e}")
        pass

    ### rule 3.1 checking if in current rung if outcoil operand is used in next series in current rung then it is OK
    logger.info(f"Rule 1 Executing check detail 3.1 for outcoil {outcoil_operand}")
    try:
        is_rule_3_1_valid = False
        if not contact_coil_current_df.empty:
            for _, current_contact_row in contact_coil_current_df.iterrows():
                attr = ast.literal_eval(current_contact_row["ATTRIBUTES"])
                if (
                    re.search(re.escape(outcoil_operand), attr["operand"])
                    and current_contact_row["OBJECT_TYPE_LIST"].lower() == "coil"
                ):
                    is_rule_3_1_valid = True
                    continue

                if is_rule_3_1_valid:
                    if (
                        re.search(re.escape(outcoil_operand), attr["operand"])
                        and current_contact_row["OBJECT_TYPE_LIST"].lower() == "contact"
                        and attr["negated"] == "false"
                    ):
                        return "OK", 3.1
    except Exception as e:
        logger.error(f"Rule 1 Error in check_detail_3.1_programwise: {e}")
        pass

    return "NG", 3


def check_detail_4_programwise(
    autorun_section_df: pd.DataFrame,
    outcoil_operand: str,
    current_rung: int,
    start_rung: str,
    end_rung: str,
    program_name: str,
    program_comment_data: dict,
) -> Tuple[str, int]:

    logger.info(f"Rule 1 Executing check detail 4 for outcoil {outcoil_operand}")

    ### rule 4 is checking if the coil is self holding
    try:
        if not autorun_section_df.empty and current_rung >= 0:
            self_holding_operand = check_self_holding(
                autorun_section_df[autorun_section_df["RUNG"] == current_rung]
            )
            if outcoil_operand and outcoil_operand in self_holding_operand:
                return "OK", 4
    except Exception as e:
        logger.error(f"Rule 1 Error in check_detail_4_programwise: {e}")
        pass

    ### rule 4.1 in checking if NG or abnormal is present in comment
    logger.info(f"Rule 1 Executing check detail 4.1 for outcoil {outcoil_operand}")

    try:
        if outcoil_operand and isinstance(outcoil_operand, str):
            coil_comment = get_the_comment_from_program(
                outcoil_operand, program_name, program_comment_data
            )
            if isinstance(coil_comment, list) and coil_comment:
                if regex_pattern_check("異常", coil_comment) or regex_pattern_check(
                    "NG", coil_comment
                ):
                    return "OK", 4.1
    except Exception as e:
        logger.error(f"Rule 1 Error in check_detail_4.1_programwise: {e}")
        pass

    ### rule 4.2 checking that if outcoil operand same contact is present in next rung and if yes then its outcoil check is self holding or not
    logger.info(f"Rule 1 Executing check detail 4.2 for outcoil {outcoil_operand}")

    try:
        if current_rung >= 0 and not autorun_section_df.empty:
            contact_coil_current_df = autorun_section_df[
                (
                    (autorun_section_df["OBJECT_TYPE_LIST"].str.lower() == "contact")
                    | (autorun_section_df["OBJECT_TYPE_LIST"].str.lower() == "coil")
                )
                & (autorun_section_df["RUNG"] == current_rung)
            ]
        else:
            contact_coil_current_df = pd.DataFrame()

        ### rule check for current rung if the outcoil is used as contact and the outcoil of contact is self holding
        logger.info(
            f"Rule 1 Executing check detail 4.2 for current rung {current_rung} and  outcoil {outcoil_operand}"
        )

        is_rule_4_2_current_rung_valid = False
        is_contact_present_current_rung = False

        if not contact_coil_current_df.empty:
            for _, current_contact_row in contact_coil_current_df.iterrows():
                attr = ast.literal_eval(current_contact_row["ATTRIBUTES"])
                if outcoil_operand and isinstance(outcoil_operand, str):
                    if (
                        re.search(re.escape(outcoil_operand), attr["operand"])
                        and current_contact_row["OBJECT_TYPE_LIST"].lower() == "coil"
                        and not is_rule_4_2_current_rung_valid
                    ):
                        is_rule_4_2_current_rung_valid = True
                        continue

                if is_rule_4_2_current_rung_valid:
                    if (
                        outcoil_operand
                        and isinstance(outcoil_operand, str)
                        and outcoil_operand == attr.get("operand")
                        and attr.get("negated") == "false"
                        and current_contact_row["OBJECT_TYPE_LIST"].lower() == "contact"
                        and not is_contact_present_current_rung
                    ):
                        is_contact_present_current_rung = True

                if is_rule_4_2_current_rung_valid and is_contact_present_current_rung:
                    if current_contact_row["OBJECT_TYPE_LIST"].lower() == "coil":
                        coil_operand = attr.get("operand")
                        self_holding_operand = check_self_holding(
                            contact_coil_current_df[
                                contact_coil_current_df["RUNG"] == current_rung
                            ]
                        )

                        if (
                            coil_operand
                            and isinstance(coil_operand, str)
                            and self_holding_operand
                        ):
                            if coil_operand in self_holding_operand:
                                return "OK", 4.2

        logger.info(
            f"Rule 1 Executing check detail 4.2 for next rung to end rung and  outcoil {outcoil_operand}"
        )

        if current_rung >= 0 and end_rung >= 0 and not autorun_section_df.empty:
            contact_coil_next_rung_to_end_rung_df = autorun_section_df[
                (
                    (autorun_section_df["OBJECT_TYPE_LIST"].str.lower() == "contact")
                    | (autorun_section_df["OBJECT_TYPE_LIST"].str.lower() == "coil")
                )
                & (autorun_section_df["RUNG"] > current_rung)
                & (autorun_section_df["RUNG"] <= end_rung)
            ]
        else:
            contact_coil_next_rung_to_end_rung_df = pd.DataFrame()

        ### check from next rung to 1 cycle end rung if the outcoil is used as contact and the outcoil of contact is self holding
        is_contact_operand_present_next_rung = False

        if not contact_coil_next_rung_to_end_rung_df.empty:
            for _, contact_coil_row in contact_coil_next_rung_to_end_rung_df.iterrows():

                attr = ast.literal_eval(contact_coil_row["ATTRIBUTES"])
                if (
                    contact_coil_row["OBJECT_TYPE_LIST"].lower() == "contact"
                    and outcoil_operand == attr["operand"]
                ):
                    is_contact_operand_present_next_rung = True
                    current_rung_number = contact_coil_row["RUNG"]
                    continue

                if (
                    is_contact_operand_present_next_rung
                    and contact_coil_row["OBJECT_TYPE_LIST"].lower() == "coil"
                    and contact_coil_row["RUNG"] == current_rung_number
                ):
                    attr = ast.literal_eval(contact_coil_row["ATTRIBUTES"])

                    coil_operand = attr.get("operand")
                    self_holding_operand = check_self_holding(
                        contact_coil_next_rung_to_end_rung_df[
                            contact_coil_next_rung_to_end_rung_df["RUNG"]
                            == contact_coil_row["RUNG"]
                        ]
                    )
                    if coil_operand in self_holding_operand:
                        return "OK", 4.2
                    if contact_coil_row["RUNG"] > current_rung_number:
                        return "NG", 4.2
                    break
    except Exception as e:
        logger.error(f"Rule 1 Error in check_detail_4.2_programwise: {e}")
        pass

    ### rule 4.3 checking that if outcoil operand current first contact is present in prev rung to start rung check if that contact outcoil is present and if yes then its outcoil check is self holding or not
    logger.info(f"Rule 1 Executing check detail 4.3 for outcoil {outcoil_operand}")

    try:

        if not autorun_section_df.empty and current_rung >= 0:
            current_rung_first_contact_df = autorun_section_df[
                (autorun_section_df["OBJECT_TYPE_LIST"].str.lower() == "contact")
                & (autorun_section_df["RUNG"] == current_rung)
            ].iloc[0]
        else:
            current_rung_first_contact_df = pd.DataFrame()

        # Parse ATTRIBUTES field and extract operand
        if not current_rung_first_contact_df.empty:
            first_contact_attr = ast.literal_eval(
                current_rung_first_contact_df["ATTRIBUTES"]
            )
            first_contact_operand = first_contact_attr.get("operand", "NONE")

            prev_rung = current_rung_first_contact_df["RUNG"] - 1
            for rung_number in range(prev_rung, start_rung, -1):
                self_holding_operand = check_self_holding(
                    autorun_section_df[autorun_section_df["RUNG"] == rung_number]
                )
                if first_contact_operand in self_holding_operand:
                    return "OK", 4.3

    except Exception as e:
        logger.error(f"Rule 1 Error in check_detail_4.3_programwise: {e}")
        pass

    return "NG", 4


def check_detail_cycle_start_end_info_programwise(
    program_df: pd.DataFrame,
    program_name: str,
    program_comment_data: str,
    section_name: str,
    cycle_start_end_rung_number: List,
    output_rows,
) -> List:

    logger.info("Rule 1 Running Rule 1 checks on all check details.")

    try:
        curr_program_df = program_df[program_df["PROGRAM"] == program_name].copy()

        autorun_section_df = curr_program_df[
            curr_program_df["BODY"]
            .str.lower()
            .isin([section_name, section_name_with_star])
        ]

        if not autorun_section_df.empty:
            coil_df = autorun_section_df[
                (autorun_section_df["OBJECT_TYPE_LIST"].str.lower() == "coil")
                & (autorun_section_df["RUNG"] > cycle_start_end_rung_number[0])
                & (autorun_section_df["RUNG"] < cycle_start_end_rung_number[1])
            ]
        else:
            coil_df = pd.DataFrame()

        if not coil_df.empty:
            for _, coil_row in coil_df.iterrows():
                attr = ast.literal_eval(coil_row["ATTRIBUTES"])

                logger.info(
                    f"Rule 1 Executing all check detail for coil {attr['operand']} of program {program_name} and section {section_name}"
                )

                if attr.get("operand") and isinstance(attr.get("operand"), str):

                    try:
                        cc2_status, cc2_rule_number = check_detail_2_programwise(
                            autorun_section_df=autorun_section_df,
                            outcoil_operand=attr["operand"],
                            start_rung=cycle_start_end_rung_number[0],
                            end_rung=coil_row["RUNG"] - 1,
                        )
                    except:
                        logger.error(
                            f"Rule 1 Error in check_detail_2_programwise for {attr['operand']}"
                        )
                        cc2_status, cc2_rule_number = "NG", -1

                    try:
                        cc3_status, cc3_rule_number = check_detail_3_programwise(
                            autorun_section_df=autorun_section_df,
                            outcoil_operand=attr["operand"],
                            current_rung=coil_row["RUNG"],
                            start_rung=coil_row["RUNG"] + 1,
                            end_rung=cycle_start_end_rung_number[1],
                        )
                    except:
                        logger.error(
                            f"Rule 1 Error in check_detail_3_programwise for {attr['operand']}"
                        )
                        cc3_status, cc3_rule_number = "NG", -1

                    try:
                        cc4_status, cc4_rule_number = check_detail_4_programwise(
                            autorun_section_df=autorun_section_df,
                            outcoil_operand=attr["operand"],
                            current_rung=coil_row["RUNG"],
                            start_rung=cycle_start_end_rung_number[0],
                            end_rung=cycle_start_end_rung_number[1],
                            program_name=program_name,
                            program_comment_data=program_comment_data,
                        )
                    except:
                        logger.error(
                            f"Rule 1 Error in check_detail_4_programwise for {attr['operand']}"
                        )
                        cc4_status, cc4_rule_number = "NG", -1

                    check_results = [
                        (cc2_status, cc2_rule_number, "cc2"),
                        (cc3_status, cc3_rule_number, "cc3"),
                        (cc4_status, cc4_rule_number, "cc4"),
                    ]

                    for index, (status, rule_number, check_key) in enumerate(
                        check_results
                    ):
                        ng_name = ng_content.get(check_key) if status == "NG" else ""
                        output_rows.append(
                            {
                                "Result": status,
                                "Task": program_name,
                                "Section": "AutoRun",
                                "RungNo": coil_row["RUNG"] - 1,
                                "Target": attr["operand"],
                                "CheckItem": rule_1_check_item,
                                "Detail": ng_name,
                                "Status": "",
                            }
                        )

        return output_rows

    except:
        logger.error("Error in check_detail_cycle_start_end_info_programwise")
        return output_rows


# ============================== Function-Wise Function Definitions ===============================
# These functions perform operations specific to each function, supporting rule validations and logic checks.
# ===============================================================================================


def extract_cycle_start_end_functionwise(
    function_df: pd.DataFrame,
    section_name: str,
    section_name_with_star: str,
    function_comment_data: dict,
) -> dict:

    logger.info(
        "Rule 1 Checking if cycle start/end exists and retrieving corresponding rung number and operand information"
    )

    cycle_comment = "ｻｲｸﾙ"
    start_comment = "開始"
    end_comment = "終了"
    cycle_start_end_exist_rung_number_dict = {}
    cycle_start_end_operand_dict = {}

    if not function_df.empty:
        unique_function_values = function_df["FUNCTION_BLOCK"].unique()

        for function_name in unique_function_values:

            unique_section_df = function_df[function_df["FUNCTION_BLOCK"] == function]
            unique_section_values = unique_section_df["BODY_TYPE"].str.lower().unique()

            if (
                "autorun" in unique_section_values
                or "autorun★" in unique_section_values
            ):

                logger.info(
                    f"Rule 1 Execute function {function_name} and section {section_name}, section {section_name}"
                )

                cycle_start_exist_rung_number = -1
                cycle_end_exist_rung_number = -1
                cycle_start_operand = ""
                cycle_end_operand = ""

                curr_function_df = function_df[
                    function_df["FUNCTION_BLOCK"] == function_name
                ].copy()

                if not curr_function_df.empty:

                    autorun_section_df = curr_function_df[
                        curr_function_df["BODY_TYPE"]
                        .str.lower()
                        .isin([section_name, section_name_with_star])
                    ]

                    if not autorun_section_df.empty:
                        try:
                            coil_df = autorun_section_df[
                                autorun_section_df["OBJECT_TYPE_LIST"].str.lower()
                                == "coil"
                            ]

                            if not coil_df.empty:
                                for _, coil_row in coil_df.iterrows():
                                    attr = ast.literal_eval(coil_row["ATTRIBUTES"])
                                    coil_comment = get_the_comment_from_function(
                                        attr["operand"],
                                        function_name,
                                        function_comment_data,
                                    )
                                    if isinstance(coil_comment, list) and coil_comment:
                                        if (
                                            regex_pattern_check(
                                                cycle_comment, coil_comment
                                            )
                                            and regex_pattern_check(
                                                start_comment, coil_comment
                                            )
                                            and cycle_start_exist_rung_number == -1
                                        ):
                                            cycle_start_exist_rung_number = coil_row[
                                                "RUNG"
                                            ]
                                            cycle_start_operand = attr.get("operand")
                                        if (
                                            regex_pattern_check(
                                                cycle_comment, coil_comment
                                            )
                                            and regex_pattern_check(
                                                end_comment, coil_comment
                                            )
                                            and cycle_end_exist_rung_number == -1
                                        ):
                                            cycle_end_exist_rung_number = coil_row[
                                                "RUNG"
                                            ]
                                            cycle_end_operand = attr.get("operand")

                        except:
                            logger.info(
                                f"Rule 1 Error processing function {function_name} for cycle start/end extraction."
                            )
                            continue

                cycle_start_end_exist_rung_number_dict[function_name] = [
                    cycle_start_exist_rung_number,
                    cycle_end_exist_rung_number,
                ]
                cycle_start_end_operand_dict[function_name] = [
                    cycle_start_operand,
                    cycle_end_operand,
                ]
    else:
        logger.info("Rule 1 function DataFrame is empty, returning empty dictionaries.")

    return cycle_start_end_exist_rung_number_dict, cycle_start_end_operand_dict


# def rule_1_check_detail():
def check_detail_2_functionwise(
    autorun_section_df: pd.DataFrame,
    outcoil_operand: str,
    start_rung: int,
    end_rung: int,
) -> Tuple[str, int]:

    logger.info(f"Rule 1 Executing check detail 2 for outcoil {outcoil_operand}")

    try:

        contact_coil_df = autorun_section_df[
            (autorun_section_df["RUNG"] >= start_rung)
            & (autorun_section_df["RUNG"] <= end_rung)
        ]
        if not contact_coil_df.empty:
            for _, contact_coil_row in contact_coil_df.iterrows():
                attr = ast.literal_eval(contact_coil_row["ATTRIBUTES"])
                if contact_coil_row["OBJECT_TYPE_LIST"].lower() in ["contact", "coil"]:
                    if attr["operand"] == outcoil_operand:
                        return "NG", 2
        return "OK", 2

    except Exception as e:
        logger.error(f"Rule 1 Error in check_detail_2_functionwise: {e}")
        return "NG", 2


def check_detail_3_functionwise(
    autorun_section_df: pd.DataFrame,
    outcoil_operand: str,
    current_rung: int,
    start_rung: int,
    end_rung: int,
) -> Tuple[str, int]:

    logger.info(f"Rule 1 Executing check detail 3 for outcoil {outcoil_operand}")

    ### Check rule 3 from next rung to end rung if coil variable is same varialbe contact is there from next to last rung it it is then return OK else NG
    try:

        if not autorun_section_df.empty:
            if start_rung >= 0 and end_rung >= 0:
                contact_next_rung_to_end_rung_df = autorun_section_df[
                    (autorun_section_df["OBJECT_TYPE_LIST"].str.lower() == "contact")
                    & (autorun_section_df["RUNG"] >= start_rung)
                    & (autorun_section_df["RUNG"] <= end_rung)
                ]
            else:
                contact_next_rung_to_end_rung_df = pd.DataFrame()

            if not contact_next_rung_to_end_rung_df.empty:
                for _, contact_row in contact_next_rung_to_end_rung_df.iterrows():
                    attr = ast.literal_eval(contact_row["ATTRIBUTES"])
                    if attr.get("operand") == outcoil_operand:
                        return "OK", 3

            if current_rung >= 0:
                contact_coil_current_df = autorun_section_df[
                    (
                        (
                            autorun_section_df["OBJECT_TYPE_LIST"].str.lower()
                            == "contact"
                        )
                        | (autorun_section_df["OBJECT_TYPE_LIST"].str.lower() == "coil")
                    )
                    & (autorun_section_df["RUNG"] == current_rung)
                ]
            else:
                contact_coil_current_df = pd.DataFrame()

    except Exception as e:
        logger.error(f"Rule 1 Error in check_detail_3_functionwise: {e}")
        pass

    ### rule 3.1 checking if in current rung if outcoil operand is used in next series in current rung then it is OK
    logger.info(f"Rule 1 Executing check detail 3.1 for outcoil {outcoil_operand}")
    try:
        is_rule_3_1_valid = False
        if not contact_coil_current_df.empty:
            for _, current_contact_row in contact_coil_current_df.iterrows():
                attr = ast.literal_eval(current_contact_row["ATTRIBUTES"])
                if (
                    re.search(re.escape(outcoil_operand), attr["operand"])
                    and current_contact_row["OBJECT_TYPE_LIST"].lower() == "coil"
                ):
                    is_rule_3_1_valid = True
                    continue

                if is_rule_3_1_valid:
                    if (
                        re.search(re.escape(outcoil_operand), attr["operand"])
                        and current_contact_row["OBJECT_TYPE_LIST"].lower() == "contact"
                        and attr["negated"] == "false"
                    ):
                        return "OK", 3.1
    except Exception as e:
        logger.error(f"Rule 1 Error in check_detail_3.1_functionwise: {e}")
        pass

    return "NG", 3


def check_detail_4_functionwise(
    autorun_section_df: pd.DataFrame,
    outcoil_operand: str,
    current_rung: int,
    start_rung: str,
    end_rung: str,
    function_name: str,
    function_comment_data: dict,
) -> Tuple[str, int]:

    logger.info(f"Rule 1 Executing check detail 4 for outcoil {outcoil_operand}")

    ### rule 4 is checking if the coil is self holding
    try:
        if not autorun_section_df.empty and current_rung >= 0:
            self_holding_operand = check_self_holding(
                autorun_section_df[autorun_section_df["RUNG"] == current_rung]
            )
            if outcoil_operand and outcoil_operand in self_holding_operand:
                return "OK", 4
    except Exception as e:
        logger.error(f"Rule 1 Error in check_detail_4_functionwise: {e}")
        pass

    ### rule 4.1 in checking if NG or abnormal is present in comment
    logger.info(f"Rule 1 Executing check detail 4.1 for outcoil {outcoil_operand}")

    try:
        if outcoil_operand and isinstance(outcoil_operand, str):
            coil_comment = get_the_comment_from_function(
                outcoil_operand, function_name, function_comment_data
            )
            if isinstance(coil_comment, list) and coil_comment:
                if regex_pattern_check("異常", coil_comment) or regex_pattern_check(
                    "NG", coil_comment
                ):
                    return "OK", 4.1
    except Exception as e:
        logger.error(f"Rule 1 Error in check_detail_4.1_functionwise: {e}")
        pass

    ### rule 4.2 checking that if outcoil operand same contact is present in next rung and if yes then its outcoil check is self holding or not
    logger.info(f"Rule 1 Executing check detail 4.2 for outcoil {outcoil_operand}")

    try:
        if current_rung >= 0 and not autorun_section_df.empty:
            contact_coil_current_df = autorun_section_df[
                (
                    (autorun_section_df["OBJECT_TYPE_LIST"].str.lower() == "contact")
                    | (autorun_section_df["OBJECT_TYPE_LIST"].str.lower() == "coil")
                )
                & (autorun_section_df["RUNG"] == current_rung)
            ]
        else:
            contact_coil_current_df = pd.DataFrame()

        ### rule check for current rung if the outcoil is used as contact and the outcoil of contact is self holding
        logger.info(
            f"Rule 1 Executing check detail 4.2 for current rung {current_rung} and  outcoil {outcoil_operand}"
        )

        is_rule_4_2_current_rung_valid = False
        is_contact_present_current_rung = False

        if not contact_coil_current_df.empty:
            for _, current_contact_row in contact_coil_current_df.iterrows():
                attr = ast.literal_eval(current_contact_row["ATTRIBUTES"])
                if outcoil_operand and isinstance(outcoil_operand, str):
                    if (
                        re.search(re.escape(outcoil_operand), attr["operand"])
                        and current_contact_row["OBJECT_TYPE_LIST"].lower() == "coil"
                        and not is_rule_4_2_current_rung_valid
                    ):
                        is_rule_4_2_current_rung_valid = True
                        continue

                if is_rule_4_2_current_rung_valid:
                    if (
                        outcoil_operand
                        and isinstance(outcoil_operand, str)
                        and outcoil_operand == attr.get("operand")
                        and attr.get("negated") == "false"
                        and current_contact_row["OBJECT_TYPE_LIST"].lower() == "contact"
                        and not is_contact_present_current_rung
                    ):
                        is_contact_present_current_rung = True

                if is_rule_4_2_current_rung_valid and is_contact_present_current_rung:
                    if current_contact_row["OBJECT_TYPE_LIST"].lower() == "coil":
                        coil_operand = attr.get("operand")
                        self_holding_operand = check_self_holding(
                            contact_coil_current_df[
                                contact_coil_current_df["RUNG"] == current_rung
                            ]
                        )

                        if (
                            coil_operand
                            and isinstance(coil_operand, str)
                            and self_holding_operand
                        ):
                            if coil_operand in self_holding_operand:
                                return "OK", 4.2

        logger.info(
            f"Rule 1 Executing check detail 4.2 for next rung to end rung and  outcoil {outcoil_operand}"
        )

        if current_rung >= 0 and end_rung >= 0 and not autorun_section_df.empty:
            contact_coil_next_rung_to_end_rung_df = autorun_section_df[
                (
                    (autorun_section_df["OBJECT_TYPE_LIST"].str.lower() == "contact")
                    | (autorun_section_df["OBJECT_TYPE_LIST"].str.lower() == "coil")
                )
                & (autorun_section_df["RUNG"] > current_rung)
                & (autorun_section_df["RUNG"] <= end_rung)
            ]
        else:
            contact_coil_next_rung_to_end_rung_df = pd.DataFrame()

        ### check from next rung to 1 cycle end rung if the outcoil is used as contact and the outcoil of contact is self holding
        is_contact_operand_present_next_rung = False

        if not contact_coil_next_rung_to_end_rung_df.empty:
            for _, contact_coil_row in contact_coil_next_rung_to_end_rung_df.iterrows():

                attr = ast.literal_eval(contact_coil_row["ATTRIBUTES"])
                if (
                    contact_coil_row["OBJECT_TYPE_LIST"].lower() == "contact"
                    and outcoil_operand == attr["operand"]
                ):
                    is_contact_operand_present_next_rung = True
                    current_rung_number = contact_coil_row["RUNG"]
                    continue

                if (
                    is_contact_operand_present_next_rung
                    and contact_coil_row["OBJECT_TYPE_LIST"].lower() == "coil"
                    and contact_coil_row["RUNG"] == current_rung_number
                ):
                    attr = ast.literal_eval(contact_coil_row["ATTRIBUTES"])

                    coil_operand = attr.get("operand")
                    self_holding_operand = check_self_holding(
                        contact_coil_next_rung_to_end_rung_df[
                            contact_coil_next_rung_to_end_rung_df["RUNG"]
                            == contact_coil_row["RUNG"]
                        ]
                    )
                    if coil_operand in self_holding_operand:
                        return "OK", 4.2
                    if contact_coil_row["RUNG"] > current_rung_number:
                        return "NG", 4.2
                    break
    except Exception as e:
        logger.error(f"Rule 1 Error in check_detail_4.2_functionwise: {e}")
        pass

    ### rule 4.3 checking that if outcoil operand current first contact is present in prev rung to start rung check if that contact outcoil is present and if yes then its outcoil check is self holding or not
    logger.info(f"Rule 1 Executing check detail 4.3 for outcoil {outcoil_operand}")

    try:

        if not autorun_section_df.empty and current_rung >= 0:
            current_rung_first_contact_df = autorun_section_df[
                (autorun_section_df["OBJECT_TYPE_LIST"].str.lower() == "contact")
                & (autorun_section_df["RUNG"] == current_rung)
            ].iloc[0]
        else:
            current_rung_first_contact_df = pd.DataFrame()

        # Parse ATTRIBUTES field and extract operand
        if not current_rung_first_contact_df.empty:
            first_contact_attr = ast.literal_eval(
                current_rung_first_contact_df["ATTRIBUTES"]
            )
            first_contact_operand = first_contact_attr.get("operand", "NONE")

            prev_rung = current_rung_first_contact_df["RUNG"] - 1
            for rung_number in range(prev_rung, start_rung, -1):
                self_holding_operand = check_self_holding(
                    autorun_section_df[autorun_section_df["RUNG"] == rung_number]
                )
                if first_contact_operand in self_holding_operand:
                    return "OK", 4.3

    except Exception as e:
        logger.error(f"Rule 1 Error in check_detail_4.3_functionwise: {e}")
        pass

    return "NG", 4


def check_detail_cycle_start_end_info_functionwise(
    function_df: pd.DataFrame,
    function_name: str,
    function_comment_data: str,
    section_name: str,
    cycle_start_end_rung_number: List,
    output_rows,
) -> List:

    logger.info("Rule 1 Running Rule 1 checks on all check details.")

    try:
        curr_function_df = function_df[
            function_df["FUNCTION_BLOCK"] == function_name
        ].copy()

        autorun_section_df = curr_function_df[
            curr_function_df["BODY_TYPE"]
            .str.lower()
            .isin([section_name, section_name_with_star])
        ]

        if not autorun_section_df.empty:
            coil_df = autorun_section_df[
                (autorun_section_df["OBJECT_TYPE_LIST"].str.lower() == "coil")
                & (autorun_section_df["RUNG"] > cycle_start_end_rung_number[0])
                & (autorun_section_df["RUNG"] < cycle_start_end_rung_number[1])
            ]
        else:
            coil_df = pd.DataFrame()

        if not coil_df.empty:
            for _, coil_row in coil_df.iterrows():
                attr = ast.literal_eval(coil_row["ATTRIBUTES"])

                logger.info(
                    f"Rule 1 Executing all check detail for coil {attr['operand']} of function {function_name} and section {section_name}"
                )

                if attr.get("operand") and isinstance(attr.get("operand"), str):

                    try:
                        cc2_status, cc2_rule_number = check_detail_2_functionwise(
                            autorun_section_df=autorun_section_df,
                            outcoil_operand=attr["operand"],
                            start_rung=cycle_start_end_rung_number[0],
                            end_rung=coil_row["RUNG"] - 1,
                        )
                    except:
                        logger.error(
                            f"Rule 1 Error in check_detail_2_functionwise for {attr['operand']}"
                        )
                        cc2_status, cc2_rule_number = "NG", -1

                    try:
                        cc3_status, cc3_rule_number = check_detail_3_functionwise(
                            autorun_section_df=autorun_section_df,
                            outcoil_operand=attr["operand"],
                            current_rung=coil_row["RUNG"],
                            start_rung=coil_row["RUNG"] + 1,
                            end_rung=cycle_start_end_rung_number[1],
                        )
                    except:
                        logger.error(
                            f"Rule 1 Error in check_detail_3_functionwise for {attr['operand']}"
                        )
                        cc3_status, cc3_rule_number = "NG", -1

                    try:
                        cc4_status, cc4_rule_number = check_detail_4_functionwise(
                            autorun_section_df=autorun_section_df,
                            outcoil_operand=attr["operand"],
                            current_rung=coil_row["RUNG"],
                            start_rung=cycle_start_end_rung_number[0],
                            end_rung=cycle_start_end_rung_number[1],
                            function_name=function_name,
                            function_comment_data=function_comment_data,
                        )
                    except:
                        logger.error(
                            f"Rule 1 Error in check_detail_4_functionwise for {attr['operand']}"
                        )
                        cc4_status, cc4_rule_number = "NG", -1

                    check_results = [
                        (cc2_status, cc2_rule_number, "cc2"),
                        (cc3_status, cc3_rule_number, "cc3"),
                        (cc4_status, cc4_rule_number, "cc4"),
                    ]

                    for index, (status, rule_number, check_key) in enumerate(
                        check_results
                    ):
                        ng_name = ng_content.get(check_key) if status == "NG" else ""
                        output_rows.append(
                            {
                                "Result": status,
                                "Task": function_name,
                                "Section": "AutoRun",
                                "RungNo": coil_row["RUNG"] - 1,
                                "Target": attr["operand"],
                                "CheckItem": rule_1_check_item,
                                "Detail": ng_name,
                                "Status": "",
                            }
                        )

        return output_rows

    except:
        logger.error("Error in check_detail_cycle_start_end_info_functionwise")
        return output_rows


# ============================== Program-Wise Execution Starts Here ===============================
def execute_rule_1_programwise(
    input_program_file: str,
    input_program_comment_file: str,
) -> pd.DataFrame:

    logger.info("Rule 1 Start executing rule 1 program wise")
    output_rows = []

    try:
        program_df = pd.read_csv(input_program_file)
        with open(input_program_comment_file, "r", encoding="utf-8") as file:
            program_comment_data = json.load(file)

        unique_program_values = program_df["PROGRAM"].unique()

        for program in unique_program_values:

            unique_section_df = program_df[program_df["PROGRAM"] == program]
            unique_section_values = unique_section_df["BODY"].str.lower().unique()

            if (
                "autorun" in unique_section_values
                or "autorun★" in unique_section_values
            ):

                (
                    program_cycle_start_end_rungnumber_info,
                    program_cycle_start_end_operand_info,
                ) = extract_cycle_start_end_programwise(
                    program_df=program_df,
                    section_name=section_name,
                    section_name_with_star=section_name_with_star,
                    program_comment_data=program_comment_data,
                )
                for (
                    program_name,
                    cycle_start_end_rung_number,
                ) in program_cycle_start_end_rungnumber_info.items():
                    if (
                        cycle_start_end_rung_number[0] != -1
                        and cycle_start_end_rung_number[1] != -1
                    ):
                        for index, cycle_start_end_info in enumerate(
                            [cycle_start_end_rung_number, cycle_start_end_rung_number]
                        ):
                            cycle_start_end_coil_operand = (
                                program_cycle_start_end_operand_info[program_name][0]
                                if index == 0
                                else program_cycle_start_end_operand_info[program_name][
                                    1
                                ]
                            )
                            target_rung = (
                                cycle_start_end_info[0]
                                if index == 0
                                else cycle_start_end_info[1]
                            )
                            output_rows.append(
                                {
                                    "Result": "OK",
                                    "Task": program_name,
                                    "Section": "AutoRun",
                                    "RungNo": target_rung - 1,
                                    "Target": cycle_start_end_coil_operand,
                                    "CheckItem": rule_1_check_item,
                                    "Detail": "",
                                    "Status": "",
                                }
                            )

                        output_rows = check_detail_cycle_start_end_info_programwise(
                            program_df=program_df,
                            program_name=program_name,
                            program_comment_data=program_comment_data,
                            section_name=section_name,
                            cycle_start_end_rung_number=cycle_start_end_rung_number,
                            output_rows=output_rows,
                        )
                    else:
                        output_rows.append(
                            {
                                "Result": "NG",
                                "Task": program_name,
                                "Section": "AutoRun",
                                "RungNo": -1,
                                "Target": "",
                                "CheckItem": rule_1_check_item,
                                "Detail": ng_content.get("cc1"),
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
        logger.error(f"Rule 1 Error in execute_rule_1_programwise: {e}")

        return {"status": "NOT OK", "error": e}


# ============================== Function-Wise Execution Starts Here ===============================
def execute_rule_1_functionwise(
    input_function_file: str,
    input_function_comment_file: str,
) -> pd.DataFrame:

    logger.info("Rule 1 Start executing rule 1 function wise")
    output_rows = []

    try:
        function_df = pd.read_csv(input_function_file)
        with open(input_function_comment_file, "r", encoding="utf-8") as file:
            function_comment_data = json.load(file)

        unique_function_values = function_df["FUNCTION_BLOCK"].unique()

        for function in unique_function_values:

            unique_section_df = function_df[function_df["FUNCTION_BLOCK"] == function]
            unique_section_values = unique_section_df["BODY_TYPE"].str.lower().unique()

            if (
                "autorun" in unique_section_values
                or "autorun★" in unique_section_values
            ):

                (
                    function_cycle_start_end_rungnumber_info,
                    function_cycle_start_end_operand_info,
                ) = extract_cycle_start_end_functionwise(
                    function_df=function_df,
                    section_name=section_name,
                    section_name_with_star=section_name_with_star,
                    function_comment_data=function_comment_data,
                )
                for (
                    function_name,
                    cycle_start_end_rung_number,
                ) in function_cycle_start_end_rungnumber_info.items():
                    if (
                        cycle_start_end_rung_number[0] != -1
                        and cycle_start_end_rung_number[1] != -1
                    ):
                        for index, cycle_start_end_info in enumerate(
                            [cycle_start_end_rung_number, cycle_start_end_rung_number]
                        ):
                            cycle_start_end_coil_operand = (
                                function_cycle_start_end_operand_info[function_name][0]
                                if index == 0
                                else function_cycle_start_end_operand_info[
                                    function_name
                                ][1]
                            )
                            target_rung = (
                                cycle_start_end_info[0]
                                if index == 0
                                else cycle_start_end_info[1]
                            )

                            output_rows.append(
                                {
                                    "Result": "OK",
                                    "Task": function_name,
                                    "Section": "AutoRun",
                                    "RungNo": target_rung - 1,
                                    "Target": cycle_start_end_coil_operand,
                                    "CheckItem": rule_1_check_item,
                                    "Detail": ng_content.get("cc"),
                                    "Status": "",
                                }
                            )

                        output_rows = check_detail_cycle_start_end_info_functionwise(
                            function_df=function_df,
                            function_name=function_name,
                            function_comment_data=function_comment_data,
                            section_name=section_name,
                            cycle_start_end_rung_number=cycle_start_end_rung_number,
                            output_rows=output_rows,
                        )
                    else:

                        output_rows.append(
                            {
                                "Result": "NG",
                                "Task": function_name,
                                "Section": "AutoRun",
                                "RungNo": -1,
                                "Target": "",
                                "CheckItem": rule_1_check_item,
                                "Detail": ng_content.get("cc1"),
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
        logger.error(f"Rule 1 Error in execute_rule_1_functionwise: {e}")

        return {"status": "NOT OK", "error": str(e)}
