from bs4 import BeautifulSoup
from typing import *
import pandas as pd
import re, json
import itertools

#########################################################################################


def get_the_comment_from_program(
    variable: str, program: str = "", input_comment_data: Dict = None
) -> List:

    data = input_comment_data
    special_case_flag = 0

    # scan for special case first
    try:
        variable_prefix = variable.split(".")[0]
        variable_suffix = variable.split(".")[1]
        special_case_flag = 1

    except:
        special_case_flag = 0

    if special_case_flag:

        return_list = []

        ################################Extract the Prefix First ####################################################
        # Scan Programwise
        comment_prefix = data.get(f"{variable_prefix}@PG@{program}", "NONE")

        if comment_prefix != "NONE":

            variable_english_comment = comment_prefix.get(
                "variable_english_comment", "NONE"
            )
            variable_japanese_comment = comment_prefix.get(
                "variable_japanese_comment", "NONE"
            )
            documentation = comment_prefix.get("documentation", "NONE")

            if (
                (variable_english_comment != "NONE")
                or (variable_japanese_comment != "NONE")
                or (documentation != "NONE")
            ):

                return_list.append(comment_prefix)

        # Scan for Global Variables
        comment_prefix = data.get(f"{variable_prefix}@GBVAR", "NONE")
        if comment_prefix != "NONE":

            variable_english_comment = comment_prefix.get(
                "variable_english_comment", "NONE"
            )
            variable_japanese_comment = comment_prefix.get(
                "variable_japanese_comment", "NONE"
            )
            documentation = comment_prefix.get("documentation", "NONE")

            if (
                (variable_english_comment != "NONE")
                or (variable_japanese_comment != "NONE")
                or (documentation != "NONE")
            ):

                return_list.append(comment_prefix)

        ####################################Extract the Suffix then ##################################################3
        # Scan Programwise
        comment_suffix = data.get(f"{variable_suffix}@PG@{program}", "NONE")

        if comment_suffix != "NONE":

            variable_english_comment = comment_suffix.get(
                "variable_english_comment", "NONE"
            )
            variable_japanese_comment = comment_suffix.get(
                "variable_japanese_comment", "NONE"
            )
            documentation = comment_suffix.get("documentation", "NONE")

            if (
                (variable_english_comment != "NONE")
                or (variable_japanese_comment != "NONE")
                or (documentation != "NONE")
            ):

                return_list.append(comment_suffix)

        # Scan for Global Variables
        comment_suffix = data.get(f"{variable_suffix}@GBNMSP", "NONE")
        if comment_suffix != "NONE":

            member_english_comment = comment_suffix.get(
                "member_english_comment", "NONE"
            )
            member_japanese_comment = comment_suffix.get(
                "member_japanese_comment", "NONE"
            )
            member_documentation = comment_suffix.get("member_documentation", "NONE")

            if (
                (member_english_comment != "NONE")
                or (member_japanese_comment != "NONE")
                or (member_documentation != "NONE")
            ):

                return_list.append(comment_suffix)

        # Flatten the list
        return_list = [list(ele.values()) for ele in return_list]
        return_list = list(itertools.chain(*return_list))

        return return_list

    else:

        # Scan Programwise
        comment_program = data.get(f"{variable}@PG@{program}", "NONE")

        if comment_program != "NONE":

            variable_english_comment = comment_program.get(
                "variable_english_comment", "NONE"
            )
            variable_japanese_comment = comment_program.get(
                "variable_japanese_comment", "NONE"
            )
            documentation = comment_program.get("documentation", "NONE")

            if (
                (variable_english_comment != "NONE")
                or (variable_japanese_comment != "NONE")
                or (documentation != "NONE")
            ):

                comment_list = list(comment_program.values())
                return comment_list

        # Scan for Global Variables
        comment_program = data.get(f"{variable}@GBVAR", "NONE")
        if comment_program != "NONE":

            variable_english_comment = comment_program.get(
                "variable_english_comment", "NONE"
            )
            variable_japanese_comment = comment_program.get(
                "variable_japanese_comment", "NONE"
            )
            documentation = comment_program.get("documentation", "NONE")

            if (
                (variable_english_comment != "NONE")
                or (variable_japanese_comment != "NONE")
                or (documentation != "NONE")
            ):

                comment_list = list(comment_program.values())
                return comment_list


########################################################
def get_the_comment_from_function(
    variable: str, function: str = "", input_comment_data: Dict = None
) -> List:

    data = input_comment_data
    special_case_flag = 0

    # scan for special case first
    try:
        variable_prefix = variable.split(".")[0]
        variable_suffix = variable.split(".")[1]
        special_case_flag = 1

    except:
        special_case_flag = 0

    if special_case_flag:

        return_list = []

        ################################Extract the Prefix First ####################################################
        # Scan Programwise
        comment_prefix = data.get(f"{variable_prefix}@FN@{function}", "NONE")

        if comment_prefix != "NONE":

            variable_english_comment = comment_prefix.get(
                "variable_english_comment", "NONE"
            )
            variable_japanese_comment = comment_prefix.get(
                "variable_japanese_comment", "NONE"
            )
            documentation = comment_prefix.get("documentation", "NONE")

            if (
                (variable_english_comment != "NONE")
                or (variable_japanese_comment != "NONE")
                or (documentation != "NONE")
            ):

                return_list.append(comment_prefix)

        # Scan for Global Variables
        comment_prefix = data.get(f"{variable_prefix}@GBVAR", "NONE")
        if comment_prefix != "NONE":

            variable_english_comment = comment_prefix.get(
                "variable_english_comment", "NONE"
            )
            variable_japanese_comment = comment_prefix.get(
                "variable_japanese_comment", "NONE"
            )
            documentation = comment_prefix.get("documentation", "NONE")

            if (
                (variable_english_comment != "NONE")
                or (variable_japanese_comment != "NONE")
                or (documentation != "NONE")
            ):

                return_list.append(comment_prefix)

        ####################################Extract the Suffix then ##################################################3
        # Scan Programwise
        comment_suffix = data.get(f"{variable_suffix}@FN@{function}", "NONE")

        if comment_suffix != "NONE":

            variable_english_comment = comment_suffix.get(
                "variable_english_comment", "NONE"
            )
            variable_japanese_comment = comment_suffix.get(
                "variable_japanese_comment", "NONE"
            )
            documentation = comment_suffix.get("documentation", "NONE")

            if (
                (variable_english_comment != "NONE")
                or (variable_japanese_comment != "NONE")
                or (documentation != "NONE")
            ):

                return_list.append(comment_suffix)

        # Scan for Global Variables
        comment_suffix = data.get(f"{variable_suffix}@GBNMSP", "NONE")
        if comment_suffix != "NONE":

            variable_english_comment = comment_suffix.get(
                "variable_english_comment", "NONE"
            )
            variable_japanese_comment = comment_suffix.get(
                "variable_japanese_comment", "NONE"
            )
            documentation = comment_suffix.get("documentation", "NONE")

            if (
                (variable_english_comment != "NONE")
                or (variable_japanese_comment != "NONE")
                or (documentation != "NONE")
            ):

                return_list.append(comment_suffix)

        # Flatten the list
        return_list = [list(ele.values()) for ele in return_list]
        return_list = list(itertools.chain(*return_list))

        return return_list

    else:

        # Scan Programwise
        comment_program = data.get(f"{variable}@FN@{function}", "NONE")

        if comment_program != "NONE":

            variable_english_comment = comment_program.get(
                "variable_english_comment", "NONE"
            )
            variable_japanese_comment = comment_program.get(
                "variable_japanese_comment", "NONE"
            )
            documentation = comment_program.get("documentation", "NONE")

            if (
                (variable_english_comment != "NONE")
                or (variable_japanese_comment != "NONE")
                or (documentation != "NONE")
            ):

                comment_list = list(comment_program.values())
                return comment_list

        # Scan for Global Variables
        comment_program = data.get(f"{variable}@GBVAR", "NONE")
        if comment_program != "NONE":

            variable_english_comment = comment_program.get(
                "variable_english_comment", "NONE"
            )
            variable_japanese_comment = comment_program.get(
                "variable_japanese_comment", "NONE"
            )
            documentation = comment_program.get("documentation", "NONE")

            if (
                (variable_english_comment != "NONE")
                or (variable_japanese_comment != "NONE")
                or (documentation != "NONE")
            ):

                comment_list = list(comment_program.values())
                return comment_list


############################################


if __name__ == "__main__":

    input_comment_file = "C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/GithubCode/rules_personal/data_modelling/data_model_Rule_27_32_NG_v2/data_model_Rule_27_32_NG_v2_programwise.json"
    with open(input_comment_file, "r", encoding="utf-8") as file:
        data = json.load(file)

    kk = get_the_comment_from_function(
        "L_RB_IN.B[833]", "P122_27NG", input_comment_data=data
    )
    print(kk)
