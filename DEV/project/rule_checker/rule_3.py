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


#########################
rule_content_3 = "After reading QR/ID, it is necessary to perform matching (checking for a match between the workpiece and the information)."
rule_3_check_item = "After reading QR/ID, it is necessary to perform matching (checking for a match between the workpiece and the information)."
rule_3_check_content_1 = "If an outcoil with “OK/Normal” in the variable comment in ⑤ is not found despite the detection of an outcoil with “QR読取開始(Start QR reading)” in the variable comment in ②, it is NG."
rule_3_check_content_2 = "If “Equal Function” is included in the condition of the outcoil detected in (5), the screen of the entire rung where the outcoil detected in (5) exists shall be displayed on the UI.Otherwise, go to ❸.At this time, if “equal function” is not included in the condition, then ❸ is performed. Otherwise, complete with ❷."
rule_3_check_content_3 = "To detect the variable of the A contact point included in the condition of the outcoil detected in ⑤."
rule_3_check_content_4 = "Detecting an outcoil with the same variable name of that variable and displaying on the UI a screen of the entire rung where that detected outcoil exists, as shown in ❷."


#######################3 Function for execution ##################################
def execute_rule_3(
    input_file: str,
    input_program_comment_file: str,
    program_key: str = "PROGRAM",
    body_type_key: str = "BODY",
) -> None:

    logger.info("Executing Rule 3")

    cycle_half_jp = r"ｻｲｸﾙ"
    cycle_full_jp = r"サイクル"

    start_jp = r"開始"
    end_jp = r"終了"
    normal_jp = r"正常"
    read_jp = r"読取"
    ok_jp = r"OK"
    qr_jp = r"QR"

    check_1_NG_comment = "NG name:QR読み取り後の照合を行う回路が検出できなかったため、照合していない可能性有(No circuit to perform matching after QR reading.)"

    # output_dict={'TASK_NAME':[], 'SECTION_NAME':[],   'RULE_NUMBER': [], 'CHECK_NUMBER':[], "RUNG_NUMBER":[], 'RULE_CONTENT':[], 'CHECK_CONTENT':[],  'TARGET_OUTCOIL': [], 'STATUS': [], 'NG_EXPLANATION':[]}
    output_dict = {
        "Result": [],
        "Task": [],
        "Section": [],
        "RungNo": [],
        "Target": [],
        "CheckItem": [],
        "Detail": [],
        "Status": [],
    }

    output_df = pd.DataFrame(output_dict)

    acceptable_en_block_list = ["=", ">", "<", "<>", "<=", "=>"]

    with open(input_program_comment_file, "r", encoding="utf-8") as file:
        comment_data = json.load(file)

    try:

        ladder_df = pl.read_csv(input_file)
        unique_program_values = ladder_df[program_key].unique()

        print("input_file", input_file)
        print("unique_program_values", unique_program_values)

        program_range_dict = {}
        ##The following code block detects the range required

        # program loop: The for loop that extracts ladder program filtered by program
        for program in unique_program_values:

            rung_range_dict = {}
            start_list = []
            end_list = []

            ladder_program = ladder_df.filter(ladder_df[program_key] == program)
            ladder_autorun = ladder_program.filter(
                (pl.col(body_type_key).cast(pl.Utf8).str.to_lowercase() == "autorun")
                | (pl.col(body_type_key).cast(pl.Utf8).str.to_lowercase() == "autorun★")
            )

            out_coil_df = ladder_autorun.filter(
                pl.col("OBJECT_TYPE_LIST").cast(pl.Utf8).str.to_lowercase() == "coil"
            )
            outcoil_rung_list = list(out_coil_df["RUNG"])
            outcoil_rung_name_list = list(out_coil_df["RUNG_NAME"])
            outcoil_attributes_list = list(out_coil_df["ATTRIBUTES"])

            # autorun loop: The loop examines the content inside Autorun loop only
            for rung_num, rung_name, coil_attribute in zip(
                outcoil_rung_list, outcoil_rung_name_list, outcoil_attributes_list
            ):

                coil_attribute = eval(coil_attribute)
                coil_operand = coil_attribute["operand"]
                coil_comment_dict = get_the_comment_from_program(
                    variable=coil_operand,
                    program=program,
                    input_comment_data=comment_data,
                )

                # Chcek If QR read start comment exists or not, and not the end list
                if (
                    regex_pattern_check(qr_jp, coil_comment_dict)
                    and regex_pattern_check(read_jp, coil_comment_dict)
                    and regex_pattern_check(start_jp, coil_comment_dict)
                ):
                    start_list.append(rung_num)

                # Check if Cycle End comment exists and that will be the endlist
                if regex_pattern_check(
                    cycle_half_jp, coil_comment_dict
                ) or regex_pattern_check(cycle_full_jp, coil_comment_dict):

                    if regex_pattern_check(end_jp, coil_comment_dict):

                        end_list.append(rung_num)

            rung_range_dict["start_list"] = start_list
            rung_range_dict["end_list"] = end_list
            program_range_dict[program] = rung_range_dict

        # Now Examine the each range, select those ranges which are non-zero, have equal length and end point is greater than start point
        for program in program_range_dict.keys():

            logger.info(f"Rule 3 Executing Rule 3 in program : {program}")

            start_list = program_range_dict[program]["start_list"]
            end_list = program_range_dict[program]["end_list"]

            if (len(start_list) != 0) and (len(end_list) != 0):
                logger.info(
                    f"Rule 3 Ranges detected for program: {program} start_list: {start_list}  end_list: {end_list}"
                )

            if len(start_list) != len(end_list):

                logger.info(
                    f"Rule 3 Open Ended Range: {program} start_list:{start_list} end_list:{end_list}"
                )

                # Open Eneded Range detected
                detail_dict = {}
                detail_dict["start_list"] = start_list
                detail_dict["end_list"] = end_list

                # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["3"],
                #                                             'CHECK_NUMBER':["2"], 'RULE_CONTENT':["Open Ended Range"], 'STATUS': ['NG'], 'DETAILS': [detail_dict],
                #                                             'NG_EXPLANATION':["Range that has either Start point or End point not both"]}

                # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["3"],
                #             'CHECK_NUMBER':["2"], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_3],
                #             'CHECK_CONTENT':[rule_3_check_content_2],'TARGET_OUTCOIL': [detail_dict],
                #             'STATUS': ["NG"], 'NG_EXPLANATION':["Range that has either Start point or End point not both"]}

                sub_dict = {
                    "Result": ["NG"],
                    "Task": [program],
                    "Section": ["AutoRun"],
                    "RungNo": [""],
                    "Target": [detail_dict],
                    "CheckItem": rule_3_check_item,
                    "Detail": [
                        "Range that has either Start point or End point not both"
                    ],
                    "Status": [""],
                }

                sub_df = pd.DataFrame(sub_dict)

                output_df = pd.concat([output_df, sub_df], ignore_index=True)

            if len(start_list) == len(end_list):

                if len(start_list) != 0:

                    check_1_NG_flag = 0

                    # range loop: The loop that examines as per each range detected
                    for start_rung, end_rung in zip(start_list, end_list):

                        if int(end_rung) > int(start_rung):

                            # Once in the rung range, extract the ladder, contacts, coils and block that fall within the range
                            ladder_program = ladder_df.filter(
                                ladder_df[program_key] == program
                            )
                            ladder_autorun = ladder_program.filter(
                                (pl.col(body_type_key) == "AutoRun")
                                | (pl.col(body_type_key) == "AutoRun★")
                            )

                            out_coil_df = ladder_autorun.filter(
                                pl.col("OBJECT_TYPE_LIST")
                                .cast(pl.Utf8)
                                .str.to_lowercase()
                                == "coil"
                            )
                            out_coil_df_rung_range = out_coil_df.filter(
                                (pl.col("RUNG") >= start_rung)
                                & (pl.col("RUNG") <= end_rung)
                            )

                            ladder_program_for_rungs_in_range = ladder_autorun.filter(
                                (pl.col("RUNG") >= start_rung)
                                & (pl.col("RUNG") <= end_rung)
                            )

                            # Extract all the rung, attributes from each of the contacts, blocks and coils
                            contacts_program_for_rungs_in_range = (
                                ladder_program_for_rungs_in_range.filter(
                                    (
                                        pl.col("OBJECT_TYPE_LIST")
                                        .cast(pl.Utf8)
                                        .str.to_lowercase()
                                        == "contact"
                                    )
                                )
                            )
                            contacts_rungs = list(
                                contacts_program_for_rungs_in_range["RUNG"]
                            )
                            contacts_attributes = list(
                                contacts_program_for_rungs_in_range["ATTRIBUTES"]
                            )

                            blocks_program_for_rungs_in_range = (
                                ladder_program_for_rungs_in_range.filter(
                                    (
                                        pl.col("OBJECT_TYPE_LIST")
                                        .cast(pl.Utf8)
                                        .str.to_lowercase()
                                        == "block"
                                    )
                                )
                            )
                            blocks_rungs = list(
                                blocks_program_for_rungs_in_range["RUNG"]
                            )
                            blocks_attributes = list(
                                blocks_program_for_rungs_in_range["ATTRIBUTES"]
                            )

                            coils_program_for_rungs_in_range = (
                                ladder_program_for_rungs_in_range.filter(
                                    (
                                        pl.col("OBJECT_TYPE_LIST")
                                        .cast(pl.Utf8)
                                        .str.to_lowercase()
                                        == "coil"
                                    )
                                )
                            )
                            coils_rungs = list(coils_program_for_rungs_in_range["RUNG"])
                            coils_attributes = list(
                                coils_program_for_rungs_in_range["ATTRIBUTES"]
                            )

                            # Extract the names of all rungs that fall within in the range
                            rungs_in_the_range = list(
                                ladder_program_for_rungs_in_range["RUNG"].unique()
                            )
                            rungs_in_the_range.sort(reverse=True)

                            # Identify the rungs which are have two or more outcoils and sort them in descending order and also extract their respective rungs
                            out_coil_per_rung_frequency = sorted(
                                out_coil_df_rung_range.select(
                                    pl.col("RUNG").value_counts()
                                )
                                .unnest("RUNG")
                                .filter(pl.col("count") >= 2)
                                .to_dicts(),
                                key=lambda x: x["RUNG"],
                                reverse=True,
                            )

                            rungs_with_two_or_more_outcoils = [
                                ele["RUNG"] for ele in out_coil_per_rung_frequency
                            ]

                            """
                            In the below code block
                            1) Look into each of the rung which has two or more outcoils
                            2) Look in each rung whther there exist an outcoil with ok_jp comment
                                Then look in the same rung whether EN block of any type exists or not.
                                If exists then break the loop set status ='OK'
                            3) If not in the same rung look for 'A' contacts which has ok_jp comment, 
                                Let this contact be CC1
                                Look in the preceeding rungs whether an outcoil exists which has same operand as CC1,
                                Now look in the same rung, whether EN block exists or not
                                If exists break the loop and set status ='OK'
                            
                            If the status is not 'OK' then it is 'NG'
                            
                            """

                            status_flag = 0
                            target_en_rung = ""
                            ok_outcoil_operand = ""
                            minimum_one_ok_contact_flag = 0

                            for (
                                rung_with_two_or_more_outcoils
                            ) in rungs_with_two_or_more_outcoils:

                                logger.info(
                                    f"Rule 3 In Rung with two or more coils, looking into {rung_with_two_or_more_outcoils-1}"
                                )

                                df_rung_for_check = (
                                    ladder_program_for_rungs_in_range.filter(
                                        (
                                            pl.col("RUNG")
                                            == rung_with_two_or_more_outcoils
                                        )
                                    )
                                )

                                df_rung_outcoil_for_check = df_rung_for_check.filter(
                                    (
                                        pl.col("OBJECT_TYPE_LIST")
                                        .cast(pl.Utf8)
                                        .str.to_lowercase()
                                        == "coil"
                                    )
                                )

                                outcoil_for_check_attributes = list(
                                    df_rung_outcoil_for_check["ATTRIBUTES"]
                                )
                                ok_outcoil_found_flag = 0
                                en_block_found_flag = 0

                                # This code block checks whether EN block exists with in the rung with the OK outcoil
                                for outcoil_attribute in outcoil_for_check_attributes:
                                    outcoil_attribute = eval(outcoil_attribute)
                                    coil_operand = outcoil_attribute["operand"]
                                    comment_list = get_the_comment_from_program(
                                        variable=coil_operand,
                                        program=program,
                                        input_comment_data=comment_data,
                                    )

                                    comment_patterns_to_checked = [ok_jp, normal_jp]

                                    for pattern_ in comment_patterns_to_checked:

                                        if regex_pattern_check(pattern_, comment_list):

                                            ok_outcoil_found_flag = 1
                                            ok_outcoil_operand = coil_operand

                                            check_1_NG_flag = 1

                                            logger.info(
                                                f"Rule 3 For start_rung:{start_rung-1}, end_rung:{end_rung-1}, pattern: {pattern_} found"
                                            )

                                            # Append the result to output, for Check 1, when the outcoil of the required comment is found
                                            detail_dict = {}
                                            detail_dict["rung_range"] = (
                                                f"{start_rung-1}-{end_rung-1}"
                                            )
                                            detail_dict["pattern_checked"] = pattern_
                                            detail_dict["coil_variable"] = (
                                                ok_outcoil_operand
                                            )

                                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["3"],
                                            #         'CHECK_NUMBER':["1"], 'RULE_CONTENT':[f"Outcoil with {pattern_} Found"],
                                            #         'STATUS': ['OK'], 'DETAILS': [detail_dict],
                                            #         'NG_EXPLANATION':["NONE"]}

                                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["3"],
                                            #     'CHECK_NUMBER':["1"], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_3],
                                            #     'CHECK_CONTENT':[rule_3_check_content_1],'TARGET_OUTCOIL': [detail_dict],
                                            #     'STATUS': ["OK"], 'NG_EXPLANATION':["NONE"]}

                                            sub_dict = {
                                                "Result": ["OK"],
                                                "Task": [program],
                                                "Section": ["AutoRun"],
                                                "RungNo": [""],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_3_check_item,
                                                "Detail": [""],
                                                "Status": [""],
                                            }

                                            sub_df = pd.DataFrame(sub_dict)

                                            output_df = pd.concat(
                                                [output_df, sub_df], ignore_index=True
                                            )

                                            # If in the same rung, EN block is found, then condition is met, OK will be the output
                                            for blocks_rung, blocks_attribute in zip(
                                                blocks_rungs, blocks_attributes
                                            ):
                                                blocks_attribute = eval(
                                                    blocks_attribute
                                                )

                                                if (
                                                    blocks_rung
                                                    == rung_with_two_or_more_outcoils
                                                ):
                                                    blocks_typename = (
                                                        blocks_attribute.get(
                                                            "typeName", "NONE"
                                                        )
                                                    )

                                                    # If the block type matches with any one in the acceptable list
                                                    if (
                                                        blocks_typename
                                                        in acceptable_en_block_list
                                                    ):

                                                        en_block_found_flag = 1
                                                        status_flag = 1

                                                        target_en_rung = blocks_rung
                                                        ok_outcoil_operand = (
                                                            coil_operand
                                                        )

                                                        # Append the data to output df, For the check 2
                                                        detail_dict = {}

                                                        detail_dict["rung_range"] = (
                                                            f"{start_rung-1}-{end_rung-1}"
                                                        )
                                                        detail_dict[
                                                            "target_EN_rung"
                                                        ] = (target_en_rung - 1)
                                                        detail_dict["OK_out_coil"] = (
                                                            coil_operand
                                                        )
                                                        detail_dict[
                                                            "pattern_checked"
                                                        ] = pattern_

                                                        # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["3"],
                                                        #         'CHECK_NUMBER':["2"], 'RULE_CONTENT':[f"EN {blocks_typename} block detected"],
                                                        #         'STATUS': ['OK'], 'DETAILS': [detail_dict],
                                                        #         'NG_EXPLANATION':["NONE"]}

                                                        # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["3"],
                                                        #     'CHECK_NUMBER':["2"], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_3],
                                                        #     'CHECK_CONTENT':[rule_3_check_content_2],'TARGET_OUTCOIL': [detail_dict],
                                                        #     'STATUS': ["OK"], 'NG_EXPLANATION':["NONE"]}

                                                        sub_dict = {
                                                            "Result": ["OK"],
                                                            "Task": [program],
                                                            "Section": ["AutoRun"],
                                                            "RungNo": [""],
                                                            "Target": [detail_dict],
                                                            "CheckItem": rule_3_check_item,
                                                            "Detail": [""],
                                                            "Status": [""],
                                                        }

                                                        sub_df = pd.DataFrame(sub_dict)

                                                        output_df = pd.concat(
                                                            [output_df, sub_df],
                                                            ignore_index=True,
                                                        )

                                                        logger.info(
                                                            f"Rule 3 EN {blocks_typename} block found in \n{blocks_rung} \n{ok_outcoil_operand} \nfor start_rung{start_rung} and end_rung:{end_rung}"
                                                        )

                                                        break

                                            # If EN block is not found in same rung, then look for A contacts which have OK/Normal pattern
                                            if not (en_block_found_flag):

                                                ok_contacts = []

                                                # Looks for contacts in the same rung with Ok comment
                                                for (
                                                    contacts_rung,
                                                    contacts_attribute,
                                                ) in zip(
                                                    contacts_rungs, contacts_attributes
                                                ):

                                                    if (
                                                        contacts_rung
                                                        <= rung_with_two_or_more_outcoils
                                                    ):

                                                        contacts_attribute = eval(
                                                            contacts_attribute
                                                        )
                                                        contact_comment_list = get_the_comment_from_program(
                                                            variable=contacts_attribute[
                                                                "operand"
                                                            ],
                                                            program=program,
                                                            input_comment_data=comment_data,
                                                        )

                                                        if (
                                                            regex_pattern_check(
                                                                pattern_,
                                                                contact_comment_list,
                                                            )
                                                            and (
                                                                contacts_attribute[
                                                                    "operand"
                                                                ]
                                                                != ok_outcoil_operand
                                                            )
                                                            and (
                                                                contacts_attribute[
                                                                    "negated"
                                                                ]
                                                                == "false"
                                                            )
                                                        ):

                                                            ok_contacts.append(
                                                                contacts_attribute[
                                                                    "operand"
                                                                ]
                                                            )

                                                # Check 3, when A contacts with same variable is not found
                                                if len(ok_contacts) == 0:

                                                    minimum_one_ok_contact_flag = 1
                                                    status_flag = 0

                                                    detail_dict = {}

                                                    detail_dict["rung_range"] = (
                                                        f"{start_rung-1}-{end_rung-1}"
                                                    )
                                                    detail_dict["target_rung"] = (
                                                        rung_with_two_or_more_outcoils
                                                        - 1
                                                    )
                                                    detail_dict["pattern_checked"] = (
                                                        pattern_
                                                    )
                                                    detail_dict["OK_out_coil"] = "NONE"
                                                    detail_dict[
                                                        "outcoil_with_no_contact"
                                                    ] = coil_operand

                                                    ng_str = "A contact with same variable not found"

                                                    # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["3"],
                                                    #         'CHECK_NUMBER':["3"], 'RULE_CONTENT':[ng_str],
                                                    #          'STATUS': ['NG'], 'DETAILS': [detail_dict],
                                                    #         'NG_EXPLANATION':[ng_str]}

                                                    # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["3"],
                                                    #         'CHECK_NUMBER':["3"], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_3],
                                                    #         'CHECK_CONTENT':[rule_3_check_content_3],'TARGET_OUTCOIL': [detail_dict],
                                                    #         'STATUS': ["NG"], 'NG_EXPLANATION':[ng_str]}

                                                    # sub_dict={'Result':["NG"], 'Task':[program], 'Section': ['AutoRun'],
                                                    #         'RungNo':[""], "Target":[detail_dict],
                                                    #         'CheckItem':rule_3_check_item,
                                                    #         'Detail': [ng_str],  'Status': ['']}

                                                    # sub_df=pd.DataFrame(sub_dict)

                                                    # output_df=pd.concat([output_df, sub_df], ignore_index=True)

                                                    # logger.info(f"Rule 3 Matching A contacts not found for {coil_operand} in {detail_dict['target_rung']}")

                                                else:

                                                    detail_dict = {}

                                                    detail_dict["rung_range"] = (
                                                        f"{start_rung-1}-{end_rung-1}"
                                                    )
                                                    detail_dict["target_rung"] = (
                                                        rung_with_two_or_more_outcoils
                                                        - 1
                                                    )
                                                    detail_dict["pattern_checked"] = (
                                                        pattern_
                                                    )
                                                    detail_dict["OK_out_coil"] = (
                                                        ok_contacts
                                                    )
                                                    detail_dict[
                                                        "outcoil_with_no_contact"
                                                    ] = coil_operand

                                                    # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["3"],
                                                    #         'CHECK_NUMBER':["3"], 'RULE_CONTENT':["A contacts with same variable name found"],
                                                    #          'STATUS': ['OK'], 'DETAILS': [detail_dict],
                                                    #         'NG_EXPLANATION':["NONE"]}

                                                    # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["3"],
                                                    #         'CHECK_NUMBER':["3"], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_3],
                                                    #         'CHECK_CONTENT':[rule_3_check_content_3],'TARGET_OUTCOIL': [detail_dict],
                                                    #         'STATUS': ["OK"], 'NG_EXPLANATION':["NONE"]}

                                                    # sub_dict={'Result':["OK"], 'Task':[program], 'Section': ['AutoRun'],
                                                    #         'RungNo':[""], "Target":[detail_dict],
                                                    #         'CheckItem':rule_3_check_item,
                                                    #         'Detail': [""],  'Status': ['']}

                                                    # sub_df=pd.DataFrame(sub_dict)

                                                    # output_df=pd.concat([output_df, sub_df], ignore_index=True)

                                                # For each ok contact, get the outcoil with same variable and then check whetehr EN block exists or not
                                                ok_contact_en_block_flag = 0
                                                for ok_contact in ok_contacts:

                                                    for (
                                                        coils_rung,
                                                        coils_attribute,
                                                    ) in zip(
                                                        coils_rungs, coils_attributes
                                                    ):

                                                        if (
                                                            coils_rung
                                                            <= rung_with_two_or_more_outcoils
                                                        ):

                                                            coils_attribute = eval(
                                                                coils_attribute
                                                            )

                                                            if (
                                                                ok_contact
                                                                == coils_attribute[
                                                                    "operand"
                                                                ]
                                                            ):

                                                                for (
                                                                    blocks_rung,
                                                                    blocks_attribute,
                                                                ) in zip(
                                                                    blocks_rungs,
                                                                    blocks_attributes,
                                                                ):

                                                                    if (
                                                                        blocks_rung
                                                                        == coils_rung
                                                                    ):

                                                                        blocks_attribute = eval(
                                                                            blocks_attribute
                                                                        )

                                                                        blocks_attribute_typename = blocks_attribute.get(
                                                                            "typeName",
                                                                            "NONE",
                                                                        )
                                                                        if (
                                                                            blocks_attribute_typename
                                                                            in acceptable_en_block_list
                                                                        ):

                                                                            ok_contact_en_block_flag = (
                                                                                1
                                                                            )
                                                                            target_en_rung = blocks_rung
                                                                            ok_outcoil_operand = coils_attribute[
                                                                                "operand"
                                                                            ]

                                                                            # Check3, when the A contact found with same variable found
                                                                            detail_dict = (
                                                                                {}
                                                                            )

                                                                            detail_dict[
                                                                                "rung_range"
                                                                            ] = f"{start_rung-1}-{end_rung-1}"
                                                                            detail_dict[
                                                                                "target_EN_rung"
                                                                            ] = (
                                                                                target_en_rung
                                                                                - 1
                                                                            )
                                                                            detail_dict[
                                                                                "pattern_checked"
                                                                            ] = pattern_
                                                                            detail_dict[
                                                                                "OK_out_coil"
                                                                            ] = coils_attribute[
                                                                                "operand"
                                                                            ]

                                                                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["3"],
                                                                            #         'CHECK_NUMBER':["4"], 'RULE_CONTENT':[f"EN block type:{blocks_attribute_typename} found"],
                                                                            #         'STATUS': ['OK'], 'DETAILS': [detail_dict],
                                                                            #         'NG_EXPLANATION':["NONE"]}

                                                                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["3"],
                                                                            #         'CHECK_NUMBER':["4"], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_3],
                                                                            #         'CHECK_CONTENT':[rule_3_check_content_4],'TARGET_OUTCOIL': [detail_dict],
                                                                            #         'STATUS': ["OK"], 'NG_EXPLANATION':["NONE"]}

                                                                            sub_dict = {
                                                                                "Result": [
                                                                                    "OK"
                                                                                ],
                                                                                "Task": [
                                                                                    program
                                                                                ],
                                                                                "Section": [
                                                                                    "AutoRun"
                                                                                ],
                                                                                "RungNo": [
                                                                                    ""
                                                                                ],
                                                                                "Target": [
                                                                                    detail_dict
                                                                                ],
                                                                                "CheckItem": rule_3_check_item,
                                                                                "Detail": [
                                                                                    ""
                                                                                ],
                                                                                "Status": [
                                                                                    ""
                                                                                ],
                                                                            }

                                                                            sub_df = pd.DataFrame(
                                                                                sub_dict
                                                                            )

                                                                            output_df = pd.concat(
                                                                                [
                                                                                    output_df,
                                                                                    sub_df,
                                                                                ],
                                                                                ignore_index=True,
                                                                            )

                                                                            logger.info(
                                                                                f"Rule 3 EN = block found in preceeding rungs {blocks_rung-1} {ok_outcoil_operand} for start_rung{start_rung-1} and end_rung:{end_rung-1}"
                                                                            )

                                                                            break

                                                # even if the En block not found for ok_contact in preceeding rungs, then flag it NG, check 4
                                                if ok_contact_en_block_flag == 0:

                                                    detail_dict = {}

                                                    detail_dict["rung_range"] = (
                                                        f"{start_rung-1}-{end_rung-1}"
                                                    )
                                                    detail_dict["pattern_checked"] = (
                                                        pattern_
                                                    )

                                                    # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["3"],
                                                    #         'CHECK_NUMBER':["4"], 'RULE_CONTENT':["EN block NOT detected in preceeding rungs"],
                                                    #         'STATUS': ['NG'], 'DETAILS': [detail_dict],
                                                    #         'NG_EXPLANATION':["EN block NOT detected in preceeding rungs"]}

                                                    # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["3"],
                                                    #         'CHECK_NUMBER':["4"], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_3],
                                                    #         'CHECK_CONTENT':[rule_3_check_content_4],'TARGET_OUTCOIL': [detail_dict],
                                                    #         'STATUS': ["NG"], 'NG_EXPLANATION':["EN block NOT detected in preceeding rungs"]}

                                                    sub_dict = {
                                                        "Result": ["NG"],
                                                        "Task": [program],
                                                        "Section": ["AutoRun"],
                                                        "RungNo": [""],
                                                        "Target": [detail_dict],
                                                        "CheckItem": rule_3_check_item,
                                                        "Detail": [
                                                            "EN block NOT detected in preceeding rungs"
                                                        ],
                                                        "Status": [""],
                                                    }

                                                    sub_df = pd.DataFrame(sub_dict)

                                                    output_df = pd.concat(
                                                        [output_df, sub_df],
                                                        ignore_index=True,
                                                    )

                                                    logger.info(
                                                        f"Rule 3 EN = block NOT found in preceeding rungs {blocks_rung-1}  for start_rung{start_rung-1} and end_rung:{end_rung-1}"
                                                    )

                        # After each iteration of range loop, check whether ok or normal pattern has been detected, if NO, then flag the error type 1
                        if check_1_NG_flag == 0:

                            detail_dict = {}

                            detail_dict["rung_range"] = f"{start_rung-1}-{end_rung-1}"

                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["3"],
                            #         'CHECK_NUMBER':["1"], 'RULE_CONTENT':["Range detected but Ok/Normal not detected"],
                            #         'STATUS': ['NG'], 'DETAILS': [detail_dict],
                            #         'NG_EXPLANATION':[check_1_NG_comment]}

                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["3"],
                            #     'CHECK_NUMBER':["1"], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_3],
                            #     'CHECK_CONTENT':[rule_3_check_content_1],'TARGET_OUTCOIL': [detail_dict],
                            #     'STATUS': ["NG"], 'NG_EXPLANATION':[check_1_NG_comment]}

                            sub_dict = {
                                "Result": ["NG"],
                                "Task": [program],
                                "Section": ["AutoRun"],
                                "RungNo": [""],
                                "Target": [detail_dict],
                                "CheckItem": rule_3_check_item,
                                "Detail": [check_1_NG_comment],
                                "Status": [""],
                            }

                            sub_df = pd.DataFrame(sub_dict)

                            output_df = pd.concat(
                                [output_df, sub_df], ignore_index=True
                            )

                            logger.info(
                                f"Rule 3 NG of nature: QRReadStart Found but not OK/NORMAL found in start_rung:{start_rung} end_rung:{end_rung}"
                            )

        return {"status": "OK", "output_df": output_df}

    except Exception as e:
        logger.error(str(e))
        return {"status": "NOT OK", "error": e}


######################################################################################3


###########################################################################################


# if __name__ == "__main__":

#     input_file=r"C:\Users\aniln\OneDrive - OPTIMIZED SOLUTIONS LTD\DENSO\GithubCode\rules_personal\Important\FASTAPI_POC_DENSO\input_files\Coding Checker_Rule1-3_250611\Coding Checker_Rule1-3_250611_programwise.csv"
#     input_program_comment_file=r"C:\Users\aniln\OneDrive - OPTIMIZED SOLUTIONS LTD\DENSO\GithubCode\rules_personal\Important\FASTAPI_POC_DENSO\input_files\Coding Checker_Rule1-3_250611\Coding Checker_Rule1-3_250611_programwise.json"
#     execute_rule_3(input_file=input_file, input_program_comment_file=input_program_comment_file,output_file_name="output_Rule_3_program.csv",
#                                program_key='PROGRAM', body_type_key='BODY')


# input_file=r"C:\Users\Lenovo\OneDrive - OPTIMIZED SOLUTIONS LTD\Desktop\Evertything\DENSO_coding_checker\CodeBase\Repository\data_modelling\data_model_Rule_3\data_model_Rule_3_functionwise.csv"
# input_program_comment_file=r"C:\Users\Lenovo\OneDrive - OPTIMIZED SOLUTIONS LTD\Desktop\Evertything\DENSO_coding_checker\CodeBase\Repository\data_modelling\data_model_Rule_3\comments_rule_3_functionwise.json"

# execute_rule_3(input_file=input_file, input_program_comment_file=input_program_comment_file,output_file_name="output_Rule_3_function.csv",
#                            program_key='FUNCTION_BLOCK', body_type_key='BODY_TYPE')


# kk=get_the_comment_from_program(variable='LB600[199]' ,program='P111_Sample1_OK', input_comment_file=input_program_comment_file)
# print(kk)
# 568,413
