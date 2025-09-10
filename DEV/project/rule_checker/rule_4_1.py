from typing import *
import polars as pl
import re
import pandas as pd
from ...main import logger
from .extract_comment_from_variable import *
from .japanese_half_full_width_mapping import full_to_half_conversion
from .ladder_utils import regex_pattern_check, clean_rung_number

#########################
rule_contenta_4_1 = "The same contacts should be used for operating circuits and memory set conditions so that the transfer and memory feeds are synchronized."
rule_4_1_check_item = "Rule of Memoryfeeding(P&P)"
rule_4_1_check_content_1 = (
    "If the target is to be detected in ① but not in ③, NG is assumed."
)
rule_4_1_check_content_2 = "In the 'Individual' section, find an out-coil that contains 'チャック(chuck)' and ”起動条件(start condition)” and check that the condition is the same as the A contact detected by ③."
rule_4_1_check_content_3 = "In the 'MemoryFeeding' section, find an Set-outcoil that contains 'チャック(chuck)' and ”開始(start)” and ”記憶(memory)” and check that the condition is the same as the A contact detected by ③."


#######################3 Function for execution ##################################
def execute_rule_4_1_programwise(
    input_file: str, input_program_comment_file: str, input_image: str = None
) -> None:

    logger.info("Executing Rule 4.1")

    try:

        # Global Variables
        # program_of_interest_pattern=r"P111_"

        confirmation_jp = r"確認"
        condition_jp = r"条件"

        chuck_jp = r"ﾁｬｯｸ"
        unchuck_jp = r"ｱﾝﾁｬｯｸ"

        chuck_confirmation_operands = []
        unchuck_confirmation_operands = []

        chuck_condition_operands = []
        unchuck_condition_operands = []

        chuck_operands = []
        unchuck_operands = []

        # output_dict={'TASK_NAME':[], 'SECTION_NAME':[],   'RULE_NUMBER': [], 'CHECK_NUMBER':[], 'RULE_CONTENT':[], 'STATUS': [], 'DETAILS': [], 'NG_EXPLANATION':[]}
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

        check_1_chuck_NG_explanation = "チャック確認中：チャック確認回路が検出できなかった(Chuck checking: Chuck checking circuit could not could not be detected.)"
        check_1_unchuck_NG_explanation = "アンチャック確認中：アンチャック確認回路が検出できなかった(Unchuck checking: Unchuck checking circuit could not be detected.)"

        check_2_individual_NG_explanation = """Individual”セクション内でチャックの起動条件に搬送と同じ接点が使用されていないため、同期していない可能性有
                    (In the 'Individual' section, the chuck move start condition do not use the same conditions as the 'AutoRun' section, so they may not be synchronized.)"""

        check_2_memfeeding_NG_explanation = """Memory Feeding セクション内でチャック開始記憶の条件に搬送と同じ接点が使用されていないため、同期していない可能性有
                    (In the 'Memory Feeding' section, the chuck start memory condition do not use the same conditions as the 'AutoRun' section, so they may not be synchronized.)"""

        check_2_autorun_NG_explanation = """NONE"""

        ##################################################

        ladder_df = pl.read_csv(input_file)
        input_image_program_df = pl.read_csv(input_image)

        task_names = (
            input_image_program_df.filter(
                pl.col("Unit").cast(pl.Utf8).str.to_lowercase() == "p&p"
            )
            .select(pl.col("Task name").cast(pl.Utf8).str.to_lowercase())
            .to_series()
            .to_list()
        )

        with open(input_program_comment_file, "r", encoding="utf-8") as file:
            program_comment_data = json.load(file)

        unique_program_values = ladder_df["PROGRAM"].unique()

        for program in unique_program_values:

            logger.info(f"Rule 4.1 Executing Rule 4.1 in {program}")

            # Only if the Program is P111_
            print("program", program, task_names)
            if program.lower() in task_names:

                ladder_pgm_wise = ladder_df.filter(ladder_df["PROGRAM"] == program)

                # Get the section name in each of the PROGRAM
                unique_section_values = ladder_pgm_wise["BODY"].unique()

                ladder_autorun = ladder_pgm_wise.filter(
                    pl.col("BODY").cast(pl.Utf8).str.to_lowercase() == "autorun"
                )
                ladder_memory_feeding = ladder_pgm_wise.filter(
                    pl.col("BODY").cast(pl.Utf8).str.to_lowercase() == "memoryfeeding"
                )
                ladder_individual = ladder_pgm_wise.filter(
                    pl.col("BODY").cast(pl.Utf8).str.to_lowercase() == "individual"
                )

                # Scanning for Chuck and Unhuck condition OBJECT_TYPE_LIST

                if len(ladder_autorun) != 0:

                    out_coil_df = ladder_autorun.filter(
                        pl.col("OBJECT_TYPE_LIST").cast(pl.Utf8).str.to_lowercase()
                        == "coil"
                    )
                    outcoil_rung_list = list(out_coil_df["RUNG"])
                    outcoil_rung_name_list = list(out_coil_df["RUNG_NAME"])
                    outcoil_attributes_list = list(out_coil_df["ATTRIBUTES"])

                    for rung_num, rung_name, coil_attribute in zip(
                        outcoil_rung_list,
                        outcoil_rung_name_list,
                        outcoil_attributes_list,
                    ):
                        coil_attribute = eval(coil_attribute)

                        coil_operand = coil_attribute["operand"]
                        coil_comment_dict = get_the_comment_from_program(
                            variable=coil_operand,
                            program=program,
                            input_comment_data=program_comment_data,
                        )

                        if regex_pattern_check(
                            confirmation_jp, coil_comment_dict
                        ) and regex_pattern_check(unchuck_jp, coil_comment_dict):

                            operand_dict = {}
                            operand_dict[coil_operand] = coil_comment_dict
                            unchuck_confirmation_operands.append(coil_operand)

                            detail_dict = {}
                            detail_dict["VARIABLE"] = coil_operand

                            detail_dict["RUNG"] = rung_num

                            # sub_dict={'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["4.1"],
                            #         'CHECK_NUMBER':["1"],  "RUNG_NUMBER":[rung_num], 'RULE_CONTENT':[rule_contenta_4_1],
                            #         'CHECK_CONTENT':rule_4_1_check_content_1,
                            #         'TARGET_OUTCOIL': [detail_dict],  'STATUS': ['OK'],
                            #         'NG_EXPLANATION':['NONE']}

                            sub_dict = {
                                "Result": ["OK"],
                                "Task": [program],
                                "Section": ["AutoRun"],
                                "RungNo": [rung_num],
                                "Target": [detail_dict],
                                "CheckItem": rule_4_1_check_item,
                                "Detail": ["NONE"],
                                "Status": [""],
                            }

                            sub_df = pd.DataFrame(sub_dict)

                            output_df = pd.concat(
                                [output_df, sub_df], ignore_index=True
                            )

                        elif regex_pattern_check(
                            confirmation_jp, coil_comment_dict
                        ) and regex_pattern_check(chuck_jp, coil_comment_dict):

                            operand_dict = {}
                            operand_dict[coil_operand] = coil_comment_dict
                            chuck_confirmation_operands.append(coil_operand)

                            detail_dict = {}
                            detail_dict["VARIABLE"] = coil_operand

                            detail_dict["RUNG"] = rung_num

                            # sub_dict={

                            #     'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'],   'RULE_NUMBER': ["4.1"],
                            #     'CHECK_NUMBER':["1"], 'RULE_CONTENT':["CHUCK CONFIRMATION EXISTS"],
                            #     'STATUS': ['OK'], 'DETAILS': [detail_dict],
                            #     'NG_EXPLANATION':['NONE']

                            #     }

                            # sub_dict = {'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["4.1"],
                            #         'CHECK_NUMBER':["1"],  "RUNG_NUMBER":[rung_num], 'RULE_CONTENT':[rule_contenta_4_1],
                            #         'CHECK_CONTENT':rule_4_1_check_content_1,
                            #         'TARGET_OUTCOIL': [detail_dict],  'STATUS': ['OK'],
                            #         'NG_EXPLANATION':['NONE']}

                            sub_dict = {
                                "Result": ["OK"],
                                "Task": [program],
                                "Section": ["AutoRun"],
                                "RungNo": [rung_num],
                                "Target": [detail_dict],
                                "CheckItem": rule_4_1_check_item,
                                "Detail": ["NONE"],
                                "Status": [""],
                            }

                            sub_df = pd.DataFrame(sub_dict)

                            output_df = pd.concat(
                                [output_df, sub_df], ignore_index=True
                            )

                        elif regex_pattern_check(
                            condition_jp, coil_comment_dict
                        ) and regex_pattern_check(unchuck_jp, coil_comment_dict):

                            operand_dict = {}
                            unchuck_condition_ref_rung_dict = {}

                            operand_dict[coil_operand] = coil_comment_dict
                            unchuck_condition_operands.append(coil_operand)

                            unchuck_condition_ref_rung_dict[coil_operand] = rung_num

                        elif regex_pattern_check(
                            condition_jp, coil_comment_dict
                        ) and regex_pattern_check(chuck_jp, coil_comment_dict):

                            operand_dict = {}
                            operand_dict[coil_operand] = coil_comment_dict
                            chuck_condition_operands.append(coil_operand)

                            chuck_condition_ref_rung_dict = {}
                            chuck_condition_ref_rung_dict[coil_operand] = rung_num

                check_2_ladder_list = [
                    ladder_memory_feeding,
                    ladder_individual,
                    ladder_autorun,
                ]
                check_2_section_list = ["MemoryFeeding", "Individual", "AutoRun"]

                # Checking for Check 1
                if len(unchuck_confirmation_operands) == 0:

                    detail_dict = {}

                    # sub_dict={

                    #     'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'],   'RULE_NUMBER': ["4.1"],
                    #     'CHECK_NUMBER':["1"], 'RULE_CONTENT':["UNCHUCK CONFIRMATION  DOESNOT EXISTS"],
                    #     'STATUS': ['NG'], 'DETAILS': [detail_dict],

                    #     'NG_EXPLANATION':[check_1_unchuck_NG_explanation]

                    #     }

                    # sub_dict = {'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["4.1"],
                    #                 'CHECK_NUMBER':["1"],  "RUNG_NUMBER":[""], 'RULE_CONTENT':[rule_contenta_4_1],
                    #                 'CHECK_CONTENT':rule_4_1_check_content_1,
                    #                 'TARGET_OUTCOIL': [detail_dict],  'STATUS': ['NG'],
                    #                 'NG_EXPLANATION':[check_1_unchuck_NG_explanation]}

                    sub_dict = {
                        "Result": ["NG"],
                        "Task": [program],
                        "Section": ["AutoRun"],
                        "RungNo": [""],
                        "Target": [detail_dict],
                        "CheckItem": rule_4_1_check_item,
                        "Detail": [check_1_unchuck_NG_explanation],
                        "Status": [""],
                    }

                    sub_df = pd.DataFrame(sub_dict)

                    output_df = pd.concat([output_df, sub_df], ignore_index=True)

                if len(chuck_confirmation_operands) == 0:

                    detail_dict = {}

                    # sub_dict={

                    #     'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'],   'RULE_NUMBER': ["4.1"],
                    #     'CHECK_NUMBER':["1"], 'RULE_CONTENT':["CHUCK CONFIRMATION  DOESNOT EXISTS"],
                    #     'STATUS': ['NG'], 'DETAILS': [detail_dict],
                    #     'NG_EXPLANATION':[check_1_chuck_NG_explanation]

                    #     }

                    # sub_dict = {'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["4.1"],
                    #                 'CHECK_NUMBER':["1"],  "RUNG_NUMBER":[""], 'RULE_CONTENT':[rule_contenta_4_1],
                    #                 'CHECK_CONTENT':rule_4_1_check_content_1,
                    #                 'TARGET_OUTCOIL': [detail_dict],  'STATUS': ['NG'],
                    #                 'NG_EXPLANATION':[check_1_chuck_NG_explanation]}

                    sub_dict = {
                        "Result": ["NG"],
                        "Task": [program],
                        "Section": ["AutoRun"],
                        "RungNo": [""],
                        "Target": [detail_dict],
                        "CheckItem": rule_4_1_check_item,
                        "Detail": [check_1_chuck_NG_explanation],
                        "Status": [""],
                    }

                    sub_df = pd.DataFrame(sub_dict)

                    output_df = pd.concat([output_df, sub_df], ignore_index=True)

                # Performing Check2 for the each of the ladder sections

                for ladder, section in zip(check_2_ladder_list, check_2_section_list):

                    logger.info(f"Rule 4.1 Executing Rule 4.2 in {section}")

                    if section == "Individual":
                        NG_EXP = check_2_individual_NG_explanation
                    elif section == "MemoryFeeding":
                        NG_EXP = check_2_memfeeding_NG_explanation
                    elif section == "AutoRun":
                        NG_EXP = check_2_autorun_NG_explanation

                    if len(ladder) != 0:

                        out_coil_df = ladder.filter(
                            pl.col("OBJECT_TYPE_LIST").cast(pl.Utf8).str.to_lowercase()
                            == "coil"
                        )
                        outcoil_rung_list = list(out_coil_df["RUNG"])
                        outcoil_rung_name_list = list(out_coil_df["RUNG_NAME"])
                        outcoil_attributes_list = list(out_coil_df["ATTRIBUTES"])

                        for rung_num, rung_name, coil_attribute in zip(
                            outcoil_rung_list,
                            outcoil_rung_name_list,
                            outcoil_attributes_list,
                        ):
                            coil_attribute = eval(coil_attribute)

                            coil_operand = coil_attribute["operand"]
                            coil_comment_dict = get_the_comment_from_program(
                                variable=coil_operand,
                                program=program,
                                input_comment_data=program_comment_data,
                            )

                            if coil_comment_dict != None:

                                if regex_pattern_check(unchuck_jp, coil_comment_dict):

                                    contact_df = ladder.filter(
                                        (ladder["RUNG"] == rung_num)
                                        & (
                                            pl.col("OBJECT_TYPE_LIST")
                                            .cast(pl.Utf8)
                                            .str.to_lowercase()
                                            == "contact"
                                        )
                                    )

                                    contact_attributes_list = list(
                                        contact_df["ATTRIBUTES"]
                                    )

                                    for attrb in contact_attributes_list:
                                        attrb = eval(attrb)

                                        if attrb["operand"] in list(
                                            unchuck_condition_operands
                                        ):

                                            detail_dict = {}

                                            detail_dict["VARIABLE"] = attrb["operand"]
                                            detail_dict["TARGET_VARIABLE"] = (
                                                coil_operand
                                            )

                                            attrb_operand_index = (
                                                unchuck_condition_operands.index(
                                                    attrb["operand"]
                                                )
                                            )
                                            detail_dict["REFERENCE_RUNG"] = (
                                                unchuck_condition_ref_rung_dict[
                                                    unchuck_condition_operands[
                                                        attrb_operand_index
                                                    ]
                                                ]
                                            )

                                            detail_dict["REFERENCE_SECTION"] = "AutoRun"

                                            detail_dict["TARGET_RUNG"] = rung_num

                                            # sub_dict={

                                            #     'TASK_NAME':[program], 'SECTION_NAME':[section],   'RULE_NUMBER': ["4.1"],
                                            #     'CHECK_NUMBER':["2"], 'RULE_CONTENT':["UNCHUCK OUTCOIL ASSOCIATED WITH 'A' CONTACT"],
                                            #     'STATUS': ['OK'], 'DETAILS': [detail_dict],
                                            #     'NG_EXPLANATION':['NONE']

                                            #     }

                                            # sub_dict = {'TASK_NAME':[program], 'SECTION_NAME':[section], 'RULE_NUMBER': ["4.1"],
                                            #         'CHECK_NUMBER':["2"],  "RUNG_NUMBER":[rung_num], 'RULE_CONTENT':[rule_contenta_4_1],
                                            #         'CHECK_CONTENT':rule_4_1_check_content_2,
                                            #         'TARGET_OUTCOIL': [detail_dict],  'STATUS': ['OK'],
                                            #         'NG_EXPLANATION':["NONE"]}

                                            sub_dict = {
                                                "Result": ["OK"],
                                                "Task": [program],
                                                "Section": [section],
                                                "RungNo": [rung_num],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_4_1_check_item,
                                                "Detail": ["NONE"],
                                                "Status": [""],
                                            }

                                            sub_df = pd.DataFrame(sub_dict)

                                            output_df = pd.concat(
                                                [output_df, sub_df], ignore_index=True
                                            )

                                        else:

                                            detail_dict = {}

                                            detail_dict["VARIABLE"] = attrb["operand"]
                                            detail_dict["TARGET_VARIABLE"] = "NONE"

                                            detail_dict["REFERENCE_RUNG"] = "NONE"
                                            detail_dict["REFERENCE_SECTION"] = "AutoRun"
                                            detail_dict["TARGET_RUNG"] = rung_num

                                            # sub_dict={

                                            #     'TASK_NAME':[program], 'SECTION_NAME':[section],   'RULE_NUMBER': ["4.1"],
                                            #     'CHECK_NUMBER':["2"], 'RULE_CONTENT':["UNCHUCK OUTCOIL NOT ASSOCIATED WITH 'A' CONTACT"],
                                            #     'STATUS': ['NG'], 'DETAILS': [detail_dict],
                                            #     'NG_EXPLANATION':[NG_EXP]

                                            #     }

                                            # sub_dict = {'TASK_NAME':[program], 'SECTION_NAME':[section], 'RULE_NUMBER': ["4.1"],
                                            #         'CHECK_NUMBER':["2"],  "RUNG_NUMBER":[rung_num], 'RULE_CONTENT':[rule_contenta_4_1],
                                            #         'CHECK_CONTENT':rule_4_1_check_content_2,
                                            #         'TARGET_OUTCOIL': [detail_dict],  'STATUS': ['NG'],
                                            #         'NG_EXPLANATION':[NG_EXP]}

                                            sub_dict = {
                                                "Result": ["NG"],
                                                "Task": [program],
                                                "Section": [section],
                                                "RungNo": [rung_num],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_4_1_check_item,
                                                "Detail": [NG_EXP],
                                                "Status": [""],
                                            }

                                            sub_df = pd.DataFrame(sub_dict)

                                            output_df = pd.concat(
                                                [output_df, sub_df], ignore_index=True
                                            )

                                elif regex_pattern_check(chuck_jp, coil_comment_dict):

                                    contact_df = ladder.filter(
                                        (ladder["RUNG"] == rung_num)
                                        & (
                                            pl.col("OBJECT_TYPE_LIST")
                                            .cast(pl.Utf8)
                                            .str.to_lowercase()
                                            == "contact"
                                        )
                                    )

                                    contact_attributes_list = list(
                                        contact_df["ATTRIBUTES"]
                                    )

                                    for attrb in contact_attributes_list:
                                        attrb = eval(attrb)

                                        if attrb["operand"] in list(
                                            chuck_condition_operands
                                        ):

                                            detail_dict = {}

                                            detail_dict["VARIABLE"] = attrb["operand"]
                                            detail_dict["TARGET_VARIABLE"] = (
                                                coil_operand
                                            )

                                            attrb_operand_index = (
                                                chuck_condition_operands.index(
                                                    attrb["operand"]
                                                )
                                            )
                                            detail_dict["REFERENCE_RUNG"] = (
                                                chuck_condition_ref_rung_dict[
                                                    chuck_condition_operands[
                                                        attrb_operand_index
                                                    ]
                                                ]
                                            )
                                            detail_dict["REFERENCE_SECTION"] = "AutoRun"

                                            detail_dict["TARGET_RUNG"] = rung_num

                                            # sub_dict={

                                            #     'TASK_NAME':[program], 'SECTION_NAME':[section],   'RULE_NUMBER': ["4.1"],
                                            #     'CHECK_NUMBER':["2"], 'RULE_CONTENT':["CHUCK OUTCOIL  ASSOCIATED WITH 'A' CONTACT"],
                                            #     'STATUS': ['OK'], 'DETAILS': [detail_dict],
                                            #     'NG_EXPLANATION':["NONE"]}

                                            # sub_dict = {'TASK_NAME':[program], 'SECTION_NAME':[section], 'RULE_NUMBER': ["4.1"],
                                            #         'CHECK_NUMBER':["2"],  "RUNG_NUMBER":[rung_num], 'RULE_CONTENT':[rule_contenta_4_1],
                                            #         'CHECK_CONTENT':rule_4_1_check_content_2,
                                            #         'TARGET_OUTCOIL': [detail_dict],  'STATUS': ['OK'],
                                            #         'NG_EXPLANATION':["NONE"]}

                                            sub_dict = {
                                                "Result": ["OK"],
                                                "Task": [program],
                                                "Section": [section],
                                                "RungNo": [rung_num],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_4_1_check_item,
                                                "Detail": ["NONE"],
                                                "Status": [""],
                                            }

                                            sub_df = pd.DataFrame(sub_dict)

                                            output_df = pd.concat(
                                                [output_df, sub_df], ignore_index=True
                                            )

                                        else:

                                            detail_dict = {}

                                            detail_dict["VARIABLE"] = attrb["operand"]
                                            detail_dict["TARGET_VARIABLE"] = "NONE"

                                            detail_dict["REFERENCE_RUNG"] = "NONE"
                                            detail_dict["REFERENCE_SECTION"] = "AutoRun"

                                            detail_dict["TARGET_RUNG"] = rung_num

                                            # sub_dict={

                                            #     'TASK_NAME':[program], 'SECTION_NAME':[section],   'RULE_NUMBER': ["4.1"],
                                            #     'CHECK_NUMBER':["2"], 'RULE_CONTENT':["CHUCK OUTCOIL NOT ASSOCIATED WITH 'A' CONTACT"],
                                            #     'STATUS': ['NG'], 'DETAILS': [detail_dict],
                                            #     'NG_EXPLANATION':[NG_EXP]
                                            #     }

                                            # sub_dict = {'TASK_NAME':[program], 'SECTION_NAME':[section], 'RULE_NUMBER': ["4.1"],
                                            #         'CHECK_NUMBER':["2"],  "RUNG_NUMBER":[rung_num], 'RULE_CONTENT':[rule_contenta_4_1],
                                            #         'CHECK_CONTENT':rule_4_1_check_content_2,
                                            #         'TARGET_OUTCOIL': [detail_dict],  'STATUS': ['NG'],
                                            #         'NG_EXPLANATION':[NG_EXP]}

                                            sub_dict = {
                                                "Result": ["OK"],
                                                "Task": [program],
                                                "Section": [section],
                                                "RungNo": [rung_num],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_4_1_check_item,
                                                "Detail": [NG_EXP],
                                                "Status": [""],
                                            }

                                            sub_df = pd.DataFrame(sub_dict)

                                            output_df = pd.concat(
                                                [output_df, sub_df], ignore_index=True
                                            )

        return {"status": "OK", "output_df": output_df}

    except Exception as e:
        logger.error(str(e))
        return {"status": "NOT OK", "error": str(e)}


######################################################################################3


def execute_rule_4_1_functionwise(
    input_file: str, input_function_comment_file: str, input_image: str = None
) -> None:

    logger.info("Executing Rule 4.1")

    try:

        # Global Variables
        program_of_interest_pattern = r"P111_"

        confirmation_jp = r"確認"
        condition_jp = r"条件"

        chuck_jp = r"ﾁｬｯｸ"
        unchuck_jp = r"ｱﾝﾁｬｯｸ"

        chuck_confirmation_operands = []
        unchuck_confirmation_operands = []

        chuck_condition_operands = []
        unchuck_condition_operands = []

        chuck_operands = []
        unchuck_operands = []

        # output_dict={'TASK_NAME':[], 'SECTION_NAME':[],   'RULE_NUMBER': [], 'CHECK_NUMBER':[], 'RULE_CONTENT':[], 'STATUS': [], 'DETAILS': [], 'NG_EXPLANATION':[]}
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

        check_1_chuck_NG_explanation = "チャック確認中：チャック確認回路が検出できなかった(Chuck checking: Chuck checking circuit could not could not be detected.)"
        check_1_unchuck_NG_explanation = "アンチャック確認中：アンチャック確認回路が検出できなかった(Unchuck checking: Unchuck checking circuit could not be detected.)"

        check_2_individual_NG_explanation = """Individual”セクション内でチャックの起動条件に搬送と同じ接点が使用されていないため、同期していない可能性有
                    (In the 'Individual' section, the chuck move start condition do not use the same conditions as the 'AutoRun' section, so they may not be synchronized.)"""

        check_2_memfeeding_NG_explanation = """Memory Feeding セクション内でチャック開始記憶の条件に搬送と同じ接点が使用されていないため、同期していない可能性有
                    (In the 'Memory Feeding' section, the chuck start memory condition do not use the same conditions as the 'AutoRun' section, so they may not be synchronized.)"""

        check_2_autorun_NG_explanation = """NONE"""

        ##################################################

        ladder_df = pl.read_csv(input_file)
        input_image_function_df = pl.read_csv(input_image)

        task_names = (
            input_image_function_df.filter(
                pl.col("Unit").cast(pl.Utf8).str.to_lowercase() == "p&p"
            )
            .select(pl.col("Task name").cast(pl.Utf8).str.to_lowercase())
            .to_series()
            .to_list()
        )

        with open(input_function_comment_file, "r", encoding="utf-8") as file:
            function_comment_data = json.load(file)

        unique_function_values = ladder_df["FUNCTION_BLOCK"].unique()

        for function_ in unique_function_values:

            logger.info(f"Rule 4.1 Executing Rule 4.1 in {function_}")

            # Only if the Program is P111_
            if function_.lower() in task_names:

                ladder_pgm_wise = ladder_df.filter(
                    ladder_df["FUNCTION_BLOCK"] == function_
                )

                # Get the section name in each of the PROGRAM
                unique_section_values = ladder_pgm_wise["BODY_TYPE"].unique()

                ladder_autorun = ladder_pgm_wise.filter(
                    ladder_pgm_wise["BODY_TYPE"] == "AutoRun"
                )
                ladder_memory_feeding = ladder_pgm_wise.filter(
                    ladder_pgm_wise["BODY_TYPE"] == "MemoryFeeding"
                )
                ladder_individual = ladder_pgm_wise.filter(
                    ladder_pgm_wise["BODY_TYPE"] == "Individual"
                )

                # Scanning for Chuck and Unhuck condition

                if len(ladder_autorun) != 0:

                    out_coil_df = ladder_autorun.filter(
                        pl.col("OBJECT_TYPE_LIST").cast(pl.Utf8).str.to_lowercase()
                        == "coil"
                    )
                    outcoil_rung_list = list(out_coil_df["RUNG"])
                    outcoil_rung_name_list = list(out_coil_df["RUNG_NAME"])
                    outcoil_attributes_list = list(out_coil_df["ATTRIBUTES"])

                    for rung_num, rung_name, coil_attribute in zip(
                        outcoil_rung_list,
                        outcoil_rung_name_list,
                        outcoil_attributes_list,
                    ):
                        coil_attribute = eval(coil_attribute)

                        coil_operand = coil_attribute["operand"]
                        coil_comment_dict = get_the_comment_from_function(
                            variable=coil_operand,
                            function=function_,
                            input_comment_data=function_comment_data,
                        )

                        if regex_pattern_check(
                            confirmation_jp, coil_comment_dict
                        ) and regex_pattern_check(unchuck_jp, coil_comment_dict):

                            operand_dict = {}
                            operand_dict[coil_operand] = coil_comment_dict
                            unchuck_confirmation_operands.append(coil_operand)

                            detail_dict = {}
                            detail_dict["VARIABLE"] = coil_operand

                            detail_dict["RUNG"] = rung_num

                            # sub_dict={

                            #     'TASK_NAME':[function_], 'SECTION_NAME':['AutoRun'],   'RULE_NUMBER': ["4.1"],
                            #     'CHECK_NUMBER':["1"], 'RULE_CONTENT':["UNCHUCK CONFIRMATION EXISTS"],
                            #     'STATUS': ['OK'], 'DETAILS': [detail_dict],
                            #     'NG_EXPLANATION':['NONE']

                            # }

                            # sub_dict = {'TASK_NAME':[function_], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["4.1"],
                            #                         'CHECK_NUMBER':["1"],  "RUNG_NUMBER":[rung_num], 'RULE_CONTENT':[rule_contenta_4_1],
                            #                         'CHECK_CONTENT':rule_4_1_check_content_1,
                            #                         'TARGET_OUTCOIL': [detail_dict],  'STATUS': ['OK'],
                            #                         'NG_EXPLANATION':['NONE']}

                            sub_dict = {
                                "Result": ["OK"],
                                "Task": [function_],
                                "Section": ["AutoRun"],
                                "RungNo": [rung_num],
                                "Target": [detail_dict],
                                "CheckItem": rule_4_1_check_item,
                                "Detail": ["NONE"],
                                "Status": [""],
                            }

                            sub_df = pd.DataFrame(sub_dict)

                            output_df = pd.concat(
                                [output_df, sub_df], ignore_index=True
                            )

                        elif regex_pattern_check(
                            confirmation_jp, coil_comment_dict
                        ) and regex_pattern_check(chuck_jp, coil_comment_dict):

                            operand_dict = {}
                            operand_dict[coil_operand] = coil_comment_dict
                            chuck_confirmation_operands.append(coil_operand)

                            detail_dict = {}
                            detail_dict["VARIABLE"] = coil_operand

                            detail_dict["RUNG"] = rung_num

                            # sub_dict={

                            #     'TASK_NAME':[function_], 'SECTION_NAME':['AutoRun'],   'RULE_NUMBER': ["4.1"],
                            #     'CHECK_NUMBER':["1"], 'RULE_CONTENT':["CHUCK CONFIRMATION EXISTS"],
                            #     'STATUS': ['OK'], 'DETAILS': [detail_dict],
                            #     'NG_EXPLANATION':['NONE']

                            #     }

                            # sub_dict = {'TASK_NAME':[function_], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["4.1"],
                            #                         'CHECK_NUMBER':["1"],  "RUNG_NUMBER":[rung_num], 'RULE_CONTENT':[rule_contenta_4_1],
                            #                         'CHECK_CONTENT':rule_4_1_check_content_1,
                            #                         'TARGET_OUTCOIL': [detail_dict],  'STATUS': ['OK'],
                            #                         'NG_EXPLANATION':['NONE']}

                            sub_dict = {
                                "Result": ["OK"],
                                "Task": [function_],
                                "Section": ["AutoRun"],
                                "RungNo": [rung_num],
                                "Target": [detail_dict],
                                "CheckItem": rule_4_1_check_item,
                                "Detail": ["NONE"],
                                "Status": [""],
                            }

                            sub_df = pd.DataFrame(sub_dict)

                            output_df = pd.concat(
                                [output_df, sub_df], ignore_index=True
                            )

                        elif regex_pattern_check(
                            condition_jp, coil_comment_dict
                        ) and regex_pattern_check(unchuck_jp, coil_comment_dict):

                            operand_dict = {}
                            unchuck_condition_ref_rung_dict = {}

                            operand_dict[coil_operand] = coil_comment_dict
                            unchuck_condition_operands.append(coil_operand)

                            unchuck_condition_ref_rung_dict[coil_operand] = rung_num

                        elif regex_pattern_check(
                            condition_jp, coil_comment_dict
                        ) and regex_pattern_check(chuck_jp, coil_comment_dict):

                            operand_dict = {}
                            operand_dict[coil_operand] = coil_comment_dict
                            chuck_condition_operands.append(coil_operand)

                            chuck_condition_ref_rung_dict = {}
                            chuck_condition_ref_rung_dict[coil_operand] = rung_num

                check_2_ladder_list = [
                    ladder_memory_feeding,
                    ladder_individual,
                    ladder_autorun,
                ]
                check_2_section_list = ["MemoryFeeding", "Individual", "AutoRun"]

                # Checking for Check 1
                if len(unchuck_confirmation_operands) == 0:

                    detail_dict = {}

                    # sub_dict={

                    #     'TASK_NAME':[function_], 'SECTION_NAME':['AutoRun'],   'RULE_NUMBER': ["4.1"],
                    #     'CHECK_NUMBER':["1"], 'RULE_CONTENT':["UNCHUCK CONFIRMATION  DOESNOT EXISTS"],
                    #     'STATUS': ['NG'], 'DETAILS': [detail_dict],

                    #     'NG_EXPLANATION':[check_1_unchuck_NG_explanation]

                    #     }

                    # sub_dict = {'TASK_NAME':[function_], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["4.1"],
                    #                                 'CHECK_NUMBER':["1"],  "RUNG_NUMBER":[""], 'RULE_CONTENT':[rule_contenta_4_1],
                    #                                 'CHECK_CONTENT':rule_4_1_check_content_1,
                    #                                 'TARGET_OUTCOIL': [detail_dict],  'STATUS': ['NG'],
                    #                                 'NG_EXPLANATION':[check_1_unchuck_NG_explanation]}

                    sub_dict = {
                        "Result": ["NG"],
                        "Task": [function_],
                        "Section": ["AutoRun"],
                        "RungNo": [""],
                        "Target": [detail_dict],
                        "CheckItem": rule_4_1_check_item,
                        "Detail": [check_1_unchuck_NG_explanation],
                        "Status": [""],
                    }

                    sub_df = pd.DataFrame(sub_dict)

                    output_df = pd.concat([output_df, sub_df], ignore_index=True)

                if len(chuck_confirmation_operands) == 0:

                    detail_dict = {}

                    # sub_dict={

                    #     'TASK_NAME':[function_], 'SECTION_NAME':['AutoRun'],   'RULE_NUMBER': ["4.1"],
                    #     'CHECK_NUMBER':["1"], 'RULE_CONTENT':["CHUCK CONFIRMATION  DOESNOT EXISTS"],
                    #     'STATUS': ['NG'], 'DETAILS': [detail_dict],
                    #     'NG_EXPLANATION':[check_1_chuck_NG_explanation]

                    #     }

                    # sub_dict = {'TASK_NAME':[function_], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ["4.1"],
                    #                                 'CHECK_NUMBER':["1"],  "RUNG_NUMBER":[""], 'RULE_CONTENT':[rule_contenta_4_1],
                    #                                 'CHECK_CONTENT':rule_4_1_check_content_1,
                    #                                 'TARGET_OUTCOIL': [detail_dict],  'STATUS': ['NG'],
                    #                                 'NG_EXPLANATION':[check_1_unchuck_NG_explanation]}

                    sub_dict = {
                        "Result": ["NG"],
                        "Task": [function_],
                        "Section": ["AutoRun"],
                        "RungNo": [""],
                        "Target": [detail_dict],
                        "CheckItem": rule_4_1_check_item,
                        "Detail": [check_1_unchuck_NG_explanation],
                        "Status": [""],
                    }

                    sub_df = pd.DataFrame(sub_dict)

                    output_df = pd.concat([output_df, sub_df], ignore_index=True)

                # Performing Check2 for the each of the ladder sections

                for ladder, section in zip(check_2_ladder_list, check_2_section_list):

                    logger.info(f"Rule 4.1 Executing Rule 4.2 in {section}")

                    if section == "Individual":
                        NG_EXP = check_2_individual_NG_explanation
                    elif section == "MemoryFeeding":
                        NG_EXP = check_2_memfeeding_NG_explanation
                    elif section == "AutoRun":
                        NG_EXP = check_2_autorun_NG_explanation

                    if len(ladder) != 0:

                        out_coil_df = ladder.filter(
                            pl.col("OBJECT_TYPE_LIST").cast(pl.Utf8).str.to_lowercase()
                            == "coil"
                        )
                        outcoil_rung_list = list(out_coil_df["RUNG"])
                        outcoil_rung_name_list = list(out_coil_df["RUNG_NAME"])
                        outcoil_attributes_list = list(out_coil_df["ATTRIBUTES"])

                        for rung_num, rung_name, coil_attribute in zip(
                            outcoil_rung_list,
                            outcoil_rung_name_list,
                            outcoil_attributes_list,
                        ):
                            coil_attribute = eval(coil_attribute)

                            coil_operand = coil_attribute["operand"]
                            coil_comment_dict = get_the_comment_from_function(
                                variable=coil_operand,
                                function=function_,
                                input_comment_data=function_comment_data,
                            )

                            if coil_comment_dict != None:

                                if regex_pattern_check(unchuck_jp, coil_comment_dict):

                                    contact_df = ladder.filter(
                                        (ladder["RUNG"] == rung_num)
                                        & (
                                            pl.col("OBJECT_TYPE_LIST")
                                            .cast(pl.Utf8)
                                            .str.to_lowercase()
                                            == "contact"
                                        )
                                    )

                                    contact_attributes_list = list(
                                        contact_df["ATTRIBUTES"]
                                    )

                                    for attrb in contact_attributes_list:
                                        attrb = eval(attrb)

                                        if attrb["operand"] in list(
                                            unchuck_condition_operands
                                        ):

                                            detail_dict = {}

                                            detail_dict["VARIABLE"] = attrb["operand"]
                                            detail_dict["TARGET_VARIABLE"] = (
                                                coil_operand
                                            )

                                            attrb_operand_index = (
                                                unchuck_condition_operands.index(
                                                    attrb["operand"]
                                                )
                                            )
                                            detail_dict["REFERENCE_RUNG"] = (
                                                unchuck_condition_ref_rung_dict[
                                                    unchuck_condition_operands[
                                                        attrb_operand_index
                                                    ]
                                                ]
                                            )

                                            detail_dict["REFERENCE_SECTION"] = "AutoRun"

                                            detail_dict["TARGET_RUNG"] = rung_num

                                            # sub_dict={

                                            #     'TASK_NAME':[function_], 'SECTION_NAME':[section],   'RULE_NUMBER': ["4.1"],
                                            #     'CHECK_NUMBER':["2"], 'RULE_CONTENT':["UNCHUCK OUTCOIL ASSOCIATED WITH 'A' CONTACT"],
                                            #     'STATUS': ['OK'], 'DETAILS': [detail_dict],
                                            #     'NG_EXPLANATION':['NONE']

                                            #     }

                                            # sub_dict = {'TASK_NAME':[function_], 'SECTION_NAME':[section], 'RULE_NUMBER': ["4.1"],
                                            #         'CHECK_NUMBER':["2"],  "RUNG_NUMBER":[rung_num], 'RULE_CONTENT':[rule_contenta_4_1],
                                            #         'CHECK_CONTENT':rule_4_1_check_content_2,
                                            #         'TARGET_OUTCOIL': [detail_dict],  'STATUS': ['OK'],
                                            #         'NG_EXPLANATION':["NONE"]}

                                            sub_dict = {
                                                "Result": ["OK"],
                                                "Task": [function_],
                                                "Section": [section],
                                                "RungNo": [rung_num],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_4_1_check_item,
                                                "Detail": ["NONE"],
                                                "Status": [""],
                                            }

                                            sub_df = pd.DataFrame(sub_dict)

                                            output_df = pd.concat(
                                                [output_df, sub_df], ignore_index=True
                                            )

                                        else:

                                            detail_dict = {}

                                            detail_dict["VARIABLE"] = attrb["operand"]
                                            detail_dict["TARGET_VARIABLE"] = "NONE"

                                            detail_dict["REFERENCE_RUNG"] = "NONE"
                                            detail_dict["REFERENCE_SECTION"] = "AutoRun"
                                            detail_dict["TARGET_RUNG"] = rung_num

                                            # sub_dict={

                                            #     'TASK_NAME':[function_], 'SECTION_NAME':[section],   'RULE_NUMBER': ["4.1"],
                                            #     'CHECK_NUMBER':["2"], 'RULE_CONTENT':["UNCHUCK OUTCOIL NOT ASSOCIATED WITH 'A' CONTACT"],
                                            #     'STATUS': ['NG'], 'DETAILS': [detail_dict],
                                            #     'NG_EXPLANATION':[NG_EXP]

                                            #     }

                                            # sub_dict = {'TASK_NAME':[function_], 'SECTION_NAME':[section], 'RULE_NUMBER': ["4.1"],
                                            #         'CHECK_NUMBER':["2"],  "RUNG_NUMBER":[rung_num], 'RULE_CONTENT':[rule_contenta_4_1],
                                            #         'CHECK_CONTENT':rule_4_1_check_content_2,
                                            #         'TARGET_OUTCOIL': [detail_dict],  'STATUS': ['NG'],
                                            #         'NG_EXPLANATION':[NG_EXP]}

                                            sub_dict = {
                                                "Result": ["NG"],
                                                "Task": [function_],
                                                "Section": [section],
                                                "RungNo": [rung_num],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_4_1_check_item,
                                                "Detail": [NG_EXP],
                                                "Status": [""],
                                            }

                                            sub_df = pd.DataFrame(sub_dict)

                                            output_df = pd.concat(
                                                [output_df, sub_df], ignore_index=True
                                            )

                                elif regex_pattern_check(chuck_jp, coil_comment_dict):

                                    contact_df = ladder.filter(
                                        (ladder["RUNG"] == rung_num)
                                        & (
                                            pl.col("OBJECT_TYPE_LIST")
                                            .cast(pl.Utf8)
                                            .str.to_lowercase()
                                            == "contact"
                                        )
                                    )

                                    contact_attributes_list = list(
                                        contact_df["ATTRIBUTES"]
                                    )

                                    for attrb in contact_attributes_list:
                                        attrb = eval(attrb)

                                        if attrb["operand"] in list(
                                            chuck_condition_operands
                                        ):

                                            detail_dict = {}

                                            detail_dict["VARIABLE"] = attrb["operand"]
                                            detail_dict["TARGET_VARIABLE"] = (
                                                coil_operand
                                            )

                                            attrb_operand_index = (
                                                chuck_condition_operands.index(
                                                    attrb["operand"]
                                                )
                                            )
                                            detail_dict["REFERENCE_RUNG"] = (
                                                chuck_condition_ref_rung_dict[
                                                    chuck_condition_operands[
                                                        attrb_operand_index
                                                    ]
                                                ]
                                            )
                                            detail_dict["REFERENCE_SECTION"] = "AutoRun"

                                            detail_dict["TARGET_RUNG"] = rung_num

                                            # sub_dict={

                                            #     'TASK_NAME':[function_], 'SECTION_NAME':[section],   'RULE_NUMBER': ["4.1"],
                                            #     'CHECK_NUMBER':["2"], 'RULE_CONTENT':["CHUCK OUTCOIL  ASSOCIATED WITH 'A' CONTACT"],
                                            #     'STATUS': ['OK'], 'DETAILS': [detail_dict],
                                            #     'NG_EXPLANATION':["NONE"]}

                                            # sub_dict = {'TASK_NAME':[function_], 'SECTION_NAME':[section], 'RULE_NUMBER': ["4.1"],
                                            #         'CHECK_NUMBER':["2"],  "RUNG_NUMBER":[rung_num], 'RULE_CONTENT':[rule_contenta_4_1],
                                            #         'CHECK_CONTENT':rule_4_1_check_content_2,
                                            #         'TARGET_OUTCOIL': [detail_dict],  'STATUS': ['OK'],
                                            #         'NG_EXPLANATION':["NONE"]}

                                            sub_dict = {
                                                "Result": ["OK"],
                                                "Task": [function_],
                                                "Section": [section],
                                                "RungNo": [rung_num],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_4_1_check_item,
                                                "Detail": ["NONE"],
                                                "Status": [""],
                                            }

                                            sub_df = pd.DataFrame(sub_dict)

                                            output_df = pd.concat(
                                                [output_df, sub_df], ignore_index=True
                                            )

                                        else:

                                            detail_dict = {}

                                            detail_dict["VARIABLE"] = attrb["operand"]
                                            detail_dict["TARGET_VARIABLE"] = "NONE"

                                            detail_dict["REFERENCE_RUNG"] = "NONE"
                                            detail_dict["REFERENCE_SECTION"] = "AutoRun"

                                            detail_dict["TARGET_RUNG"] = rung_num

                                            # sub_dict={

                                            #     'TASK_NAME':[function_], 'SECTION_NAME':[section],   'RULE_NUMBER': ["4.1"],
                                            #     'CHECK_NUMBER':["2"], 'RULE_CONTENT':["CHUCK OUTCOIL NOT ASSOCIATED WITH 'A' CONTACT"],
                                            #     'STATUS': ['NG'], 'DETAILS': [detail_dict],
                                            #     'NG_EXPLANATION':[NG_EXP]
                                            #     }

                                            # sub_dict = {'TASK_NAME':[function_], 'SECTION_NAME':[section], 'RULE_NUMBER': ["4.1"],
                                            #         'CHECK_NUMBER':["2"],  "RUNG_NUMBER":[rung_num], 'RULE_CONTENT':[rule_contenta_4_1],
                                            #         'CHECK_CONTENT':rule_4_1_check_content_2,
                                            #         'TARGET_OUTCOIL': [detail_dict],  'STATUS': ['NG'],
                                            #         'NG_EXPLANATION':[NG_EXP]}

                                            sub_dict = {
                                                "Result": ["NG"],
                                                "Task": [function_],
                                                "Section": [section],
                                                "RungNo": [rung_num],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_4_1_check_item,
                                                "Detail": [NG_EXP],
                                                "Status": [""],
                                            }

                                            sub_df = pd.DataFrame(sub_dict)

                                            output_df = pd.concat(
                                                [output_df, sub_df], ignore_index=True
                                            )
        return {"status": "OK", "output_df": output_df}

    except Exception as e:
        logger.error(str(e))
        return {"status": "NOT OK", "error": str(e)}


###########################################################################################


# if __name__ == "__main__":

#     input_file=r"Rule_41_programwise.csv"
#     input_program_comment_file=r"Rule4_1_programwise.json"
#     execute_rule_4_1_programwise(input_file=input_file, input_program_comment_file=input_program_comment_file,output_file_name="Rule_41_program.csv")


#     input_file=r"Rule_41_functionwise.csv"
#     input_function_comment_file=r"Rule4_1_functionwise.json"
#     execute_rule_4_1_functionwise(input_file=input_file, input_function_comment_file=input_function_comment_file,output_file_name="Rule_41_function.csv")
