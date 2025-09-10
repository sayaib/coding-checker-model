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
from .rule_2_ladder_utils import (
    get_block_connections,
    check_self_holding,
    get_in_parallel_A_contacts,
    get_parallel_contacts,
)
from .ladder_utils import regex_pattern_check, clean_rung_number

##################################################################################################

rule_content_2 = "The judgment program requires the judgment data and the judgment signal to be cleared every cycle before starting the judgment."
rule_2_check_item = "The judgment program requires the judgment data and the judgment signal to be cleared every cycle before starting the judgment."
########### Extracting set , reset, outcoils and respective contacts in memory feeding section ################3


def process_memory_feeding(
    ladder_df: pd.DataFrame, program_key: str, body_type_key: str
) -> Dict[str, Any]:

    # ladder_df = pl.read_csv(input_file)
    unique_program_values = ladder_df[program_key].unique()

    outcoil_config_dict = {}
    for program in unique_program_values:

        ladder_program = ladder_df.filter(ladder_df[program_key] == program)

        ladder_memory_feeding = ladder_program.filter(
            pl.col(body_type_key).cast(pl.Utf8).str.to_lowercase() == "memoryfeeding"
        )

        # Get the outcoils
        out_coil_df = ladder_memory_feeding.filter(
            pl.col("OBJECT_TYPE_LIST").cast(pl.Utf8).str.to_lowercase() == "coil"
        )
        outcoil_rung_list = list(out_coil_df["RUNG"].unique())

        # Get the contacts
        contact_df = ladder_memory_feeding.filter(
            pl.col("OBJECT_TYPE_LIST").cast(pl.Utf8).str.to_lowercase() == "contact"
        )

        # Look in to each  rung (outcoils only)
        for outcoil_rung in outcoil_rung_list:

            out_coil_rung_df = out_coil_df.filter(out_coil_df["RUNG"] == outcoil_rung)

            outcoil_rung_attributes_list = list(out_coil_rung_df["ATTRIBUTES"])

            # Extract all the properties of outcoil
            for attr_ in outcoil_rung_attributes_list:

                coil_attr = eval(attr_)
                coil_in_list = coil_attr.get("in_list", "NONE")
                coil_operand = coil_attr.get("operand", "NONE")
                coil_attr_latch = coil_attr.get("latch", "NONE")

                contact_rung_df = contact_df.filter(contact_df["RUNG"] == outcoil_rung)

                contact_rung_attributes_list = list(contact_rung_df["ATTRIBUTES"])

                # In the rung, find out the contacts and their properties
                for contact_attr in contact_rung_attributes_list:

                    contact_attr = eval(contact_attr)

                    contact_out_list = contact_attr.get("out_list", "NONE")
                    contact_operand = contact_attr.get("operand", "NONE")
                    contact_negated_status = contact_attr.get("negated", "NONE")

                    # Only coil with a definite latch status and 'A' contacts to be filtered
                    if (
                        (coil_attr_latch != "NONE")
                        and (contact_negated_status != "NONE")
                        and (contact_negated_status == "false")
                    ):

                        # If the coil and contact are connected then not the contact
                        if set(coil_in_list) & set(contact_out_list):

                            outcoil_config_dict[
                                f"{program}@{outcoil_rung}@{coil_operand}"
                            ] = []
                            outcoil_config_dict[
                                f"{program}@{outcoil_rung}@{coil_operand}"
                            ].append(contact_attr)
                            outcoil_config_dict[
                                f"{program}@{outcoil_rung}@{coil_operand}"
                            ].append(coil_attr)

    return outcoil_config_dict


############################### 'B' contacts in Condition #########################


def condtion_section_B_contacts(
    ladder_df: pd.DataFrame, program_key: str, body_type_key: str
) -> Dict[str, Any]:

    unique_program_values = ladder_df[program_key].unique()

    B_contact_dict = {}
    for program in unique_program_values:

        ladder_program = ladder_df.filter(ladder_df[program_key] == program)

        ladder_condition = ladder_program.filter(
            pl.col(body_type_key).cast(pl.Utf8).str.to_lowercase() == "condition"
        )

        contact_df = ladder_condition.filter(
            pl.col("OBJECT_TYPE_LIST").cast(pl.Utf8).str.to_lowercase() == "contact"
        )
        contact_rung_list = list(contact_df["RUNG"].unique())

        for rung in contact_rung_list:

            contact_rung_df = contact_df.filter(contact_df["RUNG"] == rung)

            contact_attributes = list(contact_rung_df["ATTRIBUTES"])

            for attr_ in contact_attributes:
                contact_attr = eval(attr_)
                contact_negated_status = contact_attr.get("negated", "NONE")
                contact_operand = contact_attr.get("operand", "NONE")

                if (contact_negated_status != "NONE") and (
                    contact_negated_status == "true"
                ):

                    B_contact_dict[f"{program}@{rung}@{contact_operand}"] = []

                    B_contact_dict[f"{program}@{rung}@{contact_operand}"].append(
                        contact_attr
                    )

    return B_contact_dict


######################## create Dataframes ####################
def create_dataframes(
    task_name: str,
    section_name: str,
    rule_number: str,
    check_number: str,
    rule_content: str,
    status: str,
    details: Dict,
    NG_exp_en: str,
    NG_exp_jp: str,
) -> List:

    # output_dict={'TASK_NAME':[], 'SECTION_NAME':[], 'RULE_NUMBER': [], 'CHECK_NUMBER':[], 'RULE_CONTENT':[], 'STATUS': [], 'DETAILS': [], 'NG_EXPLANATION':[]}
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
    output_df_jp = pd.DataFrame(output_dict)

    # sub_df_en=pd.DataFrame({'TASK_NAME':[task_name], 'SECTION_NAME':[section_name], 'RULE_NUMBER': [rule_number],
    #                         'CHECK_NUMBER':[check_number], "RUNG_NUMBER":[details.get('rung', '')],'RULE_CONTENT':[rule_content],
    #                         'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [details],
    #                         'STATUS': [status], 'NG_EXPLANATION':[NG_exp_en]})

    sub_df_en = pd.DataFrame(
        {
            "Result": [status],
            "Task": [task_name],
            "Section": [section_name],
            "RungNo": [details.get("rung", "")],
            "Target": [details],
            "CheckItem": rule_2_check_item,
            "Detail": [NG_exp_en],
            "Status": [""],
        }
    )

    output_df = pd.concat([output_df, sub_df_en], ignore_index=True)

    # sub_df_jp=pd.DataFrame({'TASK_NAME':[task_name], 'SECTION_NAME':[section_name], 'RULE_NUMBER': [rule_number],
    #                         'CHECK_NUMBER':[check_number], "RUNG_NUMBER":[details.get('rung', '')],'RULE_CONTENT':[rule_content],
    #                         'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [details],
    #                         'STATUS': [status], 'NG_EXPLANATION':[NG_exp_jp]})

    sub_df_jp = pd.DataFrame(
        {
            "Result": [status],
            "Task": [task_name],
            "Section": [section_name],
            "RungNo": [details.get("rung", "")],
            "Target": [details],
            "CheckItem": rule_2_check_item,
            "Detail": [NG_exp_jp],
            "Status": [""],
        }
    )

    output_df_jp = pd.concat([output_df_jp, sub_df_jp], ignore_index=True)

    return [output_df, output_df_jp]


################################################## Rule 2 chcek Program #########################
def Rule_2_Check_2_program(
    ladder_df: pd.DataFrame, main_ladder: pd.DataFrame, ref_rung: int
):
    logger.debug(f"Checking for Condition 2 Rule 2 {ref_rung}")
    gsb_regex = r"GSB"
    ld_regex = r"LD"

    class BreakAll(Exception):
        pass

    output_dict = {}

    # First get the all the contacts, excluding GSB
    ladder_contacts = ladder_df.filter(
        pl.col("OBJECT_TYPE_LIST").cast(pl.Utf8).str.to_lowercase() == "contact"
    )
    ladder_contacts_attributes = list(ladder_contacts["ATTRIBUTES"])
    contacts_list = []

    for attr_ in ladder_contacts_attributes:
        attr_ = eval(attr_)

        attr_operand = attr_["operand"]
        if re.search(gsb_regex, attr_operand):
            pass
        else:
            contacts_list.append(attr_operand)

    # Next extract all the blocks, AS of now We are only taking in to consideration EN with =, <, >, <= and >= blocks
    blocks = get_block_connections(ladder_df)

    type_list = ["=", "<", ">", "<=", ">=", "<>"]
    LD_flag = 0

    try:

        for block in blocks:
            block_key = list(block.keys())[0]

            if block_key in type_list:

                block_values = block[block_key]

                for block_val in block_values:

                    block_LD = block_val.get("In1", "NONE")

                    if block_LD != "NONE":
                        block_LD = block_LD[0]
                        block_type = block_val
                        LD_flag = 1
                        raise BreakAll

    except Exception as e:
        logger.info(
            f"Breaking early, Variable:{block_LD}, type: {block_key}  obtained "
        )

    # If LD input is not found, the flag NG and return the output
    if LD_flag == 0:
        output_dict["status"] = "NG"
        output_dict["NG_explanantion"] = "LD type input not found"
        output_dict["check_number"] = "5.3"
        output_dict["ref_rung"] = f"{ref_rung}"

        logger.warning(f"LD type input not found, hence NG")
        return output_dict

    # For block_LD, check in the previous rungs wheter it is associated with Move or not

    move_block_ld_flag = 0
    move_ld_rung = ""
    move_ld_inp = ""
    if re.search(ld_regex, block_LD):

        main_ladder = main_ladder.sort("RUNG", descending=True)

        try:

            for prev_rung in range(ref_rung - 1, 0, -1):

                rung_df = main_ladder.filter(pl.col("RUNG") == prev_rung)
                rung_blocks = get_block_connections(rung_df)

                for block in rung_blocks:

                    rung_block_key = list(block.keys())[0]

                    if any([rung_block_key == "MOVE", rung_block_key == "@MOVE"]):

                        block_values = block[rung_block_key]

                        move_out_flag = 0
                        move_in_flag = 0
                        move_out_val = ""
                        move_in_val = ""

                        for block_val in block_values:

                            move_out = block_val.get("Out", ["NONE"])
                            if move_out[0] != "NONE":
                                move_out_val = move_out[0]
                                move_out_flag = 1

                            move_in = block_val.get("In", ["NONE"])
                            if move_in[0] != "NONE":
                                move_in_val = move_in[0]
                                move_in_flag = 1

                            if move_out_flag and move_in_flag:

                                if move_out_val == block_LD:
                                    move_block_ld_flag = 1
                                    move_ld_rung = prev_rung
                                    move_ld_inp = move_in_val

                                    raise BreakAll

        except Exception as e:

            logger.info(
                f"Breaking early, LD type is found in Rung: {move_ld_rung}, Input:{move_ld_inp} obtained "
            )

        if move_block_ld_flag == 0:
            output_dict["status"] = "NG"
            output_dict["NG_explanantion"] = (
                f"LD type input {block_LD} was not found in output of MOVE block"
            )
            output_dict["check_number"] = "5.4"
            output_dict["ref_rung"] = f"{ref_rung}"

            logger.warning(
                f"LD type input {block_LD} was not found in output of MOVE block"
            )
            return output_dict

        # Check for whether move_ld_inp is set to Int#0
        move_ld_rst_flag = 0
        if move_ld_rung != "":

            try:

                for prev_rung in range(move_ld_rung - 1, 0, -1):

                    rung_df = main_ladder.filter(pl.col("RUNG") == prev_rung)
                    rung_blocks = get_block_connections(rung_df)

                    for block in rung_blocks:
                        rung_block_key = list(block.keys())[0]

                        if rung_block_key in type_list:

                            # Verrify if you are looking into same En =* type
                            if rung_block_key == block_key:

                                block_values = block[rung_block_key]
                                In_2_val = ""
                                In_2_flag = 0
                                In_1_val = ""
                                In_1_flag = 0

                                for block_val in block_values:

                                    In_2 = block_val.get("In2", ["NONE"])
                                    if In_2[0] != "NONE":
                                        In_2_val = In_2[0]
                                        In_2_flag = 1

                                    In_1 = block_val.get("In1", ["NONE"])
                                    if In_1[0] != "NONE":
                                        In_1_val = In_1[0]
                                        In_1_flag = 1

                                    # Verify that tehe inputs arre not none and Input is same as move_ld_inp extracted before
                                    if all(
                                        [
                                            In_2_flag == 1,
                                            In_1_flag == 1,
                                            In_1_val == move_ld_inp,
                                            In_2_val == "INT#0",
                                        ]
                                    ):

                                        move_ld_rst_flag = 1
                                        move_ld_rst_rung = prev_rung
                                        raise BreakAll

            except Exception as e:

                logger.info(
                    f"Breaking early, Variable :{move_ld_inp} set to zero Rung:{move_ld_rst_rung} obtained "
                )

        if move_ld_rst_flag == 0:
            output_dict["status"] = "NG"
            output_dict["NG_explanantion"] = (
                f"Variable :{move_ld_inp} was not set to Zero"
            )
            output_dict["check_number"] = "5.5"
            output_dict["ref_rung"] = f"{ref_rung}"

            logger.warning(f"Variable :{move_ld_inp} was not set to Zero")
            return output_dict

        ### Now check the move_ld_rst_rung, whether contacts in contacts_list exist as B contacts or not
        # Later well will check whetehr they are series or not
        # move_ld_rst_rung=6
        series_B_contacts_flag = 0
        series_B_contacts = []
        series_B_attributes = []
        if move_ld_rst_flag == 1:

            series_rung_df = main_ladder.filter(pl.col("RUNG") == move_ld_rst_rung)

            series_rung_contacts = series_rung_df.filter(
                pl.col("OBJECT_TYPE_LIST").cast(pl.Utf8).str.to_lowercase() == "contact"
            )

            contact_attributes = list(series_rung_contacts["ATTRIBUTES"])

            for attr_ in contact_attributes:
                attr_ = eval(attr_)

                attr_operand = attr_["operand"]
                attr_operand_negated_status = attr_.get("negated", "NONE")

                if all(
                    [
                        attr_operand in contacts_list,
                        attr_operand_negated_status != "NONE",
                        attr_operand_negated_status == "true",
                    ]
                ):

                    series_B_contacts.append(attr_operand)

                    # Store the Series B attributes, for future processing
                    series_B_attributes.append(attr_)

            # Verify if all contacts are captured or not
            if set(contacts_list) == set(series_B_contacts):
                series_B_contacts_flag = 1
            else:
                logger.warning("Contacts List and Series B contacts do not match")

        if series_B_contacts_flag == 0:
            output_dict["status"] = "NG"
            output_dict["NG_explanantion"] = (
                f"All the Contacts were not associated with B counterparts"
            )
            output_dict["check_number"] = "5.6"
            output_dict["ref_rung"] = f"{ref_rung}"

            logger.warning(f"All the Contacts were not associated with B counterparts")
            return output_dict

        if series_B_contacts_flag == 1:

            output_dict["status"] = "OK"
            output_dict["target_rung"] = move_ld_rst_rung
            output_dict["MOVE_Inp"] = move_ld_inp
            output_dict["ref_rung"] = f"{ref_rung}"
            output_dict["check_number"] = "5"
            output_dict["NG_explanantion"] = "NONE"

            logger.info(
                f"ll the Contacts were associated with B counterparts in target rung:{move_ld_rst_rung}"
            )
            return output_dict


#######################3 Function for execution ##################################


def execute_rule_2(
    input_file: str,
    input_program_comment_file: str,
    program_key: str,
    body_type_key: str,
) -> pd.DataFrame:

    print("in execute_rule_2")
    logger.warning(
        "======================================Executing Rule 2==========================="
    )

    class BreakInner(Exception):
        pass

    cycle_half_jp = r"ｻｲｸﾙ"
    cycle_full_jp = r"サイクル"

    start_jp = r"開始"
    end_jp = r"終了"

    normal_jp = r"正常"
    abnormal_jp = r"異常"

    ok_jp = r"OK"
    ng_jp = r"NG"

    rule_content_condition_1_en = ""
    rule_content_condition_1_jp = ""

    NG_exp_missing_contacts_en = "No Judgment Memory Exists"
    NG_exp_missing_contacts_jp = "判定記憶なし"

    NG_exp_set_en = "No Judgment Memory Exists"
    NG_exp_set_jp = "判定記憶なし"

    NG_exp_set_reset_en = "No circuit to clear Judgment Memory"
    NG_exp_set_reset_jp = "判定記憶をクリアする回路がない"

    NG_exp_condition_en = (
        "Failure to perform OFF check of judgment memory before judgment"
    )
    NG_exp_condition_jp = "判定記憶のOFFチェックを判定前に実施できていない"

    NG_exp_condition_2_5_3_en = "Condition of judgment OK is not yet satisfied"
    NG_exp_condition_2_5_3_jp = ":判定OKの条件が未成立となっている("

    NG_exp_condition_2_5_3_en = "Condition of judgment OK is not yet satisfied"
    NG_exp_condition_2_5_3_jp = ":判定OKの条件が未成立となっている("

    output_dict = {
        "TASK_NAME": [],
        "SECTION_NAME": [],
        "RULE_NUMBER": [],
        "CHECK_NUMBER": [],
        "RUNG_NUMBER": [],
        "RULE_CONTENT": [],
        "CHECK_CONTENT": [],
        "TARGET_OUTCOIL": [],
        "STATUS": [],
        "NG_EXPLANATION": [],
    }

    # output_dict={'TASK_NAME':[], 'SECTION_NAME':[], 'RULE_NUMBER': [], 'CHECK_NUMBER':[], 'RULE_CONTENT':[], 'STATUS': [], 'DETAILS': [], 'NG_EXPLANATION':[]}
    output_df = pd.DataFrame(output_dict)
    output_df_jp = pd.DataFrame(output_dict)

    with open(input_program_comment_file, "r", encoding="utf-8") as file:
        comment_data = json.load(file)

    ##########################Range Detection#################################3
    try:

        ladder_df = pl.read_csv(input_file)
        ladder_pd_df = pd.read_csv(input_file)
        unique_program_values = ladder_df[program_key].unique()
        program_range_dict = {}

        for program in unique_program_values:
            logger.info(f"Detecting Ranges in {program}")
            rung_range_dict = {}
            start_list = []
            end_list = []

            # In each program select only the Autorun Section
            ladder_program = ladder_df.filter(ladder_df[program_key] == program)
            ladder_autorun = ladder_program.filter(
                pl.col(body_type_key)
                .cast(pl.Utf8)
                .str.to_lowercase()
                .is_in(["autorun", "autorun★"])
            )
            # ladder_autorun = ladder_program[
            #     pl.col(body_type_key).cast(pl.Utf8).str.to_lowercase().is_in(['autorun', 'autorun★'])
            # ]
            # ladder_autorun=ladder_program.filter(ladder_program[body_type_key] == 'AutoRun')

            # In the autorun section select the the rows which have only outcoils, also get the their respective rungs and attributes
            out_coil_df = ladder_autorun.filter(
                pl.col("OBJECT_TYPE_LIST").cast(pl.Utf8).str.to_lowercase() == "coil"
            )
            outcoil_rung_list = list(out_coil_df["RUNG"])
            outcoil_attributes_list = list(out_coil_df["ATTRIBUTES"])

            for rung_num, coil_attribute in zip(
                outcoil_rung_list, outcoil_attributes_list
            ):

                # Get the variable name of each outcoil and then get the comment asscoaited with that variable
                coil_attribute = eval(coil_attribute)
                coil_operand = coil_attribute["operand"]
                coil_comment_dict = get_the_comment_from_program(
                    variable=coil_operand,
                    program=program,
                    input_comment_data=comment_data,
                )

                # Chcek for cycle start and cycle end, create lists for each one of those, cycle start and cycle end
                if regex_pattern_check(
                    cycle_half_jp, coil_comment_dict
                ) or regex_pattern_check(cycle_full_jp, coil_comment_dict):

                    if regex_pattern_check(start_jp, coil_comment_dict):

                        start_list.append(rung_num)

                if regex_pattern_check(
                    cycle_half_jp, coil_comment_dict
                ) or regex_pattern_check(cycle_full_jp, coil_comment_dict):

                    if regex_pattern_check(end_jp, coil_comment_dict):

                        end_list.append(rung_num)

            rung_range_dict["start_list"] = start_list
            rung_range_dict["end_list"] = end_list
            program_range_dict[program] = rung_range_dict

        ##### program_loop: Loop in to each program, with start and endlist########################################3
        for program in program_range_dict.keys():

            """This Code block is for Check1"""
            logger.info(f"Checking Condition 1 in Rule 2 {program}")

            start_list = program_range_dict[program]["start_list"]
            end_list = program_range_dict[program]["end_list"]

            if (len(start_list) != 0) and (len(end_list) != 0):
                logger.warning(
                    f"Ranges detected for program: {program} start_list: {start_list}  end_list: {end_list}"
                )
            if len(start_list) != len(end_list):
                logger.warning(
                    f"Open Ended Range: {program} start_list:{start_list} end_list:{end_list}"
                )

                detail_dict = {}
                detail_dict["start_list"] = start_list
                detail_dict["end_list"] = end_list

                # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                #                             'CHECK_NUMBER':['1'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                #                             'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                #                             'STATUS': ["NG"], 'NG_EXPLANATION':['Open Ended Range']})

                sub_df_en = pd.DataFrame(
                    {
                        "Result": ["NG"],
                        "Task": [program],
                        "Section": ["AutoRun"],
                        "RungNo": [""],
                        "Target": [detail_dict],
                        "CheckItem": rule_2_check_item,
                        "Detail": ["Open Ended Range"],
                        "Status": [""],
                    }
                )

                output_df = pd.concat([output_df, sub_df_en], ignore_index=True)

            if len(start_list) == len(end_list):

                if len(start_list) != 0:

                    check_1_NG_flag = 0

                    # Loop for each pair of start_rung and end_rung
                    for start_rung, end_rung in zip(start_list, end_list):

                        if int(end_rung) > int(start_rung):

                            # Find out which rungs have two or more outcoils
                            ladder_program = ladder_df.filter(
                                ladder_df[program_key] == program
                            )
                            # ladder_autorun=ladder_program.filter(pl.col(body_type_key).cast(pl.Utf8).str.to_lowercase().is_in(['autorun', 'autorun★']))
                            # ladder_autorun = ladder_program.filter(
                            #         pl.col(body_type_key).cast(pl.Utf8).str.to_lowercase().is_in(["autorun", "autorun★"])
                            #     )

                            ladder_autorun = ladder_program.filter(
                                pl.col(body_type_key)
                                .cast(pl.Utf8)
                                .str.to_lowercase()
                                .is_in(["autorun", "autorun★"])
                            )
                            # Extract the section memory feeding from ladder in the program under question
                            ladder_memory_feeding = ladder_program.filter(
                                pl.col(body_type_key).cast(pl.Utf8).str.to_lowercase()
                                == "memoryfeeding"
                            )

                            # Extract the Condition Section
                            ladder_condition = ladder_program.filter(
                                pl.col(body_type_key).cast(pl.Utf8).str.to_lowercase()
                                == "condition"
                            )

                            # ladder_autorun = ladder_program[
                            #     pl.col(body_type_key).cast(pl.Utf8).str.to_lowercase().isin(['autorun', 'autorun★'])
                            # ]

                            # ladder_memory_feeding = ladder_program[
                            #     pl.col(body_type_key).cast(pl.Utf8).str.to_lowercase() == 'memoryfeeding'
                            # ]

                            # ladder_condition = ladder_program[
                            #     pl.col(body_type_key).cast(pl.Utf8).str.to_lowercase() == 'condition'
                            # ]

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

                            rungs_in_the_range = list(
                                ladder_program_for_rungs_in_range["RUNG"].unique()
                            )
                            rungs_in_the_range.sort(reverse=True)

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

                            logger.warning(
                                f"Rungs with >2 coils: {rungs_with_two_or_more_outcoils} in program:{program}"
                            )

                            # Check in each rung which has two or more outcoils, whether each of Ok/Normal, NG/abnormal exists or not
                            ok_coil_operands = []
                            normal_coil_operands = []

                            ng_coil_operands = []
                            abnormal_coil_operands = []

                            for (
                                rung_with_two_or_more_outcoils
                            ) in rungs_with_two_or_more_outcoils:

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

                                # Extract all the ok/normal , Ng/abnormal operands
                                for outcoil_attribute in outcoil_for_check_attributes:
                                    outcoil_attribute = eval(outcoil_attribute)
                                    coil_operand = outcoil_attribute["operand"]
                                    comment_list = get_the_comment_from_program(
                                        variable=coil_operand,
                                        program=program,
                                        input_comment_data=comment_data,
                                    )

                                    comment_patterns_to_checked = [
                                        ok_jp,
                                        normal_jp,
                                        ng_jp,
                                        abnormal_jp,
                                    ]

                                    for pattern_ in comment_patterns_to_checked:

                                        if regex_pattern_check(pattern_, comment_list):

                                            if pattern_ == ok_jp:
                                                ok_coil_operands.append(coil_operand)

                                            if pattern_ == normal_jp:
                                                normal_coil_operands.append(
                                                    coil_operand
                                                )

                                            if pattern_ == ng_jp:
                                                ng_coil_operands.append(coil_operand)

                                            if pattern_ == abnormal_jp:
                                                abnormal_coil_operands.append(
                                                    coil_operand
                                                )

                            # Once the Ok/NG operands are extracted, begin the next step
                            logger.warning(
                                f"program:{program} Coil Operands in Autorun \n Ok:{ok_coil_operands},\n normal:{normal_coil_operands},\n NG:{ng_coil_operands}, \n Abnormal:{abnormal_coil_operands}"
                            )
                            memory_feeding_attributes = list(
                                ladder_memory_feeding["ATTRIBUTES"]
                            )
                            memory_feeding_objects = list(
                                ladder_memory_feeding["OBJECT_TYPE_LIST"]
                            )
                            memory_feeding_rungs = list(
                                ladder_memory_feeding["RUNG"].unique()
                            )

                            block_connections_list = []
                            contact_coil_pairs = []
                            # Extract the contact coil pairs in all therungs of memory feeding section
                            for rung in memory_feeding_rungs:

                                ladder_mem_rung = ladder_memory_feeding.filter(
                                    pl.col("RUNG") == rung
                                )

                                mem_rung_coils = ladder_mem_rung.filter(
                                    pl.col("OBJECT_TYPE_LIST")
                                    .cast(pl.Utf8)
                                    .str.to_lowercase()
                                    == "coil"
                                )
                                mem_rung_coils_attributes = list(
                                    mem_rung_coils["ATTRIBUTES"]
                                )

                                mem_rung_contacts = ladder_mem_rung.filter(
                                    pl.col("OBJECT_TYPE_LIST")
                                    .cast(pl.Utf8)
                                    .str.to_lowercase()
                                    == "contact"
                                )
                                mem_rung_contacts_attributes = list(
                                    mem_rung_contacts["ATTRIBUTES"]
                                )

                                # Extract the Move and Clear block details if present for future use
                                block_connections_list.append(
                                    get_block_connections(ladder_mem_rung)
                                )

                                for contact in mem_rung_contacts_attributes:
                                    contact = eval(contact)

                                    if contact["negated"] == "false":

                                        for coil in mem_rung_coils_attributes:
                                            coil = eval(coil)
                                            temp_dict = {}
                                            # Chcek whther the coil and contact are in series connection
                                            if set(contact["out_list"]) & set(
                                                coil["in_list"]
                                            ):

                                                temp_dict[contact["operand"]] = coil

                                                contact_coil_pairs.append(temp_dict)

                            ok_mem_coils = []
                            normal_mem_coils = []

                            ng_mem_coils = []
                            abnormal_mem_coils = []

                            ok_contacts_found = []
                            normal_contacts_found = []
                            ng_contacts_found = []
                            abnormal_contacts_found = []

                            # Get all the contacts in the memory feeding sections
                            # Check whether the A contacts in Memory feeding section is among the Ok/Normal, NG/Abnormal coil operands
                            # If the match is found  the store the connected Coil to the A contact
                            memory_feeding_contacts = ladder_memory_feeding.filter(
                                pl.col("OBJECT_TYPE_LIST")
                                .cast(pl.Utf8)
                                .str.to_lowercase()
                                == "contact"
                            )
                            memory_feeding_contacts_attributes = list(
                                memory_feeding_contacts["ATTRIBUTES"]
                            )

                            for contact_attr in memory_feeding_contacts_attributes:
                                contact_attr = eval(contact_attr)

                                if contact_attr["negated"] == "false":

                                    if contact_attr["operand"] in ok_coil_operands:
                                        for cc_pair in contact_coil_pairs:

                                            coil_match = cc_pair.get(
                                                contact_attr["operand"], "NONE"
                                            )
                                            if coil_match != "NONE":
                                                ok_contacts_found.append(
                                                    contact_attr["operand"]
                                                )
                                                ok_mem_coils.append(coil_match)

                                                logger.warning(
                                                    f"Program:{program} Ok Coil operand : {contact_attr['operand']} found in Memory Feedings"
                                                )

                                                break

                                    if contact_attr["operand"] in normal_coil_operands:
                                        for cc_pair in contact_coil_pairs:

                                            coil_match = cc_pair.get(
                                                contact_attr["operand"], "NONE"
                                            )
                                            if coil_match != "NONE":
                                                normal_contacts_found.append(
                                                    contact_attr["operand"]
                                                )
                                                normal_mem_coils.append(coil_match)
                                                logger.warning(
                                                    f"Program:{program} Normal Coil operand : {contact_attr['operand']} found in Memory Feedings"
                                                )

                                                break

                                    if contact_attr["operand"] in ng_coil_operands:
                                        for cc_pair in contact_coil_pairs:

                                            coil_match = cc_pair.get(
                                                contact_attr["operand"], "NONE"
                                            )
                                            if coil_match != "NONE":

                                                ng_contacts_found.append(
                                                    contact_attr["operand"]
                                                )
                                                ng_mem_coils.append(coil_match)
                                                logger.warning(
                                                    f"Program:{program} NG Coil operand : {contact_attr['operand']} found in Memory Feedings"
                                                )

                                                break

                                    if (
                                        contact_attr["operand"]
                                        in abnormal_coil_operands
                                    ):
                                        for cc_pair in contact_coil_pairs:

                                            coil_match = cc_pair.get(
                                                contact_attr["operand"], "NONE"
                                            )
                                            if coil_match != "NONE":

                                                abnormal_contacts_found.append(
                                                    contact_attr["operand"]
                                                )
                                                abnormal_mem_coils.append(coil_match)
                                                logger.warning(
                                                    f"Program:{program} Abnormal Coil operand : {contact_attr['operand']} found in Memory Feedings"
                                                )

                                                break

                            # For the OK/Normal NG/Abnormal coils that could not be located in Memory Feedings create a NG report
                            ok_missing_contacts = [
                                item
                                for item in ok_coil_operands
                                if item not in ok_contacts_found
                            ]
                            normal_missing_contacts = [
                                item
                                for item in normal_coil_operands
                                if item not in normal_contacts_found
                            ]
                            ng_missing_contacts = [
                                item
                                for item in ng_coil_operands
                                if item not in ng_contacts_found
                            ]
                            abnormal_missing_contacts = [
                                item
                                for item in abnormal_coil_operands
                                if item not in abnormal_contacts_found
                            ]

                            # Write the Ok Missing Contacts
                            detail_dict = {}
                            detail_dict["ok_missings_contacts"] = ok_missing_contacts

                            # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                            #                     'CHECK_NUMBER':['1'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                            #                     'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                            #                     'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_missing_contacts_en]})

                            sub_df_en = pd.DataFrame(
                                {
                                    "Result": ["NG"],
                                    "Task": [program],
                                    "Section": ["AutoRun"],
                                    "RungNo": [""],
                                    "Target": [detail_dict],
                                    "CheckItem": rule_2_check_item,
                                    "Detail": [NG_exp_missing_contacts_en],
                                    "Status": [""],
                                }
                            )

                            output_df = pd.concat(
                                [output_df, sub_df_en], ignore_index=True
                            )

                            # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                            #                     'CHECK_NUMBER':['1'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                            #                     'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                            #                     'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_missing_contacts_jp]})

                            sub_df_jp = pd.DataFrame(
                                {
                                    "Result": ["NG"],
                                    "Task": [program],
                                    "Section": ["AutoRun"],
                                    "RungNo": [""],
                                    "Target": [detail_dict],
                                    "CheckItem": rule_2_check_item,
                                    "Detail": [NG_exp_missing_contacts_jp],
                                    "Status": [""],
                                }
                            )

                            output_df_jp = pd.concat(
                                [output_df_jp, sub_df_jp], ignore_index=True
                            )

                            # Write the Normal Missing Contacts
                            detail_dict = {}
                            detail_dict["normal_missings_contacts"] = (
                                normal_missing_contacts
                            )

                            # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                            #                     'CHECK_NUMBER':['1'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                            #                     'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                            #                     'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_missing_contacts_en]})

                            sub_df_en = pd.DataFrame(
                                {
                                    "Result": ["NG"],
                                    "Task": [program],
                                    "Section": ["AutoRun"],
                                    "RungNo": [""],
                                    "Target": [detail_dict],
                                    "CheckItem": rule_2_check_item,
                                    "Detail": [NG_exp_missing_contacts_en],
                                    "Status": [""],
                                }
                            )

                            output_df = pd.concat(
                                [output_df, sub_df_en], ignore_index=True
                            )

                            # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                            #                         'CHECK_NUMBER':['1'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                            #                         'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                            #                         'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_missing_contacts_jp]})

                            sub_df_jp = pd.DataFrame(
                                {
                                    "Result": ["NG"],
                                    "Task": [program],
                                    "Section": ["AutoRun"],
                                    "RungNo": [""],
                                    "Target": [detail_dict],
                                    "CheckItem": rule_2_check_item,
                                    "Detail": [NG_exp_missing_contacts_jp],
                                    "Status": [""],
                                }
                            )

                            output_df_jp = pd.concat(
                                [output_df_jp, sub_df_jp], ignore_index=True
                            )

                            # Write the NG Missing Contacts
                            detail_dict = {}
                            detail_dict["ng_missings_contacts"] = ng_missing_contacts

                            # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                            #                         'CHECK_NUMBER':['1'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                            #                         'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                            #                         'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_missing_contacts_en]})

                            sub_df_en = pd.DataFrame(
                                {
                                    "Result": ["NG"],
                                    "Task": [program],
                                    "Section": ["AutoRun"],
                                    "RungNo": [""],
                                    "Target": [detail_dict],
                                    "CheckItem": rule_2_check_item,
                                    "Detail": [NG_exp_missing_contacts_en],
                                    "Status": [""],
                                }
                            )

                            output_df = pd.concat(
                                [output_df, sub_df_en], ignore_index=True
                            )

                            # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                            #                         'CHECK_NUMBER':['1'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                            #                         'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                            #                         'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_missing_contacts_jp]})

                            sub_df_jp = pd.DataFrame(
                                {
                                    "Result": ["NG"],
                                    "Task": [program],
                                    "Section": ["AutoRun"],
                                    "RungNo": [""],
                                    "Target": [detail_dict],
                                    "CheckItem": rule_2_check_item,
                                    "Detail": [NG_exp_missing_contacts_jp],
                                    "Status": [""],
                                }
                            )

                            output_df_jp = pd.concat(
                                [output_df_jp, sub_df_jp], ignore_index=True
                            )

                            # Write the Abnormal Missing Contacts

                            detail_dict = {}
                            detail_dict["abnormal_missings_contacts"] = (
                                abnormal_missing_contacts
                            )

                            # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                            #                         'CHECK_NUMBER':['1'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                            #                         'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                            #                         'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_missing_contacts_en]})

                            sub_df_en = pd.DataFrame(
                                {
                                    "Result": ["NG"],
                                    "Task": [program],
                                    "Section": ["AutoRun"],
                                    "RungNo": [""],
                                    "Target": [detail_dict],
                                    "CheckItem": rule_2_check_item,
                                    "Detail": [NG_exp_missing_contacts_en],
                                    "Status": [""],
                                }
                            )

                            output_df = pd.concat(
                                [output_df, sub_df_en], ignore_index=True
                            )

                            # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                            #                         'CHECK_NUMBER':['1'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                            #                         'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                            #                         'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_missing_contacts_jp]})

                            sub_df_jp = pd.DataFrame(
                                {
                                    "Result": ["NG"],
                                    "Task": [program],
                                    "Section": ["AutoRun"],
                                    "RungNo": [""],
                                    "Target": [detail_dict],
                                    "CheckItem": rule_2_check_item,
                                    "Detail": [NG_exp_missing_contacts_jp],
                                    "Status": [""],
                                }
                            )

                            output_df_jp = pd.concat(
                                [output_df_jp, sub_df_jp], ignore_index=True
                            )

                            # After Writing the missing contacts report, write the OK report for the same

                            if len(ok_contacts_found) > 0:
                                detail_dict = {}
                                detail_dict["ok_contacts_found"] = ok_contacts_found

                                sub_df_en, sub_df_jp = create_dataframes(
                                    task_name=program,
                                    section_name="AutoRun",
                                    rule_number="2",
                                    check_number="1",
                                    rule_content=rule_content_2,
                                    status="OK",
                                    details=detail_dict,
                                    NG_exp_en="NONE",
                                    NG_exp_jp="NONE",
                                )

                                output_df = pd.concat(
                                    [output_df, sub_df_en], ignore_index=True
                                )
                                output_df_jp = pd.concat(
                                    [output_df_jp, sub_df_jp], ignore_index=True
                                )

                            if len(normal_contacts_found) > 0:
                                detail_dict = {}
                                detail_dict["normal_contacts_found"] = (
                                    normal_contacts_found
                                )

                                sub_df_en, sub_df_jp = create_dataframes(
                                    task_name=program,
                                    section_name="AutoRun",
                                    rule_number="2",
                                    check_number="1",
                                    rule_content=rule_content_2,
                                    status="OK",
                                    details=detail_dict,
                                    NG_exp_en="",
                                    NG_exp_jp="",
                                )

                                output_df = pd.concat(
                                    [output_df, sub_df_en], ignore_index=True
                                )
                                output_df_jp = pd.concat(
                                    [output_df_jp, sub_df_jp], ignore_index=True
                                )

                            if len(ng_contacts_found) > 0:
                                detail_dict = {}
                                detail_dict["ng_contacts_found"] = ng_contacts_found

                                sub_df_en, sub_df_jp = create_dataframes(
                                    task_name=program,
                                    section_name="AutoRun",
                                    rule_number="2",
                                    check_number="1",
                                    rule_content=rule_content_2,
                                    status="OK",
                                    details=detail_dict,
                                    NG_exp_en="",
                                    NG_exp_jp="",
                                )

                                output_df = pd.concat(
                                    [output_df, sub_df_en], ignore_index=True
                                )
                                output_df_jp = pd.concat(
                                    [output_df_jp, sub_df_jp], ignore_index=True
                                )

                            if len(abnormal_contacts_found) > 0:
                                detail_dict = {}
                                detail_dict["abnormal_contacts_found"] = (
                                    abnormal_contacts_found
                                )

                                sub_df_en, sub_df_jp = create_dataframes(
                                    task_name=program,
                                    section_name="AutoRun",
                                    rule_number="2",
                                    check_number="1",
                                    rule_content=rule_content_2,
                                    status="OK",
                                    details=detail_dict,
                                    NG_exp_en="",
                                    NG_exp_jp="",
                                )

                                output_df = pd.concat(
                                    [output_df, sub_df_en], ignore_index=True
                                )
                                output_df_jp = pd.concat(
                                    [output_df_jp, sub_df_jp], ignore_index=True
                                )

                            ############################################################################

                            # print(ok_mem_coils, normal_mem_coils, ng_mem_coils, abnormal_mem_coils)
                            logger.warning(
                                f"program:{program} Coil Counterparts in Memory Feedings \n Ok:{ok_mem_coils},\n normal:{normal_mem_coils},\n NG:{ng_mem_coils}, \n Abnormal:{abnormal_mem_coils}"
                            )

                            # Extract the coils in memory that are both seta dn reset
                            memory_feeding_coils = ladder_memory_feeding.filter(
                                pl.col("OBJECT_TYPE_LIST")
                                .cast(pl.Utf8)
                                .str.to_lowercase()
                                == "coil"
                            )
                            memory_feeding_coils_attributes = list(
                                memory_feeding_coils["ATTRIBUTES"]
                            )
                            memory_feeding_coils_rungs = list(
                                memory_feeding_coils["RUNG"]
                            )

                            mem_coils_set_list = []
                            mem_coils_set_rung_list = []

                            mem_coils_reset_list = []
                            mem_coils_reset_rung_list = []

                            for coil_attr, mem_rung in zip(
                                memory_feeding_coils_attributes,
                                memory_feeding_coils_rungs,
                            ):
                                coil_attr = eval(coil_attr)

                                coil_latch = coil_attr.get("latch", "NONE")

                                if coil_latch != "NONE":

                                    if coil_latch == "reset":
                                        mem_coils_reset_list.append(
                                            coil_attr["operand"]
                                        )
                                        mem_coils_reset_rung_list.append(mem_rung)

                                    if coil_latch == "set":
                                        mem_coils_set_list.append(coil_attr["operand"])
                                        mem_coils_set_rung_list.append(mem_rung)

                            # Compare the contacts found with set coils, and write the data to report
                            for ok_mem_coil in ok_mem_coils:
                                if ok_mem_coil["operand"] in mem_coils_set_list:

                                    detail_dict = {}
                                    detail_dict["OK_Set_Coil"] = ok_mem_coil["operand"]

                                    # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                    #                 'CHECK_NUMBER':['2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                    #                 'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                    #                 'STATUS': ["OK"], 'NG_EXPLANATION':['NONE']})

                                    sub_df_en = pd.DataFrame(
                                        {
                                            "Result": ["OK"],
                                            "Task": [program],
                                            "Section": ["AutoRun"],
                                            "RungNo": [""],
                                            "Target": [detail_dict],
                                            "CheckItem": rule_2_check_item,
                                            "Detail": [""],
                                            "Status": [""],
                                        }
                                    )

                                    output_df = pd.concat(
                                        [output_df, sub_df_en], ignore_index=True
                                    )

                                    # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                    #                 'CHECK_NUMBER':['2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                    #                 'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                    #                 'STATUS': ["OK"], 'NG_EXPLANATION':['NONE']})

                                    sub_df_jp = pd.DataFrame(
                                        {
                                            "Result": ["OK"],
                                            "Task": [program],
                                            "Section": ["AutoRun"],
                                            "RungNo": [""],
                                            "Target": [detail_dict],
                                            "CheckItem": rule_2_check_item,
                                            "Detail": [""],
                                            "Status": [""],
                                        }
                                    )

                                    output_df_jp = pd.concat(
                                        [output_df_jp, sub_df_jp], ignore_index=True
                                    )

                                else:

                                    detail_dict = {}
                                    detail_dict["OK_Set_Coil"] = ok_mem_coil["operand"]

                                    #                                 sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                    #                                                 'CHECK_NUMBER':['2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                    #                                                 'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                    #                                                 'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_set_en]}
                                    # )
                                    sub_df_en = pd.DataFrame(
                                        {
                                            "Result": ["NG"],
                                            "Task": [program],
                                            "Section": ["AutoRun"],
                                            "RungNo": [""],
                                            "Target": [detail_dict],
                                            "CheckItem": rule_2_check_item,
                                            "Detail": [NG_exp_set_en],
                                            "Status": [""],
                                        }
                                    )

                                    output_df = pd.concat(
                                        [output_df, sub_df_en], ignore_index=True
                                    )

                                    # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                    #                 'CHECK_NUMBER':['2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                    #                 'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                    #                 'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_set_jp]})

                                    sub_df_jp = pd.DataFrame(
                                        {
                                            "Result": ["NG"],
                                            "Task": [program],
                                            "Section": ["AutoRun"],
                                            "RungNo": [""],
                                            "Target": [detail_dict],
                                            "CheckItem": rule_2_check_item,
                                            "Detail": [NG_exp_set_jp],
                                            "Status": [""],
                                        }
                                    )

                                    output_df_jp = pd.concat(
                                        [output_df_jp, sub_df_jp], ignore_index=True
                                    )

                            for normal_mem_coil in normal_mem_coils:
                                if normal_mem_coil["operand"] in mem_coils_set_list:

                                    detail_dict = {}
                                    detail_dict["Normal_Set_Coil"] = normal_mem_coil[
                                        "operand"
                                    ]

                                    # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                    #                 'CHECK_NUMBER':['2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                    #                 'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                    #                 'STATUS': ["OK"], 'NG_EXPLANATION':["NONE"]})

                                    sub_df_en = pd.DataFrame(
                                        {
                                            "Result": ["OK"],
                                            "Task": [program],
                                            "Section": ["AutoRun"],
                                            "RungNo": [""],
                                            "Target": [detail_dict],
                                            "CheckItem": rule_2_check_item,
                                            "Detail": [""],
                                            "Status": [""],
                                        }
                                    )

                                    output_df = pd.concat(
                                        [output_df, sub_df_en], ignore_index=True
                                    )

                                    # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                    #                 'CHECK_NUMBER':['2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                    #                 'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                    #                 'STATUS': ["OK"], 'NG_EXPLANATION':["NONE"]})

                                    sub_df_jp = pd.DataFrame(
                                        {
                                            "Result": ["OK"],
                                            "Task": [program],
                                            "Section": ["AutoRun"],
                                            "RungNo": [""],
                                            "Target": [detail_dict],
                                            "CheckItem": rule_2_check_item,
                                            "Detail": [""],
                                            "Status": [""],
                                        }
                                    )

                                    output_df_jp = pd.concat(
                                        [output_df_jp, sub_df_jp], ignore_index=True
                                    )

                                else:

                                    detail_dict = {}
                                    detail_dict["Normal_Set_Coil"] = normal_mem_coil[
                                        "operand"
                                    ]

                                    # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                    #                 'CHECK_NUMBER':['2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                    #                 'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                    #                 'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_set_en]})

                                    sub_df_en = pd.DataFrame(
                                        {
                                            "Result": ["NG"],
                                            "Task": [program],
                                            "Section": ["AutoRun"],
                                            "RungNo": [""],
                                            "Target": [detail_dict],
                                            "CheckItem": rule_2_check_item,
                                            "Detail": [NG_exp_set_en],
                                            "Status": [""],
                                        }
                                    )

                                    output_df = pd.concat(
                                        [output_df, sub_df_en], ignore_index=True
                                    )

                                    # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                    #                 'CHECK_NUMBER':['2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                    #                 'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                    #                 'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_set_jp]})

                                    sub_df_jp = pd.DataFrame(
                                        {
                                            "Result": ["NG"],
                                            "Task": [program],
                                            "Section": ["AutoRun"],
                                            "RungNo": [""],
                                            "Target": [detail_dict],
                                            "CheckItem": rule_2_check_item,
                                            "Detail": [NG_exp_set_jp],
                                            "Status": [""],
                                        }
                                    )

                                    output_df_jp = pd.concat(
                                        [output_df_jp, sub_df_jp], ignore_index=True
                                    )

                            for ng_mem_coil in ng_mem_coils:
                                if ng_mem_coil["operand"] in mem_coils_set_list:

                                    detail_dict = {}
                                    detail_dict["ng_Set_Coil"] = ng_mem_coil["operand"]

                                    # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                    #                 'CHECK_NUMBER':['2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                    #                 'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                    #                 'STATUS': ["OK"], 'NG_EXPLANATION':["NONE"]})

                                    sub_df_en = pd.DataFrame(
                                        {
                                            "Result": ["OK"],
                                            "Task": [program],
                                            "Section": ["AutoRun"],
                                            "RungNo": [""],
                                            "Target": [detail_dict],
                                            "CheckItem": rule_2_check_item,
                                            "Detail": [""],
                                            "Status": [""],
                                        }
                                    )

                                    output_df = pd.concat(
                                        [output_df, sub_df_en], ignore_index=True
                                    )

                                    # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                    #                 'CHECK_NUMBER':['2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                    #                 'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                    #                 'STATUS': ["OK"], 'NG_EXPLANATION':["NONE"]})

                                    sub_df_jp = pd.DataFrame(
                                        {
                                            "Result": ["OK"],
                                            "Task": [program],
                                            "Section": ["AutoRun"],
                                            "RungNo": [""],
                                            "Target": [detail_dict],
                                            "CheckItem": rule_2_check_item,
                                            "Detail": [""],
                                            "Status": [""],
                                        }
                                    )

                                    output_df_jp = pd.concat(
                                        [output_df_jp, sub_df_jp], ignore_index=True
                                    )

                                else:

                                    detail_dict = {}
                                    detail_dict["ng_Set_Coil"] = ng_mem_coil["operand"]

                                    # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                    #                 'CHECK_NUMBER':['2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                    #                 'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                    #                 'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_set_en]})

                                    sub_df_en = pd.DataFrame(
                                        {
                                            "Result": ["NG"],
                                            "Task": [program],
                                            "Section": ["AutoRun"],
                                            "RungNo": [""],
                                            "Target": [detail_dict],
                                            "CheckItem": rule_2_check_item,
                                            "Detail": [NG_exp_set_en],
                                            "Status": [""],
                                        }
                                    )

                                    output_df = pd.concat(
                                        [output_df, sub_df_en], ignore_index=True
                                    )

                                    # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                    #                 'CHECK_NUMBER':['2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                    #                 'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                    #                 'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_set_jp]})

                                    sub_df_jp = pd.DataFrame(
                                        {
                                            "Result": ["NG"],
                                            "Task": [program],
                                            "Section": ["AutoRun"],
                                            "RungNo": [""],
                                            "Target": [detail_dict],
                                            "CheckItem": rule_2_check_item,
                                            "Detail": [NG_exp_set_jp],
                                            "Status": [""],
                                        }
                                    )

                                    output_df_jp = pd.concat(
                                        [output_df_jp, sub_df_jp], ignore_index=True
                                    )

                            for abnormal_mem_coil in abnormal_mem_coils:
                                if abnormal_mem_coil["operand"] in mem_coils_set_list:

                                    detail_dict = {}
                                    detail_dict["abnormal_Set_Coil"] = (
                                        abnormal_mem_coil["operand"]
                                    )

                                    # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                    #                 'CHECK_NUMBER':['2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                    #                 'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                    #                 'STATUS': ["OK"], 'NG_EXPLANATION':["NONE"]})

                                    sub_df_en = pd.DataFrame(
                                        {
                                            "Result": ["OK"],
                                            "Task": [program],
                                            "Section": ["AutoRun"],
                                            "RungNo": [""],
                                            "Target": [detail_dict],
                                            "CheckItem": rule_2_check_item,
                                            "Detail": [""],
                                            "Status": [""],
                                        }
                                    )

                                    output_df = pd.concat(
                                        [output_df, sub_df_en], ignore_index=True
                                    )

                                    # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                    #                 'CHECK_NUMBER':['2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                    #                 'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                    #                 'STATUS': ["OK"], 'NG_EXPLANATION':["NONE"]})

                                    sub_df_jp = pd.DataFrame(
                                        {
                                            "Result": ["OK"],
                                            "Task": [program],
                                            "Section": ["AutoRun"],
                                            "RungNo": [""],
                                            "Target": [detail_dict],
                                            "CheckItem": rule_2_check_item,
                                            "Detail": [""],
                                            "Status": [""],
                                        }
                                    )

                                    output_df_jp = pd.concat(
                                        [output_df_jp, sub_df_jp], ignore_index=True
                                    )

                                else:

                                    detail_dict = {}
                                    detail_dict["abnormal_Set_Coil"] = (
                                        abnormal_mem_coil["operand"]
                                    )

                                    # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                    #                 'CHECK_NUMBER':['2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                    #                 'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                    #                 'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_set_en]})

                                    sub_df_en = pd.DataFrame(
                                        {
                                            "Result": ["NG"],
                                            "Task": [program],
                                            "Section": ["AutoRun"],
                                            "RungNo": [""],
                                            "Target": [detail_dict],
                                            "CheckItem": rule_2_check_item,
                                            "Detail": [NG_exp_set_en],
                                            "Status": [""],
                                        }
                                    )

                                    output_df = pd.concat(
                                        [output_df, sub_df_en], ignore_index=True
                                    )

                                    # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                    #                 'CHECK_NUMBER':['2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                    #                 'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                    #                 'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_set_jp]})

                                    sub_df_jp = pd.DataFrame(
                                        {
                                            "Result": ["NG"],
                                            "Task": [program],
                                            "Section": ["AutoRun"],
                                            "RungNo": [""],
                                            "Target": [detail_dict],
                                            "CheckItem": rule_2_check_item,
                                            "Detail": [NG_exp_set_jp],
                                            "Status": [""],
                                        }
                                    )

                                    output_df_jp = pd.concat(
                                        [output_df_jp, sub_df_jp], ignore_index=True
                                    )

                            set_reset_coils = list(
                                set(mem_coils_reset_list) & set(mem_coils_set_list)
                            )
                            logger.warning(
                                f"In program :{program} Coils that are both sEt and Reset :{set_reset_coils}"
                            )

                            ###################### Now check whether the Set connections are reset are not ##########################
                            ## Check for each ok, normal, ng and abnormal coils #################################################
                            ##########Chcek for all the three ways###########################################################
                            set_reset_check_dict = {
                                "ok_mem_coils": ok_mem_coils,
                                "normal_mem_coils": normal_mem_coils,
                                "ng_mem_coils": ng_mem_coils,
                                "abnormal_mem_coils": normal_mem_coils,
                            }

                            set_reset_check_keys = set_reset_check_dict.keys()
                            set_reset_check_values = set_reset_check_dict.values()

                            for coil_list_keys, coil_list_values in zip(
                                set_reset_check_keys, set_reset_check_values
                            ):

                                for coil in coil_list_values:
                                    coil_check_flag = 0

                                    # Reset check type 1 , check wheteher the coil is directly in set_reset pair
                                    if coil["operand"] in set_reset_coils:
                                        coil_check_flag = 1

                                        detail_dict = {}

                                        if coil_list_keys == "ok_mem_coils":
                                            detail_dict["OK_reset_coils"] = coil[
                                                "operand"
                                            ]

                                        if coil_list_keys == "normal_mem_coils":
                                            detail_dict["normal_reset_coils"] = coil[
                                                "operand"
                                            ]

                                        if coil_list_keys == "ng_mem_coils":
                                            detail_dict["ng_reset_coils"] = coil[
                                                "operand"
                                            ]

                                        if coil_list_keys == "abnormal_mem_coils":
                                            detail_dict["abnormal_reset_coils"] = coil[
                                                "operand"
                                            ]

                                        # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                        #             'CHECK_NUMBER':['3'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                        #             'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                        #             'STATUS': ["OK"], 'NG_EXPLANATION':['NONE']})

                                        sub_df_en = pd.DataFrame(
                                            {
                                                "Result": ["OK"],
                                                "Task": [program],
                                                "Section": ["AutoRun"],
                                                "RungNo": [""],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_2_check_item,
                                                "Detail": [""],
                                                "Status": [""],
                                            }
                                        )

                                        output_df = pd.concat(
                                            [output_df, sub_df_en], ignore_index=True
                                        )

                                        # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                        #             'CHECK_NUMBER':['3'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                        #             'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                        #             'STATUS': ["OK"], 'NG_EXPLANATION':['NONE']})

                                        sub_df_jp = pd.DataFrame(
                                            {
                                                "Result": ["OK"],
                                                "Task": [program],
                                                "Section": ["AutoRun"],
                                                "RungNo": [""],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_2_check_item,
                                                "Detail": [""],
                                                "Status": [""],
                                            }
                                        )

                                        output_df_jp = pd.concat(
                                            [output_df_jp, sub_df_jp], ignore_index=True
                                        )

                                        break

                                    # Reset check type 2 and 3, check whetehr the coil is associated with move and clear block
                                    if coil_check_flag != 1:
                                        for ele_block in block_connections_list:

                                            for sub_dict in ele_block:

                                                # check in clear block
                                                clear_block = sub_dict.get(
                                                    "Clear", "NONE"
                                                )
                                                if clear_block != "NONE":
                                                    for conns in clear_block:
                                                        clear_inout_conns = conns.get(
                                                            "InOut", "NONE"
                                                        )
                                                        if clear_inout_conns != "NONE":
                                                            if (
                                                                coil["operand"]
                                                                in clear_inout_conns
                                                            ):
                                                                coil_check_flag = 1

                                                                detail_dict = {}

                                                                if (
                                                                    coil_list_keys
                                                                    == "ok_mem_coils"
                                                                ):
                                                                    detail_dict[
                                                                        "OK_reset_coils"
                                                                    ] = coil["operand"]

                                                                if (
                                                                    coil_list_keys
                                                                    == "normal_mem_coils"
                                                                ):
                                                                    detail_dict[
                                                                        "normal_reset_coils"
                                                                    ] = coil["operand"]

                                                                if (
                                                                    coil_list_keys
                                                                    == "ng_mem_coils"
                                                                ):
                                                                    detail_dict[
                                                                        "ng_reset_coils"
                                                                    ] = coil["operand"]

                                                                if (
                                                                    coil_list_keys
                                                                    == "abnormal_mem_coils"
                                                                ):
                                                                    detail_dict[
                                                                        "abnormal_reset_coils"
                                                                    ] = coil["operand"]

                                                                # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                                                #                 'CHECK_NUMBER':['3'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                                                #                 'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                                                #                 'STATUS': ["OK"], 'NG_EXPLANATION':['NONE']})

                                                                sub_df_en = pd.DataFrame(
                                                                    {
                                                                        "Result": [
                                                                            "OK"
                                                                        ],
                                                                        "Task": [
                                                                            program
                                                                        ],
                                                                        "Section": [
                                                                            "AutoRun"
                                                                        ],
                                                                        "RungNo": [""],
                                                                        "Target": [
                                                                            detail_dict
                                                                        ],
                                                                        "CheckItem": rule_2_check_item,
                                                                        "Detail": [""],
                                                                        "Status": [""],
                                                                    }
                                                                )

                                                                output_df = pd.concat(
                                                                    [
                                                                        output_df,
                                                                        sub_df_en,
                                                                    ],
                                                                    ignore_index=True,
                                                                )

                                                                # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                                                #             'CHECK_NUMBER':['3'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                                                #             'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                                                #             'STATUS': ["OK"], 'NG_EXPLANATION':['NONE']})

                                                                sub_df_jp = pd.DataFrame(
                                                                    {
                                                                        "Result": [
                                                                            "OK"
                                                                        ],
                                                                        "Task": [
                                                                            program
                                                                        ],
                                                                        "Section": [
                                                                            "AutoRun"
                                                                        ],
                                                                        "RungNo": [""],
                                                                        "Target": [
                                                                            detail_dict
                                                                        ],
                                                                        "CheckItem": rule_2_check_item,
                                                                        "Detail": [""],
                                                                        "Status": [""],
                                                                    }
                                                                )

                                                                output_df_jp = pd.concat(
                                                                    [
                                                                        output_df_jp,
                                                                        sub_df_jp,
                                                                    ],
                                                                    ignore_index=True,
                                                                )
                                                                break

                                                # Check in Move block
                                                move_block = sub_dict.get(
                                                    "MOVE", "NONE"
                                                )
                                                if move_block != "NONE":
                                                    for conns in move_block:
                                                        move_out_conns = conns.get(
                                                            "Out", "NONE"
                                                        )
                                                        if move_out_conns != "NONE":
                                                            if (
                                                                coil["operand"]
                                                                in move_out_conns
                                                            ):
                                                                coil_check_flag = 1

                                                                detail_dict = {}

                                                                if (
                                                                    coil_list_keys
                                                                    == "ok_mem_coils"
                                                                ):
                                                                    detail_dict[
                                                                        "OK_reset_coils"
                                                                    ] = coil["operand"]

                                                                if (
                                                                    coil_list_keys
                                                                    == "normal_mem_coils"
                                                                ):
                                                                    detail_dict[
                                                                        "normal_reset_coils"
                                                                    ] = coil["operand"]

                                                                if (
                                                                    coil_list_keys
                                                                    == "ng_mem_coils"
                                                                ):
                                                                    detail_dict[
                                                                        "ng_reset_coils"
                                                                    ] = coil["operand"]

                                                                if (
                                                                    coil_list_keys
                                                                    == "abnormal_mem_coils"
                                                                ):
                                                                    detail_dict[
                                                                        "abnormal_reset_coils"
                                                                    ] = coil["operand"]

                                                                # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                                                #         'CHECK_NUMBER':['3'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                                                #         'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                                                #         'STATUS': ["OK"], 'NG_EXPLANATION':['NONE']})

                                                                sub_df_en = pd.DataFrame(
                                                                    {
                                                                        "Result": [
                                                                            "OK"
                                                                        ],
                                                                        "Task": [
                                                                            program
                                                                        ],
                                                                        "Section": [
                                                                            "AutoRun"
                                                                        ],
                                                                        "RungNo": [""],
                                                                        "Target": [
                                                                            detail_dict
                                                                        ],
                                                                        "CheckItem": rule_2_check_item,
                                                                        "Detail": [""],
                                                                        "Status": [""],
                                                                    }
                                                                )

                                                                output_df = pd.concat(
                                                                    [
                                                                        output_df,
                                                                        sub_df_en,
                                                                    ],
                                                                    ignore_index=True,
                                                                )

                                                                # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                                                #         'CHECK_NUMBER':['3'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                                                #         'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                                                #         'STATUS': ["OK"], 'NG_EXPLANATION':['NONE']})

                                                                sub_df_jp = pd.DataFrame(
                                                                    {
                                                                        "Result": [
                                                                            "OK"
                                                                        ],
                                                                        "Task": [
                                                                            program
                                                                        ],
                                                                        "Section": [
                                                                            "AutoRun"
                                                                        ],
                                                                        "RungNo": [""],
                                                                        "Target": [
                                                                            detail_dict
                                                                        ],
                                                                        "CheckItem": rule_2_check_item,
                                                                        "Detail": [""],
                                                                        "Status": [""],
                                                                    }
                                                                )

                                                                output_df_jp = pd.concat(
                                                                    [
                                                                        output_df_jp,
                                                                        sub_df_jp,
                                                                    ],
                                                                    ignore_index=True,
                                                                )

                                                                break

                                    if coil_check_flag == 0:

                                        detail_dict = {}

                                        if coil_list_keys == "ok_mem_coils":
                                            detail_dict["OK_reset_coils"] = coil[
                                                "operand"
                                            ]

                                        if coil_list_keys == "normal_mem_coils":
                                            detail_dict["normal_reset_coils"] = coil[
                                                "operand"
                                            ]

                                        if coil_list_keys == "ng_mem_coils":
                                            detail_dict["ng_reset_coils"] = coil[
                                                "operand"
                                            ]

                                        if coil_list_keys == "abnormal_mem_coils":
                                            detail_dict["abnormal_reset_coils"] = coil[
                                                "operand"
                                            ]

                                        # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                        #                         'CHECK_NUMBER':['3'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                        #                         'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                        #                         'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_set_reset_en]})

                                        sub_df_en = pd.DataFrame(
                                            {
                                                "Result": ["NG"],
                                                "Task": [program],
                                                "Section": ["AutoRun"],
                                                "RungNo": [""],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_2_check_item,
                                                "Detail": [NG_exp_set_reset_en],
                                                "Status": [""],
                                            }
                                        )

                                        output_df = pd.concat(
                                            [output_df, sub_df_en], ignore_index=True
                                        )

                                        # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                        #                         'CHECK_NUMBER':['3'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                        #                         'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                        #                         'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_set_reset_jp]})

                                        sub_df_jp = pd.DataFrame(
                                            {
                                                "Result": ["NG"],
                                                "Task": [program],
                                                "Section": ["AutoRun"],
                                                "RungNo": [""],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_2_check_item,
                                                "Detail": [NG_exp_set_reset_jp],
                                                "Status": [""],
                                            }
                                        )

                                        output_df_jp = pd.concat(
                                            [output_df_jp, sub_df_jp], ignore_index=True
                                        )

                            #############Now Cheeck for 'B' contacts in Condition Section #############
                            condition_contacts = ladder_condition.filter(
                                pl.col("OBJECT_TYPE_LIST")
                                .cast(pl.Utf8)
                                .str.to_lowercase()
                                == "contact"
                            )
                            condition_contact_attributes = list(
                                condition_contacts["ATTRIBUTES"]
                            )
                            condtion_B_contact_list = []
                            condtion_B_contact_dict_list = []

                            ## Extract all the B contacts in the Condition section
                            for contact_attr in condition_contact_attributes:
                                contact_attr = eval(contact_attr)

                                contact_negated_status = contact_attr.get(
                                    "negated", "NONE"
                                )

                                if all(
                                    [
                                        contact_negated_status != "NONE",
                                        contact_negated_status == "true",
                                    ]
                                ):
                                    condtion_B_contact_dict_list.append(contact_attr)
                                    condtion_B_contact_list.append(
                                        contact_attr["operand"]
                                    )

                            logger.warning(
                                f"In program: {program} Condition Section B contacts are {condtion_B_contact_list}"
                            )

                            for coil_list_keys, coil_list_values in zip(
                                set_reset_check_keys, set_reset_check_values
                            ):

                                for coil in coil_list_values:

                                    B_contact_check_flag = 0

                                    if coil["operand"] in condtion_B_contact_list:
                                        B_contact_check_flag = 1

                                        detail_dict = {}

                                        if coil_list_keys == "ok_mem_coils":
                                            detail_dict["OK_condtion_coils"] = coil[
                                                "operand"
                                            ]

                                        if coil_list_keys == "normal_mem_coils":
                                            detail_dict["normal_condition_coils"] = (
                                                coil["operand"]
                                            )

                                        if coil_list_keys == "ng_mem_coils":
                                            detail_dict["ng_condition_coils"] = coil[
                                                "operand"
                                            ]

                                        if coil_list_keys == "abnormal_mem_coils":
                                            detail_dict["abnormal_condition_coils"] = (
                                                coil["operand"]
                                            )

                                        # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                        #                         'CHECK_NUMBER':['4.1'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                        #                         'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                        #                         'STATUS': ["OK"], 'NG_EXPLANATION':["NONE"]})

                                        sub_df_en = pd.DataFrame(
                                            {
                                                "Result": ["OK"],
                                                "Task": [program],
                                                "Section": ["AutoRun"],
                                                "RungNo": [""],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_2_check_item,
                                                "Detail": [""],
                                                "Status": [""],
                                            }
                                        )

                                        output_df = pd.concat(
                                            [output_df, sub_df_en], ignore_index=True
                                        )

                                        # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                        #                         'CHECK_NUMBER':['4.1'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                        #                         'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                        #                         'STATUS': ["OK"], 'NG_EXPLANATION':["NONE"]})

                                        sub_df_jp = pd.DataFrame(
                                            {
                                                "Result": ["OK"],
                                                "Task": [program],
                                                "Section": ["AutoRun"],
                                                "RungNo": [""],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_2_check_item,
                                                "Detail": [""],
                                                "Status": [""],
                                            }
                                        )

                                        output_df_jp = pd.concat(
                                            [output_df_jp, sub_df_jp], ignore_index=True
                                        )

                                        if re.search(r"\[0\]", coil["operand"]):

                                            detail_dict = {}

                                            if coil_list_keys == "ok_mem_coils":
                                                detail_dict["OK_condtion_coils"] = coil[
                                                    "operand"
                                                ]

                                            if coil_list_keys == "normal_mem_coils":
                                                detail_dict[
                                                    "normal_condition_coils"
                                                ] = coil["operand"]

                                            if coil_list_keys == "ng_mem_coils":
                                                detail_dict["ng_condition_coils"] = (
                                                    coil["operand"]
                                                )

                                            if coil_list_keys == "abnormal_mem_coils":
                                                detail_dict[
                                                    "abnormal_condition_coils"
                                                ] = coil["operand"]

                                            # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                            #                     'CHECK_NUMBER':['4.2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                            #                     'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                            #                     'STATUS': ["OK"], 'NG_EXPLANATION':["NONE"]})

                                            sub_df_en = pd.DataFrame(
                                                {
                                                    "Result": ["OK"],
                                                    "Task": [program],
                                                    "Section": ["AutoRun"],
                                                    "RungNo": [""],
                                                    "Target": [detail_dict],
                                                    "CheckItem": rule_2_check_item,
                                                    "Detail": [""],
                                                    "Status": [""],
                                                }
                                            )

                                            output_df = pd.concat(
                                                [output_df, sub_df_en],
                                                ignore_index=True,
                                            )

                                            # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                            #                     'CHECK_NUMBER':['4.2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                            #                     'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                            #                     'STATUS': ["OK"], 'NG_EXPLANATION':["NONE"]})

                                            sub_df_jp = pd.DataFrame(
                                                {
                                                    "Result": ["OK"],
                                                    "Task": [program],
                                                    "Section": ["AutoRun"],
                                                    "RungNo": [""],
                                                    "Target": [detail_dict],
                                                    "CheckItem": rule_2_check_item,
                                                    "Detail": [""],
                                                    "Status": [""],
                                                }
                                            )

                                            output_df_jp = pd.concat(
                                                [output_df_jp, sub_df_jp],
                                                ignore_index=True,
                                            )

                                        else:

                                            detail_dict = {}

                                            if coil_list_keys == "ok_mem_coils":
                                                detail_dict["OK_condtion_coils"] = coil[
                                                    "operand"
                                                ]

                                            if coil_list_keys == "normal_mem_coils":
                                                detail_dict[
                                                    "normal_condition_coils"
                                                ] = coil["operand"]

                                            if coil_list_keys == "ng_mem_coils":
                                                detail_dict["ng_condition_coils"] = (
                                                    coil["operand"]
                                                )

                                            if coil_list_keys == "abnormal_mem_coils":
                                                detail_dict[
                                                    "abnormal_condition_coils"
                                                ] = coil["operand"]

                                            # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                            #                     'CHECK_NUMBER':['4.2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                            #                     'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                            #                     'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_condition_en]})

                                            sub_df_en = pd.DataFrame(
                                                {
                                                    "Result": ["NG"],
                                                    "Task": [program],
                                                    "Section": ["AutoRun"],
                                                    "RungNo": [""],
                                                    "Target": [detail_dict],
                                                    "CheckItem": rule_2_check_item,
                                                    "Detail": [NG_exp_condition_en],
                                                    "Status": [""],
                                                }
                                            )

                                            output_df = pd.concat(
                                                [output_df, sub_df_en],
                                                ignore_index=True,
                                            )

                                            # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                            #                     'CHECK_NUMBER':['4.2'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                            #                     'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                            #                     'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_condition_jp]})

                                            sub_df_jp = pd.DataFrame(
                                                {
                                                    "Result": ["NG"],
                                                    "Task": [program],
                                                    "Section": ["AutoRun"],
                                                    "RungNo": [""],
                                                    "Target": [detail_dict],
                                                    "CheckItem": rule_2_check_item,
                                                    "Detail": [NG_exp_condition_jp],
                                                    "Status": [""],
                                                }
                                            )

                                            output_df_jp = pd.concat(
                                                [output_df_jp, sub_df_jp],
                                                ignore_index=True,
                                            )

                                        break

                                    else:

                                        detail_dict = {}

                                        if coil_list_keys == "ok_mem_coils":
                                            detail_dict["OK_condtion_coils"] = coil[
                                                "operand"
                                            ]

                                        if coil_list_keys == "normal_mem_coils":
                                            detail_dict["normal_condition_coils"] = (
                                                coil["operand"]
                                            )

                                        if coil_list_keys == "ng_mem_coils":
                                            detail_dict["ng_condition_coils"] = coil[
                                                "operand"
                                            ]

                                        if coil_list_keys == "abnormal_mem_coils":
                                            detail_dict["abnormal_condition_coils"] = (
                                                coil["operand"]
                                            )

                                        # sub_df_en=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                        #                         'CHECK_NUMBER':['4.1'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                        #                         'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                        #                         'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_condition_en]})

                                        sub_df_en = pd.DataFrame(
                                            {
                                                "Result": ["NG"],
                                                "Task": [program],
                                                "Section": ["AutoRun"],
                                                "RungNo": [""],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_2_check_item,
                                                "Detail": [NG_exp_condition_en],
                                                "Status": [""],
                                            }
                                        )

                                        output_df = pd.concat(
                                            [output_df, sub_df_en], ignore_index=True
                                        )

                                        # sub_df_jp=pd.DataFrame({'TASK_NAME':[program], 'SECTION_NAME':['AutoRun'], 'RULE_NUMBER': ['2'],
                                        #                         'CHECK_NUMBER':['4.1'], "RUNG_NUMBER":[""],'RULE_CONTENT':[rule_content_condition_1_en],
                                        #                         'CHECK_CONTENT':[""],'TARGET_OUTCOIL': [detail_dict],
                                        #                         'STATUS': ["NG"], 'NG_EXPLANATION':[NG_exp_condition_jp]})

                                        sub_df_jp = pd.DataFrame(
                                            {
                                                "Result": ["NG"],
                                                "Task": [program],
                                                "Section": ["AutoRun"],
                                                "RungNo": [""],
                                                "Target": [detail_dict],
                                                "CheckItem": rule_2_check_item,
                                                "Detail": [NG_exp_condition_jp],
                                                "Status": [""],
                                            }
                                        )

                                        output_df_jp = pd.concat(
                                            [output_df_jp, sub_df_jp], ignore_index=True
                                        )

        ###########################################################################################################################33
        """" Code block for Check 2 """

        for program in unique_program_values:

            rung_range_dict = {}

            ladder_program = ladder_df.filter(ladder_df[program_key] == program)

            # ladder_autorun=ladder_program.filter(pl.col(body_type_key).cast(pl.Utf8).str.to_lowercase().is_in(['autorun', 'autorun★']))
            ladder_autorun = ladder_program.filter(
                pl.col(body_type_key)
                .cast(pl.Utf8)
                .str.to_lowercase()
                .is_in(["autorun", "autorun★"])
            )
            ladder_autorun = ladder_autorun.sort("RUNG", descending=True)

            ladder_condition = ladder_program.filter(
                pl.col(body_type_key).cast(pl.Utf8).str.to_lowercase() == "condition"
            )
            ladder_condition = ladder_condition.sort("RUNG", descending=True)

            out_coil_df = ladder_autorun.filter(
                ladder_autorun["OBJECT_TYPE_LIST"] == "Coil"
            )
            outcoil_rung_list = list(out_coil_df["RUNG"])

            outcoil_rung_unique_list = list(out_coil_df["RUNG"].unique())
            outcoil_attributes_list = list(out_coil_df["ATTRIBUTES"])

            # Get all the contacts and their respective rung

            start_list = []
            ok_coil_list = []
            ok_coil_rung_list = []

            normal_coil_list = []
            normal_coil_rung_list = []

            for rung_num, coil_attribute in zip(
                outcoil_rung_list, outcoil_attributes_list
            ):

                coil_attribute = eval(coil_attribute)
                coil_operand = coil_attribute["operand"]
                coil_comment_dict = get_the_comment_from_program(
                    variable=coil_operand,
                    program=program,
                    input_comment_data=comment_data,
                )

                out_coil_rung_df = out_coil_df.filter(out_coil_df["RUNG"] == rung_num)

                # get the starting point, only the first point is important
                if regex_pattern_check(
                    cycle_half_jp, coil_comment_dict
                ) or regex_pattern_check(cycle_full_jp, coil_comment_dict):
                    if regex_pattern_check(start_jp, coil_comment_dict):
                        start_list.append(rung_num)

                if regex_pattern_check(ok_jp, coil_comment_dict):

                    if len(out_coil_rung_df) > 1:

                        ok_coil_list.append(coil_attribute["operand"])
                        ok_coil_rung_list.append(rung_num)

                if regex_pattern_check(normal_jp, coil_comment_dict):

                    if len(out_coil_rung_df) > 1:

                        normal_coil_list.append(coil_attribute["operand"])
                        normal_coil_rung_list.append(rung_num)

            # ic(program, start_list, ok_coil_list, ok_coil_rung_list)

            # Once the ok contact rungs are obtained Check in the preceeding rungs , whether Self holding exist or nor

            self_holding_parallel = {}
            ladder_pd_program = ladder_pd_df[ladder_pd_df[program_key] == program]
            ladder_pd_autorun = ladder_pd_program[
                ladder_pd_program[body_type_key] == "AutoRun"
            ]

            # ic(ok_coil_rung_list)

            # cheek for self holding contacts and their respective parallel chains
            # This Iteration is for Ok Coils
            for ok_coil, ok_rung in zip(ok_coil_list, ok_coil_rung_list):

                for unique_rung in outcoil_rung_unique_list:

                    if all([unique_rung <= ok_rung, unique_rung >= start_list[0]]):

                        autorun_rung_contacts_pd = ladder_pd_autorun[
                            ladder_pd_autorun["RUNG"] == unique_rung
                        ]
                        autorun_rung_contacts = ladder_autorun.filter(
                            ladder_autorun["RUNG"] == unique_rung
                        )

                        coil_df_unique_rung = autorun_rung_contacts.filter(
                            pl.col("OBJECT_TYPE_LIST") == "Coil"
                        )

                        # In each of the rung, check for only rungs that have two or more outcoils
                        if len(coil_df_unique_rung) > 1:

                            self_holding_contacts = check_self_holding(
                                autorun_rung_contacts_pd
                            )
                            logger.warning(
                                f"In program : {program}, Self holding contacts are \nrung:{unique_rung} \n {self_holding_contacts}"
                            )

                            if len(self_holding_contacts) > 0:

                                detail_dict = {}
                                detail_dict["rung"] = unique_rung
                                detail_dict["comment_type"] = "OK"
                                detail_dict["self_holding"] = self_holding_contacts

                                sub_df_en, sub_df_jp = create_dataframes(
                                    task_name=program,
                                    section_name="AutoRun",
                                    rule_number="2",
                                    check_number="5",
                                    rule_content=rule_content_2,
                                    status="OK",
                                    details=detail_dict,
                                    NG_exp_en="",
                                    NG_exp_jp="",
                                )

                                output_df = pd.concat(
                                    [output_df, sub_df_en], ignore_index=True
                                )
                                output_df_jp = pd.concat(
                                    [output_df_jp, sub_df_jp], ignore_index=True
                                )

                            else:

                                detail_dict = {}
                                detail_dict["rung"] = unique_rung
                                detail_dict["comment_type"] = "OK"
                                sub_df_en, sub_df_jp = create_dataframes(
                                    task_name=program,
                                    section_name="AutoRun",
                                    rule_number="2",
                                    check_number="5",
                                    rule_content=rule_content_2,
                                    status="NG",
                                    details=detail_dict,
                                    NG_exp_en="Self Holding Contacts Not Found",
                                    NG_exp_jp="Self Holding Contacts Not Found",
                                )

                                output_df = pd.concat(
                                    [output_df, sub_df_en], ignore_index=True
                                )
                                output_df_jp = pd.concat(
                                    [output_df_jp, sub_df_jp], ignore_index=True
                                )

                        # Proceed ahead only if the coild is self holding, and get the parallel A contacts names
                        if ok_coil in self_holding_contacts:

                            parallel_connections = list(
                                get_parallel_contacts(autorun_rung_contacts).values()
                            )

                            parallel_contacts = get_in_parallel_A_contacts(
                                parallel_connections, ok_coil
                            )

                            if len(parallel_contacts) > 0:

                                detail_dict = {}
                                detail_dict["rung"] = unique_rung
                                detail_dict["parallel_contacts"] = parallel_contacts
                                detail_dict["comment_type"] = "OK"

                                sub_df_en, sub_df_jp = create_dataframes(
                                    task_name=program,
                                    section_name="AutoRun",
                                    rule_number="2",
                                    check_number="5",
                                    rule_content=rule_content_2,
                                    status="OK",
                                    details=detail_dict,
                                    NG_exp_en="",
                                    NG_exp_jp="",
                                )

                                output_df = pd.concat(
                                    [output_df, sub_df_en], ignore_index=True
                                )
                                output_df_jp = pd.concat(
                                    [output_df_jp, sub_df_jp], ignore_index=True
                                )

                            else:

                                detail_dict = {}
                                detail_dict["rung"] = unique_rung
                                detail_dict["parallel_contacts"] = []
                                detail_dict["comment_type"] = "OK"

                                sub_df_en, sub_df_jp = create_dataframes(
                                    task_name=program,
                                    section_name="AutoRun",
                                    rule_number="2",
                                    check_number="5",
                                    rule_content=rule_content_2,
                                    status="NG",
                                    details=detail_dict,
                                    NG_exp_en="Parallel A Contacts not found",
                                    NG_exp_jp="Parallel A Contacts not found",
                                )

                                output_df = pd.concat(
                                    [output_df, sub_df_en], ignore_index=True
                                )
                                output_df_jp = pd.concat(
                                    [output_df_jp, sub_df_jp], ignore_index=True
                                )

                            logger.warning(
                                f"In program : {program}, Parallel contacts are \nrung:{unique_rung} \n {parallel_contacts}"
                            )

                            # Once the parallel contacts are obtained, get the previous outcoils
                            # print(unique_rung, parallel_contacts)

                            # Look in to previous rungs for matching outcoil

                            for parallel_contact in parallel_contacts:
                                parallel_contact_flag = 0

                                try:

                                    for prev_rung in range(unique_rung - 1, 0, -1):
                                        out_coil_previous_df = out_coil_df.filter(
                                            pl.col("RUNG") == prev_rung
                                        )
                                        out_coil_previous_attributes = list(
                                            out_coil_previous_df["ATTRIBUTES"]
                                        )

                                        for prev_attr in out_coil_previous_attributes:

                                            prev_attr = eval(prev_attr)
                                            prev_attr_operand = prev_attr["operand"]

                                            if parallel_contact == prev_attr_operand:

                                                rule_2_ladder = ladder_autorun.filter(
                                                    pl.col("RUNG") == prev_rung
                                                )
                                                check_2_results = (
                                                    Rule_2_Check_2_program(
                                                        ladder_df=rule_2_ladder,
                                                        main_ladder=ladder_autorun,
                                                        ref_rung=prev_rung,
                                                    )
                                                )

                                                logger.warning(
                                                    f"In program :{program} AutoRun section \n ref_rung:{prev_rung} \n parallel contact:{parallel_contact} \n Results :{check_2_results} "
                                                )

                                                if check_2_results["status"] == "OK":

                                                    detail_dict = {}
                                                    detail_dict["rung"] = (
                                                        check_2_results["ref_rung"]
                                                    )
                                                    detail_dict["target_rung"] = (
                                                        check_2_results["target_rung"]
                                                    )
                                                    detail_dict["MOVE_Inp"] = (
                                                        check_2_results["MOVE_Inp"]
                                                    )
                                                    detail_dict["comment_type"] = "OK"

                                                    sub_df_en, sub_df_jp = (
                                                        create_dataframes(
                                                            task_name=program,
                                                            section_name="AutoRun",
                                                            rule_number="2",
                                                            check_number=check_2_results[
                                                                "check_number"
                                                            ],
                                                            rule_content=rule_content_2,
                                                            status=check_2_results[
                                                                "status"
                                                            ],
                                                            details=detail_dict,
                                                            NG_exp_en=check_2_results[
                                                                "NG_explanantion"
                                                            ],
                                                            NG_exp_jp=check_2_results[
                                                                "NG_explanantion"
                                                            ],
                                                        )
                                                    )

                                                    output_df = pd.concat(
                                                        [output_df, sub_df_en],
                                                        ignore_index=True,
                                                    )
                                                    output_df_jp = pd.concat(
                                                        [output_df_jp, sub_df_jp],
                                                        ignore_index=True,
                                                    )
                                                    parallel_contact_flag = 1

                                                    raise BreakInner

                                                if check_2_results["status"] == "NG":

                                                    check_2_condition_results = Rule_2_Check_2_program(
                                                        ladder_df=rule_2_ladder,
                                                        main_ladder=ladder_condition,
                                                        ref_rung=prev_rung,
                                                    )
                                                    logger.warning(
                                                        f"In program :{program} Condition section \n ref_rung:{prev_rung} \n parallel contact:{parallel_contact} \n Results :{check_2_condition_results} "
                                                    )

                                                    if (
                                                        check_2_condition_results[
                                                            "status"
                                                        ]
                                                        == "OK"
                                                    ):

                                                        detail_dict = {}
                                                        detail_dict["rung"] = (
                                                            check_2_condition_results[
                                                                "ref_rung"
                                                            ]
                                                        )
                                                        detail_dict["target_rung"] = (
                                                            check_2_condition_results[
                                                                "target_rung"
                                                            ]
                                                        )
                                                        detail_dict["MOVE_Inp"] = (
                                                            check_2_condition_results[
                                                                "MOVE_Inp"
                                                            ]
                                                        )
                                                        detail_dict["comment_type"] = (
                                                            "OK"
                                                        )

                                                        sub_df_en, sub_df_jp = (
                                                            create_dataframes(
                                                                task_name=program,
                                                                section_name="Condition",
                                                                rule_number="2",
                                                                check_number=check_2_condition_results[
                                                                    "check_number"
                                                                ],
                                                                rule_content=rule_content_2,
                                                                status=check_2_condition_results[
                                                                    "status"
                                                                ],
                                                                details=detail_dict,
                                                                NG_exp_en=check_2_condition_results[
                                                                    "NG_explanantion"
                                                                ],
                                                                NG_exp_jp=check_2_condition_results[
                                                                    "NG_explanantion"
                                                                ],
                                                            )
                                                        )

                                                        output_df = pd.concat(
                                                            [output_df, sub_df_en],
                                                            ignore_index=True,
                                                        )
                                                        output_df_jp = pd.concat(
                                                            [output_df_jp, sub_df_jp],
                                                            ignore_index=True,
                                                        )
                                                        parallel_contact_flag = 1

                                                        raise BreakInner

                                                if parallel_contact_flag != 1:

                                                    detail_dict = {}
                                                    detail_dict["rung"] = (
                                                        check_2_results["ref_rung"]
                                                    )
                                                    detail_dict["comment_type"] = "OK"

                                                    sub_df_en, sub_df_jp = (
                                                        create_dataframes(
                                                            task_name=program,
                                                            section_name="AutoRun",
                                                            rule_number="2",
                                                            check_number=check_2_results[
                                                                "check_number"
                                                            ],
                                                            rule_content=rule_content_2,
                                                            status=check_2_results[
                                                                "status"
                                                            ],
                                                            details=detail_dict,
                                                            NG_exp_en=check_2_results[
                                                                "NG_explanantion"
                                                            ],
                                                            NG_exp_jp=check_2_results[
                                                                "NG_explanantion"
                                                            ],
                                                        )
                                                    )

                                                    output_df = pd.concat(
                                                        [output_df, sub_df_en],
                                                        ignore_index=True,
                                                    )
                                                    output_df_jp = pd.concat(
                                                        [output_df_jp, sub_df_jp],
                                                        ignore_index=True,
                                                    )

                                except BreakInner:
                                    logger.warning(
                                        f"Breaking early  Results obtained for \n program:{program} \n Parallen contact: {parallel_contact} in rung :{unique_rung}"
                                    )

            ###############################===========Condition 2 check for Normal Coil List ################################

            # This is the iteration for Normal Coil list
            logger.debug("Iteration for Normal coils")
            for normal_coil, normal_rung in zip(
                normal_coil_list, normal_coil_rung_list
            ):

                for unique_rung in outcoil_rung_unique_list:

                    if all([unique_rung <= normal_rung, unique_rung >= start_list[0]]):

                        autorun_rung_contacts_pd = ladder_pd_autorun[
                            ladder_pd_autorun["RUNG"] == unique_rung
                        ]
                        autorun_rung_contacts = ladder_autorun.filter(
                            ladder_autorun["RUNG"] == unique_rung
                        )

                        coil_df_unique_rung = autorun_rung_contacts.filter(
                            pl.col("OBJECT_TYPE_LIST") == "Coil"
                        )

                        if len(coil_df_unique_rung) > 1:

                            self_holding_contacts = check_self_holding(
                                autorun_rung_contacts_pd
                            )
                            logger.warning(
                                f"In program : {program}, Self holding Normal contacts are \nrung:{unique_rung} \n {self_holding_contacts}"
                            )

                            if len(self_holding_contacts) > 0:

                                detail_dict = {}
                                detail_dict["rung"] = unique_rung
                                detail_dict["comment_type"] = "Normal"
                                detail_dict["self_holding"] = self_holding_contacts
                                sub_df_en, sub_df_jp = create_dataframes(
                                    task_name=program,
                                    section_name="AutoRun",
                                    rule_number="2",
                                    check_number="5",
                                    rule_content=rule_content_2,
                                    status="OK",
                                    details=detail_dict,
                                    NG_exp_en="",
                                    NG_exp_jp="",
                                )

                                output_df = pd.concat(
                                    [output_df, sub_df_en], ignore_index=True
                                )
                                output_df_jp = pd.concat(
                                    [output_df_jp, sub_df_jp], ignore_index=True
                                )

                            else:

                                detail_dict = {}
                                detail_dict["rung"] = unique_rung
                                detail_dict["comment_type"] = "Normal"
                                sub_df_en, sub_df_jp = create_dataframes(
                                    task_name=program,
                                    section_name="AutoRun",
                                    rule_number="2",
                                    check_number="5",
                                    rule_content=rule_content_2,
                                    status="NG",
                                    details=detail_dict,
                                    NG_exp_en="Self Holding Contacts Not Found",
                                    NG_exp_jp="Self Holding Contacts Not Found",
                                )

                                output_df = pd.concat(
                                    [output_df, sub_df_en], ignore_index=True
                                )
                                output_df_jp = pd.concat(
                                    [output_df_jp, sub_df_jp], ignore_index=True
                                )

                        # Proceed ahead only if the coild is self holding, and get the parallel A contacts names
                        if normal_coil in self_holding_contacts:

                            parallel_connections = list(
                                get_parallel_contacts(autorun_rung_contacts).values()
                            )

                            parallel_contacts = get_in_parallel_A_contacts(
                                parallel_connections, normal_coil
                            )

                            if len(parallel_contacts) > 0:

                                detail_dict = {}
                                detail_dict["rung"] = unique_rung
                                detail_dict["parallel_contacts"] = parallel_contacts
                                detail_dict["comment_type"] = "Normal"

                                sub_df_en, sub_df_jp = create_dataframes(
                                    task_name=program,
                                    section_name="AutoRun",
                                    rule_number="2",
                                    check_number="5",
                                    rule_content=rule_content_2,
                                    status="OK",
                                    details=detail_dict,
                                    NG_exp_en="",
                                    NG_exp_jp="",
                                )

                                output_df = pd.concat(
                                    [output_df, sub_df_en], ignore_index=True
                                )
                                output_df_jp = pd.concat(
                                    [output_df_jp, sub_df_jp], ignore_index=True
                                )

                            else:

                                detail_dict = {}
                                detail_dict["rung"] = unique_rung
                                detail_dict["parallel_contacts"] = []
                                detail_dict["comment_type"] = "Normal"

                                sub_df_en, sub_df_jp = create_dataframes(
                                    task_name=program,
                                    section_name="AutoRun",
                                    rule_number="2",
                                    check_number="5",
                                    rule_content=rule_content_2,
                                    status="NG",
                                    details=detail_dict,
                                    NG_exp_en="Parallel A Contacts not found",
                                    NG_exp_jp="Parallel A Contacts not found",
                                )

                                output_df = pd.concat(
                                    [output_df, sub_df_en], ignore_index=True
                                )
                                output_df_jp = pd.concat(
                                    [output_df_jp, sub_df_jp], ignore_index=True
                                )

                            logger.warning(
                                f"In program : {program}, Parallel Normal contacts are \nrung:{unique_rung} \n {parallel_contacts}"
                            )

                            # Once the parallel contacts are obtained, get the previous outcoils
                            # print(unique_rung, parallel_contacts)

                            # Look in to previous rungs for matching outcoil

                            for parallel_contact in parallel_contacts:
                                parallel_contact_flag = 0

                                try:

                                    for prev_rung in range(unique_rung - 1, 0, -1):
                                        out_coil_previous_df = out_coil_df.filter(
                                            pl.col("RUNG") == prev_rung
                                        )
                                        out_coil_previous_attributes = list(
                                            out_coil_previous_df["ATTRIBUTES"]
                                        )

                                        for prev_attr in out_coil_previous_attributes:

                                            prev_attr = eval(prev_attr)
                                            prev_attr_operand = prev_attr["operand"]

                                            if parallel_contact == prev_attr_operand:

                                                rule_2_ladder = ladder_autorun.filter(
                                                    pl.col("RUNG") == prev_rung
                                                )
                                                check_2_results = (
                                                    Rule_2_Check_2_program(
                                                        ladder_df=rule_2_ladder,
                                                        main_ladder=ladder_autorun,
                                                        ref_rung=prev_rung,
                                                    )
                                                )

                                                logger.warning(
                                                    f"In program :{program} AutoRun section \n ref_rung:{prev_rung} \n parallel contact:{parallel_contact} \n Results :{check_2_results} "
                                                )

                                                if check_2_results["status"] == "OK":

                                                    detail_dict = {}
                                                    detail_dict["rung"] = (
                                                        check_2_results["ref_rung"]
                                                    )
                                                    detail_dict["target_rung"] = (
                                                        check_2_results["target_rung"]
                                                    )
                                                    detail_dict["MOVE_Inp"] = (
                                                        check_2_results["MOVE_Inp"]
                                                    )
                                                    detail_dict["comment_type"] = (
                                                        "Normal"
                                                    )

                                                    sub_df_en, sub_df_jp = (
                                                        create_dataframes(
                                                            task_name=program,
                                                            section_name="AutoRun",
                                                            rule_number="2",
                                                            check_number=check_2_results[
                                                                "check_number"
                                                            ],
                                                            rule_content=rule_content_2,
                                                            status=check_2_results[
                                                                "status"
                                                            ],
                                                            details=detail_dict,
                                                            NG_exp_en=check_2_results[
                                                                "NG_explanantion"
                                                            ],
                                                            NG_exp_jp=check_2_results[
                                                                "NG_explanantion"
                                                            ],
                                                        )
                                                    )

                                                    output_df = pd.concat(
                                                        [output_df, sub_df_en],
                                                        ignore_index=True,
                                                    )
                                                    output_df_jp = pd.concat(
                                                        [output_df_jp, sub_df_jp],
                                                        ignore_index=True,
                                                    )
                                                    parallel_contact_flag = 1

                                                    raise BreakInner

                                                if check_2_results["status"] == "NG":

                                                    check_2_condition_results = Rule_2_Check_2_program(
                                                        ladder_df=rule_2_ladder,
                                                        main_ladder=ladder_condition,
                                                        ref_rung=prev_rung,
                                                    )
                                                    logger.warning(
                                                        f"In program :{program} Condition section \n ref_rung:{prev_rung} \n parallel contact:{parallel_contact} \n Results :{check_2_condition_results} "
                                                    )

                                                    if (
                                                        check_2_condition_results[
                                                            "status"
                                                        ]
                                                        == "OK"
                                                    ):

                                                        detail_dict = {}
                                                        detail_dict["rung"] = (
                                                            check_2_condition_results[
                                                                "ref_rung"
                                                            ]
                                                        )
                                                        detail_dict["target_rung"] = (
                                                            check_2_condition_results[
                                                                "target_rung"
                                                            ]
                                                        )
                                                        detail_dict["MOVE_Inp"] = (
                                                            check_2_condition_results[
                                                                "MOVE_Inp"
                                                            ]
                                                        )
                                                        detail_dict["comment_type"] = (
                                                            "Normal"
                                                        )

                                                        sub_df_en, sub_df_jp = (
                                                            create_dataframes(
                                                                task_name=program,
                                                                section_name="Condition",
                                                                rule_number="2",
                                                                check_number=check_2_condition_results[
                                                                    "check_number"
                                                                ],
                                                                rule_content=rule_content_2,
                                                                status=check_2_condition_results[
                                                                    "status"
                                                                ],
                                                                details=detail_dict,
                                                                NG_exp_en=check_2_condition_results[
                                                                    "NG_explanantion"
                                                                ],
                                                                NG_exp_jp=check_2_condition_results[
                                                                    "NG_explanantion"
                                                                ],
                                                            )
                                                        )

                                                        output_df = pd.concat(
                                                            [output_df, sub_df_en],
                                                            ignore_index=True,
                                                        )
                                                        output_df_jp = pd.concat(
                                                            [output_df_jp, sub_df_jp],
                                                            ignore_index=True,
                                                        )
                                                        parallel_contact_flag = 1

                                                        raise BreakInner

                                                if parallel_contact_flag != 1:

                                                    detail_dict = {}
                                                    detail_dict["rung"] = (
                                                        check_2_results["ref_rung"]
                                                    )
                                                    detail_dict["comment_type"] = (
                                                        "Normal"
                                                    )

                                                    sub_df_en, sub_df_jp = (
                                                        create_dataframes(
                                                            task_name=program,
                                                            section_name="AutoRun",
                                                            rule_number="2",
                                                            check_number=check_2_results[
                                                                "check_number"
                                                            ],
                                                            rule_content=rule_content_2,
                                                            status=check_2_results[
                                                                "status"
                                                            ],
                                                            details=detail_dict,
                                                            NG_exp_en=check_2_results[
                                                                "NG_explanantion"
                                                            ],
                                                            NG_exp_jp=check_2_results[
                                                                "NG_explanantion"
                                                            ],
                                                        )
                                                    )

                                                    output_df = pd.concat(
                                                        [output_df, sub_df_en],
                                                        ignore_index=True,
                                                    )
                                                    output_df_jp = pd.concat(
                                                        [output_df_jp, sub_df_jp],
                                                        ignore_index=True,
                                                    )

                                except BreakInner:
                                    logger.warning(
                                        f"Breaking early  Results obtained for \n program:{program} \n Parallen contact: {parallel_contact} in rung :{unique_rung}"
                                    )

        # final_output_csv =  pd.concat([output_df, output_df_jp], ignore_index=True)

        return {"status": "OK", "output_df": output_df}

    except Exception as e:
        logger.error(f"Error in execute_rule_2: {e}")
        return {"status": "NOT OK", "error": e}


######################################################################################3

# ladder_df=pl.read_csv("data_model_Rule_3_programwise.csv")
# ladder_df=ladder_df.filter(pl.col('PROGRAM')=='P111_Sample1_OK')
# ladder_df=ladder_df.filter(pl.col('BODY')=='AutoRun')
# ladder_df=ladder_df.filter(pl.col('RUNG')==15)

# main_ladder=pl.read_csv("data_model_Rule_3_programwise.csv")
# main_ladder=main_ladder.filter(pl.col('PROGRAM')=='P111_Sample1_OK')
# main_ladder=main_ladder.filter(pl.col('BODY')=='AutoRun')
# ref_rung=15


# kk=Rule_2_Check_2_program(ladder_df, main_ladder, ref_rung)
# print(kk)

###########################################################################################


# if __name__ == "__main__":


#     input_file=r"data_model_Rule_3_programwise.csv"
#     input_program_comment_file=r"comments_rule_3_programwise.json"
#     execute_rule_2(input_file=input_file, input_program_comment_file=input_program_comment_file,output_file_name="output_Rule_2_program",
#                                program_key='PROGRAM', body_type_key='BODY')
