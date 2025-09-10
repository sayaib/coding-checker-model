from bs4 import BeautifulSoup
from typing import *
import pandas as pd
import re, json
from loguru import logger
import os, sys

##################################################################


def ingest_file(xml_file_path: str):

    with open(xml_file_path, "r", encoding="utf-8") as file:

        xml_content = file.read()

    soup = BeautifulSoup(xml_content, "lxml-xml")

    return soup


#########################


def extract_variable_comment_functionwise(
    ladder_program: pd.DataFrame, dest_comment_name: str, data_model_dir: str
) -> None:

    try:

        function_blocks = [
            function["name"] for function in ladder_program.find_all("FunctionBlock")
        ]

        variable_attributes = {}
        attribute_dict = {}
        array_rgx_pattern = "ARRAY"
        array_rgx_pattern_small = "array"
        array_variant_1_pattern = "sRB_OUT"
        array_variant_2_pattern = "sRB_IN"

        ############ This code block looks in to all InputVars rags and extracts the comments##############
        for fn_name in function_blocks:

            logger.info(f"Extracting comments from Function {fn_name} and InputVars")

            fn_block = ladder_program.find("FunctionBlock", {"name": fn_name})

            in_vars = fn_block.find_all("InputVars")

            for in_var in in_vars:

                for variable in in_var.find_all("Variable"):

                    variable_attrs = list(variable.attrs)

                    # catch all the attriutes
                    for _attr in variable_attrs:

                        if _attr != "name":

                            attribute_dict[_attr] = variable.attrs[_attr]

                    variable_type = variable.find("TypeName")

                    if variable_type:
                        variable_type_text = variable_type.text
                    else:
                        variable_type_text = "NONE"

                    #################
                    documentation = variable.find("Documentation")

                    if documentation:
                        documentation_text = documentation.text
                    else:
                        documentation_text = "NONE"

                    ###################
                    variable_comment = variable.find("VariableComment")

                    if variable_comment:

                        variable_text = variable_comment.find("Text")

                        if variable_text:

                            if "id" in list(variable_text.attrs.keys()):

                                if variable_text.attrs["id"] == "2":
                                    variable_english_comment = variable_text.text
                                    variable_japanese_comment = "NONE"

                                elif variable_text.attrs["id"] == "1":
                                    variable_japanese_comment = variable_text.text
                                    variable_english_comment = "NONE"

                    # Look for Array types, if  found, extract them recursively
                    if any(
                        [
                            re.search(array_rgx_pattern, variable_type.text),
                            re.search(array_rgx_pattern_small, variable_type.text),
                            re.search(array_variant_1_pattern, variable_type.text),
                            re.search(array_variant_2_pattern, variable_type.text),
                        ]
                    ):

                        for child_element in variable.find_all("ElementComment"):

                            for child_text in child_element.find_all("Text"):

                                if child_text.attrs["id"] == "1":
                                    attribute_dict["variable_japanese_comment"] = (
                                        child_text.text
                                    )
                                if child_text.attrs["id"] == "2":
                                    attribute_dict["variable_english_comment"] = (
                                        child_text.text
                                    )

                                attribute_dict["tag"] = "InputVars"
                                attribute_dict["type"] = variable_type_text

                                attribute_dict["documentation"] = documentation_text

                                attribute_dict["variable_english_comment"] = (
                                    variable_english_comment
                                )
                                attribute_dict["variable_japanese_comment"] = (
                                    variable_japanese_comment
                                )

                            variable_attributes[
                                f"{variable.attrs['name']}{child_element.attrs['element']}@FN@{fn_name}"
                            ] = attribute_dict
                            attribute_dict = {}

                    else:

                        attribute_dict["tag"] = "InputVars"
                        attribute_dict["type"] = variable_type_text
                        attribute_dict["documentation"] = documentation_text

                        attribute_dict["variable_english_comment"] = (
                            variable_english_comment
                        )
                        attribute_dict["variable_japanese_comment"] = (
                            variable_japanese_comment
                        )

                        variable_attributes[
                            f"{variable.attrs['name']}@FN@{fn_name}"
                        ] = attribute_dict
                        attribute_dict = {}

        ############ Extract from the OutputVars
        for fn_name in function_blocks:

            logger.info(f"Extracting comments from Function {fn_name} and OutputVars")

            fn_block = ladder_program.find("FunctionBlock", {"name": fn_name})

            out_vars = fn_block.find_all("OutputVars")

            for out_var in out_vars:

                for variable in out_var.find_all("Variable"):

                    variable_attrs = list(variable.attrs)

                    # catch all the attriutes
                    for _attr in variable_attrs:

                        if _attr != "name":

                            attribute_dict[_attr] = variable.attrs[_attr]

                    variable_type = variable.find("TypeName")

                    if variable_type:
                        variable_type_text = variable_type.text
                    else:
                        variable_type_text = "NONE"

                    #################
                    documentation = variable.find("Documentation")

                    if documentation:
                        documentation_text = documentation.text
                    else:
                        documentation_text = "NONE"

                    ###################
                    variable_comment = variable.find("VariableComment")

                    if variable_comment:

                        variable_text = variable_comment.find("Text")

                        if variable_text:

                            if "id" in list(variable_text.attrs.keys()):

                                if variable_text.attrs["id"] == "2":
                                    variable_english_comment = variable_text.text
                                    variable_japanese_comment = "NONE"

                                elif variable_text.attrs["id"] == "1":
                                    variable_japanese_comment = variable_text.text
                                    variable_english_comment = "NONE"

                    # Look for Array types, if  found, extract them recursively
                    if re.search(array_rgx_pattern, variable_type.text):

                        for child_element in variable.find_all("ElementComment"):

                            for child_text in child_element.find_all("Text"):

                                if child_text.attrs["id"] == "1":
                                    attribute_dict["variable_japanese_comment"] = (
                                        child_text.text
                                    )
                                if child_text.attrs["id"] == "2":
                                    attribute_dict["variable_english_comment"] = (
                                        child_text.text
                                    )

                                attribute_dict["tag"] = "OutputVars"
                                attribute_dict["type"] = variable_type_text

                                attribute_dict["documentation"] = documentation_text

                                attribute_dict["variable_english_comment"] = (
                                    variable_english_comment
                                )
                                attribute_dict["variable_japanese_comment"] = (
                                    variable_japanese_comment
                                )

                            variable_attributes[
                                f"{variable.attrs['name']}{child_element.attrs['element']}@FN@{fn_name}"
                            ] = attribute_dict
                            attribute_dict = {}

                    else:

                        attribute_dict["tag"] = "OutputVars"
                        attribute_dict["type"] = variable_type_text
                        attribute_dict["documentation"] = documentation_text

                        attribute_dict["variable_english_comment"] = (
                            variable_english_comment
                        )
                        attribute_dict["variable_japanese_comment"] = (
                            variable_japanese_comment
                        )

                        variable_attributes[
                            f"{variable.attrs['name']}@FN@{fn_name}"
                        ] = attribute_dict
                        attribute_dict = {}

        ############ Extract from the Vars
        for fn_name in function_blocks:

            logger.info(f"Extracting comments from Function {fn_name} and Vars")

            fn_block = ladder_program.find("FunctionBlock", {"name": fn_name})

            _vars = fn_block.find_all("Vars")

            for _var in _vars:

                for variable in _var.find_all("Variable"):

                    variable_attrs = list(variable.attrs)

                    # catch all the attriutes
                    for _attr in variable_attrs:

                        if _attr != "name":

                            attribute_dict[_attr] = variable.attrs[_attr]

                    variable_type = variable.find("TypeName")

                    if variable_type:
                        variable_type_text = variable_type.text
                    else:
                        variable_type_text = "NONE"

                    #################
                    documentation = variable.find("Documentation")

                    if documentation:
                        documentation_text = documentation.text
                    else:
                        documentation_text = "NONE"

                    ###################
                    variable_comment = variable.find("VariableComment")

                    if variable_comment:

                        variable_text = variable_comment.find("Text")

                        if variable_text:

                            if "id" in list(variable_text.attrs.keys()):

                                if variable_text.attrs["id"] == "2":
                                    variable_english_comment = variable_text.text
                                    variable_japanese_comment = "NONE"

                                elif variable_text.attrs["id"] == "1":
                                    variable_japanese_comment = variable_text.text
                                    variable_english_comment = "NONE"

                    # Look for Array types, if  found, extract them recursively
                    if re.search(array_rgx_pattern, variable_type.text):

                        for child_element in variable.find_all("ElementComment"):

                            for child_text in child_element.find_all("Text"):

                                if child_text.attrs["id"] == "1":
                                    attribute_dict["variable_japanese_comment"] = (
                                        child_text.text
                                    )
                                if child_text.attrs["id"] == "2":
                                    attribute_dict["variable_english_comment"] = (
                                        child_text.text
                                    )

                                attribute_dict["tag"] = "Vars"
                                attribute_dict["type"] = variable_type_text

                                attribute_dict["documentation"] = documentation_text

                                attribute_dict["variable_english_comment"] = (
                                    variable_english_comment
                                )
                                attribute_dict["variable_japanese_comment"] = (
                                    variable_japanese_comment
                                )

                            variable_attributes[
                                f"{variable.attrs['name']}{child_element.attrs['element']}@FN@{fn_name}"
                            ] = attribute_dict
                            attribute_dict = {}

                    else:

                        attribute_dict["tag"] = "Vars"
                        attribute_dict["type"] = variable_type_text
                        attribute_dict["documentation"] = documentation_text

                        attribute_dict["variable_english_comment"] = (
                            variable_english_comment
                        )
                        attribute_dict["variable_japanese_comment"] = (
                            variable_japanese_comment
                        )

                        variable_attributes[
                            f"{variable.attrs['name']}@FN@{fn_name}"
                        ] = attribute_dict
                        attribute_dict = {}

        ##Write the data to file
        os.makedirs(data_model_dir, exist_ok=True)
        dest_comment_name = f"{data_model_dir}/{dest_comment_name}"

        with open(dest_comment_name, "w", encoding="utf-8") as json_file:
            json.dump(variable_attributes, json_file, ensure_ascii=False, indent=4)

    except Exception as e:

        logger.error(str(e))

    return None


######################

if __name__ == "__main__":

    input_ladder_file_path = r"Coding Checker_Rule1-3_250611.xml"
    ladder_program = ingest_file(input_ladder_file_path)
    dest_comment_name = "Rule_functionwise.json"

    extract_variable_comment_functionwise(ladder_program, dest_comment_name)
