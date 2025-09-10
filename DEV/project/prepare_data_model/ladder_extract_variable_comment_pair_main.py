from bs4 import BeautifulSoup
from typing import *
import pandas as pd
import re, json
from pathlib import Path
from ...main import logger
from .ladder_extract_variable_comment_pair_functionwise import *
import os, sys

#########################################################


def ingest_file(xml_file_path: str):

    with open(xml_file_path, "r", encoding="utf-8") as file:

        xml_content = file.read()

    soup = BeautifulSoup(xml_content, "lxml-xml")

    return soup


###################


######## Programwise variable extraction #############################


def extract_variable_comment_programwise(
    ladder_program: pd.DataFrame, dest_comment_name: str, data_model_dir: str
) -> None:

    try:

        program_names = [
            program["name"] for program in ladder_program.find_all("Program")
        ]

        variable_attributes = {}
        attribute_dict = {}
        constant_type = "NONE"

        # Look in to each of the program
        for pg_name in program_names:

            logger.info(f"Extracting comments from program {pg_name} and ExternalVars")

            program = ladder_program.find("Program", {"name": pg_name})

            # Extract all the external variables, loop in all the variables, all extract all its components
            external_vars = program.find_all("ExternalVars")

            for ext_var in external_vars:

                if "constant" in list(ext_var.attrs):

                    constant_type = ext_var.attrs["constant"]

                else:
                    constant_type = "NONE"

                for var in ext_var.find_all("Variable"):

                    for child in var.find_all():

                        if child.name == "TypeName":

                            attribute_dict["tag"] = "ExternalVars"
                            attribute_dict["type"] = child.text

                            attribute_dict["constant"] = constant_type

                        if child.name == "Text":

                            if "id" in list(child.attrs.keys()):

                                if child.attrs["id"] == "2":
                                    attribute_dict["variable_english_comment"] = (
                                        child.text
                                    )
                                    attribute_dict["constant"] = constant_type

                                elif child.attrs["id"] == "1":
                                    attribute_dict["variable_japanese_comment"] = (
                                        child.text
                                    )
                                    attribute_dict["constant"] = constant_type

                        else:
                            attribute_dict["variable_japanese_comment"] = "NONE"
                            attribute_dict["variable_english_comment"] = "NONE"

                        if child.name == "Documentation":
                            attribute_dict["documentation"] = child.text
                        else:
                            attribute_dict["documentation"] = "NONE"

                    variable_attributes[f"{ var.attrs['name'] }@PG@{pg_name}"] = (
                        attribute_dict
                    )
                    attribute_dict = {}

        # Extracting Array variables Program specific in the External Variables Tag
        for pg_name in program_names:

            logger.info(
                f"Extracting comments from program {pg_name} and ExternalVars and Array Types"
            )

            program = ladder_program.find("Program", {"name": pg_name})

            _vars = program.find_all("ExternalVars")
            array_rgx_pattern = "ARRAY"

            array_flag = 0

            for _var in _vars:

                if "accessSpecifier" in list(_var.attrs):
                    access_type = _var.attrs["accessSpecifier"]
                else:
                    access_type = "NONE"

                for variable in _var.find_all("Variable"):

                    documentation = variable.find("Documentation")

                    if documentation:
                        variable_documentation = documentation.text
                    else:
                        variable_documentation = "NONE"

                    variable_type = variable.find("TypeName")

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

                                attribute_dict["tag"] = "ExternalVars"
                                attribute_dict["type"] = variable_type.text
                                attribute_dict["access_type"] = access_type
                                attribute_dict["documentation"] = variable_documentation

                            variable_attributes[
                                f"{variable.attrs['name']}{child_element.attrs['element']}@PG@{pg_name}"
                            ] = attribute_dict
                            attribute_dict = {}

                    else:

                        documentation = variable.find("Documentation")
                        if documentation:
                            attribute_dict["documentation"] = documentation.text
                        else:
                            attribute_dict["documentation"] = "NONE"

                        attribute_dict["tag"] = "ExternalVars"
                        attribute_dict["type"] = variable_type.text
                        attribute_dict["access_type"] = access_type
                        variable_attributes[f"{ var.attrs['name'] }@PG@{pg_name}"] = (
                            attribute_dict
                        )
                        attribute_dict = {}

        # Extracting Array variables Program specific in the Variables Tag
        for pg_name in program_names:

            logger.info(
                f"Extracting comments from program {pg_name} and Vars and Array Types"
            )

            program = ladder_program.find("Program", {"name": pg_name})

            _vars = program.find_all("Vars")
            array_rgx_pattern = "ARRAY"
            array_rgx_pattern_small = "array"
            array_variant_1_pattern = "sRB_OUT"
            array_variant_2_pattern = "sRB_IN"

            array_flag = 0
            access_type = "NONE"

            for _var in _vars:

                if "accessSpecifier" in list(_var.attrs):

                    access_type = _var.attrs["accessSpecifier"]

                else:
                    access_type = "NONE"

                for variable in _var.find_all("Variable"):

                    documentation = variable.find("Documentation")

                    if documentation:
                        variable_documentation = documentation.text
                    else:
                        variable_documentation = "NONE"

                    variable_type = variable.find("TypeName")

                    if any(
                        [
                            re.search(array_rgx_pattern, variable_type.text),
                            re.search(array_rgx_pattern_small, variable_type.text),
                            re.search(array_variant_1_pattern, variable_type.text),
                            re.search(array_variant_2_pattern, variable_type.text),
                        ]
                    ):

                        child_element_comments = variable.find_all("ElementComment")

                        # There areArrays with multiple elements and only one element.So check the element length before going ahead
                        if len(child_element_comments) > 0:

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
                                    attribute_dict["type"] = variable_type.text
                                    attribute_dict["access_type"] = access_type
                                    attribute_dict["documentation"] = (
                                        variable_documentation
                                    )

                                variable_attributes[
                                    f"{variable.attrs['name']}{child_element.attrs['element']}@PG@{pg_name}"
                                ] = attribute_dict
                                attribute_dict = {}

                        else:
                            attribute_dict["tag"] = "Vars"
                            attribute_dict["type"] = variable_type.text
                            attribute_dict["access_type"] = access_type
                            attribute_dict["documentation"] = variable_documentation
                            variable_attributes[
                                f"{ variable.attrs['name'] }@PG@{pg_name}"
                            ] = attribute_dict
                            attribute_dict = {}

                    else:

                        documentation = variable.find("Documentation")
                        if documentation:
                            attribute_dict["documentation"] = documentation.text

                        else:
                            attribute_dict["documentation"] = "NONE"

                        attribute_dict["tag"] = "Vars"
                        attribute_dict["type"] = variable_type.text
                        attribute_dict["access_type"] = access_type
                        variable_attributes[
                            f"{ variable.attrs['name'] }@PG@{pg_name}"
                        ] = attribute_dict
                        attribute_dict = {}

        ########## Going for Global variables
        global_vars = [
            global_var for global_var in ladder_program.find_all("GlobalVars")
        ]
        array_rgx_pattern = "ARRAY"
        array_flag = 0
        attribute_dict = {}

        for global_var in global_vars:

            logger.info(f"Extracting comments from Global variables")

            for variable in global_var.find_all("Variable"):

                variable_type = variable.find("TypeName")

                if re.search(array_rgx_pattern, variable_type.text):

                    documentation = variable.find("Documentation")
                    if documentation:
                        documentation_text = documentation.text

                    else:
                        documentation_text = "NONE"

                    variable_comment = variable.find("VariableComment")
                    if variable_comment:
                        variable_comment_text = variable_comment.find("Text")

                        if variable_comment_text:

                            if variable_comment_text.attrs["id"] == "1":
                                attribute_dict["variable_japanese_comment"] = (
                                    variable_comment_text.text
                                )
                            if variable_comment_text.attrs["id"] == "2":
                                attribute_dict["variable_english_comment"] = (
                                    variable_comment_text.text
                                )

                            child_elements = variable_comment.find_all("ElementComment")
                            if child_elements:

                                for child_element in variable.find_all(
                                    "ElementComment"
                                ):

                                    for child_text in child_element.find_all("Text"):

                                        if child_text.attrs["id"] == "1":
                                            attribute_dict[
                                                "variable_japanese_comment"
                                            ] = child_text.text
                                        if child_text.attrs["id"] == "2":
                                            attribute_dict[
                                                "variable_english_comment"
                                            ] = child_text.text

                                        attribute_dict["tag"] = "GlobalVars"
                                        attribute_dict["documentation"] = (
                                            documentation_text
                                        )
                                        attribute_dict["type"] = variable_type.text

                                    variable_attributes[
                                        f"{variable.attrs['name']}{child_element.attrs['element']}@GBVAR"
                                    ] = attribute_dict
                                    attribute_dict = {}

                            else:
                                attribute_dict["documentation"] = documentation_text
                                variable_attributes[
                                    f"{variable.attrs['name']}@GBVAR"
                                ] = attribute_dict
                                attribute_dict = {}

                else:
                    documentation = variable.find("Documentation")
                    if documentation:
                        documentation_text = documentation.text
                    else:
                        documentation_text = "NONE"

                    variable_comment = variable.find("VariableComment")
                    if variable_comment:
                        variable_comment_text = variable_comment.find("Text")

                        if variable_comment_text:
                            if variable_comment_text.attrs["id"] == "1":
                                attribute_dict["variable_japanese_comment"] = (
                                    variable_comment_text.text
                                )
                            if variable_comment_text.attrs["id"] == "2":
                                attribute_dict["variable_english_comment"] = (
                                    variable_comment_text.text
                                )

                    attribute_dict["tag"] = "GlobalVars"
                    attribute_dict["type"] = variable_type.text
                    attribute_dict["documentation"] = documentation_text
                    variable_attributes[f"{variable.attrs['name']}@GBVAR"] = (
                        attribute_dict
                    )
                    attribute_dict = {}

        ###GEtting the values from Global namespaces
        global_namespaces = [
            global_name for global_name in ladder_program.find_all("GlobalNamespace")
        ]
        attribute_dict = {}

        for global_namespace in global_namespaces:

            logger.info(f"Extracting comments from Global Namepaces")

            data_type_specs = global_namespace.find_all("DataTypeDecl")

            if data_type_specs:

                for data_type_spec in data_type_specs:

                    data_type_spec_documentation = data_type_spec.find("Documentation")

                    members = data_type_spec.find_all("Member")

                    if members:
                        for member in members:

                            member_documentation = member.find("Documentation")
                            if member_documentation:
                                attribute_dict["member_documentation"] = (
                                    member_documentation.text
                                )
                            else:
                                attribute_dict["member_documentation"] = "NONE"

                            member_text = member.find("Text")
                            if member_text:
                                if member_text.attrs["id"] == "1":

                                    attribute_dict["member_japanese_comment"] = (
                                        member_text.text
                                    )
                                elif member_text.attrs["id"] == "2":
                                    attribute_dict["member_english_comment"] = (
                                        member_text.text
                                    )

                            member_type = member.find("TypeName")
                            if member_type:
                                attribute_dict["member_type"] = member_type.text

                            if data_type_spec_documentation:
                                attribute_dict["data_type_spec_documentation"] = (
                                    data_type_spec_documentation.text
                                )
                                attribute_dict["data_type_spec_name"] = (
                                    data_type_spec.attrs["name"]
                                )

                            else:
                                attribute_dict["data_type_spec_documentation"] = "NONE"
                                attribute_dict["data_type_spec_name"] = "NONE"

                            variable_attributes[f"{member.attrs['name']}@GBNMSP"] = (
                                attribute_dict
                            )
                            attribute_dict = {}

        #############GEtting the variable comments with Data sources and data sink ###################33

        os.makedirs(data_model_dir, exist_ok=True)
        dest_comment_name = f"{data_model_dir}/{dest_comment_name}"

        print("dest_comment_name", dest_comment_name)

        with open(dest_comment_name, "w", encoding="utf-8") as json_file:
            json.dump(variable_attributes, json_file, ensure_ascii=False, indent=4)

        logger.info("All data extracted")

    except Exception as e:

        logger.error(str(e))

    return None


############################################# Extract the Comment ###################


# def main_program_extract_comments(input_ladder_file_path:str, dest_comment_file_name:str, data_model_dir:str)-> None:
def main_program_extract_comments(input_ladder_file_path: str) -> None:
    """Main Function that extracts comments program wise and function wise"""

    ladder_program = ingest_file(input_ladder_file_path)

    dest_comment_file_name = os.path.splitext(os.path.basename(input_ladder_file_path))[
        0
    ]
    # data_model_dir =

    # Get the path to the current file (datamodel.py)
    current_file = Path(__file__).resolve()

    # Go up 3 levels to reach the root FastAPI directory
    project_root = current_file.parents[3]

    # Now get the full path to input_files
    data_model_dir = project_root / "input_files"

    extract_variable_comment_programwise(
        ladder_program, f"{dest_comment_file_name}_programwise.json", data_model_dir
    )
    extract_variable_comment_functionwise(
        ladder_program, f"{dest_comment_file_name}_functionwise.json", data_model_dir
    )

    return None


#############################

if __name__ == "__main__":
    input_ladder_file_path = r"C:\Users\Lenovo\OneDrive - OPTIMIZED SOLUTIONS LTD\Desktop\Evertything\DENSO_coding_checker\Extracted_Data_models\Rule27_32\Coding Checker_Rule27_32.xml"
    dest_comment_file_name = "comments_rule_27"
    data_model_dir = "data_model_Rule_27"
    main_program_extract_comments(
        input_ladder_file_path, dest_comment_file_name, data_model_dir
    )
