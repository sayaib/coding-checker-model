from bs4 import BeautifulSoup
from typing import *
import pandas as pd
import re, json
from ...main import logger
import os
import traceback

# Optional import for YOLO - only import if available
try:
    from ultralytics import YOLO

    YOLO_AVAILABLE = True
    # Force CPU usage for YOLO and PyTorch
    os.environ["CUDA_VISIBLE_DEVICES"] = ""
    os.environ["TORCH_DEVICE"] = "cpu"
except ImportError:
    YOLO_AVAILABLE = False
    YOLO = None

### Global directory_paths

# logger.remove()


###   Ingesting the file #############


def ingest_file(xml_file_path: str):

    with open(xml_file_path, "r", encoding="utf-8") as file:

        xml_content = file.read()

    soup = BeautifulSoup(xml_content, "lxml-xml")

    return soup


# def load_model(model_path:str):
#     if not YOLO_AVAILABLE:
#         raise ImportError("YOLO is not available. Install ultralytics package.")
#     model = YOLO(model_path)
#     model.to('cpu')  # Force CPU usage
#     print(f"✅ Model loaded on CPU: {model_path}")
#     return model

######################Sub programs #######################


def extract_from_contact(
    pg_name: str, bd_name: str, rg_name: str, rg_order: str, rg_child, section_type: str
) -> pd.DataFrame:

    logger.info(f"Contact Extraction from {pg_name} {bd_name} {rg_order}")

    try:

        program_name_list = []
        body_name_list = []

        rung_order_list = []
        rung_name_list = []

        object_list = []
        object_type_list = []
        attributes_list = []

        connection_in_list = []
        connection_out_list = []

        program_name_list.append(pg_name)
        body_name_list.append(bd_name)
        rung_order_list.append(rg_order)
        rung_name_list.append(rg_name)

        object_list.append(rg_child.name)
        object_type_list.append(rg_child.attrs["xsi:type"])

        children_attrs_keys = list(rg_child.attrs.keys())
        attribute_dict = {}

        if "operand" in children_attrs_keys:
            attribute_dict["operand"] = rg_child.attrs["operand"]

        if "edge" in children_attrs_keys:
            attribute_dict["edge"] = rg_child.attrs["edge"]

        if "negated" in children_attrs_keys:

            attribute_dict["negated"] = rg_child.attrs["negated"]
        else:
            attribute_dict["negated"] = "false"

        ####################
        in_connection_all = rg_child.find_all("Connection")

        for in_connection in in_connection_all:
            connection_in_list.append(in_connection.attrs["refConnectionPointOutId"])

        attribute_dict["in_list"] = connection_in_list

        ######################
        out_connection_all = rg_child.find_all("ConnectionPointOut")

        for out_connection in out_connection_all:
            connection_out_list.append(out_connection.attrs["connectionPointOutId"])

        attribute_dict["out_list"] = connection_out_list

        attributes_list.append(attribute_dict)

        # decide the column names accordingly

        if section_type == "program":

            program_key = "PROGRAM"
            body_key = "BODY"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        elif section_type == "function":

            program_key = "FUNCTION_BLOCK"
            body_key = "BODY_TYPE"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        contact_dict = {
            program_key: program_name_list,
            body_key: body_name_list,
            rung_key: rung_order_list,
            rung_name_key: rung_name_list,
            object_key: object_list,
            object_type_key: object_type_list,
            attribute_key: attributes_list,
        }

        contact_dataframe = pd.DataFrame(contact_dict)

    except Exception as e:

        logger.error(f"str(e), traceback.format_exc()")

    return contact_dataframe


###############


def extract_from_coil(
    pg_name: str, bd_name: str, rg_name: str, rg_order: str, rg_child, section_type: str
) -> pd.DataFrame:

    logger.info(f"Coil Extraction from {pg_name} {bd_name} {rg_order}")

    try:

        program_name_list = []
        body_name_list = []

        rung_order_list = []
        rung_name_list = []

        object_list = []
        object_type_list = []
        attributes_list = []

        connection_in_list = []
        connection_out_list = []

        program_name_list.append(pg_name)
        body_name_list.append(bd_name)
        rung_order_list.append(rg_order)
        rung_name_list.append(rg_name)

        object_list.append(rg_child.name)
        object_type_list.append(rg_child.attrs["xsi:type"])

        children_attrs_keys = list(rg_child.attrs.keys())
        attribute_dict = {}

        if "operand" in children_attrs_keys:
            attribute_dict["operand"] = rg_child.attrs["operand"]

        if "latch" in children_attrs_keys:
            attribute_dict["latch"] = rg_child.attrs["latch"]

        ####################
        in_connection_all = rg_child.find_all("Connection")

        for in_connection in in_connection_all:
            connection_in_list.append(in_connection.attrs["refConnectionPointOutId"])

        attribute_dict["in_list"] = connection_in_list

        ######################
        out_connection_all = rg_child.find_all("ConnectionPointOut")

        for out_connection in out_connection_all:
            connection_out_list.append(out_connection.attrs["connectionPointOutId"])

        attribute_dict["out_list"] = connection_out_list

        attributes_list.append(attribute_dict)

        # decide the column names accordingly

        if section_type == "program":

            program_key = "PROGRAM"
            body_key = "BODY"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        elif section_type == "function":

            program_key = "FUNCTION_BLOCK"
            body_key = "BODY_TYPE"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        coil_dict = {
            program_key: program_name_list,
            body_key: body_name_list,
            rung_key: rung_order_list,
            rung_name_key: rung_name_list,
            object_key: object_list,
            object_type_key: object_type_list,
            attribute_key: attributes_list,
        }

        coil_dataframe = pd.DataFrame(coil_dict)

    except Exception as e:

        logger.error(str(e))

    return coil_dataframe


############


def extract_from_PowerRails(
    pg_name: str, bd_name: str, rg_name: str, rg_order: str, rg_child, section_type: str
) -> pd.DataFrame:

    logger.info(f"PowerRail Extraction from {pg_name} {bd_name} {rg_order}")

    try:

        program_name_list = []
        body_name_list = []

        rung_order_list = []
        rung_name_list = []

        object_list = []
        object_type_list = []
        attributes_list = []

        connection_in_list = []
        connection_out_list = []

        program_name_list.append(pg_name)
        body_name_list.append(bd_name)
        rung_order_list.append(rg_order)
        rung_name_list.append(rg_name)

        object_list.append(rg_child.name)
        object_type_list.append(rg_child.attrs["xsi:type"])

        children_attrs_keys = list(rg_child.attrs.keys())
        attribute_dict = {}

        ####################
        in_connection_all = rg_child.find_all("Connection")

        for in_connection in in_connection_all:
            connection_in_list.append(in_connection.attrs["refConnectionPointOutId"])

        attribute_dict["in_list"] = connection_in_list

        ######################
        out_connection_all = rg_child.find_all("ConnectionPointOut")

        for out_connection in out_connection_all:
            connection_out_list.append(out_connection.attrs["connectionPointOutId"])

        attribute_dict["out_list"] = connection_out_list

        attributes_list.append(attribute_dict)

        # decide the column names accordingly

        if section_type == "program":

            program_key = "PROGRAM"
            body_key = "BODY"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        elif section_type == "function":

            program_key = "FUNCTION_BLOCK"
            body_key = "BODY_TYPE"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        rail_dict = {
            program_key: program_name_list,
            body_key: body_name_list,
            rung_key: rung_order_list,
            rung_name_key: rung_name_list,
            object_key: object_list,
            object_type_key: object_type_list,
            attribute_key: attributes_list,
        }

        rail_dataframe = pd.DataFrame(rail_dict)

    except Exception as e:

        logger.error(str(e))

    return rail_dataframe


###################################
def extract_from_Memcopy(
    pg_name: str, bd_name: str, rg_name: str, rg_order: str, rg_child, section_type: str
) -> pd.DataFrame:

    logger.info(f"Memcopy Extraction from {pg_name} {bd_name} {rg_order}")

    try:

        program_name_list = []
        body_name_list = []

        rung_order_list = []
        rung_name_list = []

        object_list = []
        object_type_list = []
        attributes_list = []

        connection_in_list = []
        connection_out_list = []

        program_name_list.append(pg_name)
        body_name_list.append(bd_name)
        rung_order_list.append(rg_order)
        rung_name_list.append(rg_name)

        object_list.append(rg_child.name)
        object_type_list.append(rg_child.attrs["xsi:type"])

        children_attrs_keys = list(rg_child.attrs.keys())
        attribute_dict = {}

        attribute_dict["typeName"] = rg_child.attrs["typeName"]

        ####################

        in_out_variables = rg_child.find_all("InOutVariable")

        for in_out_var in in_out_variables:

            in_out_var_param_name = in_out_var.attrs["parameterName"]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_order"] = [
                in_out_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_list"] = [
                in_out_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_order"] = [
                in_out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_list"] = [
                in_out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        in_variables = rg_child.find_all("InputVariable")

        for in_var in in_variables:

            in_var_param_name = in_var.attrs["parameterName"]

            attribute_dict[f"{in_var_param_name}_inVar_in_order"] = [
                in_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_var_param_name}_inVar_in_list"] = [
                in_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

        out_variables = rg_child.find_all("OutputVariable")

        for out_var in out_variables:

            out_var_param_name = out_var.attrs["parameterName"]

            attribute_dict[f"{out_var_param_name}_outVar_out_order"] = [
                out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{out_var_param_name}_outVar_out_list"] = [
                out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        attributes_list.append(attribute_dict)

        # decide the column names accordingly

        if section_type == "program":

            program_key = "PROGRAM"
            body_key = "BODY"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        elif section_type == "function":

            program_key = "FUNCTION_BLOCK"
            body_key = "BODY_TYPE"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        mem_dict = {
            program_key: program_name_list,
            body_key: body_name_list,
            rung_key: rung_order_list,
            rung_name_key: rung_name_list,
            object_key: object_list,
            object_type_key: object_type_list,
            attribute_key: attributes_list,
        }

        mem_dataframe = pd.DataFrame(mem_dict)

    except Exception as e:

        logger.error(str(e))

    return mem_dataframe


###############################


def extract_from_Clear(
    pg_name: str, bd_name: str, rg_name: str, rg_order: str, rg_child, section_type: str
) -> pd.DataFrame:

    logger.info(f"Clear Extraction from {pg_name} {bd_name} {rg_order}")

    try:

        program_name_list = []
        body_name_list = []

        rung_order_list = []
        rung_name_list = []

        object_list = []
        object_type_list = []
        attributes_list = []

        connection_in_list = []
        connection_out_list = []

        program_name_list.append(pg_name)
        body_name_list.append(bd_name)
        rung_order_list.append(rg_order)
        rung_name_list.append(rg_name)

        object_list.append(rg_child.name)
        object_type_list.append(rg_child.attrs["xsi:type"])

        children_attrs_keys = list(rg_child.attrs.keys())
        attribute_dict = {}

        attribute_dict["typeName"] = rg_child.attrs["typeName"]

        ####################

        in_out_variables = rg_child.find_all("InOutVariable")

        for in_out_var in in_out_variables:

            in_out_var_param_name = in_out_var.attrs["parameterName"]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_order"] = [
                in_out_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_list"] = [
                in_out_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_order"] = [
                in_out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_list"] = [
                in_out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        in_variables = rg_child.find_all("InputVariable")

        for in_var in in_variables:

            in_var_param_name = in_var.attrs["parameterName"]

            attribute_dict[f"{in_var_param_name}_inVar_in_order"] = [
                in_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_var_param_name}_inVar_in_list"] = [
                in_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

        out_variables = rg_child.find_all("OutputVariable")

        for out_var in out_variables:

            out_var_param_name = out_var.attrs["parameterName"]

            attribute_dict[f"{out_var_param_name}_outVar_out_order"] = [
                out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{out_var_param_name}_outVar_out_list"] = [
                out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        attributes_list.append(attribute_dict)

        # decide the column names accordingly

        if section_type == "program":

            program_key = "PROGRAM"
            body_key = "BODY"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        elif section_type == "function":

            program_key = "FUNCTION_BLOCK"
            body_key = "BODY_TYPE"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        clear_dict = {
            program_key: program_name_list,
            body_key: body_name_list,
            rung_key: rung_order_list,
            rung_name_key: rung_name_list,
            object_key: object_list,
            object_type_key: object_type_list,
            attribute_key: attributes_list,
        }

        clear_dataframe = pd.DataFrame(clear_dict)

    except Exception as e:

        logger.error(str(e))

    return clear_dataframe


#################################


def extract_from_Data_Sink_or_Source(
    pg_name: str, bd_name: str, rg_name: str, rg_order: str, rg_child, section_type: str
) -> pd.DataFrame:

    logger.info(f"Data Source Sink Extraction from {pg_name} {bd_name} {rg_order}")

    try:

        program_name_list = []
        body_name_list = []

        rung_order_list = []
        rung_name_list = []

        object_list = []
        object_type_list = []
        attributes_list = []

        connection_in_list = []
        connection_out_list = []

        program_name_list.append(pg_name)
        body_name_list.append(bd_name)
        rung_order_list.append(rg_order)
        rung_name_list.append(rg_name)

        object_list.append(rg_child.name)
        object_type_list.append(rg_child.attrs["xsi:type"])

        children_attrs_keys = list(rg_child.attrs.keys())
        attribute_dict = {}

        attribute_dict["identifier"] = rg_child.attrs["identifier"]

        ####################

        if rg_child.attrs["xsi:type"] == "DataSink":

            attribute_dict["in_list"] = [
                rg_child.find("Connection").attrs["refConnectionPointOutId"]
            ]

        if rg_child.attrs["xsi:type"] == "DataSource":

            attribute_dict["out_list"] = [
                rg_child.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        attributes_list.append(attribute_dict)

        # decide the column names accordingly

        if section_type == "program":

            program_key = "PROGRAM"
            body_key = "BODY"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        elif section_type == "function":

            program_key = "FUNCTION_BLOCK"
            body_key = "BODY_TYPE"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        sink_source_dict = {
            program_key: program_name_list,
            body_key: body_name_list,
            rung_key: rung_order_list,
            rung_name_key: rung_name_list,
            object_key: object_list,
            object_type_key: object_type_list,
            attribute_key: attributes_list,
        }

        sink_source_dataframe = pd.DataFrame(sink_source_dict)

    except Exception as e:
        logger.error(str(e))

    return sink_source_dataframe


###################################
def extract_from_Move(
    pg_name: str, bd_name: str, rg_name: str, rg_order: str, rg_child, section_type: str
) -> pd.DataFrame:

    logger.info(f"Move Extraction from {pg_name} {bd_name} {rg_order}")

    try:

        program_name_list = []
        body_name_list = []

        rung_order_list = []
        rung_name_list = []

        object_list = []
        object_type_list = []
        attributes_list = []

        connection_in_list = []
        connection_out_list = []

        program_name_list.append(pg_name)
        body_name_list.append(bd_name)
        rung_order_list.append(rg_order)
        rung_name_list.append(rg_name)

        object_list.append(rg_child.name)
        object_type_list.append(rg_child.attrs["xsi:type"])

        children_attrs_keys = list(rg_child.attrs.keys())
        attribute_dict = {}

        attribute_dict["typeName"] = rg_child.attrs["typeName"]

        ####################

        in_out_variables = rg_child.find_all("InOutVariable")

        for in_out_var in in_out_variables:

            in_out_var_param_name = in_out_var.attrs["parameterName"]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_order"] = [
                in_out_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_list"] = [
                in_out_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_order"] = [
                in_out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_list"] = [
                in_out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        in_variables = rg_child.find_all("InputVariable")

        for in_var in in_variables:

            in_var_param_name = in_var.attrs["parameterName"]

            attribute_dict[f"{in_var_param_name}_inVar_in_order"] = [
                in_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_var_param_name}_inVar_in_list"] = [
                in_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

        out_variables = rg_child.find_all("OutputVariable")

        for out_var in out_variables:

            out_var_param_name = out_var.attrs["parameterName"]

            attribute_dict[f"{out_var_param_name}_outVar_out_order"] = [
                out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{out_var_param_name}_outVar_out_list"] = [
                out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        attributes_list.append(attribute_dict)

        # decide the column names accordingly

        if section_type == "program":

            program_key = "PROGRAM"
            body_key = "BODY"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        elif section_type == "function":

            program_key = "FUNCTION_BLOCK"
            body_key = "BODY_TYPE"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        move_dict = {
            program_key: program_name_list,
            body_key: body_name_list,
            rung_key: rung_order_list,
            rung_name_key: rung_name_list,
            object_key: object_list,
            object_type_key: object_type_list,
            attribute_key: attributes_list,
        }

        move_dataframe = pd.DataFrame(move_dict)

    except Exception as e:

        logger.error(str(e))

    return move_dataframe


########################################


def extract_from_Zonecmp(
    pg_name: str, bd_name: str, rg_name: str, rg_order: str, rg_child, section_type: str
) -> pd.DataFrame:

    logger.info(f"Zonecmp Extraction from {pg_name} {bd_name} {rg_order}")

    try:

        program_name_list = []
        body_name_list = []

        rung_order_list = []
        rung_name_list = []

        object_list = []
        object_type_list = []
        attributes_list = []

        connection_in_list = []
        connection_out_list = []

        program_name_list.append(pg_name)
        body_name_list.append(bd_name)
        rung_order_list.append(rg_order)
        rung_name_list.append(rg_name)

        object_list.append(rg_child.name)
        object_type_list.append(rg_child.attrs["xsi:type"])

        children_attrs_keys = list(rg_child.attrs.keys())
        attribute_dict = {}

        attribute_dict["typeName"] = rg_child.attrs["typeName"]

        ####################

        in_out_variables = rg_child.find_all("InOutVariable")

        for in_out_var in in_out_variables:

            in_out_var_param_name = in_out_var.attrs["parameterName"]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_order"] = [
                in_out_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_list"] = [
                in_out_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_order"] = [
                in_out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_list"] = [
                in_out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        in_variables = rg_child.find_all("InputVariable")

        for in_var in in_variables:

            in_var_param_name = in_var.attrs["parameterName"]

            attribute_dict[f"{in_var_param_name}_inVar_in_order"] = [
                in_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_var_param_name}_inVar_in_list"] = [
                in_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

        out_variables = rg_child.find_all("OutputVariable")

        for out_var in out_variables:

            out_var_param_name = out_var.attrs["parameterName"]

            attribute_dict[f"{out_var_param_name}_outVar_out_order"] = [
                out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{out_var_param_name}_outVar_out_list"] = [
                out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        attributes_list.append(attribute_dict)

        # decide the column names accordingly

        if section_type == "program":

            program_key = "PROGRAM"
            body_key = "BODY"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        elif section_type == "function":

            program_key = "FUNCTION_BLOCK"
            body_key = "BODY_TYPE"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        zone_dict = {
            program_key: program_name_list,
            body_key: body_name_list,
            rung_key: rung_order_list,
            rung_name_key: rung_name_list,
            object_key: object_list,
            object_type_key: object_type_list,
            attribute_key: attributes_list,
        }

        zone_dataframe = pd.DataFrame(zone_dict)

    except Exception as e:

        logger.error(str(e))

    return zone_dataframe


#############################
def extract_from_underscore(
    pg_name: str, bd_name: str, rg_name: str, rg_order: str, rg_child, section_type: str
) -> pd.DataFrame:

    logger.info(f"Underscore Extraction from {pg_name} {bd_name} {rg_order}")

    try:

        program_name_list = []
        body_name_list = []

        rung_order_list = []
        rung_name_list = []

        object_list = []
        object_type_list = []
        attributes_list = []

        connection_in_list = []
        connection_out_list = []

        program_name_list.append(pg_name)
        body_name_list.append(bd_name)
        rung_order_list.append(rg_order)
        rung_name_list.append(rg_name)

        object_list.append(rg_child.name)
        object_type_list.append(rg_child.attrs["xsi:type"])

        children_attrs_keys = list(rg_child.attrs.keys())
        attribute_dict = {}

        attribute_dict["typeName"] = rg_child.attrs["typeName"]

        ####################

        in_out_variables = rg_child.find_all("InOutVariable")

        for in_out_var in in_out_variables:

            in_out_var_param_name = in_out_var.attrs["parameterName"]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_order"] = [
                in_out_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_list"] = [
                in_out_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_order"] = [
                in_out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_list"] = [
                in_out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        in_variables = rg_child.find_all("InputVariable")

        for in_var in in_variables:

            in_var_param_name = in_var.attrs["parameterName"]

            attribute_dict[f"{in_var_param_name}_inVar_in_order"] = [
                in_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_var_param_name}_inVar_in_list"] = [
                in_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

        out_variables = rg_child.find_all("OutputVariable")

        for out_var in out_variables:

            out_var_param_name = out_var.attrs["parameterName"]

            attribute_dict[f"{out_var_param_name}_outVar_out_order"] = [
                out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{out_var_param_name}_outVar_out_list"] = [
                out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        attributes_list.append(attribute_dict)

        # decide the column names accordingly

        if section_type == "program":

            program_key = "PROGRAM"
            body_key = "BODY"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        elif section_type == "function":

            program_key = "FUNCTION_BLOCK"
            body_key = "BODY_TYPE"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        under_dict = {
            program_key: program_name_list,
            body_key: body_name_list,
            rung_key: rung_order_list,
            rung_name_key: rung_name_list,
            object_key: object_list,
            object_type_key: object_type_list,
            attribute_key: attributes_list,
        }

        under_dataframe = pd.DataFrame(under_dict)

    except Exception as e:

        logger.error(str(e))

    return under_dataframe


##########################################3
def extract_from_fwdslash(
    pg_name: str, bd_name: str, rg_name: str, rg_order: str, rg_child, section_type: str
) -> pd.DataFrame:

    logger.info(f"// Extraction from {pg_name} {bd_name} {rg_order}")

    try:

        program_name_list = []
        body_name_list = []

        rung_order_list = []
        rung_name_list = []

        object_list = []
        object_type_list = []
        attributes_list = []

        connection_in_list = []
        connection_out_list = []

        program_name_list.append(pg_name)
        body_name_list.append(bd_name)
        rung_order_list.append(rg_order)
        rung_name_list.append(rg_name)

        object_list.append(rg_child.name)
        object_type_list.append(rg_child.attrs["xsi:type"])

        children_attrs_keys = list(rg_child.attrs.keys())
        attribute_dict = {}

        attribute_dict["typeName"] = rg_child.attrs["typeName"]

        ####################

        in_out_variables = rg_child.find_all("InOutVariable")

        for in_out_var in in_out_variables:

            in_out_var_param_name = in_out_var.attrs["parameterName"]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_order"] = [
                in_out_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_list"] = [
                in_out_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_order"] = [
                in_out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_list"] = [
                in_out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        in_variables = rg_child.find_all("InputVariable")

        for in_var in in_variables:

            in_var_param_name = in_var.attrs["parameterName"]

            attribute_dict[f"{in_var_param_name}_inVar_in_order"] = [
                in_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_var_param_name}_inVar_in_list"] = [
                in_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

        out_variables = rg_child.find_all("OutputVariable")

        for out_var in out_variables:

            out_var_param_name = out_var.attrs["parameterName"]

            attribute_dict[f"{out_var_param_name}_outVar_out_order"] = [
                out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{out_var_param_name}_outVar_out_list"] = [
                out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        attributes_list.append(attribute_dict)

        # decide the column names accordingly

        if section_type == "program":

            program_key = "PROGRAM"
            body_key = "BODY"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        elif section_type == "function":

            program_key = "FUNCTION_BLOCK"
            body_key = "BODY_TYPE"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        fwd_dict = {
            program_key: program_name_list,
            body_key: body_name_list,
            rung_key: rung_order_list,
            rung_name_key: rung_name_list,
            object_key: object_list,
            object_type_key: object_type_list,
            attribute_key: attributes_list,
        }

        fwd_dataframe = pd.DataFrame(fwd_dict)

    except Exception as e:

        logger.error(str(e))

    return fwd_dataframe


###############################
def extract_from_DINT_TO_LREAL(
    pg_name: str, bd_name: str, rg_name: str, rg_order: str, rg_child, section_type: str
) -> pd.DataFrame:

    logger.info(f"DINT TO LREAL Extraction from {pg_name} {bd_name} {rg_order}")

    try:

        program_name_list = []
        body_name_list = []

        rung_order_list = []
        rung_name_list = []

        object_list = []
        object_type_list = []
        attributes_list = []

        connection_in_list = []
        connection_out_list = []

        program_name_list.append(pg_name)
        body_name_list.append(bd_name)
        rung_order_list.append(rg_order)
        rung_name_list.append(rg_name)

        object_list.append(rg_child.name)
        object_type_list.append(rg_child.attrs["xsi:type"])

        children_attrs_keys = list(rg_child.attrs.keys())
        attribute_dict = {}

        attribute_dict["typeName"] = rg_child.attrs["typeName"]

        ####################

        in_out_variables = rg_child.find_all("InOutVariable")

        for in_out_var in in_out_variables:

            in_out_var_param_name = in_out_var.attrs["parameterName"]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_order"] = [
                in_out_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_list"] = [
                in_out_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_order"] = [
                in_out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_list"] = [
                in_out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        in_variables = rg_child.find_all("InputVariable")

        for in_var in in_variables:

            in_var_param_name = in_var.attrs["parameterName"]

            attribute_dict[f"{in_var_param_name}_inVar_in_order"] = [
                in_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_var_param_name}_inVar_in_list"] = [
                in_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

        out_variables = rg_child.find_all("OutputVariable")

        for out_var in out_variables:

            out_var_param_name = out_var.attrs["parameterName"]

            attribute_dict[f"{out_var_param_name}_outVar_out_order"] = [
                out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{out_var_param_name}_outVar_out_list"] = [
                out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        attributes_list.append(attribute_dict)

        # decide the column names accordingly

        if section_type == "program":

            program_key = "PROGRAM"
            body_key = "BODY"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        elif section_type == "function":

            program_key = "FUNCTION_BLOCK"
            body_key = "BODY_TYPE"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        dint_dict = {
            program_key: program_name_list,
            body_key: body_name_list,
            rung_key: rung_order_list,
            rung_name_key: rung_name_list,
            object_key: object_list,
            object_type_key: object_type_list,
            attribute_key: attributes_list,
        }

        dint_dataframe = pd.DataFrame(dint_dict)

    except Exception as e:

        logger.error(str(e))

    return dint_dataframe


#####################################################
def extract_from_EN_block(
    pg_name: str, bd_name: str, rg_name: str, rg_order: str, rg_child, section_type: str
) -> pd.DataFrame:

    logger.info(f"EN block Extraction from {pg_name} {bd_name} {rg_order}")

    try:

        program_name_list = []
        body_name_list = []

        rung_order_list = []
        rung_name_list = []

        object_list = []
        object_type_list = []
        attributes_list = []

        connection_in_list = []
        connection_out_list = []

        program_name_list.append(pg_name)
        body_name_list.append(bd_name)
        rung_order_list.append(rg_order)
        rung_name_list.append(rg_name)

        object_list.append(rg_child.name)
        object_type_list.append(rg_child.attrs["xsi:type"])

        children_attrs_keys = list(rg_child.attrs.keys())
        attribute_dict = {}

        attribute_dict["typeName"] = rg_child.attrs["typeName"]

        ####################

        in_out_variables = rg_child.find_all("InOutVariable")

        for in_out_var in in_out_variables:

            in_out_var_param_name = in_out_var.attrs["parameterName"]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_order"] = [
                in_out_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_list"] = [
                in_out_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_order"] = [
                in_out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_list"] = [
                in_out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        in_variables = rg_child.find_all("InputVariable")

        for in_var in in_variables:

            in_var_param_name = in_var.attrs["parameterName"]

            attribute_dict[f"{in_var_param_name}_inVar_in_order"] = [
                in_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_var_param_name}_inVar_in_list"] = [
                in_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

        out_variables = rg_child.find_all("OutputVariable")

        for out_var in out_variables:

            out_var_param_name = out_var.attrs["parameterName"]

            attribute_dict[f"{out_var_param_name}_outVar_out_order"] = [
                out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{out_var_param_name}_outVar_out_list"] = [
                out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        attributes_list.append(attribute_dict)

        # decide the column names accordingly

        if section_type == "program":

            program_key = "PROGRAM"
            body_key = "BODY"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        elif section_type == "function":

            program_key = "FUNCTION_BLOCK"
            body_key = "BODY_TYPE"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        EN_dict = {
            program_key: program_name_list,
            body_key: body_name_list,
            rung_key: rung_order_list,
            rung_name_key: rung_name_list,
            object_key: object_list,
            object_type_key: object_type_list,
            attribute_key: attributes_list,
        }

        EN_dataframe = pd.DataFrame(EN_dict)

    except Exception as e:

        logger.error(str(e))

    return EN_dataframe


#################### Compute ADD, SUB, MUL, DIV blocks ####################
def extract_from_COMP_block(
    pg_name: str, bd_name: str, rg_name: str, rg_order: str, rg_child, section_type: str
) -> pd.DataFrame:

    logger.info(f"COMPUTE block Extraction from {pg_name} {bd_name} {rg_order}")

    try:

        program_name_list = []
        body_name_list = []

        rung_order_list = []
        rung_name_list = []

        object_list = []
        object_type_list = []
        attributes_list = []

        connection_in_list = []
        connection_out_list = []

        program_name_list.append(pg_name)
        body_name_list.append(bd_name)
        rung_order_list.append(rg_order)
        rung_name_list.append(rg_name)

        object_list.append(rg_child.name)
        object_type_list.append(rg_child.attrs["xsi:type"])

        children_attrs_keys = list(rg_child.attrs.keys())
        attribute_dict = {}

        attribute_dict["typeName"] = rg_child.attrs["typeName"]

        ####################

        in_out_variables = rg_child.find_all("InOutVariable")

        for in_out_var in in_out_variables:

            in_out_var_param_name = in_out_var.attrs["parameterName"]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_order"] = [
                in_out_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_list"] = [
                in_out_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_order"] = [
                in_out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_list"] = [
                in_out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        in_variables = rg_child.find_all("InputVariable")

        for in_var in in_variables:

            in_var_param_name = in_var.attrs["parameterName"]

            attribute_dict[f"{in_var_param_name}_inVar_in_order"] = [
                in_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_var_param_name}_inVar_in_list"] = [
                in_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

        out_variables = rg_child.find_all("OutputVariable")

        for out_var in out_variables:

            out_var_param_name = out_var.attrs["parameterName"]

            attribute_dict[f"{out_var_param_name}_outVar_out_order"] = [
                out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{out_var_param_name}_outVar_out_list"] = [
                out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        attributes_list.append(attribute_dict)

        # decide the column names accordingly

        if section_type == "program":

            program_key = "PROGRAM"
            body_key = "BODY"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        elif section_type == "function":

            program_key = "FUNCTION_BLOCK"
            body_key = "BODY_TYPE"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        COMP_dict = {
            program_key: program_name_list,
            body_key: body_name_list,
            rung_key: rung_order_list,
            rung_name_key: rung_name_list,
            object_key: object_list,
            object_type_key: object_type_list,
            attribute_key: attributes_list,
        }

        COMP_dataframe = pd.DataFrame(COMP_dict)

    except Exception as e:

        logger.error(str(e))

    return COMP_dataframe


###################### Extract from Increment and Decrement Block #######################
def extract_from_INC_DNC_block(
    pg_name: str, bd_name: str, rg_name: str, rg_order: str, rg_child, section_type: str
) -> pd.DataFrame:

    logger.info(
        f"INCREMENT DECREMENT block Extraction from {pg_name} {bd_name} {rg_order}"
    )

    try:

        program_name_list = []
        body_name_list = []

        rung_order_list = []
        rung_name_list = []

        object_list = []
        object_type_list = []
        attributes_list = []

        connection_in_list = []
        connection_out_list = []

        program_name_list.append(pg_name)
        body_name_list.append(bd_name)
        rung_order_list.append(rg_order)
        rung_name_list.append(rg_name)

        object_list.append(rg_child.name)
        object_type_list.append(rg_child.attrs["xsi:type"])

        children_attrs_keys = list(rg_child.attrs.keys())
        attribute_dict = {}

        attribute_dict["typeName"] = rg_child.attrs["typeName"]

        ####################

        in_out_variables = rg_child.find_all("InOutVariable")

        for in_out_var in in_out_variables:

            in_out_var_param_name = in_out_var.attrs["parameterName"]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_order"] = [
                in_out_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_list"] = [
                in_out_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_order"] = [
                in_out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_list"] = [
                in_out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        in_variables = rg_child.find_all("InputVariable")

        for in_var in in_variables:

            in_var_param_name = in_var.attrs["parameterName"]

            attribute_dict[f"{in_var_param_name}_inVar_in_order"] = [
                in_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_var_param_name}_inVar_in_list"] = [
                in_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

        out_variables = rg_child.find_all("OutputVariable")

        for out_var in out_variables:

            out_var_param_name = out_var.attrs["parameterName"]

            attribute_dict[f"{out_var_param_name}_outVar_out_order"] = [
                out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{out_var_param_name}_outVar_out_list"] = [
                out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        attributes_list.append(attribute_dict)

        # decide the column names accordingly

        if section_type == "program":

            program_key = "PROGRAM"
            body_key = "BODY"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        elif section_type == "function":

            program_key = "FUNCTION_BLOCK"
            body_key = "BODY_TYPE"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        inc_dnc_dict = {
            program_key: program_name_list,
            body_key: body_name_list,
            rung_key: rung_order_list,
            rung_name_key: rung_name_list,
            object_key: object_list,
            object_type_key: object_type_list,
            attribute_key: attributes_list,
        }

        inc_dnc_dataframe = pd.DataFrame(inc_dnc_dict)

    except Exception as e:

        logger.error(str(e))

    return inc_dnc_dataframe


###########################################Trigon Math Functions #####################
def extract_from_TRIGON_block(
    pg_name: str, bd_name: str, rg_name: str, rg_order: str, rg_child, section_type: str
) -> pd.DataFrame:

    logger.info(f"TRIGONOMETRY block Extraction from {pg_name} {bd_name} {rg_order}")

    try:

        program_name_list = []
        body_name_list = []

        rung_order_list = []
        rung_name_list = []

        object_list = []
        object_type_list = []
        attributes_list = []

        connection_in_list = []
        connection_out_list = []

        program_name_list.append(pg_name)
        body_name_list.append(bd_name)
        rung_order_list.append(rg_order)
        rung_name_list.append(rg_name)

        object_list.append(rg_child.name)
        object_type_list.append(rg_child.attrs["xsi:type"])

        children_attrs_keys = list(rg_child.attrs.keys())
        attribute_dict = {}

        attribute_dict["typeName"] = rg_child.attrs["typeName"]

        ####################

        in_out_variables = rg_child.find_all("InOutVariable")

        for in_out_var in in_out_variables:

            in_out_var_param_name = in_out_var.attrs["parameterName"]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_order"] = [
                in_out_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_list"] = [
                in_out_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_order"] = [
                in_out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_list"] = [
                in_out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        in_variables = rg_child.find_all("InputVariable")

        for in_var in in_variables:

            in_var_param_name = in_var.attrs["parameterName"]

            attribute_dict[f"{in_var_param_name}_inVar_in_order"] = [
                in_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_var_param_name}_inVar_in_list"] = [
                in_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

        out_variables = rg_child.find_all("OutputVariable")

        for out_var in out_variables:

            out_var_param_name = out_var.attrs["parameterName"]

            attribute_dict[f"{out_var_param_name}_outVar_out_order"] = [
                out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{out_var_param_name}_outVar_out_list"] = [
                out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        attributes_list.append(attribute_dict)

        # decide the column names accordingly

        if section_type == "program":

            program_key = "PROGRAM"
            body_key = "BODY"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        elif section_type == "function":

            program_key = "FUNCTION_BLOCK"
            body_key = "BODY_TYPE"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        trigon_dict = {
            program_key: program_name_list,
            body_key: body_name_list,
            rung_key: rung_order_list,
            rung_name_key: rung_name_list,
            object_key: object_list,
            object_type_key: object_type_list,
            attribute_key: attributes_list,
        }

        trigon_dataframe = pd.DataFrame(trigon_dict)

    except Exception as e:

        logger.error(str(e))

    return trigon_dataframe


####################### Miscellaneous blocks #########################
def extract_from_misc_block(
    pg_name: str, bd_name: str, rg_name: str, rg_order: str, rg_child, section_type: str
) -> pd.DataFrame:

    logger.info(f"Miscellanous blocks  Extraction from {pg_name} {bd_name} {rg_order}")

    try:

        program_name_list = []
        body_name_list = []

        rung_order_list = []
        rung_name_list = []

        object_list = []
        object_type_list = []
        attributes_list = []

        connection_in_list = []
        connection_out_list = []

        program_name_list.append(pg_name)
        body_name_list.append(bd_name)
        rung_order_list.append(rg_order)
        rung_name_list.append(rg_name)

        object_list.append(rg_child.name)
        object_type_list.append(rg_child.attrs["xsi:type"])

        children_attrs_keys = list(rg_child.attrs.keys())
        attribute_dict = {}

        attribute_dict["typeName"] = rg_child.attrs["typeName"]

        ####################

        in_out_variables = rg_child.find_all("InOutVariable")

        for in_out_var in in_out_variables:

            in_out_var_param_name = in_out_var.attrs["parameterName"]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_order"] = [
                in_out_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_list"] = [
                in_out_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_order"] = [
                in_out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_list"] = [
                in_out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        in_variables = rg_child.find_all("InputVariable")

        for in_var in in_variables:

            in_var_param_name = in_var.attrs["parameterName"]

            attribute_dict[f"{in_var_param_name}_inVar_in_order"] = [
                in_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_var_param_name}_inVar_in_list"] = [
                in_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

        out_variables = rg_child.find_all("OutputVariable")

        for out_var in out_variables:

            out_var_param_name = out_var.attrs["parameterName"]

            attribute_dict[f"{out_var_param_name}_outVar_out_order"] = [
                out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{out_var_param_name}_outVar_out_list"] = [
                out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        attributes_list.append(attribute_dict)

        # decide the column names accordingly

        if section_type == "program":

            program_key = "PROGRAM"
            body_key = "BODY"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        elif section_type == "function":

            program_key = "FUNCTION_BLOCK"
            body_key = "BODY_TYPE"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        misc_dict = {
            program_key: program_name_list,
            body_key: body_name_list,
            rung_key: rung_order_list,
            rung_name_key: rung_name_list,
            object_key: object_list,
            object_type_key: object_type_list,
            attribute_key: attributes_list,
        }

        misc_dataframe = pd.DataFrame(misc_dict)

    except Exception as e:

        logger.error(str(e))

    return misc_dataframe


#########################################


def extract_from_FLOW_CONTROL(
    pg_name: str, bd_name: str, rg_name: str, rg_order: str, rg_child, section_type: str
) -> pd.DataFrame:

    logger.info(f"FLOW CONTROL Extraction from {pg_name} {bd_name} {rg_order}")

    try:

        program_name_list = []
        body_name_list = []

        rung_order_list = []
        rung_name_list = []

        object_list = []
        object_type_list = []
        attributes_list = []

        connection_in_list = []
        connection_out_list = []

        program_name_list.append(pg_name)
        body_name_list.append(bd_name)
        rung_order_list.append(rg_order)
        rung_name_list.append(rg_name)

        object_list.append(rg_child.name)
        object_type_list.append(rg_child.attrs["xsi:type"])

        children_attrs_keys = list(rg_child.attrs.keys())
        attribute_dict = {}

        attribute_dict["typeName"] = rg_child.attrs["typeName"]
        attribute_dict["instanceName"] = rg_child.attrs["instanceName"]

        ####################

        in_out_variables = rg_child.find_all("InOutVariable")

        for in_out_var in in_out_variables:

            in_out_var_param_name = in_out_var.attrs["parameterName"]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_order"] = [
                in_out_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_in_list"] = [
                in_out_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_order"] = [
                in_out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{in_out_var_param_name}_inoutVar_out_list"] = [
                in_out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        in_variables = rg_child.find_all("InputVariable")

        for in_var in in_variables:

            in_var_param_name = in_var.attrs["parameterName"]

            attribute_dict[f"{in_var_param_name}_inVar_in_order"] = [
                in_var.find("ConnectionPointInOrder").attrs["order"]
            ]

            attribute_dict[f"{in_var_param_name}_inVar_in_list"] = [
                in_var.find("Connection").attrs["refConnectionPointOutId"]
            ]

        out_variables = rg_child.find_all("OutputVariable")

        for out_var in out_variables:

            out_var_param_name = out_var.attrs["parameterName"]

            attribute_dict[f"{out_var_param_name}_outVar_out_order"] = [
                out_var.find("ConnectionPointOutOrder").attrs["order"]
            ]

            attribute_dict[f"{out_var_param_name}_outVar_out_list"] = [
                out_var.find("ConnectionPointOut").attrs["connectionPointOutId"]
            ]

        attributes_list.append(attribute_dict)

        # decide the column names accordingly

        if section_type == "program":

            program_key = "PROGRAM"
            body_key = "BODY"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        elif section_type == "function":

            program_key = "FUNCTION_BLOCK"
            body_key = "BODY_TYPE"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        FLOW_dict = {
            program_key: program_name_list,
            body_key: body_name_list,
            rung_key: rung_order_list,
            rung_name_key: rung_name_list,
            object_key: object_list,
            object_type_key: object_type_list,
            attribute_key: attributes_list,
        }

        FLOW_dataframe = pd.DataFrame(FLOW_dict)

    except Exception as e:
        logger.error(str(e))

    return FLOW_dataframe


###############################
def extract_in_line_block_data(
    pg_name, bd_name, rg_name, rg_order, rg_child, section_type="program"
):

    try:

        program_name_list = []
        body_name_list = []

        rung_order_list = []
        rung_name_list = []

        object_list = []
        object_type_list = []
        attributes_list = []

        program_name_list.append(pg_name)
        body_name_list.append(bd_name)
        rung_order_list.append(rg_order)
        rung_name_list.append(rg_name)
        object_list.append("data_block")

        object_type_list.append(rg_child.attrs["xsi:type"])

        attribute_dict = {}

        ########################

        comments_list = []
        data_inputs_list = []

        st_tag = rg_child.find("smcext:ST").find("ST")

        # Output the result

        for line in st_tag.text.strip().splitlines():

            if line.strip():

                if re.search(r"^/s*//.*", line):

                    comments_list.append(line)
                if re.search(r":=", line):

                    data_inputs_list.append(line)

        attribute_dict["comments"] = comments_list
        attribute_dict["data_inputs"] = data_inputs_list
        attributes_list.append(attribute_dict)

        # #decide the column names accordingly

        if section_type == "program":

            program_key = "PROGRAM"
            body_key = "BODY"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        elif section_type == "function":

            program_key = "FUNCTION_BLOCK"
            body_key = "BODY_TYPE"
            rung_key = "RUNG"
            rung_name_key = "RUNG_NAME"
            object_key = "OBJECT"
            object_type_key = "OBJECT_TYPE_LIST"
            attribute_key = "ATTRIBUTES"

        block_dict = {
            program_key: program_name_list,
            body_key: body_name_list,
            rung_key: rung_order_list,
            rung_name_key: rung_name_list,
            object_key: object_list,
            object_type_key: object_type_list,
            attribute_key: attributes_list,
        }

        block_dataframe = pd.DataFrame(block_dict)

    except Exception as e:

        logger.error(f"str(e), traceback.format_exc()")

    return block_dataframe


##############################


def data_modelling_program_wise(
    ladder_program, data_model_dir: str, dest_file_name: str
) -> None:

    logger.info("Extracting objects Programwise")

    try:

        program_names = [
            program["name"] for program in ladder_program.find_all("Program")
        ]

        dest_file_dict = {
            "PROGRAM": [],
            "BODY": [],
            "RUNG": [],
            "RUNG_NAME": [],
            "OBJECT": [],
            "OBJECT_TYPE_LIST": [],
            "ATTRIBUTES": [],
        }
        dest_file_df = pd.DataFrame(dest_file_dict)

        data_source_df = pd.DataFrame(
            {
                "PROGRAM": [],
                "BODY": [],
                "RUNG": [],
                "OBJECT_TYPE_LIST": [],
                "ATTRIBUTES": [],
            }
        )

        program_name_list = []
        body_name_list = []
        rung_order_list = []
        rung_name_list = []
        object_list = []
        object_type_list = []
        attributes_list = []

        connection_in_list = []
        connection_out_list = []

        for pg_name in program_names:

            program = ladder_program.find("Program", {"name": pg_name})
            body_names = [body["name"] for body in program.find_all("BodyContent")]

            for bd_name in body_names:
                body = program.find("BodyContent", {"name": bd_name})
                rung_orders = [
                    rung["evaluationOrder"] for rung in body.find_all("Rung")
                ]

                for rg_order in rung_orders:

                    rung = body.find("Rung", {"evaluationOrder": rg_order})
                    rung_children = [child for child in rung.find_all(recursive=False)]

                    rg_name_child_tag = rung.find("CommonObject")

                    if rg_name_child_tag:

                        rg_name = rg_name_child_tag.text

                        # Store the relevant data in separate file, this is to extract the comments for the data source and sinks
                        content_text = rg_name_child_tag.find("Content")
                        if content_text:
                            sub_data_source_df = pd.DataFrame(
                                {
                                    "PROGRAM": [pg_name],
                                    "BODY": [bd_name],
                                    "RUNG": [rg_order],
                                    "OBJECT_TYPE_LIST": ["Data Source/Sink Comments"],
                                    "ATTRIBUTES": [content_text.text.strip()],
                                }
                            )
                            data_source_df = pd.concat(
                                [data_source_df, sub_data_source_df], axis=0
                            )

                    else:
                        rg_name = "NONE"

                    for rg_child in rung_children:

                        if rg_child.attrs["xsi:type"] == "Contact":

                            contact_df = extract_from_contact(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, contact_df], axis=0)

                        elif rg_child.attrs["xsi:type"] == "Coil":

                            coil_df = extract_from_coil(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, coil_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "LeftPowerRail") or (
                            rg_child.attrs["xsi:type"] == "RightPowerRail"
                        ):

                            rail_df = extract_from_PowerRails(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, rail_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            rg_child.attrs["typeName"] == "MemCopy"
                        ):

                            mem_df = extract_from_Memcopy(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, mem_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            rg_child.attrs["typeName"] == "Clear"
                        ):

                            clear_df = extract_from_Clear(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, clear_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            (rg_child.attrs["typeName"] == "MOVE")
                            or (rg_child.attrs["typeName"] == "@MOVE")
                        ):

                            move_df = extract_from_Move(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, move_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            rg_child.attrs["typeName"] == "ZoneCmp"
                        ):

                            zone_df = extract_from_Zonecmp(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, zone_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            (rg_child.attrs["typeName"] == "-")
                            or (rg_child.attrs["typeName"] == "+")
                            or (rg_child.attrs["typeName"] == "*")
                            or (rg_child.attrs["typeName"] == "**")
                        ):

                            under_df = extract_from_underscore(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, under_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            rg_child.attrs["typeName"] == "/"
                        ):

                            fwd_df = extract_from_fwdslash(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, fwd_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            re.search(r"_TO_", rg_child.attrs["typeName"])
                        ):

                            #   (rg_child.attrs['typeName']=="DINT_TO_LREAL")):

                            dint_df = extract_from_DINT_TO_LREAL(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, dint_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            (rg_child.attrs["typeName"] == "=")
                            or (rg_child.attrs["typeName"] == "<")
                            or (rg_child.attrs["typeName"] == ">")
                            or (rg_child.attrs["typeName"] == "<>")
                            or (rg_child.attrs["typeName"] == "<=")
                            or (rg_child.attrs["typeName"] == ">=")
                        ):

                            EN_df = extract_from_EN_block(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, EN_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            (rg_child.attrs["typeName"] == "ADD")
                            or (rg_child.attrs["typeName"] == "SUB")
                            or (rg_child.attrs["typeName"] == "MUL")
                            or (rg_child.attrs["typeName"] == "DIV")
                            or (rg_child.attrs["typeName"] == "MOD")
                        ):

                            COMP_df = extract_from_COMP_block(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, COMP_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            (rg_child.attrs["typeName"] == "Inc")
                            or (rg_child.attrs["typeName"] == "Dec")
                        ):

                            inc_dnc_df = extract_from_INC_DNC_block(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, inc_dnc_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            (rg_child.attrs["typeName"] == "SQRT")
                            or (rg_child.attrs["typeName"] == "LN")
                            or (rg_child.attrs["typeName"] == "EXP")
                            or (rg_child.attrs["typeName"] == "EXPT")
                            or (rg_child.attrs["typeName"] == "LOG")
                            or (rg_child.attrs["typeName"] == "DegToRad")
                            or (rg_child.attrs["typeName"] == "RadToDeg")
                            or (rg_child.attrs["typeName"] == "ABS")
                            or (rg_child.attrs["typeName"] == "SIN")
                            or (rg_child.attrs["typeName"] == "ASIN")
                            or (rg_child.attrs["typeName"] == "COS")
                            or (rg_child.attrs["typeName"] == "ACOS")
                            or (rg_child.attrs["typeName"] == "TAN")
                            or (rg_child.attrs["typeName"] == "ATAN")
                        ):

                            trigon_df = extract_from_TRIGON_block(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, trigon_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            rg_child.attrs["typeName"] == "TestABitN"
                        ):

                            misc_df = extract_from_misc_block(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, misc_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            rg_child.attrs["typeName"] == "FlowControlDataJudge_ZDS"
                        ):

                            flow_df = extract_from_FLOW_CONTROL(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, flow_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            rg_child.attrs["typeName"] == "FlowControlDataWrite_ZFC"
                        ):

                            flow_df = extract_from_FLOW_CONTROL(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, flow_df], axis=0)

                        # The FloW control block is reusable for TOn block too, For now we will reuse Flow Control block sub module only
                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            rg_child.attrs["typeName"] == "TON"
                        ):

                            TON_df = extract_from_FLOW_CONTROL(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, TON_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            rg_child.attrs["typeName"] == "CTD"
                        ):

                            CTD_df = extract_from_FLOW_CONTROL(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, CTD_df], axis=0)

                        elif rg_child.attrs["xsi:type"] == "smcext:InlineST":

                            block_df = extract_in_line_block_data(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, block_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "DataSink") or (
                            rg_child.attrs["xsi:type"] == "DataSource"
                        ):

                            SS_df = extract_from_Data_Sink_or_Source(
                                pg_name,
                                bd_name,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="program",
                            )
                            dest_file_df = pd.concat([dest_file_df, SS_df], axis=0)

                        else:

                            program_name_list = []
                            body_name_list = []
                            rung_order_list = []
                            rung_name_list = []
                            object_list = []
                            object_type_list = []
                            attributes_list = []

                            program_name_list.append(pg_name)
                            body_name_list.append(bd_name)

                            rung_order_list.append(rg_order)
                            rung_name_list.append(rg_name)

                            object_list.append(rg_child.name)
                            object_type_list.append(rg_child.attrs["xsi:type"])

                            children_attrs_keys = list(rg_child.attrs.keys())

                            attribute_dict = {}

                            attributes_list.append(attribute_dict)

                            ladder_dict = {
                                "PROGRAM": program_name_list,
                                "BODY": body_name_list,
                                "RUNG": rung_order_list,
                                "RUNG_NAME": rung_name_list,
                                "OBJECT": object_list,
                                "OBJECT_TYPE_LIST": object_type_list,
                                "ATTRIBUTES": attributes_list,
                            }
                            ladder_dataframe = pd.DataFrame(ladder_dict)

                            dest_file_df = pd.concat(
                                [dest_file_df, ladder_dataframe], axis=0
                            )

        data_source_file_name = (
            f"{dest_file_name.split("_")[0]}_datasource_comments_programwise.csv"
        )

        dest_file_df.to_csv(
            f"{data_model_dir}/{dest_file_name}", index=False, encoding="utf-8-sig"
        )
        data_source_df.to_csv(
            f"{data_model_dir}/{data_source_file_name}",
            index=False,
            encoding="utf-8-sig",
        )

        return program_names
    except Exception as e:
        logger.error(str(e))

    return []


# Execute the program


######################## Extraction from Function Blocks #################################


def data_modelling_function_wise(
    ladder_program, data_model_dir: str, dest_file_name: str
) -> None:

    logger.info("Extracting objects Functionwise")

    try:

        function_blocks = [
            function_["name"] for function_ in ladder_program.find_all("FunctionBlock")
        ]

        dest_file_dict = {
            "FUNCTION_BLOCK": [],
            "BODY_TYPE": [],
            "RUNG": [],
            "RUNG_NAME": [],
            "OBJECT": [],
            "OBJECT_TYPE_LIST": [],
            "ATTRIBUTES": [],
        }
        dest_file_df = pd.DataFrame(dest_file_dict)

        data_source_df = pd.DataFrame(
            {
                "FUNCTION_BLOCK": [],
                "BODY_TYPE": [],
                "RUNG": [],
                "OBJECT_TYPE_LIST": [],
                "ATTRIBUTES": [],
            }
        )

        function_block_list = []
        body_type_list = []
        rung_order_list = []
        rung_name_list = []
        object_list = []
        object_type_list = []
        attributes_list = []

        connection_in_list = []
        connection_out_list = []

        for function_name in function_blocks:

            function_block = ladder_program.find(
                "FunctionBlock", {"name": function_name}
            )
            body_units = function_block.find_all("BodyContent")

            for body_unit in body_units:

                rung_orders = [
                    rung["evaluationOrder"] for rung in body_unit.find_all("Rung")
                ]
                body_type = body_unit.attrs["xsi:type"]

                for rg_order in rung_orders:

                    rung = body_unit.find("Rung", {"evaluationOrder": rg_order})
                    rung_children = [child for child in rung.find_all(recursive=False)]

                    rg_name_child_tag = rung.find("CommonObject")

                    if rg_name_child_tag:

                        rg_name = rg_name_child_tag.text

                        rg_name = rg_name_child_tag.text

                        # Store the relevant data in separate file, this is to extract the comments for the data source and sinks
                        content_text = rg_name_child_tag.find("Content")
                        if content_text:
                            sub_data_source_df = pd.DataFrame(
                                {
                                    "FUNCTION_BLOCK": [function_name],
                                    "BODY_TYPE": [body_type],
                                    "RUNG": [rg_order],
                                    "OBJECT_TYPE_LIST": ["Data Source/Sink Comments"],
                                    "ATTRIBUTES": [content_text.text.strip()],
                                }
                            )
                            data_source_df = pd.concat(
                                [data_source_df, sub_data_source_df], axis=0
                            )

                    else:
                        rg_name = "NONE"

                    for rg_child in rung_children:

                        if rg_child.attrs["xsi:type"] == "Contact":

                            contact_df = extract_from_contact(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, contact_df], axis=0)

                        elif rg_child.attrs["xsi:type"] == "Coil":

                            coil_df = extract_from_coil(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, coil_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "LeftPowerRail") or (
                            rg_child.attrs["xsi:type"] == "RightPowerRail"
                        ):

                            rail_df = extract_from_PowerRails(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, rail_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            rg_child.attrs["typeName"] == "MemCopy"
                        ):

                            mem_df = extract_from_Memcopy(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, mem_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            rg_child.attrs["typeName"] == "Clear"
                        ):

                            clear_df = extract_from_Clear(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, clear_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            (rg_child.attrs["typeName"] == "MOVE")
                            or (rg_child.attrs["typeName"] == "@MOVE")
                        ):

                            move_df = extract_from_Move(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, move_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            rg_child.attrs["typeName"] == "ZoneCmp"
                        ):

                            zone_df = extract_from_Zonecmp(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, zone_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            (rg_child.attrs["typeName"] == "-")
                            or (rg_child.attrs["typeName"] == "*")
                            or (rg_child.attrs["typeName"] == "**")
                            or (rg_child.attrs["typeName"] == "+")
                        ):

                            under_df = extract_from_underscore(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, under_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            rg_child.attrs["typeName"] == "/"
                        ):

                            fwd_df = extract_from_fwdslash(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, fwd_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            re.search(r"_TO_", rg_child.attrs["typeName"])
                        ):

                            dint_df = extract_from_DINT_TO_LREAL(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, dint_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            (rg_child.attrs["typeName"] == "=")
                            or (rg_child.attrs["typeName"] == "<")
                            or (rg_child.attrs["typeName"] == ">")
                            or (rg_child.attrs["typeName"] == "<>")
                            or (rg_child.attrs["typeName"] == "<=")
                            or (rg_child.attrs["typeName"] == "=>")
                        ):

                            EN_df = extract_from_EN_block(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, EN_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            (rg_child.attrs["typeName"] == "ADD")
                            or (rg_child.attrs["typeName"] == "SUB")
                            or (rg_child.attrs["typeName"] == "MUL")
                            or (rg_child.attrs["typeName"] == "DIV")
                            or (rg_child.attrs["typeName"] == "MOD")
                        ):

                            COMP_df = extract_from_COMP_block(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, COMP_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            (rg_child.attrs["typeName"] == "Inc")
                            or (rg_child.attrs["typeName"] == "Dec")
                        ):

                            inc_dnc_df = extract_from_INC_DNC_block(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, inc_dnc_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            (rg_child.attrs["typeName"] == "SQRT")
                            or (rg_child.attrs["typeName"] == "LN")
                            or (rg_child.attrs["typeName"] == "EXP")
                            or (rg_child.attrs["typeName"] == "EXPT")
                            or (rg_child.attrs["typeName"] == "LOG")
                            or (rg_child.attrs["typeName"] == "DegToRad")
                            or (rg_child.attrs["typeName"] == "RadToDeg")
                            or (rg_child.attrs["typeName"] == "ABS")
                            or (rg_child.attrs["typeName"] == "SIN")
                            or (rg_child.attrs["typeName"] == "ASIN")
                            or (rg_child.attrs["typeName"] == "COS")
                            or (rg_child.attrs["typeName"] == "ACOS")
                            or (rg_child.attrs["typeName"] == "TAN")
                            or (rg_child.attrs["typeName"] == "ATAN")
                        ):

                            trigon_df = extract_from_TRIGON_block(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, trigon_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            rg_child.attrs["typeName"] == "TestABitN"
                        ):

                            misc_df = extract_from_misc_block(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, misc_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            rg_child.attrs["typeName"] == "FlowControlDataJudge_ZDS"
                        ):

                            flow_df = extract_from_FLOW_CONTROL(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, flow_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            rg_child.attrs["typeName"] == "FlowControlDataWrite_ZFC"
                        ):

                            flow_df = extract_from_FLOW_CONTROL(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, flow_df], axis=0)

                        # Reusing the Flow Control sub module for tON too.
                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            rg_child.attrs["typeName"] == "TON"
                        ):

                            TON_df = extract_from_FLOW_CONTROL(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, TON_df], axis=0)

                        elif rg_child.attrs["xsi:type"] == "smcext:InlineST":

                            block_df = extract_in_line_block_data(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, block_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "Block") and (
                            rg_child.attrs["typeName"] == "CTD"
                        ):

                            CTD_df = extract_from_FLOW_CONTROL(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, CTD_df], axis=0)

                        elif (rg_child.attrs["xsi:type"] == "DataSink") or (
                            rg_child.attrs["xsi:type"] == "DataSource"
                        ):

                            SS_df = extract_from_Data_Sink_or_Source(
                                function_name,
                                body_type,
                                rg_name,
                                rg_order,
                                rg_child,
                                section_type="function",
                            )
                            dest_file_df = pd.concat([dest_file_df, SS_df], axis=0)

                        else:

                            function_block_list = []
                            body_type_list = []
                            rung_order_list = []
                            rung_name_list = []
                            object_list = []
                            object_type_list = []
                            attributes_list = []

                            function_block_list.append(function_name)
                            body_type_list.append(body_type)

                            rung_order_list.append(rg_order)
                            rung_name_list.append(rg_name)

                            object_list.append(rg_child.name)
                            object_type_list.append(rg_child.attrs["xsi:type"])

                            children_attrs_keys = list(rg_child.attrs.keys())

                            attribute_dict = {}

                            attributes_list.append(attribute_dict)

                            ladder_dict = {
                                "FUNCTION_BLOCK": function_block_list,
                                "BODY_TYPE": body_type_list,
                                "RUNG": rung_order_list,
                                "RUNG_NAME": rung_name_list,
                                "OBJECT": object_list,
                                "OBJECT_TYPE_LIST": object_type_list,
                                "ATTRIBUTES": attributes_list,
                            }
                            ladder_dataframe = pd.DataFrame(ladder_dict)

                            dest_file_df = pd.concat(
                                [dest_file_df, ladder_dataframe], axis=0
                            )

        # data_source_file_name = dest_file_name.split("_")[0] + '_datasource_comments_functionwise.csv'
        data_source_file_name = (
            f"{dest_file_name.split("_")[0]}_datasource_comments_functionwise.csv"
        )

        dest_file_df.to_csv(
            f"{data_model_dir}/{dest_file_name}", index=False, encoding="utf-8-sig"
        )
        data_source_df.to_csv(
            f"{data_model_dir}/{data_source_file_name}",
            index=False,
            encoding="utf-8-sig",
        )

        return function_blocks

    except Exception as e:
        logger.error(str(e))

    return []


################ Check directory content path ##################################


def check_directory(input_dir_path: str) -> List:

    files_list = os.listdir(input_dir_path)
    if len(files_list) != 2:
        return [False, "Wrong number of files"]

    pdf_file = ""
    pdf_file_flag = 0
    xml_file = ""
    xml_file_flag = 0

    for file in files_list:
        file_prefix = file.split(".")[0]
        file_suffix = file.split(".")[1]

        if file_suffix == "pdf":
            pdf_file = file_prefix
            pdf_file_flag = 1

        if file_suffix == "xml":
            xml_file = file_prefix
            xml_file_flag = 1

    if any([pdf_file == "", pdf_file_flag == 0, xml_file == "", xml_file_flag == 0]):
        return [False, "Files with improper extension"]

    if pdf_file != xml_file:
        return [False, "XML and PDF file names do not match"]

    return [True, "Files Match, Go ahead"]


####################################################################################
def main_ladder_data_modelling(
    input_xml_file_path: str, data_model_dir: str, dest_file_name: str
) -> None:
    """Main file for extracting data from Programs and Functions"""

    ladder_program = ingest_file(input_xml_file_path)

    data_modelling_program_wise(
        ladder_program, data_model_dir, f"{dest_file_name}_programwise.csv"
    )
    data_modelling_function_wise(
        ladder_program, data_model_dir, f"{dest_file_name}_functionwise.csv"
    )

    return None


##### Function to extract  block data################

if __name__ == "__main__":

    input_xml_file_path = r"C:/Users/aniln/OneDrive - OPTIMIZED SOLUTIONS LTD/DENSO/Denso/Ladder_pdf_xml_rule/OK_coding_checker/Coding Checker_Rule10-15_23/Coding Checker_Rule10-15_23.xml"
    dest_file_name = "data_model_Rule_10_15_OK"
    data_model_dir = dest_file_name
    # model = load_model("yolo11m.pt")

    # main_ladder_data_modelling(input_xml_file_path, dest_file_name, data_model_dir, model)
    main_ladder_data_modelling(input_xml_file_path, dest_file_name, data_model_dir)
