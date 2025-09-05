
import time
import pandas as pd

from .rule_1   import execute_rule_1_programwise, execute_rule_1_functionwise
from .rule_2   import execute_rule_2
from .rule_3   import execute_rule_3
from .rule_4_1 import execute_rule_4_1_programwise, execute_rule_4_1_functionwise
from .rule_4_2 import execute_rule_4_2_program_wise, execute_rule_4_2_function_wise
from .rule_4_3 import execute_rule_4_3_program_wise, execute_rule_4_3_function_wise
from .rule_10  import execute_rule_10
from .rule_11 import execute_rule_11
from .rule_12 import execute_rule_12_programwise, execute_rule_12_functionwise
from .rule_14 import execute_rule_14
from .rule_15 import execute_rule_15
from .rule_16  import execute_rule_16_programwise, execute_rule_16_functionwise
from .rule_18  import execute_rule_18_programwise, execute_rule_18_functionwise
from .rule_19 import execute_rule_19_programwise, execute_rule_19_functionwise
from .rule_20 import execute_rule_20_programwise, execute_rule_20_functionwise
from .rule_24 import execute_rule_24_programwise, execute_rule_24_functionwise
from .rule_25 import execute_rule_25_programwise, execute_rule_25_functionwise
from .rule_26 import execute_rule_26_programwise, execute_rule_26_functionwise
from .rule_27 import execute_rule_27_programwise, execute_rule_27_functionwise
from .rule_33  import execute_rule_33
from .rule_34 import execute_rule_34_programwise
from .rule_35  import execute_rule_35_programwise, execute_rule_35_functionwise
from .rule_37  import execute_rule_37
from .rule_40  import execute_rule_40_program_functionwise
from .rule_45  import execute_rule_45_programwise
from .rule_46 import execute_rule_46_programwise, execute_rule_46_functionwise
from .rule_47 import execute_rule_47_programwise, execute_rule_47_functionwise
from .rule_48 import execute_rule_48_programwise, execute_rule_48_functionwise
from .rule_49 import execute_rule_49
from .rule_50 import execute_rule_50
from .rule_51 import execute_rule_51_programwise, execute_rule_51_functionwise
from .rule_55 import execute_rule_55_programwise, execute_rule_55_functionwise
from .rule_56 import execute_rule_56_programwise, execute_rule_56_functionwise
from .rule_62 import execute_rule_62_programwise
from .rule_63 import execute_rule_63_programwise, execute_rule_63_functionwise
from .rule_67 import execute_rule_67_programwise, execute_rule_67_functionwise
from .rule_70  import execute_rule_70_programwise, execute_rule_70_functionwise
from .rule_71  import execute_rule_71_programwise, execute_rule_71_functionwise
from .rule_80 import execute_rule_80_programwise, execute_rule_80_functionwise
from .rule_100 import execute_rule_100


def rule1(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    
    program_output_status_df = execute_rule_1_programwise(program_file_csv, program_comment_file)
    function_output_status_df = execute_rule_1_functionwise(function_file_csv ,function_comment_file)

    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}


def rule2(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    program_output_status_df = execute_rule_2(program_file_csv, program_comment_file, program_key="PROGRAM", body_type_key="BODY")
    function_output_status_df = execute_rule_2(function_file_csv, function_comment_file, program_key="FUNCTION_BLOCK", body_type_key="BODY_TYPE")
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}



def rule3(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    program_output_status_df = execute_rule_3(program_file_csv, program_comment_file, program_key="PROGRAM", body_type_key="BODY")
    function_output_status_df = execute_rule_3(function_file_csv, function_comment_file, program_key="FUNCTION_BLOCK", body_type_key="BODY_TYPE")
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    print("program_file_csv", program_file_csv)
    print("function_file_csv",function_file_csv)
    print("program_output_df",program_output_df)
    print("len",len(program_output_df))

    print("function_output_df",function_output_df)
    print("len",len(function_output_df))
    
    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}



def rule4_1(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    program_output_status_df = execute_rule_4_1_programwise(program_file_csv, program_comment_file, input_image)
    function_output_status_df = execute_rule_4_1_functionwise(function_file_csv, function_comment_file, input_image)
    
    print("program_output_status_df",program_output_status_df)
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}


def rule4_2(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    program_output_status_df = execute_rule_4_2_program_wise(program_file_csv, program_comment_file, input_image)
    function_output_status_df = execute_rule_4_2_function_wise(function_file_csv, function_comment_file,  input_image)
    print("4.2program_output_status_df",program_output_status_df)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}

def rule4_3(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    program_output_status_df = execute_rule_4_3_program_wise(program_file_csv, program_comment_file, input_image)
    function_output_status_df = execute_rule_4_3_function_wise(function_file_csv, function_comment_file,  input_image)
    print("4.3 program_output_status_df",program_output_status_df)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}



def rule10(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    program_output_status_df = execute_rule_10(program_file_csv, input_image, program_key="PROGRAM", body_type_key="BODY")
    function_output_status_df = execute_rule_10(function_file_csv, input_image, program_key="FUNCTION_BLOCK", body_type_key="BODY_TYPE")
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}


def rule11(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    program_output_status_df = execute_rule_11(program_file_csv, input_image, program_key="PROGRAM", body_type_key="BODY")
    function_output_status_df = execute_rule_11(function_file_csv, input_image, program_key="FUNCTION_BLOCK", body_type_key="BODY_TYPE")
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    print(program_status, function_status)

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}

def rule12(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    program_output_status_df = execute_rule_12_programwise(program_file_csv, program_comment_file, input_image)
    function_output_status_df = execute_rule_12_functionwise(function_file_csv, function_comment_file, input_image)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    print("*"*100)
    print("program_status",program_status)
    print("function_status",function_status)

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}


def rule14(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    program_output_status_df = execute_rule_14(program_file_csv, input_image, program_key="PROGRAM", body_type_key="BODY")
    function_output_status_df = execute_rule_14(function_file_csv, input_image, program_key="FUNCTION_BLOCK", body_type_key="BODY_TYPE")
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}


def rule15(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    program_output_status_df = execute_rule_15(program_file_csv, input_image, program_key="PROGRAM", body_type_key="BODY")
    function_output_status_df = execute_rule_15(function_file_csv, input_image, program_key="FUNCTION_BLOCK", body_type_key="BODY_TYPE")
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}


def rule16(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    print("Executing Rule 18")
    program_output_status_df = execute_rule_16_programwise(program_file_csv, program_comment_file)
    function_output_status_df = execute_rule_16_functionwise(function_file_csv, function_comment_file)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}


def rule18(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    print("Executing Rule 18")
    program_output_status_df = execute_rule_18_programwise(program_file_csv, program_comment_file)
    function_output_status_df = execute_rule_18_functionwise(function_file_csv, function_comment_file)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}
    
def rule19(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    print("Executing Rule 18")
    program_output_status_df = execute_rule_19_programwise(program_file_csv, program_comment_file, input_image)
    function_output_status_df = execute_rule_19_functionwise(function_file_csv, function_comment_file, input_image)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}

def rule20(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    print("Executing Rule 18")
    program_output_status_df = execute_rule_20_programwise(program_file_csv, program_comment_file, input_image)
    function_output_status_df = execute_rule_20_functionwise(function_file_csv, function_comment_file, input_image)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}

def rule24(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    print("Executing Rule 18")
    program_output_status_df = execute_rule_24_programwise(program_file_csv, program_comment_file, input_image)
    function_output_status_df = execute_rule_24_functionwise(function_file_csv, function_comment_file, input_image)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}


def rule25(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    print("Executing Rule 18")
    program_output_status_df = execute_rule_25_programwise(program_file_csv, program_comment_file, datasource_program_file, input_image)
    function_output_status_df = execute_rule_25_functionwise(function_file_csv, function_comment_file, datasource_function_file, input_image)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}


def rule26(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    print("Executing Rule 18")
    program_output_status_df = execute_rule_26_programwise(program_file_csv, program_comment_file, datasource_program_file, input_image)
    function_output_status_df = execute_rule_26_functionwise(function_file_csv, function_comment_file, datasource_function_file, input_image)
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}

def rule27(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    print("Executing Rule 18")
    program_output_status_df = execute_rule_27_programwise(program_file_csv, program_comment_file, input_image)
    function_output_status_df = execute_rule_27_functionwise(function_file_csv, function_comment_file, input_image)
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}


def rule33(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    print("Executing Rule 33")

    program_output_status_df = execute_rule_33(program_file_csv, input_image, program_key="PROGRAM", body_type_key="BODY")
    function_output_status_df = execute_rule_33(function_file_csv, input_image, program_key="FUNCTION_BLOCK", body_type_key="BODY_TYPE")
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}
    
def rule34(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    print("Executing Rule 33")

    program_output_status_df = execute_rule_34_programwise(program_file_csv, program_comment_file, input_image)
    # function_output_status_df = execute_rule_33(function_file_csv, function_comment_file, input_image)
    
    program_status = program_output_status_df.get('status')
    # function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    # function_output_df = function_output_status_df.get('output_df')

    # if program_status == 'OK' and function_status == 'OK':
    #     final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
    #     return {"status": "SUCCESS", "output_df": final_output_df}
    # elif program_status == 'OK':
    #     return {"status": "SUCCESS", "output_df": program_output_df}
    # else:
    #     return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}
    

def rule35(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    print("Executing Rule 33")

    program_output_status_df = execute_rule_35_programwise(program_file_csv, program_comment_file)
    function_output_status_df = execute_rule_35_functionwise(function_file_csv, function_comment_file)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}

def rule37(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    print("Executing Rule 33")

    program_output_status_df = execute_rule_37(program_file_csv, program_key="PROGRAM", body_type_key="BODY")
    function_output_status_df = execute_rule_37(function_file_csv, program_key="FUNCTION_BLOCK", body_type_key="BODY_TYPE")
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}


def rule40(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    print("Executing Rule 40")

    program_function_status_df = execute_rule_40_program_functionwise(program_file_csv, function_file_csv)
    
    program_function_status = program_function_status_df.get('status')

    if program_function_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_function_status_df.get('output_df')}
    else:
        return {"status": "FAILED", "error": program_function_status_df.get('error', 'Error Occured')}


def rule45(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    program_output_status_df = execute_rule_45_programwise(program_file_csv, program_comment_file)
    # function_output_status_df = execute_rule_4_1_functionwise(function_file_csv, function_comment_file,  input_image)
    # print("4.2program_output_status_df",program_output_status_df)
    
    program_status = program_output_status_df.get('status')
    # function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    # function_output_df = function_output_status_df.get('output_df')

    # if program_status == 'OK' and function_status == 'OK':
    #     final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
    #     return {"status": "SUCCESS", "output_df": final_output_df}
    # elif program_status == 'OK':
    #     return {"status": "SUCCESS", "output_df": program_output_df}
    # else:
    return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}

def rule46(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    program_output_status_df = execute_rule_46_programwise(program_file_csv)
    function_output_status_df = execute_rule_46_functionwise(function_file_csv)
    print("4.2program_output_status_df",program_output_status_df)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}


def rule47(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    program_output_status_df = execute_rule_47_programwise(program_file_csv, program_comment_file)
    function_output_status_df = execute_rule_47_functionwise(function_file_csv, function_comment_file)
    print("4.2program_output_status_df",program_output_status_df)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}

def rule48(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    program_output_status_df = execute_rule_48_programwise(program_file_csv)
    function_output_status_df = execute_rule_48_functionwise(function_file_csv)
    print("4.2program_output_status_df",program_output_status_df)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}

def rule49(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    program_output_status_df = execute_rule_49(program_file_csv, program_comment_file, program_key="PROGRAM", body_type_key="BODY")
    function_output_status_df = execute_rule_49(function_file_csv, function_comment_file, program_key="FUNCTION_BLOCK", body_type_key="BODY_TYPE")
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}

def rule50(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    program_output_status_df = execute_rule_50(program_file_csv, program_comment_file, program_key="PROGRAM", body_type_key="BODY")
    function_output_status_df = execute_rule_50(function_file_csv, function_comment_file, program_key="FUNCTION_BLOCK", body_type_key="BODY_TYPE")
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}

def rule51(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):

    program_output_status_df = execute_rule_51_programwise(program_file_csv, program_comment_file)
    function_output_status_df = execute_rule_51_functionwise(function_file_csv, function_comment_file)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}

def rule55(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):

    program_output_status_df = execute_rule_55_programwise(program_file_csv, program_comment_file)
    function_output_status_df = execute_rule_55_functionwise(function_file_csv, function_comment_file)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}

def rule56(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):

    program_output_status_df = execute_rule_56_programwise(program_file_csv, program_comment_file)
    function_output_status_df = execute_rule_56_functionwise(function_file_csv, function_comment_file)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}

def rule62(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):

    program_output_status_df = execute_rule_62_programwise(program_file_csv, program_comment_file)
    # function_output_status_df = execute_rule_56_functionwise(function_file_csv, function_comment_file)
    
    program_status = program_output_status_df.get('status')
    # function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    # function_output_df = function_output_status_df.get('output_df')

    # if program_status == 'OK' and function_status == 'OK':
    #     final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
    #     return {"status": "SUCCESS", "output_df": final_output_df}
    # elif program_status == 'OK':
    #     return {"status": "SUCCESS", "output_df": program_output_df}
    # else:
    #     return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}

def rule63(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):

    program_output_status_df = execute_rule_63_programwise(program_file_csv, program_comment_file)
    function_output_status_df = execute_rule_56_functionwise(function_file_csv, function_comment_file)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}

def rule67(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):

    program_output_status_df = execute_rule_67_programwise(program_file_csv, program_comment_file)
    function_output_status_df = execute_rule_67_functionwise(function_file_csv, function_comment_file)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}


def rule70(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):

    program_output_status_df = execute_rule_70_programwise(program_file_csv, program_comment_file)
    function_output_status_df = execute_rule_70_functionwise(function_file_csv, function_comment_file)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}

def rule71(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):

    program_output_status_df = execute_rule_71_programwise(program_file_csv, program_comment_file)
    function_output_status_df = execute_rule_71_functionwise(function_file_csv, function_comment_file)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}

def rule80(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):

    program_output_status_df = execute_rule_80_programwise(program_file_csv, program_comment_file)
    function_output_status_df = execute_rule_80_functionwise(function_file_csv, function_comment_file)
    
    program_status = program_output_status_df.get('status')
    function_status = function_output_status_df.get('status')
    program_output_df = program_output_status_df.get('output_df')
    function_output_df = function_output_status_df.get('output_df')

    if program_status == 'OK' and function_status == 'OK':
        final_output_df = pd.concat([program_output_df, function_output_df], ignore_index=True)
        return {"status": "SUCCESS", "output_df": final_output_df}
    elif program_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_output_df}
    else:
        return {"status": "FAILED", "error": program_output_status_df.get('error', 'Error Occured')}


def rule100(program_file_csv, program_comment_file, datasource_program_file, function_file_csv, function_comment_file, datasource_function_file, input_image):
    print("Executing Rule 100")
    program_function_status_df = execute_rule_100(program_file_csv, function_file_csv)

    print("program_function_status_df", program_function_status_df)
    program_function_status = program_function_status_df.get('status')

    if program_function_status == 'OK':
        return {"status": "SUCCESS", "output_df": program_function_status_df.get('output_df')}
    else:
        return {"status": "FAILED", "error": program_function_status_df.get('error', 'Error Occured')}
