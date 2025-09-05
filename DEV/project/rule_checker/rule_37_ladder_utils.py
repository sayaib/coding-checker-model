from typing import *
import polars as pl
import re, json
from loguru import logger
import pandas as pd
import pprint
import copy
from itertools import combinations
from collections import defaultdict, deque
import ast
import icecream as ic  

# print(ladder_df)

def get_block_connections(ladder_df:pd.DataFrame)->List[Dict]:
    
    in_regex=r"in_list"
    out_regex=r"out_list"
    inout_regex=r"inoutVar"
    in_var_regex=r"inVar"
    out_var_regex=r"outVar"
    
    object_list=list(ladder_df['OBJECT'])
    object_type_list=list(ladder_df['OBJECT_TYPE_LIST'])
    attributes_list=list(ladder_df['ATTRIBUTES'])
    
    rest_object_list=list(ladder_df['OBJECT'])
    rest_object_type_list=list(ladder_df['OBJECT_TYPE_LIST'])
    rest_attributes_list=list(ladder_df['ATTRIBUTES'])
    output_list=[]
    
    for block_count, (block_type, block_attr) in enumerate(zip(object_type_list, attributes_list)):
        
        block_attr=eval(block_attr)
        
        if (block_type=='Block') and (len(block_attr)!=0):
            
            block_keys=block_attr.keys()
            block_values=block_attr.values()
            
            
            out_block_dict={}
            out_block_dict[block_attr['typeName']]=[]
                       
            
            for block_key,block_value in zip(block_keys, block_values):
                
                for rest_block_count, (rest_block_type, rest_block_attr) in enumerate(zip(rest_object_type_list, rest_attributes_list)):
                    
                    if rest_block_count!=block_count:
                    
                        rest_block_attr=eval(rest_block_attr)
                        
                        if len(rest_block_attr)!=0:
                            
                            rest_block_attr_keys=rest_block_attr.keys()
                            rest_block_attr_values=rest_block_attr.values()
                            
                            for rest_block_key, rest_block_value in zip(rest_block_attr_keys, rest_block_attr_values):
                                
                                temp_block_dict={}
                                temp_block_conn_list=[]
                                
                                
                                if (type(block_value) is list) and (type(rest_block_value) is list):
                                    if set(block_value) & set(rest_block_value):
                                        
                                        if re.search(in_regex, block_key) and re.search(out_regex, rest_block_key):
                                            
                                            block_value_orig=block_key.split("_")[0]
                                            block_value_orig_exists=temp_block_dict.get(block_value_orig, 'NONE')

                                            if rest_block_type=='DataSource':
                                                if block_value_orig_exists=='NONE':
                                                    temp_block_dict[block_value_orig]=[rest_block_attr['identifier']]
                                                    
                                                else:
                                                    temp_block_dict[block_value_orig].append(rest_block_attr['identifier'])
                                                    
                                                    

                                            if (rest_block_type=='Contact') or (rest_block_type=='Coil'):
                                                if block_value_orig_exists=='NONE':
                                                    temp_block_dict[block_value_orig]=[rest_block_attr['operand']]
                                                    
                                                else:
                                                    temp_block_dict[block_value_orig].append(rest_block_attr['operand'])
                                                    
                                                    

                                            if (rest_block_type=='LeftPowerRail'):
                                                if block_value_orig_exists=='NONE':
                                                    temp_block_dict[block_value_orig]=['LeftPowerRail']
                                                    
                                                else:
                                                    temp_block_dict[block_value_orig].append('LeftPowerRail')
                       
                                            
                                                                                   
                                            
                                            out_block_dict[block_attr['typeName']].append(temp_block_dict)
                                            
                                            
                                            
                                            
                                        if re.search(out_regex, block_key) and re.search(in_regex, rest_block_key):
                                            
                                            block_value_orig=block_key.split("_")[0]
                                            block_value_orig_exists=temp_block_dict.get(block_value_orig, 'NONE')
                                            

                                            if rest_block_type=='DataSink':
                                                if block_value_orig_exists=='NONE':
                                                    temp_block_dict[block_value_orig]=[rest_block_attr['identifier']]
                                                    
                                                else:
                                                    temp_block_dict[block_value_orig].append(rest_block_attr['identifier'])
                                                    
                                                    

                                            if (rest_block_type=='Contact') or (rest_block_type=='Coil'):
                                                if block_value_orig_exists=='NONE':
                                                    temp_block_dict[block_value_orig]=[rest_block_attr['operand']]
                                                    
                                                else:
                                                    temp_block_dict[block_value_orig].append(rest_block_attr['operand'])
                                                    
                                                    

                                            if (rest_block_type=='RightPowerRail'):
                                                if block_value_orig_exists=='NONE':
                                                    temp_block_dict[block_value_orig]=['RightPowerRail']
                                                    
                                                else:
                                                    temp_block_dict[block_value_orig].append('RightPowerRail')
                                            
                                            out_block_dict[block_attr['typeName']].append(temp_block_dict)
                                            
                                            
            output_list.append(out_block_dict)
                                            
    return(output_list)  



###########################3 Get the comments from the data source Sink ################3

def get_comments_from_datasource(input_variable:str, program_name:str, program_type:str, body_name:str, rung_order:int, df:pd.DataFrame ) -> List:
    
    if "program" in program_type:
        program_key='PROGRAM'
        body_key='BODY'
        
    if program_type=="FUNCTION":
        program_key='FUNCTION_BLOCK'
        body_key='BODY_TYPE'
    
    df=df[df[program_key]==program_name]
    # df=df[df[body_key]==body_name]
    # df=df[df['RUNG']==rung_order]
    
    attr_list=list(df['ATTRIBUTES'])
    
    out_list=[]
    
    for ele in attr_list:
        if re.search(rf"{input_variable}", ele):
            out_list.append(ele)

    return out_list
