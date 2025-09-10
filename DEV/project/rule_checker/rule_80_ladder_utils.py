from typing import *
import polars as pl
import re, json
import pandas as pd
from itertools import combinations
from collections import defaultdict, deque
import ast
#### Self holding contacts#####################3


# print(laddeclsr_df)

def check_self_holding_aditya(ladder_df:pd.DataFrame)->Dict[str,Any]:
    
    coil_df=ladder_df.filter(ladder_df["OBJECT_TYPE_LIST"] == 'Coil')
    contact_df=ladder_df.filter(ladder_df["OBJECT_TYPE_LIST"] == 'Contact')
    
    coil_attributes_list=list(coil_df['ATTRIBUTES'])
    contact_attributes_list=list(contact_df['ATTRIBUTES'])
    
    self_holding_coils_pair={}
    
    for coil_attr in coil_attributes_list:
        coil_attr=eval(coil_attr)
        coil_operand=coil_attr.get('operand', 'NONE')
        
        coil_in_list=coil_attr.get('in_list', 'NONE')
        
        for contact_attr in contact_attributes_list:
            contact_attr=eval(contact_attr)
            
            contact_out_list=contact_attr.get('out_list', 'NONE')
            contact_operand=contact_attr.get('operand', 'NONE')
            contact_negated_status=contact_attr.get('negated', 'NONE')
            
            if (contact_negated_status!='NONE') and (contact_negated_status=='false') and (contact_operand==coil_operand):
                
                if set(coil_in_list) & set(contact_out_list):
                    
                    self_holding_coils_pair[coil_operand]=contact_operand
            
            
        
    return  self_holding_coils_pair   
        
##################### Self holding new###########################################

def search_forward_from_inlist(target_operand, current_inlist, graph, visited=None):
    if visited is None:
        visited = set()

    for node_str in graph:
        node = ast.literal_eval(node_str) if isinstance(node_str, str) else node_str

        if any(out in current_inlist for out in node['out_list']):
            node_id = f"{node['operand']}_{','.join(node['in_list'])}_{','.join(node['out_list'])}"
            if node_id in visited:
                continue
            visited.add(node_id)

            # if node['operand'] == target_operand:
            # if 'negated' in node and node.get('operand') == target_operand and node['negated'] == 'false':
            #     return True
            if node.get('operand') == target_operand and node.get('negated') == 'true':
                return False  # Invalid path due to negation

            # ✅ Return True only if same operand AND negated is 'false'
            if node.get('operand') == target_operand and node.get('negated') == 'false':
                return True

            if search_forward_from_inlist(target_operand, node['in_list'], graph, visited):
                return True

    return False

def check_self_holding(ladder_df: pd.DataFrame) -> List[str]:

    # Filter coil and contact rows
    coil_df = ladder_df[ladder_df["OBJECT_TYPE_LIST"] == "Coil"].copy()
    contact_df = ladder_df[ladder_df["OBJECT_TYPE_LIST"] == "Contact"].copy()

    # Convert stringified dicts to real dicts
    coil_df["ATTR_DICT"] = coil_df["ATTRIBUTES"].apply(ast.literal_eval)
    contact_attr_list = contact_df["ATTRIBUTES"].tolist()

    # Extract fields from coil dicts
    coil_df["operand"] = coil_df["ATTR_DICT"].apply(lambda x: x.get("operand", "NONE"))
    coil_df["in_list"] = coil_df["ATTR_DICT"].apply(lambda x: x.get("in_list", []))

    # Apply self-holding check using search function
    coil_df["self_holding"] = coil_df.apply(
        lambda row: search_forward_from_inlist(row["operand"], row["in_list"], contact_attr_list),
        axis=1
    )

    # Get operands that are self-holding
    self_holding_data = coil_df[coil_df["self_holding"] == True]["operand"].tolist()

    return self_holding_data




    
############################ Move Block #######################
  

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
                                            
                                            # block_value_orig=block_key.split("_")[0]
                                            split_keys = [
                                                "inVar_in_list",
                                                "_inVar_in_order",
                                                "_outVar_out_list",
                                                "_outVar_out_order",
                                                "_OUT_outVar_out_list",
                                                "_outVar_out_list"
                                            ]

                                            # Find the first match from the list
                                            matched_key = next((key for key in split_keys if key in block_key), None)

                                            # Split based on the matched key or default "_"
                                            if matched_key:
                                                block_value_orig = block_key.split(matched_key)[0].rstrip("_")
                                            else:
                                                block_value_orig = block_key.split("_")[0]

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
                                            
                                            # block_value_orig=block_key.split("_")[0]
                                            # List of possible substrings to match
                                            split_keys = [
                                                "_inVar_in_list",
                                                "_inVar_in_order",
                                                "_outVar_out_list",
                                                "_outVar_out_order",
                                                "_OUT_outVar_out_list",
                                                "_outVar_out_list"
                                            ]

                                            # Find the first match from the list
                                            matched_key = next((key for key in split_keys if key in block_key), None)

                                            # Split based on the matched key or default "_"
                                            if matched_key:
                                                block_value_orig = block_key.split(matched_key)[0].rstrip("_")
                                            else:
                                                block_value_orig = block_key.split("_")[0]
                                            
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



################################################





############ Get series connection pairs #################

def build_chains(data):
    # Step 1: Build a map from outputs to list of dicts that consume them
    index_map = {i: d for i, d in enumerate(data)}
    chains = []

    # Step 2: For each dict, attempt to build a chain forward
    def dfs(path, visited):
        last = path[-1]
        extended = False

        for i, candidate in index_map.items():
            if i in visited:
                continue
            # Check for overlap between last out_list and candidate in_list
            
            out_list_present=last.get('out_list', [])
            in_list_present=candidate.get('in_list', [])
                    
            
            if set(out_list_present) & set(in_list_present):
                dfs(path + [candidate], visited | {i})
                extended = True

        if not extended:
            chains.append(path)

    # Step 3: Start chains from each dictionary
    for i in range(len(data)):
        dfs([data[i]], {i})

    return chains


#############################################
def create_super_sets(sub_list:List)->List:
    
    #This code block
    sets = [set(lst) for lst in sub_list]

    
    visited = set()
    groups = []

    for i, s in enumerate(sets):
        if not s or i in visited:
            continue
        group = set(s)
        queue = [i]
        visited.add(i)

        while queue:
            current = queue.pop()
            for j, t in enumerate(sets):
                if j not in visited and t and group & t:  # at least one common element
                    group |= t
                    visited.add(j)
                    queue.append(j)

        groups.append(sorted(group))

    # Include completely empty lists if they were in the original
    if any(not lst for lst in sub_list):
        groups.append([])

    
    return groups



################### replace with sub_lists with super lists###############3

def replace_sub_list_with_super_list(single:List, multi:List[List])->List:
    
    replacement = None

    # Find the first sublist that intersects with single
    for group in multi:
        if set(single) & set(group):
            replacement = group
            break   
                
        
    if replacement:
        single = replacement   
        
        
    return single


##############################################################################


def get_series_contacts(ladder_df:pd.DataFrame)->List:
    
    ladder_contact=ladder_df.filter(ladder_df["OBJECT_TYPE_LIST"] == 'Contact')
    attribute_list=list(ladder_contact['ATTRIBUTES'])
    attribute_list=[eval(ele) for ele in attribute_list]
    
    chains=build_chains(attribute_list)
    return chains
    
                  
      
############### get Unique Dicts#############3

def dict_to_tuple(d):
    return tuple(sorted((k, tuple(v) if isinstance(v, list) else v) for k, v in d.items()))

def get_unique_dicts(in_list:List)->List:
    
    seen = set()
    unique = []
    for d in in_list:
        key = dict_to_tuple(d)
        if key not in seen:
            seen.add(key)
            unique.append(d) 
                       
    return unique
               



######################## get the Parallel contacts##########################
def get_parallel_contacts(ladder_df:pd.DataFrame)->List:
    
    
    attribute_list=list(ladder_df['ATTRIBUTES'])
    attribute_list=[eval(ele) for ele in attribute_list]
    object_type_list=list(ladder_df['OBJECT_TYPE_LIST'])
    new_attribute_list=[]
    
    
    #AAdd the object type to the dictionaries
    for attr_ , object_type in zip(attribute_list, object_type_list):
        attr_['object_type']=object_type
        new_attribute_list.append(attr_)
      
    
    chains=build_chains(new_attribute_list)
    
    #Create a superlist merging all the inlist and outlist, and subsume the sublist in to superlist
    super_list=[]
    
    for attr_ in attribute_list:
        
        attr_in_list=attr_.get('in_list', [])
        attr_out_list=attr_.get('out_list', [])
        
        super_list.append(attr_in_list)
        super_list.append(attr_out_list)
 
    merged_super_list=create_super_sets(super_list)
    
    chain_with_super_nodes=[]
    
    

    #Modify the merged super list with super nodes
    for ele_dict in chains:
        subchain_with_super_nodes=[]
        for ele in ele_dict:
            
            in_list_super_set=replace_sub_list_with_super_list( ele.get('in_list', []) , merged_super_list)
            out_list_super_set=replace_sub_list_with_super_list(  ele.get('out_list', []),   merged_super_list)
            
            ele['in_list']=in_list_super_set
            ele['out_list']=out_list_super_set
            
            subchain_with_super_nodes.append(ele)
            
        chain_with_super_nodes.append(subchain_with_super_nodes)
        
        
    #Create attributes with super lists
    attributes_with_super_nodes=[]
    
    for item in new_attribute_list:
        
         
         in_list_super_set=replace_sub_list_with_super_list(item['in_list'], merged_super_list)
         out_list_super_set=replace_sub_list_with_super_list(item['out_list'], merged_super_list)
         
         item['in_list']=in_list_super_set
         item['out_list']=out_list_super_set
         
         attributes_with_super_nodes.append(item)
         
       
    #Extract the parallel pairs
    parallel_pairs={}
    
    for indiv_count,indiv in enumerate(attributes_with_super_nodes):
        
        if (indiv['object_type']=='Contact'):
            
            
        
            for chain_count, chain in enumerate(chain_with_super_nodes):
                
                start_point=0
                contact_stack=[]
                
                
                
                for sub_dict in chain:
                   
                    
                    if (sub_dict['object_type']=='Contact'):
                        
                        if indiv!=sub_dict:
                                                                  
                            
                            if (indiv['in_list']==sub_dict['in_list']):
                                start_point=1
                                contact_stack.append(sub_dict)
                                
                                
                                
                            if (indiv['out_list']==sub_dict['out_list']) and (start_point==1):
                                
                                
                                pair_dict={}
                                contact_stack.append(sub_dict)
                                
                                
                                contact_stack=get_unique_dicts(contact_stack)
                                pair_dict['contact_chain']=contact_stack
                                
                                                               
                                # pair_dict['start_contact']=contact_stack
                                # pair_dict['stop_contact']=sub_dict
                                pair_dict['ref_contact']=indiv
                                
                                parallel_pairs[f"{indiv_count}_{chain_count}"]=pair_dict
                                                        
                                break
                                
                                
                                
                            
    #Remove the duplicates
    seen = set()
    unique_pairs = {}

    for key, value in parallel_pairs.items():
        serialized = json.dumps(value, sort_keys=True)
        if serialized not in seen:
            seen.add(serialized)
            unique_pairs[key] = value                    
            

 
    
    return unique_pairs
    
########################################################3

def get_in_parallel_A_contacts(contact_chain_list:List, contact:str)-> List:
    
    in_parallel_list=[]
    for contact_chain in contact_chain_list:
        
        ref_contact=contact_chain['ref_contact']['operand']
        chain= contact_chain['contact_chain']
        
        if ref_contact==contact:
            for contact_operand in chain:
                
                contact_operand_negated_status=contact_operand.get("negated", 'NONE')
                
                if all([contact_operand_negated_status!='NONE', contact_operand_negated_status=='false']):
                    
                
                    in_parallel_list.append(contact_operand['operand'])
                
            
        
    return in_parallel_list 


##########################Rule 2 Elements Chcek ######################3


###########################3 Get the comments from the data source Sink ################3

def get_comments_from_datasource(input_variable:str, program_name:str, program_type:str, body_name:str, rung_order:int, data_comments_source_file:str ) -> List:
    
    df=pd.read_csv(data_comments_source_file)
    
    
    if program_type=="program":
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


###################################################################################

if __name__ == "__main__":

    

    ladder_df = pd.read_csv(r"C:\Users\aniln\OneDrive - OPTIMIZED SOLUTIONS LTD\DENSO\GithubCode\rules_personal\FASTAPI\input_files\Coding Checker_Rule10-23_33_250729\Coding Checker_Rule10-23_33_250729_programwise.csv")

    ladder_df=ladder_df[ladder_df['PROGRAM'] == 'P111_XXXPRS_Function1']
    ladder_df=ladder_df[ladder_df['BODY'] == 'AutoRun']
    ladder_df=ladder_df[ladder_df['RUNG'] == 18]
    # print(ladder_df['ATTRIBUTES']['BLOC'])
    block = ladder_df[(ladder_df['OBJECT_TYPE_LIST'] == 'Block') & (ladder_df['ATTRIBUTES'].str.contains('typeName', na=False))]
    kk=get_block_connections(pl.from_pandas(ladder_df))
    print("block", block)
    print(kk)
    
#     input_variable="_PLC_ErrSta"
#     ll=get_comments_from_datasource(input_variable=input_variable, program_name='P000_InitialSetting',
#                                     program_type='program', body_name='Initial',
#                                     rung_order=4, data_comments_source_file="./data_model_Rule_10/data_model_Rule_10_datasource_comments_programwise.csv" )
    
#     pprint.pprint(ll)





