import pandas as pd
import ast
from typing import Dict, Any, List

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

            if node.get('operand') == target_operand and node.get('negated') == 'true':
                return False  # Invalid path due to negation

            # ✅ Return True only if same operand AND negated is 'false'
            if (
                node.get('operand') == target_operand
                and node.get('negated') == 'false'
                and any(out in current_inlist for out in node['out_list'])  # direct connection check
            ):
                return True


            if search_forward_from_inlist(target_operand, node['in_list'], graph, visited):
                return True

    return False


def check_self_holding(ladder_df: pd.DataFrame) -> List[str]:
    coil_df = ladder_df[ladder_df["OBJECT_TYPE_LIST"] == "Coil"].copy()
    contact_df = ladder_df[ladder_df["OBJECT_TYPE_LIST"] == "Contact"].copy()

    coil_df["ATTR_DICT"] = coil_df["ATTRIBUTES"].apply(ast.literal_eval)
    contact_df["ATTR_DICT"] = contact_df["ATTRIBUTES"].apply(ast.literal_eval)

    coil_df["operand"] = coil_df["ATTR_DICT"].apply(lambda x: x.get("operand", "NONE"))
    coil_df["in_list"] = coil_df["ATTR_DICT"].apply(lambda x: x.get("in_list", []))

    contact_df["operand"] = contact_df["ATTR_DICT"].apply(lambda x: x.get("operand", "NONE"))
    contact_df["out_list"] = contact_df["ATTR_DICT"].apply(lambda x: x.get("out_list", []))
    contact_df["negated"] = contact_df["ATTR_DICT"].apply(lambda x: x.get("negated", "false"))

    self_holding_operands = []

    for _, coil_row in coil_df.iterrows():
        for _, contact_row in contact_df.iterrows():
            if (
                coil_row["operand"] == contact_row["operand"]        # same variable
                and contact_row["negated"] == "false"                # not negated
                and any(out in coil_row["in_list"] for out in contact_row["out_list"])  # direct connection
            ):
                self_holding_operands.append(coil_row["operand"])
                break  # no need to check more contacts for this coil

    return self_holding_operands

# def check_self_holding(ladder_df: pd.DataFrame) -> List[str]:

#     # Filter coil and contact rows
#     coil_df = ladder_df[ladder_df["OBJECT_TYPE_LIST"] == "Coil"].copy()
#     contact_df = ladder_df[ladder_df["OBJECT_TYPE_LIST"] == "Contact"].copy()

#     # Convert stringified dicts to real dicts
#     coil_df["ATTR_DICT"] = coil_df["ATTRIBUTES"].apply(ast.literal_eval)
#     contact_attr_list = contact_df["ATTRIBUTES"].tolist()

#     # Extract fields from coil dicts
#     coil_df["operand"] = coil_df["ATTR_DICT"].apply(lambda x: x.get("operand", "NONE"))
#     coil_df["in_list"] = coil_df["ATTR_DICT"].apply(lambda x: x.get("in_list", []))

#     # Apply self-holding check using search function
#     try:
#         coil_df["self_holding"] = coil_df.apply(
#             lambda row: search_forward_from_inlist(row["operand"], row["in_list"], contact_attr_list),
#             axis=1
#         )

#         # Get operands that are self-holding
#         self_holding_data = coil_df[coil_df["self_holding"] == True]["operand"].tolist()
#     except Exception as e:
#         return []

#     return self_holding_data



#========================================================================================================

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
    
