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

            # if node['operand'] == target_operand:
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
    try:
        coil_df["self_holding"] = coil_df.apply(
            lambda row: search_forward_from_inlist(row["operand"], row["in_list"], contact_attr_list),
            axis=1
        )

        # Get operands that are self-holding
        self_holding_data = coil_df[coil_df["self_holding"] == True]["operand"].tolist()
    except Exception as e:
        return []

    return self_holding_data
