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


##############################################################################


def get_series_contacts(ladder_df:pd.DataFrame)->List:
    
    ladder_contact=ladder_df.filter(ladder_df["OBJECT_TYPE_LIST"] == 'Contact')
    attribute_list=list(ladder_contact['ATTRIBUTES'])
    attribute_list=[eval(ele) for ele in attribute_list]
    
    chains=build_chains(attribute_list)
    return chains
    
                  
      

