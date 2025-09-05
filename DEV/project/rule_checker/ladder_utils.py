from typing import List
import re
from .japanese_half_full_width_mapping import full_to_half_conversion

def regex_pattern_check(check_pattern:str, comment_list:list)->bool:
    
    ret_flag=0

    if check_pattern and isinstance(check_pattern, str) and comment_list and isinstance(comment_list, list):
        check_pattern = ''.join(full_to_half_conversion.get(char, char) for char in check_pattern)
        for comment in comment_list:
            if comment and isinstance(comment, str):
                half_width_convert_comment = ''.join(full_to_half_conversion.get(char, char) for char in comment)

                if re.search(check_pattern, half_width_convert_comment):
                    ret_flag=1
                    break
                
        if ret_flag==0:
            return False
        else:
            
            return True
    else:
        return False
    
    
def clean_rung_number(val):
    if isinstance(val, list):
        if -1 in val:
            return ''
        else:
            return val
    elif val == -1:
        return ''
    else:
        return val