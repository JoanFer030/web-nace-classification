import os
import sys
import re
import yaml
import pandas as pd
from tqdm import tqdm
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.csv import open_csv, save_csv

import warnings
warnings.simplefilter("ignore")

def filter_by_words(data: pd.DataFrame, delete_words: tuple[str, list]) -> pd.DataFrame:
    """
    Deletes the rows whose value in the columns included in 
    “delete_words” is one of the indicated words.
    """
    for name, words in delete_words:
        pattern = r"\b(" + "|".join(words) + r")\b"
        delete_rows = data[name].str.contains(pattern, case = False, na = False)
        data = data.iloc[list(~delete_rows), :]
    return data

def format_str(value: str):
    """
    Converts the input value into a string.
    """
    try:
        return str(int(value))
    except:
        return str(value)

def format_int(value: str):
    """
    Converts the input value into a integer.
    """
    try:
        return int(value)
    except:
        return -1

def format_url(value: str) -> str:
    """
    Extracts the url from the input value, in case there is one.
    """
    matchs = re.findall(r"^(www\.[a-zñA-ZÑ0-9.-]+\.[a-zA-Z]{2,})", value)
    if matchs:
        return matchs[0]
    return ""

def format_list(value: str) -> list:
    """
    Converts the input string to a list, where 
    its values is converted to the subtype indicated.
    """
    value = str(value)
    if value == "nan":
        return []
    if "[" in value:
        value = value[1:-1]
        items = value.split(", ")
        return [code[1:-1] for code in items]
    else:
        return [value]

def format_data(data: pd.DataFrame, column_types: list[str]) -> pd.DataFrame:
    """
    Gives the data the format indicated in the column_types list
    and deletes the rows that cannot be converted to the indicated format.
    """
    for name, col_type in tqdm(list(zip(data.columns, column_types)), desc = "Formatting data"):
        if col_type == "str":
            data[name] = data[name].apply(format_str)
        elif col_type == "int":
            data[name] = data[name].apply(format_int)
        elif col_type == "url":
            data[name] = data[name].apply(format_url)
        elif col_type == "list":
            data[name] = data[name].apply(format_list)
        else:
            raise ValueError("Invalid column type.")
    return data
    

def edit_columns(data: pd.DataFrame, column_names: list[str]) -> pd.DataFrame:
    """
    Renames columns and deletes unnecessary columns.
    """
    keep_cols = [i for i, n in enumerate(column_names) if n]
    names = [name for name in column_names if name]
    data = data.iloc[:, keep_cols]
    data.columns = names
    return data

def process_sabi(config: dict):
    """
    Filter required columns, format them and clean data.
    """
    merged_path = config["data"]["merged_path"]
    column_names = config["data_parameters"]["column_names"]
    column_types = config["data_parameters"]["column_types"]
    delete_words = config["data_parameters"]["delete_words"]
    save_path = config["data"]["filtered_path"]
    data = open_csv(merged_path)
    data = edit_columns(data, column_names)
    data = format_data(data, column_types)
    data = filter_by_words(data, delete_words)
    save_csv(data, save_path)

if __name__ == "__main__":
    with open("config/parameters.yaml", "r") as file:
        config = yaml.safe_load(file)
    process_sabi(config)