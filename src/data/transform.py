import re
from tqdm import tqdm
from dateutil.parser import parser
from utils.csv import open_csv, save_csv

def filter_by_words(data: list[dict], delete_words: tuple[str, list]) -> list[dict]:
    """
    Deletes the rows whose value in the columns 
    included in “delete_words” is one of the indicated
    words .
    """
    new_data = []
    for row in data:
        if not any(any(word in row[col].lower() for word in words)for col, words in delete_words):
            new_data.append(row)
    return new_data

def format_column_str(value: str, *args) -> str:
    """
    Converts the value to a string.
    """
    return str(value)

def format_column_int(value: str, *args) -> int:
    """
    Converts the value to a integer if it can be done, otherwise returns the value.
    """
    if re.match(r"\d+", value):
        value = int(value)
    return value

def format_column_url(value: str, *args) -> str:
    """
    Extracts the url from the input value, in case there is one.
    """
    matchs = re.findall(r"^(www\.[a-zñA-ZÑ0-9.-]+\.[a-zA-Z]{2,})", value)
    if matchs:
        return matchs[0]
    return ""

def format_column_date(value, *args) -> str:
    """
    Checks whether or not the input value is a 
    date, otherwise returns '01/01/1900'.
    """
    def is_date(string: str) -> bool:
        try: 
            parser(string)
            return True
        except:
            return False
    if is_date:
        return value
    return "01/01/1900"

def format_column_list(value: str, subtype: str) -> list:
    """
    Converts the input string to a list, where 
    its values is converted to the subtype indicated.
    """
    if value == "nan":
        return []
    format_subtype = format_column_int if subtype == "int" else format_column_str
    if "[" in value and "]" in value:
        value = value[1:-1]
        values = value.split(",")
        values_list = []
        for t in values:
            v = format_subtype(t.strip()[1:-1])
            values_list.append(v)
        return values_list
    return [format_subtype(value)]

def format_data(data: list[dict], column_types: list[str]) -> list:
    """
    Gives the data the format indicated in the column_types list
    and deletes the rows that cannot be converted to the indicated format.
    """
    functions = {"int": format_column_int, "str": format_column_str, "url": format_column_url,
                 "date": format_column_date}
    column_functions = []
    for column in column_types:
        if "list" in column:
            subtype = column.replace("list", "")[1:-1]
            column_functions.append((format_column_list, subtype))
        elif column in functions:
            column_functions.append((functions[column], ""))
        else:
            raise ValueError("Incorrect data type.")
    new_data = []
    for row in tqdm(data, desc = "Formatting data"):
        n_row = {h: func(v, subtype) for ((func, subtype), (h, v)) in zip(column_functions, row.items())}
        if n_row["url"]:
            new_data.append(n_row)
    return new_data

def edit_columns(data: list[dict], column_names: list[str]) -> list[dict]:
    """
    Renames the columns.
    """
    new_data = []
    for row in data:
        n_row = {n: v for n, v in zip(column_names, row.values()) if n}
        new_data.append(n_row)
    return new_data

def transform_data(config: dict):
    """
    Transform Pipeline:
    Filter required columns, format them and clean data.
    """
    merged_path = config["data"]["merged_path"]
    filtered_path = config["data"]["filtered_path"]
    column_names = config["data_parameters"]["column_names"]
    column_types = config["data_parameters"]["column_types"]
    delete_words = config["data_parameters"]["delete_words"]
    data = open_csv(merged_path)
    data = edit_columns(data, column_names)
    data = format_data(data, column_types)
    data = filter_by_words(data, delete_words)
    save_csv(data, filtered_path)
    print(f"Saved {len(data):,} companies from SABI")