import os
import sys
import yaml
from tqdm import tqdm
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.csv import save_csv

import warnings
warnings.simplefilter("ignore")

def load_sabi_file(path: str, sep: str):
    """
    Loads the file and processes it to obtain the required format.
    """
    with open(path, "r", encoding = "utf-16") as file:
        lines = file.read().splitlines()
    memory = [format_line(lines[0], sep)[1]]
    data = []
    for line in lines[1:]:
        new, processed_line = format_line(line, sep)
        if new:
            row = format_row(memory)
            data.append(row)
            memory = []
        memory.append(processed_line)
    return data

def format_row(rows: list[list]) -> list:
    """
    Processes each batch of rows to obtain one single row.
    """
    processed_row = [[] for _ in range(len(rows[0]))]
    for row in rows:
        for i, value in enumerate(row):
            if value:
                processed_row[i].append(value)
    final_row = [] 
    for values in processed_row:
        l = len(values)
        if l == 0:
            value = ""
        elif l == 1:
            value = values[0]
        else:
            value = values
        final_row.append(value)
    return final_row

def format_line(line: str, sep: str) -> tuple[bool, list]:
    """
    Extracts data values from the received raw line.
    """
    values = line.split(sep)
    processed_line = []
    for value in values:
        temp = value.replace('"', "")
        processed_line.append(temp)
    if processed_line[0]:
        return True, processed_line
    return False, processed_line

def merge_sabi(config: dict) -> list[list]:
    """
    Lists the directory and processes each file.
    """
    folder_path = config["data"]["sabi_data_path"]
    save_path = config["data"]["merged_path"]
    separator = config["data_parameters"]["separator"]
    files = os.listdir(folder_path)
    data = []
    for i, file in enumerate(tqdm(files, desc = "Processing SABI files")):
        file_path = folder_path + file
        file_data = load_sabi_file(file_path, separator)
        if i > 0:
            file_data = file_data[1:]
        data.extend(file_data)
    save_csv(data, save_path, row_type = "list")
    return data

if __name__ == "__main__":
    with open("config/parameters.yaml", "r") as file:
        config = yaml.safe_load(file)
    merge_sabi(config)