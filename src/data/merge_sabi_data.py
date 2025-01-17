"""
Merge all the data exported from SABI, saved in a folder because they 
are exported in several batch, into one unique file. This processes is
conducted to simplify the handling of the data. Additionally, it gives
the resulting file the correct format to the future work.
"""
import os
import re
import csv
import yaml
from tqdm import tqdm

def load_sabi_file(path: str):
    with open(path, "r", encoding = "utf-16") as file:
        lines = file.read().splitlines()
    memory = [format_line(lines[0])[1]]
    data = []
    for line in lines[1:]:
        new, processed_line = format_line(line)
        if new:
            row = format_row(memory)
            data.append(row)
            memory = []
        memory.append(processed_line)
    return data

def format_row(rows: list[list]) -> list:
    processed_row = [[] for _ in range(len(rows[0]))]
    for row in rows:
        for i, value in enumerate(row):
            if value:
                processed_row[i].append(value)
    final_row = [] 
    for values in processed_row:
        l = len(values)
        if l == 0:
            value = float("nan")
        elif l == 1:
            value = values[0]
        else:
            value = values
        final_row.append(value)
    return final_row

def format_line(line: str, sep: str = ";") -> tuple[bool, list]:
    values = line.split(sep)
    processed_line = []
    for value in values:
        temp = value[1:-1]
        match = re.match(r"\d+", temp)
        if match and match.span()[-1] == len(temp):
            temp = int(temp)
        processed_line.append(temp)
    if processed_line[0]:
        return True, processed_line
    return False, processed_line


def save_data(data: list[list], save_path: str):
    with open(save_path, "w", encoding = "utf-8") as file:
        writer = csv.writer(file)
        writer.writerows(data)

def load_data(folder_path: str) -> list[list]:
    files = os.listdir(folder_path)
    data = []
    for i, file in enumerate(tqdm(files, desc = "Processing files")):
        file_path = folder_path + file
        file_data = load_sabi_file(file_path)
        if i > 0:
            file_data = file_data[1:]
        data.extend(file_data)
    return data

def main(folder_path: str, save_path: str):
    data = load_data(folder_path)
    save_data(data, save_path)

if __name__ == "__main__":
    with open("config/parameters.yaml", "r") as file:
        config = yaml.safe_load(file)
    sabi_path = config["raw_data"]["sabi_data_path"]
    companies_path = config["processed_data"]["companies_path"]
    main(folder_path = sabi_path, save_path = companies_path)