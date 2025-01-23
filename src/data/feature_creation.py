import socket
from tqdm import tqdm
from utils.csv import open_csv, save_csv

def create_availability_feature(data: list[dict], column_name: str, new_name: str = "url_availability") -> list[dict]:
    """
    Create a new feature, checks whether or not the web exists. 
    This check is done making a request to a DNS.
    """
    def web_exits(url: str) -> bool:
        try:
            socket.gethostbyname(url)
            return True
        except:
            return False
    new_data = []
    for row in tqdm(data, desc = "Web availability feature"):
        n_row = row.copy()
        n_row[new_name] = web_exits(n_row[column_name])
        new_data.append(n_row)
    return new_data

def create_feature(config: dict):
    """
    Feature Creation Pipeline:
    Creates the new feature, these are configured on the parameters file.
    """
    functions = {"url_availability": create_availability_feature}
    filtered_path = config["data"]["filtered_path"]
    available_path = config["data"]["available_path"]
    feature_creation = config["data_parameters"]["feature_creation"]
    data = open_csv(filtered_path)
    for col_name, func_name in feature_creation:
        func = functions[func_name]
        data = func(data, col_name)
    save_csv(data, available_path)
    print(f"Saved {len(data):,} companies from SABI")