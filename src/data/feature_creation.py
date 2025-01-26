import os
import sys
import yaml
import pandas as pd
from tqdm import tqdm
tqdm.pandas()
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.csv import open_csv, save_csv

import warnings
warnings.simplefilter("ignore")

def filter_availability(data: pd.DataFrame, availability: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the data by its availability.
    """
    availability["nif"] = availability["BVD_ID"].apply(lambda x: x[2:])
    filtered = pd.merge(data, availability, on = "nif", how = "inner")
    filtered = filtered[data.columns]
    return filtered

def fix_url(row):
    """
    Checks the existence of redirected url.
    """
    if str(row["redirect_url"]) == "nan":
        row["redirect_url"] = row["sabi_url"]
    return row

def add_redirects(data: pd.DataFrame, redirects: pd.DataFrame) -> pd.DataFrame:
    """
    Adds the redirected url.
    """
    redirects.columns = ["", "id", "sabi_url", "redirect_url"]
    data_redirected = pd.merge(data, redirects, on = "sabi_url", how = "left")
    data_redirected = data_redirected[list(data.columns) + ["redirect_url"]]
    data_redirected = data_redirected.progress_apply(fix_url, axis = 1)
    data_redirected = data_redirected.drop_duplicates()
    return data_redirected

def feature_creation(config: dict):
    """
    Filters the data by its availability and adds the redirected url.
    """
    filtered_path = config["data"]["filtered_path"]
    availability_path = config["data"]["availability_path"]
    redirects_path = config["data"]["redirects_path"]
    companies_path = config["data"]["companies_path"]
    data = open_csv(filtered_path)
    print("Filtering companies by web availability")
    availability = open_csv(availability_path)
    data = filter_availability(data, availability)
    print("Adding redirected url")
    redirects = open_csv(redirects_path)
    data = add_redirects(data, redirects)
    save_csv(data, companies_path)

if __name__ == "__main__":
    with open("config/parameters.yaml", "r") as file:
        config = yaml.safe_load(file)
    feature_creation(config)