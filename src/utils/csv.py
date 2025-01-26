import csv
import pandas as pd

def open_csv(path: str) -> pd.DataFrame:
    """
    Open the data stored in a csv format.
    """
    df = pd.read_csv(path,
                     dtype=str)
    return df

def save_csv(data, save_path: str, row_type = "dict"):
    """
    Saves the data in a csv format.

    """
    if isinstance(data, pd.DataFrame):
        data.to_csv(save_path, index = False, quotechar = '"', quoting = csv.QUOTE_NONNUMERIC)
        print(f"Saved {len(data):,} rows")
    else:
        with open(save_path, "w", encoding = "utf-8") as file:
            if row_type == "dict":
                writer = csv.writer(file, quotechar = '"', quoting = csv.QUOTE_NONNUMERIC)
                header = list(data[0].keys())
                writer.writerow(header)
                for row in data[1:]:
                    r_data = list(row.values())
                    writer.writerow(r_data)
                print(f"Saved {len(data)-1:,} rows")
            elif row_type == "list":
                writer = csv.writer(file, quotechar = '"', quoting = csv.QUOTE_NONNUMERIC)
                writer.writerows(data)
                print(f"Saved {len(data):,} rows")
            else:
                raise ValueError("Incorrect row type. It should be 'dict' or 'list'")