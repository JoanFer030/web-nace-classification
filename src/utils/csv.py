import csv

def open_csv(path: str, header: bool = True) -> list[dict]:
    """
    Open the data stored in a csv format.
    """

    with open(path, "r", encoding = "utf-8") as file:
        csv_data = csv.reader(file)
        csv_data = list(csv_data)
    if header:
        header_data = csv_data[0]
        csv_data = csv_data[1:]
    else:
        header_data = [str(i) for i in range(len(csv_data[0]))]
    data = [{h: v for h, v in zip(header_data, row)} for row in csv_data]
    return data

def save_csv(data: list, save_path: str, row_type = "dict"):
    """
    Saves the data in a csv format.
    """
    if not data:
        raise ValueError("The data is empty and cannot be saved.")
    with open(save_path, "w", encoding = "utf-8") as file:
        if row_type == "dict":
            writer = csv.writer(file, quotechar = '"', quoting = csv.QUOTE_NONNUMERIC)
            header = list(data[0].keys())
            writer.writerow(header)
            for row in data[1:]:
                r_data = list(row.values())
                writer.writerow(r_data)
        elif row_type == "list":
            writer = csv.writer(file, quotechar = '"', quoting = csv.QUOTE_NONNUMERIC)
            writer.writerows(data)
        else:
            raise ValueError("Incorrect row type. It should be 'dict' or 'list'")