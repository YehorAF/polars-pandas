from datetime import datetime
from typing import Any
import os

INVENTORIES_FILE = os.path.join(
    os.path.abspath("."), 
    "datasets/lego/inventories.csv"
)
INVENTORY_PARTS_FILE = os.path.join(
    os.path.abspath("."), 
    "datasets/lego/inventory_parts.csv"
)
PARTS_FILE = os.path.join(
    os.path.abspath("."), 
    "datasets/lego/parts.csv"
)
RESULT_FILE = os.path.join(
    os.path.abspath("."),
    "datasets/lego/result.csv"
)


def print_df(header: str, df: Any):
    print("= "*20, header, " ="*20)
    print("Len: ", len(df))
    print("Columns: ", df.columns)
    print("Head:\n", df.head())
    print("Description:\n", df.describe())
    print(end="\n\n")