import time
import tracemalloc
import pandas as pd

from utils import (
    INVENTORIES_FILE, 
    INVENTORY_PARTS_FILE, 
    PARTS_FILE,
    RESULT_FILE, 
    print_df
)

tracemalloc.start()
start_time = time.time()

invetories_df = pd.read_csv(INVENTORIES_FILE)
inventory_parts_df = pd.read_csv(INVENTORY_PARTS_FILE)
parts_df = pd.read_csv(PARTS_FILE)

df = invetories_df.merge( 
    right=inventory_parts_df, 
    how="outer", 
    left_on="id", 
    right_on="inventory_id"
).merge(parts_df, how="outer", on="part_num").drop(columns=["id"])
# print_df("Base data", df)

agg_by_part_df = df.groupby("part_num").agg(
    inventory_num=("inventory_id", "count"),
    popular_inventories=("inventory_id", pd.Series.mode),
    color_num=("color_id", "count"),
    popular_colors=("color_id", pd.Series.mode)
).reset_index()
# print_df("Agg by part_num", agg_by_part_df)

agg_by_inventory_df = df.groupby("inventory_id").agg(
    part_num=("part_num", "count"),
    popular_parts=("part_num", pd.Series.mode),
    color_num=("color_id", "count"),
    popular_colors=("color_id", lambda x: pd.Series.mode(x).to_list())   
).reset_index()
# print_df("Agg by inventory_id", agg_by_inventory_df)

part_set = set(agg_by_part_df[
    agg_by_part_df["color_num"] > 3
]["part_num"].values.tolist())
inventory_set = set(agg_by_inventory_df[
    agg_by_inventory_df["part_num"] < 5
]["inventory_id"].values.tolist())

filtered_df = df[
    df["part_num"].isin(part_set) &
    df["inventory_id"].isin(inventory_set) &
    ((df["quantity"] > 1) | (df["is_spare"] == "t"))
]
# print_df("Filtered", filtered_df)

filtered_df.to_csv(RESULT_FILE)

print("Time result (elapsed seconds): ", time.time() - start_time)
print("Memory: ", tracemalloc.get_traced_memory())

tracemalloc.stop()