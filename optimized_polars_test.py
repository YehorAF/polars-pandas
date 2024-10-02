import time
import tracemalloc
import polars as pl

from utils import (
    INVENTORIES_FILE, 
    INVENTORY_PARTS_FILE, 
    PARTS_FILE,
    RESULT_FILE, 
    print_df
)

tracemalloc.start()
start_time = time.time()

invetories_df = pl.scan_csv(INVENTORIES_FILE)
inventory_parts_df = pl.scan_csv(INVENTORY_PARTS_FILE)
parts_df = pl.scan_csv(PARTS_FILE)

df = invetories_df.join(
    other=inventory_parts_df, 
    how="full",
    left_on="id",
    right_on="inventory_id"
).join(parts_df, how="full", on="part_num").drop(["id"]).collect()

agg_by_part_df = df.lazy().group_by("part_num").agg(
    pl.col("inventory_id").count().alias("inventory_num"),
    pl.col("part_num").mode().alias("popular_parts"),
    pl.col("color_id").count().alias("color_num"),
    pl.col("color_id").mode().alias("populat_colors")
)

agg_by_inventory_df = df.lazy().group_by("inventory_id").agg(
    pl.col("part_num").count(),
    pl.col("part_num").mode().alias("popular_parts"),
    pl.col("color_id").count().alias("color_num"),
    pl.col("color_id").mode().alias("popular_colors")
)

part_set = set(agg_by_part_df.filter(
    pl.col("color_num") > 3
).select("part_num").collect()["part_num"].to_list())
inventory_set = set(agg_by_inventory_df.filter(
    pl.col("part_num") < 5
).select("inventory_id").collect()["inventory_id"].to_list())

filtered_df = df.filter(
    pl.col("part_num").is_in(part_set) &
    pl.col("inventory_id").is_in(inventory_set) &
    ((pl.col("quantity") > 1) | (pl.col("is_spare") == "t"))
)
# print_df("Filtered", filtered_df)

filtered_df.write_csv(RESULT_FILE)

print("Time result (elapsed seconds): ", time.time() - start_time)
print("Memory: ", tracemalloc.get_traced_memory())

tracemalloc.stop()