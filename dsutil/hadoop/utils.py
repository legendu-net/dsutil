"""Utils functions for Hadoop.
"""
from typing import Union, List
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, spark_partition_id, rank, coalesce, lit, max, sum


def sample(
    frame: DataFrame,
    ratio: Union[float, int],
    total: Union[int, None] = None
) -> DataFrame:
    """Sample rows from a PySpark DataFrame.

    :param frame: The PySpark DataFrame from which to sample rows.
    :param ratio: The acceptance ratio or the number of rows to sample.
    :param total: The total number of rows in the DataFrame.
    :return: A PySpark DataFrame containing sampled rows.
    """
    if isinstance(ratio, int):
        if total is None:
            frame.persist()
            total = frame.count()
        ratio /= total
    if ratio >= 1:
        return frame
    return frame.sample(ratio)


def calc_global_rank(frame: DataFrame, order_by: Union[str, List[str]]) -> DataFrame:
    """Calculate global ranks.
    This function uses a smart algorithm to avoding shuffling all rows
    to a single node which causes OOM.

    :param frame: A PySpark DataFrame. 
    :param order_by: The columns to sort the DataFrame by. 
    :return: A DataFrame with new columns ("part_id", "local_rank", "cum_rank", "sum_factor" and "rank") added.
    """
    if isinstance(order_by, str):
        order_by = [order_by]
    # calculate local rank
    wspec1 = Window.partitionBy("part_id").orderBy(*order_by)
    frame_local_rank = frame.orderBy(order_by).withColumn(
        "part_id", spark_partition_id()
    ).withColumn("local_rank",
                 rank().over(wspec1)).persist()
    # calculate accumulative rank
    wspec2 = Window.orderBy("part_id").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    stat = frame_local_rank.groupBy("part_id").agg(
        max("local_rank").alias("max_rank")
    ).withColumn("cum_rank",
                 sum("max_rank").over(wspec2))
    # self join and shift 1 row to get sum factor
    stat2 = stat.alias("l").join(
        stat.alias("r"),
        col("l.part_id") == col("r.part_id") + 1, "left_outer"
    ).select(col("l.part_id"),
             coalesce(col("r.cum_rank"), lit(0)).alias("sum_factor"))
    return frame_local_rank.join(
        #broadcast(stat2),
        stat2,
        ["part_id"],
    ).withColumn("rank",
                 col("local_rank") + col("sum_factor"))
