"""Utils functions for Hadoop.
"""
from __future__ import annotations
from typing import Union
import sys
import datetime
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, spark_partition_id, rank, coalesce, lit, max, sum


def sample(
    frame: DataFrame,
    ratio: Union[float, int],
    total: Union[int, None] = None,
    persist: bool = False
) -> DataFrame:
    """Sample rows from a PySpark DataFrame.

    :param frame: The PySpark DataFrame from which to sample rows.
    :param ratio: The acceptance ratio or the number of rows to sample.
    :param total: The total number of rows in the DataFrame.
    :param persist: Whether to persist the table when it is needed multiple times.
        False by default.
    :return: A PySpark DataFrame containing sampled rows.
    """
    if isinstance(ratio, int):
        if total is None:
            if persist:
                frame.persist()
            total = frame.count()
        if total == 0:
            return frame
        ratio /= total
    if ratio >= 1:
        return frame
    return frame.sample(ratio)


def calc_global_rank(frame: DataFrame, order_by: Union[str, list[str]]) -> DataFrame:
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


def repart_hdfs(
    spark,
    path: str,
    num_parts: Optional[int] = None,
    mb_per_part: float = 64,
    min_num_parts: int = 1,
    coalesce: bool = False
) -> None:
    """Repartition a HDFS path of the Parquet format.

    :param spark: A SparkSession object. 
    :param path: The HDFS path to repartition. 
    :param num_parts: The new number of partitions. 
    :param coalesce: If True, use coalesce instead of repartition.
    """
    sc = spark.sparkContext
    hdfs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())  # pylint: disable=W0212
    path = path.rstrip("/")
    path_hdfs = sc._jvm.org.apache.hadoop.fs.Path(path)
    # num of partitions
    if num_parts is None:
        bytes_path = hdfs.getContentSummary(path_hdfs).getLength()
        num_parts = round(bytes_path / 1_048_576 / mb_per_part)
    num_parts = max(num_parts, min_num_parts)
    # temp path for repartitioned table
    ts = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
    path_tmp = path + f"_repart_tmp_{ts}"
    # repartition
    if coalesce:
        spark.read.parquet(path).coalesce(num_parts) \
            .write.mode("overwrite").parquet(path_tmp)
    else:
        spark.read.parquet(path).repartition(num_parts) \
            .write.mode("overwrite").parquet(path_tmp)
    # rename path
    if hdfs.delete(path_hdfs, True):  # pylint: disable=W0212
        if not hdfs.rename(
            sc._jvm.org.apache.hadoop.fs.Path(path_tmp),  # pylint: disable=W0212
            path_hdfs,  # pylint: disable=W0212
        ):
            sys.exit(f"Failed to rename the HDFS path {path_tmp} to {path}!")
    else:
        sys.exit(f"Failed to remove the (old) HDFS path: {path}!")
