def calc_global_rank(args: Namespace) -> DataFrame:
    """Calculate global ranks.
    This function uses a smart algorithm to avoding shuffling all items
    to a single node which causes OOM.
    :return: A DataFrame containing item metrics from both marketing and site.
    """
    item = combine_market_site(args)
    # calculate local rank
    wspec1 = Window.partitionBy("part_id").orderBy(
        col("col1").desc(),
        col("col2"),
        col("col3").desc(),
        col("col4").desc(),
        col("col5").desc(),
    )
    item_local_rank = item.orderBy(
        [
            "col1",  # descending
            "col2",  # ascending
            "col3",  # descending
            "col4",  # descending
            "col5",  # descending
        ],
        ascending=[
            False,
            True,
            False,
            False,
            False,
        ]
    ).withColumn("part_id",
                 spark_partition_id()).withColumn("local_rank",
                                                  rank().over(wspec1)).persist()
    # calculate accumulative rank
    wspec2 = Window.orderBy("part_id").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    stat = item_local_rank.groupBy("part_id").agg(
        max("local_rank").alias("max_rank")
    ).withColumn("cum_rank",
                 sum("max_rank").over(wspec2))
    # self join and shift 1 row to get sum factor
    stat2 = stat.alias("l").join(
        stat.alias("r"),
        col("l.part_id") == col("r.part_id") + 1, "left_outer"
    ).select(col("l.part_id"),
             coalesce(col("r.cum_rank"), lit(0)).alias("sum_factor"))
    return item_local_rank.join(
        #broadcast(stat2),
        stat2,
        ["part_id"],
    ).withColumn("rank",
                 col("local_rank") + col("sum_factor"))