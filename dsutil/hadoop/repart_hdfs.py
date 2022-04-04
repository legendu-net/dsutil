"""Repartition a HDFS path of the Parquet format. 
"""
from typing import Optional
from argparse import ArgumentParser, Namespace
from pyspark.sql import SparkSession
from .utils import repart_hdfs

spark = SparkSession.builder.appName("Repart_HDFS").enableHiveSupport().getOrCreate()


def parse_args(args=None, namespace=None) -> Namespace:
    """Parse command-line arguments.

    :param args: The arguments to parse.
        If None, the arguments from command-line are parsed.
    :param namespace: An inital Namespace object.
    :return: A namespace object containing parsed options.
    """
    parser = ArgumentParser(
        description="Repartition a HDFS path which is of the Parquet format."
    )
    parser.add_argument(
        "-p",
        "--path",
        "--hdfs-path",
        dest="hdfs_path",
        type=str,
        help="The HDFS path (of the Parquet format) to repartition."
    )
    parser.add_argument(
        "-n",
        "--num-parts",
        dest="num_parts",
        type=int,
        help="The new number of partitions."
    )
    return parser.parse_args(args=args, namespace=namespace)


def main(args: Optional[Namespace] = None):
    """The main function for script usage.
    """
    if args is None:
        args = parse_args()
    repart_hdfs(spark, src_path=args.hdfs_path, num_parts=args.num_parts)


if __name__ == "__main__":
    main()
