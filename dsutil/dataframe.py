"""Pandas DataFrame related utils.
"""
from typing import List, Union
from pathlib import Path
from loguru import logger
import pandas as pd
from pandas_profiling import ProfileReport


def table_2w(frame: pd.DataFrame, columns: Union[str, List[str], None], na_as=None):
    """Create 2-way table from columns of a DataFrame.
    """
    if na_as is not None:
        frame = frame.fillna(na_as)
    if isinstance(frame, pd.Series):
        df = frame.unstack()
        df.index = pd.MultiIndex.from_product([[df.index.name], df.index.values])
        df.columns = pd.MultiIndex.from_product([[df.columns.name], df.columns.values])
        return df
    if isinstance(frame, pd.DataFrame):
        if isinstance(columns, str):
            columns = [columns]
        return table_2w(frame[columns].groupby(columns).size(), columns=None)
    raise TypeError('"frame" must be pandas.Series or pandas.DataFrame.')


def read_csv(path: Union[str, Path], **kwargs):
    """Read many CSV files into a DataFrame at once.

    :param kwargs: Additional arguments to pass to pandas::read_csv.
    """
    if isinstance(path, str):
        path = Path(path)
    if path.is_file():
        return pd.read_csv(path, **kwargs)
    return pd.concat(pd.read_csv(csv, **kwargs) for csv in path.glob("*.csv"))


def dump_profile(
    df: Union[pd.DataFrame, str, Path], title: str, output_dir: Union[str, Path]
):
    """Run pandas profiling on a DataFrame and dump the report into files.

    :param df: A pandas DataFrame.
    :param title: The title of the generated report.
    :param output_dir: The output directory for reports.
    """
    if isinstance(df, str):
        df = Path(df)
    if isinstance(df, Path):
        logger.info("Reading the DataFrame from {}...", df)
        ext = df.suffix.lower()
        if ext == ".parquet":
            df = pd.read_parquet(df)
        elif ext == ".pickle":
            df = pd.read_pickle(df)
        elif ext == ".csv":
            df = pd.read_csv(df)
        else:
            raise ValueError("Only Parquet, Pickle and CSV files are support!")
    logger.info("Shape of the DataFrame: {}", df.shape)
    logger.info("Profiling the DataFrame...")
    report = ProfileReport(df, title=title, minimal=True, explorative=True)
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    # dump report
    logger.info("Dumping the report to HTML...")
    report.to_file(output_dir / "report.html")
    logger.info("Dumping the report to JSON...")
    report.to_file(output_dir / "report.json")
    logger.info("Dumping the report to Pickle...")
    report.dump(output_dir / "report.pickle")
