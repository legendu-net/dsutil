"""Pandas DataFrame related utils.
"""

from pathlib import Path
from loguru import logger
import pandas as pd
from ydata_profiling import ProfileReport


def table_2w(
    frame: pd.DataFrame | pd.Series,
    columns: str | list[str] | None,
    na_as=None,
) -> pd.DataFrame:
    """Create 2-way table from columns of a DataFrame.

    :param frame: A pandas DataFrame.
    :param columns: Columns based on which to generate 2-way tables.
    :param na_as: The value to replace NAs.
    :raises TypeError: If frame is neither a pandas DataFrame nor a Series.
    :return: A 2-way table as a pandas DataFrame.
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


def read_csv(path: str | Path, **kwargs) -> pd.DataFrame:
    """Read many CSV files into a DataFrame at once.

    :param path: A path to a CSV file or to a directory containing CSV files.
    :param kwargs: Additional arguments to pass to pandas::read_csv.
    :return: A pandas DataFrame.
    """
    if isinstance(path, str):
        path = Path(path)
    if path.is_file():
        return pd.read_csv(path, **kwargs)
    return pd.concat(pd.read_csv(csv, **kwargs) for csv in path.glob("*.csv"))
