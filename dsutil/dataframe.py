"""Pandas DataFrame related utils.
"""
import os
import re
import subprocess as sp
from typing import List, Sequence, Union
import pandas as pd


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


def read_csv(path: str, **kwargs):
    """Read many CSV files into a DataFrame at once.
    """
    if isinstance(path, str):
        path = Path(path)
    if path.is_file():
        return pd.read_csv(path, **kwargs)
    return pd.concat(pd.read_csv(csv, **kwargs) for csv in path.glob("*.csv"))
