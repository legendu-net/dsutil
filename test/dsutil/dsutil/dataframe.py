import os
import re
import pandas as pd
import subprocess as sp
from typing import List, Sequence, Union


def table_2w(frame: pd.DataFrame, columns: List[str], na_as=None):
    """Create 2-way table from columns of a DataFrame.
    """
    if na_as is not None:
        frame = frame.fillna(na_as)
    if type(frame) == pd.Series:
        df = frame.unstack()
        df.index = pd.MultiIndex.from_product([[df.index.name], df.index.values])
        df.columns = pd.MultiIndex.from_product([[df.columns.name], df.columns.values])
        return df
    if isinstance(frame, pd.DataFrame):
        return table_2w(frame[columns].groupby(columns).size())  # pylint: disable=E1120
    raise TypeError('"frame" must be pandas.Series or pandas.DataFrame.')


def read_csv(path: str, **kwargs):
    """Read many CSV files into a DataFrame at once.
    """
    if os.path.isfile(path):
        return pd.read_csv(path, **kwargs)
    frame_list = []
    if os.path.isdir(path):
        for file in os.listdir(path):
            if os.path.splitext(file)[1].lower() == '.csv':
                file = os.path.join(path, file)
                frame_list.append(pd.read_csv(file, **kwargs))
    return pd.concat(frame_list)
