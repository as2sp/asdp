from pyspark.sql import DataFrame
from typing import List


def joiner_func1(dfs: List[DataFrame], param1: str) -> DataFrame:
    df = dfs[0]
    for other_df in dfs[1:]:
        df = df.join(other_df, on=param1)
    return df


def joiner_func2(dfs: List[DataFrame], param1: str) -> DataFrame:
    df = dfs[0]
    for other_df in dfs[1:]:
        df = df.join(other_df, on=param1, how='left')
    return df
