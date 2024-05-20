from logging_config import transformer_logger
from typing import Any, List
import sqlalchemy as sa
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import lit
from pyspark.sql.types import DecimalType, ArrayType, TimestampType, DateType, BooleanType, StringType, DoubleType, \
    IntegerType


def set_columns_to_null(df: DataFrame, column_names: list) -> DataFrame:
    transformer_logger.info(f"Started set_columns_to_null function")
    transformer_logger.debug(f"extractor_jdbc called with arguments: {locals()}")
    for column_name in column_names:
        df = df.withColumn(column_name, F.lit(None).cast(df.schema[column_name].dataType))
    return df


def rename_cols(df: DataFrame, rename_cols) -> DataFrame:
    transformer_logger.info(f"Started rename_cols function")
    transformer_logger.debug(f"rename_cols called with arguments: {locals()}")
    for old_column, new_column in rename_cols.items():
        df = df.withColumnRenamed(old_column, new_column)
    return df


def drop_cols(df: DataFrame, drop_cols: list) -> DataFrame:
    transformer_logger.info(f"Started drop_cols function")
    transformer_logger.debug(f"drop_cols called with arguments: {locals()}")
    if not drop_cols:
        return df
    else:
        for col in drop_cols:
            df = df.drop(col)
    return df


def drop_duplicates(df: DataFrame, drop_dup_cols: list) -> DataFrame:
    transformer_logger.info(f"Started drop_duplicates function")
    transformer_logger.debug(f"drop_duplicates called with arguments: {locals()}")
    if not drop_dup_cols:
        df = df.dropDuplicates()
    else:
        df = df.dropDuplicates(subset=drop_dup_cols)
    return df


def drop_rows_with_null_in_notnull_cols(df: DataFrame, not_null: list) -> DataFrame:
    transformer_logger.info(f"Started drop_rows_with_null_in_notnull_cols function")
    transformer_logger.debug(f"drop_rows_with_null_in_notnull_cols called with arguments: {locals()}")
    if not not_null:
        return df
    else:
        for col in not_null:
            df = df.filter(df[col].isNotNull())
    return df


def fill_cols_with_value(df: DataFrame, cols) -> DataFrame:
    transformer_logger.info(f"Started fill_cols_with_value function")
    transformer_logger.debug(f"fill_cols_with_value called with arguments: {locals()}")
    for col_name, col_value in cols.items():
        df = df.withColumn(col_name, F.lit(col_value))
    return df


def fill_nulls(df: DataFrame, col, new_val) -> DataFrame:
    transformer_logger.info(f"Started fill_nulls function")
    transformer_logger.debug(f"fill_nulls called with arguments: {locals()}")
    return df.fillna({col: new_val})


def set_col_equal_another_col(df: DataFrame, src_col, dest_col) -> DataFrame:
    transformer_logger.info(f"Started set_col_equal_another_col function")
    transformer_logger.debug(f"set_col_equal_another_col called with arguments: {locals()}")
    return df.withColumn(dest_col, df[src_col])


def add_postgres_cols(df: DataFrame, new_cols) -> DataFrame:
    transformer_logger.info(f"Started add_postgres_cols function")
    transformer_logger.debug(f"add_postgres_cols called with arguments: {locals()}")
    for col_name, col_type in new_cols.items():
        if col_type == 'string':
            col_type = StringType()
        elif col_type == 'bool':
            col_type = BooleanType()
        elif col_type == 'int':
            col_type = IntegerType()
        elif col_type == 'numeric':
            col_type = DecimalType()
        elif col_type == 'double':
            col_type = DoubleType()
        elif col_type == 'date':
            col_type = DateType()
        elif col_type == 'timestamp':
            col_type = TimestampType()
        elif col_type == 'array':
            col_type = ArrayType()
        df = df.withColumn(col_name, lit(None).cast(col_type))
    return df
