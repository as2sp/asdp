from logging_config import transformer_logger
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import lit
from pyspark.sql.types import DecimalType, ArrayType, TimestampType, DateType, BooleanType, StringType, DoubleType, \
    IntegerType


def set_columns_to_null(df: DataFrame, column_names: list) -> DataFrame:
    """
    Sets specified columns to null in the given DataFrame.

    :param df: The Spark DataFrame to be modified.
    :param column_names: A list of column names to be set to null.
    :return: The modified DataFrame with specified columns set to null.
    """
    transformer_logger.info(f"Started set_columns_to_null function")
    transformer_logger.debug(f"set_columns_to_null called with arguments: {locals()}")
    for column_name in column_names:
        df = df.withColumn(column_name, F.lit(None).cast(df.schema[column_name].dataType))
    return df


def rename_columns(df: DataFrame, rename_cols) -> DataFrame:
    """
    Renames columns in the given DataFrame.

    :param df: The Spark DataFrame to be modified.
    :param rename_cols: A dictionary where keys are old column names and values are new column names.
    :return: The modified DataFrame with columns renamed.
    """
    transformer_logger.info(f"Started rename_columns function")
    transformer_logger.debug(f"rename_columns called with arguments: {locals()}")
    for old_column, new_column in rename_cols.items():
        df = df.withColumnRenamed(old_column, new_column)
    return df


def drop_columns(df: DataFrame, drop_cols: list) -> DataFrame:
    """
    Drops specified columns from the given DataFrame.

    :param df: The Spark DataFrame to be modified.
    :param drop_cols: A list of column names to be dropped.
    :return: The modified DataFrame with specified columns dropped.
    """
    transformer_logger.info(f"Started drop_columns function")
    transformer_logger.debug(f"drop_columns called with arguments: {locals()}")
    if not drop_cols:
        return df
    else:
        for col in drop_cols:
            df = df.drop(col)
    return df


def drop_duplicates(df: DataFrame, drop_dup_cols: list) -> DataFrame:
    """
    Drops duplicate rows from the given DataFrame.

    :param df: The Spark DataFrame to be modified.
    :param drop_dup_cols: A list of column names to consider for identifying duplicates. If empty, considers all
    columns.
    :return: The modified DataFrame with duplicate rows dropped.
    """
    transformer_logger.info(f"Started drop_duplicates function")
    transformer_logger.debug(f"drop_duplicates called with arguments: {locals()}")
    if not drop_dup_cols:
        df = df.dropDuplicates()
    else:
        df = df.dropDuplicates(subset=drop_dup_cols)
    return df


def drop_rows_with_null_in_notnull_columns(df: DataFrame, not_null: list) -> DataFrame:
    """
    Drops rows with null values in specified columns from the given DataFrame.

    :param df: The Spark DataFrame to be modified.
    :param not_null: A list of column names that should not have null values.
    :return: The modified DataFrame with rows containing nulls in specified columns dropped.
    """
    transformer_logger.info(f"Started drop_rows_with_null_in_notnull_columns function")
    transformer_logger.debug(f"drop_rows_with_null_in_notnull_columns called with arguments: {locals()}")
    if not not_null:
        return df
    else:
        for col in not_null:
            df = df.filter(df[col].isNotNull())
    return df


def fill_columns_with_value(df: DataFrame, cols) -> DataFrame:
    """
    Fills specified columns with given values in the given DataFrame.

    :param df: The Spark DataFrame to be modified.
    :param cols: A dictionary where keys are column names and values are the values to fill in.
    :return: The modified DataFrame with specified columns filled with given values.
    """
    transformer_logger.info(f"Started fill_columns_with_value function")
    transformer_logger.debug(f"fill_columns_with_value called with arguments: {locals()}")
    for col_name, col_value in cols.items():
        df = df.withColumn(col_name, F.lit(col_value))
    return df


def fill_nulls_with_value(df: DataFrame, col, new_val) -> DataFrame:
    """
    Fills null values in a specified column with a given value.

    :param df: The Spark DataFrame to be modified.
    :param col: The column name where null values should be filled.
    :param new_val: The value to fill in place of nulls.
    :return: The modified DataFrame with null values in specified column filled.
    """
    transformer_logger.info(f"Started fill_nulls_with_value function")
    transformer_logger.debug(f"fill_nulls_with_value called with arguments: {locals()}")
    return df.fillna({col: new_val})


def set_column_equal_another_column(df: DataFrame, src_col, dest_col) -> DataFrame:
    """
    Sets a column's value equal to another column's value in the given DataFrame.

    :param df: The Spark DataFrame to be modified.
    :param src_col: The source column name.
    :param dest_col: The destination column name.
    :return: The modified DataFrame with destination column values set to source column values.
    """
    transformer_logger.info(f"Started set_column_equal_another_column function")
    transformer_logger.debug(f"set_column_equal_another_column called with arguments: {locals()}")
    return df.withColumn(dest_col, df[src_col])


def add_postgres_columns(df: DataFrame, new_cols) -> DataFrame:
    """
    Adds new columns with specified types to the given DataFrame, initializing them with null values.

    :param df: The Spark DataFrame to be modified.
    :param new_cols: A dictionary where keys are column names and values are their data types (e.g., 'string',
    'bool', 'int').
    :return: The modified DataFrame with new columns added.
    """
    transformer_logger.info(f"Started add_postgres_columns function")
    transformer_logger.debug(f"add_postgres_columns called with arguments: {locals()}")
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
