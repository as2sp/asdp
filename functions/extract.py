from logging_config import extractor_logger
from pyspark.sql import SparkSession, DataFrame
from utils import *


def extractor_csv(spark: SparkSession, **params) -> DataFrame:
    """
    Extracts data from a CSV file into a Spark DataFrame.

    :param spark: The Spark session object.
    :param params: Dictionary containing the path, header, and inferSchema parameters for reading the CSV file.
    :return: A Spark DataFrame containing the data from the CSV file.
    """
    extractor_logger.info(f"Started extractor_csv function")
    extractor_logger.debug(f"extractor_csv called with arguments: {locals()}")
    df = spark.read.csv(
        path=params["path"],
        header=params["header"],
        inferSchema=params["inferSchema"],
    )
    return df


def extractor_jdbc(spark: SparkSession, url: str, table_name: str, **params) -> DataFrame:
    """
    Extracts data from a JDBC source into a Spark DataFrame.

    :param spark: The Spark session object.
    :param url: The JDBC URL for the database connection.
    :param table_name: The name of the table to extract data from.
    :param params: Additional parameters, including partitioning options.
    :return: A Spark DataFrame containing the data from the JDBC source.
    """
    extractor_logger.info(f"Started extractor_jdbc function")
    extractor_logger.debug(f"extractor_jdbc called with arguments: {locals()}")
    spark_reader = spark.read.format("jdbc").options(
        url=url,
        dbtable=table_name,
        driver='org.postgresql.Driver',
    )
    spark_reader = add_partitioning_options(spark_reader, params)
    df = spark_reader.load()
    return df


def extractor_jdbc_with_sql(spark: SparkSession, url: str, sql_query: str, **params) -> DataFrame:
    """
    Extracts data from a JDBC source using a SQL query into a Spark DataFrame.

    :param spark: The Spark session object.
    :param url: The JDBC URL for the database connection.
    :param sql_query: The SQL query to execute for data extraction.
    :param params: Additional parameters, including partitioning options.
    :return: A Spark DataFrame containing the data from the SQL query.
    """
    extractor_logger.info("Started extractor_jdbc_with_sql function")
    extractor_logger.debug(f"extractor_jdbc_with_sql called with arguments: {locals()}")
    spark_reader = spark.read.format("jdbc").options(
        url=url,
        query=sql_query,
        driver='org.postgresql.Driver',
    )
    spark_reader = add_partitioning_options(spark_reader, params)
    df = spark_reader.load()
    return df


def extractor_jdbc_with_column_mapping(spark: SparkSession, url: str, table_name: str, col_map: dict, **params) \
        -> DataFrame:
    """
    Extracts data from a JDBC source into a Spark DataFrame and renames columns based on a given mapping.

    :param spark: The Spark session object.
    :param url: The JDBC URL for the database connection.
    :param table_name: The name of the table to extract data from.
    :param col_map: Dictionary mapping old column names to new column names.
    :param params: Additional parameters, including partitioning options.
    :return: A Spark DataFrame containing the data from the JDBC source with columns renamed.
    """
    extractor_logger.info("Started extractor_jdbc_with_column_mapping function")
    extractor_logger.debug(f"extractor_jdbc_with_column_mapping called with arguments: {locals()}")
    spark_reader = spark.read.format("jdbc").options(
        url=url,
        dbtable=table_name,
        driver='org.postgresql.Driver',
    )
    spark_reader = add_partitioning_options(spark_reader, params)
    df = spark_reader.load()
    for old_col, new_col in col_map.items():
        df = df.withColumnRenamed(old_col, new_col)
    return df
