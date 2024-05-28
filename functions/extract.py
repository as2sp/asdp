from logging_config import extractor_logger
from pyspark.sql import SparkSession, DataFrame
from utils import *


def extractor_csv(spark: SparkSession, **params) -> DataFrame:
    extractor_logger.info(f"Started extractor_csv function")
    extractor_logger.debug(f"extractor_csv called with arguments: {locals()}")
    df = spark.read.csv(
        path=params["path"],
        header=params["header"],
        inferSchema=params["inferSchema"],
    )
    return df


def extractor_jdbc(spark: SparkSession, url: str, table_name: str, **params) -> DataFrame:
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


def extractor_jdbc_with_sql(spark: SparkSession, url: str, sql_query: str, col_map: dict, **params) -> DataFrame:
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


def extractor_jdbc_with_col_map(spark: SparkSession, url: str, table_name: str, col_map: dict, **params) -> DataFrame:
    extractor_logger.info("Started extractor_jdbc_with_col_map function")
    extractor_logger.debug(f"extractor_jdbc_with_col_map called with arguments: {locals()}")
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
