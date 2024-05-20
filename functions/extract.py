from logging_config import setup_logging, extractor_logger
from pyspark.sql import SparkSession, DataFrame

setup_logging()


def extractor_jdbc(spark: SparkSession, url: str, table_name: str, **params) -> DataFrame:
    extractor_logger.info(f"Started extractor_jdbc function")
    extractor_logger.debug(f"extractor_jdbc called with arguments: {locals()}")
    df = spark.read.format("jdbc").options(
        url=url,
        dbtable=table_name,
        driver='org.postgresql.Driver',
    ).load()
    return df
