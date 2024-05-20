import logging
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger('extractors')


def extractor_jdbc(spark: SparkSession, url: str, table_name: str, **params) -> DataFrame:

    logger.info(f"Started extractor_jdbc function")
    logger.debug(f"extractor_jdbc called with arguments: {locals()}")

    df = spark.read.format("jdbc").options(
        url=url,
        dbtable=table_name,
        driver='org.postgresql.Driver',
    ).load()
    return df
