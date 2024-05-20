import logging
from pyspark.sql import DataFrame

logger = logging.getLogger('loaders')


def loader_jdbc(df: DataFrame, url: str, table_name: str, **params) -> None:

    logger.info(f"Started loader_jdbc function")
    logger.debug(f"loader_jdbc called with arguments: {locals()}")

    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .mode("overwrite") \
        .save()
