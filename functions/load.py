from logging_config import setup_logging, loader_logger
from pyspark.sql import DataFrame

setup_logging()


def loader_jdbc(df: DataFrame, url: str, table_name: str, **params) -> None:
    loader_logger.info(f"Started loader_jdbc function")
    loader_logger.debug(f"loader_jdbc called with arguments: {locals()}")
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .mode("overwrite") \
        .save()
