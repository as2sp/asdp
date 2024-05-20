from logging_config import loader_logger
from pyspark.sql import DataFrame


def loader_jdbc(df: DataFrame, url: str, table_name: str, **params) -> None:
    loader_logger.info(f"Started loader_jdbc function")
    loader_logger.debug(f"loader_jdbc called with arguments: {locals()}")
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .mode("overwrite") \
        .save()
