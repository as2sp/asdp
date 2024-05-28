from logging_config import loader_logger
from pyspark.sql import DataFrame


def loader_jdbc(df: DataFrame, url: str, table_name: str, **params) -> None:
    """
    Loads data from a Spark DataFrame into a JDBC database.

    :param df: The Spark DataFrame to be written to the JDBC database.
    :param url: The JDBC URL for the database connection.
    :param table_name: The name of the table to write data to.
    :param params: Additional parameters for the write operation.
    :return: None
    """
    loader_logger.info(f"Started loader_jdbc function")
    loader_logger.debug(f"loader_jdbc called with arguments: {locals()}")
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .mode("overwrite") \
        .save()
