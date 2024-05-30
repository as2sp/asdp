from logging_config import loader_logger
from pyspark.sql import DataFrame


def loader_csv(df: DataFrame, **params) -> None:
    loader_logger.info(f"Started loader_csv function")
    loader_logger.debug(f"loader_csv called with arguments: {locals()}")
    df.write.csv(
        path=params["path"],
        mode=params["mode"],
        header=params["header"],
    )
    return None


def loader_jdbc(df: DataFrame, url: str, table_name: str, mode: str,  **params) -> None:
    """
    Loads data from a Spark DataFrame into a JDBC database.

    :param df: The Spark DataFrame to be written to the JDBC database.
    :param url: The JDBC URL for the database connection.
    :param table_name: The name of the table to write data to.
    :param mode: Load type. Must be 'overwrite' or 'append'.
                 - 'overwrite': All existing data in the table will be deleted and replaced by the new data.
                 - 'append': The new data from the DataFrame will be added to the existing data in the table.
    :param params: Additional parameters for the write operation.
    :return: None
    """
    if mode not in ["overwrite", "append"]:
        raise ValueError("Mode must be either 'overwrite' or 'append'")

    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .mode(mode) \
        .save()
    return None
