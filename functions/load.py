from pyspark.sql import DataFrame


def loader_jdbc(df: DataFrame, url: str, table_name: str, **params) -> None:
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .mode("overwrite") \
        .save()
