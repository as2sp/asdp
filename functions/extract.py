from pyspark.sql import SparkSession, DataFrame


def extractor_jdbc(spark: SparkSession, url: str, table_name: str, **params) -> DataFrame:
    df = spark.read.format("jdbc").options(
        url=url,
        dbtable=table_name,
        driver='org.postgresql.Driver',
    ).load()
    return df
