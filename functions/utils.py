def add_partitioning_options(spark_reader, params):
    if 'partitionColumn' in params:
        spark_reader = spark_reader.option("partitionColumn", params['partitionColumn']) \
            .option("lowerBound", params['lowerBound']) \
            .option("upperBound", params['upperBound']) \
            .option("numPartitions", params['numPartitions'])
    return spark_reader
