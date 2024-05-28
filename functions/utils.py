def add_partitioning_options(spark_reader, params):
    """
    Adds partitioning options to the Spark reader if they are present in the params.

    :param spark_reader: The Spark reader object to which the options will be added.
    :param params: Dictionary containing the partitioning parameters.
    :return: The Spark reader object with partitioning options added.
    """
    if 'partitionColumn' in params:
        spark_reader = spark_reader.option("partitionColumn", params['partitionColumn']) \
            .option("lowerBound", params['lowerBound']) \
            .option("upperBound", params['upperBound']) \
            .option("numPartitions", params['numPartitions'])
    return spark_reader
