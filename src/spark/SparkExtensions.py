def read_csv(spark, path, delimiter=";"):
    """
    Standardized CSV reader for BatchJobs.
    """
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("sep", delimiter) \
        .csv(path)