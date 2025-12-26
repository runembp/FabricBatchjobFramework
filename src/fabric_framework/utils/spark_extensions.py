from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F, SparkSession


def read_csv(self, batchjob_id, path, delimiter=";"):
    """
    Standardized CSV reader for BatchJobs.
    """
    csv_path = f"data/test.csv"

    dataframe = self.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("sep", delimiter) \
        .csv(path)

    if dataframe is not None and len(dataframe.columns) > 0:
        return dataframe

    dataframe = self.read.table(f"{batchjob_id}_transformed")

    if dataframe is not None and len(dataframe.columns) > 0:
        return dataframe

    return None


def increment_retry_count_on_failed_records(self, batchjob_id: str):
    try:
        failed_df = self.table(f"{batchjob_id}_ingest") \
            .filter("VALID = 'INVALID'") \
            .withColumn("retry_count", F.col("retry_count") + 1)

        failed_df.createOrReplaceTempView(f"{batchjob_id}_ingest")
    except Exception as e:
        raise e


def prepare_transformed_table(self, batchjob_id: str, dataframe):
    if dataframe is None:
        return None

    dataframe_transformed = dataframe.withColumn("retry_count", F.lit(None).cast(IntegerType()))
    dataframe_transformed.createOrReplaceTempView(f"{batchjob_id}_transformed")

    return dataframe_transformed


def validate_required_columns(self, dataframe, required_columns: list):
    return dataframe


def prepare_ingest_table(self, batchjob_id: str, entity_class, required_columns: list):
    self.table(f"{batchjob_id}_transformed") \
        .filter("VALID = 'VALID'") \
        .discard_remaining_columns(entity_class) \
        .createOrReplaceTempView(f"{batchjob_id}_ingest")

    dataframe_transformed = dataframe.validate_required_columns(required_columns)

    ## Remove rows from transformed table that are in the ingest by dropping them - they are always matched by the test_batchnoegle column
    ingest_df = self.table(f"{batchjob_id}_ingest").select("test_batchnoegle").distinct()
    transformed_df = self.table(f"{batchjob_id}_transformed")

    print("Transformed DF before filtering:")
    transformed_df.show()

    filtered_transformed_df = transformed_df.join(ingest_df,
                                                  transformed_df["test_batchnoegle"] == ingest_df["test_batchnoegle"],
                                                  "left_anti")
    filtered_transformed_df.createOrReplaceTempView(f"{batchjob_id}_transformed")


SparkSession.read_csv = read_csv
SparkSession.increment_retry_count_on_failed_records = increment_retry_count_on_failed_records
SparkSession.prepare_transformed_table = prepare_transformed_table
SparkSession.prepare_ingest_table = prepare_ingest_table
