from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F, SparkSession


def read_csv(self, path, delimiter=";"):
    """
    Standardized CSV reader for BatchJobs.
    """
    return self.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("sep", delimiter) \
        .csv(path)


def prepare_transformed_table(self, batchjob_id: str, dataframe, required_columns: list):
    dataframe_transformed = dataframe.validate_required_columns(required_columns)
    dataframe_transformed = dataframe_transformed.withColumn("retry_count", F.lit(None).cast(IntegerType()))
    dataframe_transformed.createOrReplaceTempView(f"{batchjob_id}_transformed")


def prepare_ingest_table(self, batchjob_id: str, entity_class):
    self.table(f"{batchjob_id}_transformed") \
        .filter("VALID = 'VALID'") \
        .discard_remaining_columns(entity_class) \
        .createOrReplaceTempView(f"{batchjob_id}_ingest")
    
    ## Remove rows from transformed table that are in the ingest by dropping them - they are always matched by the test_batchnoegle column
    ingest_df = self.table(f"{batchjob_id}_ingest").select("test_batchnoegle").distinct()
    transformed_df = self.table(f"{batchjob_id}_transformed")
    
    print("Transformed DF before filtering:")
    transformed_df.show()
    
    filtered_transformed_df = transformed_df.join(ingest_df, transformed_df["test_batchnoegle"] == ingest_df["test_batchnoegle"], "left_anti")
    filtered_transformed_df.createOrReplaceTempView(f"{batchjob_id}_transformed")
    

SparkSession.read_csv = read_csv
SparkSession.prepare_transformed_table = prepare_transformed_table
SparkSession.prepare_ingest_table = prepare_ingest_table
