import pytest
from pyspark.sql import SparkSession

def seed_fake_lakehouse(spark: SparkSession):
    """Registers the mock Dataverse tables as temp views."""
    police_data = [("GUID-123", "POLICENR123"), ("GUID-456", "POLICENR456")]

    # Ensure column names match what your LookupBuilder expects
    spark.createDataFrame(police_data, ["test_policenummer", "test_batchnoegle"]) \
        .createOrReplaceTempView("test_police")


@pytest.fixture(scope="session")
def spark_session():
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("RiderLocalTest")
             .getOrCreate())

    seed_fake_lakehouse(spark)
    return spark