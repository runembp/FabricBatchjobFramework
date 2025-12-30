import os
import pytest
from pyspark.sql import SparkSession

from src.fabric_framework.batchjobs.runner.runner import Runner
from src.fabric_framework.batchjobs.testbatchjob.testbatchjob import TestBatchjob


def setup_mock_csv():
    os.makedirs("data", exist_ok=True)
    with open("data/test.csv", "w") as f:
        f.write("ACTION;STATUS;SALARY;VALUE;ID;POLICENR\n")
        f.write("Update;Active;50000.50;Test1;1;POL-999\n")


def run_total_test():
    # 1. Setup Local Environment
    setup_mock_csv()
    spark, jdbc_url = create_spark_session()
    seed_database(spark, jdbc_url)

    # 2. Execute Job (Injecting Spark)
    job = TestBatchjob()
    runner = Runner(None, spark)

    runner.run(TestBatchjob)


def create_spark_session(db_path="data/local_lakehouse.duckdb"):
    # Path to the JAR you downloaded
    jdbc_jar_path = "drivers/duckdb_jdbc-1.4.3.0.jar"

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("FabricDuckDBTest") \
        .config("spark.jars", jdbc_jar_path) \
        .config("spark.driver.extraClassPath", jdbc_jar_path) \
        .getOrCreate()

    # Define the JDBC connection string for DuckDB
    # Use :memory: for in-memory, or a file path for persistence
    jdbc_url = f"jdbc:duckdb:{db_path}"

    return spark, jdbc_url


def seed_database(spark: SparkSession, jdbc_url: str):
    police_data = [("GUID-100", "POL-999", "GUID-100POL-999")]
    df_police = spark.createDataFrame(police_data, ["test_policeid", "test_policenummer", "test_batchnoegle"])

    df_police.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "vel_police") \
        .option("driver", "org.duckdb.DuckDBDriver") \
        .mode("overwrite") \
        .save()
    
    transformed_data = [
        ("Update", "GUID-100", "Active", 50000.50, 123, 0, 0)
    ]
    
    spark.createDataFrame(transformed_data, ["ACTION", "police_nummer", "STATUS", "SALARY", "VALUE", "ID", "retry_count"]) \
        .createOrReplaceTempView("001_transformed")


if __name__ == "__main__":
    run_total_test()
