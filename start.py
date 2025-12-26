from src.fabric_framework.batchjobs.runner.runner import Runner
from src.fabric_framework.utils.spark_motor import create_spark_session
from tests.Harness import seed_fake_lakehouse
from tests.NotebookMock import MockNotebookUtils


def test_batchjob_locally(spark_session):
    mock_utils = MockNotebookUtils()
    job = Runner(notebookutils=mock_utils, spark=spark_session)
    job.run()


if __name__ == "__main__":
    spark = create_spark_session()
    seed_fake_lakehouse(spark)
    test_batchjob_locally(spark)

    transformed_results = spark.table("001_transformed")
    final_results = spark.table("001_ingest")

    transformed_results.show()
    final_results.show()
