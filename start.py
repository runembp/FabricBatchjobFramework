from src.batchjobs.testbatchjob.test_batchjob import TestBatchjob
from src.spark.sparkmotor import create_spark_session
from tests.Harness import seed_fake_lakehouse
from tests.NotebookMock import MockNotebookUtils


def test_batchjob_locally(spark_session):
    # 1. Setup Mock NotebookUtils
    mock_utils = MockNotebookUtils()

    # 2. Instantiate with injected dependencies
    job = TestBatchjob(notebookutils=mock_utils, spark=spark_session)

    # 3. Patch the file_location if necessary, or ensure the folder structure exists
    # If running locally, you might want to point to a specific local file:

    job.run()


if __name__ == "__main__":
    # 1. Create the session
    spark = create_spark_session()

    # 2. SEED THE DATA (Crucial step)
    seed_fake_lakehouse(spark)

    # 3. Run the test logic
    test_batchjob_locally(spark)
