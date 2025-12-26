class Runner:
    def __init__(self, notebookutils, spark):
        self.notebookutils = notebookutils
        self.spark = spark

    def run(self, batch_job_map):
        batchjob_id = batch_job_map.batchjob_id
        required_columns = batch_job_map.required_columns
        entity_class = batch_job_map.entity_class

        self.spark.increment_retry_count_on_failed_records(batchjob_id)
        dataframe_transformed = batch_job_map.map_csv_file(self, batchjob_id)
        self.spark.prepare_transformed_table(batchjob_id, dataframe_transformed)
        batch_job_map.map.add_lookup_and_prepare_ingest(batchjob_id, dataframe_transformed)
        self.spark.validate_required_columns(batchjob_id, required_columns)
        self.spark.prepare_ingest_table(batchjob_id, entity_class)
        self.spark.ingest(batchjob_id, entity_class)
