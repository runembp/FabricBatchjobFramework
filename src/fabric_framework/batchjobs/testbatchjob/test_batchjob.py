from pyspark.sql import DataFrame

from src.fabric_framework.dataverse_entities.Account import Account
from src.fabric_framework.dataverse_entities.Police import Police

from src.fabric_framework.utils.dataframe_extensions import optionset_column
from src.fabric_framework.utils.spark_extensions import read_csv
from src.fabric_framework.utils.enums import StringEnum


class TestBatchjob:
    def __init__(self, notebookutils, spark):
        self.notebookutils = notebookutils
        self.spark = spark

    def run(self, delimiter=";"):
        batchjob_id = "001"
        file_location = "data/test.csv"
        dataframe_from_csv = self.spark.read_csv(file_location, delimiter)

        dataframe_transformed = dataframe_from_csv \
            .string_concatenated(Account.batch_noegle, [TestBatchjobMap.POLICE_NUMMER, TestBatchjobMap.ID]) \
            .string(TestBatchjobMap.ACTION, TestBatchjobMap.ACTION) \
            .string(Account.status, TestBatchjobMap.STATUS) \
            .decimal(Account.loen, TestBatchjobMap.SALARY) \
            .optionset(Account.value, TestBatchjobMap.value_optionset_converter) \
            .integer(Account.account_number, TestBatchjobMap.ID)

        dataframe_transformed = dataframe_transformed \
            .add_lookup_to(Account.police_nummer) \
            .from_table(Police.logical_name) \
            .to_column(Police.police_nummer) \
            .where_values_in(TestBatchjobMap.POLICE_NUMMER)

        required_columns = [Account.police_nummer, Account.account_number]
        self.spark.prepare_transformed_table(batchjob_id, dataframe_transformed, required_columns)
        self.spark.prepare_ingest_table(batchjob_id, Account)


class TestBatchjobMap(StringEnum):
    ACTION = "ACTION"
    POLICE_NUMMER = "POLICENR"
    STATUS = "STATUS"
    VALUE = "VALUE"
    SALARY = "SALARY"
    ID = "ID"

    @staticmethod
    def value_optionset_converter(dataframe: DataFrame, target_column_name: str):
        value_map = {
            "Test1": 123,
            "Test2": 456
        }

        return dataframe.optionset_column(target_column_name, TestBatchjobMap.VALUE, value_map)
