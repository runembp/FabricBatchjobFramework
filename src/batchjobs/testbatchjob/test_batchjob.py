from batchjobs.spark.sparkmotor import create_spark_session
from batchjobs.spark.DataFrameExtensions import optionset_column

from entities.Account import Account
from entities.Police import Police
from pyspark.sql import DataFrame

from src.batchjobs.spark.SparkExtensions import read_csv
from src.batchjobs.spark.StringEnum import StringEnum


class TestBatchjob:
    def __init__(self, notebookutils, spark):
        self.notebookutils = notebookutils
        self.spark = spark

    def run(self, delimiter=";"):
        file_location = "src/test.csv"
        dataframe_from_csv = read_csv(self.spark, file_location, delimiter)
        
        dataframe_from_csv.show()

        dataframe_transformed = dataframe_from_csv \
            .string(TestBatchjobMap.ACTION, TestBatchjobMap.ACTION) \
            .string(Account.status, TestBatchjobMap.STATUS) \
            .decimal(Account.loen, TestBatchjobMap.SALARY) \
            .optionset(Account.value, TestBatchjobMap.value_optionset_converter) \
            .integer(Account.account_number, TestBatchjobMap.ID)

        dataframe_transformed = dataframe_transformed \
            .add_lookup_to(Account.police_nummer) \
            .from_table(Police.logical_name) \
            .to_column(Police.police_nummer) \
            .where_values_in(TestBatchjobMap.POLICE_NUMMER) \
            .execute()

        dataframe_transformed = dataframe_transformed.discard_remaining_columns(Account)

        dataframe_transformed.show()


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
