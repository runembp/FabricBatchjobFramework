from src.fabric_framework.dataverse_entities.Account import Account
from src.fabric_framework.dataverse_entities.Police import Police
from src.fabric_framework.utils.dataframe_extensions import optionset_column
from src.fabric_framework.utils.spark_extensions import read_csv
from src.fabric_framework.utils.enums import StringEnum
from pyspark.sql import DataFrame


class TestBatchjob:
    batchjob_id = "001"
    entity_class = Account
    required_columns = [Account.police_nummer, Account.account_number]
    
    def map_csv_file(self, batchjob_id, delimiter=";") -> DataFrame:
        dataframe = self.spark.read_csv(batchjob_id, delimiter)

        if dataframe is None:
            return None

        dataframe_transformed = dataframe \
            .string(TestBatchJobColumns.ACTION, TestBatchJobColumns.ACTION) \
            .string_concatenated(Account.batch_noegle, [TestBatchJobColumns.POLICE_NUMMER, TestBatchJobColumns.ID]) \
            .string(Account.status, TestBatchJobColumns.STATUS) \
            .decimal(Account.loen, TestBatchJobColumns.SALARY) \
            .optionset(Account.value, TestBatchjob.value_optionset_converter) \
            .integer(Account.account_number, TestBatchJobColumns.ID)

        dataframe_transformed.show()

        return dataframe_transformed

    @staticmethod
    def value_optionset_converter(dataframe: DataFrame, target_column_name: str):
        value_map = {
            "Test1": 123,
            "Test2": 456
        }

        return dataframe.optionset_column(target_column_name, TestBatchJobColumns.VALUE, value_map)

    @staticmethod
    def add_lookup_and_prepare_ingest(batchjob_id, dataframe_with_lookups):
        if dataframe_with_lookups is None:
            return None

        dataframe_with_lookups = dataframe_with_lookups \
            .add_lookup_to(Account.police_nummer) \
            .from_table(Police.logical_name) \
            .to_column(Police.police_nummer) \
            .where_values_in(TestBatchJobColumns.POLICE_NUMMER)

        return dataframe_with_lookups


class TestBatchJobColumns(StringEnum):
    ACTION = "ACTION"
    POLICE_NUMMER = "POLICENR"
    STATUS = "STATUS"
    VALUE = "VALUE"
    SALARY = "SALARY"
    ID = "ID"
