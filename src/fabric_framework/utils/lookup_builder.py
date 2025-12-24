from pyspark.sql import DataFrame, functions as F


class LookupBuilder:
    def __init__(self, df: DataFrame, target_col):
        self._df = df
        self._target_col = str(target_col)
        self._entity_name = None
        self._search_col = None
        self._source_col = None

    def from_table(self, entity_logical_name):
        self._entity_name = str(entity_logical_name)
        return self

    def to_column(self, dataverse_search_col):
        self._search_col = str(dataverse_search_col)
        return self

    def where_values_in(self, source_csv_col):
        self._source_col = str(source_csv_col)
        return self.execute()

    def execute(self) -> DataFrame:
        primary_key = "test_batchnoegle"

        if not all([self._entity_name, self._search_col, self._source_col]):
            raise ValueError("LookupBuilder is missing required configuration: From, To, or Where.")

        spark = self._df.sparkSession

        lookup_df = spark.read.table(self._entity_name).select(
            F.col(primary_key).alias(self._target_col),
            F.col(self._search_col).alias("_lookup_key")
        )

        return self._df.join(
            F.broadcast(lookup_df),
            self._df[self._source_col] == lookup_df[self._search_col],
            "left"
        ).drop("_lookup_key")
