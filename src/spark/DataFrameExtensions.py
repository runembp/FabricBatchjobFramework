from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import regexp_replace, coalesce, create_map, lit, col


def add_lookup_to(self, target_col):
    """
    Injected into DataFrame. 
    Starts the builder and passes the current DF instance.
    """
    return LookupBuilder(self, target_col)


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

        return self

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


def string_column(self, new_column_name, enum_column):
    return self.withColumn(new_column_name, col(str(enum_column)))


def decimal_column(self, new_column_name, enum_column):
    return self.withColumn(new_column_name, regexp_replace(col(str(enum_column)), ",", ".").cast("decimal(18,2)"))


def optionset_column(self, new_column_name, source_column, mapping_dict):
    column_name = str(source_column)
    mapping_expr = create_map(*[lit(kv) for kv in sum(mapping_dict.items(), ())])
    return self.withColumn(str(new_column_name), coalesce(mapping_expr[col(column_name)], lit(None)))


def optionset(self, new_column_name, converter_func):
    return converter_func(self, str(new_column_name))


def integer_column(self, new_column_name, enum_column):
    return self.withColumn(new_column_name, col(str(enum_column)).cast("integer"))


def discard_remaining_columns(self, entity_class):
    action_column = "Action"
    entity_fields = {str(e.value).lower() for e in entity_class}
    entity_fields.add(str(action_column).lower())

    to_select = [c for c in self.columns if c.lower() in entity_fields]

    return self.select(*to_select)


DataFrame.string = string_column
DataFrame.decimal = decimal_column
DataFrame.optionset_column = optionset_column
DataFrame.optionset = optionset
DataFrame.integer = integer_column
DataFrame.add_lookup_to = add_lookup_to
DataFrame.discard_remaining_columns = discard_remaining_columns
