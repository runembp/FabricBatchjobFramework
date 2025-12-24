import functools

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import regexp_replace, coalesce, create_map, lit, col

from src.fabric_framework.utils.lookup_builder import LookupBuilder


def add_lookup_to(self, target_col):
    """
    Injected into DataFrame. 
    Starts the builder and passes the current DF instance.
    """
    return LookupBuilder(self, target_col)


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


def string_concatenated(self, target_column, source_columns):
    return self.withColumn(target_column, F.concat_ws("", *[F.col(c) for c in source_columns]))


def discard_remaining_columns(self, entity_class):
    action_column = "Action"
    entity_fields = {str(e.value).lower() for e in entity_class}
    entity_fields.add(str(action_column).lower())

    to_select = [c for c in self.columns if c.lower() in entity_fields]

    return self.select(*to_select)


def validate_required_columns(self, required_column_list):
    if not required_column_list:
        return self

    conditions = [
        (F.col(c).isNotNull()) & (F.trim(F.col(c)) != "")
        for c in required_column_list
    ]

    combined_condition = functools.reduce(lambda x, y: x & y, conditions)

    return self.withColumn("VALID",
                           F.when(combined_condition, "VALID")
                           .otherwise(None)
                           )


DataFrame.string = string_column
DataFrame.decimal = decimal_column
DataFrame.optionset_column = optionset_column
DataFrame.optionset = optionset
DataFrame.integer = integer_column
DataFrame.string_concatenated = string_concatenated
DataFrame.add_lookup_to = add_lookup_to
DataFrame.discard_remaining_columns = discard_remaining_columns
DataFrame.validate_required_columns = validate_required_columns
