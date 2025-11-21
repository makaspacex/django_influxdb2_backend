from django.db.backends.base.features import BaseDatabaseFeatures


class DatabaseFeatures(BaseDatabaseFeatures):
    minimum_database_version = (2, 0)
    can_introspect_autofield = False
    can_return_columns_from_insert = False
    can_return_rows_from_bulk_insert = False
    allows_group_by_lob = False
    uses_savepoints = False
    supports_transactions = False
    supports_over_clause = False
    supports_slicing_ordering_in_compound = False

    # InfluxDB is schemaless and uses Flux for querying.
    supports_paramstyle_pyformat = False
    supports_paramstyle_named = False
