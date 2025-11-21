from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple

from django.db.backends.base.operations import BaseDatabaseOperations
from django.utils.dateparse import parse_datetime


def _format_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = "django_influxdb2_backend.compiler"

    def __init__(self, connection):
        super().__init__(connection)
        self.value_separator = " , "

    def quote_name(self, name: str) -> str:
        return f'"{name}"'

    def adapt_datetimefield_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            parsed = parse_datetime(value)
            if parsed:
                return _format_datetime(parsed)
        if isinstance(value, datetime):
            return _format_datetime(value)
        return value

    def adapt_timefield_value(self, value):
        return self.adapt_datetimefield_value(value)

    def bulk_batch_size(self, fields: List[Any], objs: Iterable[Any]):
        return 0

    def max_name_length(self) -> int:
        return 255

    def quote_value(self, value: Any) -> str:
        if isinstance(value, str):
            escaped = value.replace("\\", "\\\\").replace("\"", "\\\"")
            return f'"{escaped}"'
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, datetime):
            return f'"{_format_datetime(value)}"'
        return str(value)

    def format_lookup(self, field_name: str, lookup: str, value: Any) -> str:
        operator_map: Dict[str, str] = {
            "exact": "==",
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
            "contains": "=~",
            "icontains": "=~",
        }
        op = operator_map.get(lookup)
        if not op:
            raise NotImplementedError(f"Lookup '{lookup}' is not supported for Flux translation.")
        rhs = self.quote_value(value)
        if lookup in {"contains", "icontains"}:
            rhs = f"/{value}/"
        return f'r["{field_name}"] {op} {rhs}'

    def compiler(self, compiler_name):
        from django_influxdb2_backend.compiler import FluxCompiler

        return FluxCompiler

    def combine_expression(self, connector, sub_expressions):
        joined = f" {connector} ".join(sub_expressions)
        return f"({joined})"

    def date_extract_sql(self, lookup_type, field_name):
        raise NotImplementedError("Date extraction is not supported in Flux compiler.")

    def lookup_cast(self, lookup_type, internal_type=None):
        return ""

    def no_limit_value(self):
        return None

    def limit_offset_sql(self, low_mark, high_mark):
        # Flux handles slicing via the limit() transformation.
        return ""

    def random_function_sql(self):
        return ""

    def pk_default_value(self):
        return None

    def tablespace_sql(self, tablespace, inline=False):
        return ""

    def last_insert_id(self, cursor, table_name, pk_name):
        return None

    def year_lookup_bounds_for_date_field(self, value):
        return None

    def savepoint_create_sql(self, sid):
        raise NotImplementedError

    def savepoint_commit_sql(self, sid):
        raise NotImplementedError

    def savepoint_rollback_sql(self, sid):
        raise NotImplementedError
