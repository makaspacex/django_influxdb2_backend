from __future__ import annotations

from typing import Any, Dict, Optional

try:
    from influxdb_client import InfluxDBClient
except ImportError:  # pragma: no cover - optional for offline tests
    InfluxDBClient = None  # type: ignore

from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.utils import DatabaseError

from .creation import DatabaseCreation
from .cursor import InfluxConnection
from .features import DatabaseFeatures
from .introspection import DatabaseIntrospection
from .operations import DatabaseOperations
from .client import DatabaseClient
from .validation import DatabaseValidation


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = "influxdb2"
    display_name = "InfluxDB 2.x"
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    introspection_class = DatabaseIntrospection
    validation_class = DatabaseValidation
    features_class = DatabaseFeatures
    ops_class = DatabaseOperations

    data_types = {}
    operators = {}
    SchemaEditorClass = None

    def __init__(self, settings_dict: Dict[str, Any], alias="default", **kwargs):
        super().__init__(settings_dict, alias, **kwargs)
        self.features = DatabaseFeatures(self)
        self.ops = DatabaseOperations(self)
        self.client = None
        self.creation = None
        self.introspection = None
        self.validation = None

    def get_connection_params(self):
        return self.settings_dict.copy()

    def _build_client(self, conn_params: Dict[str, Any]) -> Optional[Any]:
        url = conn_params.get("URL") or conn_params.get("url")
        token = conn_params.get("TOKEN") or conn_params.get("token")
        org = conn_params.get("ORG") or conn_params.get("org")
        if url and token and InfluxDBClient:
            return InfluxDBClient(url=url, token=token, org=org)
        return None

    def get_new_connection(self, conn_params):
        client = self._build_client(conn_params)
        return InfluxConnection(client=client)

    def init_connection_state(self):
        return None

    def ensure_connection(self):
        if self.connection is None:
            self.connect()
        return self.connection

    def _set_autocommit(self, autocommit):
        self.autocommit = autocommit

    def create_cursor(self, name=None):
        if not self.connection:
            self.connect()
        return self.connection.cursor()

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def is_usable(self):
        return True

    def _prepare_cursor(self, cursor):
        return cursor

    def make_debug_cursor(self, cursor):
        return cursor

    def validate_no_broken_transaction(self):
        return None

    def is_in_atomic_block(self):
        return False

    def commit(self):
        return None

    def rollback(self):
        return None

    def savepoint(self, sid=None):
        raise DatabaseError("Savepoints are not supported for InfluxDB.")

    def savepoint_rollback(self, sid):
        raise DatabaseError("Savepoints are not supported for InfluxDB.")

    def savepoint_commit(self, sid):
        raise DatabaseError("Savepoints are not supported for InfluxDB.")

    def get_schema_editor(self, **kwargs):
        raise NotImplementedError("Schema editor is not implemented for InfluxDB backend.")


Database = DatabaseWrapper
