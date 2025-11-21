from __future__ import annotations

from typing import Any, Iterable, List, Optional, Sequence, Tuple

try:
    from influxdb_client import InfluxDBClient
except ImportError:  # pragma: no cover - dependencies resolved in production
    InfluxDBClient = None  # type: ignore


class FakeFluxService:
    """Lightweight stand-in that records executed Flux queries.

    Tests can inspect :pyattr:`history` to ensure we generated the expected
    Flux pipelines without requiring a running InfluxDB server.
    """

    def __init__(self):
        self.history: List[str] = []

    def query(self, query: str):
        self.history.append(query)
        return []

    def close(self):
        return None


class FluxCursor:
    def __init__(self, service: Any):
        self.service = service
        self.last_query: Optional[str] = None
        self._result_cache: List[Any] = []

    def close(self):
        if hasattr(self.service, "close"):
            self.service.close()

    def execute(self, query: str, params: Optional[Sequence[Any]] = None):
        rendered_query = self._inject_params(query, params or [])
        self.last_query = rendered_query
        self._result_cache = list(self.service.query(rendered_query) or [])
        return self

    def fetchone(self):
        return self._result_cache.pop(0) if self._result_cache else None

    def fetchmany(self, size=None):
        size = size or 1
        chunk, self._result_cache = (
            self._result_cache[:size],
            self._result_cache[size:],
        )
        return chunk

    def fetchall(self):
        chunk, self._result_cache = self._result_cache, []
        return chunk

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def mogrify(self, query: str, params: Optional[Sequence[Any]] = None):
        return self._inject_params(query, params or [])

    def _inject_params(self, query: str, params: Sequence[Any]) -> str:
        if not params:
            return query
        rendered = query
        for value in params:
            rendered = rendered.replace("%s", self._quote(value), 1)
        return rendered

    def _quote(self, value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, str):
            escaped = value.replace("\\", "\\\\").replace("\"", "\\\"")
            return f'"{escaped}"'
        return str(value)


class InfluxConnection:
    def __init__(self, **params):
        self.params = params
        self.client: Optional[InfluxDBClient] = params.get("client")
        self.service = params.get("service") or (self.client and self.client.query_api())
        if self.service is None:
            self.service = FakeFluxService()

    def cursor(self):
        return FluxCursor(self.service)

    def close(self):
        if self.client:
            self.client.__del__()

    def is_usable(self):
        return True
