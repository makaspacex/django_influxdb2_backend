from datetime import datetime, timezone

import django
import pytest
from django.conf import settings
from django.db import connections
from django.utils import timezone as dj_timezone

from django_influxdb2_backend.metrics.models import TemperatureReading
from django_influxdb2_backend.cursor import FakeFluxService


def setup_module(module):
    if not settings.configured:
        raise RuntimeError("Django settings must be configured via pytest.ini")
    django.setup()


def test_flux_compilation_with_filters():
    start = datetime(2024, 5, 1, 12, 0, tzinfo=timezone.utc)
    end = datetime(2024, 5, 1, 14, 0, tzinfo=timezone.utc)

    qs = TemperatureReading.objects.filter(
        time__gte=start,
        time__lt=end,
        device="sensor-1",
        location__contains="lab",
    ).order_by("-time").values("device", "value")

    compiler = qs.query.get_compiler(using="default")
    flux, params = compiler.as_sql()

    assert params == ()
    expected = "\n".join(
        [
            'from(bucket: "example-bucket")',
            "|> range(start: 2024-05-01T12:00:00+00:00, stop: 2024-05-01T14:00:00+00:00)",
            '|> filter(fn: (r) => r["_measurement"] == "temperature")',
            '|> filter(fn: (r) => r["device"] == "sensor-1")',
            '|> filter(fn: (r) => r["location"] =~ /lab/)',
            '|> keep(columns: ["device", "value"])',
            '|> sort(columns: ["time"], desc: true)',
        ]
    )
    assert flux == expected


def test_limit_and_offset_are_respected():
    qs = TemperatureReading.objects.all()[5:10]
    compiler = qs.query.get_compiler(using="default")
    flux, _ = compiler.as_sql()

    assert "|> limit(n: 5, offset: 5)" in flux


def test_cursor_records_flux(monkeypatch):
    service = FakeFluxService()

    connection = connections["default"]
    connection.ensure_connection()
    connection.connection.service = service

    qs = TemperatureReading.objects.filter(device="sensor-99")
    flux, _ = qs.query.get_compiler(using="default").as_sql()

    with connection.cursor() as cursor:
        cursor.execute(flux)
        assert cursor.last_query == flux

    assert service.history[-1] == flux
