# django_influxdb2_backend

Django 5.2 database backend that targets InfluxDB 2.x via the Flux query language. The backend focuses on transparent Flux generation so you can preview the exact pipeline that will be sent to InfluxDB before enabling a real connection.

## Features

- Generates Flux pipelines from Django ORM queries (filters, projections, ordering, slicing).
- Routes time range filters (`time`/`_time` fields) into `range()` for efficient scans.
- Provides a `FakeFluxService` cursor for offline testing when InfluxDB is unavailable.

## Requirements

- Python 3.12+
- Django 5.2.x
- influxdb-client 1.42+

## Quick start

```toml
# pyproject.toml
[project]
dependencies = ["django-influxdb2-backend"]
```

```python
DATABASES = {
    "default": {
        "ENGINE": "django_influxdb2_backend",
        "NAME": "your-bucket-name",
        "TOKEN": "<token>",
        "URL": "http://localhost:8086",
    }
}
```

Models should be marked `managed = False` because InfluxDB does not support Django migrations. The `db_table` becomes the measurement name:

```python
class TemperatureReading(models.Model):
    time = models.DateTimeField(primary_key=True)
    device = models.CharField(max_length=64)
    value = models.FloatField()

    class Meta:
        managed = False
        db_table = "temperature"
        app_label = "metrics"
```

To inspect the Flux string without hitting InfluxDB:

```python
qs = TemperatureReading.objects.filter(device="sensor-1").order_by("-time")
flux, _ = qs.query.get_compiler(using="default").as_sql()
print(flux)
```

When the `URL` and `TOKEN` settings are provided, queries will be routed through `influxdb-client`. Without them, the backend defaults to the in-memory `FakeFluxService`, making it safe to run tests offline.
