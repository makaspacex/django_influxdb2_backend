from django.apps import AppConfig


class MetricsConfig(AppConfig):
    name = "django_influxdb2_backend.metrics"
    label = "metrics"
    verbose_name = "Influx metrics"
