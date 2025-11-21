from __future__ import annotations

from django.db import models


class TemperatureReading(models.Model):
    time = models.DateTimeField(primary_key=True)
    device = models.CharField(max_length=64)
    location = models.CharField(max_length=64)
    value = models.FloatField()

    class Meta:
        managed = False
        db_table = "temperature"
        app_label = "metrics"
        verbose_name = "temperature reading"
        verbose_name_plural = "temperature readings"
