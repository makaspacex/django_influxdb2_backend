from pathlib import Path

SECRET_KEY = "dummy"
DEBUG = True
INSTALLED_APPS = [
    "django_influxdb2_backend.metrics.apps.MetricsConfig",
]
DATABASES = {
    "default": {
        "ENGINE": "django_influxdb2_backend",
        "NAME": "example-bucket",
        "TEST": {"SERVICE": "fake"},
    }
}
ROOT_URLCONF = "django_influxdb2_backend.tests.urls"
USE_TZ = True
TIME_ZONE = "UTC"
BASE_DIR = Path(__file__).resolve().parent
