from django.db.backends.base.client import BaseDatabaseClient


class DatabaseClient(BaseDatabaseClient):
    executable_name = "influx"

    def runshell(self):
        raise NotImplementedError("Interactive shell is not available for Flux backend.")
