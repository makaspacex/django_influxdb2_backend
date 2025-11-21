from django.db.backends.base.validation import BaseDatabaseValidation


class DatabaseValidation(BaseDatabaseValidation):
    def check(self, **kwargs):  # pragma: no cover - defaults to empty list
        return []
