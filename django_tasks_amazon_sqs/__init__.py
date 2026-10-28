"""
django-tasks-amazon-sqs: an Amazon SQS backend for Django 6.0's task framework.

Tasks are sent to an SQS queue as JSON messages carrying their arguments,
and a worker started with ``manage.py run_sqs_tasks`` receives and runs
them. Nothing is stored in the database.
"""

__version__ = "0.1.0"

# The detection helpers use the standard library only.
from .detection import detect_aws_region, is_ecs, is_lambda

__all__ = [
    "__version__",
    "SQSBackend",
    "detect_aws_region",
    "is_ecs",
    "is_lambda",
]


def __getattr__(name):
    """Import the backend on first access.

    Importing the package, which INSTALLED_APPS does, then loads the
    detection helpers only; boto3 is imported when the backend is used.
    """
    if name == "SQSBackend":
        from .backends import SQSBackend

        return SQSBackend
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
