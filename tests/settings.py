"""Django settings for the tests."""

SECRET_KEY = "test-secret-key-for-testing-only"

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django_tasks_amazon_sqs",
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

USE_TZ = True

# The queue URL template keeps the backend from calling GetQueueUrl, so a
# test can replace the client with a stub without reaching AWS.
TASKS = {
    "default": {
        "BACKEND": "django_tasks_amazon_sqs.SQSBackend",
        "QUEUES": [],
        "OPTIONS": {
            "AWS_REGION": "us-east-1",
            "SQS_QUEUE_URL_TEMPLATE": (
                "https://sqs.us-east-1.amazonaws.com/123456789012/{queue_name}"
            ),
        },
    },
}
