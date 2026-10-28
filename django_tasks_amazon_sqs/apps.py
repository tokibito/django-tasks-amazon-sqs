"""Django application configuration."""

from django.apps import AppConfig


class DjangoTasksAmazonSQSConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "django_tasks_amazon_sqs"
    verbose_name = "Django Tasks Amazon SQS Backend"
