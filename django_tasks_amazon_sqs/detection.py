"""
Environment detection for AWS.

Only the region is detected: a worker pulls from the queue, so there is no
URL of the application to find.
"""

import os


def detect_aws_region():
    """
    Detect the AWS region from the environment.

    AWS_REGION is set by Lambda and by ECS with the awslogs driver;
    AWS_DEFAULT_REGION is what the CLI and most local setups use.

    Returns:
        Region string or None if not detected.
    """
    return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or None


def is_lambda():
    """Whether the process runs on AWS Lambda."""
    return bool(os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))


def is_ecs():
    """Whether the process runs on ECS or Fargate."""
    return bool(
        os.environ.get("ECS_CONTAINER_METADATA_URI_V4")
        or os.environ.get("ECS_CONTAINER_METADATA_URI")
    )
