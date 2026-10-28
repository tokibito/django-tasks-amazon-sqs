"""Tests for detection.py"""

from django_tasks_amazon_sqs import detect_aws_region, is_ecs, is_lambda


class TestDetectAwsRegion:
    def test_reads_aws_region(self, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "ap-northeast-1")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

        assert detect_aws_region() == "ap-northeast-1"

    def test_falls_back_to_aws_default_region(self, monkeypatch):
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

        assert detect_aws_region() == "us-east-1"

    def test_none_when_unset(self, monkeypatch):
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)

        assert detect_aws_region() is None


class TestPlatformDetection:
    def test_lambda(self, monkeypatch):
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        assert is_lambda() is False

        monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "fn")
        assert is_lambda() is True

    def test_ecs(self, monkeypatch):
        monkeypatch.delenv("ECS_CONTAINER_METADATA_URI_V4", raising=False)
        monkeypatch.delenv("ECS_CONTAINER_METADATA_URI", raising=False)
        assert is_ecs() is False

        monkeypatch.setenv("ECS_CONTAINER_METADATA_URI_V4", "http://169.254.170.2/v4")
        assert is_ecs() is True
