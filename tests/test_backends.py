"""Tests for backends.py"""

import json
from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.tasks.base import TaskResultStatus
from django.tasks.exceptions import InvalidTask
from django.tasks.signals import task_enqueued
from django.utils import timezone

from django_tasks_amazon_sqs.backends import SQSBackend

from .tasks import add_numbers, async_task, ranking_task, simple_task

QUEUE_URL_TEMPLATE = "https://sqs.us-east-1.amazonaws.com/123456789012/{queue_name}"


def make_backend(**options):
    options.setdefault("AWS_REGION", "us-east-1")
    backend = SQSBackend("default", {"QUEUES": [], "OPTIONS": options})
    backend._client = MagicMock()
    backend._client.send_message.return_value = {"MessageId": "sent-1"}
    backend._client.get_queue_url.return_value = {
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/1/looked-up"
    }
    return backend


class TestInit:
    def test_reads_the_region_from_options(self):
        assert make_backend(AWS_REGION="ap-northeast-1").region == "ap-northeast-1"

    def test_detects_the_region_from_the_environment(self, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "eu-west-1")

        backend = SQSBackend("default", {"OPTIONS": {}})

        assert backend.region == "eu-west-1"

    def test_leaves_the_region_to_boto3_when_nothing_says(self, monkeypatch):
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)

        backend = SQSBackend("default", {"OPTIONS": {}})

        assert backend.region is None

    def test_defaults(self):
        backend = make_backend()

        assert backend.endpoint_url is None
        assert backend.queue_url_template is None
        assert backend.visibility_timeout is None
        assert backend.wait_time_seconds == 20
        assert backend.max_delay_seconds == 900
        assert backend.retry_failed_tasks is True

    def test_capabilities(self):
        backend = make_backend()

        assert backend.supports_defer is True
        assert backend.supports_async_task is True
        assert backend.supports_get_result is False
        assert backend.supports_priority is False

    def test_rejects_a_delay_over_the_sqs_limit(self):
        with pytest.raises(ImproperlyConfigured, match="MAX_DELAY_SECONDS"):
            make_backend(MAX_DELAY_SECONDS=901)

    def test_rejects_a_visibility_timeout_over_the_sqs_limit(self):
        with pytest.raises(ImproperlyConfigured, match="VISIBILITY_TIMEOUT"):
            make_backend(VISIBILITY_TIMEOUT=43201)

    def test_the_client_is_created_lazily(self):
        backend = SQSBackend("default", {"OPTIONS": {"AWS_REGION": "us-east-1"}})

        assert backend._client is None


class TestQueueUrl:
    def test_uses_the_template(self):
        backend = make_backend(SQS_QUEUE_URL_TEMPLATE=QUEUE_URL_TEMPLATE)

        assert backend.get_queue_url("ranking") == (
            "https://sqs.us-east-1.amazonaws.com/123456789012/ranking"
        )
        backend._client.get_queue_url.assert_not_called()

    def test_looks_the_url_up_once(self):
        backend = make_backend()

        first = backend.get_queue_url("default")
        second = backend.get_queue_url("default")

        assert first == second == "https://sqs.us-east-1.amazonaws.com/1/looked-up"
        backend._client.get_queue_url.assert_called_once_with(QueueName="default")

    def test_close_forgets_the_urls(self):
        backend = make_backend()
        backend.get_queue_url("default")

        backend.close()

        assert backend._client is None
        assert backend._queue_urls == {}


class TestEnqueue:
    def test_sends_the_payload(self, backend):
        result = backend.enqueue(add_numbers, (1, 2), {"extra": None})

        backend._client.send_message.assert_called_once()
        call = backend._client.send_message.call_args.kwargs
        assert call["QueueUrl"] == QUEUE_URL_TEMPLATE.format(queue_name="default")
        assert call["DelaySeconds"] == 0
        payload = json.loads(call["MessageBody"])
        assert payload == {
            "task_id": result.id,
            "task_path": "tests.tasks.add_numbers",
            "args": [1, 2],
            "kwargs": {"extra": None},
            "queue_name": "default",
            "backend": "default",
            "priority": 0,
            "takes_context": False,
            "enqueued_at": result.enqueued_at.isoformat(),
        }

    def test_returns_a_ready_result(self, backend):
        result = backend.enqueue(simple_task, (21,), {})

        assert result.status == TaskResultStatus.READY
        assert result.task is simple_task
        assert result.args == [21]
        assert result.kwargs == {}
        assert result.backend == "default"
        assert result.enqueued_at is not None
        assert result.started_at is None
        assert result.worker_ids == []
        assert len(result.id) == 32

    def test_uses_the_task_queue(self, backend):
        backend.enqueue(ranking_task, (1,), {})

        call = backend._client.send_message.call_args.kwargs
        assert call["QueueUrl"] == QUEUE_URL_TEMPLATE.format(queue_name="ranking")

    def test_sends_the_signal(self, backend):
        received = []

        def receiver(sender, task_result, **kwargs):
            received.append((sender, task_result))

        task_enqueued.connect(receiver)
        try:
            result = backend.enqueue(simple_task, (1,), {})
        finally:
            task_enqueued.disconnect(receiver)

        assert received == [(SQSBackend, result)]

    def test_normalizes_the_arguments(self, backend):
        backend.enqueue(simple_task, ((1, 2),), {"nested": (3,)})

        payload = json.loads(
            backend._client.send_message.call_args.kwargs["MessageBody"]
        )
        assert payload["args"] == [[1, 2]]
        assert payload["kwargs"] == {"nested": [3]}

    def test_an_async_task_is_accepted(self, backend):
        result = backend.enqueue(async_task, (1,), {})

        assert result.task is async_task

    def test_via_the_task(self, backend):
        result = simple_task.enqueue(5)

        assert result.status == TaskResultStatus.READY
        backend._client.send_message.assert_called_once()


class TestDeferredTasks:
    def test_a_future_run_after_becomes_a_delay(self, backend):
        run_after = timezone.now() + timedelta(seconds=120)

        simple_task.using(run_after=run_after).enqueue(1)

        delay = backend._client.send_message.call_args.kwargs["DelaySeconds"]
        assert 119 <= delay <= 121

    def test_a_delay_is_rounded_up(self, backend):
        run_after = timezone.now() + timedelta(seconds=10.2)

        delay = backend.get_delay_seconds(run_after)

        assert delay == 11

    def test_a_past_run_after_is_sent_at_once(self, backend):
        run_after = timezone.now() - timedelta(seconds=30)

        simple_task.using(run_after=run_after).enqueue(1)

        assert backend._client.send_message.call_args.kwargs["DelaySeconds"] == 0

    def test_a_run_after_past_the_sqs_limit_is_rejected(self, backend):
        run_after = timezone.now() + timedelta(minutes=16)

        with pytest.raises(InvalidTask, match="further out than SQS can hold"):
            simple_task.using(run_after=run_after)

    def test_the_limit_can_be_lowered(self):
        backend = make_backend(MAX_DELAY_SECONDS=60)
        task = simple_task.using(run_after=timezone.now() + timedelta(seconds=120))

        with pytest.raises(InvalidTask, match="at most 60 seconds"):
            backend.validate_task(task)


class TestReceive:
    def test_long_polls_with_the_backend_defaults(self, backend):
        backend._client.receive_message.return_value = {"Messages": [{"Body": "{}"}]}

        messages = backend.receive("default")

        assert messages == [{"Body": "{}"}]
        backend._client.receive_message.assert_called_once_with(
            QueueUrl=QUEUE_URL_TEMPLATE.format(queue_name="default"),
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,
            AttributeNames=["ApproximateReceiveCount"],
        )

    def test_an_empty_queue_gives_an_empty_list(self, backend):
        assert backend.receive("default") == []

    def test_clamps_to_what_sqs_accepts(self, backend):
        backend.receive("default", max_messages=100, wait_seconds=600)

        call = backend._client.receive_message.call_args.kwargs
        assert call["MaxNumberOfMessages"] == 10
        assert call["WaitTimeSeconds"] == 20

    def test_passes_the_visibility_timeout(self):
        backend = make_backend(
            SQS_QUEUE_URL_TEMPLATE=QUEUE_URL_TEMPLATE, VISIBILITY_TIMEOUT=30
        )
        backend._client.receive_message.return_value = {}

        backend.receive("default", wait_seconds=0)

        call = backend._client.receive_message.call_args.kwargs
        assert call["VisibilityTimeout"] == 30
        assert call["WaitTimeSeconds"] == 0

    def test_delete_message(self, backend):
        backend.delete_message("default", "rh-1")

        backend._client.delete_message.assert_called_once_with(
            QueueUrl=QUEUE_URL_TEMPLATE.format(queue_name="default"),
            ReceiptHandle="rh-1",
        )

    def test_release_message(self, backend):
        backend.release_message("default", "rh-1", delay=5)

        backend._client.change_message_visibility.assert_called_once_with(
            QueueUrl=QUEUE_URL_TEMPLATE.format(queue_name="default"),
            ReceiptHandle="rh-1",
            VisibilityTimeout=5,
        )
