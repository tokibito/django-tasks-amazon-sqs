"""Tests for executor.py"""

import json
import threading

import pytest
from django.tasks.base import TaskResultStatus
from django.tasks.signals import task_finished, task_started

from django_tasks_amazon_sqs import executor
from django_tasks_amazon_sqs.backends import SQSBackend

from .conftest import make_message


def make_payload(task_path="tests.tasks.add_numbers", **overrides):
    payload = {
        "task_id": "task-1",
        "task_path": task_path,
        "args": [1, 2],
        "kwargs": {},
        "queue_name": "default",
        "backend": "default",
        "priority": 0,
        "takes_context": False,
        "enqueued_at": "2026-01-01T00:00:00+00:00",
    }
    payload.update(overrides)
    return payload


class TestValidatePayload:
    def test_a_complete_payload_is_valid(self):
        assert executor.validate_payload(make_payload()) is None

    def test_optional_fields_may_be_missing(self):
        payload = make_payload()
        del payload["priority"], payload["takes_context"], payload["enqueued_at"]

        assert executor.validate_payload(payload) is None

    @pytest.mark.parametrize(
        "field", ["task_id", "task_path", "args", "kwargs", "queue_name", "backend"]
    )
    def test_a_missing_field(self, field):
        payload = make_payload()
        del payload[field]

        assert executor.validate_payload(payload) == f"missing field: {field}"

    def test_a_field_of_the_wrong_type(self):
        assert (
            executor.validate_payload(make_payload(args="no")) == "args must be a list"
        )
        assert (
            executor.validate_payload(make_payload(kwargs=[]))
            == "kwargs must be an object"
        )

    def test_not_an_object(self):
        assert executor.validate_payload([1]).startswith(
            "the payload must be a JSON object"
        )
        assert "null" in executor.validate_payload(None)

    def test_a_bad_enqueued_at(self):
        assert executor.validate_payload(make_payload(enqueued_at="yesterday")) == (
            "enqueued_at must be an ISO 8601 date and time"
        )


class TestExecuteTaskFromPayload:
    def test_runs_the_task(self, backend):
        result, success = executor.execute_task_from_payload(make_payload(), "w-1")

        assert success is True
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert result.return_value == 3
        assert result.id == "task-1"
        assert result.worker_ids == ["w-1"]
        assert result.started_at is not None
        assert result.finished_at is not None
        assert result.enqueued_at.isoformat() == "2026-01-01T00:00:00+00:00"

    def test_passes_kwargs(self, backend):
        payload = make_payload(
            "tests.tasks.message_task", args=["ab"], kwargs={"count": 3}
        )

        result, success = executor.execute_task_from_payload(payload, "w-1")

        assert result.return_value == "ababab"

    def test_a_task_that_raises_fails(self, backend):
        payload = make_payload("tests.tasks.failing_task", args=[])

        result, success = executor.execute_task_from_payload(payload, "w-1")

        assert success is False
        assert result.status == TaskResultStatus.FAILED
        assert [e.exception_class_path for e in result.errors] == [
            "builtins.ValueError"
        ]
        assert "Something went wrong" in result.errors[0].traceback
        with pytest.raises(ValueError, match="Task failed"):
            _ = result.return_value

    def test_passes_the_context(self, backend):
        payload = make_payload(
            "tests.tasks.task_with_context", args=["hi"], takes_context=True
        )

        result, success = executor.execute_task_from_payload(payload, "w-1")

        assert result.return_value == "Task task-1 attempt 1: hi"

    def test_runs_a_coroutine(self, backend):
        payload = make_payload("tests.tasks.async_task", args=[1])

        result, success = executor.execute_task_from_payload(payload, "w-1")

        assert result.return_value == 2

    def test_a_task_that_cannot_be_imported(self, backend, caplog):
        payload = make_payload("tests.tasks.no_such_task")

        with caplog.at_level("ERROR", logger="django_tasks_amazon_sqs"):
            result, success = executor.execute_task_from_payload(payload, "w-1")

        assert success is False
        assert result.task is None
        assert result.status == TaskResultStatus.FAILED
        assert result.errors[0].exception_class_path == "builtins.ImportError"
        assert "Task could not be started" in caplog.text

    def test_a_path_that_is_not_a_task(self, backend):
        payload = make_payload("tests.tasks.not_a_task", args=[])

        result, success = executor.execute_task_from_payload(payload, "w-1")

        assert success is False
        assert result.errors[0].exception_class_path == "builtins.TypeError"

    def test_sends_the_signals(self, backend):
        seen = []

        def on_started(sender, task_result, **kwargs):
            seen.append(("started", sender, task_result.status))

        def on_finished(sender, task_result, **kwargs):
            seen.append(("finished", sender, task_result.status))

        task_started.connect(on_started)
        task_finished.connect(on_finished)
        try:
            executor.execute_task_from_payload(make_payload(), "w-1")
        finally:
            task_started.disconnect(on_started)
            task_finished.disconnect(on_finished)

        assert seen == [
            ("started", SQSBackend, TaskResultStatus.RUNNING),
            ("finished", SQSBackend, TaskResultStatus.SUCCESSFUL),
        ]


class TestProcessMessage:
    def test_deletes_the_message_of_a_successful_task(self, backend):
        message = make_message(make_payload(), receipt_handle="rh-ok")

        result = executor.process_message(backend, "default", message, "w-1")

        assert result.status == TaskResultStatus.SUCCESSFUL
        backend._client.delete_message.assert_called_once()
        assert (
            backend._client.delete_message.call_args.kwargs["ReceiptHandle"] == "rh-ok"
        )

    def test_leaves_the_message_of_a_failed_task(self, backend, caplog):
        message = make_message(make_payload("tests.tasks.failing_task", args=[]))

        with caplog.at_level("INFO", logger="django_tasks_amazon_sqs"):
            result = executor.process_message(backend, "default", message, "w-1")

        assert result.status == TaskResultStatus.FAILED
        backend._client.delete_message.assert_not_called()
        assert "left in queue=default for another attempt" in caplog.text

    def test_deletes_a_failed_task_when_retries_are_off(self, backend):
        backend.retry_failed_tasks = False
        try:
            message = make_message(make_payload("tests.tasks.failing_task", args=[]))
            executor.process_message(backend, "default", message, "w-1")
        finally:
            backend.retry_failed_tasks = True

        backend._client.delete_message.assert_called_once()

    def test_leaves_a_task_that_cannot_be_imported(self, backend):
        message = make_message(make_payload("tests.tasks.gone"))

        result = executor.process_message(backend, "default", message, "w-1")

        assert result.status == TaskResultStatus.FAILED
        backend._client.delete_message.assert_not_called()

    def test_discards_a_message_that_is_not_json(self, backend, caplog):
        message = make_message("not json", message_id="bad-1")

        with caplog.at_level("ERROR", logger="django_tasks_amazon_sqs"):
            result = executor.process_message(backend, "default", message, "w-1")

        assert result is None
        backend._client.delete_message.assert_called_once()
        assert "Discarding SQS message bad-1" in caplog.text
        assert "body is not JSON" in caplog.text

    def test_discards_a_payload_with_a_missing_field(self, backend, caplog):
        payload = make_payload()
        del payload["task_path"]

        with caplog.at_level("ERROR", logger="django_tasks_amazon_sqs"):
            result = executor.process_message(
                backend, "default", make_message(payload), "w-1"
            )

        assert result is None
        backend._client.delete_message.assert_called_once()
        assert "missing field: task_path" in caplog.text


class TestProcessTasks:
    def test_stops_when_the_queue_is_empty(self, backend):
        backend._client.receive_message.side_effect = [
            {"Messages": [make_message(make_payload(task_id="a"))]},
            {},
        ]

        results = executor.process_tasks()

        assert [r.id for r in results] == ["a"]
        assert backend._client.receive_message.call_count == 2

    def test_stops_at_max_tasks(self, backend):
        backend._client.receive_message.return_value = {
            "Messages": [make_message(make_payload(task_id="a"))]
        }

        results = executor.process_tasks(max_tasks=3, continuous=True)

        assert len(results) == 3

    def test_asks_for_no_more_than_it_needs(self, backend):
        backend._client.receive_message.return_value = {
            "Messages": [make_message(make_payload(task_id="a"))]
        }

        executor.process_tasks(max_tasks=2, max_messages=10)

        counts = [
            c.kwargs["MaxNumberOfMessages"]
            for c in backend._client.receive_message.call_args_list
        ]
        assert counts == [2, 1]

    def test_continuous_keeps_waiting_until_stopped(self, backend):
        stop_event = threading.Event()
        calls = []

        def receive(**kwargs):
            calls.append(kwargs)
            if len(calls) == 3:
                stop_event.set()
            return {}

        backend._client.receive_message.side_effect = receive

        results = executor.process_tasks(continuous=True, stop_event=stop_event)

        assert results == []
        assert len(calls) == 3

    def test_a_stop_releases_the_messages_not_started(self, backend):
        stop_event = threading.Event()

        def stop_after_the_first(sender, task_result, **kwargs):
            stop_event.set()

        backend._client.receive_message.return_value = {
            "Messages": [
                make_message(make_payload(task_id="a"), receipt_handle="rh-a"),
                make_message(make_payload(task_id="b"), receipt_handle="rh-b"),
            ]
        }
        # weak=False: the signal would otherwise drop a receiver that nothing
        # else references as soon as it is connected.
        task_started.connect(stop_after_the_first, weak=False)
        try:
            results = executor.process_tasks(max_messages=2, stop_event=stop_event)
        finally:
            task_started.disconnect(stop_after_the_first)

        assert [r.id for r in results] == ["a"]
        backend._client.change_message_visibility.assert_called_once()
        call = backend._client.change_message_visibility.call_args.kwargs
        assert call["ReceiptHandle"] == "rh-b"
        assert call["VisibilityTimeout"] == 0

    def test_reads_the_named_queue(self, backend):
        executor.process_tasks(queue_name="ranking", wait_seconds=0)

        call = backend._client.receive_message.call_args.kwargs
        assert call["QueueUrl"].endswith("/ranking")
        assert call["WaitTimeSeconds"] == 0

    def test_records_the_worker_id(self, backend):
        backend._client.receive_message.side_effect = [
            {"Messages": [make_message(make_payload())]},
            {},
        ]

        results = executor.process_tasks(worker_id="worker-x")

        assert results[0].worker_ids == ["worker-x"]

    def test_process_one_task(self, backend):
        backend._client.receive_message.return_value = {
            "Messages": [make_message(make_payload(task_id="one"))]
        }

        result = executor.process_one_task()

        assert result.id == "one"
        assert backend._client.receive_message.call_count == 1

    def test_process_one_task_on_an_empty_queue(self, backend):
        assert executor.process_one_task() is None


class TestRoundTrip:
    def test_what_enqueue_sends_is_what_the_worker_runs(self, backend):
        enqueued = backend.enqueue(
            __import__("tests.tasks", fromlist=["message_task"]).message_task,
            ("x",),
            {"count": 2},
        )
        body = backend._client.send_message.call_args.kwargs["MessageBody"]
        backend._client.receive_message.side_effect = [
            {"Messages": [make_message(json.loads(body))]},
            {},
        ]

        results = executor.process_tasks()

        assert results[0].id == enqueued.id
        assert results[0].return_value == "xx"
        assert results[0].enqueued_at == enqueued.enqueued_at
