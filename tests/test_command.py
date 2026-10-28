"""Tests for the run_sqs_tasks management command."""

from io import StringIO

import pytest
from django.core.management import CommandError, call_command

from .conftest import make_message
from .test_executor import make_payload


def run(*args, **kwargs):
    out = StringIO()
    call_command("run_sqs_tasks", *args, stdout=out, **kwargs)
    return out.getvalue()


class TestRunSqsTasks:
    def test_processes_what_is_queued_and_exits(self, backend):
        backend._client.receive_message.side_effect = [
            {"Messages": [make_message(make_payload(task_id="a"))]},
            {
                "Messages": [
                    make_message(make_payload("tests.tasks.failing_task", args=[]))
                ]
            },
            {},
        ]

        output = run()

        assert "Receiving tasks from queue=default backend=default" in output
        assert "Processed 2 task(s), 1 failed" in output

    def test_max_tasks(self, backend):
        backend._client.receive_message.return_value = {
            "Messages": [make_message(make_payload())]
        }

        output = run("--max-tasks", "2", "--continuous")

        assert "Processed 2 task(s), 0 failed" in output

    def test_queue_and_wait(self, backend):
        run("--queue", "ranking", "--wait-seconds", "0", "--max-messages", "5")

        call = backend._client.receive_message.call_args.kwargs
        assert call["QueueUrl"].endswith("/ranking")
        assert call["WaitTimeSeconds"] == 0
        assert call["MaxNumberOfMessages"] == 5

    def test_rejects_a_negative_max_tasks(self, backend):
        with pytest.raises(CommandError, match="--max-tasks"):
            run("--max-tasks", "-1")

    def test_rejects_zero_max_messages(self, backend):
        with pytest.raises(CommandError, match="--max-messages"):
            run("--max-messages", "0")

    def test_without_signal_handlers(self, backend):
        output = run("--no-graceful-shutdown")

        assert "Processed 0 task(s)" in output
