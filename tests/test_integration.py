"""
Tests against a running SQS implementation.

The other test modules stub the boto3 client, which covers the requests the
backend builds but not what a queue does with them: whether a sent message
comes back, whether deleting it stops the redelivery and leaving it brings
it back, whether the delay the backend asks for is honoured. moto answers
those in-process, with no AWS account and no network.
"""

import time
from datetime import timedelta

import pytest
from django.tasks.base import TaskResultStatus
from django.utils import timezone

from django_tasks_amazon_sqs import executor
from django_tasks_amazon_sqs.backends import SQSBackend

from .tasks import failing_task, ranking_task, simple_task

moto_server = pytest.importorskip(
    "moto.server", reason="needs moto: pip install -e '.[dev]'"
)

#: Visibility timeout the redelivery tests run with. Short, because a test
#: that waits for it to expire waits this long for real.
VISIBILITY_TIMEOUT = 1

#: How long a test waits for a message it expects. Never reached when the
#: message is there, which is the case these tests are written for.
WAIT = 5

#: Region moto is addressed with. No request leaves the machine.
REGION = "us-east-1"


@pytest.fixture(scope="session")
def sqs_endpoint():
    """A moto server standing in for SQS, for the whole session."""
    # Bound to the loopback address rather than moto's default 0.0.0.0: the
    # endpoint is built from the bound address, and Windows refuses a
    # connection to 0.0.0.0. It also keeps moto off the network.
    server = moto_server.ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    server.start()
    host, port = server.get_host_and_port()
    try:
        yield f"http://{host}:{port}"
    finally:
        server.stop()


@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch):
    """Credentials boto3 insists on having, which moto does not check."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_REGION", REGION)


@pytest.fixture
def sqs(sqs_endpoint):
    """A client on the queues the tests use, emptied around each test."""
    import boto3

    client = boto3.client("sqs", region_name=REGION, endpoint_url=sqs_endpoint)
    urls = {
        name: client.create_queue(QueueName=name)["QueueUrl"]
        for name in ("default", "ranking")
    }
    try:
        yield client
    finally:
        # Deleted rather than purged: a queue starts empty, and no test
        # inherits what another one left behind.
        for url in urls.values():
            client.delete_queue(QueueUrl=url)


@pytest.fixture
def backend(sqs, sqs_endpoint, settings):
    """The default backend, talking to the moto server."""
    settings.TASKS = {
        "default": {
            "BACKEND": "django_tasks_amazon_sqs.SQSBackend",
            "QUEUES": [],
            "OPTIONS": {
                "AWS_REGION": REGION,
                "SQS_ENDPOINT_URL": sqs_endpoint,
                "VISIBILITY_TIMEOUT": VISIBILITY_TIMEOUT,
            },
        }
    }
    from django.tasks import task_backends

    backend = task_backends["default"]
    assert isinstance(backend, SQSBackend)
    try:
        yield backend
    finally:
        backend.close()


def ids(results):
    return [result.id for result in results]


class TestDelivery:
    def test_an_enqueued_task_is_run_by_the_worker(self, backend):
        enqueued = simple_task.enqueue(21)

        results = executor.process_tasks(wait_seconds=WAIT)

        assert ids(results) == [enqueued.id]
        assert results[0].status == TaskResultStatus.SUCCESSFUL
        assert results[0].return_value == 42

    def test_nothing_comes_back_from_an_empty_queue(self, backend):
        assert executor.process_tasks(wait_seconds=0) == []

    def test_each_queue_keeps_its_own_messages(self, backend):
        enqueued = ranking_task.enqueue(1)

        assert executor.process_tasks(queue_name="default", wait_seconds=0) == []
        assert ids(executor.process_tasks(queue_name="ranking", wait_seconds=WAIT)) == [
            enqueued.id
        ]

    def test_it_finds_a_queue_by_name(self, backend, sqs):
        expected = sqs.get_queue_url(QueueName="ranking")["QueueUrl"]

        assert backend.get_queue_url("ranking") == expected

    def test_a_queue_that_does_not_exist_is_an_error(self, backend):
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError):
            simple_task.using(queue_name="no-such-queue").enqueue(1)

    def test_several_messages_in_one_receive(self, backend):
        enqueued = [simple_task.enqueue(i) for i in range(3)]

        results = executor.process_tasks(max_messages=10, wait_seconds=WAIT)

        assert sorted(ids(results)) == sorted(ids(enqueued))


class TestAcknowledgement:
    def test_a_successful_task_is_not_run_again(self, backend):
        simple_task.enqueue(1)

        executor.process_tasks(wait_seconds=WAIT)
        time.sleep(VISIBILITY_TIMEOUT + 0.5)

        assert executor.process_tasks(wait_seconds=0) == []

    def test_a_failed_task_comes_back(self, backend):
        enqueued = failing_task.enqueue()

        # One task per call: the message is back after the visibility
        # timeout, which is shorter than the wait, so an open-ended loop
        # would run it again and again.
        first = executor.process_tasks(max_tasks=1, wait_seconds=WAIT)
        time.sleep(VISIBILITY_TIMEOUT + 0.5)
        second = executor.process_tasks(max_tasks=1, wait_seconds=WAIT)

        assert ids(first) == ids(second) == [enqueued.id]
        assert second[0].status == TaskResultStatus.FAILED

    def test_a_failed_task_is_dropped_when_retries_are_off(self, backend):
        backend.retry_failed_tasks = False
        failing_task.enqueue()

        executor.process_tasks(wait_seconds=WAIT)
        time.sleep(VISIBILITY_TIMEOUT + 0.5)

        assert executor.process_tasks(wait_seconds=0) == []

    def test_a_message_no_worker_can_run_is_dropped(self, backend, sqs):
        """Otherwise the queue hands the same unusable message over forever."""
        sqs.send_message(QueueUrl=backend.get_queue_url("default"), MessageBody="nope")

        assert executor.process_tasks(wait_seconds=WAIT) == []

        time.sleep(VISIBILITY_TIMEOUT + 0.5)
        assert executor.process_tasks(wait_seconds=0) == []

    def test_a_released_message_comes_back_at_once(self, backend):
        simple_task.enqueue(1)
        message = backend.receive("default", wait_seconds=WAIT)[0]

        backend.release_message("default", message["ReceiptHandle"])

        assert backend.receive("default", wait_seconds=WAIT) != []


class TestDeferredTasks:
    def test_a_delayed_task_is_held_back_and_then_delivered(self, backend):
        run_after = timezone.now() + timedelta(seconds=2)

        enqueued = simple_task.using(run_after=run_after).enqueue(1)

        assert executor.process_tasks(wait_seconds=0) == []
        assert ids(executor.process_tasks(wait_seconds=WAIT)) == [enqueued.id]
