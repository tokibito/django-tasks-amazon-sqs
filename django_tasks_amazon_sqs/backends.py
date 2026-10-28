"""Amazon SQS backend for the Django tasks framework."""

import json
import logging

from django.core.exceptions import ImproperlyConfigured
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import TaskResult, TaskResultStatus
from django.tasks.exceptions import InvalidTask
from django.tasks.signals import task_enqueued
from django.utils import timezone
from django.utils.crypto import get_random_string

from .detection import detect_aws_region

logger = logging.getLogger("django_tasks_amazon_sqs")

#: The longest delay SQS accepts on a message, in seconds.
SQS_MAX_DELAY_SECONDS = 900

#: The most messages a single ReceiveMessage call may return.
SQS_MAX_RECEIVE_COUNT = 10

#: The longest a receive call may wait for a message, in seconds.
SQS_MAX_WAIT_SECONDS = 20

#: The longest a visibility timeout may be, in seconds (12 hours).
SQS_MAX_VISIBILITY_TIMEOUT = 43200


class SQSBackend(BaseTaskBackend):
    """
    Task backend using Amazon SQS.

    A task is sent to the SQS queue named by its ``queue_name`` as a JSON
    message carrying the task path and its arguments. A worker started with
    ``manage.py run_sqs_tasks`` receives the message, runs the task and
    deletes the message. Nothing is stored in the database, so results
    cannot be retrieved.

    Options:
        AWS_REGION              Region. Detected from AWS_REGION or
                                AWS_DEFAULT_REGION when unset, and left to
                                boto3's own configuration otherwise.
        SQS_ENDPOINT_URL        Endpoint override, for LocalStack or moto.
        SQS_QUEUE_URL_TEMPLATE  Queue URL with a {queue_name} placeholder.
                                Set it to skip the GetQueueUrl call, which
                                also saves needing sqs:GetQueueUrl.
        VISIBILITY_TIMEOUT      Seconds a received message stays hidden.
                                Leave unset to use the queue's own setting;
                                it has to outlast the longest task.
        WAIT_TIME_SECONDS       Long polling wait when receiving
                                (default: 20, the SQS limit).
        MAX_DELAY_SECONDS       Largest delay to put on a message
                                (default: 900, the SQS limit). A task with a
                                run_after further out is rejected.
        RETRY_FAILED_TASKS      Whether a failed task's message is left in
                                the queue for SQS to deliver again
                                (default: True). Set the queue's redrive
                                policy to cap the attempts.
    """

    supports_defer = True
    supports_async_task = True
    supports_get_result = False
    supports_priority = False

    def __init__(self, alias, params):
        super().__init__(alias, params)

        self.region = self.options.get("AWS_REGION") or detect_aws_region()
        self.endpoint_url = self.options.get("SQS_ENDPOINT_URL")
        self.queue_url_template = self.options.get("SQS_QUEUE_URL_TEMPLATE")
        self.visibility_timeout = self.options.get("VISIBILITY_TIMEOUT")
        self.wait_time_seconds = self.options.get(
            "WAIT_TIME_SECONDS", SQS_MAX_WAIT_SECONDS
        )
        self.retry_failed_tasks = self.options.get("RETRY_FAILED_TASKS", True)

        max_delay = self.options.get("MAX_DELAY_SECONDS", SQS_MAX_DELAY_SECONDS)
        if not 0 <= max_delay <= SQS_MAX_DELAY_SECONDS:
            raise ImproperlyConfigured(
                f"MAX_DELAY_SECONDS must be between 0 and {SQS_MAX_DELAY_SECONDS}, "
                f"the longest delay SQS accepts (got {max_delay})."
            )
        self.max_delay_seconds = max_delay

        if self.visibility_timeout is not None and not (
            0 <= self.visibility_timeout <= SQS_MAX_VISIBILITY_TIMEOUT
        ):
            raise ImproperlyConfigured(
                "VISIBILITY_TIMEOUT must be between 0 and "
                f"{SQS_MAX_VISIBILITY_TIMEOUT} (got {self.visibility_timeout})."
            )

        self._client = None
        self._queue_urls = {}

    # -- client ----------------------------------------------------------

    @property
    def client(self):
        """The boto3 SQS client, created on first use."""
        if self._client is None:
            import boto3

            self._client = boto3.client(
                "sqs",
                region_name=self.region,
                endpoint_url=self.endpoint_url,
            )
        return self._client

    def close(self):
        """Drop the client and the cached queue URLs."""
        self._client = None
        self._queue_urls.clear()

    def get_queue_url(self, queue_name):
        """
        Get the URL of the SQS queue a Django queue name refers to.

        Looked up once per queue and then kept, since the URL of a queue
        does not change.
        """
        if queue_name not in self._queue_urls:
            if self.queue_url_template:
                url = self.queue_url_template.format(queue_name=queue_name)
            else:
                url = self.client.get_queue_url(QueueName=queue_name)["QueueUrl"]
            self._queue_urls[queue_name] = url
        return self._queue_urls[queue_name]

    # -- enqueue ---------------------------------------------------------

    def validate_task(self, task):
        super().validate_task(task)

        if task.run_after is not None:
            delay = self.get_delay_seconds(task.run_after)
            if delay > self.max_delay_seconds:
                raise InvalidTask(
                    "run_after is further out than SQS can hold a message: "
                    f"at most {self.max_delay_seconds} seconds from now."
                )

    def get_delay_seconds(self, run_after):
        """Seconds from now until run_after, or 0 when it has passed."""
        if run_after is None:
            return 0
        delay = (run_after - timezone.now()).total_seconds()
        if delay <= 0:
            return 0
        # Rounded up: a message delivered a fraction early would run the
        # task before the time it was asked for.
        return int(-(-delay // 1))

    def build_payload(self, task, task_id, args, kwargs, enqueued_at):
        """The message body a worker reads back; see executor.PAYLOAD_FIELDS."""
        return {
            "task_id": task_id,
            "task_path": task.module_path,
            "args": list(args),
            "kwargs": dict(kwargs),
            "queue_name": task.queue_name,
            "backend": self.alias,
            "priority": task.priority,
            "takes_context": task.takes_context,
            "enqueued_at": enqueued_at.isoformat(),
        }

    def enqueue(self, task, args, kwargs):
        """Send the task to its SQS queue."""
        self.validate_task(task)

        task_id = get_random_string(32)
        now = timezone.now()

        task_result = TaskResult(
            task=task,
            id=task_id,
            status=TaskResultStatus.READY,
            enqueued_at=now,
            started_at=None,
            finished_at=None,
            last_attempted_at=None,
            args=list(args),
            kwargs=dict(kwargs),
            backend=self.alias,
            errors=[],
            worker_ids=[],
        )

        # Built from the result, whose __post_init__ has normalized the
        # arguments to JSON, so that what is sent is what a worker gets.
        payload = self.build_payload(
            task, task_id, task_result.args, task_result.kwargs, now
        )

        response = self.client.send_message(
            QueueUrl=self.get_queue_url(task.queue_name),
            MessageBody=json.dumps(payload),
            DelaySeconds=min(
                self.get_delay_seconds(task.run_after), SQS_MAX_DELAY_SECONDS
            ),
        )
        logger.debug(
            "Task id=%s path=%s sent to queue=%s message=%s",
            task_id,
            task.module_path,
            task.queue_name,
            response.get("MessageId"),
        )

        task_enqueued.send(sender=type(self), task_result=task_result)

        return task_result

    # -- receive ---------------------------------------------------------

    def receive(self, queue_name, max_messages=1, wait_seconds=None):
        """
        Long poll a queue and return the raw messages it hands over.

        SQS accepts at most 10 messages and 20 seconds of waiting per
        call, so larger values are clamped rather than rejected.
        """
        if wait_seconds is None:
            wait_seconds = self.wait_time_seconds

        request = {
            "QueueUrl": self.get_queue_url(queue_name),
            "MaxNumberOfMessages": max(
                1, min(int(max_messages), SQS_MAX_RECEIVE_COUNT)
            ),
            "WaitTimeSeconds": max(0, min(int(wait_seconds), SQS_MAX_WAIT_SECONDS)),
            # The receive count says which attempt this is, for the log.
            "AttributeNames": ["ApproximateReceiveCount"],
        }
        if self.visibility_timeout is not None:
            request["VisibilityTimeout"] = self.visibility_timeout

        response = self.client.receive_message(**request)
        return response.get("Messages", [])

    def delete_message(self, queue_name, receipt_handle):
        """Delete a message, so SQS does not hand it over again."""
        self.client.delete_message(
            QueueUrl=self.get_queue_url(queue_name), ReceiptHandle=receipt_handle
        )

    def release_message(self, queue_name, receipt_handle, delay=0):
        """
        Make a message visible again, so it is delivered another time.

        Args:
            delay: Seconds to keep it hidden first. Zero, the default,
                returns it straight away.
        """
        self.client.change_message_visibility(
            QueueUrl=self.get_queue_url(queue_name),
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=max(0, min(int(delay), SQS_MAX_VISIBILITY_TIMEOUT)),
        )
