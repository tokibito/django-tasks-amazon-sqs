"""
Task execution: turning an SQS message back into a running task.

``execute_task_from_payload`` runs the task a message body names;
``process_message`` wraps it with what the queue needs (deleting the
message, or leaving it for another attempt); ``process_tasks`` is the loop
``run_sqs_tasks`` runs, usable from any process of your own.
"""

import json
import logging
from datetime import datetime
from traceback import format_exception

from django.tasks import DEFAULT_TASK_BACKEND_ALIAS, task_backends
from django.tasks.base import (
    Task,
    TaskContext,
    TaskError,
    TaskResult,
    TaskResultStatus,
)
from django.tasks.signals import task_finished, task_started
from django.utils import timezone
from django.utils.crypto import get_random_string
from django.utils.module_loading import import_string

logger = logging.getLogger("django_tasks_amazon_sqs")

# Fields execute_task_from_payload() requires, with the JSON type of each.
PAYLOAD_FIELDS = {
    "task_id": (str, "a string"),
    "task_path": (str, "a string"),
    "args": (list, "a list"),
    "kwargs": (dict, "an object"),
    "queue_name": (str, "a string"),
    "backend": (str, "a string"),
}


def generate_worker_id():
    """An id for the process running tasks, recorded in worker_ids."""
    return get_random_string(32)


def _json_type(value):
    """Name the JSON type of a value parsed from JSON."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "a boolean"
    if isinstance(value, int | float):
        return "a number"
    if isinstance(value, str):
        return "a string"
    return "an array"


def validate_payload(payload):
    """
    Check that a payload can be executed.

    Returns:
        str: Why the payload cannot be executed, or None when it can
    """
    if not isinstance(payload, dict):
        return f"the payload must be a JSON object, not {_json_type(payload)}"

    for name, (expected_type, description) in PAYLOAD_FIELDS.items():
        if name not in payload:
            return f"missing field: {name}"
        if not isinstance(payload[name], expected_type):
            return f"{name} must be {description}"

    enqueued_at = payload.get("enqueued_at")
    if enqueued_at is not None:
        try:
            datetime.fromisoformat(enqueued_at)
        except (TypeError, ValueError):
            return "enqueued_at must be an ISO 8601 date and time"

    return None


def _task_error(exc):
    exception_type = type(exc)
    return TaskError(
        exception_class_path=f"{exception_type.__module__}.{exception_type.__qualname__}",
        traceback="".join(format_exception(exc)),
    )


def execute_task_from_payload(payload, worker_id, backend=None):
    """
    Execute the task a payload names.

    A task that cannot be resolved (the module was renamed, the function
    removed, the worker runs older code than the process that enqueued it,
    or the path names something that is not a Task) is reported as FAILED
    with the error, like a task that raised. task_started and task_finished
    are not sent for it, since there is no task to send them with; the
    ERROR record "Task could not be started" reports it instead.

    Args:
        payload: The message body, parsed from JSON and validated
        worker_id: Identifier of the worker running the task
        backend: The backend the message came from, the sender of the
            signals; the one the payload names when omitted

    Returns:
        tuple: (TaskResult, success: bool). The result of a task that could
        not be started has ``task`` set to None.
    """
    task_id = payload["task_id"]
    task_path = payload["task_path"]
    args = payload["args"]
    kwargs = payload["kwargs"]
    backend_alias = payload["backend"]
    takes_context = payload.get("takes_context", False)
    enqueued_at_str = payload.get("enqueued_at")

    if backend is None:
        backend = task_backends[backend_alias]
    sender = type(backend)

    enqueued_at = None
    if enqueued_at_str:
        enqueued_at = datetime.fromisoformat(enqueued_at_str)

    now = timezone.now()

    try:
        task = import_string(task_path)
        if not isinstance(task, Task):
            raise TypeError(f"{task_path} is not a Task")
    except Exception as e:
        error = _task_error(e)
        logger.exception(
            "Task could not be started: id=%s path=%s error=%s",
            task_id,
            task_path,
            error.exception_class_path,
        )
        task_result = TaskResult(
            task=None,
            id=task_id,
            status=TaskResultStatus.FAILED,
            enqueued_at=enqueued_at,
            started_at=None,
            finished_at=now,
            last_attempted_at=now,
            args=args,
            kwargs=kwargs,
            backend=backend_alias,
            errors=[error],
            worker_ids=[worker_id],
        )
        return task_result, False

    task_result = TaskResult(
        task=task,
        id=task_id,
        status=TaskResultStatus.RUNNING,
        enqueued_at=enqueued_at,
        started_at=now,
        finished_at=None,
        last_attempted_at=now,
        args=args,
        kwargs=kwargs,
        backend=backend_alias,
        errors=[],
        worker_ids=[worker_id],
    )

    task_started.send(sender=sender, task_result=task_result)

    try:
        if takes_context:
            return_value = task.call(
                TaskContext(task_result=task_result), *args, **kwargs
            )
        else:
            return_value = task.call(*args, **kwargs)
    except KeyboardInterrupt:
        # If the user tried to terminate, let them.
        raise
    except BaseException as e:
        object.__setattr__(task_result, "finished_at", timezone.now())
        object.__setattr__(task_result, "status", TaskResultStatus.FAILED)
        error = _task_error(e)
        task_result.errors.append(error)

        logger.error(
            "Task failed: id=%s path=%s error=%s",
            task_id,
            task_path,
            error.exception_class_path,
        )
        task_finished.send(sender=sender, task_result=task_result)
        return task_result, False

    object.__setattr__(task_result, "finished_at", timezone.now())
    object.__setattr__(task_result, "status", TaskResultStatus.SUCCESSFUL)
    object.__setattr__(task_result, "_return_value", return_value)

    logger.info("Task completed successfully: id=%s path=%s", task_id, task_path)
    task_finished.send(sender=sender, task_result=task_result)
    return task_result, True


def read_payload(message):
    """
    Read the payload out of an SQS message.

    Returns:
        tuple: (payload, error). The payload is None when the body cannot
        be executed, and the error says why.
    """
    try:
        payload = json.loads(message.get("Body", ""))
    except ValueError:
        return None, "body is not JSON"

    error = validate_payload(payload)
    if error is not None:
        return None, error
    return payload, None


def process_message(backend, queue_name, message, worker_id):
    """
    Run the task one SQS message carries, and settle the message.

    A message that ran successfully is deleted. A message whose task failed,
    or could not be started, is deleted when the backend's
    RETRY_FAILED_TASKS is False, and otherwise left alone: it reappears
    when its visibility timeout expires, and the queue's redrive policy
    moves it to the dead-letter queue after the attempts it allows. A
    message nothing can run (not JSON, or missing a field) is deleted,
    since every attempt would end the same way, and reported at ERROR.

    Returns:
        TaskResult or None: the result, or None for a message that could
        not be read.
    """
    receipt_handle = message.get("ReceiptHandle")
    receive_count = message.get("Attributes", {}).get("ApproximateReceiveCount")

    payload, error = read_payload(message)
    if payload is None:
        logger.error(
            "Discarding SQS message %s from queue=%s: %s (receive_count=%s)",
            message.get("MessageId"),
            queue_name,
            error,
            receive_count,
        )
        backend.delete_message(queue_name, receipt_handle)
        return None

    logger.debug(
        "Task id=%s path=%s received from queue=%s receive_count=%s",
        payload["task_id"],
        payload["task_path"],
        queue_name,
        receive_count,
    )

    task_result, success = execute_task_from_payload(payload, worker_id, backend)

    if success or not backend.retry_failed_tasks:
        backend.delete_message(queue_name, receipt_handle)
    else:
        logger.info(
            "Task id=%s left in queue=%s for another attempt (receive_count=%s)",
            task_result.id,
            queue_name,
            receive_count,
        )

    return task_result


def process_tasks(
    queue_name=None,
    backend_name=DEFAULT_TASK_BACKEND_ALIAS,
    max_tasks=0,
    continuous=False,
    wait_seconds=None,
    max_messages=1,
    stop_event=None,
    worker_id=None,
):
    """
    Receive messages from a queue and run the tasks they carry.

    Args:
        queue_name: Queue to read from (default: the backend's "default").
        backend_name: Alias of the SQS backend in TASKS.
        max_tasks: Stop after this many tasks (0 = no limit).
        continuous: Keep waiting when the queue is empty. Without it, the
            loop returns the first time a receive brings nothing back.
        wait_seconds: Long polling wait per receive (default: the backend's
            WAIT_TIME_SECONDS). Each wait is capped at 20 seconds, the SQS
            limit, so a stop request is noticed within that.
        max_messages: Messages to receive per call, at most 10.
        stop_event: A threading.Event-like object; the loop returns after
            the current task when it is set.
        worker_id: Identifier recorded in worker_ids (default: generated).

    Returns:
        list of TaskResult: the tasks that ran, in order.
    """
    from django.tasks import DEFAULT_TASK_QUEUE_NAME

    backend = task_backends[backend_name]
    queue_name = queue_name or DEFAULT_TASK_QUEUE_NAME
    worker_id = worker_id or generate_worker_id()
    results = []

    def stopping():
        return stop_event is not None and stop_event.is_set()

    while not stopping():
        if max_tasks and len(results) >= max_tasks:
            break

        remaining = max_tasks - len(results) if max_tasks else max_messages
        messages = backend.receive(
            queue_name,
            max_messages=min(max_messages, remaining),
            wait_seconds=wait_seconds,
        )
        if not messages:
            if continuous:
                continue
            break

        for message in messages:
            if stopping():
                # Not touched: it comes back when the visibility timeout
                # expires, or sooner when released here.
                backend.release_message(queue_name, message["ReceiptHandle"])
                continue
            result = process_message(backend, queue_name, message, worker_id)
            if result is not None:
                results.append(result)

    return results


def process_one_task(
    queue_name=None,
    backend_name=DEFAULT_TASK_BACKEND_ALIAS,
    wait_seconds=None,
    worker_id=None,
):
    """
    Receive one message and run its task.

    Returns:
        TaskResult or None: the result, or None when the queue was empty
        or the message could not be read.
    """
    results = process_tasks(
        queue_name=queue_name,
        backend_name=backend_name,
        max_tasks=1,
        wait_seconds=wait_seconds,
        worker_id=worker_id,
    )
    return results[0] if results else None
