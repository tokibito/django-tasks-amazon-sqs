"""Shared fixtures."""

import json
from unittest.mock import MagicMock

import pytest
from django.tasks import task_backends


def make_message(payload, message_id="m-1", receipt_handle="rh-1", receive_count=1):
    """An SQS message as ReceiveMessage returns it."""
    body = payload if isinstance(payload, str) else json.dumps(payload)
    return {
        "MessageId": message_id,
        "ReceiptHandle": receipt_handle,
        "Body": body,
        "Attributes": {"ApproximateReceiveCount": str(receive_count)},
    }


@pytest.fixture
def backend():
    """The default backend with its SQS client replaced by a stub."""
    backend = task_backends["default"]
    backend.close()
    backend._client = MagicMock()
    backend._client.send_message.return_value = {"MessageId": "sent-1"}
    backend._client.receive_message.return_value = {}
    try:
        yield backend
    finally:
        backend.close()
