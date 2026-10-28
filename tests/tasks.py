"""Task definitions for the tests."""

from django.tasks import task


@task
def add_numbers(x, y):
    """Task that adds two numbers."""
    return x + y


@task
def simple_task(x):
    """Task that doubles its input."""
    return x * 2


@task
def message_task(message, count=1):
    """Task that repeats a message."""
    return message * count


@task
def failing_task():
    """Task that always fails."""
    raise ValueError("Something went wrong")


@task(takes_context=True)
def task_with_context(context, message):
    """Task that receives the context."""
    return f"Task {context.task_result.id} attempt {context.attempt}: {message}"


@task
async def async_task(x):
    """Coroutine task."""
    return x + 1


@task(queue_name="ranking")
def ranking_task(x):
    """Task on a queue other than the default."""
    return x


def not_a_task():
    """A plain function, which a payload must not be able to run."""
