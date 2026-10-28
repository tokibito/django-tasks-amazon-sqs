"""
Management command to run an SQS task worker.

The worker receives from the queue, runs the task each message names, and
deletes the message. On SIGTERM or SIGINT no new task is started, the
running one is finished, and the process exits; a second signal exits at
once.
"""

import logging
import signal
import sys
import threading

from django.core.management.base import BaseCommand, CommandError
from django.tasks import DEFAULT_TASK_BACKEND_ALIAS, DEFAULT_TASK_QUEUE_NAME
from django.tasks.base import TaskResultStatus

from django_tasks_amazon_sqs import executor

logger = logging.getLogger("django_tasks_amazon_sqs")

if sys.platform == "win32":
    SHUTDOWN_SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGBREAK)
else:
    SHUTDOWN_SIGNALS = (signal.SIGINT, signal.SIGTERM)


class Command(BaseCommand):
    help = "Run a worker that receives tasks from Amazon SQS and runs them"

    def add_arguments(self, parser):
        parser.add_argument(
            "--queue",
            dest="queue_name",
            default=DEFAULT_TASK_QUEUE_NAME,
            help=f"Queue to receive from (default: {DEFAULT_TASK_QUEUE_NAME})",
        )
        parser.add_argument(
            "--backend",
            dest="backend_name",
            default=DEFAULT_TASK_BACKEND_ALIAS,
            help=f"Backend alias in TASKS (default: {DEFAULT_TASK_BACKEND_ALIAS})",
        )
        parser.add_argument(
            "--continuous",
            action="store_true",
            default=False,
            help="Keep waiting when the queue is empty instead of exiting",
        )
        parser.add_argument(
            "--max-tasks",
            type=int,
            default=0,
            help="Exit after this many tasks (default: 0, no limit)",
        )
        parser.add_argument(
            "--wait-seconds",
            type=int,
            default=None,
            help=(
                "Long polling wait per receive, at most 20 "
                "(default: the backend's WAIT_TIME_SECONDS)"
            ),
        )
        parser.add_argument(
            "--max-messages",
            type=int,
            default=1,
            help="Messages to receive per call, at most 10 (default: 1)",
        )
        parser.add_argument(
            "--no-graceful-shutdown",
            action="store_true",
            default=False,
            help="Do not install the SIGTERM/SIGINT handlers",
        )

    def handle(self, *args, **options):
        queue_name = options["queue_name"]
        backend_name = options["backend_name"]
        max_tasks = options["max_tasks"]

        if max_tasks < 0:
            raise CommandError(f"--max-tasks must not be negative, got {max_tasks}")
        if options["max_messages"] < 1:
            raise CommandError("--max-messages must be at least 1")

        stop_event = threading.Event()
        original_handlers = {}

        def request_stop(signum, frame):
            if stop_event.is_set():
                self.stderr.write("Second signal received, exiting now")
                sys.exit(1)
            stop_event.set()
            self.stdout.write(
                "Shutdown requested, finishing the current task "
                "(send the signal again to exit now)"
            )

        if not options["no_graceful_shutdown"]:
            for signum in SHUTDOWN_SIGNALS:
                original_handlers[signum] = signal.signal(signum, request_stop)

        self.stdout.write(
            f"Receiving tasks from queue={queue_name} backend={backend_name}"
            + (" (continuous)" if options["continuous"] else "")
        )

        worker_id = executor.generate_worker_id()
        try:
            results = executor.process_tasks(
                queue_name=queue_name,
                backend_name=backend_name,
                max_tasks=max_tasks,
                continuous=options["continuous"],
                wait_seconds=options["wait_seconds"],
                max_messages=options["max_messages"],
                stop_event=stop_event,
                worker_id=worker_id,
            )
        finally:
            for signum, handler in original_handlers.items():
                signal.signal(signum, handler)

        failed = sum(1 for r in results if r.status == TaskResultStatus.FAILED)
        self.stdout.write(
            self.style.SUCCESS(
                f"Processed {len(results)} task(s), {failed} failed, worker={worker_id}"
            )
        )
