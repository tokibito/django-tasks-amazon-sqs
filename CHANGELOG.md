# Changelog

## 0.1.0

Initial release: a task backend for Django 6.0's task framework on Amazon
SQS. Tasks are sent to the SQS queue named by their `queue_name` as JSON
messages carrying their arguments, and run by the `run_sqs_tasks` worker,
which receives them, runs them, and deletes the message. A failed task's
message is left for SQS to deliver again, so the queue's redrive policy
decides the retries. Delayed execution through `run_after`, up to the 15
minute SQS limit; coroutine tasks; graceful shutdown of the worker on
`SIGTERM` and `SIGINT`; and detection of the region from the environment.
