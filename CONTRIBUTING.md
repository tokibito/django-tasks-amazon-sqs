# Contributing

Thanks for your interest in django-tasks-amazon-sqs.

Issues and pull requests are welcome at
<https://github.com/tokibito/django-tasks-amazon-sqs>. Everything that
lands in the repository — code, comments, docstrings, commit messages, pull
request descriptions and documentation — is written in English.

## Getting set up

Python 3.12 or newer and Django 6.0 or newer. The tests need no AWS
account, no credentials and no network.

```bash
git clone https://github.com/tokibito/django-tasks-amazon-sqs.git
cd django-tasks-amazon-sqs

python -m venv venv
venv/bin/pip install -e ".[dev]"
```

## Running the tests

```bash
venv/bin/pytest
```

Tests use pytest-django with the settings in `tests/settings.py`, which
configure the backend with a fixed region and a queue URL template, so
that nothing reaches AWS. Most tests replace the boto3 client with a stub
and check the requests the backend builds. `tests/test_integration.py`
starts a [moto](https://docs.getmoto.org/) server in-process and runs the
same code against it, which is what proves that a sent message comes back,
that a deleted one does not, and that a failed one does. Those tests wait
for real visibility timeouts, so the module takes a little under a minute.

Each test module passes when run on its own:

```bash
venv/bin/pytest tests/test_backends.py
venv/bin/pytest tests/test_executor.py
venv/bin/pytest -k "not integration"
```

CI runs the suite against Python 3.12 to 3.14 and Django 6.0 and 6.1. To
catch Django's deprecations early, run it once with them turned into
errors:

```bash
venv/bin/pytest -W error::DeprecationWarning
```

## Linting and formatting

```bash
venv/bin/ruff check .
venv/bin/ruff format .
```

Both have to pass; CI runs `ruff format --check`, which fails on
unformatted code rather than fixing it. The `dev` extra pins the Ruff
version CI uses, so a fresh `pip install -e ".[dev]"` gives you the same
one. The Markdown files are outside Ruff's scope.

## Making a change

A few things are easy to forget:

- **The changelog.** Add a line to the *Unreleased* section of
  `CHANGELOG.md`, saying what a user of the package will notice. Leave
  `version` in `pyproject.toml` and `__version__` in
  `django_tasks_amazon_sqs/__init__.py` alone; releases are cut separately.

- **The README.** New options, settings, environment variables and command
  options are documented there.

- **The message payload.** The JSON body the backend sends is what the
  worker reads back, possibly much later: a queue can hold a message for
  days, and the redrive policy retries it. A change to a field in the
  payload needs a path for messages that were sent by the previous version
  and are still in the queue when the worker is upgraded.

### Backwards compatibility

Projects depend on the keys in `OPTIONS` and their defaults, the
environment variables the region detection reads, the command's options,
and what the worker does with a message after a task fails, so a change to
any of them needs a path that keeps working for someone who upgrades
without reading the release notes. Where something has to go, deprecate it
first: keep it working with a `DeprecationWarning` that names the version
it is removed in. A new option defaults to the behaviour the package had
without it.

## Pull requests

- Branch off `main`.
- One purpose per pull request: a refactor and the feature it makes
  possible, or a fix noticed on the way, are separate pull requests.
- Say what the change is for. A description that explains the problem is
  worth more than one that restates the diff.
- Keep the tests and the linters green.
- Add tests for a behaviour change. A bug fix wants the test that fails
  without it.
