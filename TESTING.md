# api-prodn Testing

## Goal

Keep this file brutally practical: how to run the backend safely, how to run tests, and what is currently broken or ambiguous.

## Runtime truth

- VPS repo path: `/var/www/AoE2HDBets/api-prodn`
- service: `aoe2hdbets-api.service`
- bind: `127.0.0.1:3330`
- production exec:

```bash
/var/www/AoE2HDBets/api-prodn/venv/bin/uvicorn app:app --host 127.0.0.1 --port 3330
```

## Local setup

The repo currently has multiple Python env hints. Do not assume `pytest` is on the default shell path.

Use an explicit venv:

```bash
cd /Users/tonyblum/projects/AoE2HDBets/api-prodn
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If the project-specific environment already exists, use it explicitly instead of guessing:

```bash
source .venv-codex/bin/activate
```

## Local API run

The actual app entrypoint is `app:app`.

Suggested local run:

```bash
cd /Users/tonyblum/projects/AoE2HDBets/api-prodn
source .venv-codex/bin/activate
uvicorn app:app --reload --host 127.0.0.1 --port 3330
```

Older scripts and README references still mention ports like `8002` and `8003`. Those are legacy/dev leftovers, not current production truth.

## Test commands

Preferred:

```bash
cd /Users/tonyblum/projects/AoE2HDBets/api-prodn
source .venv-codex/bin/activate
pytest -q
```

Targeted test file:

```bash
pytest tests/<file>.py -q
```

## Current testing debt

Known current problem:
- in the default shell session, `pytest` was not found on PATH

That means the testing workflow is still too implicit and needs cleanup.

Before trusting a future test pass, verify:
- the active venv
- installed test dependencies
- any async pytest plugin requirements
- replay fixture availability

## Suggested cleanup next

1. make one venv path canonical
2. document one exact passing test command
3. remove or clearly mark legacy run scripts using old ports
4. identify which tests are real gatekeepers for replay parsing and upload flows
