---
id: "aoe2war.api-prodn.agent-readme-agent"
title: "api-prodn AI Agent Guide"
type: "reference"
status: "active"
owner: "aoe2war-api"
systems: ["api-prodn"]
audience: ["ai-agents","developers"]
source_of_truth: "git"
authority: "ai-agent-contract"
reviewed_at: "2026-07-26"
review_interval_days: 30
sensitivity: "internal"
---

# api-prodn AI Agent Guide

This file is the bounded entry point for AI-assisted work in `api-prodn`.

## Authority boundary

The API owns replay parsing, parser evidence, finality semantics, and API contracts. It does **not** own public market presentation, WoloChain consensus, custody, settlement execution, or payout truth.

HTTP success confirms transport only. It does not prove replay finality, public projection, market settlement, or on-chain payment.

Candidate parser output must remain isolated until explicit promotion. It may never silently mutate effective public or financial truth.

## Start here

1. Read `../README.md` for service structure and runtime entry points.
2. Read `../docs/REPLAY_ENGINE_ROOM_WORKER.md` before changing replay processing or recovery behavior.
3. Read `../TESTING.md` before editing code.
4. Read the cross-system authority model in the sibling `AoE2WAR-docs` repository.

## Working rules

- Inspect current files, tests, migrations, and evidence before proposing a patch.
- Keep raw replay evidence, parser runs, human evidence, adjudications, and effective truth separate.
- Preserve idempotency and append-only evidence.
- Never reinterpret a successful upload as a settled game.
- Never rewrite historical evidence to make a newer conclusion appear original.
- Keep secrets, tokens, private player information, and restricted evidence out of prompts and logs.
- Run the repository documentation and test gates after intentional changes.

## Documentation gate

```bash
python3 scripts/docs_v2_check.py
```

Regenerate the repository index and registry only after an intentional documentation change:

```bash
python3 scripts/docs_v2_check.py --write
```
