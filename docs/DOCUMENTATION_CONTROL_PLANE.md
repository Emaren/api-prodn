---
id: "aoe2war.api-prodn.docs-documentation-control-plane"
title: "api-prodn Documentation Index"
type: "generated"
status: "generated"
owner: "aoe2war-api"
systems: ["api-prodn"]
audience: ["developers","operators","ai-agents"]
source_of_truth: "generated"
authority: "generated-repository-index"
reviewed_at: "2026-07-26"
review_interval_days: 0
sensitivity: "internal"
---

# api-prodn Documentation Index

Repository ID: `api-prodn`

Documentation owner: `aoe2war-api`

Implementation baseline: `main` at `a1ac329cd27506c98bb0d493711deb526a818f61`

The implementation baseline identifies the code commit described by this documentation. Documentation-only commits may follow it without creating a self-referential registry hash.

This page is generated from the validated front matter in this repository. Cross-system architecture, governance, and the unified portal live in the sibling `AoE2WAR-docs` control-plane repository.

## Documentation health

- Authoritative repository documents: **5**
- Path moves in this migration: **0**
- Every listed document has an explicit owner, lifecycle, authority, and review interval.

### Types

- `generated`: 1
- `how-to`: 1
- `reference`: 2
- `runbook`: 1

### Lifecycle

- `active`: 4
- `generated`: 1

## Documents

| Document | Type | Status | Authority |
| --- | --- | --- | --- |
| [api-prodn](../README.md) | `reference` | `active` | `repository-entrypoint` |
| [api-prodn Testing](../TESTING.md) | `how-to` | `active` | `developer-procedure` |
| [api-prodn AI Agent Guide](../agent/README.agent.md) | `reference` | `active` | `ai-agent-contract` |
| [Replay Engine Room worker](REPLAY_ENGINE_ROOM_WORKER.md) | `runbook` | `active` | `operational-procedure` |

## Canonical commands

```bash
python3 scripts/docs_v2_check.py
python3 scripts/docs_v2_check.py --write
python3 scripts/docs_v2_check.py --write --refresh-baseline
```

Use `--write` for documentation-only changes. Use `--refresh-baseline` only after intentional implementation changes, then review the generated index and registry before committing them.
