#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import posixpath
import subprocess
from collections import Counter
from datetime import date, datetime, timezone
from typing import Any

EXCLUDED_EXACT = frozenset(
    {
        ".git",
        ".next",
        ".direnv",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        ".tox",
        ".nox",
        ".cache",
        "__pycache__",
        "node_modules",
        "site-packages",
        "dist-packages",
        "coverage",
        "htmlcov",
        "dist",
        "build",
        "site",
    }
)
EXCLUDED_PREFIXES = (".venv", "venv")
REQUIRED_FIELDS = (
    "id",
    "title",
    "type",
    "status",
    "owner",
    "systems",
    "audience",
    "source_of_truth",
    "authority",
    "reviewed_at",
    "review_interval_days",
    "sensitivity",
)
ALLOWED_TYPES = {
    "tutorial",
    "how-to",
    "reference",
    "explanation",
    "runbook",
    "adr",
    "generated",
    "historical",
    "working",
}
ALLOWED_STATUSES = {"active", "draft", "historical", "superseded", "generated"}
ALLOWED_SENSITIVITY = {"public", "internal", "restricted"}
ALLOWED_SOURCES = {"git", "generated", "runtime-evidence", "historical-evidence"}
INDEX_TITLES = {
    "app-prodn": "app-prodn Documentation Index",
    "api-prodn": "api-prodn Documentation Index",
    "aoe2-watcher": "aoe2-watcher Documentation Index",
}
CATALOG_SPECS = {
    "app-prodn": {
        "title": "AoE2WAR Web Application",
        "type": "website",
        "owner": "group:default/aoe2war-web",
        "depends_on": [
            "component:default/api-prodn",
            "resource:default/wolochain",
        ],
    },
    "api-prodn": {
        "title": "AoE2WAR Replay API",
        "type": "service",
        "owner": "group:default/aoe2war-api",
        "depends_on": [
            "resource:default/postgresql",
            "resource:default/replay-archive",
        ],
    },
    "aoe2-watcher": {
        "title": "AoE2WAR Desktop Watcher",
        "type": "application",
        "owner": "group:default/aoe2war-watcher",
        "depends_on": ["component:default/api-prodn"],
    },
}
DOCUMENTATION_OWNED_EXACT = {
    "catalog-info.yaml",
    "docs/document-registry.json",
    "scripts/docs_v2_check.py",
}


def is_excluded(relative: pathlib.PurePath) -> bool:
    for part in relative.parts:
        lowered = part.lower()
        if lowered in EXCLUDED_EXACT:
            return True
        if any(lowered.startswith(prefix) for prefix in EXCLUDED_PREFIXES):
            return True
    return False


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_value(root: pathlib.Path, *arguments: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), *arguments], text=True
    ).strip()


def git_success(root: pathlib.Path, *arguments: str) -> bool:
    return (
        subprocess.run(
            ["git", "-C", str(root), *arguments],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        ).returncode
        == 0
    )


def documentation_owned_path(path_value: str) -> bool:
    path = pathlib.PurePosixPath(path_value)
    if path_value in DOCUMENTATION_OWNED_EXACT:
        return True
    if path.suffix.lower() in {".md", ".mdx"}:
        return True
    if path.parts and path.parts[0] == "docs":
        return True
    return False


def validate_implementation_baseline(
    root: pathlib.Path,
    baseline_branch: str,
    baseline_commit: str,
) -> None:
    if not baseline_branch.strip():
        raise ValueError("implementation baseline branch is empty")
    if not baseline_commit.strip():
        raise ValueError("implementation baseline commit is empty")
    if not git_success(root, "cat-file", "-e", f"{baseline_commit}^{{commit}}"):
        raise ValueError(
            f"implementation baseline commit does not exist: {baseline_commit}"
        )
    current_head = git_value(root, "rev-parse", "HEAD")
    if not git_success(root, "merge-base", "--is-ancestor", baseline_commit, current_head):
        raise ValueError(
            "implementation baseline is not an ancestor of the current repository HEAD"
        )
    changed = git_value(
        root,
        "diff",
        "--name-only",
        f"{baseline_commit}..{current_head}",
    ).splitlines()
    invalid = sorted(path for path in changed if not documentation_owned_path(path))
    if invalid:
        raise ValueError(
            "implementation changed after the recorded baseline; regenerate intentionally with "
            "python3 scripts/docs_v2_check.py --write --refresh-baseline. "
            f"Non-document paths: {invalid}"
        )


def project_markdown(root: pathlib.Path) -> list[pathlib.Path]:
    found: list[pathlib.Path] = []
    for path in root.rglob("*"):
        if path.is_symlink() or not path.is_file():
            continue
        if path.suffix.lower() not in {".md", ".mdx"}:
            continue
        relative = path.relative_to(root)
        if is_excluded(relative):
            continue
        found.append(path)
    return sorted(found)


def parse_frontmatter(
    path: pathlib.Path, root: pathlib.Path
) -> tuple[dict[str, Any], str]:
    relative = path.relative_to(root).as_posix()
    text = path.read_text(errors="replace")
    if not text.startswith("---\n"):
        raise ValueError(f"{relative}: missing YAML front matter")
    end = text.find("\n---\n", 4)
    if end < 0:
        raise ValueError(f"{relative}: unterminated YAML front matter")
    block = text[4:end]
    metadata: dict[str, Any] = {}
    for number, line in enumerate(block.splitlines(), start=2):
        if not line.strip():
            continue
        if line[:1].isspace() or ":" not in line:
            raise ValueError(
                f"{relative}:{number}: front matter must use one JSON-compatible value per key"
            )
        key, raw = line.split(":", 1)
        key = key.strip()
        if not key or key in metadata:
            raise ValueError(f"{relative}:{number}: invalid or duplicate key {key!r}")
        try:
            metadata[key] = json.loads(raw.strip())
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"{relative}:{number}: value for {key} must be valid JSON-compatible YAML: {exc}"
            ) from exc
    return metadata, text[end + 5 :]


def render_frontmatter(metadata: dict[str, Any]) -> str:
    lines = ["---"]
    for key in REQUIRED_FIELDS:
        lines.append(
            f"{key}: {json.dumps(metadata[key], ensure_ascii=False, separators=(',', ': '))}"
        )
    lines.extend(["---", ""])
    return "\n".join(lines) + "\n"


def validate_metadata(metadata: dict[str, Any], label: str) -> list[str]:
    errors: list[str] = []
    missing = [key for key in REQUIRED_FIELDS if key not in metadata]
    if missing:
        errors.append(f"{label}: missing metadata {missing}")
        return errors
    extra = sorted(set(metadata) - set(REQUIRED_FIELDS))
    if extra:
        errors.append(f"{label}: unsupported metadata fields {extra}")
    for key in (
        "id",
        "title",
        "type",
        "status",
        "owner",
        "source_of_truth",
        "authority",
        "reviewed_at",
        "sensitivity",
    ):
        value = metadata.get(key)
        if not isinstance(value, str) or not value.strip():
            errors.append(f"{label}: {key} must be a non-empty string")
    doc_id = metadata.get("id")
    if isinstance(doc_id, str) and not doc_id.startswith("aoe2war."):
        errors.append(f"{label}: id must start with aoe2war.")
    if metadata.get("type") not in ALLOWED_TYPES:
        errors.append(f"{label}: unsupported type {metadata.get('type')!r}")
    if metadata.get("status") not in ALLOWED_STATUSES:
        errors.append(f"{label}: unsupported status {metadata.get('status')!r}")
    if metadata.get("sensitivity") not in ALLOWED_SENSITIVITY:
        errors.append(f"{label}: unsupported sensitivity {metadata.get('sensitivity')!r}")
    if metadata.get("source_of_truth") not in ALLOWED_SOURCES:
        errors.append(f"{label}: unsupported source_of_truth {metadata.get('source_of_truth')!r}")
    reviewed_at = metadata.get("reviewed_at")
    if isinstance(reviewed_at, str):
        try:
            parsed = date.fromisoformat(reviewed_at)
            if parsed.isoformat() != reviewed_at:
                raise ValueError
        except ValueError:
            errors.append(f"{label}: reviewed_at must use YYYY-MM-DD")
    for key in ("systems", "audience"):
        value = metadata.get(key)
        if not isinstance(value, list) or not value or not all(
            isinstance(item, str) and item.strip() for item in value
        ):
            errors.append(f"{label}: {key} must be a non-empty string list")
    interval = metadata.get("review_interval_days")
    if not isinstance(interval, int) or isinstance(interval, bool) or interval < 0:
        errors.append(f"{label}: review_interval_days must be a non-negative integer")
    status = metadata.get("status")
    if status in {"historical", "superseded", "generated"} and interval != 0:
        errors.append(f"{label}: {status} documents require review_interval_days 0")
    if metadata.get("type") == "generated" and status != "generated":
        errors.append(f"{label}: generated type requires generated status")
    if status == "generated" and metadata.get("source_of_truth") != "generated":
        errors.append(f"{label}: generated status requires generated source_of_truth")
    if metadata.get("type") == "historical" and status != "historical":
        errors.append(f"{label}: historical type requires historical status")
    return errors


def first_heading(body: str) -> str | None:
    lines = body.splitlines()
    for index, line in enumerate(lines):
        if line.startswith("# "):
            return line[2:].strip()
        if index + 1 < len(lines) and line.strip():
            marker = lines[index + 1].strip()
            if marker and set(marker) in ({"="}, {"-"}):
                return line.strip()
    return None


def load_documents(root: pathlib.Path) -> list[dict[str, Any]]:
    errors: list[str] = []
    documents: list[dict[str, Any]] = []
    for path in project_markdown(root):
        relative = path.relative_to(root).as_posix()
        try:
            metadata, body = parse_frontmatter(path, root)
        except ValueError as exc:
            errors.append(str(exc))
            continue
        errors.extend(validate_metadata(metadata, relative))
        heading = first_heading(body)
        if not heading:
            errors.append(f"{relative}: missing first-level heading")
        elif heading != metadata.get("title"):
            errors.append(
                f"{relative}: first heading {heading!r} does not match title {metadata.get('title')!r}"
            )
        documents.append({**metadata, "path": relative, "sha256": sha256(path)})
    ids = [str(item.get("id")) for item in documents]
    paths = [str(item.get("path")) for item in documents]
    if len(ids) != len(set(ids)):
        errors.append("duplicate document IDs")
    if len(paths) != len(set(paths)):
        errors.append("duplicate document paths")
    if errors:
        raise ValueError("repository documentation contract failed:\n- " + "\n- ".join(errors))
    return sorted(documents, key=lambda item: str(item["path"]))


def render_catalog_info(repo_id: str) -> str:
    spec = CATALOG_SPECS.get(repo_id)
    if spec is None:
        raise ValueError(f"unsupported repository ID {repo_id!r}")
    lines = [
        "apiVersion: backstage.io/v1alpha1",
        "kind: Component",
        "metadata:",
        f"  name: {repo_id}",
        f"  title: {spec['title']}",
        "  annotations:",
        "    backstage.io/techdocs-ref: dir:.",
        "spec:",
        f"  type: {spec['type']}",
        "  lifecycle: production",
        f"  owner: {spec['owner']}",
        "  system: aoe2war",
        "  dependsOn:",
    ]
    for dependency in spec["depends_on"]:
        lines.append(f"    - {dependency}")
    return "\n".join(lines) + "\n"


def render_repository_index(
    repo_id: str,
    owner: str,
    documents: list[dict[str, Any]],
    baseline_branch: str,
    baseline_commit: str,
) -> str:
    adapter_path = "docs/DOCUMENTATION_CONTROL_PLANE.md"
    adapter = next((item for item in documents if item["path"] == adapter_path), None)
    if adapter is None:
        raise ValueError(f"{adapter_path} is missing")
    expected_title = INDEX_TITLES.get(repo_id)
    if not expected_title:
        raise ValueError(f"unsupported repository ID {repo_id!r}")
    if adapter["title"] != expected_title:
        raise ValueError(
            f"{adapter_path}: title must be {expected_title!r}, found {adapter['title']!r}"
        )
    if adapter["owner"] != owner:
        raise ValueError(f"{adapter_path}: owner mismatch")

    type_counts = Counter(str(item["type"]) for item in documents)
    status_counts = Counter(str(item["status"]) for item in documents)
    lines = [
        render_frontmatter({key: adapter[key] for key in REQUIRED_FIELDS}).rstrip(),
        "",
        f"# {expected_title}",
        "",
        f"Repository ID: `{repo_id}`",
        "",
        f"Documentation owner: `{owner}`",
        "",
        f"Implementation baseline: `{baseline_branch}` at `{baseline_commit}`",
        "",
        (
            "The implementation baseline identifies the code commit described by this documentation. "
            "Documentation-only commits may follow it without creating a self-referential registry hash."
        ),
        "",
        (
            "This page is generated from the validated front matter in this repository. "
            "Cross-system architecture, governance, and the unified portal live in the sibling "
            "`AoE2WAR-docs` control-plane repository."
        ),
        "",
        "## Documentation health",
        "",
        f"- Authoritative repository documents: **{len(documents)}**",
        "- Path moves in this migration: **0**",
        "- Every listed document has an explicit owner, lifecycle, authority, and review interval.",
        "",
        "### Types",
        "",
    ]
    for key, value in sorted(type_counts.items()):
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "### Lifecycle", ""])
    for key, value in sorted(status_counts.items()):
        lines.append(f"- `{key}`: {value}")
    lines.extend(
        [
            "",
            "## Documents",
            "",
            "| Document | Type | Status | Authority |",
            "| --- | --- | --- | --- |",
        ]
    )
    for item in documents:
        if item["path"] == adapter_path:
            continue
        relative_link = posixpath.relpath(str(item["path"]), start="docs")
        lines.append(
            f"| [{item['title']}]({relative_link}) | `{item['type']}` | "
            f"`{item['status']}` | `{item['authority']}` |"
        )
    lines.extend(
        [
            "",
            "## Canonical commands",
            "",
            "```bash",
            "python3 scripts/docs_v2_check.py",
            "python3 scripts/docs_v2_check.py --write",
            "python3 scripts/docs_v2_check.py --write --refresh-baseline",
            "```",
            "",
            (
                "Use `--write` for documentation-only changes. Use `--refresh-baseline` only after "
                "intentional implementation changes, then review the generated index and registry "
                "before committing them."
            ),
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def resolve_configuration(
    root: pathlib.Path,
    registry_path: pathlib.Path,
    repo_id_argument: str | None,
    owner_argument: str | None,
) -> tuple[str, str, dict[str, Any]]:
    stored: dict[str, Any] = {}
    if registry_path.exists():
        loaded = json.loads(registry_path.read_text())
        if isinstance(loaded, dict):
            stored = loaded
    repo_id = repo_id_argument or stored.get("repo")
    owner = owner_argument or stored.get("owner")
    if not isinstance(repo_id, str) or not repo_id:
        raise ValueError("repository ID is unavailable; pass --repo-id")
    if not isinstance(owner, str) or not owner:
        raise ValueError("documentation owner is unavailable; pass --owner")
    return repo_id, owner, stored


def stored_baseline(stored: dict[str, Any]) -> tuple[str | None, str | None]:
    baseline = stored.get("implementation_baseline")
    if isinstance(baseline, dict):
        branch = baseline.get("branch")
        commit = baseline.get("commit")
        return (
            branch if isinstance(branch, str) else None,
            commit if isinstance(commit, str) else None,
        )
    old_branch = stored.get("branch")
    old_head = stored.get("head")
    return (
        old_branch if isinstance(old_branch, str) else None,
        old_head if isinstance(old_head, str) else None,
    )


def build_registry(
    documents: list[dict[str, Any]],
    repo_id: str,
    owner: str,
    baseline_branch: str,
    baseline_commit: str,
    generated_at: str,
) -> dict[str, Any]:
    for item in documents:
        if item["owner"] != owner:
            raise ValueError(
                f"{item['path']}: owner {item['owner']!r} does not match repository owner {owner!r}"
            )
    return {
        "schema_version": "2.1.0",
        "generated_at": generated_at,
        "repo": repo_id,
        "owner": owner,
        "implementation_baseline": {
            "branch": baseline_branch,
            "commit": baseline_commit,
        },
        "documents": documents,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--refresh-baseline", action="store_true")
    parser.add_argument("--root")
    parser.add_argument("--registry")
    parser.add_argument("--head")
    parser.add_argument("--branch")
    parser.add_argument("--generated-at")
    parser.add_argument("--repo-id")
    parser.add_argument("--owner")
    args = parser.parse_args()

    if args.refresh_baseline and not args.write:
        raise ValueError("--refresh-baseline requires --write")

    default_root = pathlib.Path(__file__).resolve().parents[1]
    root = pathlib.Path(args.root).expanduser().resolve() if args.root else default_root
    registry_path = (
        pathlib.Path(args.registry).expanduser().resolve()
        if args.registry
        else root / "docs/document-registry.json"
    )
    repo_id, owner, stored = resolve_configuration(
        root, registry_path, args.repo_id, args.owner
    )
    generated_at = (
        args.generated_at
        or str(stored.get("generated_at") or "")
        or datetime.now(timezone.utc).isoformat()
    )
    stored_branch, stored_commit = stored_baseline(stored)
    current_branch = git_value(root, "branch", "--show-current")
    current_head = git_value(root, "rev-parse", "HEAD")
    baseline_branch = (
        args.branch
        or (current_branch if args.refresh_baseline else stored_branch)
        or current_branch
    )
    baseline_commit = (
        args.head
        or (current_head if args.refresh_baseline else stored_commit)
        or current_head
    )
    validate_implementation_baseline(root, baseline_branch, baseline_commit)

    catalog_path = root / "catalog-info.yaml"
    expected_catalog = render_catalog_info(repo_id)
    if args.write:
        if not catalog_path.exists() or catalog_path.read_text(errors="replace") != expected_catalog:
            catalog_path.write_text(expected_catalog)
    elif not catalog_path.exists() or catalog_path.read_text(errors="replace") != expected_catalog:
        raise ValueError(
            "catalog-info.yaml drift; run python3 scripts/docs_v2_check.py --write"
        )

    documents = load_documents(root)
    expected_index = render_repository_index(
        repo_id,
        owner,
        documents,
        baseline_branch,
        baseline_commit,
    )
    index_path = root / "docs/DOCUMENTATION_CONTROL_PLANE.md"

    if args.write:
        if index_path.read_text(errors="replace") != expected_index:
            index_path.write_text(expected_index)
        documents = load_documents(root)
        expected_index = render_repository_index(
            repo_id,
            owner,
            documents,
            baseline_branch,
            baseline_commit,
        )
        if index_path.read_text(errors="replace") != expected_index:
            raise ValueError("generated repository documentation index is not stable")
        registry = build_registry(
            documents,
            repo_id,
            owner,
            baseline_branch,
            baseline_commit,
            generated_at,
        )
        registry_path.parent.mkdir(parents=True, exist_ok=True)
        registry_path.write_text(json.dumps(registry, indent=2, sort_keys=True) + "\n")
        display = (
            registry_path.relative_to(root)
            if registry_path.is_relative_to(root)
            else registry_path
        )
        print(f"PASS: wrote {display} with {len(documents)} explicit documents")
        print(
            "PASS: implementation baseline preserved at "
            f"{baseline_branch}@{baseline_commit}"
        )
        return 0

    if index_path.read_text(errors="replace") != expected_index:
        raise ValueError(
            "generated repository documentation index drift; run "
            "python3 scripts/docs_v2_check.py --write"
        )
    if not registry_path.exists():
        raise ValueError("docs/document-registry.json is missing; run with --write")
    current = build_registry(
        documents,
        repo_id,
        owner,
        baseline_branch,
        baseline_commit,
        generated_at,
    )
    stored_compare = json.loads(registry_path.read_text())
    if not isinstance(stored_compare, dict):
        raise ValueError("documentation registry must contain an object")
    current_compare = dict(current)
    stored_compare = dict(stored_compare)
    current_compare.pop("generated_at", None)
    stored_compare.pop("generated_at", None)
    if stored_compare != current_compare:
        raise ValueError(
            "documentation registry drift; run "
            "python3 scripts/docs_v2_check.py --write and review the diff"
        )

    print(
        f"PASS: documentation registry is current "
        f"({len(documents)} explicit documents; 0 candidates; 0 unclassified)"
    )
    print(f"PASS: {repo_id} generated documentation index is current")
    print(
        "PASS: implementation baseline is valid at "
        f"{baseline_branch}@{baseline_commit}"
    )
    print("PASS: Backstage catalog metadata is canonical and valid")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as exc:
        raise SystemExit(f"STOP: {exc}") from exc
