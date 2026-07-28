#!/usr/bin/env python3

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


MAX_FILE_BYTES = 8 * 1024 * 1024

PLACEHOLDER_RE = re.compile(
    r"""
    replace[-_]?me
    |change[-_]?me
    |placeholder
    |example
    |dummy
    |fake
    |test[-_]?(?:password|pass)?
    |<[^>\r\n]+>
    |\$\{[^}\r\n]+\}
    """,
    re.IGNORECASE | re.VERBOSE,
)

CHECKS = (
    (
        "Render deploy hook",
        re.compile(
            r"https://api\.render\.com/deploy/"
            r"[^\s\"'?]+"
            r"\?key=[A-Za-z0-9_-]{6,}"
        ),
        False,
    ),
    (
        "credentialed PostgreSQL URL",
        re.compile(
            r"postgres(?:ql)?(?:\+asyncpg)?://"
            r"[^:\s/@]+:"
            r"[^@\s/]+@"
        ),
        True,
    ),
    (
        "private key material",
        re.compile(
            r"-----BEGIN "
            r"(?:RSA |EC |OPENSSH )?"
            r"PRIVATE KEY-----"
        ),
        False,
    ),
)


def repository_files() -> list[Path]:
    raw = subprocess.check_output(
        [
            "git",
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ]
    )

    return [
        Path(value.decode("utf-8", errors="surrogateescape"))
        for value in raw.split(b"\0")
        if value
    ]


def main() -> int:
    findings: list[tuple[str, str]] = []
    scanned = 0
    skipped_binary = 0
    skipped_large = 0

    for path in repository_files():
        try:
            file_stat = path.stat()
        except OSError:
            continue

        if not path.is_file():
            continue

        if file_stat.st_size > MAX_FILE_BYTES:
            skipped_large += 1
            continue

        try:
            data = path.read_bytes()
        except OSError:
            continue

        if b"\0" in data[:4096]:
            skipped_binary += 1
            continue

        text = data.decode(
            "utf-8",
            errors="replace",
        )

        scanned += 1

        for label, pattern, allow_placeholder in CHECKS:
            for match in pattern.finditer(text):
                value = match.group(0)

                if (
                    allow_placeholder
                    and PLACEHOLDER_RE.search(value)
                ):
                    continue

                findings.append(
                    (
                        str(path),
                        label,
                    )
                )

    if findings:
        print(
            "Tracked-secret scan failed:",
            file=sys.stderr,
        )

        for path, label in sorted(set(findings)):
            print(
                f"- {path}: {label}",
                file=sys.stderr,
            )

        print(
            "Secret values were suppressed.",
            file=sys.stderr,
        )

        return 1

    print(
        "PASS: tracked-secret scan completed; "
        f"text_files={scanned} "
        f"binary_skipped={skipped_binary} "
        f"large_skipped={skipped_large}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
