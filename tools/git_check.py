#!/usr/bin/env python3

import os
import subprocess

repos = {
    "local": {
        "app-prodn": "/Users/tonyblum/projects/AoE2HDBets/app-prodn",
        "api-prodn": "/Users/tonyblum/projects/AoE2HDBets/api-prodn",
        "aoe2-watcher": "/Users/tonyblum/projects/AoE2HDBets/aoe2-watcher",
    },
    "vps-prod": {
        "app-prodn": "/var/www/AoE2HDBets/app-prodn",
        "api-prodn": "/var/www/AoE2HDBets/api-prodn",
    },
}


def check_status(repo_path):
    if not os.path.exists(repo_path):
        return f"{repo_path} not found"
    if not os.path.isdir(os.path.join(repo_path, ".git")):
        return f"{repo_path} is not a git repo"

    try:
        branch = subprocess.check_output(
            ["git", "-C", repo_path, "branch", "--show-current"], text=True
        ).strip()
        status = subprocess.check_output(
            ["git", "-C", repo_path, "status", "--short"], text=True
        ).strip()
        ahead_behind = subprocess.check_output(
            ["git", "-C", repo_path, "rev-list", "--left-right", "--count", f"{branch}...origin/{branch}"],
            text=True,
        ).strip()
        ahead, behind = map(int, ahead_behind.split())
        dirty = "yes" if status else "no"
        return f"{repo_path} [{branch}] ahead={ahead} behind={behind} dirty={dirty}"
    except Exception as exc:
        return f"{repo_path} error: {exc}"


def main():
    for scope, paths in repos.items():
        print(f"\n{scope.upper()}")
        for _, path in paths.items():
            print(" -", check_status(path))


if __name__ == "__main__":
    main()
