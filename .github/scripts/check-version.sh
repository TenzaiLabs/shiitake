#!/usr/bin/env bash
#
# Enforces the AGENTS.md versioning policy as a single CI invariant:
#
#   1. Lockstep — Cargo.toml [workspace.package].version must equal the
#      shiitake-py pyproject.toml version (Rust and Python ship together).
#   2. Bump-since-release — the in-code version must be strictly greater than
#      the last published release, so the first PR of each release cycle is
#      forced to bump. Later PRs in the cycle pass, since the version is
#      already ahead.
#
# With no published release yet there is no baseline, so the bump check is
# skipped; it begins gating once v0.1.0 is cut. Needs $GH_TOKEN for `gh api`.
set -euo pipefail

cargo_v=$(grep -m1 '^version = ' Cargo.toml | sed -E 's/version = "(.*)"/\1/')
py_v=$(grep -m1 '^version = ' clients/shiitake-py/pyproject.toml | sed -E 's/version = "(.*)"/\1/')

if [ "$cargo_v" != "$py_v" ]; then
  echo "::error::Version mismatch — Cargo.toml ($cargo_v) != pyproject.toml ($py_v). Bump both files together."
  exit 1
fi
echo "In-code version: $cargo_v"

last=$(gh api "repos/${GITHUB_REPOSITORY}/releases/latest" --jq .tag_name 2>/dev/null || true)
if [ -z "$last" ]; then
  echo "No published release yet — skipping bump-since-release check."
  exit 0
fi
last="${last#v}"
echo "Last published release: $last"

highest=$(printf '%s\n%s\n' "$cargo_v" "$last" | sort -V | tail -1)
if [ "$cargo_v" = "$last" ] || [ "$highest" != "$cargo_v" ]; then
  echo "::error::In-code version ($cargo_v) must be greater than the last release ($last). Bump Cargo.toml + pyproject.toml (semver by change scope)."
  exit 1
fi
echo "OK — $cargo_v > $last"
