# Contributing to Shiitake

Thanks for your interest in contributing! 🍄 This guide covers the development
environment, the checks your change needs to pass, how versioning works, and how
to get a change merged.

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE) that covers this repository.

## Development environment

The Rust toolchain is pinned in `rust-toolchain.toml`, so `rustup` picks up the
right version automatically (edition 2024).

The end-to-end test tooling — `k3d`, `kubectl`, `helm`, `python`, `uv` — is
managed by [`mise`](https://mise.jdx.dev) (`mise.toml`):

```bash
mise install   # provisions everything in mise.toml
```

CI provisions the same tools with `jdx/mise-action`.

## Running locally

See [Developing locally](README.md#developing-locally) in the README for running
the server and a worker from source with Cargo and dispatching a command.

## Build & test

Before opening a PR, run what CI runs:

```bash
cargo +nightly fmt --all --check                       # rustfmt — nightly: rustfmt.toml uses unstable options
cargo clippy --workspace --all-targets -- -D warnings  # lint; warnings are errors
cargo test --workspace                                 # unit + in-process integration tests
```

For the full Kubernetes end-to-end suite — it stands up a `k3d` cluster, builds
the images, and runs the HTTP-level and Python-client checks against it (needs
`docker`, `k3d`, `kubectl`, `helm`, `python3`, `uv`):

```bash
bash tests/build.sh && bash tests/setup.sh && bash tests/run.sh
```

CI (`.github/workflows/ci.yml` and `test.yml`) runs format, clippy, test, and a
version check as parallel jobs; all must pass before a PR can merge.

## Versioning

The version is managed in-code and kept in lockstep across the two published
artifacts:

- **Cargo workspace** — `Cargo.toml` `[workspace.package] version` (the server +
  worker images).
- **Python client** — `clients/shiitake-py/pyproject.toml` `version`.

A PR must bump the version (semver — major / minor / patch, sized to its own
changes) **if the target branch hasn't already been bumped since the last
published release**. In practice: the first PR landing after a release bumps both
files; later PRs in the same release cycle inherit that already-pending version
and leave it alone, only raising it further if their change warrants a larger
bump. Always bump both files together so the Rust and Python artifacts stay on
the same version, and the release tag must match the in-code version.

The `version` job in CI (`.github/scripts/check-version.sh`) enforces this: the
in-code version must be strictly greater than the last published release, and
`Cargo.toml` must equal `pyproject.toml`. CI verifies *that* a bump happened, not
whether its size matches the change — that stays a reviewer call.

## Submitting changes

1. For anything substantial, open an issue to discuss it before you start.
2. Fork the repo, create a branch, and make your change with tests.
3. Make sure the checks above pass and the version is bumped if needed.
4. Open a pull request describing **what** changed and **why**. Release Drafter
   autolabels PRs and maintains the draft release notes.

For the architecture, the workspace layout, and the gotchas that keep the static
musl build and the worker's clean-slate guarantee intact, read
[AGENTS.md](AGENTS.md).

## License

By contributing, you agree that your contributions will be licensed under the
[Apache License 2.0](LICENSE).
