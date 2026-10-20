# Cluster end-to-end suite

This suite builds the server and test-worker images, deploys them to k3d with
Helm, and calls the public session API through a local port-forward. It runs
under both supported topologies and includes the Python client tests.

```bash
bash tests/build.sh
bash tests/setup.sh
bash tests/run.sh
SHIITAKE_E2E_TOPOLOGY=two-pod bash tests/setup.sh
SHIITAKE_E2E_TOPOLOGY=two-pod bash tests/run.sh
```

`mise install` provides k3d, kubectl, Helm, Python, and uv. Docker must be
running. `tests/setup.sh` reuses an existing cluster and rolls out the images
again, so this sequence can be repeated after code changes. Delete the local
cluster with `k3d cluster delete shiitake-e2e` when finished.

The Helm chart defines a `default` profile with `/shared` and an `isolated`
profile without that application mount. Each profile uses a distinct dispatch
credential. In single-pod mode workers are separate containers in one pod.
In two-pod mode the server and each worker profile run in separate Deployments;
workers have no service account token and a NetworkPolicy only permits DNS and
the dispatcher. The server alone mounts its local `emptyDir` output spool.
Worker stdout and stderr cross the dispatch WebSocket and never need a shared
capture volume. The dispatch Service publishes not-ready addresses so workers
can register before server readiness succeeds. The server ServiceAccount has a
pod-reader Role with `get` permission for OOM detection.

`test_exec.py` covers profile allocation, environment, working directory,
stateful and parallel commands, streamed/ranged output, cancellation, timeout,
OOM, and readiness. `test_two_pod.py` checks the split deployment and network
policy. The shiitake-py tests cover the client and detachable terminal,
including terminal signals, resize, byte transparency, named-user drops, and
concurrent sessions. `test_pod_failures.py` kills worker pods to verify handle
and terminal reconciliation.

`tests/run.sh` waits for all default and isolated workers before beginning.
Override `SHIITAKE_E2E_WORKERS` and `SHIITAKE_E2E_ISOLATED_WORKERS` if the chart
counts are changed. The suite scrapes server metrics from its test
OpenTelemetry collector at the end.
