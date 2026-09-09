//! PTY session behavior, full stack: the real worker driven through the real
//! server's pool + `/api/v1/pty` handler, with a WebSocket client. Covers the
//! server seam the worker-only test (`shiitake-worker/tests/pty.rs`) cannot —
//! pinning a pooled worker, splicing the sockets, release + reset, concurrency,
//! the per-user identity, and the session's lifecycle (a quiet session held
//! alive by the keepalive; a clean close when the worker dies).

use serde_json::json;
use shiitake_integration_tests::{
    TestServer, connect_pty, may_create_accounts, open_frame, open_pty, open_pty_bytes_with_pause,
    recv_close, recv_until,
};
use std::{sync::Arc, time::Duration};
use tokio::time::timeout;

#[tokio::test]
async fn streams_through_the_server_then_releases_the_worker() {
    let server = TestServer::start().await;
    let _worker = server.spawn_worker().await;

    assert_eq!(server.pool.pinned_count().await, 0, "no session pinned yet");

    let text = open_pty(
        server.api_port,
        open_frame(&["bash", "-c", "echo HELLO_FROM_PTY; exit 7"]),
    )
    .await;
    assert!(
        text.contains("HELLO_FROM_PTY"),
        "the server should splice the shell's output to the client; got: {text:?}"
    );

    // Released + reset, so the pool is whole and idle again — proven by pinned
    // dropping back to 0 and a SECOND session succeeding on the same worker.
    let settled = timeout(Duration::from_secs(10), async {
        while !(server.pool.pinned_count().await == 0 && server.pool.snapshot().await.0 >= 1) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        settled.is_ok(),
        "worker was not released + re-advertised after the session"
    );

    let again = open_pty(
        server.api_port,
        open_frame(&["bash", "-c", "echo SECOND_SESSION"]),
    )
    .await;
    assert!(
        again.contains("SECOND_SESSION"),
        "the same worker should serve a second session"
    );
}

/// Two users attached at once: each session pins its own worker, both stream
/// concurrently, and the pool reports two pinned workers while they overlap.
#[tokio::test]
async fn two_sessions_run_concurrently_on_two_workers() {
    let server = TestServer::start().await;
    let _workers = server.spawn_workers(2).await;

    // Sample the pinned count while the two sessions overlap (~1s each).
    let poller = tokio::spawn({
        let pool = Arc::clone(&server.pool);
        async move {
            let mut max = 0;
            for _ in 0..40 {
                max = max.max(pool.pinned_count().await);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            max
        }
    });

    let (a, b) = tokio::join!(
        open_pty(
            server.api_port,
            open_frame(&["bash", "-c", "echo START_A; sleep 1"])
        ),
        open_pty(
            server.api_port,
            open_frame(&["bash", "-c", "echo START_B; sleep 1"])
        ),
    );
    let max_pinned = poller.await.unwrap();

    assert!(a.contains("START_A"), "session A should stream; got: {a:?}");
    assert!(b.contains("START_B"), "session B should stream; got: {b:?}");
    assert_eq!(
        max_pinned, 2,
        "both sessions should hold a pinned worker at the same time"
    );

    // Both released afterwards.
    let settled = timeout(Duration::from_secs(10), async {
        while server.pool.pinned_count().await != 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        settled.is_ok(),
        "both workers should be released after the sessions end"
    );
}

/// `drop_to.name` makes the shell run as a real login name. This actually writes
/// `/etc/passwd` + a home, so it runs ONLY inside a disposable container (root +
/// `SHIITAKE_PTY_USERADD_TEST=1`); a host `cargo test` skips it. The account is
/// removed by the between-session reset — the container run asserts nothing is
/// left behind.
#[tokio::test]
async fn names_the_session_user_when_privileged() {
    if !may_create_accounts() {
        eprintln!(
            "skipping named-user scenario: needs root on Linux AND SHIITAKE_PTY_USERADD_TEST=1 \
             (set only in a throwaway container — it writes /etc/passwd)"
        );
        return;
    }

    let server = TestServer::start().await;
    let _worker = server.spawn_worker().await;

    let mut open = open_frame(&["bash", "-c", "echo whoami=$(whoami); echo home=$HOME"]);
    open["drop_to"] = json!({
        "uid": 2_000_123,
        "gid": 2_000_123,
        "supplementary_gids": [],
        "umask": 0o022,
        "name": "alice",
        "create_home": true,
    });
    open["env"]["HOME"] = json!("/home/alice");

    let text = open_pty(server.api_port, open).await;
    assert!(
        text.contains("whoami=alice"),
        "shell should run as the named user; got: {text:?}"
    );
    assert!(
        text.contains("home=/home/alice"),
        "HOME should be the created home; got: {text:?}"
    );
}

/// `SHIITAKE_HOME_ROOT` relocates the created home; shiitake sets the child's
/// HOME to match (the client sends no HOME here). Same container-only gating as
/// the named-user scenario.
#[tokio::test]
async fn home_root_env_relocates_the_session_home() {
    if !may_create_accounts() {
        eprintln!(
            "skipping home-root scenario: needs root on Linux AND SHIITAKE_PTY_USERADD_TEST=1"
        );
        return;
    }

    let server = TestServer::start().await;
    let _worker = server
        .spawn_workers_with_env(1, &[("SHIITAKE_HOME_ROOT", "/var/lib/tzhomes")])
        .await;

    let mut open = open_frame(&["bash", "-c", "echo home=$HOME; echo whoami=$(whoami)"]);
    open["drop_to"] = json!({
        "uid": 2_000_777,
        "gid": 2_000_777,
        "supplementary_gids": [],
        "umask": 0o022,
        "name": "bob",
        "create_home": true,
    });
    // Deliberately no HOME in env — shiitake supplies it from the created home.

    let text = open_pty(server.api_port, open).await;
    assert!(
        text.contains("whoami=bob"),
        "shell should run as bob; got: {text:?}"
    );
    assert!(
        text.contains("home=/var/lib/tzhomes/bob"),
        "HOME should be under SHIITAKE_HOME_ROOT; got: {text:?}"
    );
}

#[tokio::test]
async fn quiet_session_survives_past_the_worker_lease() {
    // Short server keepalive (300ms) under a short worker lease (1s): the handler
    // pings the pinned worker often enough to keep its lease fresh, so a session
    // that produces output but takes NO input for longer than the lease is not
    // reaped. Without the keepalive the worker's lease would fire at 1s and kill
    // it before `SURVIVED`.
    let server = TestServer::start_with_keepalive(Duration::from_millis(300)).await;
    let _worker = server
        .spawn_workers_with_env(1, &[("SHIITAKE_LEASE_TIMEOUT", "1")])
        .await;

    let out = open_pty(
        server.api_port,
        open_frame(&["bash", "-c", "echo READY; sleep 2.5; echo SURVIVED"]),
    )
    .await;
    assert!(
        out.contains("SURVIVED"),
        "a quiet session must outlive the worker lease; got: {out:?}"
    );
}

#[tokio::test]
async fn large_output_survives_backpressure() {
    let server = TestServer::start().await;
    let _worker = server.spawn_worker().await;

    // 5 MB of one byte, far more than the per-session output backlog. The client
    // pauses mid-stream, which fills the bounded channel and back-pressures the
    // worker instead of buffering it all — then reads the rest. Every byte must
    // still arrive: backpressure is lossless, and the worker-side awaited send
    // must not deadlock (else this times out).
    const N: usize = 5_000_000;
    let out = open_pty_bytes_with_pause(
        server.api_port,
        open_frame(&["bash", "-c", "head -c 5000000 /dev/zero | tr '\\0' X"]),
        100_000,
        Duration::from_millis(500),
    )
    .await;

    let xs = out.iter().filter(|&&b| b == b'X').count();
    assert_eq!(
        xs, N,
        "every byte of a large output must arrive under backpressure; got {xs} of {N}"
    );
}

#[tokio::test]
async fn worker_death_closes_the_client_with_a_reason() {
    let server = TestServer::start().await;
    let mut worker = server.spawn_worker().await;

    let mut ws = connect_pty(
        server.api_port,
        open_frame(&["bash", "-c", "echo READY; sleep 10"]),
    )
    .await;
    recv_until(&mut ws, "READY").await;

    // The worker dies mid-session — the client must be told, not left hanging.
    worker.kill().await.expect("kill worker");

    let (code, reason) = recv_close(&mut ws)
        .await
        .expect("the client should receive a close frame after the worker dies");
    assert_eq!(
        code, 1011,
        "an abnormal worker loss closes with 1011; reason: {reason:?}"
    );
    assert!(
        reason.contains("worker disconnected"),
        "the close reason should name the cause; got: {reason:?}"
    );
}
