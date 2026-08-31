//! Startup reconciliation of the capture volume.
//!
//! Handles live only in memory, so a server restart orphans every capture
//! directory on the volume. `run_sweeper` purges by walking the registry, so it
//! can never reach them. Matters most in the two-pod topology, where the volume
//! outlives the server pod rather than dying with it as an `emptyDir` does.

use shiitake_server::pool::WorkerPool;
use shiitake_worker_api::{
    ExecId,
    capture::{Stream, handle_dir, stream_path},
};
use std::{path::Path, sync::Arc};
use tempfile::TempDir;

/// Capture files as a previous server's workers would have left them.
fn seed_handle(root: &Path, id: &str) {
    let handle = ExecId::new(id);
    std::fs::create_dir_all(handle_dir(root, &handle)).unwrap();
    std::fs::write(stream_path(root, &handle, Stream::Stdout), b"stale out").unwrap();
    std::fs::write(stream_path(root, &handle, Stream::Stderr), b"stale err").unwrap();
}

fn pool(capture: &TempDir) -> Arc<WorkerPool> {
    Arc::new(WorkerPool::new(
        None,
        "shiitake-test".into(),
        "test".into(),
        capture.path().to_path_buf(),
    ))
}

#[tokio::test]
async fn startup_drops_capture_left_by_a_previous_process() {
    let capture = TempDir::new().unwrap();
    for id in ["dead-1", "dead-2", "dead-3"] {
        seed_handle(capture.path(), id);
    }

    pool(&capture).reconcile_capture().await;

    assert_eq!(
        std::fs::read_dir(capture.path()).unwrap().count(),
        0,
        "every orphaned handle directory should be gone"
    );
}

// The reconcile runs before anything is dispatched, so a handle created after it
// is untouched — the sweep is a one-shot at boot, not a recurring purge.
#[tokio::test]
async fn reconcile_does_not_touch_capture_written_afterwards() {
    let capture = TempDir::new().unwrap();
    seed_handle(capture.path(), "dead-1");

    let pool = pool(&capture);
    pool.reconcile_capture().await;

    seed_handle(capture.path(), "live-1");
    let live = stream_path(capture.path(), &ExecId::new("live-1"), Stream::Stdout);
    assert!(live.exists());

    // A second reconcile would take it — which is exactly why it is called once,
    // at startup, while the registry is empty.
    assert!(!handle_dir(capture.path(), &ExecId::new("dead-1")).exists());
    assert_eq!(std::fs::read(&live).unwrap(), b"stale out");
}

// A server whose capture root doesn't exist yet (first boot on a fresh volume)
// must not fail: main creates the directory moments later.
#[tokio::test]
async fn reconcile_tolerates_a_capture_root_that_does_not_exist() {
    let parent = TempDir::new().unwrap();
    let pool = Arc::new(WorkerPool::new(
        None,
        "shiitake-test".into(),
        "test".into(),
        parent.path().join("capture-not-created-yet"),
    ));
    pool.reconcile_capture().await;
}
