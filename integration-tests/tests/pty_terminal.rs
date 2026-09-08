//! Terminal semantics over the full stack (client → server → worker → pty line
//! discipline), both directions:
//!
//! - **Input:** in-band control bytes reach the shell as real signals / EOF.
//!   They need no protocol frames — they are ordinary stdin bytes the tty turns
//!   into signals, which is why the design carries no per-signal control message.
//! - **Output:** the byte stream is passed through verbatim, whatever the
//!   encoding — arbitrary, non-UTF-8, TUI-style bytes cross unchanged, because
//!   pty output rides WebSocket *binary* frames that nothing decodes.

use shiitake_integration_tests::{
    TestServer, connect_pty, open_frame, open_pty_bytes, pty_send_after_ready, recv_until, resize,
};

/// Arm `script` (which prints `READY` once armed), send one control byte, and
/// assert `marker` — printed only if the byte had its terminal effect — comes
/// back. No `drop_to`, so this runs anywhere (no container/root needed).
async fn assert_control_byte(script: &str, ctrl: u8, marker: &str) {
    let server = TestServer::start().await;
    let _worker = server.spawn_worker().await;
    let out = pty_send_after_ready(
        server.api_port,
        open_frame(&["bash", "-c", script]),
        "READY",
        &[ctrl],
    )
    .await;
    assert!(
        out.contains(marker),
        "expected {marker:?} after the control byte; got: {out:?}"
    );
}

#[tokio::test]
async fn ctrl_c_delivers_sigint() {
    // 0x03 = VINTR → SIGINT to the foreground process group.
    assert_control_byte(
        "trap 'echo CAUGHT_INT; exit 0' INT; echo READY; read _",
        0x03,
        "CAUGHT_INT",
    )
    .await;
}

#[tokio::test]
async fn ctrl_backslash_delivers_sigquit() {
    // 0x1c = VQUIT → SIGQUIT.
    assert_control_byte(
        "trap 'echo CAUGHT_QUIT; exit 0' QUIT; echo READY; read _",
        0x1c,
        "CAUGHT_QUIT",
    )
    .await;
}

#[tokio::test]
async fn ctrl_z_delivers_sigtstp() {
    // 0x1a = VSUSP → SIGTSTP (job-control suspend); trappable, unlike SIGSTOP.
    assert_control_byte(
        "trap 'echo CAUGHT_TSTP; exit 0' TSTP; echo READY; read _",
        0x1a,
        "CAUGHT_TSTP",
    )
    .await;
}

#[tokio::test]
async fn ctrl_d_signals_eof() {
    // 0x04 = VEOF → end-of-file on the read, not a byte delivered to it.
    assert_control_byte(
        "echo READY; if read _; then echo GOTLINE; else echo GOTEOF; fi",
        0x04,
        "GOTEOF",
    )
    .await;
}

#[tokio::test]
async fn resize_reflows_the_tty() {
    let server = TestServer::start().await;
    let _worker = server.spawn_worker().await;

    // The shell reports its tty size on SIGWINCH; opened at 80x24, resized to
    // 100x40, it must see the new size — proving `op:resize` reaches the pty
    // (TIOCSWINSZ) and raises SIGWINCH. `stty size` prints "rows cols".
    let mut ws = connect_pty(
        server.api_port,
        open_frame(&[
            "bash",
            "-c",
            "trap 'stty size; exit 0' WINCH; echo READY; read _",
        ]),
    )
    .await;
    recv_until(&mut ws, "READY").await;
    resize(&mut ws, 100, 40).await;
    recv_until(&mut ws, "40 100").await;
}

#[tokio::test]
async fn arbitrary_output_passes_through_verbatim() {
    let server = TestServer::start().await;
    let _worker = server.spawn_worker().await;

    // `stty raw` turns off output post-processing (OPOST) so the bytes are
    // verbatim, then `printf` emits (octal escapes for portability): a CSI color
    // sequence, 0xFF/0xFE, an overlong-lead + lone-continuation pair, and a NUL —
    // none of which is valid as a whole UTF-8 string.
    let cmd = "stty raw -echo; printf 'A\\033[31m\\377\\376\\300\\200\\000Z'; exit 0";
    let out = open_pty_bytes(server.api_port, open_frame(&["bash", "-c", cmd])).await;

    let needle: &[u8] = b"A\x1b[31m\xff\xfe\xc0\x80\x00Z";
    assert!(
        out.windows(needle.len()).any(|w| w == needle),
        "arbitrary bytes must survive verbatim; got {} bytes: {:02x?}",
        out.len(),
        out
    );
}
