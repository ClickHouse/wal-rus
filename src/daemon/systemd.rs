//! systemd readiness + watchdog notifications (sd_notify).
//!
//! `NOTIFY_SOCKET` / `WATCHDOG_USEC` are exported by systemd for
//! `Type=notify` units and absent otherwise, so every entry point here
//! no-ops when walross is not run under systemd. Mirrors wal-g's
//! SendSdNotify, sized down to readiness + watchdog keep-alive

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::time::Duration;

/// Send one sd_notify datagram (e.g. `READY=1`, `WATCHDOG=1`). No-op
/// without `NOTIFY_SOCKET`; failures are logged, never fatal
pub fn notify(state: &str) {
    let Some(socket) = std::env::var_os("NOTIFY_SOCKET") else {
        return;
    };
    if let Err(e) = send(&socket, state) {
        tracing::debug!("sd_notify {state:?}: {e}");
    }
}

fn send(socket: &OsStr, state: &str) -> std::io::Result<()> {
    use std::os::unix::net::UnixDatagram;

    let sock = UnixDatagram::unbound()?;
    let bytes = socket.as_bytes();
    // systemd encodes an abstract-namespace socket as a leading '@'
    if bytes.first() == Some(&b'@') {
        use std::os::linux::net::SocketAddrExt;
        use std::os::unix::net::SocketAddr;
        let addr = SocketAddr::from_abstract_name(&bytes[1..])?;
        sock.send_to_addr(state.as_bytes(), &addr)?;
    } else {
        sock.send_to(state.as_bytes(), socket)?;
    }
    Ok(())
}

/// Ping `WATCHDOG=1` at half the `WATCHDOG_USEC` interval (systemd's
/// recommended margin) so a unit with `WatchdogSec` is not killed. No-op
/// when the watchdog is disabled
pub fn spawn_watchdog() {
    let Some(usec) = std::env::var("WATCHDOG_USEC")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&u| u > 0)
    else {
        return;
    };
    let period = Duration::from_micros(usec / 2);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        loop {
            tick.tick().await;
            notify("WATCHDOG=1");
        }
    });
}
