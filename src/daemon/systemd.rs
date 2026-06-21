//! systemd readiness + watchdog notifications (sd_notify).
//!
//! `NOTIFY_SOCKET` / `WATCHDOG_USEC` are exported by systemd for
//! `Type=notify` units and absent otherwise, so every entry point here
//! no-ops when walrus is not run under systemd. Mirrors wal-g's
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixDatagram;
    use std::sync::Mutex;

    // notify / spawn_watchdog read NOTIFY_SOCKET / WATCHDOG_USEC from the
    // process env; serialize every test that mutates them
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn lock_env() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn set_env(k: &str, v: Option<&str>) {
        unsafe {
            match v {
                Some(x) => std::env::set_var(k, x),
                None => std::env::remove_var(k),
            }
        }
    }

    #[test]
    fn notify_without_socket_is_noop() {
        let _g = lock_env();
        set_env("NOTIFY_SOCKET", None);
        notify("READY=1"); // absent NOTIFY_SOCKET: returns without sending
    }

    #[test]
    fn notify_sends_datagram_to_path_socket() {
        let _g = lock_env();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("notify.sock");
        let server = UnixDatagram::bind(&path).unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        set_env("NOTIFY_SOCKET", Some(path.to_str().unwrap()));
        notify("READY=1");
        set_env("NOTIFY_SOCKET", None);

        let mut buf = [0u8; 64];
        let n = server.recv(&mut buf).expect("READY datagram");
        assert_eq!(&buf[..n], b"READY=1");
    }

    #[test]
    fn send_reaches_abstract_namespace_socket() {
        use std::os::linux::net::SocketAddrExt;
        use std::os::unix::net::SocketAddr;
        let name = format!("walrus-sd-test-{}", std::process::id());
        let addr = SocketAddr::from_abstract_name(name.as_bytes()).unwrap();
        let server = UnixDatagram::bind_addr(&addr).unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        // systemd encodes abstract sockets with a leading '@'
        send(&std::ffi::OsString::from(format!("@{name}")), "WATCHDOG=1").unwrap();

        let mut buf = [0u8; 64];
        let n = server.recv(&mut buf).expect("abstract-namespace datagram");
        assert_eq!(&buf[..n], b"WATCHDOG=1");
    }

    #[test]
    fn watchdog_without_usec_is_noop() {
        let _g = lock_env();
        set_env("WATCHDOG_USEC", None);
        // No runtime needed: must return before spawning anything
        spawn_watchdog();
    }

    // multi-thread so the spawned pinger runs while the test blocks on recv
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watchdog_pings_when_enabled() {
        let _g = lock_env();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wd.sock");
        let server = UnixDatagram::bind(&path).unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        set_env("NOTIFY_SOCKET", Some(path.to_str().unwrap()));
        set_env("WATCHDOG_USEC", Some("1000000")); // 1s → first tick fires immediately
        spawn_watchdog();

        let mut buf = [0u8; 64];
        let n = server.recv(&mut buf).expect("watchdog ping");
        assert_eq!(&buf[..n], b"WATCHDOG=1");

        set_env("WATCHDOG_USEC", None);
        set_env("NOTIFY_SOCKET", None);
    }
}
