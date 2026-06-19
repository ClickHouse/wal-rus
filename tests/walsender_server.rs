//! Round-trip test: wire wal-rs's walsender server to
//! its own `ReplicationConn` client side via a real TCP socket.
//!
//! Validates the wire shapes a PG18 walreceiver needs:
//! * `StartupMessage` with `replication=true` reaches the server
//! * Server emits `AuthenticationOk`, `ParameterStatus`*,
//!   `BackendKeyData`, `ReadyForQuery`
//! * `IDENTIFY_SYSTEM` round-trips the cached identity
//! * `START_REPLICATION PHYSICAL` flips into CopyBoth mode
//! * `'w'` XLogData + `'k'` keepalive frames the client decodes via
//!   `decode_frame` match what the server emitted
//! * `'r'` standby status the client emits via `build_status_update`
//!   parses cleanly on the server via `decode_standby_status`

use std::time::Duration;

use fallible_iterator::FallibleIterator;
use pgwalrs::pg::replication::conn::{PgConfig, ReplicationConn};
use pgwalrs::pg::replication::server::{
    Identity, WalSenderConn, decode_standby_status, handshake_and_await_start,
};
use pgwalrs::pg::replication::stream::{
    Frame, build_status_update, decode_frame, encode_keepalive_frame, encode_wal_data_frame,
};
use pgwalrs::pg::replication::tls::SslMode;
use postgres_protocol::message::backend::Message;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

fn test_identity() -> Identity {
    Identity {
        system_id: "7340000000000000000".into(),
        timeline: 1,
        xlogpos: 0,
        dbname: None,
    }
}

fn client_config(port: u16, sslmode: SslMode) -> PgConfig {
    PgConfig {
        host: "127.0.0.1".into(),
        port,
        user: "u".into(),
        password: None,
        database: "u".into(),
        application_name: "wal-rs-server-test".into(),
        sslmode,
    }
}

#[tokio::test]
async fn protocol_roundtrip_through_tcp() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let identity = Identity {
        system_id: "7340000000000000000".into(),
        timeline: 1,
        xlogpos: 0x016B_3750,
        dbname: None,
    };
    let payload = b"hello world".to_vec();
    let server_task = tokio::spawn({
        let identity = identity.clone();
        let payload = payload.clone();
        async move {
            let (sock, _) = listener.accept().await.expect("accept");
            let _ = sock.set_nodelay(true);
            let mut sock = sock;
            let started = handshake_and_await_start(&mut sock, &identity)
                .await
                .expect("handshake");
            assert_eq!(started.start_lsn, 0x016B_3750);
            let mut conn = WalSenderConn::new(sock);
            // Emit one WAL data frame.
            let wal = encode_wal_data_frame(started.start_lsn, started.start_lsn, &payload);
            conn.write_raw(&wal).await.expect("write w");
            // Emit one keepalive (reply_requested = true to elicit
            // a 'r' standby status from the client).
            let ka = encode_keepalive_frame(started.start_lsn + payload.len() as u64, true);
            conn.write_raw(&ka).await.expect("write k");
            // Read inbound 'r' status update.
            let frame = conn
                .try_recv_frame()
                .await
                .expect("recv")
                .expect("frame body");
            let status = decode_standby_status(&frame).expect("status");
            (started.start_lsn, status.flush_lsn)
        }
    });

    let cfg = PgConfig {
        host: "127.0.0.1".into(),
        port: addr.port(),
        user: "u".into(),
        password: None,
        database: "u".into(),
        application_name: "wal-rs-server-test".into(),
        sslmode: SslMode::Disable,
    };
    let mut client = ReplicationConn::connect(&cfg)
        .await
        .expect("client connect");
    // IDENTIFY_SYSTEM
    client
        .send_query("IDENTIFY_SYSTEM")
        .await
        .expect("send identify");
    let mut got_systemid: Option<String> = None;
    let mut got_xlogpos: Option<String> = None;
    loop {
        let msg = client.recv_message().await.expect("identify recv");
        match msg {
            Message::DataRow(row) => {
                let mut ranges = row.ranges();
                let buf = row.buffer();
                let mut col_idx = 0;
                while let Some(maybe_range) = ranges.next().expect("next range") {
                    if let Some(range) = maybe_range {
                        let bytes = &buf[range];
                        let s = String::from_utf8_lossy(bytes).to_string();
                        match col_idx {
                            0 => got_systemid = Some(s),
                            2 => got_xlogpos = Some(s),
                            _ => {}
                        }
                    }
                    col_idx += 1;
                }
            }
            Message::CommandComplete(_) => continue,
            Message::ReadyForQuery(_) => break,
            _ => continue,
        }
    }
    assert_eq!(got_systemid.as_deref(), Some("7340000000000000000"));
    assert_eq!(got_xlogpos.as_deref(), Some("0/16B3750"));

    // START_REPLICATION PHYSICAL 0/16B3750
    client
        .send_query("START_REPLICATION PHYSICAL 0/16B3750")
        .await
        .expect("send start");
    client
        .expect_copy_both_open()
        .await
        .expect("CopyBothResponse");

    // Read 'w' XLogData + 'k' keepalive.
    let mut saw_wal = false;
    let mut saw_keepalive = false;
    for _ in 0..2 {
        let msg = client.recv_message().await.expect("copy data recv");
        match msg {
            Message::CopyData(d) => {
                let payload = d.data();
                match decode_frame(payload).expect("decode frame") {
                    Frame::Wal(w) => {
                        assert_eq!(w.start_lsn, 0x016B_3750);
                        assert_eq!(w.data, b"hello world");
                        saw_wal = true;
                    }
                    Frame::Keepalive(_k) => {
                        saw_keepalive = true;
                    }
                }
            }
            _ => panic!("unexpected message"),
        }
    }
    assert!(saw_wal && saw_keepalive);

    let status = build_status_update(
        0x016B_3750 + payload.len() as u64,
        0x016B_3750 + payload.len() as u64,
        0x016B_3750 + payload.len() as u64,
    );
    client.send_copy_data(&status).await.expect("send status");

    let (server_start, server_flush) = tokio::time::timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server timeout")
        .expect("server join");
    assert_eq!(server_start, 0x016B_3750);
    assert_eq!(
        server_flush,
        0x016B_3750 + payload.len() as u64,
        "server saw client's flush_lsn"
    );
}

/// sslmode=prefer makes the real client emit an SSLRequest first; the server's
/// startup reader must answer 'N' and re-read the actual StartupMessage.
#[tokio::test]
async fn ssl_request_restart_with_real_client() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let identity = test_identity();
    let server_task = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        handshake_and_await_start(&mut sock, &identity)
            .await
            .expect("handshake")
            .start_lsn
    });

    let mut client = ReplicationConn::connect(&client_config(addr.port(), SslMode::Prefer))
        .await
        .expect("client connect");
    client
        .send_query("START_REPLICATION PHYSICAL 0/0")
        .await
        .expect("send start");
    client
        .expect_copy_both_open()
        .await
        .expect("CopyBothResponse");

    let start_lsn = tokio::time::timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server timeout")
        .expect("server join");
    assert_eq!(start_lsn, 0);
}

/// `TIMELINE_HISTORY` row encoding on the server must parse via the real
/// client's `timeline_history`, yielding the `<tli>.history` filename + empty
/// body for a single-timeline source.
#[tokio::test]
async fn timeline_history_round_trips_through_client() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let identity = test_identity();
    let server_task = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        handshake_and_await_start(&mut sock, &identity)
            .await
            .expect("handshake");
    });

    let mut client = ReplicationConn::connect(&client_config(addr.port(), SslMode::Disable))
        .await
        .expect("client connect");
    let (name, content) = client
        .timeline_history(1)
        .await
        .expect("timeline_history")
        .expect("history row present");
    assert_eq!(name, "00000001.history");
    assert!(content.is_empty());

    client
        .send_query("START_REPLICATION PHYSICAL 0/0")
        .await
        .expect("send start");
    client
        .expect_copy_both_open()
        .await
        .expect("CopyBothResponse");
    tokio::time::timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server timeout")
        .expect("server join");
}

/// Malformed `StartupMessage`s surface as protocol errors from
/// `handshake_and_await_start` rather than hanging or panicking.
#[tokio::test]
async fn handshake_rejects_malformed_startup() {
    let identity = test_identity();

    // Unsupported protocol version (2.0)
    let (client, mut server) = tokio::io::duplex(256);
    let writer = tokio::spawn(async move {
        let mut client = client;
        let mut buf = Vec::new();
        buf.extend_from_slice(&16u32.to_be_bytes());
        buf.extend_from_slice(&0x0002_0000u32.to_be_bytes());
        buf.extend_from_slice(b"user\0u\0\0");
        let _ = client.write_all(&buf).await;
    });
    let err = handshake_and_await_start(&mut server, &identity)
        .await
        .unwrap_err();
    assert!(
        format!("{err}").contains("unsupported protocol version"),
        "{err}"
    );
    writer.await.unwrap();

    // Startup length too short (< 8)
    let (client, mut server) = tokio::io::duplex(64);
    let writer = tokio::spawn(async move {
        let mut client = client;
        let mut buf = Vec::new();
        buf.extend_from_slice(&4u32.to_be_bytes());
        buf.extend_from_slice(&196608u32.to_be_bytes());
        let _ = client.write_all(&buf).await;
    });
    let err = handshake_and_await_start(&mut server, &identity)
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("too short"), "{err}");
    writer.await.unwrap();
}

/// `WalSenderConn::try_recv_frame` rejects a CopyData envelope whose declared
/// length is below the 4-byte minimum.
#[tokio::test]
async fn walsender_conn_rejects_malformed_copy_data() {
    let (client, server) = tokio::io::duplex(64);
    let writer = tokio::spawn(async move {
        let mut client = client;
        let _ = client.write_all(&[b'd', 0, 0, 0, 3]).await;
    });
    let mut conn = WalSenderConn::new(server);
    let err = conn.try_recv_frame().await.unwrap_err();
    assert!(format!("{err}").contains("too short"), "{err}");
    writer.await.unwrap();
}
