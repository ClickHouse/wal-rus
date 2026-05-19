//! Round-trip test: wire wal-rs's walsender server (PHASE13 §2) to
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
use postgres_protocol::message::backend::Message;
use tokio::net::TcpListener;
use wal_rs::pg::replication::conn::{PgConfig, ReplicationConn};
use wal_rs::pg::replication::server::{
    Identity, WalSenderConn, decode_standby_status, handshake_and_await_start,
};
use wal_rs::pg::replication::stream::{
    Frame, build_status_update, decode_frame, encode_keepalive_frame, encode_wal_data_frame,
};
use wal_rs::pg::replication::tls::SslMode;

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
