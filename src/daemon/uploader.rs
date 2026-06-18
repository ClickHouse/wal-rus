//! Standing background WAL uploader for the daemon archive path.
//!
//! PG's archiver is serial — it runs `archive_command` for one segment, waits
//! for success, then the next — so wal-rs's per-connection `wal-push` is
//! serial too and `WALG_UPLOAD_CONCURRENCY` is a no-op here: the archiver
//! falls behind a high WAL rate. wal-g closes the gap with a per-invocation
//! `BgUploader` (wal-g `internal/databases/postgres/bguploader.go`) that scans
//! `archive_status/` and uploads look-ahead segments concurrently.
//!
//! Because the wal-rs daemon is one long-lived process, bookkeeping stays
//! in-memory: a shared `inflight` map dedups foreground pushes against
//! background look-ahead, replacing wal-g's on-disk `ArchiveStatusManager`
//! marker directory. Look-ahead uploads run in detached driver tasks, so they
//! survive a client disconnect and carry across `archive_command` invocations
//! (no per-call teardown) — the pool stays saturated while a backlog exists.
//!
//! Contract: `wal_push(N)` returns `Ok` only once `N` is durably uploaded
//! (matches wal-g). Look-ahead is a latency optimization — it pre-uploads
//! `N+1..` so their `archive_command` returns instantly — never an early ack.
//! `push::handle` renames `.ready`→`.done` on success, so PG's archiver skips
//! the segments look-ahead already promoted (`pgarch_readyXlog` re-stats
//! `.ready` and skips on `ENOENT`).
//!
//! `lookahead = upload_concurrency - 1`; at `WALG_UPLOAD_CONCURRENCY=1` no
//! look-ahead is issued and behavior is byte-identical to the serial path.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Result, anyhow};
use futures::future::{BoxFuture, FutureExt, Shared};
use tokio::sync::Semaphore;

use crate::config::Settings;
use crate::pg::wal::push;
use crate::pg::wal::segment::SegmentName;
use crate::storage::DynStorage;

/// Upper bound on `.ready` names parsed per look-ahead scan, so a pathological
/// backlog can't build an unbounded candidate list
const MAX_SCAN_CANDIDATES: usize = 4096;

/// Shared, cloneable outcome of one in-flight upload. `anyhow::Error` isn't
/// `Clone`, so failures collapse to a shared string across awaiters
type UploadFuture = Shared<BoxFuture<'static, Result<(), Arc<str>>>>;

struct State {
    /// Segments currently uploading (foreground or look-ahead). Keeps a second
    /// scan or a racing foreground push from starting a duplicate upload
    inflight: HashMap<SegmentName, UploadFuture>,
    /// Recently-completed segments. Absorbs the narrow race where PG invokes
    /// `archive_command` for a segment look-ahead already uploaded+promoted
    /// (returns Ok without reopening a possibly-recycled file). Bounded
    done: HashSet<SegmentName>,
    done_order: VecDeque<SegmentName>,
}

impl State {
    fn mark_done(&mut self, seg: SegmentName, cap: usize) {
        if self.done.insert(seg) {
            self.done_order.push_back(seg);
            while self.done_order.len() > cap {
                if let Some(old) = self.done_order.pop_front() {
                    self.done.remove(&old);
                }
            }
        }
    }
}

pub struct Uploader {
    settings: Arc<Settings>,
    storage: DynStorage,
    /// Bounds total in-flight uploads (foreground + look-ahead)
    sem: Arc<Semaphore>,
    /// Segments past the foreground one to pre-upload; `concurrency - 1`
    lookahead: usize,
    done_cap: usize,
    state: Mutex<State>,
}

impl Uploader {
    pub fn new(settings: Arc<Settings>, storage: DynStorage) -> Self {
        let concurrency = settings.upload_concurrency.max(1);
        Uploader {
            settings,
            storage,
            sem: Arc::new(Semaphore::new(concurrency)),
            lookahead: concurrency - 1,
            done_cap: (concurrency * 4).max(16),
            state: Mutex::new(State {
                inflight: HashMap::new(),
                done: HashSet::new(),
                done_order: VecDeque::new(),
            }),
        }
    }

    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    pub fn storage(&self) -> DynStorage {
        self.storage.clone()
    }

    /// Archive one segment. Issues look-ahead for adjacent `.ready` segments,
    /// then awaits this segment's durable upload before returning
    pub async fn wal_push(self: &Arc<Self>, path: &Path) -> Result<()> {
        let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        // History/partial/other non-segment files bypass the look-ahead +
        // dedup machinery — push straight through (push::handle handles them)
        let Ok(seg) = SegmentName::parse(name) else {
            return push::handle(&self.settings, self.storage.clone(), path).await;
        };
        if self.lookahead > 0
            && let Some(parent) = path.parent()
        {
            self.kick_lookahead(parent, seg).await;
        }
        self.ensure_uploaded(seg, path).await
    }

    /// Foreground path: start (or join) this segment's upload and await it
    async fn ensure_uploaded(self: &Arc<Self>, seg: SegmentName, path: &Path) -> Result<()> {
        match self.get_or_start(seg, path.to_path_buf()) {
            None => Ok(()),
            Some(fut) => fut
                .await
                .map_err(|e| anyhow!("archive {}: {e}", seg.format())),
        }
    }

    /// Scan `archive_status/` for the lowest look-ahead segments past `after`
    /// and spawn detached uploads for those not already in flight or done
    async fn kick_lookahead(self: &Arc<Self>, parent: &Path, after: SegmentName) {
        let candidates = scan_ready(parent, after).await;
        let mut to_spawn = Vec::with_capacity(self.lookahead);
        {
            let mut st = self.state.lock().unwrap();
            // Cap total in-flight (foreground + look-ahead) at upload
            // concurrency so a deep backlog can't accumulate look-ahead across
            // invocations. Foreground claims one slot — already in `inflight`
            // from earlier look-ahead, or added next by `ensure_uploaded` — so
            // reserve for it when absent
            let cap = self.lookahead + 1;
            let reserve = usize::from(!st.inflight.contains_key(&after));
            for seg in candidates {
                if st.inflight.len() + reserve >= cap {
                    break;
                }
                if st.done.contains(&seg) || st.inflight.contains_key(&seg) {
                    continue;
                }
                let fut = self.make_upload(parent.join(seg.format()));
                st.inflight.insert(seg, fut.clone());
                to_spawn.push((seg, fut));
            }
        }
        for (seg, fut) in to_spawn {
            self.spawn_driver(seg, fut);
        }
    }

    /// Returns `None` if the segment is already uploaded, else a shared handle
    /// to its (possibly newly started) upload. Reserving under the lock dedups
    /// concurrent foreground/look-ahead starts of the same segment
    fn get_or_start(self: &Arc<Self>, seg: SegmentName, path: PathBuf) -> Option<UploadFuture> {
        let mut st = self.state.lock().unwrap();
        if st.done.contains(&seg) {
            return None;
        }
        if let Some(fut) = st.inflight.get(&seg) {
            return Some(fut.clone());
        }
        let fut = self.make_upload(path);
        st.inflight.insert(seg, fut.clone());
        drop(st);
        self.spawn_driver(seg, fut.clone());
        Some(fut)
    }

    fn make_upload(&self, path: PathBuf) -> UploadFuture {
        let settings = self.settings.clone();
        let storage = self.storage.clone();
        let sem = self.sem.clone();
        async move {
            // Permit held for the upload duration bounds total concurrency
            // regardless of how many uploads are spawned
            let _permit = sem
                .acquire_owned()
                .await
                .map_err(|e| Arc::<str>::from(e.to_string()))?;
            push::handle(&settings, storage, &path)
                .await
                .map_err(|e| Arc::<str>::from(format!("{e:#}")))
        }
        .boxed()
        .shared()
    }

    /// Drive an upload to completion independent of any foreground awaiter, so
    /// it finishes even if the client disconnects, and do the bookkeeping
    /// exactly once. On failure the segment is left out of `done` and its
    /// `.ready` marker survives (push::handle only promotes on success), so the
    /// next scan or PG invocation retries it
    fn spawn_driver(self: &Arc<Self>, seg: SegmentName, fut: UploadFuture) {
        let me = self.clone();
        tokio::spawn(async move {
            let res = fut.await;
            let mut st = me.state.lock().unwrap();
            st.inflight.remove(&seg);
            if res.is_ok() {
                st.mark_done(seg, me.done_cap);
            }
        });
    }

    #[cfg(test)]
    async fn drain(&self) {
        loop {
            let futs: Vec<UploadFuture> = {
                self.state
                    .lock()
                    .unwrap()
                    .inflight
                    .values()
                    .cloned()
                    .collect()
            };
            if futs.is_empty() {
                break;
            }
            for f in futs {
                let _ = f.await;
            }
            tokio::task::yield_now().await;
        }
    }
}

/// Read `<parent>/archive_status/` and return segment names with a `.ready`
/// marker strictly greater than `after`, sorted ascending. Missing dir → empty
async fn scan_ready(parent: &Path, after: SegmentName) -> Vec<SegmentName> {
    let status_dir = parent.join("archive_status");
    let Ok(mut rd) = tokio::fs::read_dir(&status_dir).await else {
        return Vec::new();
    };
    let mut cands = Vec::new();
    while let Ok(Some(ent)) = rd.next_entry().await {
        if cands.len() >= MAX_SCAN_CANDIDATES {
            break;
        }
        let fname = ent.file_name();
        let Some(name) = fname.to_str() else { continue };
        let Some(base) = name.strip_suffix(".ready") else {
            continue;
        };
        if let Ok(seg) = SegmentName::parse(base)
            && seg > after
        {
            cands.push(seg);
        }
    }
    cands.sort_unstable();
    cands
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_settings(store: &Path, concurrency: usize) -> Settings {
        Settings {
            storage: crate::config::StorageSettings::Fs {
                path: store.to_string_lossy().into(),
            },
            compression: crate::compression::Method::None,
            compression_level: 3,
            upload_concurrency: concurrency,
            upload_queue: 1,
            download_concurrency: 1,
            prevent_wal_overwrite: false,
            retry: crate::retry::RetryPolicy::default(),
            network_rate_limit: 0,
            disk_rate_limit: 0,
            delta: Default::default(),
            crypter: None,
        }
    }

    /// `<dir>` is the pg_wal dir; lays down `count` full segments + `.ready`
    /// markers starting at timeline 1, log 0, seg 1
    fn seed_segments(dir: &Path, count: u32) -> Vec<SegmentName> {
        let status = dir.join("archive_status");
        std::fs::create_dir_all(&status).unwrap();
        let mut segs = Vec::new();
        let mut seg = SegmentName {
            timeline: 1,
            log_id: 0,
            seg_no: 1,
        };
        for i in 0..count {
            let name = seg.format();
            std::fs::write(dir.join(&name), vec![i as u8; 32]).unwrap();
            std::fs::write(status.join(format!("{name}.ready")), b"").unwrap();
            segs.push(seg);
            seg = seg.next(crate::pg::wal::segment::DEFAULT_WAL_SEG_SIZE);
        }
        segs
    }

    fn uploader(dir: &Path, concurrency: usize) -> Arc<Uploader> {
        let store = dir.join("store");
        let storage: DynStorage = Arc::new(crate::storage::fs::FsStorage::new(&store).unwrap());
        Arc::new(Uploader::new(
            Arc::new(test_settings(&store, concurrency)),
            storage,
        ))
    }

    fn archived(dir: &Path, seg: &SegmentName) -> bool {
        dir.join("store")
            .join(crate::pg::WAL_FOLDER)
            .join(seg.format())
            .exists()
    }

    fn promoted(dir: &Path, seg: &SegmentName) -> bool {
        let status = dir.join("archive_status");
        !status.join(format!("{}.ready", seg.format())).exists()
            && status.join(format!("{}.done", seg.format())).exists()
    }

    #[tokio::test]
    async fn lookahead_uploads_and_promotes_adjacent_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let segs = seed_segments(dir, 4);
        let up = uploader(dir, 4); // lookahead = 3 → all four

        up.wal_push(&dir.join(segs[0].format())).await.unwrap();
        up.drain().await;

        for seg in &segs {
            assert!(archived(dir, seg), "{} not archived", seg.format());
            assert!(promoted(dir, seg), "{} not promoted", seg.format());
        }
    }

    #[tokio::test]
    async fn concurrency_one_is_serial_no_lookahead() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let segs = seed_segments(dir, 3);
        let up = uploader(dir, 1); // lookahead = 0

        up.wal_push(&dir.join(segs[0].format())).await.unwrap();
        up.drain().await;

        assert!(archived(dir, &segs[0]));
        assert!(promoted(dir, &segs[0]));
        // look-ahead disabled: successors untouched
        assert!(!archived(dir, &segs[1]));
        assert!(!archived(dir, &segs[2]));
    }

    #[tokio::test]
    async fn lookahead_bounded_by_concurrency() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let segs = seed_segments(dir, 6);
        let up = uploader(dir, 3); // foreground + lookahead 2 → segs 0,1,2

        up.wal_push(&dir.join(segs[0].format())).await.unwrap();
        up.drain().await;

        for seg in &segs[..3] {
            assert!(archived(dir, seg), "{} not archived", seg.format());
        }
        for seg in &segs[3..] {
            assert!(!archived(dir, seg), "{} over-fetched", seg.format());
        }
    }

    #[tokio::test]
    async fn already_done_short_circuits() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let segs = seed_segments(dir, 1);
        let up = uploader(dir, 1);
        // Pretend seg already uploaded by a prior look-ahead
        up.state.lock().unwrap().mark_done(segs[0], 16);

        up.wal_push(&dir.join(segs[0].format())).await.unwrap();
        up.drain().await;

        // done fast-path: not re-uploaded
        assert!(!archived(dir, &segs[0]));
    }

    #[tokio::test]
    async fn lookahead_capped_by_cumulative_inflight() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let segs = seed_segments(dir, 8);
        let up = uploader(dir, 3); // cap = 3

        // Two look-ahead uploads from a prior invocation still in flight:
        // inserted without a driver so they never drain (FsStorage would
        // otherwise complete them instantly and hide the accumulation)
        {
            let mut st = up.state.lock().unwrap();
            for seg in &segs[1..3] {
                let fut = up.make_upload(dir.join(seg.format()));
                st.inflight.insert(*seg, fut);
            }
        }

        // Next invocation past segs[0]: two slots are taken and one is reserved
        // for the foreground, so the cap blocks any fresh look-ahead spawn
        up.kick_lookahead(dir, segs[0]).await;
        assert_eq!(up.state.lock().unwrap().inflight.len(), 2);
    }
}
