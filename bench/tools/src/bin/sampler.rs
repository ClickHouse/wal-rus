//! bench-sampler, 1 Hz on-SUT resource sampler
//!
//! Writes one CSV per metric family, sharing `ts`:
//!   mem.csv:     ts,vmpeak_kb,vmsize_kb,vmhwm_kb,vmrss_kb,rssanon_kb,cg_current_bytes,cg_peak_bytes
//!   cpu.csv:     ts,pct_usr,pct_sys,pct_cpu
//!   wal.csv:     ts,wal_bytes
//!   archive.csv: ts,archived_count,failed_count,ready_backlog,last_archived_age_s
//!   net.csv:     ts,tx_bytes,rx_bytes
//!
//! Reads /proc, /sys, and PostgreSQL via persistent psql
//!
//! Self-test (no PostgreSQL):
//!   sleep 30 & bench-sampler --pid $! --iface lo --no-pg \
//!       --outdir /tmp/samp --duration 3 --interval 1.0

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::Parser;

const SENTINEL: &str = "__SAMPLER_EOT__";

// Per-tick PG queries, framed by SENTINEL
const PG_QUERIES: [&str; 2] = [
    "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0');",
    "SELECT archived_count, failed_count, \
     COALESCE(EXTRACT(EPOCH FROM (now() - last_archived_time)), -1) \
     FROM pg_stat_archiver;",
];

#[derive(Parser)]
#[command(about = "1 Hz on-SUT resource sampler")]
struct Args {
    /// PID to sample
    #[arg(long)]
    pid: Option<i32>,
    /// systemd unit
    #[arg(long)]
    unit: Option<String>,
    /// allow blank mem/cpu when no PID resolves
    #[arg(long = "no-pid-required")]
    no_pid_required: bool,
    /// aggregate mem/cpu over processes with matching comm
    #[arg(long = "proc-match")]
    proc_match: Option<String>,
    /// archiver shorthand
    #[arg(long, value_parser = ["walg", "walrus", "pgbackrest"])]
    daemon: Option<String>,
    /// cgroup v2 dir
    #[arg(long)]
    cgroup: Option<String>,
    /// network iface
    #[arg(long)]
    iface: Option<String>,
    /// PGDATA path
    #[arg(long, default_value = "/dat/18/data")]
    pgdata: String,
    /// directory for CSV outputs
    #[arg(long)]
    outdir: String,
    /// sample interval seconds
    #[arg(long, default_value_t = 1.0)]
    interval: f64,
    /// number of ticks
    #[arg(long)]
    duration: Option<u64>,
    /// psql conninfo
    #[arg(
        long,
        default_value = "host=/var/run/postgresql user=postgres dbname=walbench"
    )]
    pg: String,
    /// skip WAL/archive queries
    #[arg(long = "no-pg")]
    no_pg: bool,
}

fn clk_tck() -> f64 {
    let v = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if v > 0 { v as f64 } else { 100.0 }
}

// Diagnostic events go to stderr; drivers redirect it to sampler.log

// --------------------------------------------------------------------------
// CSV sink
// --------------------------------------------------------------------------
struct CsvSink {
    fh: BufWriter<File>,
}

impl CsvSink {
    fn create(dir: &Path, name: &str, header: &[&str]) -> Result<Self> {
        let path = dir.join(name);
        let mut fh =
            BufWriter::new(File::create(&path).with_context(|| format!("create {path:?}"))?);
        writeln!(fh, "{}", header.join(","))?;
        fh.flush()?;
        Ok(Self { fh })
    }
    fn row(&mut self, cells: &[String]) {
        let _ = writeln!(self.fh, "{}", cells.join(","));
        let _ = self.fh.flush();
    }
}

fn ts_cell(ts: f64) -> String {
    format!("{ts:.3}")
}
fn opt_u64(v: Option<u64>) -> String {
    v.map_or(String::new(), |x| x.to_string())
}

// --------------------------------------------------------------------------
// Resolution helpers
// --------------------------------------------------------------------------
fn systemctl_value(unit: &str, prop: &str) -> Option<String> {
    let out = Command::new("systemctl")
        .args(["show", "-p", prop, "--value", unit])
        .output()
        .ok()?;
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if s.is_empty() { None } else { Some(s) }
}

fn resolve_main_pid(unit: &str) -> Option<i32> {
    let s = systemctl_value(unit, "MainPID")?;
    s.parse::<i32>().ok().filter(|&p| p > 0)
}

fn resolve_cgroup_path(unit: &str) -> Option<String> {
    let rel = systemctl_value(unit, "ControlGroup")?;
    let path = format!("/sys/fs/cgroup/{}", rel.trim_start_matches('/'));
    Path::new(&path).is_dir().then_some(path)
}

/// Map archiver token to sampling target
fn daemon_target(daemon: &str) -> (Option<String>, Option<String>) {
    match daemon {
        "walg" => (Some("wal-g.service".into()), None),
        "walrus" => (Some("walrus.service".into()), None),
        "pgbackrest" => (None, Some("pgbackrest".into())),
        _ => (None, None),
    }
}

fn list_pids_by_comm(name: &str) -> Vec<i32> {
    let mut pids = Vec::new();
    let Ok(entries) = fs::read_dir("/proc") else {
        return pids;
    };
    for e in entries.flatten() {
        let fname = e.file_name();
        let Some(s) = fname.to_str() else { continue };
        let Ok(pid) = s.parse::<i32>() else { continue };
        if let Ok(comm) = fs::read_to_string(format!("/proc/{pid}/comm"))
            && comm.trim() == name
        {
            pids.push(pid);
        }
    }
    pids
}

fn detect_default_iface() -> Option<String> {
    // /proc/net/route uses 00000000 for default destination
    let data = fs::read_to_string("/proc/net/route").ok()?;
    for line in data.lines().skip(1) {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() >= 2 && f[1] == "00000000" {
            return Some(f[0].to_string());
        }
    }
    None
}

// --------------------------------------------------------------------------
// /proc readers
// --------------------------------------------------------------------------
/// /proc/<pid>/status memory fields, kB
#[derive(Default, Clone, Copy)]
struct MemStats {
    vmpeak: Option<u64>,
    vmsize: Option<u64>,
    vmhwm: Option<u64>,
    vmrss: Option<u64>,
    rssanon: Option<u64>,
}

impl MemStats {
    const ZERO: MemStats = MemStats {
        vmpeak: Some(0),
        vmsize: Some(0),
        vmhwm: Some(0),
        vmrss: Some(0),
        rssanon: Some(0),
    };

    fn slot(&mut self, key: &str) -> Option<&mut Option<u64>> {
        Some(match key {
            "VmPeak" => &mut self.vmpeak,
            "VmSize" => &mut self.vmsize,
            "VmHWM" => &mut self.vmhwm,
            "VmRSS" => &mut self.vmrss,
            "RssAnon" => &mut self.rssanon,
            _ => return None,
        })
    }

    /// Field-wise sum, missing treated as 0
    fn add(&mut self, o: MemStats) {
        self.vmpeak = Some(self.vmpeak.unwrap_or(0) + o.vmpeak.unwrap_or(0));
        self.vmsize = Some(self.vmsize.unwrap_or(0) + o.vmsize.unwrap_or(0));
        self.vmhwm = Some(self.vmhwm.unwrap_or(0) + o.vmhwm.unwrap_or(0));
        self.vmrss = Some(self.vmrss.unwrap_or(0) + o.vmrss.unwrap_or(0));
        self.rssanon = Some(self.rssanon.unwrap_or(0) + o.rssanon.unwrap_or(0));
    }
}

fn read_proc_status(pid: i32) -> MemStats {
    let mut out = MemStats::default();
    let Ok(data) = fs::read_to_string(format!("/proc/{pid}/status")) else {
        return out;
    };
    for line in data.lines() {
        let Some((key, rest)) = line.split_once(':') else {
            continue;
        };
        if let Some(slot) = out.slot(key)
            && let Some(tok) = rest.split_whitespace().next()
            && let Ok(v) = tok.parse::<u64>()
        {
            *slot = Some(v);
        }
    }
    out
}

/// utime/stime ticks from /proc/<pid>/stat
fn read_proc_cpu_jiffies(pid: i32) -> Option<(u64, u64)> {
    let data = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let rparen = data.rfind(')')?;
    let rest: Vec<&str> = data[rparen + 1..].split_whitespace().collect();
    // utime field 14 -> index 11, stime field 15 -> index 12
    if rest.len() < 13 {
        return None;
    }
    Some((rest[11].parse().ok()?, rest[12].parse().ok()?))
}

fn read_int_file(path: &str) -> Option<u64> {
    let text = fs::read_to_string(path).ok()?;
    let text = text.trim();
    if text == "max" {
        None
    } else {
        text.parse().ok()
    }
}

// --------------------------------------------------------------------------
// Persistent psql connection
// --------------------------------------------------------------------------
#[derive(Default)]
struct PgRow {
    wal_bytes: Option<String>,
    archived_count: Option<String>,
    failed_count: Option<String>,
    last_archived_age_s: Option<String>,
}

struct PsqlConn {
    conninfo: String,
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    stdout: Option<BufReader<ChildStdout>>,
}

impl PsqlConn {
    fn new(conninfo: String) -> Self {
        let mut c = Self {
            conninfo,
            child: None,
            stdin: None,
            stdout: None,
        };
        c.spawn();
        c
    }

    fn spawn(&mut self) {
        // -A unaligned, -t tuples-only, -q quiet, -X no psqlrc
        let mut child = match Command::new("psql")
            .args([
                "-Atq",
                "-X",
                "-F",
                "|",
                "-v",
                "ON_ERROR_STOP=0",
                &self.conninfo,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(c) => c,
            Err(e) => {
                eprintln!("psql spawn failed: {e}");
                return;
            }
        };
        self.stdin = child.stdin.take();
        self.stdout = child.stdout.take().map(BufReader::new);
        eprintln!("psql spawned pid={}", child.id());
        self.child = Some(child);
    }

    fn alive(&mut self) -> bool {
        match self.child.as_mut() {
            Some(c) => matches!(c.try_wait(), Ok(None)),
            None => false,
        }
    }

    fn kill(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
        self.stdin = None;
        self.stdout = None;
    }

    fn reset_archiver(&mut self) {
        let _ = self.query_tick(); // ensure live first
        if !self.alive() {
            return;
        }
        if let (Some(stdin), Some(stdout)) = (self.stdin.as_mut(), self.stdout.as_mut()) {
            if stdin
                .write_all(b"SELECT pg_stat_reset_shared('archiver');\n")
                .is_err()
            {
                return;
            }
            let _ = stdin.flush();
            let mut line = String::new();
            let _ = stdout.read_line(&mut line);
            eprintln!("archiver stats reset");
        }
    }

    fn query_tick(&mut self) -> PgRow {
        if !self.alive() {
            eprintln!("psql not alive; respawning");
            self.spawn();
            if !self.alive() {
                return PgRow::default();
            }
        }
        let batch = format!("{}{}SELECT '{SENTINEL}';\n", PG_QUERIES[0], PG_QUERIES[1]);
        let Some(stdin) = self.stdin.as_mut() else {
            return PgRow::default();
        };
        if stdin.write_all(batch.as_bytes()).is_err() || stdin.flush().is_err() {
            eprintln!("psql write failed; respawning next tick");
            self.kill();
            return PgRow::default();
        }
        let Some(stdout) = self.stdout.as_mut() else {
            return PgRow::default();
        };
        let mut lines: Vec<String> = Vec::new();
        loop {
            let mut line = String::new();
            match stdout.read_line(&mut line) {
                Ok(0) => {
                    eprintln!("psql EOF mid-tick; respawning next tick");
                    self.kill();
                    return PgRow::default();
                }
                Ok(_) => {}
                Err(_) => {
                    self.kill();
                    return PgRow::default();
                }
            }
            let s = line.trim_end_matches('\n');
            if s == SENTINEL {
                break;
            }
            if s.is_empty() {
                continue;
            }
            lines.push(s.to_string());
        }
        parse_pg(&lines)
    }

    fn close(&mut self) {
        if self.alive()
            && let Some(stdin) = self.stdin.as_mut()
        {
            let _ = stdin.write_all(b"\\q\n");
            let _ = stdin.flush();
        }
        if let Some(c) = self.child.as_mut() {
            let _ = c.wait();
        }
    }
}

fn parse_pg(lines: &[String]) -> PgRow {
    let mut out = PgRow::default();
    if let Some(l) = lines.first() {
        out.wal_bytes = nonempty(l.trim());
    }
    if let Some(l) = lines.get(1) {
        let parts: Vec<&str> = l.split('|').collect();
        if parts.len() >= 3 {
            out.archived_count = nonempty(parts[0].trim());
            out.failed_count = nonempty(parts[1].trim());
            let age = parts[2].trim();
            // -1 sentinel means never archived
            out.last_archived_age_s = (age != "-1").then(|| nonempty(age)).flatten();
        }
    }
    out
}

fn nonempty(s: &str) -> Option<String> {
    (!s.is_empty()).then(|| s.to_string())
}

// --------------------------------------------------------------------------
// Sampler
// --------------------------------------------------------------------------
struct Sampler {
    proc_match: Option<String>,
    pid: Option<i32>,
    unit: Option<String>,
    cgroup: Option<String>,
    iface: Option<String>,
    pgdata: PathBuf,
    clk_tck: f64,
    mem: CsvSink,
    cpu: CsvSink,
    wal: CsvSink,
    archive: CsvSink,
    net: CsvSink,
    psql: Option<PsqlConn>,
    prev_cpu: Option<(u64, u64)>,
    prev_cpu_ts: Option<f64>,
    prev_cpu_map: HashMap<i32, u64>,
}

impl Sampler {
    fn new(args: &Args) -> Result<Self> {
        let outdir = Path::new(&args.outdir);
        fs::create_dir_all(outdir).with_context(|| format!("mkdir {outdir:?}"))?;

        // --daemon fills unit/proc_match unless explicit flags did
        let (mut unit, mut proc_match) = (args.unit.clone(), args.proc_match.clone());
        if let Some(d) = &args.daemon {
            let (u, pm) = daemon_target(d);
            unit = unit.or(u);
            proc_match = proc_match.or(pm);
        }
        let mut pid = args.pid;
        if proc_match.is_none()
            && pid.is_none()
            && let Some(u) = &unit
        {
            pid = resolve_main_pid(u);
            eprintln!("resolved MainPID={pid:?} for unit={u}");
        }

        let mut cgroup = args.cgroup.clone();
        if proc_match.is_none()
            && cgroup.is_none()
            && let Some(u) = &unit
        {
            cgroup = resolve_cgroup_path(u);
            eprintln!("resolved cgroup={cgroup:?} for unit={u}");
        }

        let iface = args.iface.clone().or_else(detect_default_iface);
        eprintln!("using iface={iface:?}");
        if let Some(pm) = &proc_match {
            eprintln!("proc-match mode: comm={pm:?}");
        }

        if proc_match.is_none() && pid.is_none() && !args.no_pid_required {
            eprintln!("FATAL: no PID resolved and --no-pid-required not set");
            std::process::exit(2);
        }

        let mut s = Self {
            proc_match,
            pid,
            unit,
            cgroup,
            iface,
            pgdata: PathBuf::from(&args.pgdata),
            clk_tck: clk_tck(),
            mem: CsvSink::create(
                outdir,
                "mem.csv",
                &[
                    "ts",
                    "vmpeak_kb",
                    "vmsize_kb",
                    "vmhwm_kb",
                    "vmrss_kb",
                    "rssanon_kb",
                    "cg_current_bytes",
                    "cg_peak_bytes",
                ],
            )?,
            cpu: CsvSink::create(outdir, "cpu.csv", &["ts", "pct_usr", "pct_sys", "pct_cpu"])?,
            wal: CsvSink::create(outdir, "wal.csv", &["ts", "wal_bytes"])?,
            archive: CsvSink::create(
                outdir,
                "archive.csv",
                &[
                    "ts",
                    "archived_count",
                    "failed_count",
                    "ready_backlog",
                    "last_archived_age_s",
                ],
            )?,
            net: CsvSink::create(outdir, "net.csv", &["ts", "tx_bytes", "rx_bytes"])?,
            psql: None,
            prev_cpu: None,
            prev_cpu_ts: None,
            prev_cpu_map: HashMap::new(),
        };
        if !args.no_pg {
            let mut conn = PsqlConn::new(args.pg.clone());
            conn.reset_archiver();
            s.psql = Some(conn);
        }
        Ok(s)
    }

    fn refresh_pid_if_needed(&mut self) {
        if self.proc_match.is_some() {
            return;
        }
        let Some(unit) = self.unit.clone() else {
            return;
        };
        let cur = resolve_main_pid(&unit);
        if cur != self.pid {
            eprintln!("PID change for unit={unit}: {:?} -> {cur:?}", self.pid);
            self.pid = cur;
            self.prev_cpu = None;
            self.prev_cpu_ts = None;
            if self.cgroup.is_none() {
                self.cgroup = resolve_cgroup_path(&unit);
            }
        }
    }

    fn sample_mem(&mut self, ts: f64) {
        let vals = if let Some(pm) = &self.proc_match {
            // Sum live process tree; run-level peak is max-over-time downstream
            let mut agg = MemStats::default();
            let mut found = false;
            for pid in list_pids_by_comm(pm) {
                agg.add(read_proc_status(pid));
                found = true;
            }
            // No matches means async worker drained + exited
            if found { agg } else { MemStats::ZERO }
        } else if let Some(pid) = self.pid {
            read_proc_status(pid)
        } else {
            MemStats::default()
        };
        let cg_cur = self
            .cgroup
            .as_ref()
            .and_then(|c| read_int_file(&format!("{c}/memory.current")));
        let cg_peak = self
            .cgroup
            .as_ref()
            .and_then(|c| read_int_file(&format!("{c}/memory.peak")));
        self.mem.row(&[
            ts_cell(ts),
            opt_u64(vals.vmpeak),
            opt_u64(vals.vmsize),
            opt_u64(vals.vmhwm),
            opt_u64(vals.vmrss),
            opt_u64(vals.rssanon),
            opt_u64(cg_cur),
            opt_u64(cg_peak),
        ]);
    }

    fn sample_cpu(&mut self, ts: f64) {
        if self.proc_match.is_some() {
            self.sample_cpu_proctree(ts);
            return;
        }
        let (mut usr, mut sys, mut cpu) = (String::new(), String::new(), String::new());
        let cur = self.pid.and_then(read_proc_cpu_jiffies);
        if let (Some(cur), Some(prev), Some(pts)) = (cur, self.prev_cpu, self.prev_cpu_ts) {
            let elapsed = ts - pts;
            if elapsed > 0.0 {
                let du = cur.0.saturating_sub(prev.0) as f64;
                let ds = cur.1.saturating_sub(prev.1) as f64;
                // CPU percent may exceed 100 on multi-threaded daemons
                let u = 100.0 * (du / self.clk_tck) / elapsed;
                let s = 100.0 * (ds / self.clk_tck) / elapsed;
                usr = format!("{:.2}", u.max(0.0));
                sys = format!("{:.2}", s.max(0.0));
                cpu = format!("{:.2}", (u + s).max(0.0));
            }
        }
        if let Some(cur) = cur {
            self.prev_cpu = Some(cur);
            self.prev_cpu_ts = Some(ts);
        }
        self.cpu.row(&[ts_cell(ts), usr, sys, cpu]);
    }

    fn sample_cpu_proctree(&mut self, ts: f64) {
        // Diff per-PID ticks across churning process set
        let pm = self.proc_match.clone().unwrap();
        let mut cur_map: HashMap<i32, u64> = HashMap::new();
        for pid in list_pids_by_comm(&pm) {
            if let Some((u, s)) = read_proc_cpu_jiffies(pid) {
                cur_map.insert(pid, u + s);
            }
        }
        let mut cpu = String::new();
        if let Some(pts) = self.prev_cpu_ts {
            let elapsed = ts - pts;
            if elapsed > 0.0 {
                let d_ticks: u64 = cur_map
                    .iter()
                    .map(|(pid, &total)| {
                        total.saturating_sub(self.prev_cpu_map.get(pid).copied().unwrap_or(0))
                    })
                    .sum();
                cpu = format!(
                    "{:.2}",
                    (100.0 * (d_ticks as f64 / self.clk_tck) / elapsed).max(0.0)
                );
            }
        }
        self.prev_cpu_map = cur_map;
        self.prev_cpu_ts = Some(ts);
        self.cpu
            .row(&[ts_cell(ts), String::new(), String::new(), cpu]);
    }

    fn sample_net(&mut self, ts: f64) {
        let (mut tx, mut rx) = (String::new(), String::new());
        if let Some(iface) = &self.iface {
            let base = format!("/sys/class/net/{iface}/statistics");
            tx = opt_u64(read_int_file(&format!("{base}/tx_bytes")));
            rx = opt_u64(read_int_file(&format!("{base}/rx_bytes")));
        }
        self.net.row(&[ts_cell(ts), tx, rx]);
    }

    fn sample_pg(&mut self, ts: f64) {
        let Some(psql) = self.psql.as_mut() else {
            self.wal.row(&[ts_cell(ts), String::new()]);
            self.archive.row(&[
                ts_cell(ts),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
            ]);
            return;
        };
        let res = psql.query_tick();
        let ready = self.count_ready();
        self.wal
            .row(&[ts_cell(ts), res.wal_bytes.unwrap_or_default()]);
        self.archive.row(&[
            ts_cell(ts),
            res.archived_count.unwrap_or_default(),
            res.failed_count.unwrap_or_default(),
            opt_u64(ready),
            res.last_archived_age_s.unwrap_or_default(),
        ]);
    }

    fn count_ready(&self) -> Option<u64> {
        let dir = self.pgdata.join("pg_wal").join("archive_status");
        let entries = fs::read_dir(dir).ok()?;
        Some(
            entries
                .flatten()
                .filter(|e| e.file_name().to_string_lossy().ends_with(".ready"))
                .count() as u64,
        )
    }

    fn tick(&mut self) {
        self.refresh_pid_if_needed();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0.0, |d| d.as_secs_f64());
        self.sample_mem(ts);
        self.sample_cpu(ts);
        self.sample_net(ts);
        self.sample_pg(ts);
    }

    fn close(&mut self) {
        if let Some(psql) = self.psql.as_mut() {
            psql.close();
        }
        eprintln!("sampler stopped");
    }
}

fn run(args: &Args, sampler: &mut Sampler, stop: &Arc<AtomicBool>) {
    let interval = Duration::from_secs_f64(args.interval.max(0.0));
    let mut ticks = 0u64;
    let mut next_at = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        sampler.tick();
        ticks += 1;
        if args.duration.is_some_and(|d| ticks >= d) {
            eprintln!("duration reached ({ticks} ticks); stopping");
            break;
        }
        next_at += interval;
        let now = Instant::now();
        if next_at <= now {
            next_at = now; // resync, no burst catch-up
        } else {
            // Keep SIGTERM responsive
            while !stop.load(Ordering::Relaxed) && Instant::now() < next_at {
                let remaining = next_at.saturating_duration_since(Instant::now());
                std::thread::sleep(remaining.min(Duration::from_millis(100)));
            }
        }
    }
    sampler.close();
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut sampler = Sampler::new(&args)?;
    let stop = Arc::new(AtomicBool::new(false));
    for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        signal_hook::flag::register(sig, Arc::clone(&stop))?;
    }
    run(&args, &mut sampler, &stop);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pg_full() {
        let r = parse_pg(&["123456".into(), "42|0|3.5".into()]);
        assert_eq!(r.wal_bytes.as_deref(), Some("123456"));
        assert_eq!(r.archived_count.as_deref(), Some("42"));
        assert_eq!(r.failed_count.as_deref(), Some("0"));
        assert_eq!(r.last_archived_age_s.as_deref(), Some("3.5"));
    }

    #[test]
    fn parse_pg_never_archived() {
        // -1 age sentinel becomes blank
        let r = parse_pg(&["10".into(), "0|0|-1".into()]);
        assert_eq!(r.last_archived_age_s, None);
    }

    #[test]
    fn parse_pg_empty() {
        let r = parse_pg(&[]);
        assert!(r.wal_bytes.is_none() && r.archived_count.is_none());
    }

    #[test]
    fn daemon_target_maps() {
        assert_eq!(daemon_target("walg"), (Some("wal-g.service".into()), None));
        assert_eq!(
            daemon_target("walrus"),
            (Some("walrus.service".into()), None)
        );
        assert_eq!(
            daemon_target("pgbackrest"),
            (None, Some("pgbackrest".into()))
        );
        assert_eq!(daemon_target("nope"), (None, None));
    }
}
