//! bench-analyze — plot + summarize walrus vs wal-g vs pgbackrest runs
//! (Rust port of plot.py; tiny-skia rasterizes the canvas, ab_glyph draws text,
//! tiny-skia's png-format writes the PNG).
//!
//! Reads the 1 Hz sampler CSVs for one or more run dirs and emits, into --out:
//!   mem_over_time.png  two panels: VmRSS (top), VmPeak (bottom)
//!   backlog.png        *.ready backlog over time (archive keep-up)
//!   upload_rate.png    tx_bytes upload rate (MB/s)
//!   cpu.png            daemon CPU % over time
//! Replicas of a variant are aggregated: bold = median, band = min..max.
//!
//! Plus self-describing raw exports (every row carries run metadata):
//!   samples_<stamp>.csv  long table, one row per sample per run
//!   summary_<stamp>.csv  one row per run: metadata + aggregates
//!   summary.json         same per-run aggregates as JSON

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use ab_glyph::{Font, FontRef, PxScale, ScaleFont, point};
use anyhow::{Context, Result};
use clap::Parser;
use tiny_skia::{
    Color, FillRule, LineCap, LineJoin, Paint, Path as SkPath, PathBuilder, Pixmap, Rect, Stroke,
    StrokeDash, Transform,
};

const KB: f64 = 1024.0;
const MB: f64 = 1024.0 * 1024.0;
const WAL_SEG_MB: f64 = 16.0; // archived_count -> MB approximation

const META_KEYS: [&str; 7] = [
    "daemon",
    "gomemlimit",
    "scale",
    "churn_rows",
    "burst_seconds",
    "upload_concurrency",
    "captured_at",
];

const SAMPLE_COLS: [&str; 17] = [
    "run_label",
    "variant",
    "box",
    "ts",
    "t",
    "t_int",
    "vmrss_mb",
    "vmpeak_mb",
    "vmsize_mb",
    "rssanon_mb",
    "cpu_pct",
    "archived_count",
    "failed_count",
    "ready_backlog",
    "wal_gen_mb",
    "tx_mb_s",
    "archived_mb",
];

#[derive(Clone, Copy)]
struct Rgb(u8, u8, u8);

const BG: Rgb = Rgb(0x29, 0x25, 0x22);
const FLOAT: Rgb = Rgb(0x34, 0x30, 0x2C);
const SEL: Rgb = Rgb(0x40, 0x3A, 0x36);
const MUTED: Rgb = Rgb(0xC1, 0xA7, 0x8E);
const FG: Rgb = Rgb(0xEC, 0xE1, 0xD7);

fn variant_color(variant: &str) -> Option<Rgb> {
    match variant {
        "walrus" | "walrus-serial" => Some(Rgb(0xFA, 0xFF, 0x69)),
        "walg" => Some(Rgb(0xFC, 0x3F, 0x1D)),
        "pgbackrest" => Some(Rgb(0x27, 0x68, 0x9D)),
        _ => None,
    }
}

const FALLBACK: [Rgb; 6] = [
    Rgb(0xA3, 0xA9, 0xCE),
    Rgb(0x85, 0xB6, 0x95),
    Rgb(0xCF, 0x9B, 0xC2),
    Rgb(0x89, 0xB3, 0xB6),
    Rgb(0xE4, 0x9B, 0x5D),
    Rgb(0xB3, 0x80, 0xB0),
];

#[derive(Parser)]
#[command(about = "plot + summarize bench runs")]
struct Args {
    /// run directory (repeatable; pair with --label in order)
    #[arg(long = "run", required = true)]
    runs: Vec<String>,
    /// label for the matching --run (repeatable)
    #[arg(long = "label", required = true)]
    labels: Vec<String>,
    /// output directory for plots + exports
    #[arg(long)]
    out: String,
    /// timestamp tag for output filenames (default: now, UTC)
    #[arg(long)]
    stamp: Option<String>,
}

// --------------------------------------------------------------------------
// Parsing helpers
// --------------------------------------------------------------------------
fn pf(s: Option<&String>) -> Option<f64> {
    let t = s?.trim();
    if t.is_empty() { None } else { t.parse().ok() }
}

fn variant_of(label: &str) -> String {
    // strip a trailing -b<digits>
    if let Some(idx) = label.rfind("-b") {
        let suffix = &label[idx + 2..];
        if !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) {
            return label[..idx].to_string();
        }
    }
    label.to_string()
}

fn box_of(label: &str) -> String {
    if let Some(idx) = label.rfind("-b") {
        let suffix = &label[idx + 2..];
        if !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) {
            return suffix.to_string();
        }
    }
    "0".to_string()
}

fn read_provenance(dir: &Path) -> HashMap<String, String> {
    let mut meta = HashMap::new();
    if let Ok(text) = std::fs::read_to_string(dir.join("provenance.txt")) {
        for line in text.lines() {
            if line.trim_start().starts_with('#') {
                continue;
            }
            if let Some((k, v)) = line.split_once('=') {
                meta.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
    }
    meta
}

fn read_csv(dir: &Path, name: &str) -> Option<Vec<HashMap<String, String>>> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_path(dir.join(name))
        .ok()?;
    let headers = rdr.headers().ok()?.clone();
    let mut rows: Vec<HashMap<String, String>> = Vec::new();
    for rec in rdr.records().flatten() {
        let m: HashMap<String, String> = headers
            .iter()
            .zip(rec.iter())
            .map(|(h, v)| (h.to_string(), v.to_string()))
            .collect();
        if m.get("ts").is_some_and(|s| !s.is_empty()) {
            rows.push(m);
        }
    }
    if rows.is_empty() {
        return None;
    }
    rows.sort_by(|a, b| {
        pf(a.get("ts"))
            .unwrap_or(0.0)
            .total_cmp(&pf(b.get("ts")).unwrap_or(0.0))
    });
    Some(rows)
}

// --------------------------------------------------------------------------
// Data model
// --------------------------------------------------------------------------
struct Sample {
    run_label: String,
    variant: String,
    boxid: String,
    ts: f64,
    t: f64,
    t_int: i64,
    vmrss_mb: Option<f64>,
    vmpeak_mb: Option<f64>,
    vmsize_mb: Option<f64>,
    rssanon_mb: Option<f64>,
    cpu_pct: Option<f64>,
    archived_count: Option<f64>,
    failed_count: Option<f64>,
    ready_backlog: Option<f64>,
    wal_gen_mb: Option<f64>,
    tx_mb_s: Option<f64>,
    archived_mb: Option<f64>,
    meta: HashMap<String, String>,
}

impl Sample {
    fn cell(&self, col: &str) -> String {
        let num = |v: Option<f64>| v.map_or(String::new(), fmt_num);
        match col {
            "run_label" => self.run_label.clone(),
            "variant" => self.variant.clone(),
            "box" => self.boxid.clone(),
            "ts" => format!("{:.3}", self.ts),
            "t" => fmt_num(self.t),
            "t_int" => self.t_int.to_string(),
            "vmrss_mb" => num(self.vmrss_mb),
            "vmpeak_mb" => num(self.vmpeak_mb),
            "vmsize_mb" => num(self.vmsize_mb),
            "rssanon_mb" => num(self.rssanon_mb),
            "cpu_pct" => num(self.cpu_pct),
            "archived_count" => num(self.archived_count),
            "failed_count" => num(self.failed_count),
            "ready_backlog" => num(self.ready_backlog),
            "wal_gen_mb" => num(self.wal_gen_mb),
            "tx_mb_s" => num(self.tx_mb_s),
            "archived_mb" => num(self.archived_mb),
            _ => self.meta.get(col).cloned().unwrap_or_default(),
        }
    }
}

/// Extracts a plotted metric from a sample (None = absent at that tick).
type Getter = Box<dyn Fn(&Sample) -> Option<f64>>;

struct Run {
    label: String,
    variant: String,
    boxid: String,
    dir: String,
    meta: HashMap<String, String>,
    samples: Vec<Sample>,
}

fn load_run(dir: &str, label: &str) -> Option<Run> {
    let d = Path::new(dir);
    // Degraded cells (failed burst workers, receiver shipped nothing) are stamped
    // INVALID by the drivers; exclude them so a weaker workload is not averaged in.
    if d.join("INVALID").exists() {
        eprintln!("warning: {dir} marked INVALID, skipping");
        return None;
    }
    let Some(mem) = read_csv(d, "mem.csv") else {
        eprintln!("warning: {dir} has no mem.csv, skipping");
        return None;
    };
    let meta = read_provenance(d);

    let cpu: HashMap<String, Option<f64>> = read_csv(d, "cpu.csv")
        .unwrap_or_default()
        .into_iter()
        .map(|r| (r["ts"].clone(), pf(r.get("pct_cpu"))))
        .collect();
    let arc: HashMap<String, HashMap<String, String>> = read_csv(d, "archive.csv")
        .unwrap_or_default()
        .into_iter()
        .map(|r| (r["ts"].clone(), r))
        .collect();

    let wal_rows = read_csv(d, "wal.csv").unwrap_or_default();
    let wal0 = wal_rows.iter().find_map(|r| pf(r.get("wal_bytes")));
    let wal: HashMap<String, Option<f64>> = wal_rows
        .iter()
        .map(|r| {
            let v = match (pf(r.get("wal_bytes")), wal0) {
                (Some(wb), Some(w0)) => Some((wb - w0) / MB),
                _ => None,
            };
            (r["ts"].clone(), v)
        })
        .collect();

    // tx upload rate: derivative over consecutive net samples, keyed by later ts.
    let mut net: HashMap<String, Option<f64>> = HashMap::new();
    let (mut prev_ts, mut prev_tx): (Option<f64>, Option<f64>) = (None, None);
    for r in read_csv(d, "net.csv").unwrap_or_default() {
        let ts = pf(r.get("ts"));
        let tx = pf(r.get("tx_bytes"));
        let rate = match (prev_ts, prev_tx, ts, tx) {
            (Some(pt), Some(px), Some(t), Some(x)) if t - pt > 0.0 && x - px >= 0.0 => {
                Some((x - px) / (t - pt) / MB)
            }
            _ => None,
        };
        net.insert(r["ts"].clone(), rate);
        prev_ts = ts;
        prev_tx = tx;
    }

    let variant = variant_of(label);
    let boxid = box_of(label);
    let ts0 = pf(mem[0].get("ts")).unwrap_or(0.0);
    let div = |r: &HashMap<String, String>, k: &str, denom: f64| pf(r.get(k)).map(|v| v / denom);

    let samples = mem
        .iter()
        .map(|r| {
            let tskey = r["ts"].clone();
            let ts = pf(Some(&tskey)).unwrap_or(0.0);
            let t = ts - ts0;
            let a = arc.get(&tskey);
            let archived = a.and_then(|m| pf(m.get("archived_count")));
            Sample {
                run_label: label.to_string(),
                variant: variant.clone(),
                boxid: boxid.clone(),
                ts,
                t,
                t_int: t.round() as i64,
                vmrss_mb: div(r, "vmrss_kb", KB),
                vmpeak_mb: div(r, "vmpeak_kb", KB),
                vmsize_mb: div(r, "vmsize_kb", KB),
                rssanon_mb: div(r, "rssanon_kb", KB),
                cpu_pct: cpu.get(&tskey).copied().flatten(),
                archived_count: archived,
                failed_count: a.and_then(|m| pf(m.get("failed_count"))),
                ready_backlog: a.and_then(|m| pf(m.get("ready_backlog"))),
                wal_gen_mb: wal.get(&tskey).copied().flatten(),
                tx_mb_s: net.get(&tskey).copied().flatten(),
                archived_mb: archived.map(|v| v * WAL_SEG_MB),
                meta: meta.clone(),
            }
        })
        .collect();

    Some(Run {
        label: label.to_string(),
        variant,
        boxid,
        dir: dir.to_string(),
        meta,
        samples,
    })
}

// --------------------------------------------------------------------------
// Aggregation
// --------------------------------------------------------------------------
#[derive(Clone)]
struct Style {
    color: Rgb,
    dashed: bool,
    z: i32,
}

fn style_for(variant: &str, idx: usize) -> Style {
    Style {
        color: variant_color(variant).unwrap_or(FALLBACK[idx % FALLBACK.len()]),
        dashed: variant.ends_with("-serial"),
        z: if variant.starts_with("walrus") { 10 } else { 4 },
    }
}

fn variants_ordered(labels: &[String]) -> Vec<String> {
    let mut vs: Vec<String> = labels.iter().map(|l| variant_of(l)).collect();
    vs.sort();
    vs.dedup();
    vs.sort_by_key(|v| (!v.starts_with("walrus"), v.clone()));
    vs
}

fn median(xs: &mut [f64]) -> f64 {
    xs.sort_by(f64::total_cmp);
    let n = xs.len();
    if n == 0 {
        0.0
    } else if n % 2 == 1 {
        xs[n / 2]
    } else {
        (xs[n / 2 - 1] + xs[n / 2]) / 2.0
    }
}

struct Series {
    label: String,
    xs: Vec<f64>,
    med: Vec<f64>,
    lo: Vec<f64>,
    hi: Vec<f64>,
    style: Style,
}

fn panel_series(
    samples: &[&Sample],
    variants: &[String],
    style_map: &HashMap<String, Style>,
    get: impl Fn(&Sample) -> Option<f64>,
) -> Vec<Series> {
    // variant -> elapsed-second -> values
    let mut buckets: HashMap<&str, BTreeMap<i64, Vec<f64>>> = HashMap::new();
    for s in samples {
        if let Some(v) = get(s) {
            buckets
                .entry(&s.variant)
                .or_default()
                .entry(s.t_int)
                .or_default()
                .push(v);
        }
    }
    let mut out = Vec::new();
    for variant in variants {
        let Some(tb) = buckets.get(variant.as_str()) else {
            continue;
        };
        let (mut xs, mut med, mut lo, mut hi) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for (t, vals) in tb {
            let mut v = vals.clone();
            xs.push(*t as f64);
            med.push(median(&mut v));
            lo.push(v.iter().copied().fold(f64::INFINITY, f64::min));
            hi.push(v.iter().copied().fold(f64::NEG_INFINITY, f64::max));
        }
        out.push(Series {
            label: variant.clone(),
            xs,
            med,
            lo,
            hi,
            style: style_map[variant].clone(),
        });
    }
    out
}

// --------------------------------------------------------------------------
// Rendering (tiny-skia raster + ab_glyph text)
//
// We own a small canvas: tiny-skia gives AA strokes/fills and the PNG encoder
// (png-format feature); ab_glyph rasterizes glyph coverage that we blend in.
// No plotters, no `image`. Dashing is tiny-skia's native StrokeDash (pixel
// space), so the old data-space dash_segments hack is gone.
// --------------------------------------------------------------------------
struct Panel {
    title: String,
    ylabel: String,
    series: Vec<Series>,
}

const W: u32 = 1180;
const HEADER_H: u32 = 48;
const PANEL_H: u32 = 340;

struct Fonts {
    regular: FontRef<'static>,
    bold: FontRef<'static>,
}

fn load_fonts() -> Result<Fonts> {
    let load = |b: &'static [u8]| {
        FontRef::try_from_slice(b).map_err(|_| anyhow::anyhow!("embedded font parse failed"))
    };
    Ok(Fonts {
        regular: load(dejavu::sans::regular())?,
        bold: load(dejavu::sans::bold())?,
    })
}

fn paint_rgb(c: Rgb, a: u8) -> Paint<'static> {
    let mut p = Paint::default();
    p.set_color_rgba8(c.0, c.1, c.2, a);
    p.anti_alias = true;
    p
}

fn polyline(pts: &[(f32, f32)]) -> Option<SkPath> {
    let mut pb = PathBuilder::new();
    let (x0, y0) = *pts.first()?;
    pb.move_to(x0, y0);
    for &(x, y) in &pts[1..] {
        pb.line_to(x, y);
    }
    pb.finish()
}

fn stroke_poly(
    pm: &mut Pixmap,
    pts: &[(f32, f32)],
    c: Rgb,
    a: u8,
    width: f32,
    dash: Option<[f32; 2]>,
) {
    let Some(path) = polyline(pts) else { return };
    let mut stroke = Stroke {
        width,
        line_cap: LineCap::Round,
        line_join: LineJoin::Round,
        ..Default::default()
    };
    if let Some([on, off]) = dash {
        stroke.dash = StrokeDash::new(vec![on, off], 0.0);
    }
    pm.stroke_path(
        &path,
        &paint_rgb(c, a),
        &stroke,
        Transform::identity(),
        None,
    );
}

fn fill_poly(pm: &mut Pixmap, pts: &[(f32, f32)], c: Rgb, a: u8) {
    let mut pb = PathBuilder::new();
    let Some(&(x0, y0)) = pts.first() else { return };
    pb.move_to(x0, y0);
    for &(x, y) in &pts[1..] {
        pb.line_to(x, y);
    }
    pb.close();
    let Some(path) = pb.finish() else { return };
    pm.fill_path(
        &path,
        &paint_rgb(c, a),
        FillRule::Winding,
        Transform::identity(),
        None,
    );
}

fn fill_rect(pm: &mut Pixmap, x: f32, y: f32, w: f32, h: f32, c: Rgb, a: u8) {
    if let Some(r) = Rect::from_xywh(x, y, w, h) {
        pm.fill_rect(r, &paint_rgb(c, a), Transform::identity(), None);
    }
}

fn stroke_rect(pm: &mut Pixmap, x: f32, y: f32, w: f32, h: f32, c: Rgb, width: f32) {
    stroke_poly(
        pm,
        &[(x, y), (x + w, y), (x + w, y + h), (x, y + h), (x, y)],
        c,
        255,
        width,
        None,
    );
}

/// "Nice" axis ticks: a round step (1/2/5 x 10^k) spanning [lo, hi].
fn nice_ticks(lo: f64, hi: f64, target: usize) -> Vec<f64> {
    if hi <= lo || target == 0 {
        return vec![lo];
    }
    let raw = (hi - lo) / target as f64;
    let mag = 10f64.powf(raw.log10().floor());
    let norm = raw / mag;
    let step = mag
        * if norm < 1.5 {
            1.0
        } else if norm < 3.0 {
            2.0
        } else if norm < 7.0 {
            5.0
        } else {
            10.0
        };
    let mut t = (lo / step).ceil() * step;
    let mut out = Vec::new();
    while t <= hi + step * 1e-9 {
        out.push(t);
        t += step;
    }
    out
}

// --- text: rasterize a string into a premultiplied pixmap, then blit ----------
fn text_width(font: &FontRef<'static>, px: f32, text: &str) -> f32 {
    let sf = font.as_scaled(PxScale::from(px));
    let mut w = 0.0;
    let mut prev = None;
    for ch in text.chars() {
        let g = font.glyph_id(ch);
        if let Some(p) = prev {
            w += sf.kern(p, g);
        }
        w += sf.h_advance(g);
        prev = Some(g);
    }
    w
}

fn text_pixmap(font: &FontRef<'static>, px: f32, text: &str, c: Rgb) -> Option<Pixmap> {
    let sf = font.as_scaled(PxScale::from(px));
    let w = (text_width(font, px, text).ceil() as u32 + 2).max(1);
    let h = ((sf.ascent() - sf.descent()).ceil() as u32 + 2).max(1);
    let mut pm = Pixmap::new(w, h)?;
    let baseline = sf.ascent() + 1.0;
    let data = pm.data_mut();
    let mut caret = 1.0f32;
    let mut prev = None;
    for ch in text.chars() {
        let gid = font.glyph_id(ch);
        if let Some(p) = prev {
            caret += sf.kern(p, gid);
        }
        let glyph = gid.with_scale_and_position(PxScale::from(px), point(caret, baseline));
        caret += sf.h_advance(gid);
        prev = Some(gid);
        let Some(og) = font.outline_glyph(glyph) else {
            continue;
        };
        let bb = og.px_bounds();
        og.draw(|gx, gy, cov| {
            let x = bb.min.x as i32 + gx as i32;
            let y = bb.min.y as i32 + gy as i32;
            if x < 0 || y < 0 || x as u32 >= w || y as u32 >= h {
                return;
            }
            let a = (cov.clamp(0.0, 1.0) * 255.0).round() as u8;
            if a == 0 {
                return;
            }
            let i = ((y as u32 * w + x as u32) * 4) as usize;
            // glyph boxes can overlap; keep the stronger coverage. premultiplied.
            if a >= data[i + 3] {
                let pre = |v: u8| ((v as u16 * a as u16) / 255) as u8;
                data[i] = pre(c.0);
                data[i + 1] = pre(c.1);
                data[i + 2] = pre(c.2);
                data[i + 3] = a;
            }
        });
    }
    Some(pm)
}

/// Blit a premultiplied src onto an opaque dst (src-over keeps dst opaque).
/// rotate_ccw=true places src rotated 90° counter-clockwise (vertical y-labels).
fn blit(dst: &mut Pixmap, src: &Pixmap, dx: i32, dy: i32, rotate_ccw: bool) {
    let (dw, dh) = (dst.width(), dst.height());
    let (sw, sh) = (src.width(), src.height());
    let s = src.data();
    let d = dst.data_mut();
    for sy in 0..sh {
        for sx in 0..sw {
            let si = ((sy * sw + sx) * 4) as usize;
            let a = s[si + 3];
            if a == 0 {
                continue;
            }
            let (rx, ry) = if rotate_ccw {
                (sy as i32, (sw - 1 - sx) as i32)
            } else {
                (sx as i32, sy as i32)
            };
            let (px_, py_) = (dx + rx, dy + ry);
            if px_ < 0 || py_ < 0 || px_ as u32 >= dw || py_ as u32 >= dh {
                continue;
            }
            let di = ((py_ as u32 * dw + px_ as u32) * 4) as usize;
            let inv = (255 - a) as u16;
            for k in 0..3 {
                d[di + k] = (s[si + k] as u16 + d[di + k] as u16 * inv / 255) as u8;
            }
            d[di + 3] = 255;
        }
    }
}

fn text_at(
    pm: &mut Pixmap,
    font: &FontRef<'static>,
    x: f32,
    top: f32,
    px: f32,
    text: &str,
    c: Rgb,
) {
    if let Some(t) = text_pixmap(font, px, text, c) {
        blit(pm, &t, x.round() as i32, top.round() as i32, false);
    }
}

fn text_center(
    pm: &mut Pixmap,
    font: &FontRef<'static>,
    cx: f32,
    top: f32,
    px: f32,
    text: &str,
    c: Rgb,
) {
    if let Some(t) = text_pixmap(font, px, text, c) {
        blit(
            pm,
            &t,
            (cx - t.width() as f32 / 2.0).round() as i32,
            top.round() as i32,
            false,
        );
    }
}

fn text_right(
    pm: &mut Pixmap,
    font: &FontRef<'static>,
    right: f32,
    cy: f32,
    px: f32,
    text: &str,
    c: Rgb,
) {
    if let Some(t) = text_pixmap(font, px, text, c) {
        let x = (right - t.width() as f32).round() as i32;
        let y = (cy - t.height() as f32 / 2.0).round() as i32;
        blit(pm, &t, x, y, false);
    }
}

fn text_left_mid(
    pm: &mut Pixmap,
    font: &FontRef<'static>,
    x: f32,
    cy: f32,
    px: f32,
    text: &str,
    c: Rgb,
) {
    if let Some(t) = text_pixmap(font, px, text, c) {
        blit(
            pm,
            &t,
            x.round() as i32,
            (cy - t.height() as f32 / 2.0).round() as i32,
            false,
        );
    }
}

fn text_vert(
    pm: &mut Pixmap,
    font: &FontRef<'static>,
    left: f32,
    cy: f32,
    px: f32,
    text: &str,
    c: Rgb,
) {
    if let Some(t) = text_pixmap(font, px, text, c) {
        // rotated box is t.height() wide x t.width() tall; center on cy.
        let top = (cy - t.width() as f32 / 2.0).round() as i32;
        blit(pm, &t, left.round() as i32, top, true);
    }
}

fn render(
    panels: &[Panel],
    out_path: &Path,
    header: &str,
    suffix: &str,
    xlabel: &str,
    fonts: &Fonts,
) -> Result<()> {
    let n = panels.len() as u32;
    let height = HEADER_H + n * PANEL_H;
    let xmax = panels
        .iter()
        .flat_map(|p| p.series.iter())
        .filter_map(|s| s.xs.last().copied())
        .fold(0.0_f64, f64::max)
        .max(1.0);

    let mut pm = Pixmap::new(W, height).context("alloc pixmap")?;
    pm.fill(Color::from_rgba8(BG.0, BG.1, BG.2, 255));
    text_at(&mut pm, &fonts.bold, 24.0, 8.0, 20.0, header, FG);
    if !suffix.is_empty() {
        text_at(&mut pm, &fonts.regular, 24.0, 30.0, 14.0, suffix, MUTED);
    }
    for (i, panel) in panels.iter().enumerate() {
        let py0 = HEADER_H + i as u32 * PANEL_H;
        draw_panel(&mut pm, panel, py0, xmax, xlabel, i == 0, fonts);
    }
    pm.save_png(out_path)
        .map_err(|e| anyhow::anyhow!("save {out_path:?}: {e}"))?;
    Ok(())
}

fn draw_panel(
    pm: &mut Pixmap,
    panel: &Panel,
    py0: u32,
    xmax: f64,
    xlabel: &str,
    legend: bool,
    fonts: &Fonts,
) {
    let ymax = panel
        .series
        .iter()
        .flat_map(|s| s.hi.iter().copied())
        .fold(0.0_f64, f64::max);
    let ymax = if ymax > 0.0 { ymax * 1.05 } else { 1.0 };

    let caption_h = if panel.title.is_empty() { 0.0 } else { 24.0 };
    let left = 76.0_f32;
    let right = W as f32 - 12.0;
    let top = py0 as f32 + 12.0 + caption_h;
    let bottom = py0 as f32 + PANEL_H as f32 - 42.0;

    if !panel.title.is_empty() {
        text_at(
            pm,
            &fonts.bold,
            left,
            py0 as f32 + 12.0,
            17.0,
            &panel.title,
            FG,
        );
    }

    let sx = |x: f64| left + (x / xmax) as f32 * (right - left);
    let sy = |y: f64| bottom - (y / ymax) as f32 * (bottom - top);

    for t in nice_ticks(0.0, ymax, 6) {
        let yy = sy(t);
        stroke_poly(pm, &[(left, yy), (right, yy)], SEL, 255, 1.0, None);
        text_right(pm, &fonts.regular, left - 8.0, yy, 14.0, &fmt_num(t), MUTED);
    }
    for t in nice_ticks(0.0, xmax, 8) {
        let xx = sx(t);
        stroke_poly(pm, &[(xx, top), (xx, bottom)], SEL, 255, 1.0, None);
        text_center(
            pm,
            &fonts.regular,
            xx,
            bottom + 6.0,
            14.0,
            &fmt_num(t),
            MUTED,
        );
    }
    // axis spines (left + bottom), slightly heavier
    stroke_poly(
        pm,
        &[(left, top), (left, bottom), (right, bottom)],
        SEL,
        255,
        1.5,
        None,
    );

    text_center(
        pm,
        &fonts.regular,
        (left + right) / 2.0,
        bottom + 22.0,
        14.0,
        xlabel,
        MUTED,
    );
    text_vert(
        pm,
        &fonts.regular,
        14.0,
        (top + bottom) / 2.0,
        14.0,
        &panel.ylabel,
        MUTED,
    );

    // bands first (under every line)
    for s in &panel.series {
        if s.xs.len() < 2 {
            continue;
        }
        let mut poly: Vec<(f32, f32)> =
            s.xs.iter()
                .zip(&s.lo)
                .map(|(&x, &y)| (sx(x), sy(y)))
                .collect();
        poly.extend(s.xs.iter().zip(&s.hi).rev().map(|(&x, &y)| (sx(x), sy(y))));
        fill_poly(pm, &poly, s.style.color, 36);
    }

    // median lines by ascending z (walrus rides on top)
    let mut ordered: Vec<&Series> = panel.series.iter().collect();
    ordered.sort_by_key(|s| s.style.z);
    for s in ordered {
        let pts: Vec<(f32, f32)> =
            s.xs.iter()
                .zip(&s.med)
                .map(|(&x, &y)| (sx(x), sy(y)))
                .collect();
        let dash = s.style.dashed.then_some([11.0, 7.0]);
        stroke_poly(pm, &pts, s.style.color, 200, 1.25, dash);
    }

    if legend && !panel.series.is_empty() {
        draw_legend(pm, &panel.series, left + 12.0, top + 10.0, fonts);
    }
}

fn draw_legend(pm: &mut Pixmap, series: &[Series], x: f32, y: f32, fonts: &Fonts) {
    let (pad, swatch, gap, row, fs) = (8.0_f32, 24.0_f32, 8.0_f32, 20.0_f32, 14.0_f32);
    let tw = series
        .iter()
        .map(|s| text_width(&fonts.regular, fs, &s.label))
        .fold(0.0_f32, f32::max);
    let bw = pad + swatch + gap + tw + pad;
    let bh = pad * 2.0 + row * series.len() as f32;
    fill_rect(pm, x, y, bw, bh, FLOAT, 255);
    stroke_rect(pm, x, y, bw, bh, SEL, 1.0);
    for (i, s) in series.iter().enumerate() {
        let cy = y + pad + row * i as f32 + row / 2.0;
        let lx = x + pad;
        let dash = s.style.dashed.then_some([8.0, 5.0]);
        stroke_poly(
            pm,
            &[(lx, cy), (lx + swatch, cy)],
            s.style.color,
            255,
            1.25,
            dash,
        );
        text_left_mid(pm, &fonts.regular, lx + swatch + gap, cy, fs, &s.label, FG);
    }
}

// --------------------------------------------------------------------------
// Summary + formatting
// --------------------------------------------------------------------------
fn fmt_num(v: f64) -> String {
    if v.is_finite() && v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        // %g-ish: trim trailing zeros from a 6-sig-figure rendering.
        let s = format!("{v:.6}");
        let s = s.trim_end_matches('0').trim_end_matches('.');
        s.to_string()
    }
}

fn summarize(run: &Run) -> Vec<(String, serde_json::Value)> {
    use serde_json::json;
    let collect = |get: &dyn Fn(&Sample) -> Option<f64>| -> Vec<f64> {
        run.samples.iter().filter_map(get).collect()
    };

    let mut row: Vec<(String, serde_json::Value)> = Vec::new();
    let mut push = |k: &str, v: serde_json::Value| row.push((k.to_string(), v));

    push("run_label", json!(run.label));
    push("variant", json!(run.variant));
    push("box", json!(run.boxid));
    for k in META_KEYS {
        push(k, json!(run.meta.get(k).cloned().unwrap_or_default()));
    }
    push("dir", json!(run.dir));

    let vmrss = collect(&|s| s.vmrss_mb);
    let mut vmrss_m = vmrss.clone();
    if !vmrss.is_empty() {
        push(
            "peak_vmrss_mb",
            json!(vmrss.iter().copied().fold(f64::MIN, f64::max)),
        );
        push("median_vmrss_mb", json!(median(&mut vmrss_m)));
    }
    let vmpeak = collect(&|s| s.vmpeak_mb);
    if !vmpeak.is_empty() {
        push(
            "peak_vmpeak_mb",
            json!(vmpeak.iter().copied().fold(f64::MIN, f64::max)),
        );
    }
    let vmsize = collect(&|s| s.vmsize_mb);
    let mut vmsize_m = vmsize.clone();
    if !vmsize.is_empty() {
        push(
            "peak_vmsize_mb",
            json!(vmsize.iter().copied().fold(f64::MIN, f64::max)),
        );
        push("median_vmsize_mb", json!(median(&mut vmsize_m)));
    }
    let cpu = collect(&|s| s.cpu_pct);
    if !cpu.is_empty() {
        push(
            "mean_cpu_pct",
            json!(cpu.iter().sum::<f64>() / cpu.len() as f64),
        );
        push(
            "peak_cpu_pct",
            json!(cpu.iter().copied().fold(f64::MIN, f64::max)),
        );
    }
    let backlog = collect(&|s| s.ready_backlog);
    if !backlog.is_empty() {
        push(
            "max_backlog",
            json!(backlog.iter().copied().fold(f64::MIN, f64::max) as i64),
        );
        let last = run
            .samples
            .iter()
            .rev()
            .find_map(|s| s.ready_backlog)
            .unwrap_or(0.0);
        push("final_backlog", json!(last as i64));
    }
    let archived = collect(&|s| s.archived_count);
    if !archived.is_empty() {
        let total = archived[archived.len() - 1] - archived[0];
        push("total_archived", json!(total as i64));
        let elapsed = run.samples[run.samples.len() - 1].t - run.samples[0].t;
        if elapsed > 0.0 {
            push("mean_drain_mb_s", json!(total * WAL_SEG_MB / elapsed));
        }
    }
    let failed = collect(&|s| s.failed_count);
    if !failed.is_empty() {
        push(
            "total_failed",
            json!((failed[failed.len() - 1] - failed[0]) as i64),
        );
    }
    row
}

fn json_cell(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => String::new(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.to_string()
            } else if let Some(f) = n.as_f64() {
                fmt_num(f)
            } else {
                n.to_string()
            }
        }
        other => other.to_string(),
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let fonts = load_fonts()?;
    if args.runs.len() != args.labels.len() {
        anyhow::bail!("number of --run and --label must match");
    }
    let stamp = args
        .stamp
        .clone()
        .unwrap_or_else(|| chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string());
    let out = Path::new(&args.out);
    std::fs::create_dir_all(out).with_context(|| format!("mkdir {out:?}"))?;

    let runs: Vec<Run> = args
        .runs
        .iter()
        .zip(&args.labels)
        .filter_map(|(d, l)| load_run(d, l))
        .collect();
    if runs.is_empty() {
        anyhow::bail!("no loadable runs");
    }

    let all: Vec<&Sample> = runs.iter().flat_map(|r| r.samples.iter()).collect();
    let labels: Vec<String> = runs.iter().map(|r| r.label.clone()).collect();
    let variants = variants_ordered(&labels);
    let style_map: HashMap<String, Style> = variants
        .iter()
        .enumerate()
        .map(|(i, v)| (v.clone(), style_for(v, i)))
        .collect();
    let suffix = {
        let m = &runs[0].meta;
        let g = |k: &str| m.get(k).map(String::as_str).unwrap_or("?");
        format!(
            "scale={} churn_rows={} burst={}s | {stamp}",
            g("scale"),
            g("churn_rows"),
            g("burst_seconds")
        )
    };

    let panel = |ycol: &str, title: &str, ylabel: &str| -> Panel {
        let get: Getter = match ycol {
            "vmrss_mb" => Box::new(|s: &Sample| s.vmrss_mb),
            "vmpeak_mb" => Box::new(|s: &Sample| s.vmpeak_mb),
            "ready_backlog" => Box::new(|s: &Sample| s.ready_backlog),
            "tx_mb_s" => Box::new(|s: &Sample| s.tx_mb_s),
            "cpu_pct" => Box::new(|s: &Sample| s.cpu_pct),
            _ => Box::new(|_: &Sample| None),
        };
        Panel {
            title: title.to_string(),
            ylabel: ylabel.to_string(),
            series: panel_series(&all, &variants, &style_map, get),
        }
    };

    render(
        &[
            panel(
                "vmrss_mb",
                "resident memory (vmrss) - median across replicas, band = min..max",
                "vmrss (mb)",
            ),
            panel(
                "vmpeak_mb",
                "peak virtual memory (vmpeak) - the no-overcommit metric",
                "vmpeak (mb)",
            ),
        ],
        &out.join("mem_over_time.png"),
        "memory over time: walrus vs wal-g vs pgbackrest",
        &suffix,
        "elapsed seconds",
        &fonts,
    )?;
    render(
        &[panel("ready_backlog", "", "*.ready backlog (segments)")],
        &out.join("backlog.png"),
        "archive backlog over time (lower = keeping up)",
        &suffix,
        "elapsed seconds",
        &fonts,
    )?;
    render(
        &[panel("tx_mb_s", "", "tx rate (mb/s)")],
        &out.join("upload_rate.png"),
        "network upload rate to s3 (tx_bytes derivative)",
        &suffix,
        "elapsed seconds",
        &fonts,
    )?;
    render(
        &[panel("cpu_pct", "", "cpu (%)")],
        &out.join("cpu.png"),
        "daemon cpu utilization over time",
        &suffix,
        "elapsed seconds",
        &fonts,
    )?;

    // --- raw exports -------------------------------------------------------
    let samples_path = out.join(format!("samples_{stamp}.csv"));
    let mut w = csv::Writer::from_path(&samples_path)?;
    w.write_record(SAMPLE_COLS.iter().chain(META_KEYS.iter()))?;
    for s in &all {
        let row: Vec<String> = SAMPLE_COLS
            .iter()
            .chain(META_KEYS.iter())
            .map(|c| s.cell(c))
            .collect();
        w.write_record(&row)?;
    }
    w.flush()?;

    let rows: Vec<Vec<(String, serde_json::Value)>> = runs.iter().map(summarize).collect();
    // CSV field order = first-seen union across rows.
    let mut fields: Vec<String> = Vec::new();
    for row in &rows {
        for (k, _) in row {
            if !fields.contains(k) {
                fields.push(k.clone());
            }
        }
    }
    let summary_csv = out.join(format!("summary_{stamp}.csv"));
    let mut sw = csv::Writer::from_path(&summary_csv)?;
    sw.write_record(&fields)?;
    for row in &rows {
        let map: HashMap<&str, &serde_json::Value> =
            row.iter().map(|(k, v)| (k.as_str(), v)).collect();
        let rec: Vec<String> = fields
            .iter()
            .map(|f| map.get(f.as_str()).map_or(String::new(), |v| json_cell(v)))
            .collect();
        sw.write_record(&rec)?;
    }
    sw.flush()?;

    // summary.json: BTreeMap-backed objects => keys sorted (matches sort_keys).
    let runs_json: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            serde_json::Value::Object(row.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        })
        .collect();
    let doc = serde_json::json!({ "stamp": stamp, "runs": runs_json });
    std::fs::write(
        out.join("summary.json"),
        serde_json::to_string_pretty(&doc)?,
    )?;

    println!(
        "wrote plots + {} + {} + summary.json",
        samples_path.display(),
        summary_csv.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variant_and_box() {
        assert_eq!(variant_of("walrus-serial-b0"), "walrus-serial");
        assert_eq!(box_of("walrus-serial-b0"), "0");
        assert_eq!(variant_of("walrus-r1"), "walrus-r1"); // -r1 is not -b<N>
        assert_eq!(box_of("walrus"), "0");
        assert_eq!(variant_of("pgbackrest-b12"), "pgbackrest");
        assert_eq!(box_of("pgbackrest-b12"), "12");
    }

    #[test]
    fn median_odd_even() {
        assert_eq!(median(&mut [3.0, 1.0, 2.0]), 2.0);
        assert_eq!(median(&mut [4.0, 1.0, 3.0, 2.0]), 2.5);
    }

    #[test]
    fn fmt_num_int_vs_float() {
        assert_eq!(fmt_num(42.0), "42");
        assert_eq!(fmt_num(2.5), "2.5");
    }

    #[test]
    fn variant_order_walrus_first() {
        let v = variants_ordered(&["walg-b0".into(), "walrus-b0".into(), "pgbackrest-b0".into()]);
        assert_eq!(v[0], "walrus");
    }
}
