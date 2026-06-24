//! bench-compare, grouped-bar comparison across tools
//!
//! Reads op_metrics.txt, cpu.csv, and mem.csv from each --run. Emits:
//!   backup_compare.png   stacked bar panels: size, duration, CPU, RSS, VM
//!   ops_summary.md       per-op table (size / elapsed / CPU / RSS)
//!   ops_compare_<stamp>.csv  one row per (op, variant): every metric
//!
//! Replicas aggregate to median

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use clap::Parser;
use tiny_skia::{Color, Pixmap};

use bench_tools::viz::{
    BG, FG, Fonts, HEADER_H, MUTED, SEL, Style, W, fill_rect, fmt_num, load_fonts, median,
    nice_ticks, stroke_poly, stroke_rect, style_for, text_at, text_center, text_left_mid,
    text_right, text_vert, text_width, variant_of, variants_ordered,
};

/// Backup ops in display order; wal-receive is opt-in via --ops
const DEFAULT_OPS: [&str; 5] = [
    "backup-send",
    "backup-delta",
    "backup-delta-sidecar",
    "backup-delta-summaries",
    "backup-fetch",
];

const PANEL_H: u32 = 300;

#[derive(Parser)]
#[command(about = "grouped-bar comparison of per-op backup metrics across tools")]
struct Args {
    /// run directory, repeatable
    #[arg(long = "run", required = true)]
    runs: Vec<String>,
    /// label matching --run, repeatable
    #[arg(long = "label", required = true)]
    labels: Vec<String>,
    /// output directory
    #[arg(long)]
    out: String,
    /// comma-separated op order/filter
    #[arg(long)]
    ops: Option<String>,
    /// timestamp tag for output filenames (default: now, UTC)
    #[arg(long)]
    stamp: Option<String>,
}

#[derive(Clone, Copy)]
enum Unit {
    Gb,
    Sec,
    Pct,
    Mb,
}

fn fmt_val(v: f64, unit: Unit) -> String {
    match unit {
        Unit::Gb if v < 10.0 => format!("{v:.2}"),
        Unit::Gb if v < 100.0 => format!("{v:.1}"),
        Unit::Sec if v < 10.0 => format!("{v:.1}"),
        Unit::Pct | Unit::Mb | Unit::Gb | Unit::Sec => format!("{v:.0}"),
    }
}

// --------------------------------------------------------------------------
// Ingest
// --------------------------------------------------------------------------
fn read_kv(path: &Path) -> HashMap<String, String> {
    let mut m = HashMap::new();
    if let Ok(text) = std::fs::read_to_string(path) {
        for line in text.lines() {
            if line.trim_start().starts_with('#') {
                continue;
            }
            if let Some((k, v)) = line.split_once('=') {
                m.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
    }
    m
}

/// Aggregate CSV columns, skipping blanks
fn col_agg(dir: &Path, file: &str, cols: &[&str], reduce: impl Fn(f64, f64) -> f64) -> Option<f64> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_path(dir.join(file))
        .ok()?;
    let headers = rdr.headers().ok()?.clone();
    let idx: Vec<usize> = cols
        .iter()
        .filter_map(|c| headers.iter().position(|h| h == *c))
        .collect();
    if idx.is_empty() {
        return None;
    }
    let mut acc: Option<f64> = None;
    for rec in rdr.records().flatten() {
        for &i in &idx {
            if let Some(v) = rec.get(i).and_then(|s| s.trim().parse::<f64>().ok()) {
                acc = Some(acc.map_or(v, |a| reduce(a, v)));
            }
        }
    }
    acc
}

struct Cell {
    op: String,
    variant: String,
    size_gb: Option<f64>,
    dur_s: Option<f64>,
    peak_cpu: Option<f64>,
    mean_cpu: Option<f64>,
    peak_rss_mb: Option<f64>,
    peak_vm_mb: Option<f64>,
}

fn load_cell(dir: &str, label: &str) -> Option<Cell> {
    let d = Path::new(dir);
    if d.join("INVALID").exists() {
        eprintln!("warning: {dir} marked INVALID, skipping");
        return None;
    }
    let m = read_kv(&d.join("op_metrics.txt"));
    let op = m.get("op").cloned();
    let Some(op) = op else {
        eprintln!("warning: {dir} has no op_metrics.txt op=, skipping");
        return None;
    };
    let f = |k: &str| m.get(k).and_then(|s| s.parse::<f64>().ok());
    let peak_cpu = col_agg(d, "cpu.csv", &["pct_cpu"], f64::max);
    let mean = {
        let mut rdr = csv::ReaderBuilder::new()
            .flexible(true)
            .from_path(d.join("cpu.csv"))
            .ok();
        rdr.as_mut().and_then(|r| {
            let hi = r.headers().ok()?.iter().position(|h| h == "pct_cpu")?;
            let (mut sum, mut n) = (0.0, 0u32);
            for rec in r.records().flatten() {
                if let Some(v) = rec.get(hi).and_then(|s| s.trim().parse::<f64>().ok()) {
                    sum += v;
                    n += 1;
                }
            }
            (n > 0).then(|| sum / n as f64)
        })
    };
    Some(Cell {
        op,
        variant: variant_of(label),
        size_gb: f("bytes_processed").map(|b| b / 1e9),
        dur_s: f("elapsed_s"),
        peak_cpu,
        mean_cpu: mean,
        // vmhwm is peak resident high-water; fall back to vmrss
        peak_rss_mb: col_agg(d, "mem.csv", &["vmhwm_kb", "vmrss_kb"], f64::max)
            .map(|kb| kb / 1024.0),
        // vmpeak is peak virtual address reservation (Go runtime inflates this)
        peak_vm_mb: col_agg(d, "mem.csv", &["vmpeak_kb"], f64::max).map(|kb| kb / 1024.0),
    })
}

/// Median metric for (op, variant)
fn agg(cells: &[&Cell], get: impl Fn(&Cell) -> Option<f64>) -> Option<f64> {
    let mut vs: Vec<f64> = cells.iter().filter_map(|c| get(c)).collect();
    if vs.is_empty() {
        None
    } else {
        Some(median(&mut vs))
    }
}

// --------------------------------------------------------------------------
// Bar rendering
// --------------------------------------------------------------------------
#[derive(Clone, Copy)]
enum Scale {
    Linear,
    Log,
}

struct BarPanel {
    title: String,
    ylabel: String,
    unit: Unit,
    scale: Scale,
    /// (op, variant) value
    vals: HashMap<(String, String), f64>,
}

/// Log ticks inside [lo, hi]
fn log_ticks(lo: f64, hi: f64) -> Vec<f64> {
    let mut out = Vec::new();
    let k0 = lo.log10().floor() as i32;
    let k1 = hi.log10().ceil() as i32;
    for k in k0..=k1 {
        for m in [1.0, 2.0, 5.0] {
            let v = m * 10f64.powi(k);
            if v >= lo * 0.999 && v <= hi * 1.001 {
                out.push(v);
            }
        }
    }
    if out.is_empty() {
        out.push(lo);
    }
    out
}

fn draw_bar_panel(
    pm: &mut Pixmap,
    panel: &BarPanel,
    py0: u32,
    ops: &[String],
    variants: &[String],
    style_map: &HashMap<String, Style>,
    fonts: &Fonts,
) {
    let maxv = panel.vals.values().copied().fold(0.0_f64, f64::max);
    let (lo, hi) = match panel.scale {
        Scale::Linear => (0.0, if maxv > 0.0 { maxv * 1.08 } else { 1.0 }),
        Scale::Log => {
            let minv = panel
                .vals
                .values()
                .copied()
                .filter(|v| *v > 0.0)
                .fold(f64::INFINITY, f64::min);
            let minv = if minv.is_finite() { minv } else { 1.0 };
            (
                10f64.powi(minv.log10().floor() as i32),
                10f64.powi((maxv.max(minv) * 1.0001).log10().ceil() as i32),
            )
        }
    };

    let left = 84.0_f32;
    let right = W as f32 - 16.0;
    let top = py0 as f32 + 38.0;
    let bottom = py0 as f32 + PANEL_H as f32 - 40.0;

    text_at(
        pm,
        &fonts.bold,
        left,
        py0 as f32 + 12.0,
        16.0,
        &panel.title,
        FG,
    );

    // y to pixel, clamped to baseline
    let sy = |v: f64| -> f32 {
        match panel.scale {
            Scale::Linear => {
                bottom - ((v - lo) / (hi - lo)).clamp(0.0, 1.0) as f32 * (bottom - top)
            }
            Scale::Log => {
                let v = v.max(lo);
                let f = (v.log10() - lo.log10()) / (hi.log10() - lo.log10());
                bottom - f.clamp(0.0, 1.0) as f32 * (bottom - top)
            }
        }
    };

    let ticks = match panel.scale {
        Scale::Linear => nice_ticks(0.0, hi, 5),
        Scale::Log => log_ticks(lo, hi),
    };
    for t in &ticks {
        let yy = sy(*t);
        stroke_poly(pm, &[(left, yy), (right, yy)], SEL, 255, 1.0, None);
        text_right(
            pm,
            &fonts.regular,
            left - 8.0,
            yy,
            13.0,
            &fmt_num(*t),
            MUTED,
        );
    }
    stroke_poly(
        pm,
        &[(left, top), (left, bottom), (right, bottom)],
        SEL,
        255,
        1.5,
        None,
    );
    text_vert(
        pm,
        &fonts.regular,
        16.0,
        (top + bottom) / 2.0,
        13.0,
        &panel.ylabel,
        MUTED,
    );

    let n_groups = ops.len().max(1);
    let group_w = (right - left) / n_groups as f32;
    let n_series = variants.len().max(1);
    let inner = group_w * 0.78;
    let slot = inner / n_series as f32;
    let barw = slot * 0.86;

    for (gi, op) in ops.iter().enumerate() {
        let gx = left + group_w * gi as f32;
        let xc = gx + group_w / 2.0;
        // Drop backup- prefix
        let short = op.strip_prefix("backup-").unwrap_or(op);
        text_center(pm, &fonts.regular, xc, bottom + 8.0, 13.0, short, MUTED);

        for (si, variant) in variants.iter().enumerate() {
            let Some(&v) = panel.vals.get(&(op.clone(), variant.clone())) else {
                continue;
            };
            if v <= 0.0 {
                continue;
            }
            let bx = gx + (group_w - inner) / 2.0 + slot * si as f32 + (slot - barw) / 2.0;
            let by = sy(v);
            let color = style_map[variant].color;
            fill_rect(pm, bx, by, barw, bottom - by, color, 235);
            stroke_rect(pm, bx, by, barw, bottom - by, BG, 1.0);
            text_center(
                pm,
                &fonts.regular,
                bx + barw / 2.0,
                by - 14.0,
                11.0,
                &fmt_val(v, panel.unit),
                FG,
            );
        }
    }
}

fn draw_hlegend(
    pm: &mut Pixmap,
    variants: &[String],
    style_map: &HashMap<String, Style>,
    fonts: &Fonts,
) {
    let (sw, gap, fs) = (16.0_f32, 6.0_f32, 13.0_f32);
    // Place legend from right edge
    let mut x = W as f32 - 16.0;
    for variant in variants.iter().rev() {
        let label_w = text_width(&fonts.regular, fs, variant);
        x -= label_w;
        text_left_mid(pm, &fonts.regular, x, 24.0, fs, variant, FG);
        x -= gap + sw;
        fill_rect(
            pm,
            x,
            24.0 - sw / 2.0,
            sw,
            sw,
            style_map[variant].color,
            235,
        );
        stroke_rect(pm, x, 24.0 - sw / 2.0, sw, sw, SEL, 1.0);
        x -= 16.0;
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.runs.len() != args.labels.len() {
        anyhow::bail!("number of --run and --label must match");
    }
    let fonts = load_fonts()?;
    let stamp = args
        .stamp
        .clone()
        .unwrap_or_else(|| chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string());
    let out = Path::new(&args.out);
    std::fs::create_dir_all(out).with_context(|| format!("mkdir {out:?}"))?;

    let cells: Vec<Cell> = args
        .runs
        .iter()
        .zip(&args.labels)
        .filter_map(|(d, l)| load_cell(d, l))
        .collect();
    if cells.is_empty() {
        anyhow::bail!("no loadable runs");
    }

    // Ops present, requested order first
    let order: Vec<String> = match &args.ops {
        Some(s) => s.split(',').map(|o| o.trim().to_string()).collect(),
        None => DEFAULT_OPS.iter().map(|s| s.to_string()).collect(),
    };
    let mut ops: Vec<String> = order
        .iter()
        .filter(|o| cells.iter().any(|c| &c.op == *o))
        .cloned()
        .collect();
    // Append extra ops, stable + deduped
    for c in &cells {
        if !ops.contains(&c.op) {
            ops.push(c.op.clone());
        }
    }

    let labels: Vec<String> = cells.iter().map(|c| c.variant.clone()).collect();
    let variants = variants_ordered(&labels);
    let style_map: HashMap<String, Style> = variants
        .iter()
        .enumerate()
        .map(|(i, v)| (v.clone(), style_for(v, i)))
        .collect();

    // Build aggregated metric map
    let build = |get: &dyn Fn(&Cell) -> Option<f64>| -> HashMap<(String, String), f64> {
        let mut out = HashMap::new();
        for op in &ops {
            for variant in &variants {
                let group: Vec<&Cell> = cells
                    .iter()
                    .filter(|c| &c.op == op && &c.variant == variant)
                    .collect();
                if let Some(v) = agg(&group, get) {
                    out.insert((op.clone(), variant.clone()), v);
                }
            }
        }
        out
    };

    let panels = [
        BarPanel {
            title: "backup size — stored bytes (delta = increment, fetch = restored)".into(),
            ylabel: "size (GB)".into(),
            unit: Unit::Gb,
            scale: Scale::Linear,
            vals: build(&|c| c.size_gb),
        },
        BarPanel {
            title: "duration — wall-clock of the op (log scale)".into(),
            ylabel: "elapsed (s)".into(),
            unit: Unit::Sec,
            scale: Scale::Log,
            vals: build(&|c| c.dur_s),
        },
        BarPanel {
            title: "CPU during op — peak sampled utilization (>100% = multi-core)".into(),
            ylabel: "peak CPU (%)".into(),
            unit: Unit::Pct,
            scale: Scale::Linear,
            vals: build(&|c| c.peak_cpu),
        },
        BarPanel {
            title: "memory during op — peak resident set (VmHWM)".into(),
            ylabel: "peak RSS (MB)".into(),
            unit: Unit::Mb,
            scale: Scale::Linear,
            vals: build(&|c| c.peak_rss_mb),
        },
        BarPanel {
            title: "virtual memory during op — peak address reservation (VmPeak)".into(),
            ylabel: "peak VM (MB)".into(),
            unit: Unit::Mb,
            scale: Scale::Linear,
            vals: build(&|c| c.peak_vm_mb),
        },
    ];

    let height = HEADER_H + panels.len() as u32 * PANEL_H;
    let mut pm = Pixmap::new(W, height).context("alloc pixmap")?;
    pm.fill(Color::from_rgba8(BG.0, BG.1, BG.2, 255));
    text_at(
        &mut pm,
        &fonts.bold,
        16.0,
        8.0,
        18.0,
        "backup comparison: walrus vs wal-g vs pgbackrest",
        FG,
    );
    text_at(&mut pm, &fonts.regular, 16.0, 30.0, 12.0, &stamp, MUTED);
    draw_hlegend(&mut pm, &variants, &style_map, &fonts);
    for (i, panel) in panels.iter().enumerate() {
        draw_bar_panel(
            &mut pm,
            panel,
            HEADER_H + i as u32 * PANEL_H,
            &ops,
            &variants,
            &style_map,
            &fonts,
        );
    }
    let png = out.join("backup_compare.png");
    pm.save_png(&png)
        .map_err(|e| anyhow::anyhow!("save {png:?}: {e}"))?;

    // --- markdown table -----------------------------------------------------
    let mut md = String::from("# Backup comparison\n");
    for op in &ops {
        let rows: Vec<&Cell> = cells.iter().filter(|c| &c.op == op).collect();
        if rows.is_empty() {
            continue;
        }
        md.push_str(&format!("\n### {op}\n\n"));
        md.push_str(
            "| variant | size_GB | elapsed_s | peak_CPU_% | mean_CPU_% | peak_RSS_MB | peak_VM_MB |\n",
        );
        md.push_str("|---|--:|--:|--:|--:|--:|--:|\n");
        for variant in &variants {
            let group: Vec<&Cell> = rows
                .iter()
                .copied()
                .filter(|c| &c.variant == variant)
                .collect();
            if group.is_empty() {
                continue;
            }
            let cell = |get: &dyn Fn(&Cell) -> Option<f64>, p: usize| {
                agg(&group, get).map_or("—".to_string(), |v| format!("{v:.p$}"))
            };
            md.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} |\n",
                variant,
                cell(&|c| c.size_gb, 2),
                cell(&|c| c.dur_s, 1),
                cell(&|c| c.peak_cpu, 0),
                cell(&|c| c.mean_cpu, 0),
                cell(&|c| c.peak_rss_mb, 0),
                cell(&|c| c.peak_vm_mb, 0),
            ));
        }
    }
    std::fs::write(out.join("ops_summary.md"), &md)?;

    // --- csv export ---------------------------------------------------------
    let csv_path = out.join(format!("ops_compare_{stamp}.csv"));
    let mut w = csv::Writer::from_path(&csv_path)?;
    w.write_record([
        "op",
        "variant",
        "size_gb",
        "elapsed_s",
        "peak_cpu_pct",
        "mean_cpu_pct",
        "peak_rss_mb",
        "peak_vm_mb",
    ])?;
    let num = |v: Option<f64>| v.map_or(String::new(), |x| fmt_num((x * 1000.0).round() / 1000.0));
    for op in &ops {
        for variant in &variants {
            let group: Vec<&Cell> = cells
                .iter()
                .filter(|c| &c.op == op && &c.variant == variant)
                .collect();
            if group.is_empty() {
                continue;
            }
            w.write_record([
                op.clone(),
                variant.clone(),
                num(agg(&group, |c| c.size_gb)),
                num(agg(&group, |c| c.dur_s)),
                num(agg(&group, |c| c.peak_cpu)),
                num(agg(&group, |c| c.mean_cpu)),
                num(agg(&group, |c| c.peak_rss_mb)),
                num(agg(&group, |c| c.peak_vm_mb)),
            ])?;
        }
    }
    w.flush()?;

    println!(
        "wrote {} + ops_summary.md + {}",
        png.display(),
        csv_path.display()
    );
    Ok(())
}
