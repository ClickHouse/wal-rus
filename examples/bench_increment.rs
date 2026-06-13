//! Micro-benchmark: wal-g `wi1` vs PG17 native INCREMENTAL
//!
//! For a synthetic paged file of size `FILE_BLOCKS * BLCKSZ`, with
//! `DIRTY_BLOCKS` randomly-selected dirty pages, measure:
//!   - encode (header + page bodies into a Vec<u8>)
//!   - apply (apply_increment_in_place onto a same-size in-memory target)
//!   - on-disk header overhead
//!
//! Run with: `cargo run --release --example bench_increment`

use std::io::{Cursor, Write};
use std::time::Instant;

use walross::pg::backup::delta::PG_PAGE_SIZE;
use walross::pg::backup::increment::{
    Format, apply_increment_in_place, write_increment_header, write_native_increment_header,
};

const ITERS: usize = 50;

/// Deterministic "pseudo-random" block picker: stride through the file
fn pick_blocks(file_blocks: u32, count: u32) -> Vec<u32> {
    assert!(count <= file_blocks);
    if count == 0 {
        return Vec::new();
    }
    let stride = (file_blocks / count).max(1);
    let mut out: Vec<u32> = (0..count).map(|i| (i * stride) % file_blocks).collect();
    out.sort();
    out.dedup();
    while (out.len() as u32) < count {
        // pad with extra blocks if stride collapsed dups
        let next = (out.last().copied().unwrap_or(0) + 1) % file_blocks;
        if out.contains(&next) {
            break;
        }
        out.push(next);
        out.sort();
    }
    out
}

fn encode_wi1(file_size: u64, blocks: &[u32], page_template: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16 + blocks.len() * 4 + blocks.len() * PG_PAGE_SIZE as usize);
    write_increment_header(&mut buf, file_size, blocks).unwrap();
    for _ in blocks {
        buf.write_all(page_template).unwrap();
    }
    buf
}

fn encode_native(file_blocks: u32, blocks: &[u32], page_template: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16384 + blocks.len() * PG_PAGE_SIZE as usize);
    write_native_increment_header(&mut buf, file_blocks, blocks).unwrap();
    for _ in blocks {
        buf.write_all(page_template).unwrap();
    }
    buf
}

fn bench<F: FnMut()>(label: &str, n: usize, mut f: F) -> u128 {
    // Warmup
    for _ in 0..3 {
        f();
    }
    let t0 = Instant::now();
    for _ in 0..n {
        f();
    }
    let elapsed = t0.elapsed();
    let per = elapsed / n as u32;
    println!(
        "  {label:<40} total={:>9.2?}  per={:>9.2?}  ({n} iters)",
        elapsed, per
    );
    elapsed.as_nanos() / n as u128
}

fn run_case(file_blocks: u32, dirty: u32) {
    println!(
        "\n─── file={} MiB ({} blocks), dirty={} pages ({:.1}% density) ───",
        (file_blocks as u64 * PG_PAGE_SIZE) / (1 << 20),
        file_blocks,
        dirty,
        100.0 * dirty as f64 / file_blocks as f64,
    );

    let blocks = pick_blocks(file_blocks, dirty);
    let page = vec![0xAB; PG_PAGE_SIZE as usize];
    let file_size = file_blocks as u64 * PG_PAGE_SIZE;

    let wi1 = encode_wi1(file_size, &blocks, &page);
    let native = encode_native(file_blocks, &blocks, &page);
    println!(
        "  wire-size:   wi1={} bytes  native={} bytes  diff={}",
        wi1.len(),
        native.len(),
        native.len() as i64 - wi1.len() as i64,
    );
    let wi1_header = 16 + blocks.len() * 4;
    let native_header_unpadded = 12 + blocks.len() * 4;
    let native_header_padded = native.len() - blocks.len() * PG_PAGE_SIZE as usize;
    println!(
        "  header:      wi1={} bytes  native_raw={} bytes  native_padded={} bytes",
        wi1_header, native_header_unpadded, native_header_padded,
    );

    // Encode benchmarks
    let wi1_ns = bench("encode wi1", ITERS, || {
        let _ = encode_wi1(file_size, &blocks, &page);
    });
    let native_ns = bench("encode native", ITERS, || {
        let _ = encode_native(file_blocks, &blocks, &page);
    });

    // Apply benchmarks (fresh target each iter, so apply is comparable)
    let target_template = vec![0u8; file_size as usize];
    let apply_wi1_ns = bench("apply wi1", ITERS, || {
        let mut target = Cursor::new(target_template.clone());
        let mut inc = Cursor::new(&wi1);
        let (sz, n, fmt) = apply_increment_in_place(&mut inc, &mut target).unwrap();
        debug_assert_eq!(sz, file_size);
        debug_assert_eq!(n, blocks.len());
        debug_assert_eq!(fmt, Format::Wi1);
    });
    let apply_native_ns = bench("apply native", ITERS, || {
        let mut target = Cursor::new(target_template.clone());
        let mut inc = Cursor::new(&native);
        let (_, _, fmt) = apply_increment_in_place(&mut inc, &mut target).unwrap();
        debug_assert_eq!(fmt, Format::Native);
    });

    let total_payload_mib = (blocks.len() as u64 * PG_PAGE_SIZE) as f64 / (1 << 20) as f64;
    let mib_per_s = |ns: u128| {
        if ns == 0 {
            f64::INFINITY
        } else {
            total_payload_mib * 1_000_000_000.0 / ns as f64
        }
    };
    println!(
        "  encode throughput: wi1={:>7.1} MiB/s  native={:>7.1} MiB/s",
        mib_per_s(wi1_ns),
        mib_per_s(native_ns),
    );
    println!(
        "  apply throughput:  wi1={:>7.1} MiB/s  native={:>7.1} MiB/s",
        mib_per_s(apply_wi1_ns),
        mib_per_s(apply_native_ns),
    );
}

fn main() {
    println!("walross increment format micro-benchmark");
    println!("BLCKSZ = {} bytes; ITERS = {}", PG_PAGE_SIZE, ITERS);

    // 4 MiB / sparse delta (typical OLTP)
    run_case(512, 5);
    // 64 MiB / 5% dirty (moderate write workload)
    run_case(8192, 410);
    // 1 GiB rel segment / 1% dirty
    run_case(131_072, 1310);
    // 1 GiB / 50% dirty (worst case before falling back to full)
    run_case(131_072, 65_536);
}
