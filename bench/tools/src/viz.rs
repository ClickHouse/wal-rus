//! Shared chart palette, geometry, raster, and text helpers

use ab_glyph::{Font, FontRef, PxScale, ScaleFont, point};
use anyhow::Result;
use tiny_skia::{
    FillRule, LineCap, LineJoin, Paint, Path as SkPath, PathBuilder, Pixmap, Rect, Stroke,
    StrokeDash, Transform,
};

// ── palette ────────────────────────────────────────────────────────────────
#[derive(Clone, Copy)]
pub struct Rgb(pub u8, pub u8, pub u8);

pub const BG: Rgb = Rgb(0x29, 0x25, 0x22);
pub const FLOAT: Rgb = Rgb(0x34, 0x30, 0x2C);
pub const SEL: Rgb = Rgb(0x40, 0x3A, 0x36);
pub const MUTED: Rgb = Rgb(0xC1, 0xA7, 0x8E);
pub const FG: Rgb = Rgb(0xEC, 0xE1, 0xD7);

/// Per-tool color, falling back by index
pub fn variant_color(variant: &str) -> Option<Rgb> {
    match variant {
        "walrus" | "walrus-serial" => Some(Rgb(0xFA, 0xFF, 0x69)),
        "walg" => Some(Rgb(0xFC, 0x3F, 0x1D)),
        "pgbackrest" => Some(Rgb(0x27, 0x68, 0x9D)),
        _ => None,
    }
}

pub const FALLBACK: [Rgb; 6] = [
    Rgb(0xA3, 0xA9, 0xCE),
    Rgb(0x85, 0xB6, 0x95),
    Rgb(0xCF, 0x9B, 0xC2),
    Rgb(0x89, 0xB3, 0xB6),
    Rgb(0xE4, 0x9B, 0x5D),
    Rgb(0xB3, 0x80, 0xB0),
];

// ── shared canvas geometry ───────────────────────────────────────────────────
pub const W: u32 = 1180;
pub const HEADER_H: u32 = 48;

// ── per-variant style ────────────────────────────────────────────────────────
#[derive(Clone)]
pub struct Style {
    pub color: Rgb,
    pub dashed: bool,
    pub z: i32,
}

pub fn style_for(variant: &str, idx: usize) -> Style {
    Style {
        color: variant_color(variant).unwrap_or(FALLBACK[idx % FALLBACK.len()]),
        dashed: variant.ends_with("-serial"),
        z: if variant.starts_with("walrus") { 10 } else { 4 },
    }
}

/// Strip trailing `-b<digits>` replica suffix
pub fn variant_of(label: &str) -> String {
    if let Some(idx) = label.rfind("-b") {
        let suffix = &label[idx + 2..];
        if !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) {
            return label[..idx].to_string();
        }
    }
    label.to_string()
}

pub fn box_of(label: &str) -> String {
    if let Some(idx) = label.rfind("-b") {
        let suffix = &label[idx + 2..];
        if !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) {
            return suffix.to_string();
        }
    }
    "0".to_string()
}

/// Distinct variants, walrus first
pub fn variants_ordered(labels: &[String]) -> Vec<String> {
    let mut vs: Vec<String> = labels.iter().map(|l| variant_of(l)).collect();
    vs.sort();
    vs.dedup();
    vs.sort_by_key(|v| (!v.starts_with("walrus"), v.clone()));
    vs
}

pub fn median(xs: &mut [f64]) -> f64 {
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

// ── fonts ────────────────────────────────────────────────────────────────────
pub struct Fonts {
    pub regular: FontRef<'static>,
    pub bold: FontRef<'static>,
}

pub fn load_fonts() -> Result<Fonts> {
    let load = |b: &'static [u8]| {
        FontRef::try_from_slice(b).map_err(|_| anyhow::anyhow!("embedded font parse failed"))
    };
    Ok(Fonts {
        regular: load(dejavu::sans::regular())?,
        bold: load(dejavu::sans::bold())?,
    })
}

// ── shapes ─────────────────────────────────────────────────────────────────
pub fn paint_rgb(c: Rgb, a: u8) -> Paint<'static> {
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

pub fn stroke_poly(
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

pub fn fill_poly(pm: &mut Pixmap, pts: &[(f32, f32)], c: Rgb, a: u8) {
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

pub fn fill_rect(pm: &mut Pixmap, x: f32, y: f32, w: f32, h: f32, c: Rgb, a: u8) {
    if let Some(r) = Rect::from_xywh(x, y, w, h) {
        pm.fill_rect(r, &paint_rgb(c, a), Transform::identity(), None);
    }
}

pub fn stroke_rect(pm: &mut Pixmap, x: f32, y: f32, w: f32, h: f32, c: Rgb, width: f32) {
    stroke_poly(
        pm,
        &[(x, y), (x + w, y), (x + w, y + h), (x, y + h), (x, y)],
        c,
        255,
        width,
        None,
    );
}

/// Round axis ticks spanning [lo, hi]
pub fn nice_ticks(lo: f64, hi: f64, target: usize) -> Vec<f64> {
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

// ── text ─────────────────────────────────────────────────────────────────────
pub fn text_width(font: &FontRef<'static>, px: f32, text: &str) -> f32 {
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
            // Keep stronger coverage when glyph boxes overlap
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

/// Blit premultiplied src onto opaque dst
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

pub fn text_at(
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

pub fn text_center(
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

pub fn text_right(
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

pub fn text_left_mid(
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

pub fn text_vert(
    pm: &mut Pixmap,
    font: &FontRef<'static>,
    left: f32,
    cy: f32,
    px: f32,
    text: &str,
    c: Rgb,
) {
    if let Some(t) = text_pixmap(font, px, text, c) {
        // Rotated box is t.height() wide x t.width() tall
        let top = (cy - t.width() as f32 / 2.0).round() as i32;
        blit(pm, &t, left.round() as i32, top, true);
    }
}

// ── number formatting ─────────────────────────────────────────────────────────
pub fn fmt_num(v: f64) -> String {
    if v.is_finite() && v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        // Trim trailing zeros from six-place rendering
        let s = format!("{v:.6}");
        let s = s.trim_end_matches('0').trim_end_matches('.');
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variant_and_box() {
        assert_eq!(variant_of("walrus-serial-b0"), "walrus-serial");
        assert_eq!(box_of("walrus-serial-b0"), "0");
        assert_eq!(variant_of("walrus-r1"), "walrus-r1"); // not replica suffix
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
