//! Shared rendering for bench charts

use std::time::{SystemTime, UNIX_EPOCH};

pub mod viz;

/// `YYYYMMDDTHHMMSSZ` for result-directory names
pub fn utc_stamp() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let (days, rem) = (secs / 86_400, secs % 86_400);

    // Howard Hinnant's `civil_from_days`, epoch shifted to 0000-03-01
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z % 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + u64::from(month <= 2);

    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        year,
        month,
        day,
        rem / 3600,
        rem / 60 % 60,
        rem % 60
    )
}
