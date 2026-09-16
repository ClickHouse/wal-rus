//! UTC instants, nanosecond resolution, no timezone database
//!
//! Wire format is RFC 3339 with a `Z` offset, matching Go's `time.Time` JSON so
//! walrus and wal-g can share buckets. Fractional seconds print in groups of
//! 3/6/9 digits, omitted when zero

use std::fmt;
use std::ops::{Add, Sub};
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const NANOS_PER_SEC: u32 = 1_000_000_000;
const SECS_PER_DAY: i64 = 86_400;

/// Instant on the UTC timeline. `nanos` is always in `0..NANOS_PER_SEC`, so
/// derived ordering is chronological
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Timestamp {
    secs: i64,
    nanos: u32,
}

impl Timestamp {
    pub const EPOCH: Self = Self { secs: 0, nanos: 0 };

    pub fn now() -> Self {
        SystemTime::now().into()
    }

    /// Nanoseconds past a whole second are carried into `secs`
    pub fn from_unix(secs: i64, nanos: u32) -> Self {
        Self {
            secs: secs.saturating_add((nanos / NANOS_PER_SEC) as i64),
            nanos: nanos % NANOS_PER_SEC,
        }
    }

    pub fn unix_secs(self) -> i64 {
        self.secs
    }

    /// `YYYYMMDDTHHMMSSZ`, the AWS sigv4 `x-amz-date`
    pub fn basic(self) -> String {
        let d = self.parts();
        format!(
            "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
            d.year, d.month, d.day, d.hour, d.minute, d.second
        )
    }

    /// `YYYYMMDD`, the AWS sigv4 credential scope date
    pub fn basic_date(self) -> String {
        let d = self.parts();
        format!("{:04}{:02}{:02}", d.year, d.month, d.day)
    }

    /// RFC 3339 truncated to whole seconds
    pub fn rfc3339_secs(self) -> String {
        let d = self.parts();
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
            d.year, d.month, d.day, d.hour, d.minute, d.second
        )
    }

    fn parts(self) -> Parts {
        let rem = self.secs.rem_euclid(SECS_PER_DAY);
        let (year, month, day) = civil_from_days(self.secs.div_euclid(SECS_PER_DAY));
        Parts {
            year,
            month,
            day,
            hour: (rem / 3600) as u32,
            minute: (rem / 60 % 60) as u32,
            second: (rem % 60) as u32,
        }
    }
}

struct Parts {
    year: i64,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
}

/// Days since 1970-01-01 to proleptic Gregorian `(year, month, day)`, Howard
/// Hinnant's `civil_from_days`
/// <http://howardhinnant.github.io/date_algorithms.html>
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Inverse of [`civil_from_days`]
pub fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = year - i64::from(month <= 2);
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if month > 2 { month - 3 } else { month + 9 } as i64;
    let doy = (153 * mp + 2) / 5 + day as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let d = self.parts();
        write!(
            f,
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            d.year, d.month, d.day, d.hour, d.minute, d.second
        )?;
        match self.nanos {
            0 => {}
            n if n % 1_000_000 == 0 => write!(f, ".{:03}", n / 1_000_000)?,
            n if n % 1_000 == 0 => write!(f, ".{:06}", n / 1_000)?,
            n => write!(f, ".{n:09}")?,
        }
        f.write_str("Z")
    }
}

impl fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Debug)]
pub struct ParseError;

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("not an RFC 3339 timestamp")
    }
}

impl std::error::Error for ParseError {}

fn two(b: &[u8], i: usize) -> Result<u32, ParseError> {
    let d = b.get(i..i + 2).ok_or(ParseError)?;
    if !d.iter().all(u8::is_ascii_digit) {
        return Err(ParseError);
    }
    Ok((d[0] - b'0') as u32 * 10 + (d[1] - b'0') as u32)
}

fn lit(b: &[u8], i: usize, c: u8) -> Result<(), ParseError> {
    (b.get(i) == Some(&c)).then_some(()).ok_or(ParseError)
}

impl FromStr for Timestamp {
    type Err = ParseError;

    /// Strict RFC 3339: the offset is mandatory, as in `chrono::parse_from_rfc3339`
    fn from_str(s: &str) -> Result<Self, ParseError> {
        let b = s.as_bytes();
        let year = two(b, 0)? as i64 * 100 + two(b, 2)? as i64;
        lit(b, 4, b'-')?;
        let month = two(b, 5)?;
        lit(b, 7, b'-')?;
        let day = two(b, 8)?;
        // RFC 3339 5.6 allows a space in place of `T`
        if !matches!(b.get(10), Some(b'T' | b't' | b' ')) {
            return Err(ParseError);
        }
        let hour = two(b, 11)?;
        lit(b, 13, b':')?;
        let minute = two(b, 14)?;
        lit(b, 16, b':')?;
        let second = two(b, 17)?;
        if month == 0
            || month > 12
            || day == 0
            || day > 31
            || hour > 23
            || minute > 59
            || second > 60
        {
            return Err(ParseError);
        }

        let mut i = 19;
        let mut nanos = 0;
        if b.get(i) == Some(&b'.') {
            i += 1;
            let start = i;
            let mut scale = NANOS_PER_SEC / 10;
            while b.get(i).is_some_and(u8::is_ascii_digit) {
                nanos += (b[i] - b'0') as u32 * scale;
                scale /= 10;
                i += 1;
            }
            if i == start {
                return Err(ParseError);
            }
        }

        let offset = match b.get(i) {
            Some(b'Z' | b'z') => {
                i += 1;
                0
            }
            Some(sign @ (b'+' | b'-')) => {
                let sign = if *sign == b'-' { -1 } else { 1 };
                let (oh, om) = (two(b, i + 1)?, two(b, i + 4)?);
                lit(b, i + 3, b':')?;
                if oh > 23 || om > 59 {
                    return Err(ParseError);
                }
                i += 6;
                sign * (oh as i64 * 3600 + om as i64 * 60)
            }
            _ => return Err(ParseError),
        };
        if i != b.len() {
            return Err(ParseError);
        }

        let secs = days_from_civil(year, month, day) * SECS_PER_DAY
            + hour as i64 * 3600
            + minute as i64 * 60
            + second as i64
            - offset;
        Ok(Self { secs, nanos })
    }
}

impl From<SystemTime> for Timestamp {
    fn from(t: SystemTime) -> Self {
        match t.duration_since(UNIX_EPOCH) {
            Ok(d) => Self {
                secs: d.as_secs() as i64,
                nanos: d.subsec_nanos(),
            },
            Err(e) => {
                let d = e.duration();
                let borrow = i64::from(d.subsec_nanos() != 0);
                Self {
                    secs: -(d.as_secs() as i64) - borrow,
                    nanos: (borrow as u32) * (NANOS_PER_SEC - d.subsec_nanos()),
                }
            }
        }
    }
}

impl From<Timestamp> for SystemTime {
    fn from(t: Timestamp) -> Self {
        let whole = Duration::from_secs(t.secs.unsigned_abs());
        let frac = Duration::from_nanos(t.nanos as u64);
        if t.secs >= 0 {
            UNIX_EPOCH + whole + frac
        } else {
            UNIX_EPOCH - whole + frac
        }
    }
}

impl Add<Duration> for Timestamp {
    type Output = Self;

    fn add(self, d: Duration) -> Self {
        let nanos = self.nanos + d.subsec_nanos();
        Self {
            secs: self
                .secs
                .saturating_add(d.as_secs() as i64)
                .saturating_add((nanos / NANOS_PER_SEC) as i64),
            nanos: nanos % NANOS_PER_SEC,
        }
    }
}

impl Sub<Duration> for Timestamp {
    type Output = Self;

    fn sub(self, d: Duration) -> Self {
        let borrow = self.nanos < d.subsec_nanos();
        Self {
            secs: self
                .secs
                .saturating_sub(d.as_secs() as i64)
                .saturating_sub(i64::from(borrow)),
            nanos: self.nanos + (borrow as u32) * NANOS_PER_SEC - d.subsec_nanos(),
        }
    }
}

impl Serialize for Timestamp {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = Timestamp;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an RFC 3339 timestamp")
            }

            fn visit_str<E: de::Error>(self, s: &str) -> Result<Timestamp, E> {
                s.parse()
                    .map_err(|_| E::invalid_value(de::Unexpected::Str(s), &self))
            }
        }
        d.deserialize_str(V)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_match_rfc3339_with_auto_fraction() {
        let cases = [
            (0, 0, "1970-01-01T00:00:00Z"),
            (1_700_000_000, 0, "2023-11-14T22:13:20Z"),
            (1_700_000_000, 123_000_000, "2023-11-14T22:13:20.123Z"),
            (1_700_000_000, 123_456_000, "2023-11-14T22:13:20.123456Z"),
            (1_700_000_000, 1, "2023-11-14T22:13:20.000000001Z"),
            (-1, 0, "1969-12-31T23:59:59Z"),
            (951_782_400, 0, "2000-02-29T00:00:00Z"),
            (4_107_542_400, 0, "2100-03-01T00:00:00Z"),
        ];
        for (secs, nanos, want) in cases {
            let t = Timestamp::from_unix(secs, nanos);
            assert_eq!(t.to_string(), want);
            assert_eq!(want.parse::<Timestamp>().unwrap(), t);
        }
    }

    #[test]
    fn sigv4_and_second_precision_formats() {
        let t = Timestamp::from_unix(1_440_938_160, 500);
        assert_eq!(t.basic(), "20150830T123600Z");
        assert_eq!(t.basic_date(), "20150830");
        assert_eq!(t.rfc3339_secs(), "2015-08-30T12:36:00Z");
    }

    #[test]
    fn parses_offsets_and_fraction_overflow() {
        let z = "2023-11-14T22:13:20Z".parse::<Timestamp>().unwrap();
        for s in [
            "2023-11-14t22:13:20z",
            "2023-11-14 22:13:20Z",
            "2023-11-14T23:13:20+01:00",
            "2023-11-14T16:43:20-05:30",
        ] {
            assert_eq!(s.parse::<Timestamp>().unwrap(), z, "{s}");
        }
        // digits past nanosecond resolution are dropped
        let t = "2023-11-14T22:13:20.1234567891Z"
            .parse::<Timestamp>()
            .unwrap();
        assert_eq!(t, Timestamp::from_unix(1_700_000_000, 123_456_789));
    }

    #[test]
    fn rejects_malformed() {
        for s in [
            "",
            "2023-11-14T22:13:20",
            "2023-13-14T22:13:20Z",
            "2023-11-14T25:13:20Z",
            "2023-11-14T22:13:20.Z",
            "2023-11-14T22:13:20Zx",
            "2023-11-14T22:13:20+0100",
            "not a time",
        ] {
            assert!(s.parse::<Timestamp>().is_err(), "{s}");
        }
    }

    #[test]
    fn json_round_trips_and_orders() {
        let t = Timestamp::from_unix(1_700_000_000, 123_000_000);
        assert_eq!(
            serde_json::to_string(&t).unwrap(),
            "\"2023-11-14T22:13:20.123Z\""
        );
        assert_eq!(
            serde_json::from_str::<Timestamp>("\"2023-11-14T22:13:20.123Z\"").unwrap(),
            t
        );
        assert!(serde_json::from_str::<Timestamp>("\"garbage\"").is_err());
        assert!(Timestamp::from_unix(1, 0) > Timestamp::from_unix(0, 999_999_999));
        assert_eq!(Timestamp::default(), Timestamp::EPOCH);
    }

    #[test]
    fn duration_arithmetic_borrows_across_seconds() {
        let t = Timestamp::from_unix(10, 250_000_000);
        assert_eq!(
            t + Duration::from_millis(900),
            Timestamp::from_unix(11, 150_000_000)
        );
        assert_eq!(
            t - Duration::from_millis(900),
            Timestamp::from_unix(9, 350_000_000)
        );
        assert_eq!(
            Timestamp::from_unix(0, 0) - Duration::from_nanos(1),
            Timestamp::from_unix(-1, 999_999_999)
        );
    }

    #[test]
    fn system_time_round_trips_before_and_after_epoch() {
        for (secs, nanos) in [
            (-1i64, 0u32),
            (-1, 500),
            (0, 0),
            (0, 1),
            (1_700_000_000, 999_999_999),
        ] {
            let t = Timestamp::from_unix(secs, nanos);
            assert_eq!(Timestamp::from(SystemTime::from(t)), t, "{secs}.{nanos}");
        }
    }
}
