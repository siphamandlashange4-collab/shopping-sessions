//! Timestamp parsing per rules.md.
//!
//! F1 `2026-10-03T23:58:10Z`        (UTC, whole seconds)
//! F2 `2026-10-03T19:58:10-04:00`   (offset +HH:MM / -HH:MM, subtract to get UTC)
//! F3 `1791071890000`               (digits only, epoch ms, leading zeros allowed)
//!
//! Anything else is bad: empty, fractional seconds, lowercase `z`/`t`, a space
//! instead of `T`, no zone, a non-digit in F3, an impossible calendar value, an
//! offset out of range or without its colon. `-00:00` is UTC. An F3 value that
//! is very short or too long for i64 is not malformed, only outside the window.

/// Sentinel used for an F3 value that overflows i64: far outside any window.
const OVERFLOW: i64 = i64::MAX;

#[inline]
fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

#[inline]
fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Howard Hinnant's days_from_civil: days since 1970-01-01.
#[inline]
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 } as i64;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline]
fn d2(s: &[u8]) -> Option<u32> {
    if s.len() == 2 && s[0].is_ascii_digit() && s[1].is_ascii_digit() {
        Some(((s[0] - b'0') as u32) * 10 + (s[1] - b'0') as u32)
    } else {
        None
    }
}

#[inline]
fn d4(s: &[u8]) -> Option<i64> {
    if s.len() == 4 && s.iter().all(|c| c.is_ascii_digit()) {
        Some(
            ((s[0] - b'0') as i64) * 1000
                + ((s[1] - b'0') as i64) * 100
                + ((s[2] - b'0') as i64) * 10
                + (s[3] - b'0') as i64,
        )
    } else {
        None
    }
}

/// Parse a timestamp to epoch milliseconds. None when the value is bad.
#[inline]
pub fn parse(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }

    // F3: digits only.
    if s[0].is_ascii_digit() && s.iter().all(|c| c.is_ascii_digit()) {
        let mut v: i64 = 0;
        for &c in s {
            match v
                .checked_mul(10)
                .and_then(|x| x.checked_add((c - b'0') as i64))
            {
                Some(n) => v = n,
                None => return Some(OVERFLOW), // too long for i64: valid, far outside window
            }
        }
        return Some(v);
    }

    // F1 (len 20) or F2 (len 25). Both share the first 19 bytes.
    let n = s.len();
    if n != 20 && n != 25 {
        return None;
    }
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = d4(&s[0..4])?;
    let month = d2(&s[5..7])?;
    let day = d2(&s[8..10])?;
    let hour = d2(&s[11..13])?;
    let min = d2(&s[14..16])?;
    let sec = d2(&s[17..19])?;

    if month < 1 || month > 12 {
        return None;
    }
    if day < 1 || day > days_in_month(year, month) {
        return None;
    }
    if hour > 23 || min > 59 || sec > 59 {
        return None;
    }

    let base =
        days_from_civil(year, month, day) * 86_400_000 + (hour as i64) * 3_600_000 + (min as i64) * 60_000 + (sec as i64) * 1000;

    if n == 20 {
        if s[19] != b'Z' {
            return None;
        }
        return Some(base);
    }

    // F2 offset: byte 19 is '+' or '-', then HH:MM.
    let sign = match s[19] {
        b'+' => 1i64,
        b'-' => -1i64,
        _ => return None,
    };
    if s[22] != b':' {
        return None;
    }
    let oh = d2(&s[20..22])?;
    let om = d2(&s[23..25])?;
    if oh > 23 || om > 59 {
        return None;
    }
    let off = sign * ((oh as i64) * 3_600_000 + (om as i64) * 60_000);
    // The local time equals UTC + offset, so subtract the offset.
    Some(base - off)
}

/// Floor division of epoch ms to a day number (Parquet DATE32).
#[inline]
pub fn day_of(ms: i64) -> i32 {
    ms.div_euclid(86_400_000) as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forms_agree() {
        let a = parse(b"2026-10-03T23:58:10Z").unwrap();
        let b = parse(b"2026-10-03T19:58:10-04:00").unwrap();
        assert_eq!(a, b);
        assert_eq!(parse(b"1791071890000").unwrap(), 1791071890000);
        assert_eq!(parse(b"0001791071890000").unwrap(), 1791071890000);
        assert_eq!(parse(b"-00:00"), None);
        assert_eq!(parse(b"2026-10-03T23:58:10z"), None);
        assert_eq!(parse(b"2026-10-03t23:58:10Z"), None);
        assert_eq!(parse(b"2026-10-03 23:58:10Z"), None);
        assert_eq!(parse(b"2026-10-03T23:58:10"), None);
        assert_eq!(parse(b"2026-10-03T23:58:10.5Z"), None);
        assert_eq!(parse(b"2026-13-03T23:58:10Z"), None);
        assert_eq!(parse(b"2026-02-30T23:58:10Z"), None);
        assert_eq!(parse(b"2027-02-29T00:00:00Z"), None);
        assert!(parse(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(parse(b"2026-10-03T24:00:00Z"), None);
        assert_eq!(parse(b"2026-10-03T23:58:60Z"), None);
        assert_eq!(parse(b"2026-10-03T23:58:10+24:00"), None);
        assert_eq!(parse(b"2026-10-03T23:58:10+05:60"), None);
        assert_eq!(parse(b"2026-10-03T23:58:10+0200"), None);
        assert_eq!(parse(b"1791071890000.0"), None);
        assert_eq!(parse(b"-5"), None);
        // -00:00 as a zone is UTC
        assert_eq!(
            parse(b"2026-10-03T23:58:10-00:00").unwrap(),
            parse(b"2026-10-03T23:58:10Z").unwrap()
        );
        assert_eq!(parse(b"86400000").unwrap(), 86_400_000);
    }

    #[test]
    fn day_floor() {
        assert_eq!(day_of(0), 0);
        assert_eq!(day_of(86_399_999), 0);
        assert_eq!(day_of(86_400_000), 1);
    }
}
