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

const fn is_leap(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// The first day of each representable year, with the leap flag in bit 0.
const YEAR_INFO: [i32; 10_000] = {
    let mut years = [0; 10_000];
    let mut year = 0;
    let mut first_day = -719_528; // 0000-01-01, in days since 1970-01-01
    while year < years.len() {
        let leap = is_leap(year as i32) as i32;
        years[year] = (first_day << 1) | leap;
        first_day += 365 + leap;
        year += 1;
    }
    years
};

/// Month number -> (days before that month, number of days in that month).
const MONTH_INFO: [[(i32, u32); 13]; 2] = {
    let length = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut months = [[(0, 0); 13]; 2];
    let mut leap = 0;
    while leap < 2 {
        let mut first_day = 0;
        let mut month = 1;
        while month <= 12 {
            let days = length[month] + if month == 2 { leap as u32 } else { 0 };
            months[leap][month] = (first_day, days);
            first_day += days as i32;
            month += 1;
        }
        leap += 1;
    }
    months
};

const ASCII_ZERO: u64 = u64::from_le_bytes([b'0'; 8]);
const YM_DIGITS: u64 = u64::from_le_bytes([255, 255, 255, 255, 0, 255, 255, 0]);
const YM_PUNCT: u64 = u64::from_le_bytes(*b"\0\0\0\0-\0\0-");
const DHM_DIGITS: u64 = u64::from_le_bytes([255, 255, 0, 255, 255, 0, 255, 255]);
const DHM_PUNCT: u64 = u64::from_le_bytes(*b"\0\0T\0\0:\0\0");

#[inline]
fn digit_errors(word: u64) -> u64 {
    // With all other lanes valid, a byte below '0' underflows on subtraction
    // or a byte above '9' sets the high bit on addition. The first invalid
    // byte cannot be hidden by a carry or borrow from the lower digit lanes.
    (word.wrapping_add(0x4646_4646_4646_4646) | word.wrapping_sub(ASCII_ZERO))
        & 0x8080_8080_8080_8080
}

#[inline]
fn two_digits(word: u64) -> u32 {
    ((word & 15) * 10 + ((word >> 8) & 15)) as u32
}

#[inline]
fn eight_digits(word: u64) -> i64 {
    // Pair neighboring digits at each step. Every lane is wide enough to
    // contain the pairs, groups of four, or final group of eight without carry.
    let digits = word & 0x0f0f_0f0f_0f0f_0f0f;
    let pairs = (digits.wrapping_mul(10) + (digits >> 8)) & 0x00ff_00ff_00ff_00ff;
    let quads = (pairs.wrapping_mul(100) + (pairs >> 16)) & 0x0000_ffff_0000_ffff;
    (quads.wrapping_mul(10_000).wrapping_add(quads >> 32) as u32) as i64
}

/// Parse a timestamp to epoch milliseconds. None when the value is bad.
#[inline]
pub fn parse(s: &[u8]) -> Option<i64> {
    match s.len() {
        20 | 25 if s[4] == b'-' => parse_calendar(s),
        13 => {
            let first = u64::from_le_bytes(s[..8].try_into().unwrap());
            let last = u64::from_le_bytes(s[5..].try_into().unwrap());
            if digit_errors(first) | digit_errors(last) != 0 {
                return None;
            }
            Some(eight_digits(first) * 100_000 + eight_digits(last) % 100_000)
        }
        _ => parse_epoch(s),
    }
}

#[cold]
fn parse_epoch(s: &[u8]) -> Option<i64> {
    if s.is_empty() || !s.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut value: i64 = 0;
    for &digit in s {
        let Some(next) = value
            .checked_mul(10)
            .and_then(|v| v.checked_add((digit - b'0') as i64))
        else {
            return Some(OVERFLOW);
        };
        value = next;
    }
    Some(value)
}

#[inline]
fn parse_calendar(s: &[u8]) -> Option<i64> {
    let ym = u64::from_le_bytes(s[..8].try_into().unwrap());
    let dhm = u64::from_le_bytes(s[8..16].try_into().unwrap());
    if (ym & !YM_DIGITS) != YM_PUNCT
        || (dhm & !DHM_DIGITS) != DHM_PUNCT
        || s[16] != b':'
        || !s[17].is_ascii_digit()
        || !s[18].is_ascii_digit()
        || digit_errors((ym & YM_DIGITS) | (ASCII_ZERO & !YM_DIGITS))
            | digit_errors((dhm & DHM_DIGITS) | (ASCII_ZERO & !DHM_DIGITS))
            != 0
    {
        return None;
    }

    let year = (two_digits(ym) * 100 + two_digits(ym >> 16)) as usize;
    let month = two_digits(ym >> 40) as usize;
    let day = two_digits(dhm).wrapping_sub(1);
    let hour = two_digits(dhm >> 24);
    let min = two_digits(dhm >> 48);
    let sec = ((s[17] - b'0') as u32) * 10 + (s[18] - b'0') as u32;

    let year_info = YEAR_INFO[year];
    let &(month_start, month_length) = MONTH_INFO[(year_info & 1) as usize].get(month)?;
    if day >= month_length || hour > 23 || min > 59 || sec > 59 {
        return None;
    }

    let days = (year_info >> 1) + month_start + day as i32;
    let base = days as i64 * 86_400_000 + ((hour * 60 + min) * 60 + sec) as i64 * 1000;
    if s.len() == 20 {
        return (s[19] == b'Z').then_some(base);
    }

    // F2 offset: the local time equals UTC + offset.
    let &[sign, a, b, b':', c, d] = &s[19..] else {
        return None;
    };
    if ![a, b, c, d].iter().all(u8::is_ascii_digit) {
        return None;
    }
    let hours = two_digits(u16::from_le_bytes([a, b]) as u64);
    let minutes = two_digits(u16::from_le_bytes([c, d]) as u64);
    if hours > 23 || minutes > 59 {
        return None;
    }
    let offset = (hours * 60 + minutes) as i64 * 60_000;
    match sign {
        b'+' => Some(base - offset),
        b'-' => Some(base + offset),
        _ => None,
    }
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

    #[test]
    fn calendar_eras_and_epoch_lengths() {
        assert_eq!(parse(b"0000-01-01T00:00:00Z"), Some(-62_167_219_200_000));
        assert_eq!(parse(b"1969-12-31T23:59:59Z"), Some(-1000));
        assert_eq!(parse(b"1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse(b"2000-02-29T00:00:00Z"), Some(951_782_400_000));
        assert_eq!(parse(b"1900-02-29T00:00:00Z"), None);
        assert_eq!(parse(b"9999-12-31T23:59:59Z"), Some(253_402_300_799_000));
        assert_eq!(parse(b"0000000000001"), Some(1));
        assert_eq!(parse(b"00000000000000000001"), Some(1));
        assert_eq!(parse(b"9999999999999"), Some(9_999_999_999_999));
        assert_eq!(parse(b"9999999999999999999999999"), Some(OVERFLOW));
        assert_eq!(parse(b"999999999999999999999999x"), None);
    }
}
