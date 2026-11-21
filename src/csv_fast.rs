use super::Rows;
#[cfg(target_arch = "x86_64")]
use super::split_n;

/// Visits valid rows. Return false from the visitor to stop scanning.
pub fn for_each<'a, const N: usize>(
    bytes: &'a [u8],
    mut consume: impl FnMut([&'a [u8]; N]) -> bool,
) -> bool {
    assert!(N > 0);
    #[cfg(target_arch = "x86_64")]
    if N <= 32
        && std::arch::is_x86_feature_detected!("avx2")
        && std::arch::is_x86_feature_detected!("bmi1")
        && std::arch::is_x86_feature_detected!("bmi2")
        && std::arch::is_x86_feature_detected!("popcnt")
    {
        // All features used by the row decoder were checked above.
        return unsafe { for_each_x86(bytes, consume) };
    }
    Rows::<N>::raw(bytes).all(&mut consume)
}

#[cfg(target_arch = "x86_64")]
#[derive(Clone, Copy)]
struct Marks {
    newline: u64,
    comma: u64,
    quote: u64,
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt")]
#[inline]
unsafe fn marks_64(ptr: *const u8) -> Marks {
    use std::arch::x86_64::*;

    #[inline]
    fn join(a: i32, b: i32) -> u64 {
        a as u32 as u64 | (b as u32 as u64) << 32
    }

    let a = _mm256_loadu_si256(ptr.cast());
    let b = _mm256_loadu_si256(ptr.add(32).cast());
    let newline = _mm256_set1_epi8(b'\n' as i8);
    let comma = _mm256_set1_epi8(b',' as i8);
    let quote = _mm256_set1_epi8(b'"' as i8);
    Marks {
        newline: join(
            _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, newline)),
            _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, newline)),
        ),
        comma: join(
            _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, comma)),
            _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, comma)),
        ),
        quote: join(
            _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, quote)),
            _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, quote)),
        ),
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt")]
#[inline]
unsafe fn compress_bits(value: u128, mask: u128) -> u64 {
    use std::arch::x86_64::_pext_u64;

    let low = mask as u64;
    let high = (mask >> 64) as u64;
    _pext_u64(value as u64, low) | (_pext_u64((value >> 64) as u64, high) << low.count_ones())
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt")]
#[inline]
unsafe fn decode<const N: usize>(
    line: &[u8],
    comma: u128,
    quote: u128,
) -> Option<[&[u8]; N]> {
    let terminal = 1u128 << line.len();
    let comma = comma & (terminal - 1);
    let ends = terminal | comma;
    if ends.count_ones() != N as u32 {
        return None;
    }

    let quote = quote & (terminal - 1);
    let mut quoted = 0;
    if quote != 0 {
        let starts = (comma << 1) | 1;
        let last = ends >> 1;
        // A quote may touch only the first or last byte of a field. A quote
        // touching both is a one-byte field and can't have a matching quote.
        if quote & (!(starts | last) | (starts & last)) != 0 {
            return None;
        }
        // Gather opening and closing quote flags in field order.
        quoted = compress_bits(quote, starts);
        if quoted != compress_bits(quote << 1, ends) {
            return None;
        }
    }

    let mut low = ends as u64;
    let mut high = (ends >> 64) as u64;
    let mut start = 0;
    let mut fields = [b"" as &[u8]; N];
    for field in &mut fields {
        let end = if low != 0 {
            let idx = low.trailing_zeros() as usize;
            low &= low - 1;
            idx
        } else {
            let idx = high.trailing_zeros() as usize + 64;
            high &= high - 1;
            idx
        };
        let trim = (quoted & 1) as usize;
        quoted >>= 1;
        // The exact field count and paired quotes were checked above. Each
        // range ends before its comma or at the length of the line.
        *field = line.get_unchecked(start + trim..end - trim);
        start = end + 1;
    }
    Some(fields)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt")]
unsafe fn for_each_x86<'a, const N: usize>(
    mut bytes: &'a [u8],
    mut consume: impl FnMut([&'a [u8]; N]) -> bool,
) -> bool {
    while bytes.len() >= 128 {
        // At least two 64-byte probes are available. The first set bit in a
        // newline mask points to an in-bounds end of this row.
        let low = marks_64(bytes.as_ptr());
        let (len, comma, quote) = if low.newline != 0 {
            (low.newline.trailing_zeros() as usize, low.comma as u128, low.quote as u128)
        } else {
            let high = marks_64(bytes.as_ptr().add(64));
            if high.newline == 0 {
                let Some(len) = memchr::memchr(b'\n', &bytes[128..]).map(|n| n + 128) else {
                    return split_n(bytes).is_none_or(&mut consume);
                };
                if let Some(fields) = split_n(&bytes[..len]) {
                    if !consume(fields) {
                        return false;
                    }
                }
                bytes = &bytes[len + 1..];
                continue;
            }
            (
                high.newline.trailing_zeros() as usize + 64,
                low.comma as u128 | (high.comma as u128) << 64,
                low.quote as u128 | (high.quote as u128) << 64,
            )
        };
        let line = bytes.get_unchecked(..len);
        if let Some(fields) = decode(line, comma, quote) {
            if !consume(fields) {
                return false;
            }
        }
        bytes = bytes.get_unchecked(len + 1..);
    }
    Rows::<N>::raw(bytes).all(consume)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fast_rows_match_on_quotes_and_lengths() {
        for prefix in 0..128 {
            let text = format!(
                "{},\"valid\",\"\"\n{},b,c\nx,\"b\"c,d\nx,\"\",y\nx,y,z,extra\nshort,z,",
                "x".repeat(prefix),
                "y".repeat(153),
            );
            let expected: Vec<_> = Rows::<3>::raw(text.as_bytes()).collect();
            let mut received = Vec::new();
            assert!(for_each::<3>(text.as_bytes(), |row| {
                received.push(row);
                true
            }));
            assert_eq!(received, expected);

            let mut rows = 0;
            assert!(!for_each::<3>(text.as_bytes(), |_| {
                rows += 1;
                false
            }));
            assert_eq!(rows, 1);
        }
    }
}
