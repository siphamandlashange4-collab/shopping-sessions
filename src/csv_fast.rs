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
        if std::arch::is_x86_feature_detected!("avx512bw") {
            // The AVX-512BW path also uses the row decoder's features.
            return unsafe { for_each_avx512(bytes, consume) };
        }
        return unsafe { for_each_x86(bytes, consume) };
    }
    Rows::<N>::raw(bytes).all(&mut consume)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512bw")]
#[inline]
unsafe fn marks_64_avx512(ptr: *const u8) -> Marks {
    use std::arch::x86_64::*;

    let block = _mm512_loadu_si512(ptr.cast());
    Marks {
        newline: _mm512_cmpeq_epi8_mask(block, _mm512_set1_epi8(b'\n' as i8)),
        comma: _mm512_cmpeq_epi8_mask(block, _mm512_set1_epi8(b',' as i8)),
        quote: _mm512_cmpeq_epi8_mask(block, _mm512_set1_epi8(b'"' as i8)),
    }
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
    bytes: &'a [u8],
    consume: impl FnMut([&'a [u8]; N]) -> bool,
) -> bool {
    scan_rows(bytes, consume, |ptr| marks_64(ptr))
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt,avx512bw")]
unsafe fn for_each_avx512<'a, const N: usize>(
    bytes: &'a [u8],
    consume: impl FnMut([&'a [u8]; N]) -> bool,
) -> bool {
    scan_rows(bytes, consume, |ptr| marks_64_avx512(ptr))
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn scan_rows<'a, const N: usize>(
    bytes: &'a [u8],
    mut consume: impl FnMut([&'a [u8]; N]) -> bool,
    marks: impl Fn(*const u8) -> Marks,
) -> bool {
    let mut start = 0;
    let mut prior_comma = 0u128;
    let mut prior_quote = 0u128;
    for (index, block) in bytes.chunks_exact(64).enumerate() {
        let base = index * 64;
        let Marks { mut newline, comma, quote } = marks(block.as_ptr());
        let carried = base - start;
        if newline != 0 {
            let first = newline.trailing_zeros() as usize;
            let end = base + first;
            let line = bytes.get_unchecked(start..end);
            let fields = if line.len() < 128 {
                decode(
                    line,
                    prior_comma | ((comma as u128) << carried),
                    prior_quote | ((quote as u128) << carried),
                )
            } else {
                split_n(line)
            };
            if let Some(fields) = fields {
                if !consume(fields) {
                    return false;
                }
            }
            start = end + 1;
            newline &= newline - 1;
            while newline != 0 {
                let end = base + newline.trailing_zeros() as usize;
                let shift = start - base;
                let line = bytes.get_unchecked(start..end);
                if let Some(fields) = decode(line, (comma >> shift) as u128, (quote >> shift) as u128) {
                    if !consume(fields) {
                        return false;
                    }
                }
                start = end + 1;
                newline &= newline - 1;
            }
            let shift = start - base;
            prior_comma = comma.checked_shr(shift as u32).unwrap_or(0) as u128;
            prior_quote = quote.checked_shr(shift as u32).unwrap_or(0) as u128;
        } else if carried < 128 {
            prior_comma |= (comma as u128) << carried;
            prior_quote |= (quote as u128) << carried;
        }
    }
    Rows::<N>::raw(bytes.get_unchecked(start..)).all(consume)
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
