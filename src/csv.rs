//! CSV dialect per rules.md:
//! comma separated, `\n` only line endings, `\r` is an ordinary character,
//! a field may be wrapped in double quotes (quotes are syntax, not value),
//! no value ever contains a comma / quote / newline, so every comma separates
//! fields. Wrong field count, or a quote anywhere except around a whole field,
//! is malformed. An empty line is malformed.

/// Unquote one field. Returns None if the field is malformed.
#[inline]
pub fn unquote(f: &[u8]) -> Option<&[u8]> {
    match memchr::memchr(b'"', f) {
        None => Some(f),
        Some(_) => strip_quotes(f),
    }
}

#[inline]
fn strip_quotes(field: &[u8]) -> Option<&[u8]> {
    let [b'"', inner @ .., b'"'] = field else {
        return None;
    };
    memchr::memchr(b'"', inner).is_none().then_some(inner)
}

/// Split a line into exactly N fields and unquote each.
/// Returns None when the field count is wrong or any field is malformed.
/// An empty line yields one empty field, so it fails the count for N > 1.
#[inline]
pub fn split_n<'a, const N: usize>(line: &'a [u8]) -> Option<[&'a [u8]; N]> {
    let mut out: [&[u8]; N] = [b""; N];
    let mut start = 0usize;
    let mut i = 0usize;
    let mut pos = 0usize;
    while let Some(c) = memchr::memchr(b',', &line[pos..]) {
        let at = pos + c;
        if i >= N {
            return None; // too many fields
        }
        out[i] = unquote(&line[start..at])?;
        i += 1;
        start = at + 1;
        pos = at + 1;
    }
    if i != N - 1 {
        return None; // too few (or, with i >= N above, too many) fields
    }
    out[i] = unquote(&line[start..])?;
    Some(out)
}

/// Parse rows directly from a line-aligned buffer without a header. A single
/// scan finds the commas, newlines and quotes for several fields at a time.
pub struct Rows<'a, const N: usize> {
    buf: &'a [u8],
    field_start: usize,
    block_start: usize,
    block_end: usize,
    delimiters: u32,
    quotes: u32,
}

impl<'a, const N: usize> Rows<'a, N> {
    pub fn raw(buf: &'a [u8]) -> Self {
        assert!(N > 0);
        Self {
            buf,
            field_start: 0,
            block_start: 0,
            block_end: 0,
            delimiters: 0,
            quotes: 0,
        }
    }

    #[inline]
    fn next_field(&mut self) -> (Option<&'a [u8]>, bool) {
        let mut has_quote = false;
        while self.delimiters == 0 {
            has_quote |= self.quotes != 0;
            if self.block_end == self.buf.len() {
                let field = &self.buf[self.field_start..];
                self.field_start = self.buf.len();
                return (
                    if has_quote { strip_quotes(field) } else { Some(field) },
                    true,
                );
            }
            self.block_start = self.block_end;
            let block = &self.buf[self.block_start..];
            (self.delimiters, self.quotes) = scan_block(block);
            self.block_end += block.len().min(32);
        }

        let bit = self.delimiters.trailing_zeros();
        let preceding = (1u32 << bit) - 1;
        self.delimiters &= self.delimiters - 1;
        has_quote |= self.quotes & preceding != 0;
        self.quotes &= !preceding;

        let end = self.block_start + bit as usize;
        let field = &self.buf[self.field_start..end];
        self.field_start = end + 1;
        (
            if has_quote { strip_quotes(field) } else { Some(field) },
            self.buf[end] == b'\n',
        )
    }
}

impl<'a, const N: usize> Iterator for Rows<'a, N> {
    type Item = [&'a [u8]; N];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        'row: while self.field_start < self.buf.len() {
            let mut fields = [b"" as &[u8]; N];
            for (i, slot) in fields.iter_mut().enumerate() {
                let (field, mut last) = self.next_field();
                match field {
                    Some(field) if last == (i + 1 == N) => *slot = field,
                    _ => {
                        while !last {
                            (_, last) = self.next_field();
                        }
                        continue 'row;
                    }
                }
            }
            return Some(fields);
        }
        None
    }
}

#[inline]
fn scan_block(block: &[u8]) -> (u32, u32) {
    #[cfg(target_arch = "x86_64")]
    if block.len() >= 32 {
        use std::arch::x86_64::*;

        // SSE2 is part of the x86-64 baseline. Both loads stay in `block`.
        unsafe {
            let comma = _mm_set1_epi8(b',' as i8);
            let newline = _mm_set1_epi8(b'\n' as i8);
            let quote = _mm_set1_epi8(b'"' as i8);
            let ptr = block.as_ptr();
            let a = _mm_loadu_si128(ptr.cast());
            let b = _mm_loadu_si128(ptr.add(16).cast());
            let a_delim = _mm_or_si128(_mm_cmpeq_epi8(a, comma), _mm_cmpeq_epi8(a, newline));
            let b_delim = _mm_or_si128(_mm_cmpeq_epi8(b, comma), _mm_cmpeq_epi8(b, newline));
            let delimiters = _mm_movemask_epi8(a_delim) as u32
                | ((_mm_movemask_epi8(b_delim) as u32) << 16);
            let quotes = _mm_movemask_epi8(_mm_cmpeq_epi8(a, quote)) as u32
                | ((_mm_movemask_epi8(_mm_cmpeq_epi8(b, quote)) as u32) << 16);
            return (delimiters, quotes);
        }
    }

    let mut delimiters = 0;
    let mut quotes = 0;
    for (i, byte) in block.iter().take(32).copied().enumerate() {
        delimiters |= (matches!(byte, b',' | b'\n') as u32) << i;
        quotes |= ((byte == b'"') as u32) << i;
    }
    (delimiters, quotes)
}

/// Iterate the data lines of a buffer, skipping the header and the final
/// empty fragment after a trailing newline. Only `\n` ends a line.
pub struct Lines<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Lines<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        // Drop the header row.
        let pos = match memchr::memchr(b'\n', buf) {
            Some(i) => i + 1,
            None => buf.len(),
        };
        Lines { buf, pos }
    }
    /// Lines over a slice that has no header.
    #[cfg(test)]
    pub fn raw(buf: &'a [u8]) -> Self {
        Lines { buf, pos: 0 }
    }
}

impl<'a> Iterator for Lines<'a> {
    type Item = &'a [u8];
    #[inline]
    fn next(&mut self) -> Option<&'a [u8]> {
        if self.pos >= self.buf.len() {
            return None;
        }
        let rest = &self.buf[self.pos..];
        match memchr::memchr(b'\n', rest) {
            Some(i) => {
                let line = &rest[..i];
                self.pos += i + 1;
                Some(line)
            }
            None => {
                let line = rest;
                self.pos = self.buf.len();
                Some(line)
            }
        }
    }
}

/// True when every byte is an ASCII digit and there is at least one.
#[inline]
pub fn all_digits(s: &[u8]) -> bool {
    !s.is_empty() && s.iter().all(|c| c.is_ascii_digit())
}

/// Parse an unsigned integer from ASCII digits, saturating at u64::MAX.
#[inline]
pub fn parse_u64(s: &[u8]) -> u64 {
    let mut v: u64 = 0;
    for &c in s {
        v = v.saturating_mul(10).saturating_add((c - b'0') as u64);
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_agree_at_block_boundaries() {
        for padding in 0..64 {
            let input = format!(
                "{},\"short\",\"\"\n\na,b,c,d\na,b\n\"bad,b,c\na,\"bad\"x,c\n\"{}\",你好,\r\n{},last,",
                " ".repeat(padding),
                "x".repeat(97 + padding),
                "y".repeat(padding),
            );
            let expected: Vec<_> = Lines::raw(input.as_bytes()).filter_map(split_n::<3>).collect();
            let actual: Vec<_> = Rows::<3>::raw(input.as_bytes()).collect();
            assert_eq!(actual, expected, "padding {padding}");
        }
    }
}
