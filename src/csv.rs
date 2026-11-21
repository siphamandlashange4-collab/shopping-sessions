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
        Some(_) => {
            // Only legal shape: a quote at both ends, none inside.
            if f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
                let inner = &f[1..f.len() - 1];
                if memchr::memchr(b'"', inner).is_none() {
                    Some(inner)
                } else {
                    None
                }
            } else {
                None
            }
        }
    }
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
    /// Lines over a slice that has no header (used for parallel chunks).
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
