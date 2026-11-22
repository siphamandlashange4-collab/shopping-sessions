//! Compact ingestion for IDs that fit a reversible word representation.
//! Unsupported ID shapes fall back to the full byte-string pipeline.

use super::{
    attribution::{CampaignTable, Click}, Lookup,
    chunks, csv, event_fact, exchange, find_root, shard, summarize, ts,
    Buckets, Catalogs, CompactFact, Fact, OutputName, UserRows, Window, BAD_TS, ORGANIC,
    SEVEN_DAYS_MS, SHARDS,
};
use arrow_array::StringArray;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::hash::BuildHasher;
use std::path::Path;

const UPPER_CASE: u64 = 1 << 63;

#[derive(Clone, Copy, Default, Eq, Hash, PartialEq)]
struct PackedName(u64);

#[inline]
fn pack<const PREFIX: u8>(id: &[u8]) -> Option<u64> {
    let (&first, suffix) = id.split_first()?;
    let variant = if first == PREFIX {
        0
    } else if first == PREFIX.to_ascii_uppercase() {
        UPPER_CASE
    } else {
        return None;
    };
    // A last byte of 0x7f is left out so that LONG_TAG never names a packed ID.
    if !matches!(suffix.last(), Some(1..=126)) {
        return None;
    }
    let value = if let Ok(bytes) = <[u8; 8]>::try_from(suffix) {
        u64::from_le_bytes(bytes)
    } else if suffix.len() < 8 {
        let mut bytes = [0; 8];
        bytes[..suffix.len()].copy_from_slice(suffix);
        u64::from_le_bytes(bytes)
    } else {
        return None;
    };
    Some(value | variant)
}

/// Bits 56..63 of every name that does not pack into a word. A packed name
/// can never hold this pattern there, because `pack` refuses a last byte of
/// 0x7f. The other 57 bits come from a hash of the ID, and `LongNames` proves
/// that no two distinct IDs of one run share a name.
const LONG_TAG: u64 = 0x7f << 56;
const LONG_HASH_BITS: u64 = !(0x7f << 56);

#[inline]
fn finish_hash(mut h: u64) -> u64 {
    // Every input bit reaches every output bit, the kept ones included.
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

const ONES: u64 = 0x0101_0101_0101_0101;
const HIGH: u64 = 0x8080_8080_8080_8080;

/// Per byte of a word of ASCII bytes, 0x80 where the byte lies in lo..=hi.
#[inline]
fn in_range(word: u64, lo: u8, hi: u8) -> u64 {
    let at_least = word.wrapping_add(ONES * (0x80 - lo as u64));
    let above = word.wrapping_add(ONES * (0x7f - hi as u64));
    at_least & !above & HIGH
}

/// Eight hexadecimal digits as one number (the first digit in the lowest
/// nibble), and whether any of them is a lowercase or an uppercase letter.
#[inline]
fn hex_word(word: u64) -> Option<(u64, bool, bool)> {
    if word & HIGH != 0 {
        return None;
    }
    let digit = in_range(word, b'0', b'9');
    let lower = in_range(word, b'a', b'f');
    let upper = in_range(word, b'A', b'F');
    if digit | lower | upper != HIGH {
        return None;
    }
    let mut nibbles = (word & (ONES * 0x0f)) + ((lower | upper) >> 7) * 9;
    nibbles = (nibbles | (nibbles >> 4)) & 0x00ff_00ff_00ff_00ff;
    nibbles = (nibbles | (nibbles >> 8)) & 0x0000_ffff_0000_ffff;
    nibbles = (nibbles | (nibbles >> 16)) & 0x0000_0000_ffff_ffff;
    Some((nibbles, lower != 0, upper != 0))
}

/// An ID made of one prefix byte and 32 hexadecimal digits whose letters
/// share one case, held losslessly: the digits as 128 bits, the prefix byte
/// and the case of the letters.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct HexId {
    digits: [u64; 2],
    prefix: u8,
    upper: bool,
}

#[inline]
fn hex_id(id: &[u8]) -> Option<HexId> {
    let id: &[u8; 33] = id.try_into().ok()?;
    let word = |i: usize| u64::from_le_bytes(id[1 + 8 * i..9 + 8 * i].try_into().unwrap());
    let (a, la, ua) = hex_word(word(0))?;
    let (b, lb, ub) = hex_word(word(1))?;
    let (c, lc, uc) = hex_word(word(2))?;
    let (d, ld, ud) = hex_word(word(3))?;
    let (lower, upper) = (la | lb | lc | ld, ua | ub | uc | ud);
    if lower && upper {
        return None;
    }
    Some(HexId { digits: [a | b << 32, c | d << 32], prefix: id[0], upper })
}

impl HexId {
    #[inline]
    fn hash(self) -> u64 {
        finish_hash(rustc_hash::FxBuildHasher.hash_one(self))
    }

    fn write(self, out: &mut [u8; 33]) {
        out[0] = self.prefix;
        // The distance from '0' + 10 to the first letter of the ID's case.
        let letter = if self.upper { b'A' - b'0' - 10 } else { b'a' - b'0' - 10 } as u64;
        for (i, chunk) in out[1..].chunks_exact_mut(8).enumerate() {
            // Spread eight digits, first digit lowest, one per byte.
            let mut x = (self.digits[i / 2] >> (32 * (i % 2))) & 0xffff_ffff;
            x = (x | (x << 16)) & 0x0000_ffff_0000_ffff;
            x = (x | (x << 8)) & 0x00ff_00ff_00ff_00ff;
            x = (x | (x << 4)) & 0x0f0f_0f0f_0f0f_0f0f;
            let letters = ((x + ONES * 6) >> 4) & ONES;
            let ascii = x + ONES * b'0' as u64 + letters * letter;
            chunk.copy_from_slice(&ascii.to_le_bytes());
        }
    }
}

/// The exact text behind a long name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LongText {
    Hex(HexId),
    Bytes(&'static [u8]),
}

impl LongText {
    /// The long name of an ID that does not pack, and its exact text.
    #[inline]
    fn of(id: &'static [u8]) -> (u64, Self) {
        let (hash, text) = match hex_id(id) {
            Some(hex) => (hex.hash(), Self::Hex(hex)),
            None => (finish_hash(rustc_hash::FxBuildHasher.hash_one(id)), Self::Bytes(id)),
        };
        ((hash & LONG_HASH_BITS) | LONG_TAG, text)
    }

    fn len(self) -> usize {
        match self {
            Self::Hex(_) => 33,
            Self::Bytes(id) => id.len(),
        }
    }

    fn with_bytes<R>(self, f: impl FnOnce(&[u8]) -> R) -> R {
        match self {
            Self::Hex(hex) => {
                let mut text = [0; 33];
                hex.write(&mut text);
                f(&text)
            }
            Self::Bytes(id) => f(id),
        }
    }
}

#[cfg(test)]
fn long_name(id: &'static [u8]) -> u64 {
    LongText::of(id).0
}

/// The partition of a routed person, and of the text behind a long name.
#[inline]
fn person_shard(name: u64) -> usize {
    shard(rustc_hash::FxBuildHasher.hash_one(name))
}

/// Long user IDs seen while parsing, by `person_shard` of their name, so that
/// `LongNames::verify` can check them shard by shard. A small direct-mapped
/// memory of the latest pairs skips most repeats of busy IDs; it only skips
/// a pair whose text equals the one it remembers.
struct LongSink {
    seen: Vec<(u64, LongText)>,
    names: [Vec<(u64, LongText)>; SHARDS],
}

impl LongSink {
    fn new() -> Self {
        Self {
            seen: vec![(0, LongText::Bytes(b"")); 512],
            names: std::array::from_fn(|_| Vec::new()),
        }
    }

    #[inline]
    fn push(&mut self, name: u64, text: LongText) {
        let memo = &mut self.seen[(name as usize ^ (name >> 23) as usize) & 511];
        if memo.0 == name && memo.1 == text {
            return;
        }
        *memo = (name, text);
        self.names[person_shard(name)].push((name, text));
    }
}

/// The text behind every long name of the run.
struct LongNames {
    links: FxHashMap<u64, LongText>,
    shards: Vec<FxHashMap<u64, LongText>>,
}

static LONG_NAMES: std::sync::OnceLock<LongNames> = std::sync::OnceLock::new();

impl LongNames {
    /// Merge the names seen in events and clicks with those of the links.
    /// None when two distinct IDs share a name: the exact pipeline runs then.
    fn verify(links: FxHashMap<u64, LongText>, sinks: Vec<LongSink>) -> Option<Self> {
        let mut by_shard: [Vec<Vec<(u64, LongText)>>; SHARDS] = std::array::from_fn(|_| Vec::new());
        for sink in sinks {
            for (shard, names) in by_shard.iter_mut().zip(sink.names) {
                if !names.is_empty() {
                    shard.push(names);
                }
            }
        }
        let shards = by_shard
            .into_par_iter()
            .map(|lists| {
                let mut map: FxHashMap<u64, LongText> = FxHashMap::default();
                for (name, text) in lists.into_iter().flatten() {
                    if let Some(&known) = links.get(&name) {
                        if known != text {
                            return None;
                        }
                        continue;
                    }
                    if *map.entry(name).or_insert(text) != text {
                        return None;
                    }
                }
                Some(map)
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self { links, shards })
    }

    fn text(name: u64) -> LongText {
        let names = LONG_NAMES.get().expect("long names are verified before output");
        match names.links.get(&name) {
            Some(&text) => text,
            None => names.shards[person_shard(name)][&name],
        }
    }
}

impl PackedName {
    #[inline]
    fn is_long(self) -> bool {
        self.0 & !LONG_HASH_BITS == LONG_TAG
    }

    /// The name of a user ID. Short IDs pack into the word itself; any other
    /// ID gets a long name, and its text goes to `sink` for verification.
    #[inline]
    fn of(id: &'static [u8], sink: &mut LongSink) -> Self {
        match pack::<b'u'>(id) {
            Some(value) => Self(value),
            None => Self::long(id, sink),
        }
    }

    /// Kept out of line so the parsing loops stay small for packed IDs.
    #[inline(never)]
    fn long(id: &'static [u8], sink: &mut LongSink) -> Self {
        let (name, text) = LongText::of(id);
        sink.push(name, text);
        Self(name)
    }

    fn text_len(self) -> usize {
        if self.is_long() {
            return LongNames::text(self.0).len();
        }
        let suffix = self.0 & !UPPER_CASE;
        1 + ((64 - suffix.leading_zeros()) as usize).div_ceil(8)
    }

    /// The ID's bytes; `long` holds the text of long names.
    fn text<'a>(self, long: &FxHashMap<u64, LongText>, buffer: &'a mut [u8; 33]) -> &'a [u8] {
        if self.is_long() {
            return match long[&self.0] {
                LongText::Hex(hex) => {
                    hex.write(buffer);
                    &buffer[..]
                }
                LongText::Bytes(id) => id,
            };
        }
        let len = self.text_len();
        buffer[0] = if self.0 & UPPER_CASE == 0 { b'u' } else { b'U' };
        buffer[1..9].copy_from_slice(&(self.0 & !UPPER_CASE).to_le_bytes());
        &buffer[..len]
    }

    /// Byte-wise order of the two IDs behind these names.
    fn precedes(self, other: Self, long: &FxHashMap<u64, LongText>) -> bool {
        if !self.is_long() && !other.is_long() {
            return self.byte_order() < other.byte_order();
        }
        let (mut a, mut b) = ([0; 33], [0; 33]);
        self.text(long, &mut a) < other.text(long, &mut b)
    }

    #[inline]
    fn byte_order(self) -> (bool, u64) {
        // U precedes u. The suffix is stored little endian, so its byte
        // order becomes numeric order after swapping and clearing the case bit.
        (self.0 & UPPER_CASE == 0, (self.0 & !UPPER_CASE).swap_bytes())
    }
}

impl OutputName for PackedName {
    fn string_array(names: Vec<Self>) -> StringArray {
        let mut builder =
            arrow_array::builder::StringBuilder::with_capacity(names.len(), names.len() * 9);
        let mut text = [0; 9];
        for packed in names {
            if packed.is_long() {
                LongNames::text(packed.0).with_bytes(|id| {
                    builder.append_value(std::str::from_utf8(id).expect("UTF-8 user ID"))
                });
                continue;
            }
            let Self(name) = packed;
            let suffix = name & !UPPER_CASE;
            let len = ((64 - suffix.leading_zeros()) as usize).div_ceil(8);
            text[0] = if name & UPPER_CASE == 0 { b'u' } else { b'U' };
            text[1..].copy_from_slice(&suffix.to_le_bytes());
            builder.append_value(std::str::from_utf8(&text[..=len]).expect("UTF-8 user ID"));
        }
        builder.finish()
    }

    fn write_plain(path: &Path, rows: &UserRows<Self>) -> bool {
        // This bounds the size of a plain page independently of the number
        // of repeated category values. The general writer handles longer text.
        if rows.category.iter().flatten().any(|text| text.len() > 4096) {
            return false;
        }
        let mut table = crate::plain_parquet::UserTable::new(rows.names.len());
        table.names(&rows.names, Self::text_len, |name, bytes| {
            if name.is_long() {
                LongNames::text(name.0).with_bytes(|id| bytes.extend_from_slice(id));
                return;
            }
            let suffix = (name.0 & !UPPER_CASE).to_le_bytes();
            bytes.push(if name.0 & UPPER_CASE == 0 { b'u' } else { b'U' });
            bytes.extend_from_slice(&suffix[..name.text_len() - 1]);
        });
        table.day(&rows.day);
        table.integer("sessions", &rows.sessions);
        table.integer("events", &rows.events);
        table.integer("revenue_cents", &rows.revenue);
        table.categories(&rows.category);
        table.finish(path);
        true
    }
}

/// Linked user IDs and the person each one belongs to, in one flat
/// open-addressed table: a probe touches a single 64-byte line, and the slot
/// for a name can be prefetched before it is needed. Key 0 marks an empty
/// slot; packed names are never 0. A name absent from the table is its own
/// person.
pub(super) struct PackedLinks {
    slots: Vec<[u64; 2]>,
    shift: u32,
    len: usize,
}

impl PackedLinks {
    fn with_capacity(names: usize) -> Self {
        // At most about 40% full: a large share of the people in the events
        // are not linked, and a miss has to probe until it reaches a free slot.
        let cap = (names * 2 + 16).next_power_of_two();
        Self {
            slots: vec![[0, 0]; cap],
            shift: 64 - cap.trailing_zeros(),
            len: 0,
        }
    }

    /// Double the table. Only reached when the IDs are far more distinct
    /// than links normally are; it keeps every probe sequence short.
    #[cold]
    fn grow(&mut self) {
        let mut bigger = Self::with_capacity(self.slots.len());
        let mask = bigger.slots.len() - 1;
        for &[key, value] in &self.slots {
            if key != 0 {
                let mut i = bigger.home(key);
                while bigger.slots[i][0] != 0 {
                    i = (i + 1) & mask;
                }
                bigger.slots[i] = [key, value];
                bigger.len += 1;
            }
        }
        *self = bigger;
    }

    #[inline]
    fn home(&self, key: u64) -> usize {
        (key.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> self.shift) as usize
    }

    /// Index of `key`, inserting it with `make` when absent.
    #[inline]
    fn index_or_insert(&mut self, key: u64, make: impl FnOnce() -> u64) -> u64 {
        if (self.len + 1) * 4 > self.slots.len() * 3 {
            self.grow();
        }
        let mask = self.slots.len() - 1;
        let mut i = self.home(key);
        loop {
            let slot = &mut self.slots[i];
            if slot[0] == key {
                return slot[1];
            }
            if slot[0] == 0 {
                let value = make();
                *slot = [key, value];
                self.len += 1;
                return value;
            }
            i = (i + 1) & mask;
        }
    }

    #[inline]
    fn prefetch(&self, name: PackedName) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(
                self.slots.as_ptr().add(self.home(name.0)).cast::<i8>(),
                _MM_HINT_T0,
            );
        }
    }

    /// The link table and the text of every long name in the links. None
    /// when two distinct linked IDs share a long name.
    fn build(
        buffers: &[&'static [u8]],
    ) -> Option<(Self, FxHashMap<u64, LongText>)> {
        let (parts, long_parts): (Vec<_>, Vec<_>) = chunks(buffers)
            .into_par_iter()
            .map(|chunk| {
                let mut pairs = Vec::new();
                let mut long: Vec<(u64, LongText)> = Vec::new();
                let mut name = |id: &'static [u8]| match pack::<b'u'>(id) {
                    Some(value) => PackedName(value),
                    None => {
                        let (name, text) = LongText::of(id);
                        long.push((name, text));
                        PackedName(name)
                    }
                };
                csv::for_each::<2>(chunk, |[a, b]| {
                    if a.is_empty() || b.is_empty() || a == b {
                        return true;
                    }
                    pairs.push((name(a), name(b)));
                    true
                });
                (pairs, long)
            })
            .unzip();
        let mut long_names: FxHashMap<u64, LongText> = FxHashMap::default();
        for (name, text) in long_parts.into_iter().flatten() {
            if *long_names.entry(name).or_insert(text) != text {
                return None;
            }
        }
        let endpoints = parts.iter().map(Vec::len).sum::<usize>() * 2;
        let mut links = Self::with_capacity(endpoints);
        let mut parent: Vec<u32> = Vec::with_capacity(endpoints);
        let mut rank: Vec<u8> = Vec::with_capacity(endpoints);
        let mut smallest: Vec<PackedName> = Vec::with_capacity(endpoints);
        for (a, b) in parts.into_iter().flatten() {
            let [mut a, mut b] = [a, b].map(|name| {
                let index = links.index_or_insert(name.0, || {
                    let index = parent.len() as u32;
                    parent.push(index);
                    rank.push(0u8);
                    smallest.push(name);
                    u64::from(index)
                });
                find_root(&mut parent, index as u32) as usize
            });
            if a == b {
                continue;
            }
            if rank[a] > rank[b] {
                std::mem::swap(&mut a, &mut b);
            }
            parent[a] = b as u32;
            rank[b] += u8::from(rank[a] == rank[b]);
            if smallest[a].precedes(smallest[b], &long_names) {
                smallest[b] = smallest[a];
            }
        }
        // Replace each name's index with its person's name. Canonical names
        // map to themselves, which is what a lookup miss returns anyway.
        for slot in links.slots.iter_mut() {
            if slot[0] != 0 {
                slot[1] = smallest[find_root(&mut parent, slot[1] as u32) as usize].0;
            }
        }
        Some((links, long_names))
    }

    #[inline]
    fn canonical(&self, name: PackedName) -> PackedName {
        let mask = self.slots.len() - 1;
        let mut i = self.home(name.0);
        loop {
            // `i` is always masked to the table length.
            let [key, value] = unsafe { *self.slots.get_unchecked(i) };
            if key == name.0 {
                return PackedName(value);
            }
            if key == 0 {
                return name;
            }
            i = (i + 1) & mask;
        }
    }
}

#[derive(Clone, Copy)]
struct SmallEvent {
    person: PackedName,
    fact: CompactFact,
}

#[derive(Clone, Copy)]
struct Candidate {
    key: u64,
    ingest: i64,
    event: SmallEvent,
}

/// An event whose ID is a prefix and 32 hex digits, deduplicated on the
/// lossless value of the ID.
#[derive(Clone, Copy)]
struct HexCandidate {
    id: HexId,
    ingest: i64,
    event: SmallEvent,
}

/// An event with any other ID that does not pack, deduplicated on its bytes.
#[derive(Clone, Copy)]
struct LongCandidate {
    id: &'static [u8],
    ingest: i64,
    event: SmallEvent,
}

/// One partition's events whose IDs do not pack into a word.
#[derive(Default)]
struct WideCandidates {
    hex: Vec<Vec<HexCandidate>>,
    bytes: Vec<Vec<LongCandidate>>,
}

#[inline]
fn event_key(id: u64) -> u64 {
    // Odd multiplication modulo 2^64 and rotation are reversible. The
    // complete transformed word is still an exact event identity.
    id.wrapping_mul(0xf135_7aea_2e62_a9c5).rotate_left(26)
}

struct SmallClick {
    person: PackedName,
    offset_ms: i32,
    campaign: u32,
    id: &'static [u8],
    order: u64,
}

#[inline]
fn mark(words: &mut [u64], index: usize) -> bool {
    let bit = 1_u64 << (index % 64);
    let word = &mut words[index / 64];
    let already_set = *word & bit != 0;
    *word |= bit;
    already_set
}

fn route_partition(
    pieces: Vec<Vec<Candidate>>,
    wide: WideCandidates,
    words: usize,
    links: &PackedLinks,
) -> Buckets<SmallEvent> {
    use std::collections::hash_map::Entry;

    // A hash slot shared by two IDs always goes through exact deduplication.
    let mask = words * 64 - 1;
    let mut seen = vec![0u64; words];
    let mut repeated = vec![0u64; words];
    for candidate in pieces.iter().flatten() {
        let slot = candidate.slot(mask);
        if mark(&mut seen, slot) {
            mark(&mut repeated, slot);
        }
    }
    drop(seen);

    let total: usize = pieces.iter().map(Vec::len).sum();
    let hex_total: usize = wide.hex.iter().map(Vec::len).sum();
    let bytes_total: usize = wide.bytes.iter().map(Vec::len).sum();
    let routed = total + hex_total + bytes_total;
    let mut buckets: Buckets<SmallEvent> =
        std::array::from_fn(|_| Vec::with_capacity(routed / SHARDS * 5 / 4 + 64));
    // Contested slots are a few percent of the partition; size for them up
    // front instead of rehashing as the map grows.
    let mut winners = FxHashMap::with_capacity_and_hasher(total / 8 + 64, Default::default());
    const AHEAD: usize = 32;
    for piece in &pieces {
        for (index, candidate) in piece.iter().enumerate() {
            if let Some(ahead) = piece.get(index + AHEAD) {
                prefetch_write(&repeated[ahead.slot(mask) / 64]);
                if ahead.event.fact.valid() {
                    links.prefetch(ahead.event.person);
                }
            }
            let candidate = *candidate;
            let slot = candidate.slot(mask);
            if repeated[slot / 64] & (1u64 << (slot % 64)) == 0 {
                route(candidate.event, links, &mut buckets);
                continue;
            }
            let value = (candidate.ingest, candidate.event);
            match winners.entry(candidate.key) {
                Entry::Vacant(entry) => {
                    entry.insert(value);
                }
                Entry::Occupied(mut entry) => {
                    if candidate.ingest < entry.get().0 {
                        entry.insert(value);
                    }
                }
            }
        }
    }
    for (_, event) in winners.into_values() {
        route(event, links, &mut buckets);
    }
    drop(pieces);

    // Hex IDs: the same slot marking, then exact selection on the lossless
    // value of the ID for contested slots.
    if hex_total > 0 {
        let slots = (hex_total * 8).next_power_of_two().max(64);
        let mask = slots - 1;
        let mut seen = vec![0u64; slots / 64];
        let mut repeated = vec![0u64; slots / 64];
        for candidate in wide.hex.iter().flatten() {
            let slot = candidate.id.hash() as usize & mask;
            if mark(&mut seen, slot) {
                mark(&mut repeated, slot);
            }
        }
        drop(seen);
        let mut contested: FxHashMap<HexId, (i64, SmallEvent)> = FxHashMap::default();
        for candidate in wide.hex.iter().flatten() {
            let slot = candidate.id.hash() as usize & mask;
            if repeated[slot / 64] & (1u64 << (slot % 64)) == 0 {
                route(candidate.event, links, &mut buckets);
                continue;
            }
            let value = (candidate.ingest, candidate.event);
            match contested.entry(candidate.id) {
                Entry::Vacant(entry) => {
                    entry.insert(value);
                }
                Entry::Occupied(mut entry) => {
                    if candidate.ingest < entry.get().0 {
                        entry.insert(value);
                    }
                }
            }
        }
        for (_, event) in contested.into_values() {
            route(event, links, &mut buckets);
        }
    }

    // Any other ID that does not pack is deduplicated on its exact bytes.
    let mut long_winners: FxHashMap<&'static [u8], (i64, SmallEvent)> =
        FxHashMap::with_capacity_and_hasher(bytes_total, Default::default());
    for candidate in wide.bytes.iter().flatten() {
        let value = (candidate.ingest, candidate.event);
        match long_winners.entry(candidate.id) {
            Entry::Vacant(entry) => {
                entry.insert(value);
            }
            Entry::Occupied(mut entry) => {
                if candidate.ingest < entry.get().0 {
                    entry.insert(value);
                }
            }
        }
    }
    for (_, event) in long_winners.into_values() {
        route(event, links, &mut buckets);
    }
    buckets
}

impl Candidate {
    #[inline]
    fn slot(&self, mask: usize) -> usize {
        self.key as usize & mask
    }
}

type CandidatePartitions = [Vec<Vec<Candidate>>; SHARDS];

/// Candidates per block: 128 KB. Buckets grow a block at a time, so nothing
/// is ever copied as they grow and at most one partly filled block per shard
/// is wasted. With huge pages, a generously pre-sized bucket would instead be
/// faulted in (and zeroed by the kernel) far beyond what it ever holds.
const BLOCK: usize = 4096;

struct Blocks {
    current: [Vec<Candidate>; SHARDS],
    full: [Vec<Vec<Candidate>>; SHARDS],
    hex: [Vec<HexCandidate>; SHARDS],
    long: [Vec<LongCandidate>; SHARDS],
    sink: LongSink,
}

impl Blocks {
    fn new() -> Self {
        Self {
            current: std::array::from_fn(|_| Vec::with_capacity(BLOCK)),
            full: std::array::from_fn(|_| Vec::new()),
            hex: std::array::from_fn(|_| Vec::new()),
            long: std::array::from_fn(|_| Vec::new()),
            sink: LongSink::new(),
        }
    }

    #[inline]
    fn push(&mut self, shard: usize, candidate: Candidate) {
        let current = &mut self.current[shard];
        current.push(candidate);
        if current.len() == BLOCK {
            let block = std::mem::replace(current, Vec::with_capacity(BLOCK));
            self.full[shard].push(block);
        }
    }

    #[allow(clippy::type_complexity)]
    /// An event whose ID does not pack. Kept out of line so the parsing loop
    /// stays small for packed IDs.
    #[inline(never)]
    fn push_wide(&mut self, id: &'static [u8], ingest: i64, event: SmallEvent) {
        match hex_id(id) {
            Some(id) => {
                self.hex[shard(id.hash())].push(HexCandidate { id, ingest, event });
            }
            None => {
                let partition = shard(rustc_hash::FxBuildHasher.hash_one(id));
                self.long[partition].push(LongCandidate { id, ingest, event });
            }
        }
    }

    fn finish(
        mut self,
    ) -> (
        [Vec<Vec<Candidate>>; SHARDS],
        [Vec<HexCandidate>; SHARDS],
        [Vec<LongCandidate>; SHARDS],
        LongSink,
    ) {
        for (full, current) in self.full.iter_mut().zip(self.current) {
            if !current.is_empty() {
                full.push(current);
            }
        }
        (self.full, self.hex, self.long, self.sink)
    }
}

type WidePartitions = [WideCandidates; SHARDS];

fn collect_candidates(
    buffers: &[&'static [u8]],
    window: &Window,
    catalogs: &Catalogs,
) -> Option<(CandidatePartitions, WidePartitions, usize, Vec<LongSink>)> {
    let chunks = chunks(buffers);
    let bytes = chunks.iter().map(|chunk| chunk.len()).sum::<usize>();
    let slots = (bytes / 16)
        .clamp(64 * SHARDS, (u32::MAX as usize / 2) + 1)
        .next_power_of_two();

    // Parse the CSV once and move compact rows by event ID before routing
    // them by person. Same-ID copies always reach the same partition.
    // `fold` keeps one set of buckets per stolen piece of work, so every
    // thread pulls 2 MB chunks until the input runs out instead of owning a
    // fixed share of it.
    let pieces = chunks
        .par_iter()
        .fold(
            || Some(Blocks::new()),
            |possible, &chunk| {
                let mut possible = possible?;
                let ok = csv::for_each::<7>(chunk, |[id, person, time, ingest, kind, product, price]| {
                    if id.is_empty() {
                        return true;
                    }
                    let ingest = ts::parse_ingest(ingest).unwrap_or(BAD_TS);
                    let fact = event_fact::<CompactFact, _>(
                        window,
                        &catalogs.product_ids,
                        catalogs.unknown_category,
                        person,
                        time,
                        ingest,
                        kind,
                        product,
                        price,
                    );
                    let person = if fact.valid() {
                        PackedName::of(person, &mut possible.sink)
                    } else {
                        PackedName::default()
                    };
                    let event = SmallEvent { person, fact };
                    match pack::<b'e'>(id) {
                        Some(id) => {
                            let key = event_key(id);
                            possible.push(shard(key), Candidate { key, ingest, event });
                        }
                        None => possible.push_wide(id, ingest, event),
                    }
                    true
                });
                ok.then_some(possible)
            },
        )
        .collect::<Option<Vec<_>>>()?;

    let mut partitions: CandidatePartitions = std::array::from_fn(|_| Vec::new());
    let mut wide: WidePartitions = std::array::from_fn(|_| WideCandidates::default());
    let mut sinks = Vec::with_capacity(pieces.len());
    for piece in pieces {
        let (blocks, hex, long, sink) = piece.finish();
        for (partition, blocks) in partitions.iter_mut().zip(blocks) {
            partition.extend(blocks);
        }
        for ((partition, hex), long) in wide.iter_mut().zip(hex).zip(long) {
            if !hex.is_empty() {
                partition.hex.push(hex);
            }
            if !long.is_empty() {
                partition.bytes.push(long);
            }
        }
        sinks.push(sink);
    }
    Some((partitions, wide, slots / SHARDS / 64, sinks))
}

#[inline]
fn route(mut event: SmallEvent, links: &PackedLinks, groups: &mut Buckets<SmallEvent>) {
    if event.fact.valid() {
        event.person = links.canonical(event.person);
        groups[person_shard(event.person.0)].push(event);
    }
}

fn collect_clicks(
    buffers: &[&'static [u8]],
    window: &Window,
    catalogs: &Catalogs,
    links: &PackedLinks,
) -> Option<([Vec<Vec<SmallClick>>; SHARDS], Vec<LongSink>)> {
    let start = window.start - SEVEN_DAYS_MS;
    let (pieces, sinks): (Vec<_>, Vec<_>) = chunks(buffers)
        .into_par_iter()
        .map(|chunk| {
            // Parse first, then resolve people in a second pass where the
            // link slot for a click sixteen rows ahead is already in flight.
            let mut parsed: Vec<SmallClick> = Vec::with_capacity(chunk.len() / 40);
            let mut sink = LongSink::new();
            csv::for_each::<4>(chunk, |[id, person, campaign, time]| {
                if id.is_empty() || person.is_empty() {
                    return true;
                }
                let Some(time) = ts::parse(time).filter(|time| (start..window.end).contains(time))
                else {
                    return true;
                };
                let Some(campaign) = catalogs.campaign_ids.lookup(campaign).filter(|&id| id != ORGANIC)
                else {
                    return true;
                };
                let person = PackedName::of(person, &mut sink);
                parsed.push(SmallClick {
                    person,
                    // The compact window has at most 2^31 milliseconds, and
                    // the click lookback spans only seven days before it.
                    offset_ms: (time - window.start) as i32,
                    campaign,
                    id,
                    order: crate::attribution::id_order(id),
                });
                true
            });
            let mut people: [Vec<SmallClick>; SHARDS] =
                std::array::from_fn(|_| Vec::with_capacity(parsed.len() / SHARDS * 5 / 4 + 16));
            const AHEAD: usize = 32;
            for index in 0..parsed.len() {
                if let Some(ahead) = parsed.get(index + AHEAD) {
                    links.prefetch(ahead.person);
                }
                let mut click = std::mem::replace(
                    &mut parsed[index],
                    SmallClick { person: PackedName(0), offset_ms: 0, campaign: 0, id: b"", order: 0 },
                );
                click.person = links.canonical(click.person);
                people[person_shard(click.person.0)].push(click);
            }
            (people, sink)
        })
        .unzip();
    Some((exchange(pieces), sinks))
}

/// Dense IDs for one shard's people, in a flat open-addressed table whose
/// slot for a name can be prefetched before it is needed. Key 0 marks an
/// empty slot; a routed person's packed name is never 0.
struct NameIds {
    slots: Vec<[u64; 2]>,
    shift: u32,
    len: usize,
}

impl NameIds {
    fn with_capacity(names: usize) -> Self {
        let cap = (names * 2 + 16).next_power_of_two();
        Self {
            slots: vec![[0, 0]; cap],
            shift: 64 - cap.trailing_zeros(),
            len: 0,
        }
    }

    #[inline]
    fn home(&self, key: u64) -> usize {
        (key.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> self.shift) as usize
    }

    #[inline]
    fn prefetch(&self, key: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(self.slots.as_ptr().add(self.home(key)).cast::<i8>(), _MM_HINT_T0);
        }
    }

    #[cold]
    fn grow(&mut self) {
        let mut bigger = Self::with_capacity(self.slots.len());
        let mask = bigger.slots.len() - 1;
        for &[key, value] in &self.slots {
            if key != 0 {
                let mut i = bigger.home(key);
                while bigger.slots[i][0] != 0 {
                    i = (i + 1) & mask;
                }
                bigger.slots[i] = [key, value];
                bigger.len += 1;
            }
        }
        *self = bigger;
    }

    /// The ID for `key`, assigning the next one when it is new.
    #[inline]
    fn id(&mut self, key: u64, mut make: impl FnMut() -> u64) -> u64 {
        if (self.len + 1) * 4 > self.slots.len() * 3 {
            self.grow();
        }
        let mask = self.slots.len() - 1;
        let mut i = self.home(key);
        loop {
            let slot = &mut self.slots[i];
            if slot[0] == key {
                return slot[1];
            }
            if slot[0] == 0 {
                let id = make();
                *slot = [key, id];
                self.len += 1;
                return id;
            }
            i = (i + 1) & mask;
        }
    }

    #[inline]
    fn get(&self, key: u64) -> Option<u64> {
        let mask = self.slots.len() - 1;
        let mut i = self.home(key);
        loop {
            let [k, v] = self.slots[i];
            if k == key {
                return Some(v);
            }
            if k == 0 {
                return None;
            }
            i = (i + 1) & mask;
        }
    }
}

#[inline]
fn prefetch_write<T>(slot: &T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
        _mm_prefetch((slot as *const T).cast::<i8>(), _MM_HINT_T0);
    }
}

fn aggregate_small(
    parts: Vec<Vec<SmallEvent>>,
    clicks: Vec<Vec<SmallClick>>,
    window: &Window,
    catalogs: &Catalogs,
) -> (UserRows<PackedName>, CampaignTable) {
    const AHEAD: usize = 16;
    let len: usize = parts.iter().map(Vec::len).sum();
    let mut ids = NameIds::with_capacity(len / 12);
    let mut names: Vec<PackedName> = Vec::with_capacity(len / 12);
    let mut counts: Vec<usize> = Vec::with_capacity(len / 12);
    // Once names are interned, reuse the source buffers to hold dense IDs
    // until the facts are scattered into their per-person ranges. Both
    // passes prefetch what the event sixteen rows ahead will touch.
    let mut parts = parts;
    for piece in parts.iter_mut() {
        for index in 0..piece.len() {
            if let Some(ahead) = piece.get(index + AHEAD) {
                ids.prefetch(ahead.person.0);
            }
            let event = &mut piece[index];
            let person = event.person;
            let id = ids.id(person.0, || {
                let id = names.len() as u64;
                names.push(person);
                counts.push(0);
                id
            }) as usize;
            counts[id] += 1;
            event.person = PackedName(id as u64);
        }
    }

    // Scatter each person's facts into one consecutive range; afterwards
    // offsets[i] is the end of person i's range.
    let mut end = 0;
    let mut offsets: Vec<usize> = counts
        .iter()
        .map(|&count| {
            let start = end;
            end += count;
            start
        })
        .collect();
    let mut events = vec![CompactFact::default(); len];
    for piece in parts.iter() {
        for index in 0..piece.len() {
            if let Some(ahead) = piece.get(index + AHEAD) {
                prefetch_write(&events[offsets[ahead.person.0 as usize].min(len - 1)]);
            }
            let event = piece[index];
            let person = event.person.0 as usize;
            events[offsets[person]] = event.fact;
            offsets[person] += 1;
        }
    }
    drop(parts);

    let mut grouped = Vec::with_capacity(clicks.iter().map(Vec::len).sum());
    grouped.extend(clicks.into_iter().flatten().filter_map(|click| {
        Some(Click {
            user: ids.get(click.person.0)? as u32,
            time: window.start + i64::from(click.offset_ms),
            campaign: click.campaign,
            id: click.id,
            order: click.order,
        })
    }));
    let clicks = grouped;
    summarize(
        events,
        offsets,
        names.into_iter(),
        clicks,
        &catalogs.categories,
        catalogs.channels.len(),
        window,
    )
}

pub(super) fn try_pipeline(
    event_bufs: &[&'static [u8]],
    click_bufs: &[&'static [u8]],
    link_bufs: &[&'static [u8]],
    window: &Window,
    catalogs: &Catalogs,
    outdir: &Path,
) -> Option<()> {
    // Parsing event candidates only needs the catalogs. Build the link
    // components and parse clicks in parallel, then route the dedup winners.
    let (candidates, linked_clicks) = rayon::join(
        || {
            let r = collect_candidates(event_bufs, window, catalogs);
            crate::lap("  [branch] event candidates parsed");
            r
        },
        || {
            let (links, long_links) = PackedLinks::build(link_bufs)?;
            crate::lap("  [branch] links built");
            let (clicks, click_sinks) = collect_clicks(click_bufs, window, catalogs, &links)?;
            crate::lap("  [branch] clicks parsed");
            Some((links, long_links, clicks, click_sinks))
        },
    );
    crate::lap("parse joined");
    let (candidates, wide_candidates, words, mut sinks) = candidates?;
    let (links, long_links, clicks, click_sinks) = linked_clicks?;
    sinks.extend(click_sinks);
    let (routed, long_names) = rayon::join(
        || {
            candidates
                .into_par_iter()
                .zip(wide_candidates)
                .map(|(pieces, wide)| route_partition(pieces, wide, words, &links))
                .collect()
        },
        || LongNames::verify(long_links, sinks),
    );
    // Two distinct user IDs share a long name: let the exact pipeline run.
    let long_names = long_names?;
    if LONG_NAMES.set(long_names).is_err() {
        return None;
    }
    crate::lap("dedup + route");
    let mut parts: Vec<_> = exchange(routed).into_iter().zip(clicks).enumerate().collect();
    // Heaviest first: the shards holding the most active people dominate.
    parts.sort_by_key(|(_, (events, clicks))| {
        std::cmp::Reverse(
            events.iter().map(Vec::len).sum::<usize>() + clicks.iter().map(Vec::len).sum::<usize>(),
        )
    });
    crate::write_results_scheduled(outdir, catalogs, parts, |(events, clicks)| {
        aggregate_small(events, clicks, window, catalogs)
    });
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_links_choose_the_bytewise_smallest_name() {
        let buffers = [
            &b"a,b\nuz,ua\nua,ufoo\0a\nufoo\0a,ufoo\nuxy,ux\n"[..],
            &b"a,b\nufoo,Ub\nuz,ufoo\n"[..],
        ];
        let (links, long) = PackedLinks::build(&buffers).unwrap();
        assert!(long.is_empty());
        let name = |bytes| pack::<b'u'>(bytes).unwrap();
        for alias in [b"uz".as_slice(), b"ua", b"ufoo\0a", b"ufoo", b"Ub"] {
            assert_eq!(links.canonical(PackedName(name(alias))).0, name(b"Ub"));
        }
        assert_eq!(links.canonical(PackedName(name(b"uxy"))).0, name(b"ux"));

        // IDs that do not pack join people too, and compare by their bytes.
        let hex = b"u0123456789abcdef0123456789abcdef";
        let (links, long) =
            PackedLinks::build(&[b"a,b\nua,external\nua,u0123456789abcdef0123456789abcdef\nuz,ub\n"])
                .unwrap();
        let external = long_name(b"external");
        assert_eq!(long.len(), 2);
        assert_eq!(long[&external], LongText::Bytes(b"external"));
        assert_eq!(long[&long_name(hex)], LongText::Hex(hex_id(hex).unwrap()));
        for alias in [PackedName(name(b"ua")), PackedName(long_name(hex)), PackedName(external)] {
            assert_eq!(links.canonical(alias).0, external);
        }
        assert_eq!(links.canonical(PackedName(name(b"uz"))).0, name(b"ub"));
        // "u0123..." sorts before "u1" even though it is longer.
        let (links, _) = PackedLinks::build(&[b"a,b\nu1,u0123456789abcdef0123456789abcdef\n"]).unwrap();
        assert_eq!(links.canonical(PackedName(name(b"u1"))).0, long_name(hex));
        assert_eq!(links.canonical(PackedName(long_name(hex))).0, long_name(hex));
    }

    #[test]
    fn hex_ids_round_trip_exactly() {
        for id in [
            &b"u0123456789abcdef0123456789abcdef"[..],
            b"UFEDCBA9876543210FEDCBA9876543210",
            b"k00000000000000000000000000000000",
            b"e99999999999999999999999999999999",
            b"xffffffffffffffffffffffffffffffff",
        ] {
            let hex = hex_id(id).unwrap();
            let mut text = [0; 33];
            hex.write(&mut text);
            assert_eq!(&text[..], id);
        }
        for id in [
            &b"u0123456789abcdef0123456789abcdeF"[..],
            b"u0123456789abcdef0123456789abcdeg",
            b"u0123456789abcdef0123456789abcde",
            b"u0123456789abcdef0123456789abcdef0",
            b"u0123456789abcdef0123456789abcde/",
            b"u0123456789abcdef0123456789abcde:",
            b"u0123456789abcdef0123456789abcde`",
            b"u0123456789abcdef0123456789abcde@",
            b"u0123456789abcdef0123456789abcde\xc3",
        ] {
            assert!(hex_id(id).is_none(), "{:?}", std::str::from_utf8(id));
        }
        // Different texts never share a value.
        let a = hex_id(b"u0123456789abcdef0123456789abcdef").unwrap();
        let b = hex_id(b"U0123456789ABCDEF0123456789ABCDEF").unwrap();
        let c = hex_id(b"U0123456789abcdef0123456789abcdef").unwrap();
        assert!(a != b && a != c && b != c);
        assert_eq!(hex_id(b"u10000000000000000000000000000000").unwrap().digits, [1, 0]);
        assert_eq!(hex_id(b"u00000000000000000000000000000001").unwrap().digits, [0, 1 << 60]);
    }

    #[test]
    fn long_names_never_collide_with_packed_names() {
        assert!(pack::<b'u'>(b"u").is_none());
        for id in [&b"u1"[..], b"uabcdefgh", b"U7", b"uabcdefg~"] {
            assert!(!PackedName(pack::<b'u'>(id).unwrap()).is_long());
        }
        assert!(pack::<b'u'>(b"uabcdefg\x7f").is_none());
        assert!(PackedName(long_name(b"u0123456789abcdef0123456789abcdef")).is_long());
    }

    #[test]
    fn hash_slot_collisions_keep_distinct_ids_and_earliest_copies() {
        let candidate = |key, ingest, fact| Candidate {
            key,
            ingest,
            event: SmallEvent { person: PackedName(11), fact },
        };
        let pieces = vec![
            vec![
                candidate(10, 20, CompactFact(100)),
                candidate(74, 30, CompactFact(200)),
                candidate(11, 40, CompactFact(300)),
            ],
            vec![
                candidate(10, 10, CompactFact::INVALID),
                candidate(74, 15, CompactFact(400)),
                candidate(138, 20, CompactFact(500)),
            ],
        ];
        let links = PackedLinks::with_capacity(16);
        let long_candidate = |id, ingest, fact| LongCandidate {
            id,
            ingest,
            event: SmallEvent { person: PackedName(11), fact },
        };
        let long = vec![
            vec![
                long_candidate(&b"e0123456789abcdef0123456789abcdef"[..], 20, CompactFact(600)),
                long_candidate(b"e0123456789abcdef0123456789abcdee", 30, CompactFact(700)),
            ],
            vec![long_candidate(b"e0123456789abcdef0123456789abcdef", 10, CompactFact(800))],
        ];
        let hex_candidate = |id: &[u8], ingest, fact| HexCandidate {
            id: hex_id(id).unwrap(),
            ingest,
            event: SmallEvent { person: PackedName(11), fact },
        };
        let hex = vec![
            vec![
                hex_candidate(b"e0000000000000000000000000000000a", 5, CompactFact(900)),
                hex_candidate(b"e000000000000000000000000000000b0", 5, CompactFact(910)),
            ],
            vec![
                hex_candidate(b"e0000000000000000000000000000000a", 4, CompactFact(920)),
                hex_candidate(b"E0000000000000000000000000000000A", 9, CompactFact(930)),
            ],
        ];
        let wide = WideCandidates { hex, bytes: long };
        let mut facts: Vec<_> = route_partition(pieces, wide, 1, &links)
            .into_iter()
            .flatten()
            .map(|event| event.fact.0)
            .collect();
        facts.sort_unstable();
        assert_eq!(facts, [300, 400, 500, 700, 800, 910, 920, 930]);
    }
}
