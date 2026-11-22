//! Compact ingestion for IDs that fit a reversible word representation.
//! Unsupported ID shapes fall back to the full byte-string pipeline.

use super::{
    attribution::{CampaignTable, Click}, Lookup,
    chunks, csv, event_fact, exchange, find_root, shard, summarize, ts, write_results,
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
    if !matches!(suffix.last(), Some(1..=127)) {
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

impl PackedName {
    #[inline]
    fn parse(id: &[u8]) -> Option<Self> {
        pack::<b'u'>(id).map(Self)
    }

    fn text_len(self) -> usize {
        let suffix = self.0 & !UPPER_CASE;
        1 + ((64 - suffix.leading_zeros()) as usize).div_ceil(8)
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
        for Self(name) in names {
            let suffix = name & !UPPER_CASE;
            let len = ((64 - suffix.leading_zeros()) as usize).div_ceil(8);
            text[0] = if name & UPPER_CASE == 0 { b'u' } else { b'U' };
            text[1..].copy_from_slice(&suffix.to_le_bytes());
            builder.append_value(std::str::from_utf8(&text[..=len]).expect("UTF-8 user ID"));
        }
        builder.finish()
    }

    fn write_plain(path: &Path, rows: &UserRows<Self>) -> bool {
        crate::plain_parquet::write_user_table(path, rows, Self::text_len, |name, bytes| {
            let suffix = (name.0 & !UPPER_CASE).to_le_bytes();
            bytes.push(if name.0 & UPPER_CASE == 0 { b'u' } else { b'U' });
            bytes.extend_from_slice(&suffix[..name.text_len() - 1]);
        })
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
        let cap = (names + 16).next_power_of_two();
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
        let mut bigger = Self::with_capacity(self.slots.len() * 2);
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

    pub(super) fn build(buffers: &[&'static [u8]]) -> Option<Self> {
        let parts: Vec<_> = chunks(buffers)
            .into_par_iter()
            .map(|chunk| {
                let mut pairs = Vec::new();
                let supported = csv::for_each::<2>(chunk, |[a, b]| {
                    if a.is_empty() || b.is_empty() || a == b {
                        return true;
                    }
                    let (Some(a), Some(b)) = (PackedName::parse(a), PackedName::parse(b)) else {
                        return false;
                    };
                    pairs.push((a, b));
                    true
                });
                supported.then_some(pairs)
            })
            .collect::<Option<_>>()?;
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
            if smallest[a].byte_order() < smallest[b].byte_order() {
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
        Some(links)
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
pub(super) fn mark(words: &mut [u64], index: usize) -> bool {
    let bit = 1_u64 << (index % 64);
    let word = &mut words[index / 64];
    let already_set = *word & bit != 0;
    *word |= bit;
    already_set
}

fn route_partition(
    pieces: Vec<Vec<Candidate>>,
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
    let mut buckets: Buckets<SmallEvent> =
        std::array::from_fn(|_| Vec::with_capacity(total / SHARDS * 5 / 4 + 64));
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
}

impl Blocks {
    fn new() -> Self {
        Self {
            current: std::array::from_fn(|_| Vec::with_capacity(BLOCK)),
            full: std::array::from_fn(|_| Vec::new()),
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

    fn finish(mut self) -> [Vec<Vec<Candidate>>; SHARDS] {
        for (full, current) in self.full.iter_mut().zip(self.current) {
            if !current.is_empty() {
                full.push(current);
            }
        }
        self.full
    }
}

fn collect_candidates(
    buffers: &[&'static [u8]],
    window: &Window,
    catalogs: &Catalogs,
) -> Option<(CandidatePartitions, usize)> {
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
                    let Some(id) = pack::<b'e'>(id) else {
                        return false;
                    };
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
                        let Some(person) = PackedName::parse(person) else {
                            return false;
                        };
                        person
                    } else {
                        PackedName::default()
                    };
                    let event = SmallEvent { person, fact };
                    let key = event_key(id);
                    possible.push(shard(key), Candidate { key, ingest, event });
                    true
                });
                crate::release_input(chunk);
                ok.then_some(possible)
            },
        )
        .collect::<Option<Vec<_>>>()?;

    let mut partitions: CandidatePartitions = std::array::from_fn(|_| Vec::new());
    for piece in pieces {
        for (partition, blocks) in partitions.iter_mut().zip(piece.finish()) {
            partition.extend(blocks);
        }
    }
    Some((partitions, slots / SHARDS / 64))
}

#[inline]
fn route(mut event: SmallEvent, links: &PackedLinks, groups: &mut Buckets<SmallEvent>) {
    if event.fact.valid() {
        event.person = links.canonical(event.person);
        let group = shard(rustc_hash::FxBuildHasher.hash_one(event.person.0));
        groups[group].push(event);
    }
}

fn collect_clicks(
    buffers: &[&'static [u8]],
    window: &Window,
    catalogs: &Catalogs,
    links: &PackedLinks,
) -> Option<[Vec<Vec<SmallClick>>; SHARDS]> {
    let start = window.start - SEVEN_DAYS_MS;
    let pieces = chunks(buffers)
        .into_par_iter()
        .map(|chunk| {
            // Parse first, then resolve people in a second pass where the
            // link slot for a click sixteen rows ahead is already in flight.
            let mut parsed: Vec<SmallClick> = Vec::with_capacity(chunk.len() / 40);
            let supported = csv::for_each::<4>(chunk, |[id, person, campaign, time]| {
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
                let Some(person) = PackedName::parse(person) else {
                    return false;
                };
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
            if !supported {
                return None;
            }
            // Click IDs still point into this chunk; a later read of one
            // (only on a timestamp tie) simply maps the page in again.
            crate::release_input(chunk);
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
                let group = shard(rustc_hash::FxBuildHasher.hash_one(click.person.0));
                people[group].push(click);
            }
            Some(people)
        })
        .collect::<Option<_>>()?;
    Some(exchange(pieces))
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

    let clicks = clicks
        .into_iter()
        .flatten()
        .filter_map(|click| {
            Some(Click {
                user: ids.get(click.person.0)? as u32,
                time: window.start + i64::from(click.offset_ms),
                campaign: click.campaign,
                id: click.id,
                order: click.order,
            })
        })
        .collect();
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
            let links = PackedLinks::build(link_bufs)?;
            crate::lap("  [branch] links built");
            let clicks = collect_clicks(click_bufs, window, catalogs, &links)?;
            crate::lap("  [branch] clicks parsed");
            Some((links, clicks))
        },
    );
    crate::lap("parse joined");
    let (candidates, words) = candidates?;
    let (links, clicks) = linked_clicks?;
    let routed = candidates
        .into_par_iter()
        .map(|pieces| route_partition(pieces, words, &links))
        .collect();
    crate::lap("dedup + route");
    let parts = exchange(routed)
        .into_par_iter()
        .zip(clicks)
        .map(|(events, clicks)| aggregate_small(events, clicks, window, catalogs));
    write_results(outdir, catalogs, parts);
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
        let links = PackedLinks::build(&buffers).unwrap();
        let name = |bytes| PackedName::parse(bytes).unwrap().0;
        for alias in [b"uz".as_slice(), b"ua", b"ufoo\0a", b"ufoo", b"Ub"] {
            assert_eq!(links.canonical(PackedName(name(alias))).0, name(b"Ub"));
        }
        assert_eq!(links.canonical(PackedName(name(b"uxy"))).0, name(b"ux"));
        assert!(PackedLinks::build(&[b"a,b\nua,external\n"]).is_none());
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
        let mut facts: Vec<_> = route_partition(pieces, 1, &links)
            .into_iter()
            .flatten()
            .map(|event| event.fact.0)
            .collect();
        facts.sort_unstable();
        assert_eq!(facts, [300, 400, 500]);
    }
}
