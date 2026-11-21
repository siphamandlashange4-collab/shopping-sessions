//! Compact ingestion for IDs that fit a reversible word representation.
//! Unsupported ID shapes fall back to the full byte-string pipeline.

use super::{
    attribution::{CampaignTable, Click},
    chunks, csv, event_fact, exchange, group_facts, shard, summarize, ts, write_results, Buckets,
    Catalogs, CompactFact, Fact, LinkNames, OutputName, UserRows, Window, BAD_TS, ORGANIC,
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
        // This bounds the size of a plain page independently of the number
        // of repeated category values. The general writer handles longer text.
        if rows.category.iter().flatten().any(|text| text.len() > 4096) {
            return false;
        }
        let mut table = crate::plain_parquet::UserTable::new(rows.names.len());
        table.names(&rows.names, Self::text_len, |name, bytes| {
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

struct PackedLinks(FxHashMap<PackedName, PackedName>);

impl PackedLinks {
    fn from_links(links: &LinkNames) -> Option<Self> {
        let mut aliases =
            FxHashMap::with_capacity_and_hasher(links.person.len(), Default::default());
        for (&source, &(person, _)) in links.users.vals.iter().zip(&links.person) {
            if source != person {
                aliases.insert(PackedName::parse(source)?, PackedName::parse(person)?);
            }
        }
        Some(Self(aliases))
    }

    #[inline]
    fn canonical(&self, name: PackedName) -> PackedName {
        self.0.get(&name).copied().unwrap_or(name)
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

    let mut buckets = std::array::from_fn(|_| Vec::new());
    let mut winners = FxHashMap::default();
    for candidate in pieces.into_iter().flatten() {
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

fn collect_events(
    buffers: &[&'static [u8]],
    window: &Window,
    catalogs: &Catalogs,
    links: &PackedLinks,
) -> Option<[Vec<Vec<SmallEvent>>; SHARDS]> {
    let mut chunks = chunks(buffers);
    let bytes = chunks.iter().map(|chunk| chunk.len()).sum::<usize>();
    let slots = (bytes / 16)
        .clamp(64 * SHARDS, (u32::MAX as usize / 2) + 1)
        .next_power_of_two();
    let workers = rayon::current_num_threads().min(8).min(chunks.len().max(1));
    let mut work: Vec<Vec<_>> = (0..workers).map(|_| Vec::new()).collect();
    chunks.sort_unstable_by_key(|chunk| std::cmp::Reverse(chunk.len()));
    for (index, chunk) in chunks.into_iter().enumerate() {
        work[index % workers].push(chunk);
    }

    // Parse the CSV once and move compact rows by event ID before routing
    // them by person. Same-ID copies always reach the same partition.
    let candidates = work
        .into_par_iter()
        .map(|chunks| {
            let mut possible: Buckets<Candidate> = std::array::from_fn(|_| Vec::new());
            for chunk in chunks {
                if !csv::for_each::<7>(chunk, |[id, person, time, ingest, kind, product, price]| {
                    if id.is_empty() {
                        return true;
                    }
                    let Some(id) = pack::<b'e'>(id) else {
                        return false;
                    };
                    let ingest = ts::parse_ingest(ingest).unwrap_or(BAD_TS);
                    let fact = event_fact::<CompactFact>(
                        window,
                        &catalogs.products,
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
                    possible[shard(key)].push(Candidate { key, ingest, event });
                    true
                }) {
                    return None;
                }
            }
            Some(possible)
        })
        .collect::<Option<Vec<_>>>()?;

    let routed = exchange(candidates)
        .into_par_iter()
        .map(|pieces| route_partition(pieces, slots / SHARDS / 64, links))
        .collect();
    Some(exchange(routed))
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
            let mut people = std::array::from_fn(|_| Vec::new());
            let supported = csv::for_each::<4>(chunk, |[id, person, campaign, time]| {
                if id.is_empty() || person.is_empty() {
                    return true;
                }
                let Some(time) = ts::parse(time).filter(|time| (start..window.end).contains(time))
                else {
                    return true;
                };
                let Some(campaign) = catalogs.campaigns.get(campaign).filter(|&id| id != ORGANIC)
                else {
                    return true;
                };
                let Some(person) = PackedName::parse(person) else {
                    return false;
                };
                let person = links.canonical(person);
                let group = shard(rustc_hash::FxBuildHasher.hash_one(person.0));
                people[group].push(SmallClick {
                    person,
                    // The compact window has at most 2^31 milliseconds, and
                    // the click lookback spans only seven days before it.
                    offset_ms: (time - window.start) as i32,
                    campaign,
                    id,
                });
                true
            });
            supported.then_some(people)
        })
        .collect::<Option<_>>()?;
    Some(exchange(pieces))
}

fn aggregate_small(
    parts: Vec<Vec<SmallEvent>>,
    clicks: Vec<Vec<SmallClick>>,
    window: &Window,
    catalogs: &Catalogs,
) -> (UserRows<PackedName>, CampaignTable) {
    let len: usize = parts.iter().map(Vec::len).sum();
    let mut ids = FxHashMap::with_capacity_and_hasher(len / 12, Default::default());
    let mut names = Vec::new();
    let mut counts = Vec::new();
    // Once names are interned, reuse the source buffers to hold dense IDs
    // until the facts are scattered into their per-person ranges.
    let mut parts = parts;
    for event in parts.iter_mut().flatten() {
        let id = *ids.entry(event.person).or_insert_with(|| {
            let id = names.len();
            names.push(event.person);
            counts.push(0);
            id
        });
        counts[id] += 1;
        event.person = PackedName(id as u64);
    }
    let facts = parts
        .into_iter()
        .flatten()
        .map(|event| (event.person.0 as usize, event.fact));
    let (events, offsets) = group_facts(counts, len, facts);
    let clicks = clicks
        .into_iter()
        .flatten()
        .filter_map(|click| {
            Some(Click {
                user: *ids.get(&click.person)? as u32,
                time: window.start + i64::from(click.offset_ms),
                campaign: click.campaign,
                id: click.id,
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
    links: &LinkNames,
    window: &Window,
    catalogs: &Catalogs,
    outdir: &Path,
) -> Option<()> {
    let links = PackedLinks::from_links(links)?;
    let (events, clicks) = rayon::join(
        || collect_events(event_bufs, window, catalogs, &links),
        || collect_clicks(click_bufs, window, catalogs, &links),
    );
    let parts = events?
        .into_par_iter()
        .zip(clicks?)
        .map(|(events, clicks)| aggregate_small(events, clicks, window, catalogs));
    write_results(outdir, catalogs, parts);
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let links = PackedLinks(FxHashMap::default());
        let mut facts: Vec<_> = route_partition(pieces, 1, &links)
            .into_iter()
            .flatten()
            .map(|event| event.fact.0)
            .collect();
        facts.sort_unstable();
        assert_eq!(facts, [300, 400, 500]);
    }
}
