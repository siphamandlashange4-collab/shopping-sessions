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
    id: u64,
    ingest: i64,
    event: SmallEvent,
}

struct SmallClick {
    person: PackedName,
    time: i64,
    campaign: u32,
    id: &'static [u8],
}

struct Lane {
    rows: Vec<Candidate>,
    seen: Vec<u64>,
    repeated: Vec<usize>,
}

impl Lane {
    // Each lane owns its bitmap. Collisions within a lane are saved as slot
    // indices so the lanes share no writable memory while parsing.
    fn new(words: usize, input_bytes: usize) -> Self {
        Self {
            rows: Vec::with_capacity(input_bytes / 64),
            seen: vec![0; words],
            repeated: Vec::new(),
        }
    }

    #[inline]
    fn push(&mut self, candidate: Candidate) {
        let slot = slot_of(candidate.id, self.seen.len() * 64 - 1);
        if mark(&mut self.seen, slot) {
            self.repeated.push(slot);
        }
        self.rows.push(candidate);
    }
}

#[inline]
fn slot_of(id: u64, mask: usize) -> usize {
    rustc_hash::FxBuildHasher.hash_one(id) as usize & mask
}

#[inline]
fn mark(words: &mut [u64], index: usize) -> bool {
    let bit = 1_u64 << (index % 64);
    let word = &mut words[index / 64];
    let already_set = *word & bit != 0;
    *word |= bit;
    already_set
}

fn collect_events(
    buffers: &[&'static [u8]],
    window: &Window,
    catalogs: &Catalogs,
    links: &PackedLinks,
) -> Option<[Vec<Vec<SmallEvent>>; SHARDS]> {
    let mut chunks = chunks(buffers);
    let bytes = chunks.iter().map(|chunk| chunk.len()).sum::<usize>();
    // Bitmap sizing depends only on input size. Hash collisions cause more
    // exact comparisons, never a change to which event wins.
    let slots = (bytes / 16).max(1024).next_power_of_two();
    let workers = rayon::current_num_threads().min(8).min(chunks.len().max(1));
    let mut work: Vec<Vec<_>> = (0..workers).map(|_| Vec::new()).collect();
    chunks.sort_unstable_by_key(|chunk| std::cmp::Reverse(chunk.len()));
    for (index, chunk) in chunks.into_iter().enumerate() {
        work[index % workers].push(chunk);
    }

    let lanes: Vec<Lane> = work
        .into_par_iter()
        .map(|chunks| {
            let mut lane = Lane::new(slots / 64, chunks.iter().map(|chunk| chunk.len()).sum());
            for chunk in chunks {
                if !csv::for_each::<7>(chunk, |[id, person, time, ingest, kind, product, price]| {
                    if id.is_empty() {
                        return true;
                    }
                    let Some(id) = pack::<b'e'>(id) else {
                        return false;
                    };
                    let ingest = ts::parse(ingest).unwrap_or(BAD_TS);
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
                    lane.push(Candidate {
                        id,
                        ingest,
                        event: SmallEvent { person, fact },
                    });
                    true
                }) {
                    return None;
                }
            }
            Some(lane)
        })
        .collect::<Option<_>>()?;

    let mut collided: Vec<_> = (0..slots / 64)
        .into_par_iter()
        .with_min_len(4096)
        .map(|word| {
            let mut seen = 0;
            let mut collided = 0;
            for lane in &lanes {
                let local = lane.seen[word];
                collided |= seen & local;
                seen |= local;
            }
            collided
        })
        .collect();
    for lane in &lanes {
        for &slot in &lane.repeated {
            mark(&mut collided, slot);
        }
    }

    // Most IDs only occupy one slot and can go straight to their person.
    // Every copy of an ID with a contested slot enters the same exact shard.
    let (routed, possible): (Vec<_>, Vec<_>) = lanes
        .into_par_iter()
        .map(|lane| {
            let mut people: Buckets<SmallEvent> = std::array::from_fn(|_| Vec::new());
            let mut possible: Buckets<Candidate> = std::array::from_fn(|_| Vec::new());
            for candidate in lane.rows {
                let index = slot_of(candidate.id, slots - 1);
                if collided[index / 64] & (1_u64 << (index % 64)) != 0 {
                    let hash = rustc_hash::FxBuildHasher.hash_one(candidate.id);
                    possible[shard(hash)].push(candidate);
                } else {
                    route(candidate.event, links, &mut people);
                }
            }
            (people, possible)
        })
        .unzip();

    let duplicates: Vec<_> = exchange(possible)
        .into_par_iter()
        .map(|pieces| {
            use std::collections::hash_map::Entry;
            let size: usize = pieces.iter().map(Vec::len).sum();
            let mut winners = FxHashMap::with_capacity_and_hasher(size, Default::default());
            for candidate in pieces.iter().flatten() {
                match winners.entry(candidate.id) {
                    Entry::Vacant(entry) => {
                        entry.insert(candidate);
                    }
                    Entry::Occupied(mut entry) => {
                        if candidate.ingest < entry.get().ingest {
                            entry.insert(candidate);
                        }
                    }
                }
            }
            let mut buckets = std::array::from_fn(|_| Vec::new());
            for winner in winners.into_values() {
                route(winner.event, links, &mut buckets);
            }
            buckets
        })
        .collect();

    let mut groups = exchange(routed);
    for buckets in duplicates {
        for (group, records) in groups.iter_mut().zip(buckets) {
            if !records.is_empty() {
                group.push(records);
            }
        }
    }
    Some(groups)
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
                    time,
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
    let mut facts = Vec::with_capacity(len);
    for event in parts.into_iter().flatten() {
        let id = *ids.entry(event.person).or_insert_with(|| {
            let id = names.len();
            names.push(event.person);
            counts.push(0);
            id
        });
        counts[id] += 1;
        facts.push((id, event.fact));
    }
    let (events, offsets) = group_facts(counts, facts);
    let clicks = clicks
        .into_iter()
        .flatten()
        .filter_map(|click| {
            Some(Click {
                user: *ids.get(&click.person)? as u32,
                time: click.time,
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
