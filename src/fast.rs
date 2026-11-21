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
    id: u64,
    ingest: i64,
    event: SmallEvent,
}

struct SmallClick {
    person: PackedName,
    offset_ms: i32,
    campaign: u32,
    id: &'static [u8],
}

#[inline]
fn first_comma(prefix: &[u8]) -> Option<usize> {
    let Ok(prefix) = <&[u8; 12]>::try_from(prefix) else {
        return memchr::memchr(b',', prefix);
    };

    #[inline]
    fn comma_mask(word: u64) -> u64 {
        let different = word ^ u64::from_le_bytes([b','; 8]);
        // Borrow can mark bytes above the first match. The least significant
        // marked byte is always the first comma.
        different.wrapping_sub(0x0101_0101_0101_0101) & !different & 0x8080_8080_8080_8080
    }

    let first = comma_mask(u64::from_le_bytes(prefix[..8].try_into().unwrap()));
    if first != 0 {
        return Some(first.trailing_zeros() as usize / 8);
    }
    let last = comma_mask(u64::from_le_bytes(prefix[4..].try_into().unwrap()));
    (last != 0).then(|| 4 + last.trailing_zeros() as usize / 8)
}

fn scan_slots(chunks: &[&[u8]], slots: usize) -> Buckets<u32> {
    let mut buckets = std::array::from_fn(|_| Vec::new());
    let shift = (slots / SHARDS).trailing_zeros();
    for chunk in chunks {
        let mut start = 0;
        for end in memchr::memchr_iter(b'\n', chunk).chain(std::iter::once(chunk.len())) {
            // The longest supported first field is nine ID bytes, two
            // optional quotes and a comma. Malformed rows may set bits
            // here. Only the full CSV pass can admit an event.
            let prefix = &chunk[start..end.min(start + 12)];
            start = end + 1;
            let Some(comma) = first_comma(prefix) else {
                continue;
            };
            let field = &prefix[..comma];
            let id = field
                .strip_prefix(b"\"")
                .and_then(|field| field.strip_suffix(b"\""))
                .unwrap_or(field);
            if let Some(id) = pack::<b'e'>(id) {
                let slot = rustc_hash::FxBuildHasher.hash_one(id) as usize & (slots - 1);
                buckets[slot >> shift].push(slot as u32);
            }
        }
    }
    buckets
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
    let slots = (bytes / 16)
        .clamp(64 * SHARDS, (u32::MAX as usize / 2) + 1)
        .next_power_of_two();
    let workers = rayon::current_num_threads().min(8).min(chunks.len().max(1));
    let mut work: Vec<Vec<_>> = (0..workers).map(|_| Vec::new()).collect();
    chunks.sort_unstable_by_key(|chunk| std::cmp::Reverse(chunk.len()));
    for (index, chunk) in chunks.into_iter().enumerate() {
        work[index % workers].push(chunk);
    }

    // Collect slot numbers sequentially into small buckets. Each worker
    // then builds the collision bitmap for one disjoint slice, keeping its
    // working set small and avoiding shared writes and per-lane bitmaps.
    let slots_by_shard = exchange(
        work.par_iter().map(|chunks| scan_slots(chunks, slots)).collect(),
    );
    let slots_per_shard = slots / SHARDS;
    let words_per_shard = slots_per_shard / 64;
    let mut collided = vec![0u64; slots / 64];
    collided
        .par_chunks_mut(words_per_shard)
        .zip(slots_by_shard.into_par_iter())
        .for_each(|(collided, buckets)| {
            let mut seen = vec![0u64; words_per_shard];
            for slot in buckets.into_iter().flatten() {
                let slot = slot as usize & (slots_per_shard - 1);
                if mark(&mut seen, slot) {
                    mark(collided, slot);
                }
            }
        });

    // IDs without a contested slot go straight to their person. Every copy
    // of an ID with a contested slot enters the same exact dedup shard.
    let (routed, possible): (Vec<_>, Vec<_>) = work
        .into_par_iter()
        .map(|chunks| {
            let mut people: Buckets<SmallEvent> = std::array::from_fn(|_| Vec::new());
            let mut possible: Buckets<Candidate> = std::array::from_fn(|_| Vec::new());
            let mut ingest_dates = ts::IngestDates::default();
            for chunk in chunks {
                if !csv::for_each::<7>(chunk, |[id, person, time, ingest, kind, product, price]| {
                    if id.is_empty() {
                        return true;
                    }
                    let Some(id) = pack::<b'e'>(id) else {
                        return false;
                    };
                    let ingest = ingest_dates.parse(ingest).unwrap_or(BAD_TS);
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
                    let hash = rustc_hash::FxBuildHasher.hash_one(id);
                    let slot = hash as usize & (slots - 1);
                    if collided[slot / 64] & (1_u64 << (slot % 64)) != 0 {
                        possible[shard(hash)].push(Candidate { id, ingest, event });
                    } else {
                        route(event, links, &mut people);
                    }
                    true
                }) {
                    return None;
                }
            }
            Some((people, possible))
        })
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .unzip();

    let duplicates: Vec<_> = exchange(possible)
        .into_par_iter()
        .map(|pieces| {
            use std::collections::hash_map::Entry;
            let size: usize = pieces.iter().map(Vec::len).sum();
            let mut winners = FxHashMap::with_capacity_and_hasher(size, Default::default());
            for candidate in pieces.iter().flatten() {
                let value = (candidate.ingest, candidate.event);
                match winners.entry(candidate.id) {
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
            let mut buckets = std::array::from_fn(|_| Vec::new());
            for (_, event) in winners.into_values() {
                route(event, links, &mut buckets);
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
    fn slot_scan_covers_every_supported_event_id() {
        let chunks = [
            &b"e1,,,x,y,z,t\n\"e12345678\",u,t,i,k,p,p\n\"e1\",u,t,i,k,p,p\n"[..],
            &b"\"E12345678\",u,t,i,k,p,p\ninvalid\ne12345678,u,t,i,k,p,p"[..],
        ];
        let slots = 4096;
        let mut expected = Vec::new();
        for chunk in chunks {
            csv::for_each::<7>(chunk, |fields| {
                let id = pack::<b'e'>(fields[0]).unwrap();
                expected.push((rustc_hash::FxBuildHasher.hash_one(id) as usize & (slots - 1)) as u32);
                true
            });
        }
        let mut actual: Vec<_> = scan_slots(&chunks, slots).into_iter().flatten().collect();
        actual.sort_unstable();
        expected.sort_unstable();
        assert_eq!(actual, expected);
    }
}
