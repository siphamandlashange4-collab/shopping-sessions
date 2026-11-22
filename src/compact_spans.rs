use super::{chunks, csv, event_fact, exchange, fast, pack_id, shard, ts};
use super::{Buckets, Event, Fact, LinkNames, Window, BAD_TS, SHARDS};
use rayon::prelude::*;
use rustc_hash::{FxBuildHasher, FxHashMap};
use std::collections::hash_map::Entry;
use std::hash::{BuildHasher, Hash};

const LENGTH_BITS: u32 = 10;
const LENGTH_MASK: u32 = (1 << LENGTH_BITS) - 1;
const MAX_CHUNK_LEN: usize = 1 << (u32::BITS - LENGTH_BITS);

#[derive(Clone, Copy, Default)]
struct ChunkSpan(u32);

impl ChunkSpan {
    #[inline]
    fn encode(chunk: &[u8], field: &[u8]) -> Option<Self> {
        let offset = (field.as_ptr() as usize).wrapping_sub(chunk.as_ptr() as usize);
        if offset >= MAX_CHUNK_LEN
            || field.len() > LENGTH_MASK as usize
            || offset + field.len() > chunk.len()
        {
            return None;
        }
        Some(Self(((offset as u32) << LENGTH_BITS) | field.len() as u32))
    }

    #[inline]
    fn get(self, chunk: &'static [u8]) -> &'static [u8] {
        let offset = (self.0 >> LENGTH_BITS) as usize;
        let length = (self.0 & LENGTH_MASK) as usize;
        &chunk[offset..offset + length]
    }
}

trait EventKey {
    type Exact: Eq + Hash;

    fn slot_word(&self) -> usize;
    fn exact(&self, chunk: &'static [u8]) -> Self::Exact;
}

impl EventKey for u64 {
    type Exact = Self;

    #[inline]
    fn slot_word(&self) -> usize {
        *self as usize
    }

    #[inline]
    fn exact(&self, _chunk: &'static [u8]) -> Self::Exact {
        *self
    }
}

struct OpaqueKey {
    span: ChunkSpan,
    hash: u32,
}

impl EventKey for OpaqueKey {
    type Exact = &'static [u8];

    #[inline]
    fn slot_word(&self) -> usize {
        self.hash as usize
    }

    #[inline]
    fn exact(&self, chunk: &'static [u8]) -> Self::Exact {
        self.span.get(chunk)
    }
}

struct Staged<K, F> {
    key: K,
    user: ChunkSpan,
    ingest: i32,
    fact: F,
}

struct ChunkEvents<F> {
    chunk: &'static [u8],
    packed: Buckets<Staged<u64, F>>,
    opaque: Buckets<Staged<OpaqueKey, F>>,
}

struct Piece<K, F> {
    chunk: &'static [u8],
    rows: Vec<Staged<K, F>>,
}

fn distribute<K, F>(
    chunk: &'static [u8],
    rows: Buckets<Staged<K, F>>,
    shards: &mut [Vec<Piece<K, F>>; SHARDS],
) {
    for (shard, rows) in shards.iter_mut().zip(rows) {
        if !rows.is_empty() {
            shard.push(Piece { chunk, rows });
        }
    }
}

#[inline]
fn ingest_delta(ingest: i64, anchor_seconds: i64) -> Option<i32> {
    if ingest == BAD_TS {
        return Some(i32::MIN);
    }
    if ingest % 1000 != 0 {
        return None;
    }
    i32::try_from(ingest / 1000 - anchor_seconds)
        .ok()
        .filter(|&delta| delta != i32::MIN)
}

fn stage<F: Fact>(
    chunk: &'static [u8],
    window: &Window,
    products: &FxHashMap<&'static [u8], u32>,
    unknown: u32,
) -> Option<ChunkEvents<F>> {
    if chunk.len() > MAX_CHUNK_LEN {
        return None;
    }
    let anchor_seconds = window.start.div_euclid(1000);
    let mut packed: Buckets<Staged<u64, F>> = std::array::from_fn(|_| Vec::new());
    let mut opaque: Buckets<Staged<OpaqueKey, F>> = std::array::from_fn(|_| Vec::new());
    let complete = csv::for_each::<7>(chunk, |[id, user, time, ingest, kind, product, price]| {
        if id.is_empty() {
            return true;
        }
        let ingest = ts::parse(ingest).unwrap_or(BAD_TS);
        let Some(delta) = ingest_delta(ingest, anchor_seconds) else {
            return false;
        };
        let fact: F = event_fact(window, products, unknown, user, time, ingest, kind, product, price);
        // Invalid rows can win deduplication too, but they never use the user span.
        let user = if fact.valid() {
            let Some(span) = ChunkSpan::encode(chunk, user) else {
                return false;
            };
            span
        } else {
            ChunkSpan::default()
        };
        let exact = pack_id::<b'e'>(id);
        if exact == 0 {
            let Some(span) = ChunkSpan::encode(chunk, id) else {
                return false;
            };
            let hash = FxBuildHasher.hash_one(id);
            opaque[shard(hash)].push(Staged {
                key: OpaqueKey {
                    span,
                    hash: hash as u32,
                },
                user,
                ingest: delta,
                fact,
            });
        } else {
            let key = fast::event_key(exact);
            packed[shard(key)].push(Staged {
                key,
                user,
                ingest: delta,
                fact,
            });
        }
        true
    });
    complete.then_some(ChunkEvents {
        chunk,
        packed,
        opaque,
    })
}

fn route_winners<K: EventKey, F: Fact>(
    pieces: &[Piece<K, F>],
    links: &LinkNames,
    buckets: &mut Buckets<Event<F>>,
) {
    let total = pieces.iter().map(|piece| piece.rows.len()).sum::<usize>();
    if total == 0 {
        return;
    }
    let slots = total.saturating_mul(8).clamp(64, 1 << 29).next_power_of_two();
    let mut seen = vec![0; slots / 64];
    let mut contested = vec![0; slots / 64];
    let slot_of = |key: &K| key.slot_word() & (slots - 1);
    for row in pieces.iter().flat_map(|piece| &piece.rows) {
        let slot = slot_of(&row.key);
        if fast::mark(&mut seen, slot) {
            fast::mark(&mut contested, slot);
        }
    }

    let mut route = |row: &Staged<K, F>, chunk| {
        if row.fact.valid() {
            let (user, group) = links.route(row.user.get(chunk));
            buckets[group].push(Event { user, fact: row.fact });
        }
    };
    let mut winners = FxHashMap::<K::Exact, (&Staged<K, F>, &[u8])>::with_capacity_and_hasher(
        total / 8,
        Default::default(),
    );
    for piece in pieces {
        for row in &piece.rows {
            let slot = slot_of(&row.key);
            if contested[slot / 64] & (1_u64 << (slot % 64)) == 0 {
                route(row, piece.chunk);
                continue;
            }
            // Consult the complete ID when a bitmap slot is shared.
            match winners.entry(row.key.exact(piece.chunk)) {
                Entry::Vacant(entry) => {
                    entry.insert((row, piece.chunk));
                }
                Entry::Occupied(mut entry) => {
                    if row.ingest < entry.get().0.ingest {
                        entry.insert((row, piece.chunk));
                    }
                }
            }
        }
    }
    for (row, chunk) in winners.into_values() {
        route(row, chunk);
    }
}

pub(super) fn try_events<F: Fact>(
    buffers: &[&'static [u8]],
    window: &Window,
    links: &LinkNames,
    products: &FxHashMap<&'static [u8], u32>,
    unknown: u32,
) -> Option<[Vec<Vec<Event<F>>>; SHARDS]> {
    let staged = chunks(buffers)
        .into_par_iter()
        .map(|chunk| stage::<F>(chunk, window, products, unknown))
        .collect::<Option<Vec<_>>>()?;
    let mut packed = std::array::from_fn(|_| Vec::with_capacity(staged.len()));
    let mut opaque = std::array::from_fn(|_| Vec::with_capacity(staged.len()));
    for rows in staged {
        distribute(rows.chunk, rows.packed, &mut packed);
        distribute(rows.chunk, rows.opaque, &mut opaque);
    }
    let grouped = packed
        .into_par_iter()
        .zip(opaque)
        .map(|(packed, opaque)| {
            let mut buckets = std::array::from_fn(|_| Vec::new());
            route_winners(&packed, links, &mut buckets);
            route_winners(&opaque, links, &mut buckets);
            buckets
        })
        .collect();
    Some(exchange(grouped))
}
