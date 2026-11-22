//! Shopping Sessions pipeline.
//!
//! Reads $DATA_DIR, writes $OUTPUT_DIR/user_daily/ and $OUTPUT_DIR/campaign_daily/
//! as Parquet. Implements rules.md exactly; see that file for the spec.

mod attribution;
mod csv;
mod fast;
mod plain_parquet;
mod time_bins;
mod ts;

use attribution::{Attribution, CampaignTable, CampaignTotals, Click, GroupedClicks};
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::fs;
use std::hash::BuildHasher;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;

const DAY_MS: i64 = 86_400_000;
const SESSION_GAP_MS: i64 = 1_800_000; // 30 minutes
const SEVEN_DAYS_MS: i64 = 7 * DAY_MS;
const LATE_MS: i64 = DAY_MS; // 24 hours
const BAD_TS: i64 = i64::MIN;

const SHARDS: usize = 64;



static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
static TIMING: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// Stage timer, enabled by TIMING=1. Prints elapsed time since process start.
pub(crate) fn lap(name: &str) {
    if *TIMING.get_or_init(|| std::env::var_os("TIMING").is_some()) {
        let t = START.get_or_init(std::time::Instant::now).elapsed();
        eprintln!("{:>8.3}s  {}", t.as_secs_f64(), name);
    }
}
const ORGANIC: u32 = 0;

/// Read every *.csv in a directory, in sorted order, as leaked 'static buffers.
fn read_dir_csvs(dir: &Path) -> Vec<&'static [u8]> {
    let mut paths: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false))
            .collect(),
        Err(_) => Vec::new(),
    };
    paths.sort();
    paths.into_par_iter().map(|path| read_log(&path)).collect()
}

fn read_log(path: &Path) -> &'static [u8] {
    #[cfg(target_os = "linux")]
    if let Some(bytes) = map_log(path) {
        return bytes;
    }
    read_file(path)
}

#[cfg(target_os = "linux")]
fn map_log(path: &Path) -> Option<&'static [u8]> {
    use std::os::fd::AsRawFd;
    let file = fs::File::open(path).ok()?;
    let metadata = file.metadata().ok()?;
    let len = usize::try_from(metadata.len()).ok()?;
    if !metadata.is_file() || !(64 * 1024..=isize::MAX as usize).contains(&len) {
        return None;
    }
    // The pipeline's input files are immutable for the run. The private,
    // read-only mapping remains valid until the process exits.
    let address = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ,
            libc::MAP_PRIVATE,
            file.as_raw_fd(),
            0,
        )
    };
    if address == libc::MAP_FAILED || address.is_null() {
        return None;
    }
    Some(unsafe { std::slice::from_raw_parts(address.cast(), len) })
}

fn read_file(path: &Path) -> &'static [u8] {
    let v = fs::read(path).unwrap_or_default();
    &*Box::leak(v.into_boxed_slice())
}

/// Minimal extraction of a `"key": "value"` string from the manifest.
fn manifest_value(buf: &[u8], key: &str) -> Option<i64> {
    let pat = format!("\"{}\"", key);
    let i = find(buf, pat.as_bytes())? + pat.len();
    let rest = &buf[i..];
    let q1 = memchr::memchr(b'"', rest)?;
    let after = &rest[q1 + 1..];
    let q2 = memchr::memchr(b'"', after)?;
    ts::parse(&after[..q2])
}

fn find(h: &[u8], n: &[u8]) -> Option<usize> {
    h.windows(n.len()).position(|w| w == n)
}

// ---------------------------------------------------------------- interner

/// Interns byte strings to dense u32 ids.
struct Interner<const PREFIX: u8> {
    packed: FxHashMap<u64, u32>,
    map: FxHashMap<&'static [u8], u32>,
    vals: Vec<&'static [u8]>,
}

type UserInterner = Interner<b'u'>;
type CampaignInterner = Interner<0>;

impl<const PREFIX: u8> Interner<PREFIX> {
    fn new() -> Self {
        Self::with_capacity(0)
    }
    fn with_capacity(n: usize) -> Self {
        Interner {
            packed: FxHashMap::with_capacity_and_hasher(
                if PREFIX == 0 { 0 } else { n },
                Default::default(),
            ),
            map: FxHashMap::with_capacity_and_hasher(
                if PREFIX == 0 { n } else { n.min(64) },
                Default::default(),
            ),
            vals: Vec::with_capacity(n),
        }
    }
    #[inline]
    fn intern(&mut self, s: &'static [u8]) -> u32 {
        let packed = pack_id::<PREFIX>(s);
        let add = || {
            let id = self.vals.len() as u32;
            self.vals.push(s);
            id
        };
        if packed == 0 {
            *self.map.entry(s).or_insert_with(add)
        } else {
            *self.packed.entry(packed).or_insert_with(add)
        }
    }
    #[inline]
    fn get(&self, s: &[u8]) -> Option<u32> {
        self.get_key(s, pack_id::<PREFIX>(s))
    }
    #[inline]
    fn get_key(&self, s: &[u8], packed: u64) -> Option<u32> {
        if packed == 0 {
            self.map.get(s).copied()
        } else {
            self.packed.get(&packed).copied()
        }
    }
    #[inline]
    fn name(&self, id: u32) -> &'static [u8] {
        self.vals[id as usize]
    }
    fn len(&self) -> usize {
        self.vals.len()
    }
}

#[derive(Clone, Copy)]
struct Event<F> {
    user: &'static [u8],
    fact: F,
}

struct Candidate<K, F> {
    id: K,
    ingest: i64,
    event: Event<F>,
}

struct RawClick {
    user: &'static [u8],
    id: &'static [u8],
    time: i64,
    campaign: u32,
}

#[derive(Clone, Copy, Default)]
struct WideFact {
    time: i64,
    price: u32,
    category: u32,
}

trait Fact: Copy + Default + Send + Sync {
    const INVALID: Self;
    fn encode(window: &Window, time: i64, price: u32, category: u32) -> Self;
    fn valid(self) -> bool;
    fn order(self) -> i64;
    fn time(self, window: &Window) -> i64;
    fn purchase(self) -> (u32, u32);
}

impl Fact for WideFact {
    const INVALID: Self = Self {
        time: BAD_TS,
        price: 0,
        category: 0,
    };

    #[inline]
    fn encode(_: &Window, time: i64, price: u32, category: u32) -> Self {
        Self {
            time,
            price,
            category,
        }
    }

    #[inline]
    fn valid(self) -> bool {
        self.time != BAD_TS
    }

    #[inline]
    fn order(self) -> i64 {
        self.time
    }

    #[inline]
    fn time(self, _: &Window) -> i64 {
        self.time
    }

    #[inline]
    fn purchase(self) -> (u32, u32) {
        (self.price, self.category)
    }
}

/// Common windows and catalogs fit the event fact in one word. Larger inputs
/// use WideFact with the same pipeline and no truncation.
#[derive(Clone, Copy, Default)]
struct CompactFact(u64);

impl CompactFact {
    const TIME_BITS: u32 = 31;
    const CATEGORY_BITS: u32 = 9;
    const PRICE_BITS: u32 = 24;
    const TIME_SHIFT: u32 = Self::CATEGORY_BITS + Self::PRICE_BITS;

    fn fits(window: &Window, categories: usize) -> bool {
        window.end.saturating_sub(window.start) <= 1 << Self::TIME_BITS
            && categories <= 1 << Self::CATEGORY_BITS
    }
}

impl Fact for CompactFact {
    const INVALID: Self = Self(u64::MAX);

    #[inline]
    fn encode(window: &Window, time: i64, price: u32, category: u32) -> Self {
        Self(
            ((time - window.start) as u64) << Self::TIME_SHIFT
                | (u64::from(category) << Self::PRICE_BITS)
                | u64::from(price),
        )
    }

    #[inline]
    fn valid(self) -> bool {
        self.0 != Self::INVALID.0
    }

    #[inline]
    fn order(self) -> i64 {
        (self.0 >> Self::TIME_SHIFT) as i64
    }

    #[inline]
    fn time(self, window: &Window) -> i64 {
        window.start + self.order()
    }

    #[inline]
    fn purchase(self) -> (u32, u32) {
        let price = self.0 & ((1 << Self::PRICE_BITS) - 1);
        let category = (self.0 >> Self::PRICE_BITS) & ((1 << Self::CATEGORY_BITS) - 1);
        (price as u32, category as u32)
    }
}

struct Window {
    start: i64,
    end: i64,
}

impl Window {
    fn campaign_table(&self, campaigns: usize) -> CampaignTable {
        CampaignTable::new(campaigns, ts::day_of(self.start), ts::day_of(self.end - 1))
    }
}

/// Exact catalog lookups that skip hashing for canonical IDs: a prefix
/// followed by a decimal number with no leading zero (`p23389`, `c_1206`)
/// indexes an array directly. Every other spelling (`P123`, `p0123`, `p12 `,
/// an empty string) goes to an exact byte-string map, so a canonical spelling
/// can only ever match a canonical catalog entry and the answers are the
/// same as a single hash map's.
pub(crate) struct DirectIds {
    prefix: &'static [u8],
    table: Vec<u32>, // id + 1; 0 means absent
    other: FxHashMap<&'static [u8], u32>,
}

impl DirectIds {
    const MAX_TABLE: usize = 1 << 22;

    fn new(prefix: &'static [u8]) -> Self {
        Self {
            prefix,
            table: Vec::new(),
            other: FxHashMap::default(),
        }
    }

    #[inline]
    fn number(&self, key: &[u8]) -> Option<usize> {
        let digits = key.strip_prefix(self.prefix)?;
        if digits.is_empty() || digits.len() > 7 || digits[0] == b'0' {
            return None;
        }
        let mut value = 0usize;
        for &d in digits {
            let d = d.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            value = value * 10 + d as usize;
        }
        Some(value)
    }

    /// Insert or overwrite, as a map insert would.
    fn insert(&mut self, key: &'static [u8], id: u32) {
        match self.number(key) {
            Some(n) if n < Self::MAX_TABLE => {
                if self.table.len() <= n {
                    self.table.resize(n + 1, 0);
                }
                self.table[n] = id + 1;
            }
            _ => {
                self.other.insert(key, id);
            }
        }
    }
}

pub(crate) trait Lookup: Sync {
    fn lookup(&self, key: &[u8]) -> Option<u32>;
}

impl Lookup for DirectIds {
    #[inline]
    fn lookup(&self, key: &[u8]) -> Option<u32> {
        match self.number(key) {
            Some(n) => match self.table.get(n) {
                Some(&slot) if slot != 0 => Some(slot - 1),
                // A canonical key beyond the table was stored in `other`.
                _ if n >= Self::MAX_TABLE => self.other.get(key).copied(),
                _ => None,
            },
            None => self.other.get(key).copied(),
        }
    }
}

impl Lookup for FxHashMap<&'static [u8], u32> {
    #[inline]
    fn lookup(&self, key: &[u8]) -> Option<u32> {
        self.get(key).copied()
    }
}

struct Catalogs {
    products: FxHashMap<&'static [u8], u32>,
    product_ids: DirectIds,
    campaign_ids: DirectIds,
    unknown_category: u32,
    categories: Vec<&'static str>,
    campaigns: CampaignInterner,
    channels: Vec<&'static [u8]>,
}

struct LinkNames {
    users: UserInterner,
    person: Vec<(&'static [u8], usize)>,
}

impl LinkNames {
    fn build(buffers: &[&'static [u8]]) -> Self {
        let pairs: Vec<_> = buffers
            .par_iter()
            .flat_map_iter(|buffer| {
                csv::Lines::new(buffer).filter_map(|line| {
                    let [a, b] = csv::split_n::<2>(line)?;
                    (!a.is_empty() && !b.is_empty()).then_some((a, b))
                })
            })
            .collect();
        let mut users = UserInterner::with_capacity(pairs.len() * 2);
        let mut parent = Vec::new();
        let mut rank = Vec::new();
        for (a, b) in pairs {
            let (a, b) = (users.intern(a), users.intern(b));
            while parent.len() < users.len() {
                parent.push(parent.len() as u32);
                rank.push(0);
            }
            let (mut a, mut b) = (find_root(&mut parent, a), find_root(&mut parent, b));
            if a == b {
                continue;
            }
            if rank[a as usize] > rank[b as usize] {
                std::mem::swap(&mut a, &mut b);
            }
            parent[a as usize] = b;
            rank[b as usize] += u8::from(rank[a as usize] == rank[b as usize]);
        }
        let mut smallest: Vec<u32> = (0..users.len() as u32).collect();
        for id in 0..users.len() as u32 {
            let root = find_root(&mut parent, id) as usize;
            if users.name(id) < users.name(smallest[root]) {
                smallest[root] = id;
            }
        }
        let person = (0..users.len() as u32)
            .map(|id| {
                let name = users.name(smallest[find_root(&mut parent, id) as usize]);
                (name, user_shard(name, pack_id::<b'u'>(name)))
            })
            .collect();
        Self { users, person }
    }

    #[inline]
    fn route(&self, user: &'static [u8]) -> (&'static [u8], usize) {
        let key = pack_id::<b'u'>(user);
        match self.users.get_key(user, key) {
            Some(id) => self.person[id as usize],
            None => (user, user_shard(user, key)),
        }
    }
}

fn find_root(parent: &mut [u32], mut id: u32) -> u32 {
    while parent[id as usize] != id {
        let grandparent = parent[parent[id as usize] as usize];
        parent[id as usize] = grandparent;
        id = grandparent;
    }
    id
}

/// Make evenly sized work units independent of the number or sizes of the files.
fn chunks(buffers: &[&'static [u8]]) -> Vec<&'static [u8]> {
    const TARGET: usize = 2 * 1024 * 1024;
    let mut chunks = Vec::new();
    for &buffer in buffers {
        let Some(header) = memchr::memchr(b'\n', buffer) else {
            continue;
        };
        let mut rest = &buffer[header + 1..];
        while rest.len() > TARGET {
            let Some(next) = memchr::memchr(b'\n', &rest[TARGET..]) else {
                break;
            };
            let (chunk, tail) = rest.split_at(TARGET + next + 1);
            chunks.push(chunk);
            rest = tail;
        }
        if !rest.is_empty() {
            chunks.push(rest);
        }
    }
    chunks
}

type Buckets<T> = [Vec<T>; SHARDS];

/// Runs `work` on every item in parallel with one accumulator per worker
/// thread. However rayon splits the list, each thread fills a single set of
/// buffers, created on its first item. Returns None if any call failed.
fn per_thread<I: Sync, T: Send>(
    items: &[I],
    make: impl Fn() -> T + Sync,
    work: impl Fn(&mut T, &I) -> bool + Sync,
) -> Option<Vec<T>> {
    use std::sync::atomic::{AtomicBool, Ordering};
    let threads = rayon::current_num_threads();
    let slots: Vec<std::sync::Mutex<Option<T>>> =
        (0..=threads).map(|_| std::sync::Mutex::new(None)).collect();
    let failed = AtomicBool::new(false);
    items.par_iter().for_each(|item| {
        if failed.load(Ordering::Relaxed) {
            return;
        }
        // `work` never calls back into rayon, so a worker can't be handed
        // another item while it holds its own slot.
        let index = rayon::current_thread_index().map_or(threads, |index| index.min(threads));
        let mut slot = slots[index].lock().unwrap_or_else(|poison| poison.into_inner());
        let accumulator = slot.get_or_insert_with(&make);
        if !work(accumulator, item) {
            failed.store(true, Ordering::Relaxed);
        }
    });
    if failed.load(Ordering::Relaxed) {
        return None;
    }
    Some(
        slots
            .into_iter()
            .filter_map(|slot| slot.into_inner().unwrap_or_else(|poison| poison.into_inner()))
            .collect(),
    )
}

/// Exchange whole vectors so workers can consume one shard without locks.
fn exchange<T>(buckets: Vec<Buckets<T>>) -> [Vec<Vec<T>>; SHARDS] {
    let mut shards = std::array::from_fn(|_| Vec::with_capacity(buckets.len()));
    for source in buckets {
        for (shard, records) in shards.iter_mut().zip(source) {
            if !records.is_empty() {
                shard.push(records);
            }
        }
    }
    shards
}

#[inline]
fn shard(hash: u64) -> usize {
    // Leave the low bucket bits and the high hash-table fingerprint bits free.
    (hash >> 48) as usize & (SHARDS - 1)
}

#[inline]
fn user_shard(user: &[u8], packed: u64) -> usize {
    let hash = if packed == 0 {
        rustc_hash::FxBuildHasher.hash_one(user)
    } else {
        rustc_hash::FxBuildHasher.hash_one(packed)
    };
    shard(hash)
}

/// Short IDs beginning with the selected prefix fit losslessly in a word. A
/// nonzero last byte distinguishes the length after zero padding. All other
/// IDs still use their complete bytes as the key.
#[inline]
fn pack_id<const PREFIX: u8>(id: &[u8]) -> u64 {
    if PREFIX == 0 {
        return 0;
    }
    if let [first, tail @ ..] = id {
        if *first != PREFIX {
            return 0;
        }
        if tail.len() == 8 {
            let key = u64::from_le_bytes(tail.try_into().unwrap());
            if key >> 56 != 0 {
                return key;
            }
        }
        if let 1..=7 = tail.len() {
            if tail[tail.len() - 1] != 0 {
                let mut bytes = [0; 8];
                bytes[..tail.len()].copy_from_slice(tail);
                return u64::from_le_bytes(bytes);
            }
        }
    }
    0
}

#[inline]
fn event_fact<F: Fact, L: Lookup + ?Sized>(
    window: &Window,
    products: &L,
    unknown: u32,
    user: &[u8],
    time: &[u8],
    ingest: i64,
    kind: &[u8],
    product: &[u8],
    price: &[u8],
) -> F {
    if user.is_empty() || product.is_empty() || ingest == BAD_TS {
        return F::INVALID;
    }
    let Some(time) = ts::parse(time).filter(|time| (window.start..window.end).contains(time))
    else {
        return F::INVALID;
    };
    if ingest - time > LATE_MS {
        return F::INVALID;
    }
    match kind {
        b"view" | b"cart" => F::encode(window, time, 0, 0),
        b"purchase" if csv::all_digits(price) => {
            let amount = csv::parse_u64(price);
            if !(1..=10_000_000).contains(&amount) {
                return F::INVALID;
            }
            let category = products.lookup(product).unwrap_or(unknown);
            F::encode(window, time, amount as u32, category)
        }
        _ => F::INVALID,
    }
}

fn user_events<F: Fact>(
    buffers: &[&'static [u8]],
    window: &Window,
    links: &LinkNames,
    products: &FxHashMap<&'static [u8], u32>,
    unknown: u32,
) -> [Vec<Vec<Event<F>>>; SHARDS] {
    let (packed_pieces, fallback_pieces) = chunks(buffers)
        .into_par_iter()
        .map(|chunk| {
            let mut packed_buckets: Buckets<Candidate<u64, F>> =
                std::array::from_fn(|_| Vec::new());
            let mut fallback_buckets: Buckets<Candidate<&[u8], F>> =
                std::array::from_fn(|_| Vec::new());
            for [id, user, time, ingest, kind, product, price] in csv::Rows::<7>::raw(chunk) {
                if id.is_empty() {
                    continue;
                }
                let ingest = ts::parse(ingest).unwrap_or(BAD_TS);
                let fact = event_fact(
                    window, products, unknown, user, time, ingest, kind, product, price,
                );
                let event = Event { user, fact };
                // Invalid rows still participate in the earliest-ingest election.
                let packed = pack_id::<b'e'>(id);
                if packed == 0 {
                    let hash = rustc_hash::FxBuildHasher.hash_one(id);
                    fallback_buckets[shard(hash)].push(Candidate { id, ingest, event });
                } else {
                    let hash = rustc_hash::FxBuildHasher.hash_one(packed);
                    packed_buckets[shard(hash)].push(Candidate {
                        id: packed,
                        ingest,
                        event,
                    });
                }
            }
            (packed_buckets, fallback_buckets)
        })
        .unzip();

    let grouped = exchange(packed_pieces)
        .into_par_iter()
        .zip(exchange(fallback_pieces))
        .map(|(packed, fallback)| {
            let mut buckets = std::array::from_fn(|_| Vec::new());
            route_winners(&packed, links, &mut buckets);
            route_winners(&fallback, links, &mut buckets);
            buckets
        })
        .collect();
    exchange(grouped)
}

fn route_winners<K: std::hash::Hash + Eq + Copy, F: Fact>(
    pieces: &[Vec<Candidate<K, F>>],
    links: &LinkNames,
    buckets: &mut Buckets<Event<F>>,
) {
    use std::collections::hash_map::Entry;
    let mut total = 0;
    let offsets: Vec<_> = pieces
        .iter()
        .map(|piece| {
            let offset = total;
            total += piece.len();
            offset
        })
        .collect();
    // Access another row only when its ID actually repeats. Keep the large
    // candidate scan sequential and the hash-table values eight bytes wide.
    let mut dropped = vec![false; total];
    let mut winners: FxHashMap<K, (u32, u32)> =
        FxHashMap::with_capacity_and_hasher(total, Default::default());
    for (piece, rows) in pieces.iter().enumerate() {
        for (row, candidate) in rows.iter().enumerate() {
            let current = (piece as u32, row as u32);
            match winners.entry(candidate.id) {
                Entry::Vacant(entry) => {
                    entry.insert(current);
                }
                Entry::Occupied(mut entry) => {
                    let &(prior_piece, prior_row) = entry.get();
                    let (a, b) = (prior_piece as usize, prior_row as usize);
                    if candidate.ingest < pieces[a][b].ingest {
                        dropped[offsets[a] + b] = true;
                        entry.insert(current);
                    } else {
                        dropped[offsets[piece] + row] = true;
                    }
                }
            }
        }
    }
    drop(winners);
    for (candidate, dropped) in pieces.iter().flatten().zip(dropped) {
        let mut event = candidate.event;
        if !dropped && event.fact.valid() {
            let (user, group) = links.route(event.user);
            event.user = user;
            buckets[group].push(event);
        }
    }
}

fn user_clicks(
    buffers: &[&'static [u8]],
    window: &Window,
    links: &LinkNames,
    campaigns: &CampaignInterner,
) -> [Vec<Vec<RawClick>>; SHARDS] {
    let click_start = window.start - SEVEN_DAYS_MS;
    let pieces = chunks(buffers)
        .into_par_iter()
        .map(|chunk| {
            let mut buckets = std::array::from_fn(|_| Vec::new());
            for [id, user, campaign, time] in csv::Rows::<4>::raw(chunk) {
                if id.is_empty() || user.is_empty() {
                    continue;
                }
                let Some(time) = ts::parse(time) else {
                    continue;
                };
                if !(click_start..window.end).contains(&time) {
                    continue;
                }
                let Some(campaign) = campaigns.get(campaign).filter(|&id| id != ORGANIC) else {
                    continue;
                };
                let (user, group) = links.route(user);
                // Identical click copies have the same attribution tie-break and
                // occupy one campaign in the sliding window, even if both are kept.
                buckets[group].push(RawClick {
                    user,
                    id,
                    time,
                    campaign,
                });
            }
            buckets
        })
        .collect();
    exchange(pieces)
}

#[derive(Default)]
struct UserDay {
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    category: Option<u32>,
    best_category_revenue: i64,
}

#[derive(Default)]
struct UserRows<N> {
    names: Vec<N>,
    day: Vec<i32>,
    sessions: Vec<i64>,
    events: Vec<i64>,
    revenue: Vec<i64>,
    category: Vec<Option<&'static str>>,
}

trait OutputName: Copy + Default + Send {
    fn string_array(names: Vec<Self>) -> StringArray;

    fn write_plain(_: &Path, _: &UserRows<Self>) -> bool {
        false
    }
}

impl OutputName for &'static str {
    fn string_array(names: Vec<Self>) -> StringArray {
        StringArray::from_iter_values(names)
    }
}

impl<N: OutputName> UserRows<N> {
    fn push(&mut self, name: N, day: &UserDay, categories: &[&'static str]) {
        self.names.push(name);
        self.day.push(day.day);
        self.sessions.push(day.sessions);
        self.events.push(day.events);
        self.revenue.push(day.revenue);
        self.category
            .push(day.category.map(|id| categories[id as usize]));
    }
}

fn aggregate<F: Fact>(
    event_parts: Vec<Vec<Event<F>>>,
    click_parts: Vec<Vec<RawClick>>,
    categories: &[&'static str],
    campaign_count: usize,
    window: &Window,
) -> (UserRows<&'static str>, CampaignTable) {
    let num_events = event_parts.iter().map(Vec::len).sum::<usize>();
    let mut names = UserInterner::with_capacity(num_events / 12);
    let mut named_events = Vec::with_capacity(num_events);
    let mut counts = Vec::new();
    for event in event_parts.into_iter().flatten() {
        let user = names.intern(event.user) as usize;
        if user == counts.len() {
            counts.push(0);
        }
        counts[user] += 1;
        named_events.push((user, event.fact));
    }
    let (events, offsets) = group_facts(counts, named_events.len(), named_events);
    let clicks: Vec<_> = click_parts
        .into_iter()
        .flatten()
        .filter_map(|click| {
            Some(Click {
                time: click.time,
                user: names.get(click.user)?,
                campaign: click.campaign,
                id: click.id,
                order: attribution::id_order(click.id),
            })
        })
        .collect();
    let names = names
        .vals
        .into_iter()
        .map(|name| std::str::from_utf8(name).expect("UTF-8 user ID"));
    summarize(
        events,
        offsets,
        names,
        clicks,
        categories,
        campaign_count,
        window,
    )
}

// Group by the dense person ID in linear time, then sort only within people.
fn group_facts<F: Fact>(
    counts: Vec<usize>,
    len: usize,
    named_events: impl IntoIterator<Item = (usize, F)>,
) -> (Vec<F>, Vec<usize>) {
    let mut end = 0;
    let mut offsets: Vec<_> = counts
        .into_iter()
        .map(|count| {
            let start = end;
            end += count;
            start
        })
        .collect();
    let mut events = vec![F::default(); len];
    for (user, event) in named_events {
        events[offsets[user]] = event;
        offsets[user] += 1;
    }
    (events, offsets)
}

struct UserDayWriter<'a, N> {
    name: N,
    rows: &'a mut UserRows<N>,
    categories: &'a [&'static str],
    day: UserDay,
    totals: &'a mut [i64],
    touched: &'a mut Vec<usize>,
}

impl<N: OutputName> UserDayWriter<'_, N> {
    #[inline]
    fn start_session(&mut self, start: i64, events: i64) -> i32 {
        let day = ts::day_of(start);
        if self.day.sessions != 0 && self.day.day != day {
            self.flush();
            self.day = UserDay::default();
        }
        self.day.day = day;
        self.day.sessions += 1;
        self.day.events += events;
        day
    }

    #[inline]
    fn purchase(&mut self, category: u32, amount: i64) {
        let total = &mut self.totals[category as usize];
        if *total == 0 {
            self.touched.push(category as usize);
        }
        *total += amount;
        if *total > self.day.best_category_revenue
            || (*total == self.day.best_category_revenue && Some(category) < self.day.category)
        {
            self.day.best_category_revenue = *total;
            self.day.category = Some(category);
        }
    }

    fn flush(&mut self) {
        self.rows.push(self.name, &self.day, self.categories);
        for index in self.touched.drain(..) {
            self.totals[index] = 0;
        }
    }
}

fn summarize<F: Fact, N: OutputName>(
    mut events: Vec<F>,
    offsets: Vec<usize>,
    names: impl Iterator<Item = N>,
    clicks: Vec<Click>,
    categories: &[&'static str],
    campaign_count: usize,
    window: &Window,
) -> (UserRows<N>, CampaignTable) {
    let mut clicks = GroupedClicks::new(clicks, offsets.len());
    let mut rows = UserRows::default();
    let mut campaign_totals = window.campaign_table(campaign_count);
    let mut attribution = Attribution::new(campaign_count);
    let mut category_totals = vec![0; categories.len()];
    let mut touched = Vec::new();
    let mut session_bins = None;
    let mut event_pos = 0;
    for (user, (end, name)) in offsets.into_iter().zip(names).enumerate() {
        let user_events = &mut events[event_pos..end];
        event_pos = end;
        let user_clicks = clicks.for_user(user as u32);
        attribution.reset();
        let mut output = UserDayWriter {
            name,
            rows: &mut rows,
            categories,
            day: UserDay::default(),
            totals: &mut category_totals,
            touched: &mut touched,
        };

        // Bins avoid sorting large groups. Keep the small-user sort and the
        // same session output path for windows that need too many bins.
        if user_events.len() >= 2_048 {
            let workspace = session_bins.get_or_insert_with(|| {
                time_bins::SessionBins::new(window.start, window.end, categories.len())
            });
            if let Some(bins) = workspace {
                bins.summarize(
                    user_events,
                    |event| event.time(window),
                    F::purchase,
                    |session, amounts| {
                        let day = output.start_session(session.start, session.events);
                        for (category, &amount) in amounts.iter().enumerate() {
                            if amount != 0 {
                                output.purchase(category as u32, amount);
                            }
                        }
                        output.day.revenue += session.revenue;
                        attribution.record(
                            user_clicks,
                            session.start,
                            day,
                            session.revenue,
                            &mut campaign_totals,
                        );
                    },
                );
                output.flush();
                continue;
            }
        }

        if user_events.len() >= 32_768 {
            user_events.par_sort_unstable_by_key(|e| e.order());
        } else {
            user_events.sort_unstable_by_key(|e| e.order());
        }
        let sessions = user_events.chunk_by(|a, b| b.order() - a.order() <= SESSION_GAP_MS);
        for session in sessions {
            let start = session[0].time(window);
            let day = output.start_session(start, session.len() as i64);
            let mut revenue = 0;
            for event in session {
                let (price, category) = event.purchase();
                if price != 0 {
                    let price = i64::from(price);
                    revenue += price;
                    output.purchase(category, price);
                }
            }
            output.day.revenue += revenue;
            attribution.record(user_clicks, start, day, revenue, &mut campaign_totals);
        }
        output.flush();
    }
    (rows, campaign_totals)
}

fn user_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]))
}

fn write_user_rows<N: OutputName>(path: &Path, rows: UserRows<N>, schema: Arc<Schema>) {
    if N::write_plain(path, &rows) {
        return;
    }
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(N::string_array(rows.names)),
            Arc::new(Date32Array::from(rows.day)),
            Arc::new(Int64Array::from(rows.sessions)),
            Arc::new(Int64Array::from(rows.events)),
            Arc::new(Int64Array::from(rows.revenue)),
            Arc::new(StringArray::from(rows.category)),
        ],
    )
    .expect("user_daily batch");
    write_batch(path, batch);
}

fn write_campaign_rows(
    path: &Path,
    totals: FxHashMap<(u32, i32), CampaignTotals>,
    campaigns: &CampaignInterner,
    channels: &[&'static [u8]],
) {
    let mut campaign = Vec::with_capacity(totals.len());
    let mut channel = Vec::with_capacity(totals.len());
    let mut day = Vec::with_capacity(totals.len());
    let mut sessions = Vec::with_capacity(totals.len());
    let mut purchasing = Vec::with_capacity(totals.len());
    let mut revenue = Vec::with_capacity(totals.len());
    let mut assisted = Vec::with_capacity(totals.len());
    for ((id, date), total) in totals {
        campaign.push(std::str::from_utf8(campaigns.name(id)).expect("UTF-8 campaign ID"));
        channel.push(std::str::from_utf8(channels[id as usize]).expect("UTF-8 channel"));
        day.push(date);
        sessions.push(total.sessions);
        purchasing.push(total.purchasing_sessions);
        revenue.push(total.revenue);
        assisted.push(total.assisted_sessions);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(campaign)),
        Arc::new(StringArray::from_iter_values(channel)),
        Arc::new(Date32Array::from(day)),
        Arc::new(Int64Array::from(sessions)),
        Arc::new(Int64Array::from(purchasing)),
        Arc::new(Int64Array::from(revenue)),
        Arc::new(Int64Array::from(assisted)),
    ];
    write_batch(
        path,
        RecordBatch::try_new(schema, columns).expect("campaign_daily batch"),
    );
}

fn write_batch(path: &Path, batch: RecordBatch) {
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .build();
    let file = fs::File::create(path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn main() {
    START.get_or_init(std::time::Instant::now);
    let data = std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into());
    let outdir = std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into());
    let data = Path::new(&data);
    let outdir = Path::new(&outdir);

    let ncpu = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, usize::from));
    rayon::ThreadPoolBuilder::new()
        .num_threads(ncpu)
        .build_global()
        .unwrap();

    // ---- manifest
    let mbuf = read_file(&data.join("manifest.json"));
    let window_start = manifest_value(mbuf, "window_start").expect("window_start");
    let window_end = manifest_value(mbuf, "window_end").expect("window_end");

    // ---- products.csv -> product -> category id (categories sorted bytewise)
    let pbuf = read_file(&data.join("products.csv"));
    let mut cat_names: Vec<&'static [u8]> = Vec::new();
    let mut prod_rows: Vec<(&'static [u8], &'static [u8])> = Vec::new();
    for line in csv::Lines::new(pbuf) {
        if let Some([pid, cat]) = csv::split_n::<2>(line) {
            prod_rows.push((pid, cat));
            cat_names.push(cat);
        }
    }
    cat_names.push(b"unknown");
    cat_names.sort_unstable();
    cat_names.dedup();
    let mut cat_id: FxHashMap<&'static [u8], u32> = FxHashMap::default();
    for (i, c) in cat_names.iter().enumerate() {
        cat_id.insert(c, i as u32);
    }
    let unknown_cat = cat_id[&b"unknown"[..]];
    let mut product_cat: FxHashMap<&'static [u8], u32> =
        FxHashMap::with_capacity_and_hasher(prod_rows.len(), Default::default());
    let mut product_ids = DirectIds::new(b"p");
    for (pid, cat) in prod_rows {
        product_cat.insert(pid, cat_id[cat]);
        product_ids.insert(pid, cat_id[cat]);
    }

    // ---- campaigns.csv -> campaign id -> channel. id 0 is reserved for organic.
    let cbuf = read_file(&data.join("campaigns.csv"));
    let mut camp = CampaignInterner::new();
    camp.intern(b"organic"); // id 0
    let mut campaign_ids = DirectIds::new(b"c_");
    campaign_ids.insert(b"organic", 0);
    let mut channel_of: Vec<&'static [u8]> = vec![b"organic"];
    for line in csv::Lines::new(cbuf) {
        if let Some([cid, chan]) = csv::split_n::<2>(line) {
            let id = camp.intern(cid);
            campaign_ids.insert(cid, id);
            if id as usize == channel_of.len() {
                channel_of.push(chan);
            } else {
                channel_of[id as usize] = chan;
            }
        }
    }

    lap("catalogs");
    let window = Window {
        start: window_start,
        end: window_end,
    };
    let catalogs = Catalogs {
        products: product_cat,
        product_ids,
        campaign_ids,
        unknown_category: unknown_cat,
        categories: cat_names
            .into_iter()
            .map(|name| std::str::from_utf8(name).expect("UTF-8 category"))
            .collect(),
        campaigns: camp,
        channels: channel_of,
    };
    if CompactFact::fits(&window, catalogs.categories.len()) {
        run_pipeline::<CompactFact>(data, outdir, &window, &catalogs);
    } else {
        run_pipeline::<WideFact>(data, outdir, &window, &catalogs);
    }
}

fn run_pipeline<F: Fact>(data: &Path, outdir: &Path, window: &Window, catalogs: &Catalogs) {
    let (link_bufs, (event_bufs, click_bufs)) = rayon::join(
        || read_dir_csvs(&data.join("links")),
        || {
            rayon::join(
                || read_dir_csvs(&data.join("events")),
                || read_dir_csvs(&data.join("clicks")),
            )
        },
    );
    lap("mapped inputs");
    if CompactFact::fits(window, catalogs.categories.len())
        && fast::try_pipeline(&event_bufs, &click_bufs, &link_bufs, window, catalogs, outdir).is_some()
    {
        return;
    }
    let links = LinkNames::build(&link_bufs);
    let (events, clicks) = rayon::join(
        || {
            user_events::<F>(
                &event_bufs,
                window,
                &links,
                &catalogs.products,
                catalogs.unknown_category,
            )
        },
        || user_clicks(&click_bufs, window, &links, &catalogs.campaigns),
    );
    let parts = events.into_par_iter().zip(clicks).map(|(events, clicks)| {
        aggregate(
            events,
            clicks,
            &catalogs.categories,
            catalogs.channels.len(),
            window,
        )
    });
    write_results(outdir, catalogs, parts);
}

/// Aggregate and write the parts heaviest first. Every part is its own job,
/// started in the given order (FIFO), so the few heavy parts (those holding
/// the most active people) start early instead of finishing last on one
/// thread, and a worker that steals a job while waiting runs just one part.
fn write_results_scheduled<N: OutputName, T: Send>(
    outdir: &Path,
    catalogs: &Catalogs,
    parts: Vec<(usize, T)>,
    aggregate: impl Fn(T) -> (UserRows<N>, CampaignTable) + Sync,
) {
    use std::sync::Mutex;
    let user_dir = outdir.join("user_daily");
    let campaign_dir = outdir.join("campaign_daily");
    fs::create_dir_all(&user_dir).expect("create user_daily");
    fs::create_dir_all(&campaign_dir).expect("create campaign_daily");

    let schema = user_schema();
    let totals: Mutex<Option<CampaignTable>> = Mutex::new(None);
    rayon::scope_fifo(|scope| {
        for (part, input) in parts {
            let (user_dir, schema, totals, aggregate) = (&user_dir, &schema, &totals, &aggregate);
            scope.spawn_fifo(move |_| {
                let (rows, partial) = aggregate(input);
                let path = user_dir.join(format!("part-{part}.parquet"));
                if part == 0 || !rows.names.is_empty() {
                    write_user_rows(&path, rows, schema.clone());
                } else if path.is_file() {
                    fs::remove_file(path).expect("remove empty user partition from previous run");
                }
                let mut totals = totals.lock().unwrap();
                match totals.as_mut() {
                    Some(totals) => totals.merge(partial),
                    None => *totals = Some(partial),
                }
            });
        }
    });
    let totals = totals.into_inner().unwrap().expect("at least one aggregation shard");
    lap("aggregate + user parquet");
    write_campaign_rows(
        &campaign_dir.join("part-0.parquet"),
        totals.into_rows(),
        &catalogs.campaigns,
        &catalogs.channels,
    );
    lap("campaign parquet");
}

fn write_results<N: OutputName>(
    outdir: &Path,
    catalogs: &Catalogs,
    parts: impl IndexedParallelIterator<Item = (UserRows<N>, CampaignTable)>,
) {
    let user_dir = outdir.join("user_daily");
    let campaign_dir = outdir.join("campaign_daily");
    fs::create_dir_all(&user_dir).expect("create user_daily");
    fs::create_dir_all(&campaign_dir).expect("create campaign_daily");

    let schema = user_schema();
    let totals = parts
        .enumerate()
        .map(|(part, (rows, totals))| {
            let path = user_dir.join(format!("part-{part}.parquet"));
            if part == 0 || !rows.names.is_empty() {
                write_user_rows(&path, rows, schema.clone());
            } else if path.is_file() {
                fs::remove_file(path).expect("remove empty user partition from previous run");
            }
            totals
        })
        .reduce_with(|mut totals, partial| {
            totals.merge(partial);
            totals
        })
        .expect("at least one aggregation shard");
    lap("aggregate + user parquet");
    write_campaign_rows(
        &campaign_dir.join("part-0.parquet"),
        totals.into_rows(),
        &catalogs.campaigns,
        &catalogs.channels,
    );
    lap("campaign parquet");
}
