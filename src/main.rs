//! Shopping Sessions pipeline.
//!
//! Reads $DATA_DIR, writes $OUTPUT_DIR/user_daily/ and $OUTPUT_DIR/campaign_daily/
//! as Parquet. Implements rules.md exactly; see that file for the spec.

mod attribution;
mod csv;
mod ts;

use attribution::{Attribution, CampaignTotals, Click};
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

const SHARDS: usize = 32;
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
    paths
        .par_iter()
        .map(|p| {
            let v = fs::read(p).unwrap_or_default();
            &*Box::leak(v.into_boxed_slice()) as &'static [u8]
        })
        .collect()
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
struct Interner {
    map: FxHashMap<&'static [u8], u32>,
    vals: Vec<&'static [u8]>,
}

impl Interner {
    fn new() -> Self {
        Interner {
            map: FxHashMap::default(),
            vals: Vec::new(),
        }
    }
    fn with_capacity(n: usize) -> Self {
        Interner {
            map: FxHashMap::with_capacity_and_hasher(n, Default::default()),
            vals: Vec::with_capacity(n),
        }
    }
    #[inline]
    fn intern(&mut self, s: &'static [u8]) -> u32 {
        *self.map.entry(s).or_insert_with(|| {
            let id = self.vals.len() as u32;
            self.vals.push(s);
            id
        })
    }
    #[inline]
    fn get(&self, s: &[u8]) -> Option<u32> {
        self.map.get(s).copied()
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
struct Event {
    user: &'static [u8],
    time: i64,
    price: u32,
    category: u32,
}

struct Candidate<K> {
    id: K,
    ingest: i64,
    event: Event,
}

struct RawClick {
    user: &'static [u8],
    id: &'static [u8],
    time: i64,
    campaign: u32,
}

#[derive(Clone, Copy, Default)]
struct SessionEvent {
    time: i64,
    price: u32,
    category: u32,
}

struct Window {
    start: i64,
    end: i64,
}

struct LinkNames {
    users: Interner,
    person: Vec<u32>,
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
        let mut users = Interner::with_capacity(pairs.len() * 2);
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
            .map(|id| smallest[find_root(&mut parent, id) as usize])
            .collect();
        Self { users, person }
    }

    #[inline]
    fn name(&self, user: &'static [u8]) -> &'static [u8] {
        match self.users.get(user) {
            Some(id) => self.users.name(self.person[id as usize]),
            None => user,
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
fn user_shard(user: &[u8]) -> usize {
    shard(rustc_hash::FxBuildHasher.hash_one(user))
}

/// Short event IDs beginning with e fit losslessly in a machine word. A
/// nonzero last byte distinguishes the length after zero padding. All other
/// IDs still use their complete bytes as the key.
#[inline]
fn pack_event_id(id: &[u8]) -> u64 {
    if let [b'e', tail @ ..] = id {
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

fn user_events(
    buffers: &[&'static [u8]],
    window: &Window,
    links: &LinkNames,
    products: &FxHashMap<&'static [u8], u32>,
    unknown: u32,
) -> [Vec<Vec<Event>>; SHARDS] {
    let (packed_pieces, fallback_pieces) = chunks(buffers)
        .into_par_iter()
        .map(|chunk| {
            let mut packed_buckets: Buckets<Candidate<u64>> = std::array::from_fn(|_| Vec::new());
            let mut fallback_buckets: Buckets<Candidate<&[u8]>> =
                std::array::from_fn(|_| Vec::new());
            for [id, user, time, ingest, kind, product, price] in csv::Rows::<7>::raw(chunk) {
                if id.is_empty() {
                    continue;
                }
                let ingest = ts::parse(ingest).unwrap_or(BAD_TS);
                let time = ts::parse(time).unwrap_or(BAD_TS);
                let mut event = Event {
                    user,
                    time: BAD_TS,
                    price: 0,
                    category: 0,
                };
                if !user.is_empty()
                    && !product.is_empty()
                    && ingest != BAD_TS
                    && (window.start..window.end).contains(&time)
                    && ingest - time <= LATE_MS
                {
                    match kind {
                        b"view" | b"cart" => event.time = time,
                        b"purchase" if csv::all_digits(price) => {
                            let amount = csv::parse_u64(price);
                            if (1..=10_000_000).contains(&amount) {
                                event.time = time;
                                event.price = amount as u32;
                                event.category = products.get(product).copied().unwrap_or(unknown);
                            }
                        }
                        _ => {}
                    }
                }
                // Invalid rows still participate in the earliest-ingest election.
                let packed = pack_event_id(id);
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

fn route_winners<K: std::hash::Hash + Eq + Copy>(
    pieces: &[Vec<Candidate<K>>],
    links: &LinkNames,
    buckets: &mut Buckets<Event>,
) {
    use std::collections::hash_map::Entry;
    let total = pieces.iter().map(Vec::len).sum();
    let mut winners = FxHashMap::with_capacity_and_hasher(total, Default::default());
    for candidate in pieces.iter().flatten() {
        match winners.entry(candidate.id) {
            Entry::Vacant(entry) => {
                entry.insert(candidate);
            }
            Entry::Occupied(mut entry) if candidate.ingest < entry.get().ingest => {
                entry.insert(candidate);
            }
            _ => {}
        }
    }
    for candidate in winners.into_values() {
        let mut event = candidate.event;
        if event.time != BAD_TS {
            event.user = links.name(event.user);
            buckets[user_shard(event.user)].push(event);
        }
    }
}

fn user_clicks(
    buffers: &[&'static [u8]],
    window: &Window,
    links: &LinkNames,
    campaigns: &Interner,
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
                let user = links.name(user);
                // Identical click copies have the same attribution tie-break and
                // occupy one campaign in the sliding window, even if both are kept.
                buckets[user_shard(user)].push(RawClick {
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
struct UserRows {
    names: Vec<&'static str>,
    day: Vec<i32>,
    sessions: Vec<i64>,
    events: Vec<i64>,
    revenue: Vec<i64>,
    category: Vec<Option<&'static str>>,
}

impl UserRows {
    fn push(&mut self, name: &'static str, day: &UserDay, categories: &[&'static str]) {
        self.names.push(name);
        self.day.push(day.day);
        self.sessions.push(day.sessions);
        self.events.push(day.events);
        self.revenue.push(day.revenue);
        self.category
            .push(day.category.map(|id| categories[id as usize]));
    }
}

fn aggregate(
    event_parts: Vec<Vec<Event>>,
    click_parts: Vec<Vec<RawClick>>,
    categories: &[&'static str],
    campaign_count: usize,
) -> (UserRows, FxHashMap<(u32, i32), CampaignTotals>) {
    let num_events = event_parts.iter().map(Vec::len).sum::<usize>();
    let mut names = Interner::with_capacity(num_events / 12);
    let mut named_events = Vec::with_capacity(num_events);
    let mut counts = Vec::new();
    for event in event_parts.into_iter().flatten() {
        let user = names.intern(event.user) as usize;
        if user == counts.len() {
            counts.push(0);
        }
        counts[user] += 1;
        named_events.push((
            user,
            SessionEvent {
                time: event.time,
                price: event.price,
                category: event.category,
            },
        ));
    }
    // Group by the dense person ID in linear time, then sort only within people.
    let mut end = 0;
    let mut offsets: Vec<_> = counts
        .into_iter()
        .map(|count| {
            let start = end;
            end += count;
            start
        })
        .collect();
    let mut events = vec![SessionEvent::default(); num_events];
    for (user, event) in named_events {
        events[offsets[user]] = event;
        offsets[user] += 1;
    }
    let mut clicks: Vec<_> = click_parts
        .into_iter()
        .flatten()
        .filter_map(|click| {
            Some(Click {
                time: click.time,
                user: names.get(click.user)?,
                campaign: click.campaign,
                id: click.id,
            })
        })
        .collect();
    clicks.sort_unstable_by(|a, b| {
        a.user
            .cmp(&b.user)
            .then(a.time.cmp(&b.time))
            .then_with(|| b.id.cmp(a.id))
    });

    let mut rows = UserRows::default();
    let mut campaign_totals = FxHashMap::default();
    let mut attribution = Attribution::new(campaign_count);
    let mut category_totals = vec![0; categories.len()];
    let mut touched = Vec::new();
    let mut click_pos = 0;
    let mut event_pos = 0;
    for (user, end) in offsets.into_iter().enumerate() {
        let user = user as u32;
        let user_events = &mut events[event_pos..end];
        event_pos = end;
        if user_events.len() >= 32_768 {
            user_events.par_sort_unstable_by_key(|e| e.time);
        } else {
            user_events.sort_unstable_by_key(|e| e.time);
        }
        let name = std::str::from_utf8(names.name(user)).expect("UTF-8 user ID");
        let first = click_pos;
        while click_pos < clicks.len() && clicks[click_pos].user == user {
            click_pos += 1;
        }
        let user_clicks = &clicks[first..click_pos];
        attribution.reset();
        let mut day = UserDay::default();
        let sessions = user_events.chunk_by(|a, b| b.time - a.time <= SESSION_GAP_MS);
        for session in sessions {
            let start = session[0].time;
            let session_day = ts::day_of(start);
            if day.sessions != 0 && day.day != session_day {
                rows.push(name, &day, categories);
                day = UserDay::default();
                for index in touched.drain(..) {
                    category_totals[index] = 0;
                }
            }
            day.day = session_day;
            day.sessions += 1;
            day.events += session.len() as i64;
            let mut revenue = 0;
            for event in session {
                if event.price != 0 {
                    let price = i64::from(event.price);
                    revenue += price;
                    let total = &mut category_totals[event.category as usize];
                    if *total == 0 {
                        touched.push(event.category as usize);
                    }
                    *total += price;
                    if *total > day.best_category_revenue
                        || (*total == day.best_category_revenue
                            && Some(event.category) < day.category)
                    {
                        day.best_category_revenue = *total;
                        day.category = Some(event.category);
                    }
                }
            }
            day.revenue += revenue;
            attribution.record(
                user_clicks,
                start,
                session_day,
                revenue,
                &mut campaign_totals,
            );
        }
        rows.push(name, &day, categories);
        for index in touched.drain(..) {
            category_totals[index] = 0;
        }
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

fn write_user_rows(path: &Path, rows: UserRows, schema: Arc<Schema>) {
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from_iter_values(rows.names)),
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
    campaigns: &Interner,
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
    for (pid, cat) in prod_rows {
        product_cat.insert(pid, cat_id[cat]);
    }

    // ---- campaigns.csv -> campaign id -> channel. id 0 is reserved for organic.
    let cbuf = read_file(&data.join("campaigns.csv"));
    let mut camp = Interner::new();
    camp.intern(b"organic"); // id 0
    let mut channel_of: Vec<&'static [u8]> = vec![b"organic"];
    for line in csv::Lines::new(cbuf) {
        if let Some([cid, chan]) = csv::split_n::<2>(line) {
            let id = camp.intern(cid);
            if id as usize == channel_of.len() {
                channel_of.push(chan);
            } else {
                channel_of[id as usize] = chan;
            }
        }
    }

    let window = Window {
        start: window_start,
        end: window_end,
    };
    let (links, (event_bufs, click_bufs)) = rayon::join(
        || LinkNames::build(&read_dir_csvs(&data.join("links"))),
        || {
            rayon::join(
                || read_dir_csvs(&data.join("events")),
                || read_dir_csvs(&data.join("clicks")),
            )
        },
    );
    let (events, clicks) = rayon::join(
        || user_events(&event_bufs, &window, &links, &product_cat, unknown_cat),
        || user_clicks(&click_bufs, &window, &links, &camp),
    );
    let categories: Vec<_> = cat_names
        .into_iter()
        .map(|name| std::str::from_utf8(name).expect("UTF-8 category"))
        .collect();
    let user_dir = outdir.join("user_daily");
    let campaign_dir = outdir.join("campaign_daily");
    fs::create_dir_all(&user_dir).expect("create user_daily");
    fs::create_dir_all(&campaign_dir).expect("create campaign_daily");

    let schema = user_schema();
    let parts: Vec<_> = events
        .into_par_iter()
        .zip(clicks)
        .enumerate()
        .map(|(part, (events, clicks))| {
            let (rows, totals) = aggregate(events, clicks, &categories, channel_of.len());
            let path = user_dir.join(format!("part-{part}.parquet"));
            if part == 0 || !rows.names.is_empty() {
                write_user_rows(&path, rows, schema.clone());
            } else if path.is_file() {
                fs::remove_file(path).expect("remove empty user partition from previous run");
            }
            totals
        })
        .collect();

    let mut totals: FxHashMap<_, CampaignTotals> = FxHashMap::default();
    for (key, value) in parts.into_iter().flatten() {
        *totals.entry(key).or_default() += value;
    }
    write_campaign_rows(
        &campaign_dir.join("part-0.parquet"),
        totals,
        &camp,
        &channel_of,
    );
}
