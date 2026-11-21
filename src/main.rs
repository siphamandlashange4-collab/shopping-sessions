//! Shopping Sessions pipeline.
//!
//! Reads $DATA_DIR, writes $OUTPUT_DIR/user_daily/ and $OUTPUT_DIR/campaign_daily/
//! as Parquet. Implements rules.md exactly; see that file for the spec.

mod csv;
mod ts;

use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

const DAY_MS: i64 = 86_400_000;
const SESSION_GAP_MS: i64 = 1_800_000; // 30 minutes
const SEVEN_DAYS_MS: i64 = 7 * DAY_MS;
const LATE_MS: i64 = DAY_MS; // 24 hours
const BAD_TS: i64 = i64::MIN;

const T_VIEW: u8 = 0;
const T_CART: u8 = 1;
const T_PURCHASE: u8 = 2;
const T_BAD: u8 = 255;

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
        if let Some(&id) = self.map.get(s) {
            return id;
        }
        let id = self.vals.len() as u32;
        self.vals.push(s);
        self.map.insert(s, id);
        id
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

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
struct RawEvent {
    event_id: &'static [u8],
    user: &'static [u8],
    product: &'static [u8],
    ts: i64,
    ingest: i64,
    price: u32,
    etype: u8,
}

#[derive(Clone, Copy)]
struct RawClick {
    click_id: &'static [u8],
    user: &'static [u8],
    campaign: &'static [u8],
    ts: i64,
}

#[inline]
fn etype_of(s: &[u8]) -> u8 {
    match s {
        b"view" => T_VIEW,
        b"cart" => T_CART,
        b"purchase" => T_PURCHASE,
        _ => T_BAD,
    }
}

/// Split a buffer (minus its header) into roughly `n` line-aligned chunks.
fn chunks(buf: &'static [u8], n: usize) -> Vec<&'static [u8]> {
    let body = match memchr::memchr(b'\n', buf) {
        Some(i) => &buf[i + 1..],
        None => return Vec::new(),
    };
    if body.is_empty() {
        return Vec::new();
    }
    let n = n.max(1);
    let target = body.len() / n + 1;
    let mut out = Vec::with_capacity(n);
    let mut start = 0usize;
    while start < body.len() {
        let mut end = (start + target).min(body.len());
        if end < body.len() {
            match memchr::memchr(b'\n', &body[end..]) {
                Some(i) => end += i + 1,
                None => end = body.len(),
            }
        }
        out.push(&body[start..end]);
        start = end;
    }
    out
}

fn main() {
    let data = std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into());
    let outdir = std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into());
    let data = Path::new(&data);
    let outdir = Path::new(&outdir);

    let ncpu: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));
    let _ = rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global();

    // ---- manifest
    let mbuf = read_file(&data.join("manifest.json"));
    let window_start = manifest_value(mbuf, "window_start").expect("window_start");
    let window_end = manifest_value(mbuf, "window_end").expect("window_end");
    let click_start = window_start - SEVEN_DAYS_MS;

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
    const ORGANIC: u32 = 0;

    // ---- read raw buffers
    let link_bufs = read_dir_csvs(&data.join("links"));
    let event_bufs = read_dir_csvs(&data.join("events"));
    let click_bufs = read_dir_csvs(&data.join("clicks"));

    // ---- parse links (parallel), keep pairs with both sides non-empty
    let link_pairs: Vec<(&'static [u8], &'static [u8])> = link_bufs
        .par_iter()
        .flat_map_iter(|b| {
            csv::Lines::new(b).filter_map(|line| {
                let [a, c] = csv::split_n::<2>(line)?;
                if a.is_empty() || c.is_empty() {
                    None
                } else {
                    Some((a, c))
                }
            })
        })
        .collect();

    // ---- parse events (parallel over line-aligned chunks)
    let ev_chunks: Vec<&'static [u8]> = event_bufs
        .iter()
        .flat_map(|b| chunks(b, ncpu * 2))
        .collect();
    let raw_events: Vec<RawEvent> = ev_chunks
        .par_iter()
        .flat_map_iter(|c| {
            csv::Lines::raw(c).filter_map(|line| {
                let [eid, user, tsf, ingf, et, prod, price] = csv::split_n::<7>(line)?;
                if eid.is_empty() {
                    return None;
                }
                let tsv = ts::parse(tsf).unwrap_or(BAD_TS);
                let ingest = ts::parse(ingf).unwrap_or(BAD_TS);
                let pv = if csv::all_digits(price) {
                    let v = csv::parse_u64(price);
                    if v >= 1 && v <= 10_000_000 {
                        v as u32
                    } else {
                        0
                    }
                } else {
                    0
                };
                Some(RawEvent {
                    event_id: eid,
                    user,
                    product: prod,
                    ts: tsv,
                    ingest,
                    price: pv,
                    etype: etype_of(et),
                })
            })
        })
        .collect();

    // ---- parse clicks
    let ck_chunks: Vec<&'static [u8]> = click_bufs
        .iter()
        .flat_map(|b| chunks(b, ncpu * 2))
        .collect();
    let raw_clicks: Vec<RawClick> = ck_chunks
        .par_iter()
        .flat_map_iter(|c| {
            csv::Lines::raw(c).filter_map(|line| {
                let [cid, user, campaign, tsf] = csv::split_n::<4>(line)?;
                if cid.is_empty() {
                    return None;
                }
                Some(RawClick {
                    click_id: cid,
                    user,
                    campaign,
                    ts: ts::parse(tsf).unwrap_or(BAD_TS),
                })
            })
        })
        .collect();

    // ---- dedup events on event_id, keeping the smallest ingest_ts
    let mut best: FxHashMap<&'static [u8], u32> =
        FxHashMap::with_capacity_and_hasher(raw_events.len(), Default::default());
    for (i, e) in raw_events.iter().enumerate() {
        match best.entry(e.event_id) {
            std::collections::hash_map::Entry::Occupied(mut o) => {
                let cur = &raw_events[*o.get() as usize];
                if e.ingest < cur.ingest {
                    o.insert(i as u32);
                }
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(i as u32);
            }
        }
    }

    // ---- validate kept copies and drop late events
    let kept: Vec<u32> = best.values().copied().collect();
    let mut valid_events: Vec<RawEvent> = kept
        .par_iter()
        .filter_map(|&i| {
            let e = raw_events[i as usize];
            if e.user.is_empty() || e.product.is_empty() {
                return None;
            }
            if e.ts == BAD_TS || e.ts < window_start || e.ts >= window_end {
                return None;
            }
            if e.etype == T_BAD {
                return None;
            }
            if e.etype == T_PURCHASE && e.price == 0 {
                return None;
            }
            if e.ingest == BAD_TS {
                return None;
            }
            if e.ingest - e.ts > LATE_MS {
                return None;
            }
            Some(e)
        })
        .collect();

    // ---- dedup clicks on click_id, then validate
    let mut seen_click: FxHashMap<&'static [u8], u32> =
        FxHashMap::with_capacity_and_hasher(raw_clicks.len(), Default::default());
    for (i, c) in raw_clicks.iter().enumerate() {
        seen_click.entry(c.click_id).or_insert(i as u32);
    }
    let valid_clicks: Vec<(RawClick, u32)> = seen_click
        .values()
        .filter_map(|&i| {
            let c = raw_clicks[i as usize];
            if c.user.is_empty() {
                return None;
            }
            if c.ts == BAD_TS || c.ts < click_start || c.ts >= window_end {
                return None;
            }
            let cid = camp.get(c.campaign)?;
            if cid == ORGANIC {
                return None; // "organic" never appears in campaigns.csv
            }
            Some((c, cid))
        })
        .collect();

    // ---- intern users from links, surviving events and valid clicks
    let mut users = Interner::with_capacity(
        link_pairs.len() * 2 + valid_events.len() / 4 + valid_clicks.len() / 4 + 16,
    );
    let link_ids: Vec<(u32, u32)> = link_pairs
        .iter()
        .map(|&(a, b)| (users.intern(a), users.intern(b)))
        .collect();
    let ev_user: Vec<u32> = valid_events.iter().map(|e| users.intern(e.user)).collect();
    let ck_user: Vec<u32> = valid_clicks.iter().map(|(c, _)| users.intern(c.user)).collect();

    // ---- union-find over linked ids
    let n_users = users.len();
    let mut parent: Vec<u32> = (0..n_users as u32).collect();
    let mut rank: Vec<u8> = vec![0; n_users];
    fn find_root(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let gp = parent[parent[x as usize] as usize];
            parent[x as usize] = gp; // path halving
            x = gp;
        }
        x
    }
    for &(a, b) in &link_ids {
        let (ra, rb) = (find_root(&mut parent, a), find_root(&mut parent, b));
        if ra == rb {
            continue;
        }
        let (lo, hi) = if rank[ra as usize] < rank[rb as usize] {
            (ra, rb)
        } else {
            (rb, ra)
        };
        parent[lo as usize] = hi;
        if rank[ra as usize] == rank[rb as usize] {
            rank[hi as usize] += 1;
        }
    }

    // person name = byte-wise smallest id in the component
    let mut rep_min: Vec<u32> = (0..n_users as u32).collect();
    for i in 0..n_users as u32 {
        let r = find_root(&mut parent, i);
        let cur = rep_min[r as usize];
        if users.name(i) < users.name(cur) {
            rep_min[r as usize] = i;
        }
    }
    // person id per user id: the interned id of the component's smallest name
    let person_of: Vec<u32> = (0..n_users as u32)
        .map(|i| {
            let r = find_root(&mut parent, i);
            rep_min[r as usize]
        })
        .collect();

    // ---- sessionize
    // (person, ts, idx into valid_events)
    let mut ev: Vec<(u32, i64, u32)> = (0..valid_events.len())
        .map(|i| (person_of[ev_user[i] as usize], valid_events[i].ts, i as u32))
        .collect();
    ev.par_sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    struct Session {
        person: u32,
        day: i32,
        start: i64,
        events: u32,
        revenue: i64,
        purchases: u32,
    }
    let mut sessions: Vec<Session> = Vec::new();
    // purchase facts for top_category: (person, day, cat, price)
    let mut purch: Vec<(u32, i32, u32, i64)> = Vec::new();

    let mut i = 0usize;
    while i < ev.len() {
        let person = ev[i].0;
        let mut j = i;
        while j < ev.len() && ev[j].0 == person {
            j += 1;
        }
        // one person's events, sorted by ts
        let mut k = i;
        while k < j {
            let start = ev[k].1;
            let day = ts::day_of(start);
            let mut s = Session {
                person,
                day,
                start,
                events: 0,
                revenue: 0,
                purchases: 0,
            };
            let mut prev = start;
            while k < j && (ev[k].1 - prev) <= SESSION_GAP_MS {
                prev = ev[k].1;
                let e = &valid_events[ev[k].2 as usize];
                s.events += 1;
                if e.etype == T_PURCHASE {
                    s.revenue += e.price as i64;
                    s.purchases += 1;
                    let cat = product_cat
                        .get(e.product)
                        .copied()
                        .unwrap_or(unknown_cat);
                    purch.push((person, day, cat, e.price as i64));
                }
                k += 1;
            }
            sessions.push(s);
        }
        i = j;
    }
    drop(ev);

    // ---- clicks per person, sorted by (ts, click_id)
    let mut ck: Vec<(u32, i64, u32, &'static [u8])> = (0..valid_clicks.len())
        .map(|i| {
            let (c, cid) = valid_clicks[i];
            (person_of[ck_user[i] as usize], c.ts, cid, c.click_id)
        })
        .collect();
    ck.par_sort_unstable_by(|a, b| {
        a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.3.cmp(b.3))
    });
    // index: person -> slice range in ck
    let mut ck_start: FxHashMap<u32, (u32, u32)> =
        FxHashMap::with_capacity_and_hasher(1024, Default::default());
    {
        let mut i = 0usize;
        while i < ck.len() {
            let p = ck[i].0;
            let mut j = i;
            while j < ck.len() && ck[j].0 == p {
                j += 1;
            }
            ck_start.insert(p, (i as u32, j as u32));
            i = j;
        }
    }

    // ---- attribute each session, and collect assists
    // credited rows: (campaign, day, purchasing, revenue)
    let mut credited: Vec<(u32, i32, u32, i64)> = Vec::with_capacity(sessions.len());
    let mut assisted: Vec<(u32, i32)> = Vec::new();
    let mut scratch: Vec<u32> = Vec::new();
    for s in &sessions {
        let mut chosen = ORGANIC;
        if let Some(&(lo, hi)) = ck_start.get(&s.person) {
            let slice = &ck[lo as usize..hi as usize];
            // last index with ts <= start
            let ub = slice.partition_point(|x| x.1 <= s.start);
            if ub > 0 {
                let last_ts = slice[ub - 1].1;
                if s.start - last_ts <= SEVEN_DAYS_MS {
                    // among equal max ts, the smallest click_id; the slice is
                    // already ordered by (ts, click_id), so take the first of them
                    let mut p = ub - 1;
                    while p > 0 && slice[p - 1].1 == last_ts {
                        p -= 1;
                    }
                    chosen = slice[p].2;
                }
            }
            // assists: distinct campaigns with a click in [start - 7d, start]
            let lo_i = slice.partition_point(|x| x.1 < s.start - SEVEN_DAYS_MS);
            scratch.clear();
            for x in &slice[lo_i..ub] {
                scratch.push(x.2);
            }
            scratch.sort_unstable();
            scratch.dedup();
            for &c in scratch.iter() {
                assisted.push((c, s.day));
            }
        }
        credited.push((
            chosen,
            s.day,
            if s.purchases > 0 { 1 } else { 0 },
            s.revenue,
        ));
    }

    // ---- user_daily
    let mut ud: Vec<(u32, i32, i64, i64, i64)> = sessions
        .iter()
        .map(|s| (s.person, s.day, 1i64, s.events as i64, s.revenue))
        .collect();
    ud.par_sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    // top category per (person, day)
    purch.par_sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
    let mut top_cat: FxHashMap<(u32, i32), u32> =
        FxHashMap::with_capacity_and_hasher(purch.len() / 2 + 16, Default::default());
    {
        let mut i = 0usize;
        while i < purch.len() {
            let (p, d, _, _) = purch[i];
            let mut j = i;
            let mut best_cat = u32::MAX;
            let mut best_sum = i64::MIN;
            while j < purch.len() && purch[j].0 == p && purch[j].1 == d {
                let cat = purch[j].2;
                let mut sum = 0i64;
                while j < purch.len() && purch[j].0 == p && purch[j].1 == d && purch[j].2 == cat {
                    sum += purch[j].3;
                    j += 1;
                }
                // categories are interned in byte order, so a lower id wins ties
                if sum > best_sum {
                    best_sum = sum;
                    best_cat = cat;
                }
            }
            top_cat.insert((p, d), best_cat);
            i = j;
        }
    }

    let mut ud_user: Vec<&'static [u8]> = Vec::new();
    let mut ud_day: Vec<i32> = Vec::new();
    let mut ud_sessions: Vec<i64> = Vec::new();
    let mut ud_events: Vec<i64> = Vec::new();
    let mut ud_rev: Vec<i64> = Vec::new();
    let mut ud_top: Vec<Option<&'static str>> = Vec::new();
    {
        let mut i = 0usize;
        while i < ud.len() {
            let (p, d, _, _, _) = ud[i];
            let (mut ns, mut ne, mut nr) = (0i64, 0i64, 0i64);
            let mut j = i;
            while j < ud.len() && ud[j].0 == p && ud[j].1 == d {
                ns += ud[j].2;
                ne += ud[j].3;
                nr += ud[j].4;
                j += 1;
            }
            ud_user.push(users.name(p));
            ud_day.push(d);
            ud_sessions.push(ns);
            ud_events.push(ne);
            ud_rev.push(nr);
            ud_top.push(top_cat.get(&(p, d)).map(|&c| {
                std::str::from_utf8(cat_names[c as usize]).unwrap_or("")
            }));
            i = j;
        }
    }

    // ---- campaign_daily
    credited.par_sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    assisted.par_sort_unstable();

    let mut cd: FxHashMap<(u32, i32), (i64, i64, i64, i64)> =
        FxHashMap::with_capacity_and_hasher(4096, Default::default());
    for &(c, d, pur, rev) in &credited {
        let e = cd.entry((c, d)).or_insert((0, 0, 0, 0));
        e.0 += 1;
        e.1 += pur as i64;
        e.2 += rev;
    }
    for &(c, d) in &assisted {
        let e = cd.entry((c, d)).or_insert((0, 0, 0, 0));
        e.3 += 1;
    }

    let mut cd_rows: Vec<((u32, i32), (i64, i64, i64, i64))> = cd.into_iter().collect();
    cd_rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let mut cdc: Vec<&'static [u8]> = Vec::with_capacity(cd_rows.len());
    let mut cdch: Vec<&'static [u8]> = Vec::with_capacity(cd_rows.len());
    let mut cdd: Vec<i32> = Vec::with_capacity(cd_rows.len());
    let mut cds: Vec<i64> = Vec::with_capacity(cd_rows.len());
    let mut cdp: Vec<i64> = Vec::with_capacity(cd_rows.len());
    let mut cdr: Vec<i64> = Vec::with_capacity(cd_rows.len());
    let mut cda: Vec<i64> = Vec::with_capacity(cd_rows.len());
    for ((c, d), (s, p, r, a)) in cd_rows {
        cdc.push(camp.name(c));
        cdch.push(channel_of[c as usize]);
        cdd.push(d);
        cds.push(s);
        cdp.push(p);
        cdr.push(r);
        cda.push(if c == ORGANIC { 0 } else { a });
    }

    // ---- write parquet
    fs::create_dir_all(outdir.join("user_daily")).ok();
    fs::create_dir_all(outdir.join("campaign_daily")).ok();

    let ud_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    let ud_batch = RecordBatch::try_new(
        ud_schema.clone(),
        vec![
            Arc::new(StringArray::from_iter_values(
                ud_user.iter().map(|b| std::str::from_utf8(b).unwrap_or("")),
            )) as ArrayRef,
            Arc::new(Date32Array::from(ud_day)) as ArrayRef,
            Arc::new(Int64Array::from(ud_sessions)) as ArrayRef,
            Arc::new(Int64Array::from(ud_events)) as ArrayRef,
            Arc::new(Int64Array::from(ud_rev)) as ArrayRef,
            Arc::new(StringArray::from(ud_top)) as ArrayRef,
        ],
    )
    .expect("user_daily batch");

    let cd_schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let cd_batch = RecordBatch::try_new(
        cd_schema.clone(),
        vec![
            Arc::new(StringArray::from_iter_values(
                cdc.iter().map(|b| std::str::from_utf8(b).unwrap_or("")),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                cdch.iter().map(|b| std::str::from_utf8(b).unwrap_or("")),
            )) as ArrayRef,
            Arc::new(Date32Array::from(cdd)) as ArrayRef,
            Arc::new(Int64Array::from(cds)) as ArrayRef,
            Arc::new(Int64Array::from(cdp)) as ArrayRef,
            Arc::new(Int64Array::from(cdr)) as ArrayRef,
            Arc::new(Int64Array::from(cda)) as ArrayRef,
        ],
    )
    .expect("campaign_daily batch");

    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .build();

    let f = fs::File::create(outdir.join("user_daily").join("part-0.parquet")).unwrap();
    let mut w = ArrowWriter::try_new(f, ud_schema, Some(props.clone())).unwrap();
    w.write(&ud_batch).unwrap();
    w.close().unwrap();

    let f = fs::File::create(outdir.join("campaign_daily").join("part-0.parquet")).unwrap();
    let mut w = ArrowWriter::try_new(f, cd_schema, Some(props)).unwrap();
    w.write(&cd_batch).unwrap();
    w.close().unwrap();
}
