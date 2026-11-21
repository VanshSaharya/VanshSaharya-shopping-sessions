//! Shopping Sessions pipeline: implements rules.md.
//!
//! Phases:
//!   1. small lookups (manifest, products, campaigns)
//!   2. links -> union-find -> person of every linked ID (named by the smallest ID)
//!   3. events and clicks parsed in parallel from mmapped chunks
//!   4. events deduplicated on event_id (hash buckets), survivors scattered by person
//!   5. per person bucket: sessionize, attribute, aggregate
//!   6. Parquet output

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use ahash::RandomState;
use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use memchr::{memchr, memchr_iter};
use memmap2::Mmap;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DAY_MS: i64 = 86_400_000;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const NB: usize = 256; // number of hash / person buckets
const CHUNK: usize = 8 << 20;

// ---------------------------------------------------------------- CSV dialect

/// Split one line into exactly N fields and strip quote syntax.
/// Returns false for a malformed line. An empty slice means null.
#[inline(always)]
fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let mut start = 0usize;
    let mut n = 0usize;
    for pos in memchr_iter(b',', line) {
        if n + 1 >= N {
            return false;
        }
        out[n] = &line[start..pos];
        n += 1;
        start = pos + 1;
    }
    if n + 1 != N {
        return false;
    }
    out[n] = &line[start..];
    if memchr(b'"', line).is_none() {
        return true;
    }
    for f in out.iter_mut() {
        let v = *f;
        if v.first() == Some(&b'"') {
            if v.len() < 2 || v[v.len() - 1] != b'"' {
                return false;
            }
            let inner = &v[1..v.len() - 1];
            if memchr(b'"', inner).is_some() {
                return false;
            }
            *f = inner;
        } else if memchr(b'"', v).is_some() {
            return false;
        }
    }
    true
}

/// Iterate over the data lines of a byte range of a file.
#[inline(always)]
fn for_lines(buf: &[u8], mut f: impl FnMut(&[u8])) {
    let mut pos = 0;
    let end = buf.len();
    while pos < end {
        match memchr(b'\n', &buf[pos..]) {
            Some(i) => {
                f(&buf[pos..pos + i]);
                pos += i + 1;
            }
            None => {
                f(&buf[pos..]);
                pos = end;
            }
        }
    }
}

// ---------------------------------------------------------------- timestamps

#[inline(always)]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline(always)]
fn dig(b: u8) -> Option<i64> {
    if b.is_ascii_digit() { Some((b - b'0') as i64) } else { None }
}

#[inline(always)]
fn num(s: &[u8]) -> Option<i64> {
    let mut v = 0i64;
    for &b in s {
        v = v * 10 + dig(b)?;
    }
    Some(v)
}

/// Parse F1, F2 or F3 into epoch ms. Out-of-range F3 maps to i64::MAX (outside every window).
#[inline]
fn parse_ts(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    if s.len() != 20 && s.len() != 25 || s[4] != b'-' {
        // F3 candidate
        if !s.iter().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let t = match s.iter().position(|&b| b != b'0') {
            None => return Some(0),
            Some(i) => &s[i..],
        };
        if t.len() > 19 {
            return Some(i64::MAX);
        }
        let mut v: u64 = 0;
        for &b in t {
            v = v * 10 + (b - b'0') as u64;
        }
        return Some(if v > i64::MAX as u64 { i64::MAX } else { v as i64 });
    }
    if s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = num(&s[0..4])?;
    let mo = num(&s[5..7])?;
    let d = num(&s[8..10])?;
    let h = num(&s[11..13])?;
    let mi = num(&s[14..16])?;
    let se = num(&s[17..19])?;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || se > 59 || d < 1 {
        return None;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = match mo {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if leap { 29 } else { 28 }
        }
    };
    if d > dim {
        return None;
    }
    let mut ms = days_from_civil(y, mo, d) * DAY_MS + (h * 3600 + mi * 60 + se) * 1000;
    if s.len() == 20 {
        if s[19] != b'Z' {
            return None;
        }
    } else {
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if s[22] != b':' {
            return None;
        }
        let oh = num(&s[20..22])?;
        let om = num(&s[23..25])?;
        if oh > 23 || om > 59 {
            return None;
        }
        ms -= sign * (oh * 60 + om) * 60_000;
    }
    Some(ms)
}

#[inline(always)]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() || !s.iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let t = match s.iter().position(|&b| b != b'0') {
        None => return None, // zero
        Some(i) => &s[i..],
    };
    if t.len() > 8 {
        return None;
    }
    let v = num(t)?;
    if (1..=10_000_000).contains(&v) { Some(v as u32) } else { None }
}

// ---------------------------------------------------------------- interning

/// Pack a key of at most 15 bytes into a u128 (bytes, then length in the top byte).
#[inline(always)]
fn pack(s: &[u8]) -> Option<u128> {
    if s.len() > 15 {
        return None;
    }
    let mut b = [0u8; 16];
    b[..s.len()].copy_from_slice(s);
    b[15] = s.len() as u8;
    Some(u128::from_le_bytes(b))
}

#[inline(always)]
fn mix(k: u128) -> u64 {
    let a = (k as u64) ^ 0x9E37_79B9_7F4A_7C15;
    let b = ((k >> 64) as u64) ^ 0xC2B2_AE3D_27D4_EB4F;
    let m = (a as u128).wrapping_mul(b as u128);
    (m as u64) ^ ((m >> 64) as u64)
}

#[derive(Default, Clone, Copy)]
struct MixHasher(u64);
impl std::hash::Hasher for MixHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!()
    }
    fn write_u128(&mut self, k: u128) {
        self.0 = mix(k);
    }
}
type MixState = std::hash::BuildHasherDefault<MixHasher>;
type PackedMap<V> = HashMap<u128, V, MixState>;

/// String -> value lookup with inline packed keys and a fallback for long keys.
struct Lookup<V: Copy> {
    short: PackedMap<V>,
    long: HashMap<Vec<u8>, V, RandomState>,
}
impl<V: Copy> Lookup<V> {
    fn new() -> Self {
        Lookup { short: HashMap::default(), long: HashMap::with_hasher(RandomState::with_seeds(9, 9, 9, 9)) }
    }
    fn insert(&mut self, k: &[u8], v: V) {
        match pack(k) {
            Some(p) => {
                self.short.insert(p, v);
            }
            None => {
                self.long.insert(k.to_vec(), v);
            }
        }
    }
    #[inline(always)]
    fn get(&self, k: &[u8]) -> Option<V> {
        match pack(k) {
            Some(p) => self.short.get(&p).copied(),
            None => self.long.get(k).copied(),
        }
    }
}

const SHARDS: usize = 1024;

struct Interner {
    short: Vec<Mutex<PackedMap<u32>>>,
    long: Mutex<HashMap<Box<[u8]>, u32, RandomState>>,
    next: AtomicU32,
}

impl Interner {
    fn new() -> Self {
        Interner {
            short: (0..SHARDS).map(|_| Mutex::new(HashMap::default())).collect(),
            long: Mutex::new(HashMap::with_hasher(RandomState::with_seeds(1, 2, 3, 4))),
            next: AtomicU32::new(0),
        }
    }
    #[inline]
    fn get(&self, s: &[u8]) -> u32 {
        match pack(s) {
            Some(p) => {
                let h = mix(p);
                let mut m = self.short[((h >> 32) as usize) & (SHARDS - 1)].lock().unwrap();
                *m.entry(p).or_insert_with(|| self.next.fetch_add(1, Ordering::Relaxed))
            }
            None => {
                let mut m = self.long.lock().unwrap();
                if let Some(&v) = m.get(s) {
                    return v;
                }
                let id = self.next.fetch_add(1, Ordering::Relaxed);
                m.insert(s.into(), id);
                id
            }
        }
    }
    fn snapshot(&self) -> Vec<Box<[u8]>> {
        let n = self.next.load(Ordering::Relaxed) as usize;
        let mut out: Vec<Box<[u8]>> = vec![Box::default(); n];
        for sh in &self.short {
            for (&k, &v) in sh.lock().unwrap().iter() {
                let b = k.to_le_bytes();
                out[v as usize] = b[..b[15] as usize].into();
            }
        }
        for (k, &v) in self.long.lock().unwrap().iter() {
            out[v as usize] = k.clone();
        }
        out
    }
}

// ---------------------------------------------------------------- files

struct Chunk {
    file: usize,
    start: usize,
    end: usize,
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

/// mmap every file and cut it into chunks aligned to line starts; the header line is skipped.
fn map_files(paths: &[PathBuf]) -> (Vec<Option<Mmap>>, Vec<Chunk>) {
    let maps: Vec<Option<Mmap>> = paths
        .par_iter()
        .map(|p| {
            let f = File::open(p).expect("open");
            let len = f.metadata().map(|m| m.len()).unwrap_or(0);
            if len == 0 {
                return None;
            }
            let m = unsafe { Mmap::map(&f).expect("mmap") };
            #[cfg(unix)]
            let _ = m.advise(memmap2::Advice::Sequential);
            Some(m)
        })
        .collect();
    let mut chunks = vec![];
    for (fi, m) in maps.iter().enumerate() {
        let Some(m) = m else { continue };
        let buf = &m[..];
        let mut pos = match memchr(b'\n', buf) {
            Some(i) => i + 1,
            None => continue, // header only
        };
        while pos < buf.len() {
            let mut end = (pos + CHUNK).min(buf.len());
            if end < buf.len() {
                end = match memchr(b'\n', &buf[end..]) {
                    Some(i) => end + i + 1,
                    None => buf.len(),
                };
            }
            chunks.push(Chunk { file: fi, start: pos, end });
            pos = end;
        }
    }
    // biggest chunks first is irrelevant (all ~equal); keep order
    (maps, chunks)
}

fn read_small(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_default()
}

/// Rows of a clean 2-column lookup file.
fn read_pairs(path: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let buf = read_small(path);
    let mut out = vec![];
    let start = match memchr(b'\n', &buf) {
        Some(i) => i + 1,
        None => return out,
    };
    for_lines(&buf[start..], |line| {
        let mut f: [&[u8]; 2] = [&[]; 2];
        if split(line, &mut f) && !f[0].is_empty() {
            out.push((f[0].to_vec(), f[1].to_vec()));
        }
    });
    out
}

fn manifest_ts(text: &str, key: &str) -> i64 {
    let i = text.find(key).expect("manifest key");
    let rest = &text[i + key.len()..];
    let q1 = rest.find('"').unwrap() + 1;
    let rest = &rest[q1..];
    let q3 = rest.find('"').unwrap();
    parse_ts(rest[..q3].as_bytes()).expect("manifest ts")
}

// ---------------------------------------------------------------- records

/// Raw event copy, kept for dedup. kind 0 = invalid kept copy.
#[derive(Clone, Copy)]
struct ERec {
    h0: u64,
    h1: u64,
    ingest: i64,
    key: u64, // person << 32 | (ts - base)
    price: u32,
    cat: u16,
    kind: u8,
}

/// Surviving event.
#[derive(Clone, Copy)]
struct SRec {
    key: u64,
    price: u32,
    cat: u16,
    kind: u8,
}

const CID_INLINE: usize = 22;

#[derive(Clone, Copy)]
struct CRec {
    key: u64,
    camp: u16,
    len: u8, // 255 = long id stored in side table, index in first 4 bytes
    cid: [u8; CID_INLINE],
}

fn transpose<T: Send>(parts: Vec<Vec<Vec<T>>>) -> Vec<Vec<Vec<T>>> {
    let mut out: Vec<Vec<Vec<T>>> = (0..NB).map(|_| Vec::with_capacity(parts.len())).collect();
    for p in parts {
        for (b, v) in p.into_iter().enumerate() {
            if !v.is_empty() {
                out[b].push(v);
            }
        }
    }
    out
}

fn concat<T: Copy>(parts: Vec<Vec<T>>) -> Vec<T> {
    let n = parts.iter().map(|v| v.len()).sum();
    let mut v = Vec::with_capacity(n);
    for p in parts {
        v.extend_from_slice(&p);
    }
    v
}

// ---------------------------------------------------------------- main

fn main() {
    let t0 = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let ncpu: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));
    rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().unwrap();
    let log = |s: &str| eprintln!("[{:>7.3}s] {}", t0.elapsed().as_secs_f64(), s);

    // ---- lookups
    let manifest = String::from_utf8_lossy(&read_small(&data.join("manifest.json"))).into_owned();
    let ws = manifest_ts(&manifest, "\"window_start\"");
    let we = manifest_ts(&manifest, "\"window_end\"");
    let base = ws - WEEK_MS; // click window start; all kept ts are >= base
    assert!(we - base < (1i64 << 32), "window too long for u32 offsets");

    // categories sorted byte-wise so a smaller index is a smaller name
    let products = read_pairs(&data.join("products.csv"));
    let mut cat_names: Vec<Vec<u8>> = products.iter().map(|(_, c)| c.clone()).collect();
    cat_names.push(b"unknown".to_vec());
    cat_names.sort();
    cat_names.dedup();
    let cat_index: HashMap<Vec<u8>, u16> =
        cat_names.iter().enumerate().map(|(i, c)| (c.clone(), i as u16)).collect();
    let unknown_cat = cat_index[&b"unknown"[..]];
    let n_cat = cat_names.len();
    let mut prod: Lookup<u16> = Lookup::new();
    for (p, c) in &products {
        prod.insert(p, cat_index[c]);
    }
    let campaigns = read_pairs(&data.join("campaigns.csv"));
    let mut camp: Lookup<u16> = Lookup::new();
    for (i, (c, _)) in campaigns.iter().enumerate() {
        camp.insert(c, i as u16);
    }
    let organic = campaigns.len() as u16;
    log("lookups");

    // ---- links
    let interner = Interner::new();
    let link_paths = list_csv(&data.join("links"));
    let (link_maps, link_chunks) = map_files(&link_paths);
    let pairs: Vec<Vec<(u32, u32)>> = link_chunks
        .par_iter()
        .map(|c| {
            let buf = &link_maps[c.file].as_ref().unwrap()[c.start..c.end];
            let mut v = vec![];
            for_lines(buf, |line| {
                let mut f: [&[u8]; 2] = [&[]; 2];
                if !split(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                v.push((interner.get(f[0]), interner.get(f[1])));
            });
            v
        })
        .collect();
    let n_link = interner.next.load(Ordering::Relaxed) as usize;
    let mut parent: Vec<u32> = (0..n_link as u32).collect();
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        while p[x as usize] != x {
            let g = p[p[x as usize] as usize];
            p[x as usize] = g;
            x = g;
        }
        x
    }
    for v in &pairs {
        for &(a, b) in v {
            let ra = find(&mut parent, a);
            let rb = find(&mut parent, b);
            if ra != rb {
                parent[ra as usize] = rb;
            }
        }
    }
    drop(pairs);
    drop(link_maps);
    // name of each component: the id with the byte-wise smallest string
    let person_of: Arc<Vec<u32>> = {
        let names_snapshot = interner.snapshot();
        let mut best: Vec<u32> = vec![u32::MAX; n_link];
        for x in 0..n_link as u32 {
            let r = find(&mut parent, x) as usize;
            if best[r] == u32::MAX || names_snapshot[x as usize] < names_snapshot[best[r] as usize] {
                best[r] = x;
            }
        }
        let mut po = vec![0u32; n_link];
        for x in 0..n_link {
            let r = find(&mut parent, x as u32) as usize;
            po[x] = best[r];
        }
        Arc::new(po)
    };
    log(&format!("links: {} ids", n_link));
    let person = |uid: u32| -> u32 {
        if (uid as usize) < n_link { person_of[uid as usize] } else { uid }
    };

    // ---- events and clicks
    let ev_paths = list_csv(&data.join("events"));
    let ck_paths = list_csv(&data.join("clicks"));
    let (ev_maps, ev_chunks) = map_files(&ev_paths);
    let (ck_maps, ck_chunks) = map_files(&ck_paths);
    log(&format!("mapped {} event chunks, {} click chunks", ev_chunks.len(), ck_chunks.len()));

    let long_cids: Mutex<Vec<Box<[u8]>>> = Mutex::new(vec![]);

    // clicks: valid ones only, bucketed by person
    let ck_parts: Vec<Vec<Vec<CRec>>> = ck_chunks
        .par_iter()
        .map(|c| {
            let buf = &ck_maps[c.file].as_ref().unwrap()[c.start..c.end];
            let mut out: Vec<Vec<CRec>> = (0..NB).map(|_| Vec::new()).collect();
            for_lines(buf, |line| {
                let mut f: [&[u8]; 4] = [&[]; 4];
                if !split(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let Some(ts) = parse_ts(f[3]) else { return };
                if ts < base || ts >= we {
                    return;
                }
                let Some(cp) = camp.get(f[2]) else { return };
                let p = person(interner.get(f[1]));
                let mut r = CRec {
                    key: ((p as u64) << 32) | (ts - base) as u64,
                    camp: cp,
                    len: 0,
                    cid: [0; CID_INLINE],
                };
                if f[0].len() <= CID_INLINE {
                    r.len = f[0].len() as u8;
                    r.cid[..f[0].len()].copy_from_slice(f[0]);
                } else {
                    let mut lc = long_cids.lock().unwrap();
                    r.len = 255;
                    r.cid[..4].copy_from_slice(&(lc.len() as u32).to_le_bytes());
                    lc.push(f[0].into());
                }
                out[(p as usize) % NB].push(r);
            });
            out
        })
        .collect();
    log("clicks parsed");

    // events: every well-formed copy, bucketed by event_id hash
    let ev_parts: Vec<Vec<Vec<ERec>>> = ev_chunks
        .par_iter()
        .map(|c| {
            let buf = &ev_maps[c.file].as_ref().unwrap()[c.start..c.end];
            let mut out: Vec<Vec<ERec>> = (0..NB).map(|_| Vec::with_capacity((c.end - c.start) / 80 / NB + 16)).collect();
            for_lines(buf, |line| {
                let mut f: [&[u8]; 7] = [&[]; 7];
                if !split(line, &mut f) || f[0].is_empty() {
                    return;
                }
                let h = xxhash_rust::xxh3::xxh3_128(f[0]);
                let ingest = parse_ts(f[3]).unwrap_or(i64::MAX);
                let mut r = ERec { h0: (h >> 64) as u64, h1: h as u64, ingest, key: 0, price: 0, cat: 0, kind: 0 };
                'v: {
                    if f[1].is_empty() || f[5].is_empty() {
                        break 'v;
                    }
                    let Some(ts) = parse_ts(f[2]) else { break 'v };
                    if ts < ws || ts >= we || ingest == i64::MAX || ingest - ts > DAY_MS {
                        break 'v;
                    }
                    let kind = match f[4] {
                        b"view" => 1,
                        b"cart" => 2,
                        b"purchase" => 3,
                        _ => break 'v,
                    };
                    if kind == 3 {
                        let Some(p) = parse_price(f[6]) else { break 'v };
                        r.price = p;
                    }
                    r.cat = prod.get(f[5]).unwrap_or(unknown_cat);
                    let p = person(interner.get(f[1]));
                    r.key = ((p as u64) << 32) | (ts - base) as u64;
                    r.kind = kind;
                }
                out[(r.h0 >> 56) as usize].push(r);
            });
            out
        })
        .collect();
    log("events parsed");
    drop(ev_maps);
    drop(ck_maps);

    // dedup per hash bucket, scatter survivors by person
    let ev_buckets = transpose(ev_parts);
    let sv_parts: Vec<Vec<Vec<SRec>>> = ev_buckets
        .into_par_iter()
        .map(|parts| {
            let mut v = concat(parts);
            v.sort_unstable_by(|a, b| (a.h0, a.h1, a.ingest).cmp(&(b.h0, b.h1, b.ingest)));
            let mut out: Vec<Vec<SRec>> = (0..NB).map(|_| Vec::new()).collect();
            let mut i = 0;
            while i < v.len() {
                let r = v[i];
                let mut j = i + 1;
                while j < v.len() && v[j].h0 == r.h0 && v[j].h1 == r.h1 {
                    j += 1;
                }
                if r.kind != 0 {
                    let p = (r.key >> 32) as usize;
                    out[p % NB].push(SRec { key: r.key, price: r.price, cat: r.cat, kind: r.kind });
                }
                i = j;
            }
            out
        })
        .collect();
    log("deduped");
    let ev_by_person = transpose(sv_parts);
    let ck_by_person = transpose(ck_parts);
    let long_cids = long_cids.into_inner().unwrap();

    // ---- sessions, attribution, aggregation
    let day0 = ws.div_euclid(DAY_MS);
    let n_days = ((we - 1).div_euclid(DAY_MS) - day0 + 1) as usize;
    let n_camp = campaigns.len() + 1;
    fn cid_of<'a>(r: &'a CRec, long: &'a [Box<[u8]>]) -> &'a [u8] {
        if r.len == 255 {
            let i = u32::from_le_bytes(r.cid[..4].try_into().unwrap()) as usize;
            &long[i]
        } else {
            &r.cid[..r.len as usize]
        }
    }

    struct URow {
        person: u32,
        day: i32,
        sessions: i64,
        events: i64,
        revenue: i64,
        top: Option<u16>,
    }

    let results: Vec<(Vec<URow>, Vec<[i64; 4]>)> = ev_by_person
        .into_par_iter()
        .zip(ck_by_person.into_par_iter())
        .map(|(ev_parts, ck_parts)| {
            let mut ev = concat(ev_parts);
            ev.sort_unstable_by_key(|r| r.key);
            let mut ck = concat(ck_parts);
            ck.sort_unstable_by_key(|r| r.key);
            let mut rows: Vec<URow> = Vec::new();
            let mut camp_acc = vec![[0i64; 4]; n_camp * n_days];
            let mut cat_rev = vec![0i64; n_cat];
            let mut counts = vec![0u32; n_camp];
            let mut listed = vec![false; n_camp];
            let mut active: Vec<u16> = Vec::new();
            let mut ci = 0usize;
            let mut i = 0usize;
            while i < ev.len() {
                let p = ev[i].key >> 32;
                let mut j = i;
                while j < ev.len() && ev[j].key >> 32 == p {
                    j += 1;
                }
                // this person's clicks
                while ci < ck.len() && ck[ci].key >> 32 < p {
                    ci += 1;
                }
                let c0 = ci;
                while ci < ck.len() && ck[ci].key >> 32 == p {
                    ci += 1;
                }
                let cl = &ck[c0..ci];
                let (mut lo, mut hi) = (0usize, 0usize);
                for a in active.drain(..) {
                    counts[a as usize] = 0;
                    listed[a as usize] = false;
                }

                // day accumulator
                let mut cur_day: i64 = i64::MIN;
                let (mut d_sess, mut d_ev, mut d_rev) = (0i64, 0i64, 0i64);
                let mut touched: Vec<u16> = Vec::new();
                let flush = |rows: &mut Vec<URow>, day: i64, s: i64, e: i64, r: i64, cat_rev: &mut [i64], touched: &mut Vec<u16>| {
                    let mut top: Option<u16> = None;
                    let mut best = 0i64;
                    for &c in touched.iter() {
                        let v = cat_rev[c as usize];
                        if v > best || (v == best && v > 0 && top.map_or(true, |t| c < t)) {
                            best = v;
                            top = Some(c);
                        }
                    }
                    for &c in touched.iter() {
                        cat_rev[c as usize] = 0;
                    }
                    touched.clear();
                    rows.push(URow { person: p as u32, day: day as i32, sessions: s, events: e, revenue: r, top });
                };

                let mut k = i;
                while k < j {
                    let start_off = (ev[k].key & 0xFFFF_FFFF) as i64;
                    let mut last = start_off;
                    let mut n_ev = 0i64;
                    let mut rev = 0i64;
                    let mut purchased = false;
                    let start_abs = start_off + base;
                    let day = start_abs.div_euclid(DAY_MS);
                    if day != cur_day {
                        if cur_day != i64::MIN {
                            flush(&mut rows, cur_day, d_sess, d_ev, d_rev, &mut cat_rev, &mut touched);
                        }
                        cur_day = day;
                        d_sess = 0;
                        d_ev = 0;
                        d_rev = 0;
                    }
                    while k < j {
                        let t = (ev[k].key & 0xFFFF_FFFF) as i64;
                        if t - last > GAP_MS {
                            break;
                        }
                        last = t;
                        let r = ev[k];
                        n_ev += 1;
                        if r.kind == 3 {
                            purchased = true;
                            rev += r.price as i64;
                            let c = r.cat as usize;
                            if cat_rev[c] == 0 {
                                touched.push(r.cat);
                            }
                            cat_rev[c] += r.price as i64;
                        }
                        k += 1;
                    }
                    d_sess += 1;
                    d_ev += n_ev;
                    d_rev += rev;

                    // attribution: clicks with start-7d <= ts <= start
                    while hi < cl.len() && ((cl[hi].key & 0xFFFF_FFFF) as i64) <= start_off {
                        let c = cl[hi].camp as usize;
                        if !listed[c] {
                            listed[c] = true;
                            active.push(cl[hi].camp);
                        }
                        counts[c] += 1;
                        hi += 1;
                    }
                    while lo < hi && ((cl[lo].key & 0xFFFF_FFFF) as i64) < start_off - WEEK_MS {
                        counts[cl[lo].camp as usize] -= 1;
                        lo += 1;
                    }
                    let di = (day - day0) as usize;
                    let credited = if hi > lo {
                        let t = cl[hi - 1].key;
                        let mut b = hi - 1;
                        let mut m = hi - 1;
                        while b > lo && cl[b - 1].key == t {
                            b -= 1;
                            if cid_of(&cl[b], &long_cids) < cid_of(&cl[m], &long_cids) {
                                m = b;
                            }
                        }
                        // assists
                        active.retain(|&a| {
                            let keep = counts[a as usize] > 0;
                            if !keep {
                                listed[a as usize] = false;
                            }
                            keep
                        });
                        for &a in active.iter() {
                            camp_acc[a as usize * n_days + di][3] += 1;
                        }
                        cl[m].camp
                    } else {
                        organic
                    };
                    let acc = &mut camp_acc[credited as usize * n_days + di];
                    acc[0] += 1;
                    acc[1] += purchased as i64;
                    acc[2] += rev;
                }
                if cur_day != i64::MIN {
                    flush(&mut rows, cur_day, d_sess, d_ev, d_rev, &mut cat_rev, &mut touched);
                }
                i = j;
            }
            (rows, camp_acc)
        })
        .collect();
    log("sessionized");

    // ---- merge campaign accumulators
    let mut camp_total = vec![[0i64; 4]; n_camp * n_days];
    let mut user_rows: Vec<Vec<URow>> = Vec::with_capacity(results.len());
    for (rows, acc) in results {
        for (t, a) in camp_total.iter_mut().zip(acc.iter()) {
            for x in 0..4 {
                t[x] += a[x];
            }
        }
        user_rows.push(rows);
    }

    // ---- output
    let names = interner.snapshot();
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();
    let props = || {
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .build()
    };

    let u_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    let cat_strs: Vec<String> = cat_names.iter().map(|c| String::from_utf8_lossy(c).into_owned()).collect();
    let groups: Vec<Vec<Vec<URow>>> = {
        let ng = ncpu.max(1);
        let mut g: Vec<Vec<Vec<URow>>> = (0..ng).map(|_| vec![]).collect();
        for (i, r) in user_rows.into_iter().enumerate() {
            g[i % ng].push(r);
        }
        g
    };
    groups.into_par_iter().enumerate().for_each(|(gi, parts)| {
        let rows: Vec<URow> = parts.into_iter().flatten().collect();
        let uid = StringArray::from_iter_values(
            rows.iter().map(|r| String::from_utf8_lossy(&names[r.person as usize]).into_owned()),
        );
        let day = Date32Array::from_iter_values(rows.iter().map(|r| r.day));
        let s = Int64Array::from_iter_values(rows.iter().map(|r| r.sessions));
        let e = Int64Array::from_iter_values(rows.iter().map(|r| r.events));
        let rv = Int64Array::from_iter_values(rows.iter().map(|r| r.revenue));
        let tc = StringArray::from_iter(rows.iter().map(|r| r.top.map(|c| cat_strs[c as usize].as_str())));
        let batch = RecordBatch::try_new(
            u_schema.clone(),
            vec![
                Arc::new(uid) as ArrayRef,
                Arc::new(day),
                Arc::new(s),
                Arc::new(e),
                Arc::new(rv),
                Arc::new(tc),
            ],
        )
        .unwrap();
        let f = File::create(ud.join(format!("part-{gi:04}.parquet"))).unwrap();
        let mut w = ArrowWriter::try_new(f, u_schema.clone(), Some(props())).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    });

    let c_schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let (mut cid, mut chn, mut cday, mut cs, mut cps, mut crv, mut cas) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
    for c in 0..n_camp {
        let (id, ch) = if c == organic as usize {
            ("organic".to_string(), "organic".to_string())
        } else {
            (
                String::from_utf8_lossy(&campaigns[c].0).into_owned(),
                String::from_utf8_lossy(&campaigns[c].1).into_owned(),
            )
        };
        for d in 0..n_days {
            let a = camp_total[c * n_days + d];
            if a[0] == 0 && a[3] == 0 {
                continue;
            }
            cid.push(id.clone());
            chn.push(ch.clone());
            cday.push((day0 + d as i64) as i32);
            cs.push(a[0]);
            cps.push(a[1]);
            crv.push(a[2]);
            cas.push(a[3]);
        }
    }
    let batch = RecordBatch::try_new(
        c_schema.clone(),
        vec![
            Arc::new(StringArray::from(cid)) as ArrayRef,
            Arc::new(StringArray::from(chn)),
            Arc::new(Date32Array::from(cday)),
            Arc::new(Int64Array::from(cs)),
            Arc::new(Int64Array::from(cps)),
            Arc::new(Int64Array::from(crv)),
            Arc::new(Int64Array::from(cas)),
        ],
    )
    .unwrap();
    let f = File::create(cd.join("part-0000.parquet")).unwrap();
    let mut w = ArrowWriter::try_new(f, c_schema, Some(props())).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    log("done");
}
