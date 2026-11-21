//! Fast path: every ID packs into 128 bits (at most 15 bytes), so no string interning.
//!
//! Packed keys are big-endian bytes with the length in the last byte, so comparing two
//! keys as integers compares the IDs byte-wise. If any ID needed here is longer than
//! 15 bytes, `run` returns `Err(())` and the caller falls back to the string-based path.
//!
//! Phases (all parallel):
//!   links:   parse -> keys bucketed by hash -> union-find -> person key per linked ID
//!   events:  SIMD line scan -> records bucketed by hash(event_id)
//!   clicks:  SIMD line scan -> records bucketed by hash(user_id)
//!   dedup:   per event_id bucket, open-addressing table keeps the smallest ingest_ts
//!   resolve: per user bucket, user -> person via that bucket's slice of the links
//!   session: per person bucket, sort, sessionize, attribute, aggregate

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;

use crate::slow::{
    DAY_MS, GAP_MS, Lookup, WEEK_MS, list_csv, manifest_ts, map_files, mix, parse_price, parse_ts,
    read_pairs, read_small,
};

/// Packed ID: (high, low) halves of the big-endian 128-bit key.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Debug)]
struct K(u64, u64);

impl std::hash::Hash for K {
    #[inline(always)]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u128(((self.0 as u128) << 64) | self.1 as u128);
    }
}
type KMap<V> = std::collections::HashMap<K, V, crate::slow::MixState>;

#[inline(always)]
fn pack(s: &[u8]) -> Option<K> {
    if s.len() > 15 {
        return None;
    }
    let mut b = [0u8; 16];
    b[..s.len()].copy_from_slice(s);
    b[15] = s.len() as u8;
    let v = u128::from_be_bytes(b);
    Some(K((v >> 64) as u64, v as u64))
}

#[inline(always)]
fn unpack(k: K) -> String {
    let b = (((k.0 as u128) << 64) | k.1 as u128).to_be_bytes();
    String::from_utf8_lossy(&b[..b[15] as usize]).into_owned()
}

#[inline(always)]
fn hk(k: K) -> u64 {
    mix(((k.0 as u128) << 64) | k.1 as u128)
}

const PB: usize = 256; // person / user buckets
#[inline(always)]
fn pbucket(k: K) -> usize {
    ((hk(k) >> 32) as usize) & (PB - 1)
}

// ---------------------------------------------------------------- SIMD line scanner

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
fn masks64(b: &[u8]) -> u64 {
    use std::arch::x86_64::*;
    unsafe {
        let p = b.as_ptr();
        let a = _mm256_loadu_si256(p as *const __m256i);
        let c = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let comma = _mm256_set1_epi8(b',' as i8);
        let nl = _mm256_set1_epi8(b'\n' as i8);
        let q = _mm256_set1_epi8(b'"' as i8);
        let ma = _mm256_or_si256(
            _mm256_or_si256(_mm256_cmpeq_epi8(a, comma), _mm256_cmpeq_epi8(a, nl)),
            _mm256_cmpeq_epi8(a, q),
        );
        let mc = _mm256_or_si256(
            _mm256_or_si256(_mm256_cmpeq_epi8(c, comma), _mm256_cmpeq_epi8(c, nl)),
            _mm256_cmpeq_epi8(c, q),
        );
        (_mm256_movemask_epi8(ma) as u32 as u64) | ((_mm256_movemask_epi8(mc) as u32 as u64) << 32)
    }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline(always)]
fn masks64(b: &[u8]) -> u64 {
    let mut m = 0u64;
    for (i, &c) in b[..64].iter().enumerate() {
        m |= ((c == b',' || c == b'\n' || c == b'"') as u64) << i;
    }
    m
}

#[inline(always)]
fn unquote(v: &[u8]) -> Option<&[u8]> {
    if v.first() == Some(&b'"') {
        if v.len() < 2 || v[v.len() - 1] != b'"' || memchr::memchr(b'"', &v[1..v.len() - 1]).is_some() {
            return None;
        }
        Some(&v[1..v.len() - 1])
    } else if memchr::memchr(b'"', v).is_some() {
        None
    } else {
        Some(v)
    }
}

/// Call `f` with the N fields of every well-formed line of `buf` (no header in `buf`).
#[inline(always)]
fn for_records<'a, const N: usize>(buf: &'a [u8], mut f: impl FnMut(&[&'a [u8]; N])) {
    let n = buf.len();
    let mut cuts = [0usize; N];
    let mut nc = 0usize;
    let mut quote = false;
    let mut line_start = 0usize;
    let mut emit = |start: usize, end: usize, cuts: &[usize; N], nc: usize, quote: bool| {
        if nc != N - 1 {
            return;
        }
        let mut fs: [&'a [u8]; N] = [&[]; N];
        let mut s = start;
        for i in 0..N - 1 {
            fs[i] = &buf[s..cuts[i]];
            s = cuts[i] + 1;
        }
        fs[N - 1] = &buf[s..end];
        if quote {
            for x in fs.iter_mut() {
                match unquote(x) {
                    Some(v) => *x = v,
                    None => return,
                }
            }
        }
        f(&fs);
    };
    let mut base = 0usize;
    let mut tail = [0u8; 64];
    while base < n {
        let mut mask = if base + 64 <= n {
            masks64(&buf[base..base + 64])
        } else {
            tail[..n - base].copy_from_slice(&buf[base..n]);
            tail[n - base..].fill(0);
            masks64(&tail)
        };
        while mask != 0 {
            let pos = base + mask.trailing_zeros() as usize;
            mask &= mask - 1;
            match buf[pos] {
                b',' => {
                    if nc < N {
                        cuts[nc] = pos;
                    }
                    nc += 1;
                }
                b'"' => quote = true,
                _ => {
                    emit(line_start, pos, &cuts, nc, quote);
                    line_start = pos + 1;
                    nc = 0;
                    quote = false;
                }
            }
        }
        base += 64;
    }
    if line_start < n {
        emit(line_start, n, &cuts, nc, quote);
    }
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
struct E {
    ek: K,
    uk: K,
    ingest: i64,
    ts: u32,
    price: u32,
    cat: u16,
    kind: u8, // 0 = invalid copy
}

#[derive(Clone, Copy)]
struct S {
    k: K, // user key, then person key
    ts: u32,
    price: u32,
    cat: u16,
    kind: u8,
}

#[derive(Clone, Copy)]
struct C {
    k: K, // user key, then person key
    ck: K,
    ts: u32,
    camp: u16,
}

/// Event in the session phase: key = person index << 32 | ts; price carries kind in bits 30-31.
#[derive(Clone, Copy)]
struct R {
    key: u64,
    price: u32,
    cat: u16,
}

#[derive(Clone, Copy)]
struct CR {
    key: u64,
    ck: K,
    camp: u16,
}

/// The bytes of a chunk, faulted in by the calling worker in one go.
#[inline]
fn chunk_bytes<'a>(maps: &'a [Option<memmap2::Mmap>], c: &crate::slow::Chunk) -> &'a [u8] {
    let m = maps[c.file].as_ref().unwrap();
    #[cfg(target_os = "linux")]
    {
        let page = c.start & !4095;
        let _ = m.advise_range(memmap2::Advice::PopulateRead, page, c.end - page);
    }
    &m[c.start..c.end]
}

fn concat<T: Copy>(parts: Vec<Vec<T>>) -> Vec<T> {
    let n = parts.iter().map(|v| v.len()).sum();
    let mut v = Vec::with_capacity(n);
    for p in parts {
        v.extend_from_slice(&p);
    }
    v
}

/// parts[task][bucket] -> out[bucket][task], dropping empty pieces.
fn transpose<T>(parts: Vec<Vec<Vec<T>>>, nb: usize) -> Vec<Vec<Vec<T>>> {
    let mut out: Vec<Vec<Vec<T>>> = (0..nb).map(|_| Vec::with_capacity(parts.len())).collect();
    for p in parts {
        for (b, v) in p.into_iter().enumerate() {
            if !v.is_empty() {
                out[b].push(v);
            }
        }
    }
    out
}

/// Linked IDs of one user bucket: sorted keys and the person key of each.
struct LinkBucket {
    map: KMap<K>,
}

fn build_links(data: &PathBuf, long: &AtomicBool) -> Vec<LinkBucket> {
    let paths = list_csv(&data.join("links"));
    let (maps, chunks) = map_files(&paths);
    // parse: pairs per chunk, plus every key bucketed
    let parsed: Vec<(Vec<(K, K)>, Vec<Vec<K>>)> = chunks
        .par_iter()
        .map(|c| {
            let buf = chunk_bytes(&maps, c);
            let mut pairs = vec![];
            let mut keys: Vec<Vec<K>> = (0..PB).map(|_| vec![]).collect();
            for_records::<2>(buf, |f| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let (Some(a), Some(b)) = (pack(f[0]), pack(f[1])) else {
                    long.store(true, Ordering::Relaxed);
                    return;
                };
                pairs.push((a, b));
                keys[pbucket(a)].push(a);
                keys[pbucket(b)].push(b);
            });
            (pairs, keys)
        })
        .collect();
    let mut pair_lists = Vec::with_capacity(parsed.len());
    let mut key_parts = Vec::with_capacity(parsed.len());
    for (p, k) in parsed {
        pair_lists.push(p);
        key_parts.push(k);
    }
    let keys: Vec<Vec<K>> = transpose(key_parts, PB)
        .into_par_iter()
        .map(|parts| {
            let mut v = concat(parts);
            v.sort_unstable();
            v.dedup();
            v
        })
        .collect();
    let mut off = vec![0u32; PB + 1];
    for b in 0..PB {
        off[b + 1] = off[b] + keys[b].len() as u32;
    }
    let n = off[PB] as usize;
    let id = |k: K| -> u32 {
        let b = pbucket(k);
        off[b] + keys[b].binary_search(&k).unwrap() as u32
    };
    let id_pairs: Vec<Vec<(u32, u32)>> =
        pair_lists.par_iter().map(|v| v.iter().map(|&(a, b)| (id(a), id(b))).collect()).collect();
    // union-find
    let mut parent: Vec<u32> = (0..n as u32).collect();
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        while p[x as usize] != x {
            let g = p[p[x as usize] as usize];
            p[x as usize] = g;
            x = g;
        }
        x
    }
    for v in &id_pairs {
        for &(a, b) in v {
            let ra = find(&mut parent, a);
            let rb = find(&mut parent, b);
            if ra != rb {
                parent[ra.max(rb) as usize] = ra.min(rb);
            }
        }
    }
    // name of each component: smallest key
    let mut key_of = vec![K::default(); n];
    for b in 0..PB {
        key_of[off[b] as usize..off[b + 1] as usize].copy_from_slice(&keys[b]);
    }
    let mut best: Vec<K> = vec![K(u64::MAX, u64::MAX); n];
    let mut root = vec![0u32; n];
    for x in 0..n {
        let r = find(&mut parent, x as u32);
        root[x] = r;
        if key_of[x] < best[r as usize] {
            best[r as usize] = key_of[x];
        }
    }
    keys.into_par_iter()
        .enumerate()
        .map(|(b, keys)| {
            let o = off[b] as usize;
            let mut map: KMap<K> = KMap::with_capacity_and_hasher(keys.len(), Default::default());
            for (i, k) in keys.into_iter().enumerate() {
                map.insert(k, best[root[o + i] as usize]);
            }
            LinkBucket { map }
        })
        .collect()
}

// ---------------------------------------------------------------- main

pub fn run(data: PathBuf, out: PathBuf, ncpu: usize) -> Result<(), ()> {
    let t0 = Instant::now();
    let log = |s: &str| eprintln!("[{:>7.3}s] {}", t0.elapsed().as_secs_f64(), s);
    let long = AtomicBool::new(false);

    // ---- lookups
    let manifest = String::from_utf8_lossy(&read_small(&data.join("manifest.json"))).into_owned();
    let ws = manifest_ts(&manifest, "\"window_start\"");
    let we = manifest_ts(&manifest, "\"window_end\"");
    let base = ws - WEEK_MS;
    if we - base >= (1i64 << 32) {
        return Err(());
    }
    let products = read_pairs(&data.join("products.csv"));
    let mut cat_names: Vec<Vec<u8>> = products.iter().map(|(_, c)| c.clone()).collect();
    cat_names.push(b"unknown".to_vec());
    cat_names.sort();
    cat_names.dedup();
    let cat_index: std::collections::HashMap<Vec<u8>, u16> =
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
    let n_camp = campaigns.len() + 1;

    // ---- links (in parallel with parsing)
    let ev_paths = list_csv(&data.join("events"));
    let ck_paths = list_csv(&data.join("clicks"));
    let (ev_maps, ev_chunks) = map_files(&ev_paths);
    let (ck_maps, ck_chunks) = map_files(&ck_paths);
    let ev_bytes: usize = ev_chunks.iter().map(|c| c.end - c.start).sum();
    let dbits: u32 = if ev_bytes > (4usize << 30) { 9 } else { 8 };
    let nd = 1usize << dbits;
    log(&format!("lookups; {} event chunks, {} dedup buckets", ev_chunks.len(), nd));

    let (links, (ev_parts, ck_parts)) = rayon::join(
        || build_links(&data, &long),
        || {
            rayon::join(
                || {
                    ev_chunks
                        .par_iter()
                        .map(|c| {
                            let buf = chunk_bytes(&ev_maps, c);
                            let cap = (c.end - c.start) / 70 / nd + 4;
                            let mut out: Vec<Vec<E>> = (0..nd).map(|_| Vec::with_capacity(cap)).collect();
                            for_records::<7>(buf, |f| {
                                if f[0].is_empty() {
                                    return;
                                }
                                let Some(ek) = pack(f[0]) else {
                                    long.store(true, Ordering::Relaxed);
                                    return;
                                };
                                let ingest = parse_ts(f[3]).unwrap_or(i64::MAX);
                                let mut r = E {
                                    ek,
                                    uk: K::default(),
                                    ingest,
                                    ts: 0,
                                    price: 0,
                                    cat: 0,
                                    kind: 0,
                                };
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
                                    let Some(uk) = pack(f[1]) else {
                                        long.store(true, Ordering::Relaxed);
                                        break 'v;
                                    };
                                    r.uk = uk;
                                    r.cat = prod.get(f[5]).unwrap_or(unknown_cat);
                                    r.ts = (ts - base) as u32;
                                    r.kind = kind;
                                }
                                out[(hk(ek) >> (64 - dbits)) as usize].push(r);
                            });
                            out
                        })
                        .collect::<Vec<_>>()
                },
                || {
                    ck_chunks
                        .par_iter()
                        .map(|c| {
                            let buf = chunk_bytes(&ck_maps, c);
                            let mut out: Vec<Vec<C>> = (0..PB).map(|_| Vec::new()).collect();
                            for_records::<4>(buf, |f| {
                                if f[0].is_empty() || f[1].is_empty() {
                                    return;
                                }
                                let Some(ts) = parse_ts(f[3]) else { return };
                                if ts < base || ts >= we {
                                    return;
                                }
                                let Some(cp) = camp.get(f[2]) else { return };
                                let (Some(uk), Some(ck)) = (pack(f[1]), pack(f[0])) else {
                                    long.store(true, Ordering::Relaxed);
                                    return;
                                };
                                out[pbucket(uk)].push(C { k: uk, ck, ts: (ts - base) as u32, camp: cp });
                            });
                            out
                        })
                        .collect::<Vec<_>>()
                },
            )
        },
    );
    log("parsed");
    if long.load(Ordering::Relaxed) {
        return Err(());
    }
    drop(ev_maps);
    drop(ck_maps);

    // ---- dedup: groups of event_id buckets, survivors bucketed by user
    let ev_buckets = transpose(ev_parts, nd);
    let group = (nd / (ncpu * 8)).max(1);
    let mut groups: Vec<Vec<Vec<Vec<E>>>> = vec![];
    for (i, b) in ev_buckets.into_iter().enumerate() {
        if i % group == 0 {
            groups.push(vec![]);
        }
        groups.last_mut().unwrap().push(b);
    }
    let sv_parts: Vec<Vec<Vec<S>>> = groups
        .into_par_iter()
        .map(|g| {
            let mut out: Vec<Vec<S>> = (0..PB).map(|_| Vec::new()).collect();
            let mut table: Vec<u32> = vec![];
            for parts in g {
                let v = concat(parts);
                let size = (v.len() * 2).next_power_of_two().max(16);
                let mask = size - 1;
                table.clear();
                table.resize(size, u32::MAX);
                for (i, r) in v.iter().enumerate() {
                    let mut s = hk(r.ek) as usize & mask;
                    loop {
                        let t = table[s];
                        if t == u32::MAX {
                            table[s] = i as u32;
                            break;
                        }
                        let o = &v[t as usize];
                        if o.ek == r.ek {
                            if r.ingest < o.ingest {
                                table[s] = i as u32;
                            }
                            break;
                        }
                        s = (s + 1) & mask;
                    }
                }
                for &t in &table {
                    if t != u32::MAX {
                        let r = &v[t as usize];
                        if r.kind != 0 {
                            out[pbucket(r.uk)].push(S { k: r.uk, ts: r.ts, price: r.price, cat: r.cat, kind: r.kind });
                        }
                    }
                }
            }
            out
        })
        .collect();
    log("deduped");

    // ---- resolve user -> person, bucketed by person
    let sv_by_user = transpose(sv_parts, PB);
    let ck_by_user = transpose(ck_parts, PB);
    let resolved: Vec<(Vec<Vec<S>>, Vec<Vec<C>>)> = sv_by_user
        .into_par_iter()
        .zip(ck_by_user.into_par_iter())
        .zip(links.par_iter())
        .map(|((sp, cp), lb)| {
            let look = |k: K| -> K { lb.map.get(&k).copied().unwrap_or(k) };
            let ns: usize = sp.iter().map(|v| v.len()).sum();
            let nc: usize = cp.iter().map(|v| v.len()).sum();
            let mut so: Vec<Vec<S>> = (0..PB).map(|_| Vec::with_capacity(ns / PB * 5 / 4 + 16)).collect();
            let mut co: Vec<Vec<C>> = (0..PB).map(|_| Vec::with_capacity(nc / PB * 5 / 4 + 16)).collect();
            let mut last = (K(u64::MAX, u64::MAX), K::default());
            for part in sp {
                for mut r in part {
                    if r.k != last.0 {
                        last = (r.k, look(r.k));
                    }
                    r.k = last.1;
                    so[pbucket(r.k)].push(r);
                }
            }
            for part in cp {
                for mut r in part {
                    if r.k != last.0 {
                        last = (r.k, look(r.k));
                    }
                    r.k = last.1;
                    co[pbucket(r.k)].push(r);
                }
            }
            (so, co)
        })
        .collect();
    let mut sp = Vec::with_capacity(PB);
    let mut cp = Vec::with_capacity(PB);
    for (s, c) in resolved {
        sp.push(s);
        cp.push(c);
    }
    let ev_by_person = transpose(sp, PB);
    let ck_by_person = transpose(cp, PB);
    log("resolved");

    // ---- sessions
    let day0 = ws.div_euclid(DAY_MS);
    let n_days = ((we - 1).div_euclid(DAY_MS) - day0 + 1) as usize;

    struct URow {
        person: K,
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
            // dense person index within this bucket; sort keys are (index << 32 | ts)
            let n_ev: usize = ev_parts.iter().map(|v| v.len()).sum();
            let mut idx: KMap<u32> = KMap::with_capacity_and_hasher(n_ev / 8 + 16, Default::default());
            let mut names: Vec<K> = Vec::new();
            let mut ev: Vec<R> = Vec::with_capacity(n_ev);
            let mut last = (K(u64::MAX, u64::MAX), 0u32);
            for part in ev_parts {
                for r in part {
                    if r.k != last.0 {
                        let n = names.len() as u32;
                        let id = *idx.entry(r.k).or_insert_with(|| {
                            names.push(r.k);
                            n
                        });
                        last = (r.k, id);
                    }
                    ev.push(R {
                        key: ((last.1 as u64) << 32) | r.ts as u64,
                        price: r.price | ((r.kind as u32) << 30),
                        cat: r.cat,
                    });
                }
            }
            ev.sort_unstable_by_key(|r| r.key);
            let mut ck: Vec<CR> = Vec::new();
            for part in ck_parts {
                for r in part {
                    if r.k != last.0 {
                        match idx.get(&r.k) {
                            Some(&id) => last = (r.k, id),
                            None => continue, // person without events
                        }
                    }
                    ck.push(CR { key: ((last.1 as u64) << 32) | r.ts as u64, ck: r.ck, camp: r.camp });
                }
            }
            ck.sort_unstable_by(|a, b| (a.key, a.ck).cmp(&(b.key, b.ck)));
            let mut rows: Vec<URow> = Vec::new();
            let mut camp_acc = vec![[0i64; 4]; n_camp * n_days];
            let mut cat_rev = vec![0i64; n_cat];
            let mut touched: Vec<u16> = Vec::new();
            let mut counts = vec![0u32; n_camp];
            let mut listed = vec![false; n_camp];
            let mut active: Vec<u16> = Vec::new();
            let mut ci = 0usize;
            let mut i = 0usize;
            while i < ev.len() {
                let pi = ev[i].key >> 32;
                let p = names[pi as usize];
                let mut j = i + 1;
                while j < ev.len() && ev[j].key >> 32 == pi {
                    j += 1;
                }
                while ci < ck.len() && ck[ci].key >> 32 < pi {
                    ci += 1;
                }
                let c0 = ci;
                while ci < ck.len() && ck[ci].key >> 32 == pi {
                    ci += 1;
                }
                let cl = &ck[c0..ci];
                let (mut lo, mut hi) = (0usize, 0usize);
                for a in active.drain(..) {
                    counts[a as usize] = 0;
                    listed[a as usize] = false;
                }
                let mut cur_day: i64 = i64::MIN;
                let (mut d_sess, mut d_ev, mut d_rev) = (0i64, 0i64, 0i64);
                let flush = |rows: &mut Vec<URow>,
                             day: i64,
                             s: i64,
                             e: i64,
                             r: i64,
                             cat_rev: &mut [i64],
                             touched: &mut Vec<u16>| {
                    let mut top: Option<u16> = None;
                    let mut best = 0i64;
                    for &c in touched.iter() {
                        let v = cat_rev[c as usize];
                        if v > best || (v == best && top.is_some_and(|t| c < t)) {
                            best = v;
                            top = Some(c);
                        }
                        cat_rev[c as usize] = 0;
                    }
                    touched.clear();
                    rows.push(URow { person: p, day: day as i32, sessions: s, events: e, revenue: r, top });
                };
                let mut k = i;
                while k < j {
                    let start = (ev[k].key & 0xFFFF_FFFF) as i64;
                    let mut last = start;
                    let (mut n_ev, mut rev, mut purchased) = (0i64, 0i64, false);
                    let day = (start + base).div_euclid(DAY_MS);
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
                        let r = ev[k];
                        let t = (r.key & 0xFFFF_FFFF) as i64;
                        let kind = r.price >> 30;
                        let price = (r.price & 0x3FFF_FFFF) as i64;
                        if t - last > GAP_MS {
                            break;
                        }
                        last = t;
                        n_ev += 1;
                        if kind == 3 {
                            purchased = true;
                            rev += price;
                            let c = r.cat as usize;
                            if cat_rev[c] == 0 {
                                touched.push(r.cat);
                            }
                            cat_rev[c] += price;
                        }
                        k += 1;
                    }
                    d_sess += 1;
                    d_ev += n_ev;
                    d_rev += rev;

                    while hi < cl.len() && ((cl[hi].key & 0xFFFF_FFFF) as i64) <= start {
                        let c = cl[hi].camp as usize;
                        if !listed[c] {
                            listed[c] = true;
                            active.push(cl[hi].camp);
                        }
                        counts[c] += 1;
                        hi += 1;
                    }
                    while lo < hi && ((cl[lo].key & 0xFFFF_FFFF) as i64) < start - WEEK_MS {
                        counts[cl[lo].camp as usize] -= 1;
                        lo += 1;
                    }
                    let di = (day - day0) as usize;
                    let credited = if hi > lo {
                        // latest ts; clicks are sorted by (ts, click_id), so the first of the run wins
                        let t = cl[hi - 1].key;
                        let mut b = hi - 1;
                        while b > lo && cl[b - 1].key == t {
                            b -= 1;
                        }
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
                        cl[b].camp
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
    let ng = ncpu.max(1);
    let mut groups: Vec<Vec<Vec<URow>>> = (0..ng).map(|_| vec![]).collect();
    for (i, r) in user_rows.into_iter().enumerate() {
        groups[i % ng].push(r);
    }
    groups.into_par_iter().enumerate().for_each(|(gi, parts)| {
        let rows: Vec<URow> = parts.into_iter().flatten().collect();
        let uid = StringArray::from_iter_values(rows.iter().map(|r| unpack(r.person)));
        let day = Date32Array::from_iter_values(rows.iter().map(|r| r.day));
        let s = Int64Array::from_iter_values(rows.iter().map(|r| r.sessions));
        let e = Int64Array::from_iter_values(rows.iter().map(|r| r.events));
        let rv = Int64Array::from_iter_values(rows.iter().map(|r| r.revenue));
        let tc = StringArray::from_iter(rows.iter().map(|r| r.top.map(|c| cat_strs[c as usize].as_str())));
        let batch = RecordBatch::try_new(
            u_schema.clone(),
            vec![Arc::new(uid) as ArrayRef, Arc::new(day), Arc::new(s), Arc::new(e), Arc::new(rv), Arc::new(tc)],
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
    Ok(())
}
