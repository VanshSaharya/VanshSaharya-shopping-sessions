# Shopping Sessions

A Rust implementation of [`rules.md`](rules.md). There is a single binary (`src/main.rs`). It reads `$DATA_DIR`, writes Parquet to `$OUTPUT_DIR`, and uses all `$NCPU` threads.

## How it works
**Fast path (`src/fast.rs`).** Every ID of 15 bytes or fewer is packed into a 128-bit key whose integer order is byte-wise order, so there is no string interning. Records are partitioned by key hash so that every table stays small and cache-resident: event_id buckets for dedup, user buckets for link resolution, person buckets for sessions. Lines are scanned with AVX2, 64 bytes at a time. Links are built in parallel with event parsing.

**Fallback (`src/slow.rs`).** The original string-based implementation. It runs only if an ID longer than 15 bytes appears; this is tested.

The steps below describe the fallback; the fast path follows the same rules.
1. **Lookups.** Reads the manifest, products and campaigns. Categories are sorted byte-wise so that index order breaks ties.
2. **Links.** Builds a union-find over interned IDs. Each person is named by their byte-wise smallest ID.
3. **Parse.** mmaps every CSV, cuts it into ~8 MB chunks at line boundaries and parses them in parallel with a hand-written parser for the exact dialect: quote syntax, `\r` kept as data, ASCII-only digits, and strict F1/F2/F3 timestamps.
4. **Event dedup.** Buckets by xxh3-128 of `event_id` and keeps the copy with the smallest `ingest_ts`, then validates that kept copy. Survivors are scattered by person.
5. **Per-person bucket pass.** Sorts by `(person, ts)`, sessionizes on gaps over 30 min, and attributes each session with a sliding 7-day click window (latest click; ties go to the smallest `click_id`). Assists count each distinct campaign in the window once.
6. **Output.** Writes Parquet files in parallel (`day` as `DATE`).

## Test
```sh
cargo build --release
python3 scripts/run_fixtures.py fixtures -- target/release/pipeline        # 36/36
DATA_DIR=dev OUTPUT_DIR=out target/release/pipeline && python3 scripts/compare.py out dev-answer
python3 scripts/reference.py DATA OUT_CSV   # slow, literal reference used to cross-check other seeds
```
