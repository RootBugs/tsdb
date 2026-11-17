# Chronoslice

A **chunked, append-only time-series database** with a series registry,
min/max chunk indexing, step-aggregated range queries (downsampling),
and wholesale-chunk retention — everything in the Python standard
library.

**Why it's special:** the on-disk binary format is fully documented
below (and asserted byte-for-byte in the tests), out-of-order appends
are accepted with a defined ordering policy, and retention is proven to
delete *only* fully expired chunks.

## Series registry

`create_series(metric, tags)` maps a metric name + tag dict → a stable
integer id, persisted in `registry.json`:

```python
cpu = db.create_series("cpu", {"host": "web-1"})   # -> id (int)
db.create_series("CPU", {"host": "web-1"})         # -> same id (canonicalized)
db.find_series("cpu", {"host": "web-1"})           # -> id or None
db.series_info(cpu)                                # -> ("cpu", {"host": "web-1"})
```

Identity is canonical: metric lower-cased, tags sorted by key, so tag
order never creates a duplicate series.  Ids come from a persisted
counter and are never reused.

## Documented policies

* **Range queries are half-open `[start, end)`**: a point at exactly
  `start` is returned; a point at exactly `end` is not.
* **Out-of-order points are accepted** (append-only: records are only
  ever appended to the active chunk; sealed chunks are never
  rewritten).  Queries sort by `(timestamp, append sequence)`, so
  output is chronological and deterministic — including ties, which
  keep append order across reopen.
* **Step buckets are epoch-aligned**: bucket *k* covers
  `[k*step, (k+1)*step)` (floor division — correct for negative
  timestamps too).  **Empty buckets are omitted**; each emitted bucket
  is `(bucket_start, aggregated_value, point_count)`.
* **Retention deletes whole chunks**: a *sealed* chunk is deleted only
  when its newest point is **strictly older** than
  `cutoff = now - retention_ms` (`max_ts < cutoff`; a chunk whose
  `max_ts == cutoff` is kept).  The active chunk is never deleted, and
  a chunk straddling the cutoff is kept whole — no partial rewrites.
* A corrupt or foreign file in `chunks/` is skipped at open (the store
  still starts); valid chunks are unaffected.

## Binary format

Directory layout:

```
<db>/
  registry.json               series registry (JSON, shown above)
  chunks/chunk-000001.bin     append-only chunk files
  chunks/chunk-000002.bin
```

Chunk file (little-endian throughout):

```
offset  size  field
------  ----  -------------------------------------------------------
     0     4  magic  b"TSC1"
     4     4  u32    version = 1
     8     8  u64    record_count
    16     8  i64    min_ts      <- on-disk min/max timestamp index
    24     8  i64    max_ts
    32    24  records: record_count × (see below)
```

Each record, 24 bytes:

```
     0     4  u32    series_id
     4     8  i64    timestamp (caller-defined units; README/demo uses ms)
    12     8  f64    value
    20     4  u32    seq   (global append counter; breaks timestamp ties)
```

The 32-byte header is rewritten in place on every append so the
min/max index is always current; record bytes are only appended.  At
open time the store reads just each chunk's header — the index — and
queries open only chunks whose `[min_ts, max_ts]` intersects the query
range.

## Runnable example

```python
# demo.py in this repository — output shown below
from tsdb import TSDB

db = TSDB("metrics", chunk_max_records=4, retention_ms=3000)
cpu = db.create_series("cpu", {"host": "web-1"})
for ts, value in [(1000, 4.0), (2000, 6.0), (1500, 5.0), (3000, 9.0),
                  (2500, 7.0), (4000, 8.0), (5000, 10.0)]:
    db.append(cpu, ts, value)

print("raw [1000, 6000):")
print(" ", db.query(cpu, 1000, 6000))
print("step=1000 avg:")
print(" ", db.query(cpu, 1000, 6000, step=1000, aggregate="avg"))
print("step=1000 sum / count:")
print(" ", db.query(cpu, 1000, 6000, step=1000, aggregate="sum"))
print(" ", db.query(cpu, 1000, 6000, step=1000, aggregate="count"))

stats = db.stats()
print("stats:", {k: stats[k] for k in ("series", "chunks", "records")})
for c in stats["chunk_files"]:
    print(" ", c["file"], "records=", c["records"],
          "min_ts=", c["min_ts"], "max_ts=", c["max_ts"])

print("prune(now=9000):", db.prune(now=9000))
print("remaining:", db.query(cpu, 0, 10000))
db.close()
```

Output (`python demo.py`):

```text
raw [1000, 6000):
  [(1000, 4.0), (1500, 5.0), (2000, 6.0), (2500, 7.0), (3000, 9.0), (4000, 8.0), (5000, 10.0)]
step=1000 avg:
  [(1000, 4.5), (2000, 6.5), (3000, 9.0), (4000, 8.0), (5000, 10.0)]
step=1000 sum / count:
  [(1000, 9.0), (2000, 13.0), (3000, 9.0), (4000, 8.0), (5000, 10.0)]
  [(1000, 2.0), (2000, 2.0), (3000, 1.0), (4000, 1.0), (5000, 1.0)]
stats: {'series': 1, 'chunks': 2, 'records': 7}
  chunk-000001.bin records= 4 min_ts= 1000 max_ts= 3000
  chunk-000002.bin records= 3 min_ts= 2500 max_ts= 5000
prune(now=9000): ['chunk-000001.bin']
remaining: [(2500, 7.0), (4000, 8.0), (5000, 10.0)]
```

Note the out-of-order appends (1500 after 2000) coming back sorted, the
two epoch-aligned buckets holding two points each, and retention
removing `chunk-000001` (its `max_ts=3000 < cutoff=6000`) while the
active chunk survives with everything it holds.

## API summary

```python
db = TSDB(dir, chunk_max_records=256, retention_ms=None)
db.create_series(metric, tags=None) -> id
db.find_series(metric, tags=None) -> id | None
db.series_info(id) -> (metric, tags)
db.list_series() -> [(id, metric, tags)]
db.append(id, ts, value)            # out-of-order OK
db.append_many(id, [(ts, value), ...])
db.query(id, start=None, end=None, step=None, aggregate="avg")
db.query_buckets(id, ...)           # -> [(bucket, value, count)]
db.set_retention(ms | None)
db.prune(now) -> [deleted file names]   # explicit `now`, deterministic
db.stats() -> dict                  # chunks + per-file min/max index
db.close() / with TSDB(...) as db
```

Aggregates: `min`, `max`, `avg`, `sum`, `count`.  Errors: `UnknownSeries`,
`BadQuery`, `TSDBError`, `ChunkError`, `RegistryError`.

## Test command

```
python -m unittest discover -s tests -v
```

22 tests: registry identity + persistence, append/query roundtrip,
out-of-order sorting and tie determinism, `[start, end)` boundary
behavior, hand-computed `min/max/avg/sum/count` buckets, epoch-aligned
bucket starts, omitted empty buckets, chunk rollover with min/max index
checks, byte-level binary header/record layout, reopen/resume,
retention (policy errors, only-expired pruning, cutoff boundary,
straddling chunk kept, survives reopen), and argument validation.
