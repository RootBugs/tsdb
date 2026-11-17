import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, '.')
from tsdb import TSDB

tmp = Path(tempfile.mkdtemp())
try:
    db = TSDB(tmp / "metrics", chunk_max_records=4, retention_ms=3000)
    cpu = db.create_series("cpu", {"host": "web-1"})

    # out-of-order appends are accepted (documented policy)
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

    # retention: 3000ms window -> at now=9000, cutoff=6000
    print("prune(now=9000):", db.prune(now=9000))
    print("remaining:", db.query(cpu, 0, 10000))
    db.close()
finally:
    shutil.rmtree(tmp, ignore_errors=True)
