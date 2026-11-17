"""tsdb tests: registry, roundtrip, downsampling, boundaries, retention,
persistence, binary format."""
import struct
import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from tsdb import (  # noqa: E402
    HEADER_SIZE,
    RECORD_SIZE,
    BadQuery,
    TSDB,
    TSDBError,
    UnknownSeries,
)


class TSDBTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="tsdb-")
        self.addCleanup(self._tmp.cleanup)
        self.dir = Path(self._tmp.name) / "db"

    def make(self, **kw) -> TSDB:
        db = TSDB(self.dir, **kw)
        self.addCleanup(db.close)
        return db


class TestRegistry(TSDBTestCase):
    def test_create_find_and_identity_canonicalization(self):
        db = self.make()
        a = db.create_series("cpu", {"host": "a", "zone": "z1"})
        # tag order and metric case do not create new series
        same = db.create_series("CPU", {"zone": "z1", "host": "a"})
        self.assertEqual(a, same)
        self.assertEqual(db.find_series("cpu", {"host": "a", "zone": "z1"}), a)
        other = db.create_series("cpu", {"host": "b", "zone": "z1"})
        self.assertNotEqual(a, other)
        self.assertIsNone(db.find_series("cpu", {"host": "missing"}))
        metric, tags = db.series_info(a)
        self.assertEqual(metric, "cpu")
        self.assertEqual(tags, {"host": "a", "zone": "z1"})
        self.assertEqual(len(db.list_series()), 2)

    def test_registry_persists_across_reopen(self):
        db = self.make()
        sid = db.create_series("disk", {"mount": "/a"})
        db.append(sid, 10, 1.0)
        db.close()
        db2 = self.make()
        self.assertEqual(db2.find_series("disk", {"mount": "/a"}), sid)
        self.assertEqual(db2.series_info(sid), ("disk", {"mount": "/a"}))
        # ids continue from the persisted counter
        self.assertEqual(db2.create_series("disk", {"mount": "/b"}), sid + 1)

    def test_unknown_series_errors(self):
        db = self.make()
        with self.assertRaises(UnknownSeries):
            db.append(42, 0, 1.0)
        with self.assertRaises(UnknownSeries):
            db.query(42, 0, 10)
        with self.assertRaises(UnknownSeries):
            db.series_info(42)


class TestRoundtripAndBoundaries(TSDBTestCase):
    def test_append_query_roundtrip_sorted(self):
        db = self.make()
        sid = db.create_series("cpu")
        points = [(300, 3.0), (100, 1.0), (200, 2.0), (400, 4.0)]
        for ts, v in points:
            db.append(sid, ts, v)
        self.assertEqual(db.query(sid, 0, 1000), [(100, 1.0), (200, 2.0), (300, 3.0), (400, 4.0)])
        self.assertEqual(db.query(sid), [(100, 1.0), (200, 2.0), (300, 3.0), (400, 4.0)])

    def test_out_of_order_points_accepted_and_sorted(self):
        """Documented policy: out-of-order appends are accepted; queries
        return chronological order (ties by append sequence)."""
        db = self.make()
        sid = db.create_series("cpu")
        for ts in [500, 100, 900, 300]:
            db.append(sid, ts, float(ts))
        self.assertEqual(
            db.query(sid, 0, 1000),
            [(100, 100.0), (300, 300.0), (500, 500.0), (900, 900.0)],
        )

    def test_timestamp_ties_keep_append_order_deterministically(self):
        db = self.make()
        sid = db.create_series("cpu")
        db.append(sid, 100, 1.0)
        db.append(sid, 100, 2.0)
        db.append(sid, 100, 3.0)
        self.assertEqual(db.query(sid, 0, 200), [(100, 1.0), (100, 2.0), (100, 3.0)])
        db.close()
        db2 = self.make()
        self.assertEqual(db2.query(sid, 0, 200), [(100, 1.0), (100, 2.0), (100, 3.0)])

    def test_range_is_half_open_start_inclusive_end_exclusive(self):
        db = self.make()
        sid = db.create_series("cpu")
        for ts in [0, 10, 20, 30]:
            db.append(sid, ts, float(ts))
        # start inclusive: point at exactly `start` is returned
        self.assertEqual([t for t, _ in db.query(sid, 10, 30)], [10, 20])
        # end exclusive: point at exactly `end` is excluded
        self.assertEqual([t for t, _ in db.query(sid, 0, 20)], [0, 10])
        self.assertEqual(db.query(sid, 31, 100), [])
        self.assertEqual(db.query(sid, 10, 10), [])  # empty window

    def test_multi_series_isolation(self):
        db = self.make()
        a = db.create_series("cpu", {"host": "a"})
        b = db.create_series("cpu", {"host": "b"})
        db.append(a, 10, 1.0)
        db.append(b, 10, 99.0)
        db.append(a, 20, 2.0)
        self.assertEqual(db.query(a, 0, 100), [(10, 1.0), (20, 2.0)])
        self.assertEqual(db.query(b, 0, 100), [(10, 99.0)])


class TestDownsampling(TSDBTestCase):
    def setUp(self):
        super().setUp()
        self.db = self.make(chunk_max_records=1000)
        self.sid = self.db.create_series("cpu")
        # points at 0,10,20,...,90 with values 1..10
        for i in range(10):
            self.db.append(self.sid, i * 10, float(i + 1))

    def test_hand_computed_aggregates_per_bucket(self):
        # bucket [0,50): values 1,2,3,4,5 -> sum15 avg3 min1 max5 count5
        # bucket [50,100): values 6..10 -> sum40 avg8 min6 max10 count5
        expected = {
            "sum": [(0, 15.0), (50, 40.0)],
            "avg": [(0, 3.0), (50, 8.0)],
            "min": [(0, 1.0), (50, 6.0)],
            "max": [(0, 5.0), (50, 10.0)],
            "count": [(0, 5.0), (50, 5.0)],
        }
        for agg, want in expected.items():
            with self.subTest(aggregate=agg):
                self.assertEqual(
                    self.db.query(self.sid, 0, 100, step=50, aggregate=agg), want
                )

    def test_bucket_starts_are_epoch_aligned(self):
        # window [0,100), step 40 -> buckets 0, 40, 80 (floor to 40-grid)
        rows = self.db.query_buckets(self.sid, 0, 100, step=40, aggregate="sum")
        starts = [b for b, _v, _n in rows]
        self.assertEqual(starts, [0, 40, 80])
        self.assertTrue(all(s % 40 == 0 for s in starts))
        # hand check: bucket [40,80) holds 40,50,60,70 -> 5+6+7+8 = 26
        self.assertEqual(rows[1], (40, 26.0, 4))

    def test_empty_buckets_are_omitted(self):
        # gap: no points in [200, 400)
        db = self.make()
        sid = db.create_series("cpu")
        db.append(sid, 10, 1.0)
        db.append(sid, 500, 2.0)
        rows = db.query(sid, 0, 600, step=100, aggregate="avg")
        self.assertEqual([b for b, _v in rows], [0, 500])

    def test_raw_vs_step_output_and_counts(self):
        raw = self.db.query_buckets(self.sid, 0, 100)
        self.assertEqual(len(raw), 10)
        self.assertTrue(all(n == 1 for _b, _v, n in raw))
        bucketed = self.db.query_buckets(self.sid, 0, 100, step=25, aggregate="avg")
        self.assertEqual(
            bucketed,
            [(0, 2.0, 3), (25, 4.5, 2), (50, 7.0, 3), (75, 9.5, 2)],
        )

    def test_query_window_restricts_buckets(self):
        rows = self.db.query(self.sid, 30, 70, step=20, aggregate="sum")
        # window [30,70) keeps points 30,40,50,60 (values 4,5,6,7);
        # they fall in epoch buckets 20, 40, 40, 60
        self.assertEqual(rows, [(20, 4.0), (40, 11.0), (60, 7.0)])

    def test_bad_aggregate_and_step_rejected(self):
        with self.assertRaises(BadQuery):
            self.db.query(self.sid, 0, 10, step=10, aggregate="median")
        with self.assertRaises(BadQuery):
            self.db.query(self.sid, 0, 10, step=0)
        with self.assertRaises(BadQuery):
            self.db.query(self.sid, 0, 10, step=-5)
        with self.assertRaises(BadQuery):
            self.db.query(self.sid, 10, 5)  # end < start


class TestChunksAndPersistence(TSDBTestCase):
    def test_chunks_bounded_with_min_max_index(self):
        db = self.make(chunk_max_records=3)
        sid = db.create_series("cpu")
        for ts in [500, 100, 300, 400, 200, 600]:
            db.append(sid, ts, float(ts))
        stats = db.stats()
        self.assertEqual(stats["records"], 6)
        self.assertEqual(stats["chunks"], 2)  # 3 + 3
        files = stats["chunk_files"]
        self.assertEqual([c["records"] for c in files], [3, 3])
        # on-disk min/max index reflects out-of-order appends per chunk
        self.assertEqual((files[0]["min_ts"], files[0]["max_ts"]), (100, 500))
        self.assertEqual((files[1]["min_ts"], files[1]["max_ts"]), (200, 600))
        self.assertEqual(stats["header_bytes"], HEADER_SIZE)
        self.assertEqual(stats["record_bytes"], RECORD_SIZE)
        # every chunk respects the bound
        for c in files:
            self.assertLessEqual(c["records"], 3)
        self.assertEqual(
            db.query(sid, 0, 1000),
            [(100, 100.0), (200, 200.0), (300, 300.0),
             (400, 400.0), (500, 500.0), (600, 600.0)],
        )

    def test_binary_header_format_matches_documented_layout(self):
        db = self.make(chunk_max_records=2)
        sid = db.create_series("cpu")
        db.append(sid, 123456, 2.5)
        db.close()
        path = next((self.dir / "chunks").glob("chunk-*.bin"))
        blob = path.read_bytes()
        magic, version, count, min_ts, max_ts = struct.unpack_from("<4sI Q q q", blob, 0)
        self.assertEqual(magic, b"TSC1")
        self.assertEqual(version, 1)
        self.assertEqual(count, 1)
        self.assertEqual((min_ts, max_ts), (123456, 123456))
        series, ts, value, seq = struct.unpack_from("<I q d I", blob, HEADER_SIZE)
        self.assertEqual(series, sid)
        self.assertEqual(ts, 123456)
        self.assertEqual(value, 2.5)
        self.assertGreaterEqual(seq, 1)
        self.assertEqual(len(blob), HEADER_SIZE + RECORD_SIZE)

    def test_reopen_resumes_active_chunk_and_keeps_data(self):
        db = self.make(chunk_max_records=4)
        sid = db.create_series("cpu")
        for ts in [10, 20, 30]:
            db.append(sid, ts, float(ts))
        db.close()
        db2 = self.make(chunk_max_records=4)
        self.assertEqual(db2.query(sid, 0, 100), [(10, 10.0), (20, 20.0), (30, 30.0)])
        db2.append(sid, 40, 40.0)  # continues the same chunk (3 < 4)
        db2.append(sid, 50, 50.0)  # rolls over
        stats = db2.stats()
        self.assertEqual(stats["records"], 5)
        self.assertEqual(stats["chunks"], 2)
        self.assertEqual(db2.query(sid, 0, 100)[-1], (50, 50.0))

    def test_invalid_arguments_rejected(self):
        with self.assertRaises(ValueError):
            TSDB(self.dir, chunk_max_records=0)
        with self.assertRaises(ValueError):
            TSDB(self.dir, retention_ms=0)
        db = self.make()
        sid = db.create_series("cpu")
        with self.assertRaises(TSDBError):
            db.append(sid, 1.5, 1.0)  # ts must be int
        with self.assertRaises(TSDBError):
            db.append(sid, 1, "high")  # value must be numeric


class TestRetention(TSDBTestCase):
    def test_prune_requires_policy_and_now(self):
        db = self.make(chunk_max_records=2)
        with self.assertRaises(TSDBError):
            db.prune(now=1000)
        sid = db.create_series("cpu")
        db.append(sid, 10, 1.0)
        with self.assertRaises(TSDBError):
            db.prune()  # policy set but no explicit now
        db.set_retention(100)
        with self.assertRaises(ValueError):
            db.set_retention(0)

    def test_prune_deletes_only_fully_expired_chunks(self):
        db = self.make(chunk_max_records=2, retention_ms=1000)
        sid = db.create_series("cpu")
        # chunk1: ts 100,200  chunk2: ts 900,1100  chunk3(active): 1500,1600
        for ts in [100, 200, 900, 1100, 1500, 1600]:
            db.append(sid, ts, float(ts))
        # now=2500 -> cutoff=1500; chunk1 max=200 < 1500 (expired),
        # chunk2 max=1100 < 1500 (expired), chunk3 active (never touched)
        deleted = db.prune(now=2500)
        self.assertEqual(deleted, ["chunk-000001.bin", "chunk-000002.bin"])
        self.assertEqual(db.stats()["chunks"], 1)
        # only the active chunk's points remain queryable
        self.assertEqual(db.query(sid, 0, 10000), [(1500, 1500.0), (1600, 1600.0)])

    def test_chunk_straddling_cutoff_is_kept_whole(self):
        db = self.make(chunk_max_records=2, retention_ms=1000)
        sid = db.create_series("cpu")
        for ts in [100, 1900, 5000, 5100]:  # chunk1: 100,1900 straddles
            db.append(sid, ts, float(ts))
        db.close()
        # reopen so chunk1 is sealed (chunk2 becomes active)
        db2 = self.make(chunk_max_records=2, retention_ms=1000)
        # now=2500 -> cutoff=1500: chunk1 max=1900 >= 1500 → kept
        self.assertEqual(db2.prune(now=2500), [])
        self.assertEqual(
            db2.query(sid, 0, 10000), [(100, 100.0), (1900, 1900.0),
                                       (5000, 5000.0), (5100, 5100.0)]
        )
        # boundary: max_ts == cutoff is NOT pruned (strictly older only)
        db2.close()  # release handles before a fresh instance prunes
        db3 = self.make(chunk_max_records=2, retention_ms=1000)
        self.assertEqual(db3.prune(now=2900), [])  # cutoff 1900 == max
        # now beyond it → pruned
        self.assertEqual(db3.prune(now=2901), ["chunk-000001.bin"])

    def test_prune_survives_reopen(self):
        db = self.make(chunk_max_records=2, retention_ms=500)
        sid = db.create_series("cpu")
        for ts in [10, 20, 700, 800, 900]:
            db.append(sid, ts, float(ts))
        deleted = db.prune(now=1000)  # cutoff 500: chunk1 max=20 expired
        self.assertEqual(deleted, ["chunk-000001.bin"])
        db.close()
        db2 = self.make(chunk_max_records=2, retention_ms=500)
        self.assertEqual(db2.stats()["chunks"], 2)
        self.assertEqual(
            db2.query(sid, 0, 1000), [(700, 700.0), (800, 800.0), (900, 900.0)]
        )


if __name__ == "__main__":
    unittest.main()
