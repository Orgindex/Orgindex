"""Opt-in integration checks. Supply BV_IMPORT_TEST_DSN for an isolated branch only."""
from concurrent.futures import ThreadPoolExecutor
import os
import threading
import unittest

from scripts.bv_memory import (
    CLAIM_LOCK, COMMIT_LOCK, COMMIT_SQL, Database, MAX_CONCURRENT_IMPORTS,
    PARSER_VERSION, PREPARE_COMPANIES_SQL, claim,
)


@unittest.skipUnless(os.environ.get("BV_IMPORT_TEST_DSN"), "isolated Neon branch required")
class ParallelImportIntegrationTests(unittest.TestCase):
    def test_parallel_claim_limit_and_stale_completion(self):
        db = Database(os.environ["BV_IMPORT_TEST_DSN"])
        originals = {row["source_url"]: row for row in db.sql(
            "SELECT * FROM public.orgindex_bolagsverket_file_queue")}
        self.assertFalse(any(row["status"] == "running" for row in originals.values()),
                         "test branch must have no active importer")
        gate = threading.Barrier(8)
        jobs = []

        def start():
            gate.wait()
            return claim(db)

        try:
            with ThreadPoolExecutor(max_workers=8) as pool:
                jobs = [job for job in pool.map(lambda _: start(), range(8)) if job]
            self.assertEqual(len(jobs), MAX_CONCURRENT_IMPORTS)
            self.assertEqual(len({job["source_url"] for job in jobs}), MAX_CONCURRENT_IMPORTS)
            self.assertEqual({job["source_url"].split("/")[-2] for job in jobs}, {"2026"})
            self.assertIsNone(claim(db))

            job = jobs[0]
            params = [job["source_url"], int(job["attempts"]) - 1, "[]"]
            added, result = db.locked_transaction([
                (PREPARE_COMPANIES_SQL, params),
                (COMMIT_SQL, params + ["0" * 64, 1, 0, "{}", PARSER_VERSION, 0, 0]),
            ], COMMIT_LOCK)
            self.assertEqual(int(added[0]["companies_added"]), 0)
            self.assertEqual(result, [])
            self.assertEqual(db.sql("SELECT status FROM public.orgindex_bolagsverket_file_queue "
                                    "WHERE source_url=$1", [job["source_url"]])[0]["status"], "running")
        finally:
            # Restore only the claims made by this test; never reset a newer owner.
            for job in jobs:
                old = originals[job["source_url"]]
                db.locked_transaction([("""UPDATE public.orgindex_bolagsverket_file_queue
                    SET status=$3,attempts=$4::int,lease_until=$5::timestamptz,
                        updated_at=$6::timestamptz,last_error=$7
                    WHERE source_url=$1 AND status='running' AND attempts=$2::int""",
                    [job["source_url"], job["attempts"], old["status"], old["attempts"],
                     old["lease_until"], old["updated_at"], old["last_error"]])], CLAIM_LOCK)


if __name__ == "__main__":
    unittest.main()
