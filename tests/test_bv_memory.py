"""Opt-in integration checks. Supply BV_IMPORT_TEST_DSN for an isolated branch only."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
from io import BytesIO
import os
import threading
import unittest
from unittest.mock import Mock, patch

from scripts.bv_memory import (
    CLAIM_LOCK, COMMIT_LOCK, COMMIT_SQL, Database, MAX_CONCURRENT_IMPORTS,
    ImportFailure, PARSER_VERSION, PREPARE_COMPANIES_SQL, claim, download, import_one,
)
from scripts.bv_parse import ParseError


class DownloadResponse(BytesIO):
    def __init__(self, body, headers, status=200, incomplete=False):
        super().__init__(body)
        self.headers = headers
        self.status = status
        self.incomplete = incomplete

    def read(self, size=-1):
        if self.incomplete:
            self.incomplete = False
            raise http.client.IncompleteRead(super().read(size))
        return super().read(size)


class ResumableDownloadTests(unittest.TestCase):
    url = ("https://vardefulla-datamangder.bolagsverket.se/"
           "arsredovisningar-bulkfiler/arsredovisningar/2026/22_2.zip")

    def run_download(self, responses):
        opener = Mock()
        opener.open.side_effect = responses
        with patch("scripts.bv_memory.urllib.request.build_opener", return_value=opener):
            stream, size, digest = download(self.url)
        with stream:
            body = stream.read()
        self.assertEqual(size, len(body))
        self.assertEqual(digest, hashlib.sha256(body).hexdigest())
        return body, opener.open.call_args_list

    def test_truncated_body_resumes_with_same_validator_and_exact_range(self):
        responses = [
            DownloadResponse(b"abc", {"Content-Length": "6", "ETag": '"v1"'}),
            DownloadResponse(b"def", {"Content-Length": "3", "ETag": '"v1"',
                                      "Content-Range": "bytes 3-5/6"}, 206),
        ]
        body, calls = self.run_download(responses)
        self.assertEqual(body, b"abcdef")
        self.assertEqual(calls[1].args[0].get_header("Range"), "bytes=3-")
        self.assertEqual(calls[1].args[0].get_header("If-range"), '"v1"')

    def test_incomplete_read_preserves_the_received_prefix(self):
        body, _ = self.run_download([
            DownloadResponse(b"abc", {"Content-Length": "6", "ETag": '"v1"'}, incomplete=True),
            DownloadResponse(b"def", {"Content-Length": "3", "ETag": '"v1"',
                                      "Content-Range": "bytes 3-5/6"}, 206),
        ])
        self.assertEqual(body, b"abcdef")

    def test_complete_200_response_restarts_instead_of_appending(self):
        body, _ = self.run_download([
            DownloadResponse(b"abc", {"Content-Length": "6", "ETag": '"v1"'}),
            DownloadResponse(b"new", {"Content-Length": "3", "ETag": '"v2"'}),
        ])
        self.assertEqual(body, b"new")

    def test_wrong_range_or_changed_version_is_never_imported(self):
        for headers, expected in [
            ({"Content-Length": "3", "ETag": '"v1"', "Content-Range": "bytes 2-4/6"},
             "invalid_source_range"),
            ({"Content-Length": "3", "ETag": '"v2"', "Content-Range": "bytes 3-5/6"},
             "source_changed_during_download"),
        ]:
            with self.subTest(expected=expected), self.assertRaisesRegex(ImportFailure, expected):
                self.run_download([
                    DownloadResponse(b"abc", {"Content-Length": "6", "ETag": '"v1"'}),
                    DownloadResponse(b"def", headers, 206),
                ])

    def test_retry_count_is_bounded_when_source_keeps_truncating(self):
        opener = Mock()
        opener.open.side_effect = [DownloadResponse(b"ab", {"Content-Length": "6"}) for _ in range(4)]
        with patch("scripts.bv_memory.urllib.request.build_opener", return_value=opener):
            with self.assertRaisesRegex(ImportFailure, "incomplete_download"):
                download(self.url)
        self.assertEqual(opener.open.call_count, 4)
        self.assertTrue(all(call.args[0].get_header("Range") is None for call in opener.open.call_args_list))

    def test_total_download_deadline_is_preserved(self):
        with patch("scripts.bv_memory.time.monotonic", side_effect=[0, 151]):
            with self.assertRaisesRegex(ImportFailure, "download_deadline"):
                download(self.url)


class ImportFailureDiagnosticsTests(unittest.TestCase):
    def fail_parse(self, error):
        db = Mock()
        job = {"source_url": "https://vardefulla-datamangder.bolagsverket.se/"
                             "arsredovisningar-bulkfiler/arsredovisningar/2026/19_1.zip",
               "attempts": 10}
        buffer = BytesIO(b"zip fixture")
        with patch("scripts.bv_memory.claim", return_value=job), \
             patch("scripts.bv_memory.download", return_value=(buffer, 11, "a" * 64)), \
             patch("scripts.bv_parse.parse_archive", side_effect=error):
            with self.assertRaises(ImportFailure) as caught:
                import_one(db)
        self.assertTrue(buffer.closed)
        self.assertEqual(db.sql.call_args.args[1][:2], [job["source_url"], 10])
        self.assertNotIn("completed", db.sql.call_args.args[0])
        self.assertIn("attempts=$2", db.sql.call_args.args[0])
        return str(caught.exception), db.sql.call_args.args[1][2]

    def test_parser_failure_reason_is_preserved(self):
        message, saved = self.fail_parse(ParseError("document_too_large"))
        self.assertEqual(message, "document_too_large")
        self.assertEqual(saved, message)

    def test_unexpected_exception_cannot_leak_sensitive_details(self):
        message, saved = self.fail_parse(ValueError("private database connection string"))
        self.assertEqual(message, "ValueError")
        self.assertEqual(saved, message)


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
