"""Lossless transport compaction for bounded atomic archive imports."""
import base64
import gzip
import hashlib
from io import BytesIO
import json
import unittest
from unittest.mock import Mock, patch

from scripts.bv_memory import (
    COMMIT_LOCK, COMMIT_SQL, Database, ImportFailure, MAX_DATABASE_REQUEST_BYTES,
    PREPARE_COMPANIES_SQL, company_preparation_records, compress_diagnostic_text,
    import_one, json_text,
)


class CompactArchivePayloadTests(unittest.TestCase):
    dsn = "postgresql://fixture:fixture@fixture.neon.tech/fixture"
    job = {"source_url": "https://vardefulla-datamangder.bolagsverket.se/"
                        "arsredovisningar-bulkfiler/arsredovisningar/2025/19_1.zip",
           "attempts": 3}

    def run_import(self, parsed, limit=16384):
        response = {"results": [
            {"rows": []}, {"rows": []},
            {"rows": [{"companies_added": 0}]},
            {"rows": [{"imported_rows": len(parsed["rows"]), "unmatched": 0, "companies_added": 0}]},
        ]}
        opener = Mock()
        opener.open.return_value = BytesIO(json_text(response).encode("utf-8"))
        buffer = BytesIO(b"zip fixture")
        with patch("scripts.bv_memory.claim", return_value=self.job), \
             patch("scripts.bv_memory.download", return_value=(buffer, 11, "a" * 64)), \
             patch("scripts.bv_parse.parse_archive", return_value=parsed), \
             patch("scripts.bv_memory.MAX_DATABASE_REQUEST_BYTES", limit), \
             patch("scripts.bv_memory.urllib.request.build_opener", return_value=opener):
            result = import_one(Database(self.dsn))
        self.assertTrue(buffer.closed)
        self.assertEqual(result["state"], "completed")
        # The oversized first encoding is rejected locally: only the final
        # complete transaction is sent to Neon, with the original advisory lock.
        self.assertEqual(opener.open.call_count, 1)
        request = opener.open.call_args.args[0]
        self.assertLessEqual(len(request.data), limit)
        payload = json.loads(request.data)
        self.assertEqual(len(payload["queries"]), 4)
        self.assertEqual(payload["queries"][1]["params"], [COMMIT_LOCK])
        self.assertEqual(payload["queries"][2]["query"], PREPARE_COMPANIES_SQL)
        self.assertEqual(payload["queries"][3]["query"], COMMIT_SQL)
        return payload, result

    def test_preparation_deduplicates_pairs_without_losing_name_conflicts(self):
        rows = [
            {"orgnr": "5566733472", "company_name": "Samma AB", "net_profit": 1},
            {"orgnr": "5566733472", "company_name": "Samma AB", "net_profit": 2},
            {"orgnr": "5566733472", "company_name": "Annat AB", "net_profit": 3},
            {"orgnr": "5594860594", "company_name": None, "net_profit": 4},
            {"orgnr": "5594860594", "company_name": "", "net_profit": 5},
        ]
        expected = [
            {"orgnr": "5566733472", "company_name": "Samma AB"},
            {"orgnr": "5566733472", "company_name": "Annat AB"},
            {"orgnr": "5594860594", "company_name": None},
            {"orgnr": "5594860594", "company_name": ""},
        ]
        self.assertEqual(company_preparation_records(rows), expected)
        parsed = {"rows": rows, "report_count": 5,
                  "skipped": [], "conflicts": [], "ignored_attachments": []}
        payload, _ = self.run_import(parsed)
        self.assertEqual(json.loads(payload["queries"][2]["params"][2]), expected)
        self.assertEqual(json.loads(payload["queries"][3]["params"][2]), rows)

    def test_small_diagnostics_preserve_existing_shape(self):
        skipped = [{"archive_member": "a.zip", "document": "a.xhtml",
                    "reason": "unrecognised_facts", "content": "Årsredovisning"}]
        parsed = {"rows": [], "report_count": 1, "skipped": skipped,
                  "conflicts": [], "ignored_attachments": []}
        payload, _ = self.run_import(parsed)
        stored = json.loads(payload["queries"][3]["params"][6])
        self.assertEqual(stored["skipped"], skipped)
        self.assertNotIn("content_encoding", skipped[0])

    def test_aggregate_small_documents_round_trip_multibyte_and_escaping(self):
        text = ('Åäö Göteborg 😀 "quoted" \\path\n\r\t' * 500)
        original = text.encode("utf-8")
        self.assertLess(len(original), 1024 * 1024)  # Each escapes parser compression.
        skipped = [
            {"archive_member": str(i) + ".zip", "document": "report.xhtml",
             "reason": "unsupported_facts", "content": text, "extra": {"index": i}}
            for i in range(4)
        ]
        conflicts = [{"field": "net_profit", "values": ["1", "2"]}]
        attachments = [{"document": "cover.png", "declared_bytes": 100}]
        parsed = {"rows": [], "report_count": 4, "skipped": skipped,
                  "conflicts": conflicts, "ignored_attachments": attachments}
        self.assertGreater(len(json_text(parsed).encode("utf-8")), 16384)
        payload, result = self.run_import(parsed)
        stored = json.loads(payload["queries"][3]["params"][6])
        self.assertEqual(len(stored["skipped"]), len(skipped))
        self.assertEqual(stored["conflicts"], conflicts)
        self.assertEqual(stored["ignored_attachments"], attachments)
        self.assertEqual(result["skipped"], 4)
        self.assertEqual(result["conflicts"], 1)
        for old, packed in zip(skipped, stored["skipped"]):
            self.assertEqual(gzip.decompress(base64.b64decode(packed["content"])), original)
            self.assertEqual(packed["content_encoding"], "gzip+base64")
            self.assertEqual(packed["content_bytes"], len(original))
            self.assertEqual(packed["content_sha256"], hashlib.sha256(original).hexdigest())
            for key in ("archive_member", "document", "reason", "extra"):
                self.assertEqual(packed[key], old[key])
            self.assertEqual(old["content"], text)  # Never mutate original diagnostics.
            self.assertNotIn("content_encoding", old)

    def test_existing_compressed_and_nontext_entries_are_preserved(self):
        original = b"\xfforiginal source bytes"
        entry = {"reason": "invalid_xml",
                 "content": base64.b64encode(gzip.compress(original, mtime=0)).decode("ascii"),
                 "content_encoding": "gzip+base64",
                 "content_bytes": len(original),
                 "content_sha256": hashlib.sha256(original).hexdigest()}
        none = {"reason": "no_supported_document", "content": None}
        opaque = {"reason": "custom", "content": "x" * 10000, "content_encoding": "opaque"}
        diagnostic = {"skipped": [entry, none, opaque], "conflicts": [{"x": 1}]}
        self.assertEqual(compress_diagnostic_text(diagnostic), diagnostic)

    def test_unknown_database_error_is_not_retried_locally(self):
        db = Mock()
        db.locked_transaction.side_effect = ImportFailure("database_connection_failed")
        parsed = {"rows": [], "report_count": 1,
                  "skipped": [{"content": "x" * 32768}], "conflicts": []}
        with patch("scripts.bv_memory.claim", return_value=self.job), \
             patch("scripts.bv_memory.download", return_value=(BytesIO(b"x"), 1, "a" * 64)), \
             patch("scripts.bv_parse.parse_archive", return_value=parsed):
            with self.assertRaisesRegex(ImportFailure, "database_connection_failed"):
                import_one(db)
        self.assertEqual(db.locked_transaction.call_count, 1)
        self.assertEqual(db.sql.call_args.args[1][2], "database_connection_failed")

    def test_production_request_limit_is_unchanged(self):
        self.assertEqual(MAX_DATABASE_REQUEST_BYTES, 60_000_000)


if __name__ == "__main__":
    unittest.main()

