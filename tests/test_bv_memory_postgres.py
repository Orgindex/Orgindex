"""Oversize-only PostgreSQL transport and atomic transaction regression checks."""
import base64
import gzip
from io import BytesIO
import json
import unittest
from unittest.mock import MagicMock, Mock, patch

import psycopg
from psycopg.rows import dict_row

from scripts.bv_memory import (
    COMMIT_LOCK, COMMIT_SQL, Database, ImportFailure, PREPARE_COMPANIES_SQL,
    import_one,
)


class OversizeTransportRoutingTests(unittest.TestCase):
    job = {
        "source_url": "https://vardefulla-datamangder.bolagsverket.se/"
                      "arsredovisningar-bulkfiler/arsredovisningar/2025/19_1.zip",
        "attempts": 8,
    }

    def setUp(self):
        self.original_text = 'Årsredovisning 😀 "quoted" \\path\n' * 2000
        self.parsed = {
            "rows": [{
                "orgnr": "5594078932", "company_name": "Exempel AB",
                "period_start": "2024-01-01", "period_end": "2024-12-31",
                "reporting_scope": "company", "currency": "SEK",
                "net_revenue": "2334081", "net_profit": "130885",
                "source_document_id": "report.xhtml",
            }],
            "report_count": 2,
            "skipped": [{"archive_member": "report.zip", "document": "raw.xhtml",
                         "reason": "unsupported_facts", "content": self.original_text}],
            "conflicts": [{"field": "net_profit", "values": ["1", "2"]}],
            "ignored_attachments": [{"document": "cover.png", "declared_bytes": 123}],
        }
        self.success = [
            [{"companies_added": 0}],
            [{"imported_rows": 1, "unmatched": 0, "companies_added": 0}],
        ]

    def invoke(self, db):
        archive = BytesIO(b"fixture")
        with patch("scripts.bv_memory.claim", return_value=self.job), \
             patch("scripts.bv_memory.download", return_value=(archive, 7, "a" * 64)), \
             patch("scripts.bv_parse.parse_archive", return_value=self.parsed):
            try:
                return import_one(db)
            finally:
                self.assertTrue(archive.closed)

    def test_native_fallback_requires_both_local_size_rejections_and_preserves_payload(self):
        db = Mock()
        db.locked_transaction.side_effect = [
            ImportFailure("database_request_too_large"),
            ImportFailure("database_request_too_large"),
        ]
        db.postgres_locked_transaction.return_value = self.success

        result = self.invoke(db)

        self.assertEqual(result["state"], "completed")
        self.assertEqual(result["inserted_rows"], 1)
        self.assertEqual(db.locked_transaction.call_count, 2)
        db.postgres_locked_transaction.assert_called_once()
        queries, lock_key = db.postgres_locked_transaction.call_args.args
        self.assertEqual(lock_key, COMMIT_LOCK)
        self.assertEqual([query for query, _ in queries], [PREPARE_COMPANIES_SQL, COMMIT_SQL])
        preparation, commit = queries[0][1], queries[1][1]
        self.assertEqual(preparation[:2], [self.job["source_url"], self.job["attempts"]])
        self.assertEqual(commit[:2], preparation[:2])
        self.assertEqual(json.loads(commit[2]), self.parsed["rows"])
        self.assertEqual(json.loads(preparation[2]), [
            {"orgnr": "5594078932", "company_name": "Exempel AB"},
        ])
        self.assertEqual(commit[3:6], ["a" * 64, 7, 2])
        diagnostics = json.loads(commit[6])
        retained = diagnostics["skipped"][0]
        self.assertEqual(retained["content_encoding"], "gzip+base64")
        self.assertEqual(gzip.decompress(base64.b64decode(retained["content"])).decode(),
                         self.original_text)
        self.assertEqual(diagnostics["conflicts"], self.parsed["conflicts"])
        self.assertEqual(diagnostics["ignored_attachments"], self.parsed["ignored_attachments"])
        self.assertEqual(self.parsed["skipped"][0]["content"], self.original_text)
        self.assertNotIn("content_encoding", self.parsed["skipped"][0])
        db.sql.assert_not_called()  # Completion belongs to the atomic transaction.

    def test_small_and_successfully_compacted_archives_do_not_use_native_transport(self):
        for attempts in ([self.success], [ImportFailure("database_request_too_large"), self.success]):
            with self.subTest(attempts=len(attempts)):
                db = Mock()
                db.locked_transaction.side_effect = attempts
                self.assertEqual(self.invoke(db)["state"], "completed")
                db.postgres_locked_transaction.assert_not_called()

    def test_http_and_ambiguous_connection_failures_are_never_replayed_natively(self):
        for preceding_size_error in (False, True):
            for reason in ("database_connection_failed", "database_http_503_unknown",
                           "database_invalid_response"):
                with self.subTest(after_compression=preceding_size_error, reason=reason):
                    db = Mock()
                    failures = ([ImportFailure("database_request_too_large")]
                                if preceding_size_error else [])
                    db.locked_transaction.side_effect = failures + [ImportFailure(reason)]
                    with self.assertRaisesRegex(ImportFailure, "^" + reason + "$"):
                        self.invoke(db)
                    db.postgres_locked_transaction.assert_not_called()
                    self.assertEqual(db.sql.call_args.args[1],
                                     [self.job["source_url"], self.job["attempts"], reason])
                    self.assertIn("SET status='failed'", db.sql.call_args.args[0])
                    self.assertNotIn("SET status='completed'", db.sql.call_args.args[0])

    def test_native_failure_is_not_retried_or_reported_as_complete(self):
        db = Mock()
        db.locked_transaction.side_effect = [ImportFailure("database_request_too_large")] * 2
        db.postgres_locked_transaction.side_effect = ImportFailure("database_postgres_57014")
        with self.assertRaisesRegex(ImportFailure, "^database_postgres_57014$"):
            self.invoke(db)
        db.postgres_locked_transaction.assert_called_once()
        self.assertEqual(db.sql.call_args.args[1][2], "database_postgres_57014")
        self.assertIn("attempts=$2::int", db.sql.call_args.args[0])


class NativeTransactionTests(unittest.TestCase):
    dsn = "postgresql://fixture:fixture@fixture.neon.tech/fixture"

    def setUp(self):
        self.connection = MagicMock()
        self.connection.__enter__.return_value = self.connection
        self.connection.__exit__.return_value = False
        self.cursor = MagicMock()
        self.connection.cursor.return_value.__enter__.return_value = self.cursor
        self.connection.cursor.return_value.__exit__.return_value = False
        self.queries = [
            ("SELECT $1::text AS prepared_company", ["Åäö"]),
            ("SELECT $1::text AS completed_archive", ["original $1 %s \\ text"]),
        ]
        self.expected = [[{"prepared_company": "Åäö"}],
                         [{"completed_archive": "original $1 %s \\ text"}]]
        self.current_query = None

        def execute(query, *args, **kwargs):
            self.current_query = query

        def fetchall():
            for (query, _), rows in zip(self.queries, self.expected):
                if self.current_query == query:
                    return rows
            return []

        self.cursor.execute.side_effect = execute
        self.cursor.fetchall.side_effect = fetchall

    def test_secure_single_transaction_preserves_raw_placeholders_and_lock_order(self):
        with patch("psycopg.connect", return_value=self.connection) as connect:
            result = Database(self.dsn).postgres_locked_transaction(self.queries, COMMIT_LOCK)

        self.assertEqual(result, self.expected)
        connect.assert_called_once()
        self.assertEqual(connect.call_args.args[0], self.dsn)
        options = connect.call_args.kwargs
        self.assertEqual(options["cursor_factory"], psycopg.RawCursor)
        self.assertEqual(options["row_factory"], dict_row)
        self.assertIsNone(options["prepare_threshold"])
        self.assertEqual(options["connect_timeout"], 10)
        self.assertEqual(options["sslmode"], "verify-full")
        self.assertEqual(options["sslrootcert"], "system")
        self.assertEqual(self.connection.isolation_level, psycopg.IsolationLevel.READ_COMMITTED)
        self.connection.__enter__.assert_called_once()
        self.connection.__exit__.assert_called_once_with(None, None, None)

        calls = self.cursor.execute.call_args_list
        sqls = [call.args[0] for call in calls]
        lock_index = next(i for i, sql in enumerate(sqls) if "pg_advisory_xact_lock" in sql)
        self.assertEqual(list(calls[lock_index].args[1]), [COMMIT_LOCK])
        self.assertEqual(sqls[lock_index + 1:], [query for query, _ in self.queries])
        for call, (_, params) in zip(calls[lock_index + 1:], self.queries):
            self.assertEqual(list(call.args[1]), params)
        setup = "\n".join(sqls[:lock_index])
        for setting in ("lock_timeout", "statement_timeout", "idle_in_transaction_session_timeout"):
            self.assertIn(setting, setup)
        self.assertIn("true", setup.lower())  # set_config settings are transaction-local.

    def test_statement_failure_reaches_connection_context_for_atomic_rollback(self):
        failure = psycopg.errors.UniqueViolation("private account text and password=secret")

        def execute(query, *args, **kwargs):
            self.current_query = query
            if query == self.queries[-1][0]:
                raise failure

        self.cursor.execute.side_effect = execute
        with patch("psycopg.connect", return_value=self.connection):
            with self.assertRaisesRegex(ImportFailure, "^database_postgres_23505$") as caught:
                Database(self.dsn).postgres_locked_transaction(self.queries, COMMIT_LOCK)

        self.assertNotIn("secret", str(caught.exception))
        self.assertNotIn("private account", str(caught.exception))
        self.connection.__exit__.assert_called_once()
        self.assertIs(self.connection.__exit__.call_args.args[1], failure)
        self.connection.commit.assert_not_called()

    def test_connection_failure_reports_no_dsn_or_exception_text(self):
        failure = psycopg.OperationalError("cannot reach " + self.dsn + " password=secret")
        with patch("psycopg.connect", side_effect=failure):
            with self.assertRaisesRegex(ImportFailure, "^database_postgres_connection_failed$") as caught:
                Database(self.dsn).postgres_locked_transaction(self.queries, COMMIT_LOCK)
        self.assertNotIn("fixture", str(caught.exception))
        self.assertNotIn("secret", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
