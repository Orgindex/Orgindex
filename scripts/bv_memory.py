"""Resume the verified Bolagsverket manifests without writing ZIP files.

DATABASE_URL (or BV_NEON_DSN) must be supplied by the runtime, never committed.
All account inserts, extracted records and the completion marker commit together.
"""
from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import re
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
YEARS = (2026, 2025)
ORIGIN = "https://vardefulla-datamangder.bolagsverket.se"
URL_PATTERN = re.compile(re.escape(ORIGIN) + r"/arsredovisningar-bulkfiler/arsredovisningar/(202[0-6])/(\d{2})_(\d+)\.zip\Z")
MAX_BYTES = 256 * 1024 * 1024
MAX_DOWNLOAD_ATTEMPTS = 4
DOWNLOAD_DEADLINE_SECONDS = 150
MAX_CONCURRENT_IMPORTS = 3
# Keep the complete UTF-8 JSON body below Neon's 64 MB HTTP request limit.
MAX_DATABASE_REQUEST_BYTES = 60_000_000
CLAIM_LOCK = 18472640
COMMIT_LOCK = 18472641
PARSER_VERSION = "memory-2026-11-28-v5"


class ImportFailure(RuntimeError):
    pass


def json_text(value):
    return json.dumps(value, default=str, ensure_ascii=False, separators=(",", ":"))


def company_preparation_records(rows):
    # Keep distinct names for an organisation: SQL must still reject ambiguous
    # names instead of selecting whichever account happened to appear first.
    seen = set()
    companies = []
    for row in rows:
        identity = (row["orgnr"], row.get("company_name"))
        if identity not in seen:
            seen.add(identity)
            companies.append(dict(orgnr=identity[0], company_name=identity[1]))
    return companies


def compress_diagnostic_text(diagnostics):
    # The parser already compresses individual large documents. A bulk archive
    # can also contain many smaller exceptions whose aggregate exceeds the HTTP
    # limit. Preserve all their text and metadata in the same reversible format.
    skipped = []
    for entry in diagnostics.get("skipped", []):
        content = entry.get("content")
        if isinstance(content, str) and not entry.get("content_encoding"):
            original = content.encode("utf-8")
            compressed = io.BytesIO()
            with gzip.GzipFile(fileobj=compressed, mode="wb", filename="",
                               compresslevel=6, mtime=0) as stream:
                stream.write(original)
            packed = dict(entry, content=base64.b64encode(compressed.getvalue()).decode("ascii"),
                          content_encoding="gzip+base64", content_bytes=len(original),
                          content_sha256=hashlib.sha256(original).hexdigest())
            # Do not enlarge tiny or incompressible diagnostic entries.
            if len(json_text(packed).encode("utf-8")) < len(json_text(entry).encode("utf-8")):
                entry = packed
        skipped.append(entry)
    return dict(diagnostics, skipped=skipped)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ImportFailure("redirect_rejected")


class Database:
    def __init__(self, dsn=None):
        self.dsn = dsn or os.environ.get("BV_NEON_DSN") or os.environ.get("DATABASE_URL")
        if not self.dsn:
            raise ImportFailure("missing_database_configuration")
        host = urllib.parse.urlsplit(self.dsn).hostname
        if not host or not host.endswith(".neon.tech"):
            raise ImportFailure("invalid_database_host")
        self.endpoint = "https://" + host + "/sql"

    def _request(self, payload, extra_headers=None):
        body = json_text(payload).encode("utf-8")
        if len(body) > MAX_DATABASE_REQUEST_BYTES:
            # A handled exception keeps its traceback frame alive. Do not retain
            # this oversized encoding while compacting or using native Postgres.
            del body
            raise ImportFailure("database_request_too_large")
        request = urllib.request.Request(
            self.endpoint,
            data=body,
            headers={"Content-Type": "application/json", "Neon-Connection-String": self.dsn,
                     **(extra_headers or {})},
            method="POST",
        )
        try:
            with urllib.request.build_opener(NoRedirect()).open(request, timeout=50) as response:
                result = json.load(response)
        except urllib.error.HTTPError as exc:
            try:
                code = json.loads(exc.read()).get("code", "unknown")
            except (ValueError, AttributeError):
                code = "unknown"
            if not re.fullmatch(r"[A-Za-z0-9_]{1,50}", str(code)):
                code = "unknown"
            raise ImportFailure(f"database_http_{exc.code}_{code}") from None
        except (urllib.error.URLError, TimeoutError):
            raise ImportFailure("database_connection_failed") from None
        return result

    def sql(self, query, params=()):
        result = self._request({"query": query, "params": list(params)})
        if "rows" not in result:
            raise ImportFailure("database_invalid_response")
        return result["rows"]

    def locked_transaction(self, queries, lock_key):
        # A separate statement AFTER the lock gets a fresh READ COMMITTED snapshot.
        # A lock inside the work statement's CTE would leave a stale snapshot.
        batch = [
            {"query": "SELECT set_config('lock_timeout','10s',true)", "params": []},
            {"query": "SELECT pg_advisory_xact_lock($1::bigint)", "params": [lock_key]},
        ] + [{"query": query, "params": list(params)} for query, params in queries]
        result = self._request({"queries": batch}, {"Neon-Batch-Isolation-Level": "ReadCommitted"})
        results = result.get("results", [])
        if len(results) != len(batch) or any("rows" not in row for row in results):
            raise ImportFailure("database_invalid_response")
        return [row["rows"] for row in results[2:]]

    def postgres_locked_transaction(self, queries, lock_key):
        """Send oversized imports as separate statements in one SQL transaction.

        Use only after the HTTP byte guard rejects a request locally. No HTTP
        request has then been sent, so there is no ambiguous commit to repeat.
        RawCursor preserves the existing $1 placeholders and parameter binding.
        The connection context commits all statements together, or rolls back
        everything on an error, and always closes the connection.
        """
        import psycopg
        from psycopg.rows import dict_row

        try:
            with psycopg.connect(
                self.dsn, cursor_factory=psycopg.RawCursor, row_factory=dict_row,
                connect_timeout=10, prepare_threshold=None,
                sslmode="verify-full", sslrootcert="system",
                application_name="orgindex-bv-import",
            ) as connection:
                connection.isolation_level = psycopg.IsolationLevel.READ_COMMITTED
                with connection.cursor() as cursor:
                    cursor.execute("SELECT set_config('lock_timeout','10s',true)")
                    cursor.execute("SELECT set_config('statement_timeout','50s',true)")
                    cursor.execute("SELECT set_config('idle_in_transaction_session_timeout','60s',true)")
                    cursor.execute("SELECT pg_advisory_xact_lock($1::bigint)", [lock_key])
                    results = []
                    # Each work statement starts after the advisory lock, with
                    # a fresh READ COMMITTED snapshot, as on the HTTP path.
                    for query, params in queries:
                        cursor.execute(query, list(params))
                        results.append(cursor.fetchall())
                return results
        except psycopg.Error as exc:
            # Database messages can contain credentials or entire source rows.
            # Log only a bounded SQLSTATE, never the connection/error text.
            code = getattr(exc, "sqlstate", None)
            if not isinstance(code, str) or not re.fullmatch(r"[A-Z0-9]{5}", code):
                raise ImportFailure("database_postgres_connection_failed") from None
            raise ImportFailure("database_postgres_" + code) from None


def manifests():
    urls = []
    for year in YEARS:
        current = (ROOT / "data" / "bv-manifests" / f"{year}.urls").read_text().splitlines()
        if not current or any(not URL_PATTERN.fullmatch(url) or f"/{year}/" not in url for url in current):
            raise ImportFailure("invalid_manifest")
        urls.extend(current)
    if len(set(urls)) != len(urls):
        raise ImportFailure("duplicate_manifest_url")
    return urls


def seed_queue(db):
    return db.sql("""WITH added AS (
        INSERT INTO public.orgindex_bolagsverket_file_queue(source_url)
        SELECT value FROM jsonb_array_elements_text($1::jsonb)
        ON CONFLICT(source_url) DO NOTHING RETURNING 1
    ) SELECT count(*)::int AS added FROM added""", [json_text(manifests())])[0]


CLAIM_SQL = """WITH manifest AS MATERIALIZED (
    SELECT value AS source_url FROM jsonb_array_elements_text($1::jsonb)
), current_year AS (
    SELECT max(substring(source_url from '/arsredovisningar/([0-9]{4})/')) AS year
    FROM public.orgindex_bolagsverket_file_queue
    WHERE source_url IN (SELECT source_url FROM manifest)
      AND ((status IN ('pending','failed') AND next_attempt_at <= now())
           OR (status='running' AND lease_until < now()))
), candidate AS (
    SELECT q.source_url FROM public.orgindex_bolagsverket_file_queue q, current_year y
    WHERE q.source_url IN (SELECT source_url FROM manifest)
      AND substring(q.source_url from '/arsredovisningar/([0-9]{4})/') = y.year
      AND ((q.status IN ('pending','failed') AND q.next_attempt_at <= now())
           OR (q.status='running' AND q.lease_until < now()))
      AND (SELECT count(*) FROM public.orgindex_bolagsverket_file_queue live
           WHERE live.status='running' AND live.lease_until >= now()
             AND live.source_url IN (SELECT source_url FROM manifest)) < $2::int
    ORDER BY substring(q.source_url from '/([0-9]{2})_[0-9]+[.]zip$')::int,
             substring(q.source_url from '/[0-9]{2}_([0-9]+)[.]zip$')::int
    FOR UPDATE OF q SKIP LOCKED LIMIT 1
) UPDATE public.orgindex_bolagsverket_file_queue q
  SET status='running', attempts=q.attempts+1, lease_until=now()+interval '5 minutes',
      updated_at=now(), last_error=NULL
  FROM candidate c WHERE q.source_url=c.source_url
  RETURNING q.source_url,q.attempts
"""


def claim(db):
    rows = db.locked_transaction(
        [(CLAIM_SQL, [json_text(manifests()), MAX_CONCURRENT_IMPORTS])], CLAIM_LOCK)[0]
    return rows[0] if rows else None


def download(url):
    if not URL_PATTERN.fullmatch(url):
        raise ImportFailure("invalid_source_url")
    buffer = io.BytesIO()
    digest = hashlib.sha256()
    started = time.monotonic()
    expected_size = None
    validator = None
    validator_header = None
    last_error = "incomplete_download"
    try:
        for _ in range(MAX_DOWNLOAD_ATTEMPTS):
            remaining = DOWNLOAD_DEADLINE_SECONDS - (time.monotonic() - started)
            if remaining <= 0:
                raise ImportFailure("download_deadline")
            offset = buffer.tell()
            headers = {"User-Agent": "Orgindex-Bolagsverket-Importer/1.0",
                       "Accept-Encoding": "identity"}
            resume = bool(offset and expected_size is not None and validator)
            if resume:
                headers.update({"Range": f"bytes={offset}-", "If-Range": validator})
            elif offset:
                # Never combine bytes without a validator for the same source.
                buffer.seek(0)
                buffer.truncate()
                digest = hashlib.sha256()
                offset = 0
            request = urllib.request.Request(url, headers=headers)
            try:
                with urllib.request.build_opener(NoRedirect()).open(
                        request, timeout=min(30, remaining)) as response:
                    if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                        raise ImportFailure("unsupported_source_encoding")
                    length = response.headers.get("Content-Length")
                    if length is not None and not re.fullmatch(r"[0-9]+", length):
                        raise ImportFailure("invalid_source_length")
                    length = int(length) if length is not None else None
                    if response.status == 206 and resume:
                        match = re.fullmatch(r"bytes ([0-9]+)-([0-9]+)/([0-9]+)",
                                             response.headers.get("Content-Range", ""))
                        if not match:
                            raise ImportFailure("invalid_source_range")
                        first, last, total = map(int, match.groups())
                        if (first != offset or last < first or last >= total
                                or total != expected_size
                                or (length is not None and length != last - first + 1)):
                            raise ImportFailure("invalid_source_range")
                        if response.headers.get(validator_header) != validator:
                            raise ImportFailure("source_changed_during_download")
                    elif response.status == 200:
                        # If-Range may legitimately yield a new complete version.
                        # Discard the previous prefix before reading that response.
                        if offset:
                            buffer.seek(0)
                            buffer.truncate()
                            digest = hashlib.sha256()
                        expected_size = length
                        etag = response.headers.get("ETag")
                        validator_header = "ETag" if etag and not etag.startswith("W/") else "Last-Modified"
                        validator = response.headers.get(validator_header)
                    else:
                        raise ImportFailure("unexpected_source_status")
                    if expected_size is not None and expected_size > MAX_BYTES:
                        raise ImportFailure("archive_too_large")
                    interrupted = False
                    while True:
                        if time.monotonic() - started > DOWNLOAD_DEADLINE_SECONDS:
                            raise ImportFailure("download_deadline")
                        try:
                            part = response.read(1024 * 1024)
                        except http.client.IncompleteRead as exc:
                            part = exc.partial
                            interrupted = True
                        if part:
                            if buffer.tell() + len(part) > MAX_BYTES:
                                raise ImportFailure("archive_too_large")
                            if expected_size is not None and buffer.tell() + len(part) > expected_size:
                                raise ImportFailure("invalid_source_length")
                            buffer.write(part)
                            digest.update(part)
                        if not part or interrupted:
                            break
                    if (expected_size is not None and buffer.tell() == expected_size
                            or expected_size is None and not interrupted):
                        size = buffer.tell()
                        buffer.seek(0)
                        return buffer, size, digest.hexdigest()
                    last_error = "incomplete_download"
            except urllib.error.HTTPError as exc:
                last_error = f"source_http_{exc.code}"
                if exc.code not in (408, 429, 500, 502, 503, 504):
                    raise ImportFailure(last_error) from None
            except (urllib.error.URLError, OSError, http.client.HTTPException):
                last_error = "source_connection_failed"
        raise ImportFailure(last_error)
    except BaseException:
        buffer.close()
        raise


PREPARE_COMPANIES_SQL = """WITH owned AS MATERIALIZED (
    SELECT source_url FROM public.orgindex_bolagsverket_file_queue
    WHERE source_url=$1 AND status='running' AND attempts=$2::int FOR UPDATE
), staging AS (
    SELECT * FROM jsonb_to_recordset($3::jsonb) AS r(orgnr text, company_name text)
), added AS (
    INSERT INTO public.orgindex_companies(orgnr,name,source)
    SELECT orgnr,min(company_name),'Bolagsverket' FROM staging
    WHERE EXISTS(SELECT 1 FROM owned) AND company_name IS NOT NULL AND length(company_name)>0
    GROUP BY orgnr HAVING count(DISTINCT company_name)=1
    ORDER BY orgnr
    ON CONFLICT(orgnr) DO NOTHING RETURNING 1
) SELECT count(*)::int AS companies_added FROM added
"""


COMMIT_SQL = """WITH owned AS MATERIALIZED (
    SELECT source_url FROM public.orgindex_bolagsverket_file_queue
    WHERE source_url=$1 AND status='running' AND attempts=$2::int FOR UPDATE
), staging AS MATERIALIZED (
    SELECT * FROM jsonb_to_recordset($3::jsonb) AS r(
      orgnr text, period_start date, period_end date, reporting_scope text, currency text,
      net_revenue numeric, operating_profit numeric, profit_after_financial_items numeric,
      net_profit numeric, total_assets numeric, equity numeric, employees numeric,
      source_document_id text, company_name text)
), new_companies AS (
    INSERT INTO public.orgindex_companies(orgnr,name,source)
    SELECT orgnr,min(company_name),'Bolagsverket' FROM staging
    WHERE EXISTS(SELECT 1 FROM owned) AND company_name IS NOT NULL AND length(company_name)>0
    GROUP BY orgnr HAVING count(DISTINCT company_name)=1
    ON CONFLICT(orgnr) DO NOTHING RETURNING orgnr
), matched AS MATERIALIZED (
    SELECT c.orgnr FROM public.orgindex_companies c JOIN staging s USING(orgnr)
    UNION SELECT orgnr FROM new_companies
), inserted AS (
    INSERT INTO public.orgindex_annual_accounts
      (orgnr,period_start,period_end,reporting_scope,currency,net_revenue,operating_profit,
       profit_after_financial_items,net_profit,total_assets,equity,employees,source_document_id,source)
    SELECT s.orgnr,s.period_start,s.period_end,s.reporting_scope,s.currency,s.net_revenue,
      s.operating_profit,s.profit_after_financial_items,s.net_profit,s.total_assets,s.equity,
      s.employees,s.source_document_id,'Bolagsverket'
    FROM staging s JOIN matched c USING(orgnr)
    WHERE EXISTS(SELECT 1 FROM owned)
    ON CONFLICT(orgnr,period_start,period_end,reporting_scope) DO NOTHING RETURNING 1
), counts AS (
    SELECT (SELECT count(*)::int FROM inserted) AS added,
           (SELECT count(*)::int FROM staging s WHERE NOT EXISTS
             (SELECT 1 FROM matched c WHERE c.orgnr=s.orgnr)) AS unmatched,
           (SELECT count(*)::int FROM new_companies) AS companies_added
), saved AS (
    INSERT INTO public.orgindex_bv_archive_imports
      (source_url,sha256,downloaded_bytes,report_count,parsed_rows,inserted_rows,
       unmatched_rows,extracted_records,diagnostics,parser_version)
    SELECT owned.source_url,$4,$5::bigint,$6::int,jsonb_array_length($3::jsonb),
      counts.added,counts.unmatched,$3::jsonb,$7::jsonb,$8 FROM owned,counts
    ON CONFLICT(source_url) DO UPDATE SET
      sha256=excluded.sha256,downloaded_bytes=excluded.downloaded_bytes,
      report_count=excluded.report_count,parsed_rows=excluded.parsed_rows,
      inserted_rows=excluded.inserted_rows,unmatched_rows=excluded.unmatched_rows,
      extracted_records=excluded.extracted_records,diagnostics=excluded.diagnostics,
      parser_version=excluded.parser_version,imported_at=now()
    RETURNING source_url,inserted_rows,unmatched_rows
), completed AS (
    UPDATE public.orgindex_bolagsverket_file_queue q
    SET status='completed',lease_until=NULL,completed_at=now(),updated_at=now(),
      imported_rows=saved.inserted_rows,
      last_error=CASE WHEN saved.unmatched_rows>0 OR $9::int>0 OR $10::int>0
        THEN 'Retained extracted data for review: unmatched='||saved.unmatched_rows||
             ', skipped='||$9::int||', conflicts='||$10::int ELSE NULL END
    FROM saved WHERE q.source_url=saved.source_url
    RETURNING q.source_url,q.imported_rows
) SELECT completed.source_url,completed.imported_rows,counts.unmatched,counts.companies_added FROM completed,counts
"""


def import_one(db, cached_archive=None):
    from scripts.bv_parse import ParseError, parse_archive

    job = claim(db)
    if not job:
        return {"state": "idle"}
    url, attempt = job["source_url"], int(job["attempts"])
    label = "/".join(url.split("/")[-2:])
    print(json_text({"state": "started", "file": label, "attempt": attempt}), flush=True)
    buffer = None
    started = time.monotonic()
    try:
        if cached_archive is not None:
            path = Path(cached_archive)
            if path.name != url.rsplit("/", 1)[-1] or not (0 < path.stat().st_size <= MAX_BYTES):
                raise ImportFailure("cached_archive_mismatch")
            buffer = io.BytesIO(path.read_bytes())
            size = buffer.getbuffer().nbytes
            digest = hashlib.sha256(buffer.getbuffer()).hexdigest()
        else:
            buffer, size, digest = download(url)
        downloaded_at = time.monotonic()
        parsed = parse_archive(buffer)
        # The parser returns independent records, never views of the ZIP buffer.
        # Release archive memory before building the bounded Neon HTTP body.
        buffer.close()
        buffer = None
        parsed_at = time.monotonic()
        if not parsed["report_count"]:
            raise ImportFailure("empty_archive")
        diagnostics = {k: parsed.get(k, []) for k in ("skipped", "conflicts", "ignored_attachments")}
        records = json_text(parsed["rows"])
        company_records = json_text(company_preparation_records(parsed["rows"]))
        # Preparing companies in a prior statement also sees concurrent SCB inserts
        # before the account statement's snapshot. Both statements commit together.
        queries = [
            (PREPARE_COMPANIES_SQL, [url, attempt, company_records]),
            (COMMIT_SQL, [url, attempt, records, digest, size,
                parsed["report_count"], json_text(diagnostics), PARSER_VERSION,
                len(diagnostics["skipped"]), len(diagnostics["conflicts"])]),
        ]
        try:
            companies, result = db.locked_transaction(queries, COMMIT_LOCK)
        except ImportFailure as exc:
            if str(exc) != "database_request_too_large":
                raise
            # This specific error is raised before any network request. Retry
            # locally with compressed diagnostic text. If it still cannot fit,
            # use the native Postgres protocol with the same atomic transaction.
            diagnostics = compress_diagnostic_text(diagnostics)
            queries[1][1][6] = json_text(diagnostics)
            try:
                companies, result = db.locked_transaction(queries, COMMIT_LOCK)
            except ImportFailure as compact_error:
                if str(compact_error) != "database_request_too_large":
                    raise
                companies, result = db.postgres_locked_transaction(queries, COMMIT_LOCK)
        if not result:
            raise ImportFailure("lease_lost")
        return {"state": "completed", "file": label, "bytes": size,
                "reports": parsed["report_count"], "parsed_rows": len(parsed["rows"]),
                "inserted_rows": int(result[0]["imported_rows"]),
                "unmatched_rows": int(result[0]["unmatched"]),
                "companies_added": int(companies[0]["companies_added"]) + int(result[0]["companies_added"]),
                "skipped": len(diagnostics["skipped"]), "conflicts": len(diagnostics["conflicts"]),
                "seconds": {"download": round(downloaded_at-started, 2),
                            "parse": round(parsed_at-downloaded_at, 2),
                            "commit": round(time.monotonic()-parsed_at, 2),
                            "total": round(time.monotonic()-started, 2)}}
    except Exception as exc:
        # Parser failures are controlled diagnostic codes. Preserve their reason
        # so a permanently failing file can be repaired instead of retried blind.
        # Unknown exceptions can contain credentials and remain class names only.
        message = str(exc) if isinstance(exc, (ImportFailure, ParseError)) else type(exc).__name__
        message = re.sub(r"[\r\n\t]", " ", message)[:500]
        db.sql("""UPDATE public.orgindex_bolagsverket_file_queue
           SET status='failed',lease_until=NULL,updated_at=now(),last_error=$3,
               next_attempt_at=now()+(least(3600,60*power(2,least(attempts,6)))::int * interval '1 second')
           WHERE source_url=$1 AND status='running' AND attempts=$2::int""", [url,attempt,message])
        raise ImportFailure(message) from None
    finally:
        if buffer is not None:
            buffer.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-files", type=int, default=3)
    parser.add_argument("--cached-archive")
    parser.add_argument("--seed", action="store_true")
    parser.add_argument("--watch", action="store_true", help="Wait for leases/retries until the manifests finish")
    args = parser.parse_args()
    db = Database()
    if args.seed:
        print(json_text({"seed": seed_queue(db)}), flush=True)
    completed = 0
    cache = args.cached_archive
    while args.max_files == 0 or completed < args.max_files:
        try:
            result = import_one(db, cache)
        except ImportFailure as exc:
            print(json_text({"state": "retry_scheduled", "error": str(exc)}), flush=True)
            if not args.watch:
                raise
            cache = None
            time.sleep(15)
            continue
        print(json_text(result), flush=True)
        if result["state"] == "idle":
            if not args.watch:
                break
            pending = db.sql("""SELECT count(*)::int AS remaining
              FROM public.orgindex_bolagsverket_file_queue WHERE status<>'completed'
              AND source_url IN (SELECT value FROM jsonb_array_elements_text($1::jsonb))""",
              [json_text(manifests())])[0]["remaining"]
            if not int(pending):
                print(json_text({"state": "all_manifests_completed"}), flush=True)
                break
            time.sleep(15)
        else:
            completed += 1
            cache = None


if __name__ == "__main__":
    main()

