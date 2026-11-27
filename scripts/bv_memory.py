"""Resume the verified Bolagsverket manifests without writing ZIP files.

DATABASE_URL (or BV_NEON_DSN) must be supplied by the runtime, never committed.
All account inserts, extracted records and the completion marker commit together.
"""
from __future__ import annotations

import argparse
import hashlib
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
YEARS = tuple(range(2026, 2019, -1))
ORIGIN = "https://vardefulla-datamangder.bolagsverket.se"
URL_PATTERN = re.compile(re.escape(ORIGIN) + r"/arsredovisningar-bulkfiler/arsredovisningar/(202[0-6])/(\d{2})_(\d+)\.zip\Z")
MAX_BYTES = 256 * 1024 * 1024
PARSER_VERSION = "memory-2026-11-28-v1"


class ImportFailure(RuntimeError):
    pass


def json_text(value):
    return json.dumps(value, default=str, ensure_ascii=False, separators=(",", ":"))


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

    def sql(self, query, params=()):
        request = urllib.request.Request(
            self.endpoint,
            data=json_text({"query": query, "params": list(params)}).encode(),
            headers={"Content-Type": "application/json", "Neon-Connection-String": self.dsn},
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
        if "rows" not in result:
            raise ImportFailure("database_invalid_response")
        return result["rows"]


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
), guard AS MATERIALIZED (
    SELECT pg_try_advisory_xact_lock(18472640) AS locked
), current_year AS (
    SELECT max(substring(source_url from '/arsredovisningar/([0-9]{4})/')) AS year
    FROM public.orgindex_bolagsverket_file_queue
    WHERE status <> 'completed' AND source_url IN (SELECT source_url FROM manifest)
), candidate AS (
    SELECT q.source_url FROM public.orgindex_bolagsverket_file_queue q, guard, current_year y
    WHERE guard.locked AND q.source_url IN (SELECT source_url FROM manifest)
      AND substring(q.source_url from '/arsredovisningar/([0-9]{4})/') = y.year
      AND ((q.status IN ('pending','failed') AND q.next_attempt_at <= now())
           OR (q.status='running' AND q.lease_until < now()))
      AND NOT EXISTS (SELECT 1 FROM public.orgindex_bolagsverket_file_queue live
                      WHERE live.status='running' AND live.lease_until >= now()
                        AND live.source_url IN (SELECT source_url FROM manifest))
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
    rows = db.sql(CLAIM_SQL, [json_text(manifests())])
    return rows[0] if rows else None


def download(url):
    if not URL_PATTERN.fullmatch(url):
        raise ImportFailure("invalid_source_url")
    buffer = io.BytesIO()
    digest = hashlib.sha256()
    started = time.monotonic()
    request = urllib.request.Request(url, headers={"User-Agent": "Orgindex-Bolagsverket-Importer/1.0"})
    try:
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=30) as response:
            length = response.headers.get("Content-Length")
            if length and int(length) > MAX_BYTES:
                raise ImportFailure("archive_too_large")
            while True:
                part = response.read(1024 * 1024)
                if not part:
                    break
                if buffer.tell() + len(part) > MAX_BYTES:
                    raise ImportFailure("archive_too_large")
                buffer.write(part)
                digest.update(part)
                if time.monotonic() - started > 150:
                    raise ImportFailure("download_deadline")
            if length and buffer.tell() != int(length):
                raise ImportFailure("incomplete_download")
    except urllib.error.HTTPError as exc:
        raise ImportFailure(f"source_http_{exc.code}") from None
    except (urllib.error.URLError, TimeoutError):
        raise ImportFailure("source_connection_failed") from None
    size = buffer.tell()
    buffer.seek(0)
    return buffer, size, digest.hexdigest()


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
    from scripts.bv_parse import parse_archive

    job = claim(db)
    if not job:
        return {"state": "idle"}
    url, attempt = job["source_url"], int(job["attempts"])
    label = "/".join(url.split("/")[-2:])
    print(json_text({"state": "started", "file": label, "attempt": attempt}), flush=True)
    buffer = None
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
        parsed = parse_archive(buffer)
        if not parsed["report_count"]:
            raise ImportFailure("empty_archive")
        diagnostics = {k: parsed.get(k, []) for k in ("skipped", "conflicts")}
        result = db.sql(COMMIT_SQL, [url, attempt, json_text(parsed["rows"]), digest, size,
            parsed["report_count"], json_text(diagnostics), PARSER_VERSION,
            len(diagnostics["skipped"]), len(diagnostics["conflicts"])])
        if not result:
            raise ImportFailure("lease_lost")
        return {"state": "completed", "file": label, "bytes": size,
                "reports": parsed["report_count"], "parsed_rows": len(parsed["rows"]),
                "inserted_rows": int(result[0]["imported_rows"]),
                "unmatched_rows": int(result[0]["unmatched"]),
                "companies_added": int(result[0]["companies_added"]),
                "skipped": len(diagnostics["skipped"]), "conflicts": len(diagnostics["conflicts"])}
    except Exception as exc:
        message = str(exc) if isinstance(exc, ImportFailure) else type(exc).__name__
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
