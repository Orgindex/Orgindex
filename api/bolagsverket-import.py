"""Protected, bounded bulk import invocation; called by Vercel Cron."""
import hmac
import json
import os
import threading
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlsplit

from scripts.bv_memory import Database, ImportFailure, MAX_CONCURRENT_IMPORTS, import_one


# Fluid Compute can dispatch several cron requests into the same Python process.
# Keep at most one memory-heavy archive per process; the existing database
# claim lock still permits up to three concurrent archives across processes.
_PROCESS_IMPORT_LOCK = threading.Lock()


class handler(BaseHTTPRequestHandler):
    def respond(self, status, payload):
        body = json.dumps(payload, default=str, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        secret = os.environ.get("CRON_SECRET")
        given = self.headers.get("Authorization", "")
        if not secret or not hmac.compare_digest(given.encode(), ("Bearer " + secret).encode()):
            return self.respond(401, {"error": "unauthorized"})
        try:
            db = Database()
            if parse_qs(urlsplit(self.path).query).get("status") == ["1"]:
                rows = db.sql("""SELECT substring(source_url from '/arsredovisningar/([0-9]{4})/') AS year,
                    status,count(*)::int AS files FROM public.orgindex_bolagsverket_file_queue
                    WHERE source_url LIKE 'https://vardefulla-datamangder.bolagsverket.se/arsredovisningar-bulkfiler/arsredovisningar/%'
                    GROUP BY 1,2 ORDER BY 1 DESC,2""")
                return self.respond(200, {"queue": rows, "zip_storage": False,
                                          "max_concurrent_imports": MAX_CONCURRENT_IMPORTS})
            if not _PROCESS_IMPORT_LOCK.acquire(blocking=False):
                return self.respond(200, {"state": "busy_memory_guard"})
            try:
                result = import_one(db)
            finally:
                _PROCESS_IMPORT_LOCK.release()
            print(json.dumps(result), flush=True)
            return self.respond(200, result)
        except Exception as exc:
            code = str(exc) if isinstance(exc, ImportFailure) else type(exc).__name__
            print(json.dumps({"bulk_import_error": code}), flush=True)
            return self.respond(503, {"error": code})

    do_POST = do_GET

    def log_message(self, format, *args):
        # Do not log auth headers or request query strings.
        pass
