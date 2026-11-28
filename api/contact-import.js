const { timingSafeEqual } = require('node:crypto');

const SCB_URL = 'https://apiafr.scb.se/v1/juridiskaenheter/full';
const SCB_SOURCE = 'https://www.scb.se/vara-tjanster/bestall-data-och-statistik/foretagsregistret/avgiftsfria-uppgifter-i-foretagsregistret/';
const LOCK_ID = 728493;
const PAGE_SIZE = 1000;
const MAX_PAGES = 10;
const RUN_BUDGET_MS = 45000;
const SQL_CLEANUP_RESERVE_MS = 25000;
const PAGE_PACING_MS = 500;

class ImportError extends Error {
  constructor(code, status = 0, retryAfter = null) {
    super(code);
    this.code = code;
    this.status = status;
    this.retryAfter = retryAfter;
  }
}

function validOrgNr(value) {
  if (typeof value !== 'string' && !(typeof value === 'number' && Number.isSafeInteger(value))) return null;
  let n = String(value).trim();
  if (/^16\d{10}$/.test(n)) n = n.slice(2);
  if (!/^\d{6}-?\d{4}$/.test(n)) return null;
  n = n.replace('-', '');
  if (+n[2] < 2) return null;
  const sum = [...n].reduce((s, ch, i) => {
    const v = Number(ch) * (i % 2 ? 1 : 2);
    return s + Math.floor(v / 10) + v % 10;
  }, 0);
  return sum % 10 === 0 ? n : null;
}

function normalisePhone(value) {
  if (typeof value !== 'string') return null;
  const phone = value.trim().replace(/[\s\u00a0]+/g, ' ');
  if (phone.length < 3 || phone.length > 64 || !/^[+0-9 ().-]+$/.test(phone)) return null;
  // Remove an explicitly written optional Swedish trunk zero, not an inferred digit.
  let number = phone.replace(/^(\+46|0046)\s*\(0\)/, '$1').replace(/[ ().-]/g, '');
  if (number.startsWith('00')) number = '+' + number.slice(2);
  if (/^0[1-9]\d{5,10}$/.test(number)) number = '+46' + number.slice(1);
  if (!/^\+?[0-9]{3,15}$/.test(number) || /^\+?0+$/.test(number) || number.startsWith('+0')) return null;
  return { phone, phone_uri: 'tel:' + number };
}

function integer(value) {
  if (typeof value === 'string' && /^-?\d+$/.test(value)) value = Number(value);
  return typeof value === 'number' && Number.isSafeInteger(value) ? value : null;
}

function cursor(value) {
  const n = integer(value);
  return n !== null && n >= 0 ? n : null;
}

function marketingBlock(row) {
  const value = row.telefonSparrTyp ?? row.TelefonSparrTyp ?? row.telSparrTyp ?? row.TelSparrTyp;
  const n = integer(value);
  return n !== null && n >= -2147483648 && n <= 2147483647 ? n : null;
}

function parsePage(body, previousCursor) {
  if (!body || !Array.isArray(body.jes) || body.jes.length > PAGE_SIZE
      || !body.pagination || typeof body.pagination.hasMore !== 'boolean') {
    throw new ImportError('scb_invalid_page');
  }
  const hasMore = body.pagination.hasMore;
  const next = cursor(body.pagination.nextCursorId);
  if (hasMore && (next === null || next <= previousCursor || body.jes.length === 0)) {
    throw new ImportError('scb_invalid_cursor');
  }
  // SCB may use zero/null as the terminal cursor; hasMore=false ends traversal.
  const rows = new Map();
  for (const source of body.jes) {
    if (!source || typeof source !== 'object' || Array.isArray(source)) continue;
    const orgnr = validOrgNr(source.orgNr ?? source.OrgNr ?? source.orgnr);
    if (!orgnr) continue;
    const number = normalisePhone(source.tel ?? source.Tel);
    const row = {
      orgnr,
      phone: number?.phone ?? null,
      phone_uri: number?.phone_uri ?? null,
      phone_marketing_block_type: marketingBlock(source),
    };
    const prior = rows.get(orgnr);
    if (prior && (prior.phone_uri !== row.phone_uri
      || prior.phone_marketing_block_type !== row.phone_marketing_block_type)) {
      throw new ImportError('scb_conflicting_duplicate');
    }
    if (!prior) rows.set(orgnr, row);
  }
  return {
    rows: [...rows.values()],
    scanned: body.jes.length,
    nextCursor: next !== null && next > previousCursor ? next : previousCursor,
    done: !hasMore,
  };
}

function sourcePageUrl(currentCursor) {
  const url = new URL(SCB_URL);
  url.searchParams.set('limit', String(PAGE_SIZE));
  if (currentCursor > 0) url.searchParams.set('cursorId', String(currentCursor));
  return url.href;
}

function retryAfterMs(value, nowMs) {
  if (typeof value !== 'string' || !value.trim()) return null;
  if (/^\d+$/.test(value.trim())) {
    const ms = Number(value.trim()) * 1000;
    return Number.isSafeInteger(ms) ? ms : null;
  }
  const when = Date.parse(value);
  return Number.isFinite(when) ? Math.max(0, when - nowMs) : null;
}

function retryDelayMs(error, nowMs) {
  const ordinary = error.status === 429 || error.status >= 500
    || ['scb_network_error', 'scb_timeout'].includes(error.code) ? 60000
    : [401, 403].includes(error.status) ? 900000 : 300000;
  const requested = retryAfterMs(error.retryAfter, nowMs);
  // Our retries are bounded; a server's longer Retry-After must still be respected.
  return Math.max(ordinary, requested ?? 0);
}

function authorised(req, secret) {
  if (typeof secret !== 'string' || !secret) return false;
  const header = req.headers?.authorization ?? req.headers?.Authorization;
  if (typeof header !== 'string' || header.length > 4096) return false;
  const got = Buffer.from(header);
  const expected = Buffer.from('Bearer ' + secret);
  return got.length === expected.length && timingSafeEqual(got, expected);
}

const UPSERT_SQL = `
WITH incoming AS (
  SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(
    orgnr text, phone text, phone_uri text, phone_marketing_block_type integer
  )
), saved AS (
  INSERT INTO public.orgindex_company_contacts AS existing (
    orgnr, phone, phone_uri, phone_source_name, phone_source_url,
    phone_verified_at, phone_origin, phone_marketing_block_type,
    scb_checked_at, updated_at
  )
  SELECT x.orgnr, x.phone, x.phone_uri,
    CASE WHEN x.phone IS NOT NULL THEN 'SCB' END,
    CASE WHEN x.phone IS NOT NULL THEN $2::text END,
    CASE WHEN x.phone IS NOT NULL THEN now() END,
    CASE WHEN x.phone IS NOT NULL THEN 'registry' END,
    x.phone_marketing_block_type, now(), now()
  FROM incoming x JOIN public.orgindex_companies c ON c.orgnr=x.orgnr
  ON CONFLICT (orgnr) DO UPDATE SET
    phone=CASE WHEN existing.phone_origin='verified_site' THEN existing.phone ELSE EXCLUDED.phone END,
    phone_uri=CASE WHEN existing.phone_origin='verified_site' THEN existing.phone_uri ELSE EXCLUDED.phone_uri END,
    phone_source_name=CASE WHEN existing.phone_origin='verified_site' THEN existing.phone_source_name ELSE EXCLUDED.phone_source_name END,
    phone_source_url=CASE WHEN existing.phone_origin='verified_site' THEN existing.phone_source_url ELSE EXCLUDED.phone_source_url END,
    phone_verified_at=CASE WHEN existing.phone_origin='verified_site' THEN existing.phone_verified_at ELSE EXCLUDED.phone_verified_at END,
    phone_origin=CASE WHEN existing.phone_origin='verified_site' THEN existing.phone_origin ELSE EXCLUDED.phone_origin END,
    phone_marketing_block_type=EXCLUDED.phone_marketing_block_type,
    scb_checked_at=EXCLUDED.scb_checked_at, updated_at=now()
  RETURNING phone
)
SELECT count(*)::integer AS matched, count(phone)::integer AS phone_rows FROM saved
`;

const ADVANCE_SQL = `
UPDATE public.orgindex_contact_import_state
SET cursor_id=$1, done=$2, pages=pages+1, scanned_rows=scanned_rows+$3,
    matched_rows=matched_rows+$4, phone_rows=phone_rows+$5,
    last_error=NULL, next_attempt_at=now(), updated_at=now()
WHERE id=1 AND cursor_id=$6
RETURNING *
`;

async function applyPage(client, page, previousCursor) {
  await client.query('SAVEPOINT contact_page');
  try {
    const result = await client.query(UPSERT_SQL, [JSON.stringify(page.rows), SCB_SOURCE]);
    const counts = result.rows[0];
    const advanced = await client.query(ADVANCE_SQL, [
      page.nextCursor, page.done, page.scanned, Number(counts.matched), Number(counts.phone_rows), previousCursor,
    ]);
    if (advanced.rowCount !== 1) throw new ImportError('contact_cursor_changed');
    await client.query('RELEASE SAVEPOINT contact_page');
    return advanced.rows[0];
  } catch (error) {
    try {
      await client.query('ROLLBACK TO SAVEPOINT contact_page');
      await client.query('RELEASE SAVEPOINT contact_page');
    } catch {
      error.rollbackAll = true;
    }
    throw error;
  }
}

function publicState(state) {
  if (!state) return null;
  return {
    cursor_id: String(state.cursor_id), done: state.done,
    pages: Number(state.pages), scanned_rows: Number(state.scanned_rows),
    matched_rows: Number(state.matched_rows), phone_rows: Number(state.phone_rows),
    last_error: state.last_error, next_attempt_at: state.next_attempt_at,
    updated_at: state.updated_at,
  };
}

function defaultCreateClient(env) {
  const { Client } = require('pg');
  return new Client({
    connectionString: env.DATABASE_URL,
    connectionTimeoutMillis: 5000,
    statement_timeout: 2000,
    query_timeout: 2500,
  });
}

function createHandler(deps = {}) {
  const env = deps.env ?? process.env;
  const now = deps.now ?? Date.now;
  const sleep = deps.sleep ?? (ms => new Promise(resolve => setTimeout(resolve, ms)));
  const fetchPage = deps.fetch ?? fetch;
  const createClient = deps.createClient ?? (() => defaultCreateClient(env));
  return async function handler(req, res) {
    res.setHeader('Cache-Control', 'no-store');
    if (!['GET', 'POST'].includes(req.method)) {
      res.setHeader('Allow', 'GET, POST');
      return res.status(405).json({ error: 'method_not_allowed' });
    }
    if (!env.CRON_SECRET) return res.status(503).json({ error: 'configuration_unavailable' });
    if (!authorised(req, env.CRON_SECRET)) return res.status(401).json({ error: 'unauthorised' });
    if (!env.DATABASE_URL || !env.SCB_API_KEY) return res.status(503).json({ error: 'configuration_unavailable' });
    const started = now();
    let client;
    let state;
    let locked = false;
    let transactionOpen = false;
    let committing = false;
    let pagesThisRun = 0;
    try {
      client = createClient();
      await client.connect();
      if (req.query?.status !== '1') {
        // A transaction pins pooled Neon connections and keeps the lock effective.
        await client.query('BEGIN');
        transactionOpen = true;
        const lock = await client.query('SELECT pg_try_advisory_xact_lock($1) AS locked', [LOCK_ID]);
        locked = lock.rows[0]?.locked === true;
        if (!locked) {
          await client.query('ROLLBACK');
          transactionOpen = false;
          return res.status(200).json({ status: 'busy', pages_this_run: 0 });
        }
      }
      state = (await client.query('SELECT * FROM public.orgindex_contact_import_state WHERE id=1')).rows[0];
      if (!state) throw new ImportError('contact_state_not_initialized');
      if (req.query?.status === '1') return res.status(200).json({ status: 'ok', state: publicState(state) });
      if (state.done || new Date(state.next_attempt_at).getTime() > now()) {
        committing = true;
        await client.query('COMMIT');
        transactionOpen = false;
        committing = false;
        return res.status(200).json({ status: state.done ? 'done' : 'backoff', pages_this_run: 0, state: publicState(state) });
      }
      let currentCursor = cursor(state.cursor_id);
      if (currentCursor === null) throw new ImportError('contact_invalid_saved_cursor');
      while (pagesThisRun < MAX_PAGES && now() - started < RUN_BUDGET_MS - SQL_CLEANUP_RESERVE_MS - 1000) {
        if (pagesThisRun > 0) await sleep(PAGE_PACING_MS);
        const remaining = RUN_BUDGET_MS - (now() - started);
        if (remaining < SQL_CLEANUP_RESERVE_MS + 1000) break;
        let response;
        try {
          response = await fetchPage(sourcePageUrl(currentCursor), {
            headers: { 'X-API-Key': env.SCB_API_KEY, Accept: 'application/json' },
            redirect: 'error', signal: AbortSignal.timeout(Math.min(12000, remaining - SQL_CLEANUP_RESERVE_MS)),
          });
        } catch (error) {
          throw new ImportError(['AbortError', 'TimeoutError'].includes(error.name) ? 'scb_timeout' : 'scb_network_error');
        }
        if (!response.ok) throw new ImportError('scb_http_' + response.status, response.status, response.headers?.get('retry-after'));
        let body;
        try { body = await response.json(); } catch (error) {
          throw new ImportError(['AbortError', 'TimeoutError'].includes(error.name) ? 'scb_timeout' : 'scb_invalid_json');
        }
        const page = parsePage(body, currentCursor);
        // Leave time to complete or roll back the atomic database transaction.
        if (now() - started >= RUN_BUDGET_MS - SQL_CLEANUP_RESERVE_MS) break;
        state = await applyPage(client, page, currentCursor);
        pagesThisRun++;
        currentCursor = page.nextCursor;
        if (page.done) break;
      }
      committing = true;
      await client.query('COMMIT');
      transactionOpen = false;
      committing = false;
      return res.status(200).json({ status: state.done ? 'done' : 'continued', pages_this_run: pagesThisRun, state: publicState(state) });
    } catch (error) {
      const failure = error instanceof ImportError ? error : new ImportError('contact_database_error');
      const nextAttempt = new Date(now() + retryDelayMs(failure, now()));
      let retrySaved = false;
      if (transactionOpen && locked && state && !error.rollbackAll && !committing) {
        try {
          const result = await client.query(
            'UPDATE public.orgindex_contact_import_state SET last_error=$1,next_attempt_at=$2,updated_at=now() WHERE id=1 RETURNING *',
            [failure.code, nextAttempt.toISOString()],
          );
          const savedState = result.rows[0];
          if (!savedState) throw new ImportError('contact_state_not_initialized');
          committing = true;
          await client.query('COMMIT');
          transactionOpen = false;
          committing = false;
          state = savedState;
          retrySaved = true;
        } catch { /* Roll back an unusable transaction without rewinding a committed cursor. */ }
      }
      if (transactionOpen) {
        await client.query('ROLLBACK').catch(() => {});
        transactionOpen = false;
        // An uncertain COMMIT is resolved by the next authenticated status read.
        state = null;
      }
      return res.status(503).json({ status: retrySaved ? 'retry_scheduled' : 'unavailable', error: failure.code, pages_this_run: retrySaved ? pagesThisRun : 0, state: publicState(state) });
    } finally {
      // Close also rolls back an abandoned transaction and releases its xact lock.
      if (client) await client.end().catch(() => {});
    }
  };
}

module.exports = createHandler();
module.exports._test = {
  validOrgNr, normalisePhone, marketingBlock, parsePage, sourcePageUrl,
  retryAfterMs, retryDelayMs, authorised, applyPage, createHandler,
  UPSERT_SQL, ADVANCE_SQL, PAGE_SIZE, MAX_PAGES, RUN_BUDGET_MS,
};
