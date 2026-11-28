const test = require('node:test');
const assert = require('node:assert/strict');
const t = require('../api/contact-import')._test;

const SKS = '5566733472';
const NAP = '5594860594';
const env = { CRON_SECRET: 'unit-test-cron', SCB_API_KEY: 'unit-test-key', DATABASE_URL: 'postgresql://unit-test' };
const start = Date.UTC(2026, 10, 28, 11);
const initialState = () => ({
  id: 1, cursor_id: '0', done: false, pages: '0', scanned_rows: '0',
  matched_rows: '0', phone_rows: '0', last_error: null,
  next_attempt_at: new Date(start - 1000).toISOString(), updated_at: new Date(start - 1000).toISOString(),
});
const page = (jes, nextCursorId = null, hasMore = false) => ({ jes, pagination: { nextCursorId, hasMore } });

test('identity accepts registered-company formatting and rejects invalid/person identities', () => {
  for (const value of [SKS, '556673-3472', '16' + SKS, Number(SKS)]) assert.equal(t.validOrgNr(value), SKS);
  for (const value of ['5566733473', '8507099805', '5512345678', 'abc5566733472', null, {}, '556673--3472']) {
    assert.equal(t.validOrgNr(value), null);
  }
});

test('telephone normalization preserves registered numbers without inventing leading zeroes', () => {
  const cases = [
    ['031-82 48 18', 'tel:+4631824818'],
    ['+46 (0)31 82 48 18', 'tel:+4631824818'],
    ['0046 (0)31 82 48 18', 'tel:+4631824818'],
    ['070 123 45 67', 'tel:+46701234567'],
    ['+44 20 7946 0958', 'tel:+442079460958'],
    ['11414', 'tel:11414'],
    ['31824818', 'tel:31824818'],
  ];
  for (const [value, uri] of cases) assert.equal(t.normalisePhone(value)?.phone_uri, uri);
  for (const value of [31824818, null, '', '0', '000000', '031/82 48 18', 'tel:031824818', '+46++31', '<a>031</a>', '031824818 ank 4']) {
    assert.equal(t.normalisePhone(value), null);
  }
});

test('SCB source casing, marketing block, and explicit no-email/no-homepage whitelist', () => {
  const parsed = t.parsePage(page([
    { orgNr: SKS, tel: '031-82 48 18', telefonSparrTyp: 1, epost: 'private@example.test', webb: 'https://unverified.example/' },
    { OrgNr: NAP, Tel: '+46 8 1234567', TelefonSparrTyp: '2' },
    { orgNr: '8507099805', tel: '0701234567' },
  ]), 0);
  assert.equal(parsed.rows.length, 2);
  assert.equal(parsed.scanned, 3);
  assert.equal(parsed.rows[0].phone_marketing_block_type, 1);
  assert.equal(parsed.rows[0].phone_uri, 'tel:+4631824818');
  assert.equal(parsed.rows[1].phone_marketing_block_type, 2);
  assert.deepEqual(Object.keys(parsed.rows[0]).sort(), ['orgnr', 'phone', 'phone_uri', 'phone_marketing_block_type'].sort());
  assert.equal(JSON.stringify(parsed).includes('private@example.test'), false);
  assert.equal(JSON.stringify(parsed).includes('unverified.example'), false);
});

test('consistent duplicates are deduplicated; conflicting duplicate numbers fail before DB writes', () => {
  assert.equal(t.parsePage(page([{ orgNr: SKS, tel: '031824818' }, { orgNr: SKS, tel: '031-82 48 18' }]), 0).rows.length, 1);
  assert.throws(() => t.parsePage(page([{ orgNr: SKS, tel: '031824818' }, { orgNr: SKS, tel: '031824819' }]), 0), /scb_conflicting_duplicate/);
});

test('first source request omits cursor, subsequent requests preserve fixed page size', () => {
  const first = new URL(t.sourcePageUrl(0));
  const later = new URL(t.sourcePageUrl(123456));
  assert.equal(first.searchParams.has('cursorId'), false);
  assert.equal(first.searchParams.get('limit'), '1000');
  assert.equal(later.searchParams.get('cursorId'), '123456');
  assert.equal(later.searchParams.get('limit'), '1000');
});

test('pagination requires forward progress only for continuing pages; terminal zero/null is accepted', () => {
  const row = [{ orgNr: SKS, tel: '' }];
  assert.equal(t.parsePage(page(row, 0, false), 100).nextCursor, 100);
  assert.equal(t.parsePage(page([], null, false), 100).done, true);
  for (const body of [page(row, 100, true), page(row, 99, true), page(row, null, true), page([], 101, true), { jes: row, pagination: {} }]) {
    assert.throws(() => t.parsePage(body, 100), /scb_invalid_(page|cursor)/);
  }
});

test('Retry-After supports seconds/dates and never shortens a longer server delay', () => {
  assert.equal(t.retryAfterMs('120', start), 120000);
  assert.equal(t.retryAfterMs(new Date(start + 180000).toUTCString(), start), 180000);
  assert.equal(t.retryAfterMs('bad', start), null);
  assert.equal(t.retryDelayMs({ status: 429, retryAfter: '7200' }, start), 7200000);
  assert.equal(t.retryDelayMs({ status: 429, retryAfter: null }, start), 60000);
});

class FakeClient {
  constructor(options = {}) {
    this.options = options;
    this.state = options.state === null ? null : { ...initialState(), ...options.state };
    this.calls = [];
    this.ended = false;
    this.pending = null;
    this.savepoint = null;
    this.upserts = 0;
  }
  async connect() { this.calls.push(['connect']); }
  async end() { this.ended = true; }
  async query(sql, values = []) {
    this.calls.push([sql, values]);
    if (this.options.failOn === sql) throw new Error('simulated database failure containing no user data');
    if (/pg_try_advisory.*lock/.test(sql)) return { rows: [{ locked: !this.options.busy }] };
    if (sql === 'SELECT * FROM public.orgindex_contact_import_state WHERE id=1') return { rows: this.state ? [{ ...this.state }] : [] };
    if (sql === 'BEGIN') { this.pending = { ...this.state }; return { rows: [] }; }
    if (sql === 'SAVEPOINT contact_page') { this.savepoint = { ...this.pending }; return { rows: [] }; }
    if (sql === 'ROLLBACK TO SAVEPOINT contact_page') { this.pending = { ...this.savepoint }; return { rows: [] }; }
    if (sql === 'RELEASE SAVEPOINT contact_page') { this.savepoint = null; return { rows: [] }; }
    if (sql === 'ROLLBACK') { this.pending = null; return { rows: [] }; }
    if (sql === 'COMMIT') { this.state = this.pending; this.pending = null; return { rows: [] }; }
    if (sql === t.UPSERT_SQL) {
      this.upserts++;
      if (this.options.failUpsertNumber === this.upserts) throw new Error('simulated page SQL failure');
      const rows = JSON.parse(values[0]);
      return { rows: [{ matched: rows.length, phone_rows: rows.filter(x => x.phone !== null).length }] };
    }
    if (sql === t.ADVANCE_SQL) {
      const [next, done, scanned, matched, phones, previous] = values;
      if (this.options.cursorChanged || Number(this.pending.cursor_id) !== previous) return { rowCount: 0, rows: [] };
      this.pending = {
        ...this.pending, cursor_id: String(next), done,
        pages: Number(this.pending.pages) + 1, scanned_rows: Number(this.pending.scanned_rows) + scanned,
        matched_rows: Number(this.pending.matched_rows) + matched, phone_rows: Number(this.pending.phone_rows) + phones,
        last_error: null, next_attempt_at: new Date(start).toISOString(), updated_at: new Date(start).toISOString(),
      };
      return { rowCount: 1, rows: [{ ...this.pending }] };
    }
    if (sql.startsWith('UPDATE public.orgindex_contact_import_state SET last_error=')) {
      this.pending = { ...this.pending, last_error: values[0], next_attempt_at: values[1] };
      return { rowCount: 1, rows: [{ ...this.pending }] };
    }
    throw new Error('Unexpected mocked SQL');
  }
}

async function run(options = {}) {
  const client = new FakeClient(options);
  let clock = start;
  const fetchCalls = [];
  const sleeps = [];
  const handler = t.createHandler({
    env: options.env ?? env,
    createClient: () => client,
    now: () => clock,
    sleep: async ms => { sleeps.push(ms); clock += ms; },
    fetch: async (url, init) => {
      fetchCalls.push({ url, init });
      if (options.fetchElapsed) clock += options.fetchElapsed;
      if (options.networkError) throw options.networkError;
      const response = options.responses?.[fetchCalls.length - 1] ?? { body: page([{ orgNr: SKS, tel: '031824818' }], 100, false) };
      return {
        ok: (response.status ?? 200) < 400,
        status: response.status ?? 200,
        headers: { get: key => key === 'retry-after' ? response.retryAfter ?? null : null },
        json: async () => { if (response.invalidJson) throw new Error('bad JSON'); return response.body; },
      };
    },
  });
  const res = {
    code: null, headers: {}, body: null,
    setHeader(k, v) { this.headers[k] = v; },
    status(code) { this.code = code; return this; },
    json(body) { this.body = body; return body; },
  };
  await handler({ method: options.method ?? 'GET', headers: { authorization: options.auth ?? 'Bearer unit-test-cron' }, query: options.query ?? {} }, res);
  return { client, res, fetchCalls, sleeps, elapsed: clock - start };
}

test('authentication and methods reject before database or source requests', async () => {
  for (const options of [{ auth: 'Bearer wrong' }, { auth: '' }]) {
    const out = await run(options);
    assert.equal(out.res.code, 401);
    assert.equal(out.client.calls.length, 0);
    assert.equal(out.fetchCalls.length, 0);
  }
  assert.equal((await run({ method: 'DELETE' })).res.code, 405);
});

test('busy lock, scheduled backoff, and read-only status never fetch source data', async () => {
  for (const options of [{ busy: true }, { state: { next_attempt_at: new Date(start + 1000).toISOString() } }, { query: { status: '1' } }]) {
    const out = await run(options);
    assert.equal(out.res.code, 200);
    assert.equal(out.fetchCalls.length, 0);
    assert.equal(out.client.calls.some(([sql]) => sql === t.UPSERT_SQL || sql === t.ADVANCE_SQL), false);
    assert.equal(out.client.ended, true);
  }
});

test('one complete source page commits upsert and cursor together, exposing only counters', async () => {
  const out = await run({ method: 'POST' });
  assert.equal(out.res.code, 200);
  assert.equal(out.res.body.status, 'done');
  assert.equal(out.client.state.cursor_id, '100');
  assert.equal(out.client.state.pages, 1);
  assert.equal(out.client.state.phone_rows, 1);
  const sql = out.client.calls.map(x => x[0]);
  assert.ok(sql.indexOf('BEGIN') < sql.indexOf(t.UPSERT_SQL));
  assert.ok(sql.includes('SELECT pg_try_advisory_xact_lock($1) AS locked'));
  assert.ok(sql.indexOf('SAVEPOINT contact_page') < sql.indexOf(t.UPSERT_SQL));
  assert.ok(sql.indexOf(t.UPSERT_SQL) < sql.indexOf(t.ADVANCE_SQL));
  assert.ok(sql.indexOf(t.ADVANCE_SQL) < sql.indexOf('COMMIT'));
  assert.equal(out.client.ended, true);
  assert.equal(JSON.stringify(out.res.body).includes('031824818'), false);
});

test('429 records a future Retry-After without advancing or repeatedly fetching', async () => {
  const out = await run({ responses: [{ status: 429, retryAfter: '120' }] });
  assert.equal(out.res.code, 503);
  assert.equal(out.client.state.cursor_id, '0');
  assert.equal(out.client.state.last_error, 'scb_http_429');
  assert.equal(out.client.state.next_attempt_at, new Date(start + 120000).toISOString());
  assert.equal(out.fetchCalls.length, 1);
  assert.equal(out.client.calls.some(([sql]) => sql === t.UPSERT_SQL || sql === t.ADVANCE_SQL), false);
});

test('invalid JSON and nonadvancing pages never advance the cursor', async () => {
  for (const response of [{ invalidJson: true }, { body: page([{ orgNr: SKS, tel: '031824818' }], 0, true) }, { status: 500 }]) {
    const out = await run({ responses: [response] });
    assert.equal(out.res.code, 503);
    assert.equal(out.client.state.cursor_id, '0');
    assert.equal(out.client.calls.some(([sql]) => sql === t.ADVANCE_SQL), false);
  }
});

test('failed upsert or cursor mismatch rolls back the page and preserves its cursor', async () => {
  for (const options of [{ failOn: t.UPSERT_SQL }, { cursorChanged: true }]) {
    const out = await run(options);
    assert.equal(out.res.code, 503);
    assert.equal(out.client.state.cursor_id, '0');
    assert.ok(out.client.calls.some(([sql]) => sql === 'ROLLBACK TO SAVEPOINT contact_page'));
    assert.ok(out.client.calls.some(([sql]) => sql === 'COMMIT'));
  }
});

test('a run processes at most ten pages with pacing and a monotonic cursor', async () => {
  const responses = Array.from({ length: 10 }, (_, i) => ({ body: page([{ orgNr: SKS, tel: '031824818' }], (i + 1) * 100, true) }));
  const out = await run({ responses });
  assert.equal(out.res.code, 200);
  assert.equal(out.fetchCalls.length, 10);
  assert.equal(out.sleeps.length, 9);
  assert.ok(out.sleeps.every(ms => ms === 500));
  assert.equal(out.client.state.cursor_id, '1000');
  assert.equal(out.client.state.done, false);
});

test('insufficient remaining budget leaves the fetched page uncommitted for the next run', async () => {
  const out = await run({ fetchElapsed: 40000 });
  assert.equal(out.res.code, 200);
  assert.equal(out.res.body.pages_this_run, 0);
  assert.equal(out.client.state.cursor_id, '0');
  assert.equal(out.client.calls.some(([sql]) => sql === t.UPSERT_SQL), false);
  assert.equal(out.client.ended, true);
});

test('a later source failure commits completed pages and its retry state under the same xact lock', async () => {
  const out = await run({ responses: [
    { body: page([{ orgNr: SKS, tel: '031824818' }], 100, true) },
    { status: 429, retryAfter: '90' },
  ] });
  assert.equal(out.res.code, 503);
  assert.equal(out.res.body.status, 'retry_scheduled');
  assert.equal(out.res.body.pages_this_run, 1);
  assert.equal(out.client.state.cursor_id, '100');
  assert.equal(out.client.state.pages, 1);
  assert.equal(out.client.state.last_error, 'scb_http_429');
  assert.equal(out.client.calls.filter(([sql]) => sql === 'COMMIT').length, 1);
});

test('a later SQL failure rolls back only its page and commits earlier data/cursor plus backoff', async () => {
  const out = await run({ failUpsertNumber: 2, responses: [
    { body: page([{ orgNr: SKS, tel: '031824818' }], 100, true) },
    { body: page([{ orgNr: NAP, tel: '+4681234567' }], 200, false) },
  ] });
  assert.equal(out.res.code, 503);
  assert.equal(out.res.body.status, 'retry_scheduled');
  assert.equal(out.res.body.pages_this_run, 1);
  assert.equal(out.client.state.cursor_id, '100');
  assert.equal(out.client.state.pages, 1);
  assert.equal(out.client.state.last_error, 'contact_database_error');
  assert.equal(out.client.calls.filter(([sql]) => sql === 'ROLLBACK TO SAVEPOINT contact_page').length, 1);
  assert.equal(out.client.calls.filter(([sql]) => sql === 'COMMIT').length, 1);
});

test('an uncertain failed final commit is not claimed as a saved retry or completed page', async () => {
  const out = await run({ failOn: 'COMMIT' });
  assert.equal(out.res.code, 503);
  assert.equal(out.res.body.status, 'unavailable');
  assert.equal(out.res.body.state, null);
  assert.equal(out.res.body.pages_this_run, 0);
  assert.equal(out.client.ended, true);
});
