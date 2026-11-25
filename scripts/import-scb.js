const {Client}=require('pg');
async function run(){
if(process.env.VERCEL_ENV!=='production')return;
const c=new Client({connectionString:process.env.DATABASE_URL,connectionTimeoutMillis:8000});await c.connect();
try{
const lock=await c.query('SELECT pg_try_advisory_lock(728491) AS ok');if(!lock.rows[0].ok)return;
await c.query('CREATE TABLE IF NOT EXISTS orgindex_companies (orgnr text PRIMARY KEY,name text NOT NULL,source text NOT NULL,updated_at timestamptz NOT NULL DEFAULT now())');
await c.query('CREATE TABLE IF NOT EXISTS orgindex_import_state (id integer PRIMARY KEY,cursor_id bigint NOT NULL,done boolean NOT NULL DEFAULT false)');
await c.query('INSERT INTO orgindex_import_state(id,cursor_id) VALUES(1,100) ON CONFLICT DO NOTHING');
for(let i=0;i<10;i++){
const state=(await c.query('SELECT * FROM orgindex_import_state WHERE id=1')).rows[0];if(state.done)break;
const size=Number((await c.query('SELECT pg_database_size(current_database()) AS bytes')).rows[0].bytes);if(size>=850000000){console.log('Import paused: storage threshold');break;}
const u=new URL('https://apiafr.scb.se/v1/juridiskaenheter');u.searchParams.set('limit','100');u.searchParams.set('cursorId',String(state.cursor_id));
const r=await fetch(u,{headers:{'X-API-Key':process.env.SCB_API_KEY,Accept:'application/json'},signal:AbortSignal.timeout(15000)});if(!r.ok)throw Error('SCB HTTP '+r.status);
const b=await r.json();if(!Array.isArray(b.jes)||!b.pagination)throw Error('Unexpected page format');
const next=Number(b.pagination.nextCursorId);if(b.pagination.hasMore&&(!Number.isSafeInteger(next)||next<=Number(state.cursor_id)))throw Error('Cursor did not advance');
const rows=b.jes.flatMap(x=>{let n=String(x.orgNr||x.organisationsnummer||x.peOrgNr||'').replace(/-/g,'');if(n.length===12&&n.startsWith('16'))n=n.slice(2);return /^[0-9]{10}$/.test(n)&&+n[2]>=2&&typeof x.namn==='string'&&x.namn.trim()?[[n,x.namn.trim().slice(0,300)]]:[]});
await c.query('BEGIN');try{if(rows.length)await c.query("INSERT INTO orgindex_companies(orgnr,name,source) SELECT x.orgnr,x.name,'SCB' FROM unnest($1::text[],$2::text[]) AS x(orgnr,name) ON CONFLICT(orgnr) DO NOTHING",[rows.map(x=>x[0]),rows.map(x=>x[1])]);await c.query('UPDATE orgindex_import_state SET cursor_id=$1,done=$2 WHERE id=1',[Number.isSafeInteger(next)&&next>0?next:state.cursor_id,!b.pagination.hasMore]);await c.query('COMMIT');}catch(e){await c.query('ROLLBACK');throw e;}
console.log('Orgindex confirmed company count: '+(await c.query('SELECT count(*)::int AS count FROM orgindex_companies')).rows[0].count);
await new Promise(resolve=>setTimeout(resolve,250));
}
}finally{await c.end();}}
run().catch(e=>{console.error('Import failed: '+String(e.message).slice(0,200));});