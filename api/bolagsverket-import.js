const {Pool}=require('pg');
const {timingSafeEqual}=require('node:crypto');
const {downloadDocument}=require('../scripts/bolagsverket-download');
module.exports=async(req,res)=>{
 res.setHeader('Cache-Control','no-store');
 const secret=process.env.CRON_SECRET;
 const given=Buffer.from(req.headers.authorization||'');
 const expected=Buffer.from('Bearer '+(secret||''));
 if(!secret||given.length!==expected.length||!timingSafeEqual(given,expected))return res.status(401).end();
 if(!['GET','POST'].includes(req.method))return res.status(405).end();
 const id=process.env.BOLAGSVERKET_CLIENT_ID, key=process.env.BOLAGSVERKET_CLIENT_SECRET;
 if(!id||!key||!process.env.DATABASE_URL)return res.status(503).json({error:'missing_configuration'});
 const pool=new Pool({connectionString:process.env.DATABASE_URL,max:1,connectionTimeoutMillis:5000,statement_timeout:8000});
 let client;
 try{
 client=await pool.connect();
 await client.query('BEGIN');
 const lock=await client.query('SELECT pg_try_advisory_xact_lock(18472639) AS locked');
 if(!lock.rows[0].locked){await client.query('ROLLBACK');return res.status(200).json({busy:true});}
 await client.query('CREATE TABLE IF NOT EXISTS public.orgindex_bv_discovery (orgnr text PRIMARY KEY, checked_at timestamptz NOT NULL DEFAULT now())');
 const companies=await client.query("SELECT c.orgnr FROM orgindex_companies c WHERE c.orgnr ~ '^[0-9]{10}$' AND substring(c.orgnr,3,1) >= '2' AND NOT EXISTS (SELECT 1 FROM orgindex_bv_discovery d WHERE d.orgnr=c.orgnr) ORDER BY c.orgnr LIMIT 1");
 const orgnr=companies.rows[0]?.orgnr;
 const t=await fetch('https://portal.api.bolagsverket.se/oauth2/token',{method:'POST',redirect:'error',headers:{Authorization:'Basic '+Buffer.from(id+':'+key).toString('base64'),'Content-Type':'application/x-www-form-urlencoded'},body:new URLSearchParams({grant_type:'client_credentials',scope:'vardefulla-datamangder:read'}),signal:AbortSignal.timeout(10000)});
 if(!t.ok)throw Error('token_http_'+t.status);
 const {access_token}=await t.json();
 if(!access_token)throw Error('missing_token');
 const base='https://gw.api.bolagsverket.se/vardefulla-datamangder/v1';
 await client.query('CREATE TABLE IF NOT EXISTS public.orgindex_bv_raw_documents (source_url text PRIMARY KEY, zip_bytes bytea NOT NULL, sha256 text NOT NULL, downloaded_at timestamptz NOT NULL DEFAULT now())');
 const pending=await client.query("SELECT q.source_url FROM orgindex_bolagsverket_file_queue q WHERE NOT EXISTS (SELECT 1 FROM orgindex_bv_raw_documents r WHERE r.source_url=q.source_url) AND q.source_url LIKE 'https://gw.api.bolagsverket.se/vardefulla-datamangder/v1/dokument/%' ORDER BY q.source_url LIMIT 1");
 if(pending.rows.length){const url=pending.rows[0].source_url;const file=await downloadDocument(url,access_token);await client.query('INSERT INTO orgindex_bv_raw_documents(source_url,zip_bytes,sha256) VALUES($1,$2,$3) ON CONFLICT DO NOTHING',[url,file.bytes,file.sha256]);await client.query('COMMIT');return res.json({downloaded:1,bytes:file.size});}
 if(!orgnr){await client.query('COMMIT');return res.json({discovered:0,complete:true});}
 const d=await fetch(base+'/dokumentlista',{method:'POST',redirect:'error',headers:{Authorization:'Bearer '+access_token,'Content-Type':'application/json'},body:JSON.stringify({identitetsbeteckning:orgnr}),signal:AbortSignal.timeout(10000)});
 if(!d.ok)throw Error('list_http_'+d.status);
 const data=await d.json(); const docs=new Set();
 function walk(v){if(!v||typeof v!=='object')return;const n=v.dokumentId||v.dokumentid;if(typeof n==='string'&&n.length<300)docs.add(n);for(const x of Object.values(v))walk(x);}
 walk(data);
 if(!docs.size){if(!Array.isArray(data.dokument)||data.dokument.length!==0){await client.query('ROLLBACK');return res.status(422).json({error:'no_recognized_document_ids'});}await client.query('INSERT INTO orgindex_bv_discovery(orgnr) VALUES($1) ON CONFLICT DO NOTHING',[orgnr]);await client.query('COMMIT');return res.json({discovered:0,checked:1});}
 let queued=0;
 for(const doc of docs){const r=await client.query('INSERT INTO orgindex_bolagsverket_file_queue(source_url) VALUES($1) ON CONFLICT DO NOTHING',[base+'/dokument/'+encodeURIComponent(doc)]);queued+=r.rowCount;}
 await client.query('INSERT INTO orgindex_bv_discovery(orgnr) VALUES($1) ON CONFLICT DO NOTHING',[orgnr]);
 await client.query('COMMIT');
 return res.json({queued,discovered:docs.size});
 }catch(e){if(client)await client.query('ROLLBACK').catch(()=>{});const msg=/^(token_http_|list_http_|document_http_)\d+$|^missing_token$/.test(e.message)?e.message:'worker_error';console.error('Bolagsverket discovery',msg);return res.status(502).json({error:msg});}
 finally{if(client)client.release();await pool.end().catch(()=>{});}
};
