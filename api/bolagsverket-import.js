const {Pool}=require('pg');
const {timingSafeEqual}=require('node:crypto');
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
 if(!companies.rows.length){await client.query('COMMIT');return res.json({discovered:0,complete:true});}
 const orgnr=companies.rows[0].orgnr;
 const t=await fetch('https://portal.api.bolagsverket.se/oauth2/token',{method:'POST',redirect:'error',headers:{Authorization:'Basic '+Buffer.from(id+':'+key).toString('base64'),'Content-Type':'application/x-www-form-urlencoded'},body:new URLSearchParams({grant_type:'client_credentials',scope:'vardefulla-datamangder:read'}),signal:AbortSignal.timeout(10000)});
 if(!t.ok)throw Error('token_http_'+t.status);
 const {access_token}=await t.json();
 if(!access_token)throw Error('missing_token');
 const base='https://gw.api.bolagsverket.se/vardefulla-datamangder/v1';
 const d=await fetch(base+'/dokumentlista',{method:'POST',redirect:'error',headers:{Authorization:'Bearer '+access_token,'Content-Type':'application/json'},body:JSON.stringify({identitetsbeteckning:orgnr}),signal:AbortSignal.timeout(10000)});
 if(!d.ok)throw Error('list_http_'+d.status);
 const data=await d.json(); const docs=new Set();
 function walk(v){if(!v||typeof v!=='object')return;const n=v.dokumentId||v.dokumentid;if(typeof n==='string'&&n.length<300)docs.add(n);for(const x of Object.values(v))walk(x);}
 walk(data);
 if(!docs.size){await client.query('ROLLBACK');return res.status(422).json({error:'no_recognized_document_ids'});}
 let queued=0;
 for(const doc of docs){const r=await client.query('INSERT INTO orgindex_bolagsverket_file_queue(source_url) VALUES($1) ON CONFLICT DO NOTHING',[base+'/dokument/'+encodeURIComponent(doc)]);queued+=r.rowCount;}
 await client.query('INSERT INTO orgindex_bv_discovery(orgnr) VALUES($1) ON CONFLICT DO NOTHING',[orgnr]);
 await client.query('COMMIT');
 return res.json({queued,discovered:docs.size});
 }catch(e){if(client)await client.query('ROLLBACK').catch(()=>{});const msg=/^(token_http_|list_http_)\d+$|^missing_token$/.test(e.message)?e.message:'worker_error';console.error('Bolagsverket discovery',msg);return res.status(502).json({error:msg});}
 finally{if(client)client.release();await pool.end().catch(()=>{});}
};
