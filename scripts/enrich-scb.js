const {Client}=require('pg');
async function run(){
 if(process.env.VERCEL_ENV!=='production')return;
 const c=new Client({connectionString:process.env.DATABASE_URL,connectionTimeoutMillis:8000});await c.connect();
 try{
 const lock=await c.query('SELECT pg_try_advisory_lock(728491) AS ok');if(!lock.rows[0].ok){console.log('SCB enrichment deferred: importer busy');return;}
 await c.query('CREATE TABLE IF NOT EXISTS public.orgindex_enrichment_state(id integer PRIMARY KEY,cursor_id bigint NOT NULL,done boolean NOT NULL DEFAULT false)');
 await c.query('INSERT INTO public.orgindex_enrichment_state(id,cursor_id) VALUES(1,0) ON CONFLICT DO NOTHING');
 const str=(v,n)=>typeof v==='string'&&v.trim()?v.trim().slice(0,n):null;
 const num=v=>typeof v==='number'&&Number.isSafeInteger(v)?v:null;
 for(let i=0;i<500;i++){
 const state=(await c.query('SELECT * FROM public.orgindex_enrichment_state WHERE id=1')).rows[0];if(state.done)break;
 const size=Number((await c.query('SELECT pg_database_size(current_database()) AS bytes')).rows[0].bytes);if(size>=800000000){console.log('SCB enrichment paused: storage guard');break;}
 const u=new URL('https://apiafr.scb.se/v1/juridiskaenheter/full');u.searchParams.set('limit','1000');u.searchParams.set('cursorId',String(state.cursor_id));
 const r=await fetch(u,{headers:{'X-API-Key':process.env.SCB_API_KEY,Accept:'application/json'},signal:AbortSignal.timeout(30000)});if(!r.ok)throw Error('SCB enrichment HTTP '+r.status);
 const b=await r.json();if(!Array.isArray(b.jes)||!b.pagination)throw Error('Unexpected enrichment response');
 const next=Number(b.pagination.nextCursorId);if(b.pagination.hasMore&&(!Number.isSafeInteger(next)||next<=Number(state.cursor_id)))throw Error('Enrichment cursor did not advance');
 const rows=b.jes.flatMap(x=>{const n=String(x.orgNr||'').replace(/-/g,'');if(!/^\d{2}[2-9]\d{7}$/.test(n))return [];const a=x.postAdress||{};const g=x.naringsgrenar||x.primarNaringsgren||[];const primary=Array.isArray(g)?g.find(y=>y.rangordning===1)||g[0]:g;const row={orgnr:n,address:str(a.gatuadress||a.gatuAdress,512),postal_code:str(a.postNr,5),city:str(a.postOrt,60),sni:str(primary?.naringsgren,5),legal_form:str(x.jurform,2),employee_class:num(x.anstKl),company_status:num(x.ftgStat),f_tax_status:num(x.fskattStat),vat_status:num(x.momsStat),employer_status:num(x.arbGivStat)};return Object.values(row).slice(1).some(v=>v!==null)?[row]:[];});
 if(i===0&&rows.length===0&&Number(state.cursor_id)===0)throw Error('No recognized detail fields on first page');
 await c.query('BEGIN');try{
 if(rows.length)await c.query("INSERT INTO public.orgindex_company_details(orgnr,address,postal_code,city,sni,legal_form,employee_class,company_status,f_tax_status,vat_status,employer_status) SELECT DISTINCT ON (x.orgnr) x.orgnr,x.address,x.postal_code,x.city,x.sni,x.legal_form,x.employee_class,x.company_status,x.f_tax_status,x.vat_status,x.employer_status FROM jsonb_to_recordset($1::jsonb) AS x(orgnr text,address text,postal_code text,city text,sni text,legal_form text,employee_class integer,company_status integer,f_tax_status integer,vat_status integer,employer_status integer) JOIN public.orgindex_companies c ON c.orgnr=x.orgnr ORDER BY x.orgnr ON CONFLICT(orgnr) DO NOTHING",[JSON.stringify(rows)]);
 await c.query('UPDATE public.orgindex_enrichment_state SET cursor_id=$1,done=$2 WHERE id=1',[Number.isSafeInteger(next)&&next>0?next:state.cursor_id,!b.pagination.hasMore]);await c.query('COMMIT');
 }catch(e){await c.query('ROLLBACK');throw e;}
 console.log('SCB enrichment cursor '+next+'; saved '+(await c.query('SELECT count(*) FROM public.orgindex_company_details')).rows[0].count);
 if(!b.pagination.hasMore)break;await new Promise(r=>setTimeout(r,500));
 }
 }finally{await c.end();}
}
run().catch(e=>{console.error('SCB enrichment failed: '+String(e.message).slice(0,150));process.exitCode=1;});
