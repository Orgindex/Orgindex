const { Pool } = require('pg');
let pool;
const getPool = () => pool || (pool = new Pool({connectionString:process.env.DATABASE_URL,max:2,connectionTimeoutMillis:5000,idleTimeoutMillis:10000,statement_timeout:8000}));
async function getAnnualAccounts(orgnr) {
 try {
  const { rows } = await getPool().query(`
   SELECT period_start::text AS "periodStart", period_end::text AS "periodEnd",
          net_revenue::text AS "netRevenue", net_profit::text AS "netProfit"
   FROM orgindex_annual_accounts
   WHERE orgnr = $1 AND reporting_scope = 'company' AND currency = 'SEK'
     AND (net_revenue IS NOT NULL OR net_profit IS NOT NULL)
   ORDER BY period_end DESC, period_start DESC
   LIMIT 10
  `, [orgnr]);
  return rows;
 } catch (error) {
  console.error('Annual accounts lookup failed', error.code || error.name);
  return [];
 }
}

module.exports=async(req,res)=>{res.setHeader('Cache-Control','no-store');if(req.method!=='GET')return res.status(405).json({error:'Metoden stöds inte'});const q=typeof req.query.orgnr==='string'?req.query.orgnr.trim():'';if(!/^\d{6}-?\d{4}$/.test(q))return res.status(400).json({error:'Ange tio siffror'});const n=q.replace('-','');if(+n[2]<2)return res.status(400).json({error:'Personnummer och enskilda firmor stöds inte'});if([...n].reduce((s,c,i)=>{const v=+c*(i%2?1:2);return s+Math.floor(v/10)+v%10},0)%10)return res.status(400).json({error:'Ogiltigt organisationsnummer'});try {
 const result = await getPool().query('SELECT c.name,c.orgnr,c.source,d.address,d.postal_code,d.city,d.sni,d.legal_form,d.employee_class,d.company_status,d.f_tax_status,d.vat_status,d.employer_status,d.refreshed_at FROM orgindex_companies c LEFT JOIN orgindex_company_details d USING(orgnr) WHERE c.orgnr=$1 LIMIT 1',[n]);
 const d=result.rows[0];
 if(d){res.setHeader('Cache-Control','public, s-maxage=300');return res.json({name:d.name,orgnr:d.orgnr,source:d.source||'SCB',address:d.address||'',coAddress:'',postalCode:d.postal_code||'',city:d.city||'',sni:d.sni,legalForm:d.legal_form,employeeClass:d.employee_class,companyStatus:d.company_status,fTaxStatus:d.f_tax_status,vatStatus:d.vat_status,employerStatus:d.employer_status,refreshedAt:d.refreshed_at,annualAccounts:await getAnnualAccounts(n)});}
} catch(e){console.error('Company database lookup failed',e.code||e.name);}
try{const k=process.env.SCB_API_KEY;if(!k)return res.status(503).json({error:'Registret är inte konfigurerat'});const r=await fetch('https://apiafr.scb.se/v1/juridiskaenheter/16'+n+'/full',{headers:{'X-API-Key':k,Accept:'application/json'},signal:AbortSignal.timeout(10000)});if(r.status===404)return res.status(404).json({error:'Företaget hittades inte'});if(!r.ok)return res.status(502).json({error:'SCB är tillfälligt otillgängligt'});const raw=await r.json(),d=Array.isArray(raw)?raw[0]:raw;if(!d||d.orgNr!==n||typeof d.namn!=='string')return res.status(502).json({error:'Svaret kunde inte verifieras'});res.setHeader('Cache-Control','public, s-maxage=3600');const a=d.postAdress||d.postadress||d.PostAdress||{};const clean=v=>typeof v==='string'?v.trim().slice(0,512):'';return res.json({name:d.namn,orgnr:n,source:'SCB',address:clean(a.gatuadress||a.gatuAdress||a.Gatuadress),coAddress:clean(a.coAdress||a.CoAdress),postalCode:clean(a.postNr||a.postnr||a.PostNr),city:clean(a.postOrt||a.postort||a.PostOrt)});}catch{return res.status(502).json({error:'Kunde inte nå SCB'});}};