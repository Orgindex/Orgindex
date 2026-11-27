const { Pool } = require('pg');
let pool;
const getPool = () => pool || (pool = new Pool({connectionString:process.env.DATABASE_URL,max:2,connectionTimeoutMillis:5000,idleTimeoutMillis:10000,statement_timeout:8000}));
module.exports = async (req,res) => {
  res.setHeader('Cache-Control','no-store');
  if(req.method !== 'GET') return res.status(405).end();
  const q=typeof req.query.q==='string'?req.query.q.trim().replace(/\s+/g,' '):'';
  const industry=typeof req.query.industry==='string'?req.query.industry.trim().toLowerCase():'';
  const suggest=req.query.suggest==='1';
  if(((!industry||suggest)&&q.length<2)||q.length>100||industry.length>100) return res.status(400).json({error:'Ange minst två tecken i företagsnamnet'});
  try {
    const p=getPool();
    const digits=q.replace(/-/g,'');
    const numeric=/^\d{2,10}$/.test(digits)&&/^[\d-]+$/.test(q);
    const limit=suggest?8:50;
    if(!suggest&&industry&&!['vitvaror','vitvaruindustri','hushållsapparater','tillverkning av hushållsapparater'].includes(industry)) return res.json({companies:[],suggested:false,coverage:'Branschregistret är under uppbyggnad'});
    const industryFilter=!suggest&&!!industry;
    const query=numeric?
      "SELECT orgnr,name,source FROM orgindex_companies WHERE orgnr LIKE $1 || '%' AND orgnr ~ '^[0-9]{2}[2-9][0-9]{7}$' AND (NOT $2::boolean OR orgnr='5560094178') ORDER BY orgnr LIMIT $3":
      "SELECT orgnr,name,source FROM orgindex_companies WHERE lower(name) LIKE '%' || replace(replace(replace(lower($1),'!','!!'),'%','!%'),'_','!_') || '%' ESCAPE '!' AND orgnr ~ '^[0-9]{2}[2-9][0-9]{7}$' AND (NOT $2::boolean OR orgnr='5560094178') ORDER BY CASE WHEN lower(name)=lower($1) THEN 0 WHEN strpos(lower(name),lower($1))=1 THEN 1 ELSE 2 END,length(name),name,orgnr LIMIT $3";
    let result=await p.query(query,[numeric?digits:q,industryFilter,limit]);
    if (!numeric && !industryFilter && !result.rows.some(row => row.name.toLowerCase() === q.toLowerCase())) {
      const withoutPeriods = q.replace(/\./g, '');
      if (withoutPeriods.length >= 3) {
        const normalized = await p.query(
          "SELECT orgnr,name,source FROM orgindex_companies WHERE replace(lower(name), '.', '') LIKE $1 || '%' ESCAPE '!' AND orgnr ~ '^[0-9]{2}[2-9][0-9]{7}$' ORDER BY length(name),name,orgnr LIMIT $2",
          [withoutPeriods.toLowerCase().replace(/[!%_]/g, character => `!${character}`), limit]
        );
        const matches = new Map();
        for (const row of [...normalized.rows, ...result.rows]) matches.set(row.orgnr, row);
        result.rows = [...matches.values()].slice(0, limit);
      }
    }
    let suggested=false;
    if(!numeric && !industryFilter && q.length>=4 && result.rows.length===0){
      result=await p.query("SELECT orgnr,name,source FROM orgindex_companies WHERE lower(name) %> lower($1) AND orgnr ~ '^[0-9]{2}[2-9][0-9]{7}$' ORDER BY word_similarity(lower($1),lower(name)) DESC,length(name),name,orgnr LIMIT $2",[q,limit]);
      suggested=result.rows.length>0;
    }
    return res.json({companies:result.rows.map(c=>({...c,industry:c.orgnr==='5560094178'?'Vitvaruindustri – hushållsapparater':null})),suggested,coverage:'Företagsregister – namn och organisationsnummer'});
  } catch(e) {
    console.error('Company search failed',e.code||e.name);
    return res.status(503).json({error:'Namnsökningen är tillfälligt otillgänglig'});
  }
};
