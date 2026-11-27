const {Pool}=require('pg');
let pool;
module.exports=async(req,res)=>{
res.setHeader('Cache-Control','no-store');
if(req.method!=='GET')return res.status(405).end();
try{
pool ||= new Pool({connectionString:process.env.DATABASE_URL,max:1,statement_timeout:10000});
const {rows:[r]}=await pool.query("SELECT count(*) FILTER (WHERE source='Bolagsverket')::int AS imported,count(*)::int AS total FROM public.orgindex_companies");
return res.json({...r,sourceTotal:1949246,checkedAt:new Date().toISOString(),metric:'new_company_records_only'});
}catch(e){return res.status(503).json({error:'status_unavailable'});}
};