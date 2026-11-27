const { Pool } = require('pg');

let pool;

module.exports = async (req, res) => {
 if (req.method !== 'GET') {
  res.setHeader('Allow', 'GET');
  return res.status(405).json({error: 'Metoden stöds inte'});
 }
 res.setHeader('Cache-Control', 'no-store');
 try {
  pool ||= new Pool({
   connectionString: process.env.DATABASE_URL,
   max: 2,
   connectionTimeoutMillis: 5000,
   statement_timeout: 8000
  });
  const {rows: [row]} = await pool.query(`
   SELECT
    (SELECT count(*) FROM public.orgindex_companies) AS total_companies,
    count(DISTINCT orgnr) AS companies_with_accounts,
    max(imported_at) AS latest_import
   FROM public.orgindex_annual_accounts
   WHERE source = 'Bolagsverket' AND reporting_scope = 'company'
     AND currency = 'SEK' AND (net_revenue IS NOT NULL OR net_profit IS NOT NULL)
  `);
  res.setHeader('Cache-Control', 'public, s-maxage=60');
  return res.json({
   totalCompanies: Number(row.total_companies),
   companiesWithAccounts: Number(row.companies_with_accounts),
   latestImport: row.latest_import
  });
 } catch (error) {
  console.error('Import status lookup failed', error.code || error.name);
  return res.status(503).json({error: 'Importstatus är tillfälligt otillgänglig'});
 }
};
