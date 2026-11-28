const verifiedContacts = require('../data/verified-company-contacts.json');

function publicHttpUrl(value) {
 if (typeof value !== 'string' || !value.trim() || value.length > 2048) return null;
 try {
  const url = new URL(value.trim());
  return ['https:', 'http:'].includes(url.protocol) && url.hostname.includes('.')
   && !url.username && !url.password ? url.href : null;
 } catch { return null; }
}
function dateText(value) {
 if (!value) return null;
 const date = new Date(value);
 return Number.isNaN(date.getTime()) ? null : date.toISOString().slice(0, 10);
}
function contactInfoFromRow(orgnr, row) {
 const fallback = verifiedContacts[orgnr] || {};
 if (!row) return {
  phone: null, phoneUrl: null, website: null, sources: [], status: 'not_loaded',
  ...fallback,
  status: fallback.phone || fallback.website ? 'available' : 'not_loaded'
 };
 const phoneSource = publicHttpUrl(row.phone_source_url);
 const websiteSource = publicHttpUrl(row.website_source_url);
 const phoneValid = ['registry', 'verified_site'].includes(row.phone_origin)
  && typeof row.phone === 'string' && row.phone.trim()
  && /^tel:\+?[0-9]{3,15}$/.test(row.phone_uri || '')
  && phoneSource && row.phone_source_name && row.phone_verified_at;
 const website = ['registry', 'verified_site'].includes(row.website_origin)
  && websiteSource && row.website_source_name && row.website_verified_at
  ? publicHttpUrl(row.website_url) : null;
 const result = {
  phone: phoneValid ? row.phone : null,
  phoneUrl: phoneValid ? row.phone_uri : null,
  website,
  sources: [],
  status: phoneValid || website ? 'available' : row.scb_checked_at ? 'checked' : 'not_loaded'
 };
 for (const field of ['postalAddress', 'visitingAddress', 'deliveryAddress']) {
  if (fallback[field]) result[field] = fallback[field];
 }
 const entries = [
  phoneValid && {name: row.phone_source_name, url: phoneSource, checkedAt: dateText(row.phone_verified_at), fields: ['phone']},
  website && {name: row.website_source_name, url: websiteSource, checkedAt: dateText(row.website_verified_at), fields: ['website']}
 ].filter(Boolean);
 for (const source of entries) {
  const existing = result.sources.find(item => item.url === source.url && item.name === source.name);
  if (existing) {
   existing.fields.push(...source.fields);
   if (source.checkedAt && (!existing.checkedAt || source.checkedAt < existing.checkedAt)) existing.checkedAt = source.checkedAt;
  } else result.sources.push(source);
 }
 return result;
}
async function getCompanyContactInfo(getPool, orgnr) {
 try {
  const {rows} = await getPool().query(
   'SELECT phone,phone_uri,phone_source_name,phone_source_url,phone_verified_at,phone_origin,website_url,website_source_name,website_source_url,website_verified_at,website_origin,scb_checked_at FROM public.orgindex_company_contacts WHERE orgnr=$1 LIMIT 1',
   [orgnr]
  );
  return contactInfoFromRow(orgnr, rows[0]);
 } catch (error) {
  console.error('Company contact lookup failed', error.code || error.name);
  return contactInfoFromRow(orgnr, null);
 }
}
module.exports = {getCompanyContactInfo, contactInfoFromRow, publicHttpUrl};

