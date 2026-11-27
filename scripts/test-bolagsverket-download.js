// One-company download smoke test; no database writes.
const {writeFile}=require('node:fs/promises');
async function main(){
 const orgnr=process.argv[2];
 if(!/^\d{10}$/.test(orgnr||'')) throw Error('Supply a 10-digit company registration number');
 const id=process.env.BOLAGSVERKET_CLIENT_ID, secret=process.env.BOLAGSVERKET_CLIENT_SECRET;
 if(!id||!secret) throw Error('Missing API credentials');
 const tokenResponse=await fetch('https://portal.api.bolagsverket.se/oauth2/token',{method:'POST',redirect:'error',headers:{Authorization:'Basic '+Buffer.from(id+':'+secret).toString('base64'),'Content-Type':'application/x-www-form-urlencoded'},body:new URLSearchParams({grant_type:'client_credentials',scope:'vardefulla-datamangder:read'}),signal:AbortSignal.timeout(20000)});
 if(!tokenResponse.ok) throw Error('Token HTTP '+tokenResponse.status);
 const {access_token}=await tokenResponse.json();
 if(!access_token) throw Error('Missing access token');
 const base='https://gw.api.bolagsverket.se/vardefulla-datamangder/v1';
 const headers={Authorization:'Bearer '+access_token};
 const list=await fetch(base+'/dokumentlista',{method:'POST',headers:{...headers,'Content-Type':'application/json'},body:JSON.stringify({identitetsbeteckning:orgnr}),redirect:'error',signal:AbortSignal.timeout(30000)});
 if(!list.ok) throw Error('Document list HTTP '+list.status);
 const data=await list.json();
 const docs=[]; function walk(v){if(!v||typeof v!=='object')return;if(v.dokumentId||v.dokumentid)docs.push(v);for(const x of Object.values(v))walk(x)} walk(data);
 if(!docs.length) throw Error('No document IDs returned');
 const doc=docs[0].dokumentId||docs[0].dokumentid;
 const r=await fetch(base+'/dokument/'+encodeURIComponent(doc),{headers,redirect:'error',signal:AbortSignal.timeout(60000)});
 if(!r.ok) throw Error('Document HTTP '+r.status);
 const reader=r.body.getReader(), chunks=[];let n=0;
 while(true){const {done,value}=await reader.read();if(done)break;n+=value.length;if(n>25*1024*1024){await reader.cancel();throw Error('Test file exceeds 25 MiB')}chunks.push(Buffer.from(value));}
 const bytes=Buffer.concat(chunks);
 if(bytes.length<4||bytes.readUInt32LE(0)!==0x04034b50)throw Error('Response is not a ZIP archive');
 await writeFile('bolagsverket-test.zip',bytes,{flag:'wx',mode:0o600});
 console.log('Downloaded one ZIP:',bytes.length,'bytes; no database writes');
}
main().catch(e=>{console.error(e.message);process.exitCode=1});
