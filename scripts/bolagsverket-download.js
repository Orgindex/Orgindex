const {createHash}=require('node:crypto');
async function downloadDocument(sourceUrl, token){
 const url=new URL(sourceUrl);
 const api=url.origin==='https://gw.api.bolagsverket.se'&&/^\/vardefulla-datamangder\/v1\/dokument\/[^/]+$/.test(url.pathname);
 const bulk=url.origin==='https://vardefulla-datamangder.bolagsverket.se'&&/^\/arsredovisningar-bulkfiler\/arsredovisningar\/\d{4}\/\d{2}_\d+\.zip$/.test(url.pathname);
 if((!api&&!bulk)||url.search||url.hash||url.username||url.password)throw Error('invalid_document_url');
 const response=await fetch(url,{headers:api?{Authorization:'Bearer '+token}:{},redirect:'error',signal:AbortSignal.timeout(25000)});
 if(!response.ok)throw Error('document_http_'+response.status);
 const limit=8*1024*1024;
 const reader=response.body.getReader();const chunks=[];let size=0;
 try{while(true){const {done,value}=await reader.read();if(done)break;size+=value.length;if(size>limit)throw Error('document_too_large');chunks.push(Buffer.from(value));}}finally{await reader.cancel().catch(()=>{});}
 const bytes=Buffer.concat(chunks);
 if(bytes.length<4||bytes.readUInt32LE(0)!==0x04034b50)throw Error('not_zip');
 return {bytes,size,sha256:createHash('sha256').update(bytes).digest('hex')};
}
module.exports={downloadDocument};
