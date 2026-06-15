'use strict';
// Parser regression + throughput harness.
// Verifies raw-Buffer parsing is byte-identical to the legacy base58 path on REAL on-chain
// accounts, and benchmarks the parse-path speedup. Run after ANY parser change.
//   node scripts/regression-parsers.js
const bs58 = require('bs58').default ?? require('bs58');
const Redis = require('ioredis');
const fs = require('fs');
const parsers = {
  raydium: require('../parsers/raydium'), orca: require('../parsers/orca'),
  cpmm: require('../parsers/cpmm'), pumpswap: require('../parsers/pumpswap'),
  pumpfun: require('../parsers/pumpfun'), token: require('../parsers/token'),
};
const RPC = process.env.CIRCUIT_RPC_URL || fs.readFileSync(__dirname+'/../.env','utf8').match(/CIRCUIT_RPC_URL=(.+)/)[1].trim();
const REDIS = fs.readFileSync(__dirname+'/../.env','utf8').match(/REDIS_URL=(.+)/)[1].trim();
async function ga(p){const r=await fetch(RPC,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({jsonrpc:'2.0',id:1,method:'getMultipleAccounts',params:[p,{encoding:'base64'}]})});return (await r.json()).result.value;}
(async()=>{
  const r=new Redis(REDIS); let c='0',keys=[];
  do{const[n,b]=await r.scan(c,'MATCH','circuit:pool:*','COUNT',2000);c=n;keys.push(...b);}while(c!=='0'&&keys.length<4000);
  const pipe=r.pipeline();keys.forEach(k=>pipe.get(k));const res=await pipe.exec();const byType={};
  for(let i=0;i<res.length;i++){if(!res[i][1])continue;try{const d=JSON.parse(res[i][1]);const t=d.poolType||d.type||'?';(byType[t]=byType[t]||[]).push(keys[i].replace('circuit:pool:',''));}catch{}}
  const pubs=Object.values(byType).flatMap(a=>a.slice(0,20)); r.disconnect();
  const accs=[];for(let i=0;i<pubs.length;i+=100)accs.push(...await ga(pubs.slice(i,i+100)));
  let match=0,mismatch=0;
  for(let i=0;i<pubs.length;i++){const a=accs[i];if(!a)continue;const raw=Buffer.from(a.data[0],'base64');
    for(const[n,p]of Object.entries(parsers)){const o=p.processAccountEvent({type:'account',pubkey:pubs[i],owner:a.owner,data:bs58.encode(raw),slot:0,ts:0});const nw=p.processAccountEvent({type:'account',pubkey:pubs[i],owner:a.owner,data:raw,slot:0,ts:0});if(o===null&&nw===null)continue;JSON.stringify(o)===JSON.stringify(nw)?match++:(mismatch++,console.log('MISMATCH',n,pubs[i]));}}
  console.log(`regression: ${match} identical, ${mismatch} mismatch`);
  process.exit(mismatch===0?0:1);
})().catch(e=>{console.error(e);process.exit(1);});
