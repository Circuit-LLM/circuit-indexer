#!/usr/bin/env node
// acct-sizes.js — READ-ONLY: real on-chain data length per program, to design slice tiers correctly.
// Confirms the accountsDataSlice rule: an account is dropped if data.len < (offset+length).
'use strict';
(function loadEnv(){const fs=require('fs'),path=require('path');try{for(const l of fs.readFileSync(path.join(__dirname,'..','.env'),'utf8').split('\n')){const s=l.trim();if(!s||s[0]==='#')continue;const i=s.indexOf('=');if(i<0)continue;const k=s.slice(0,i).trim();let v=s.slice(i+1).trim();if(!(k in process.env))process.env[k]=v;}}catch{}})();
const { rpcCall } = require('../lib/rpc-client');
const OWNERS = {
  'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK': 'clmm',
  'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc': 'orca',
  'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA': 'pumpswap',
  'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C': 'cpmm',
  '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P': 'pumpfun',
  'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA': 'token',
  'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb': 'token',
};
async function getMany(addrs){const out=[];for(let i=0;i<addrs.length;i+=100){const r=await rpcCall('getMultipleAccounts',[addrs.slice(i,i+100),{encoding:'base64'}]);(r?.value||[]).forEach((v,j)=>{if(v?.data?.[0])out.push({owner:v.owner,len:Buffer.from(v.data[0],'base64').length});});}return out;}
(async()=>{
  const Redis=require('ioredis');const r=new Redis(process.env.REDIS_URL,{lazyConnect:true});await r.connect();
  const scan=async(p,cap)=>{const ks=[];let c='0';do{const[n,b]=await r.scan(c,'MATCH',p,'COUNT',1000);c=n;ks.push(...b);if(ks.length>=cap)break;}while(c!=='0');return ks;};
  const pools=(await scan('circuit:pool:*',4000)).map(k=>k.slice('circuit:pool:'.length));
  const vaults=Object.keys(await r.hgetall('circuit:vault-registry').catch(()=>({}))).slice(0,400);
  const mints=(await scan('circuit:mint:*',400)).map(k=>k.slice('circuit:mint:'.length));
  await r.quit();
  const samp=(a,n)=>a.length<=n?a:a.filter((_,i)=>i%Math.ceil(a.length/n)===0).slice(0,n);
  const accts=[...await getMany(samp(pools,1500)),...await getMany(vaults),...await getMany(mints)];
  const byType={};
  for(const a of accts){const t=OWNERS[a.owner]||'other';(byType[t]||={lens:new Set(),n:0,min:1e9,max:0}).n++;byType[t].lens.add(a.len);byType[t].min=Math.min(byType[t].min,a.len);byType[t].max=Math.max(byType[t].max,a.len);}
  console.log('program   count   min    max    distinct-sizes');
  for(const[t,s]of Object.entries(byType).sort((a,b)=>b[1].n-a[1].n)){
    const sizes=[...s.lens].sort((a,b)=>a-b);
    console.log(`${t.padEnd(10)}${String(s.n).padEnd(7)} ${String(s.min).padEnd(6)} ${String(s.max).padEnd(6)} [${sizes.slice(0,8).join(',')}${sizes.length>8?',…':''}]`);
  }
})().catch(e=>{console.error(e);process.exit(1);});
