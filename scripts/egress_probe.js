'use strict';
// Read-only egress probe: subscribe with the current request, count bytes + events by program
// for N seconds, then disconnect. No writes. Measures the TRUE full-firehose volume.
const fs = require('fs');
const env = fs.readFileSync('/home/watchtower/circuit-indexer/.env','utf8');
const endpoint = env.match(/GEYSER_ENDPOINT=(.+)/)[1].trim();
const token    = env.match(/GEYSER_TOKEN=(.+)/)[1].trim();
const bs58 = require('bs58').default ?? require('bs58');
const yg = require('@triton-one/yellowstone-grpc');
const Client = yg.default ?? yg;
const { SubscribeRequest } = require('@triton-one/yellowstone-grpc/dist/grpc/geyser');
const PROGRAMS = ['CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK','CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C','whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc','6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P','pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA','TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA','TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'];
const NAME = {'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA':'TOKEN','TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb':'TOKEN-2022','6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P':'pump.fun','pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA':'pumpswap','CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C':'ray-cpmm','CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK':'ray-clmm','whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc':'orca','LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo':'meteora'};
const DUR = 45000;
(async()=>{
  const client = new Client(endpoint, token, {'grpc.max_receive_message_length':64*1024*1024});
  const stream = await client.subscribe();
  const acctBytesByOwner={}, acctCntByOwner={};
  let txCnt=0, txBytes=0, slotCnt=0, acctCnt=0, acctBytes=0;
  stream.on('data', d=>{
    if(d.account){const a=d.account.account; if(!a)return; const o=bs58.encode(Buffer.from(a.owner)); const len=(a.data?.length||0)+64; acctCnt++; acctBytes+=len; acctBytesByOwner[o]=(acctBytesByOwner[o]||0)+len; acctCntByOwner[o]=(acctCntByOwner[o]||0)+1;}
    else if(d.transaction){txCnt++; const tx=d.transaction.transaction; const sz=(tx?.transaction?.message?.accountKeys?.length||0)*32 + JSON.stringify(tx?.meta?.preTokenBalances||[]).length + JSON.stringify(tx?.meta?.postTokenBalances||[]).length + 200; txBytes+=sz;}
    else if(d.slot){slotCnt++;}
  });
  stream.on('error',e=>{console.error('stream err',e.message);});
  const req=SubscribeRequest.fromPartial({accounts:{c:{account:[],owner:PROGRAMS,filters:[]}},transactions:{c:{vote:false,failed:false,accountInclude:PROGRAMS,accountExclude:[],accountRequired:[]}},slots:{c:{}},blocks:{},blocksMeta:{},commitment:1,accountsDataSlice:[]});
  await new Promise((res,rej)=>stream.write(req,e=>e?rej(e):res()));
  console.log('probe running '+(DUR/1000)+'s (read-only, no writes)...');
  await new Promise(r=>setTimeout(r,DUR));
  stream.cancel?.(); try{stream.end?.();}catch{}
  const sec=DUR/1000;
  const totalBytes=acctBytes+txBytes;
  const mbps=totalBytes/sec/1e6;
  console.log('\n=== FULL-FIREHOSE EGRESS (raw data, pre-compression) ===');
  console.log('  total: '+(totalBytes/1e6).toFixed(1)+' MB in '+sec+'s = '+mbps.toFixed(2)+' MB/s = '+(mbps*86400/1000).toFixed(1)+' GB/day');
  console.log('  events/sec: accounts '+Math.round(acctCnt/sec)+' | txns '+Math.round(txCnt/sec)+' | slots '+Math.round(slotCnt/sec));
  console.log('  bytes split: accounts '+(acctBytes/1e6).toFixed(1)+'MB ('+Math.round(100*acctBytes/totalBytes)+'%) | txns '+(txBytes/1e6).toFixed(1)+'MB ('+Math.round(100*txBytes/totalBytes)+'%)');
  console.log('\n  account bytes by program (where the waste is):');
  Object.entries(acctBytesByOwner).sort((a,b)=>b[1]-a[1]).forEach(([o,b])=>console.log('    '+(NAME[o]||o.slice(0,8)).padEnd(11)+(b/1e6).toFixed(1)+'MB  '+acctCntByOwner[o]+' events'));
  process.exit(0);
})().catch(e=>{console.error('probe failed:',e.message);process.exit(1);});
