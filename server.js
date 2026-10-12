// Lagos Striker server: Vatar purchase checks, cloud save, leaderboard, friend challenges, live duels (incl. Vatar matches).
const http=require('http'),fs=require('fs'),crypto=require('crypto'),{WebSocketServer}=require('ws');
const PORT=process.env.PORT||8080,DB=process.env.DB_FILE||'db.json';
const HOST=process.env.VATAR_HOST||'https://lagoslife.app',API=HOST+'/api/games/v1';
const SECRET=process.env.VATAR_SECRET||'',WHSEC=process.env.VATAR_WEBHOOK_SECRET||'',RES=process.env.VATAR_RESULTS_SECRET||'';
let db={players:{},challenges:{},ent:{},save:{}};try{db=Object.assign(db,JSON.parse(fs.readFileSync(DB)))}catch(e){}
const save=()=>{try{fs.writeFileSync(DB,JSON.stringify(db))}catch(e){}};setInterval(save,15000);process.on('SIGTERM',()=>{save();process.exit(0)});
const cors={'Access-Control-Allow-Origin':'*','Access-Control-Allow-Headers':'content-type','Access-Control-Allow-Methods':'GET,POST,OPTIONS'};
const clean=n=>String(n||'').replace(/[^\w .-]/g,'').trim().slice(0,12)||'Player';
const rawb=r=>new Promise(res=>{const c=[];let n=0;r.on('data',x=>{n+=x.length;if(n>2e5)r.destroy();else c.push(x)});r.on('end',()=>res(Buffer.concat(c)))});
const body=async r=>{try{return JSON.parse((await rawb(r)).toString()||'{}')}catch(e){return{}}};
const out=(res,o,c=200)=>{res.writeHead(c,{...cors,'Content-Type':'application/json'});res.end(JSON.stringify(o))};
const rid=()=>Math.random().toString(36).slice(2,8).toUpperCase();
const stars=v=>Math.min(3,Math.max(0,+v|0));
const va=async(path,b)=>{try{const r=await fetch(API+path,{method:b?'POST':'GET',body:b&&JSON.stringify(b),headers:{Authorization:'Bearer '+SECRET,'Content-Type':'application/json'}});return{http:r.status,...(await r.json().catch(()=>({})))}}catch(e){return{http:0}}};
const signed=(raw,h)=>{if(!WHSEC)return false;const w=Buffer.from('sha256='+crypto.createHmac('sha256',WHSEC).update(raw).digest('hex')),g=Buffer.from(String(h||''));return w.length===g.length&&crypto.timingSafeEqual(w,g)};
const srv=http.createServer(async(req,res)=>{
 if(req.method==='OPTIONS')return out(res,{});
 const u=new URL(req.url,'http://x'),p=u.pathname,post=req.method==='POST';
 // Vatar: verify the player's session and the purchase with Vatar, grant once, mark delivered
 if(p==='/api/claim'&&post){const b=await body(req),me=await va('/session/verify',{session:b.session}),pu=await va('/purchases/'+encodeURIComponent(String(b.purchaseId||'')));
  if(me.http!==200||pu.http!==200||pu.playerId!==me.playerId)return out(res,{error:'forbidden'},403);if(pu.status==='refunded')return out(res,{error:'refunded'},409);
  const e=db.ent[me.playerId]=db.ent[me.playerId]||{items:[],pids:[]};if(!e.pids.includes(pu.id)){e.pids.push(pu.id);if(!e.items.includes(pu.productId))e.items.push(pu.productId);save()}
  await va('/purchases/'+pu.id+'/deliver',{});return out(res,{items:e.items})}
 if(p==='/api/me'&&post){const b=await body(req),me=await va('/session/verify',{session:b.session});if(me.http!==200)return out(res,{error:'forbidden'},403);
  return out(res,{items:(db.ent[me.playerId]||{}).items||[],stars:(db.save[me.playerId]||{}).stars||[]})}
 if(p==='/api/save'&&post){const b=await body(req),me=await va('/session/verify',{session:b.session});if(me.http!==200)return out(res,{error:'forbidden'},403);
  const s=db.save[me.playerId]=db.save[me.playerId]||{stars:[]};(Array.isArray(b.stars)?b.stars:[]).slice(0,5000).forEach((v,i)=>{v=stars(v);if(v>(s.stars[i]||0))s.stars[i]=v});return out(res,{ok:1})}
 if(p==='/vatar-webhook'&&post){const raw=await rawb(req);if(!signed(raw,req.headers['x-vatar-signature']))return out(res,{},401);return out(res,{ok:1})}
 // standalone leaderboard and challenges (used outside Lagos Life)
 if(p==='/api/score'&&post){const b=await body(req),n=clean(b.name),l=+b.level;if(!(l>=0&&l<500000))return out(res,{},400);
  const q=db.players[n]=db.players[n]||{name:n,lv:{}};q.lv[l]=Math.max(q.lv[l]||0,stars(b.stars));q.total=Object.values(q.lv).reduce((a,c)=>a+c,0);return out(res,{ok:1,total:q.total})}
 if(p==='/api/leaderboard')return out(res,Object.values(db.players).sort((a,b)=>b.total-a.total).slice(0,25).map(q=>({name:q.name,total:q.total})));
 if(p==='/api/challenge'&&post){const b=await body(req),code=rid();db.challenges[code]={code,level:Math.max(0,+b.level|0),res:[{name:clean(b.name),stars:stars(b.stars),ms:+b.ms|0}]};return out(res,{code})}
 const m=p.match(/^\/api\/challenge\/(\w+)(\/result)?$/);
 if(m){const c=db.challenges[m[1].toUpperCase()];if(!c)return out(res,{},404);
  if(m[2]&&post){const b=await body(req),n=clean(b.name),r=c.res.find(x=>x.name===n),s=stars(b.stars),ms=+b.ms|0;if(!r)c.res.push({name:n,stars:s,ms});else if(s>r.stars||(s===r.stars&&ms<r.ms)){r.stars=s;r.ms=ms}}
  c.res.sort((a,b)=>b.stars-a.stars||a.ms-b.ms);return out(res,c)}
 out(res,{ok:1,game:'lagos-striker'})});
// live duels: both players race the same 3 levels, first to score wins each round
const wss=new WebSocketServer({server:srv});let queue=null;const rooms={},vrooms={};
const tx=(w,o)=>{try{w&&w.readyState===1&&w.send(JSON.stringify(o))}catch(e){}};
async function vres(r,w){if(!r.vid||r.posted||!RES)return;r.posted=1;try{const path='/api/games/v1/matches/'+r.vid+'/result',b=JSON.stringify({winner:w<0?'draw':r.p[w].pid}),ts=String(Math.floor(Date.now()/1000)),sha=crypto.createHash('sha256').update(b).digest('hex'),
 sig='v1='+crypto.createHmac('sha256',RES).update(['POST',path,ts,r.nonce,sha].join('\n')).digest('hex');
 await fetch(new URL(path,HOST),{method:'POST',body:b,headers:{Authorization:'Bearer '+SECRET,'Content-Type':'application/json','X-Vatar-Timestamp':ts,'X-Vatar-Signature':sig}})}catch(e){console.log('result post failed',e.message)}}
function pair(a,b){const lv=[0,1,2,3,4,5,6,7].sort(()=>Math.random()-.5).slice(0,3).sort((x,y)=>x-y);
 const r={p:[a,b],lv,round:0,sc:[0,0],done:[0,0],over:0};a.room=b.room=r;a.idx=0;b.idx=1;
 [a,b].forEach(w=>tx(w,{t:'match',opp:(w===a?b:a).name||'Player',levels:lv,idx:w.idx}));return r}
function endRound(r,win){if(r.over)return;if(win>=0)r.sc[win]++;r.round++;r.done=[0,0];
 const fin=r.round>=3||r.sc[0]>=2||r.sc[1]>=2;
 if(fin){r.over=1;const w=r.sc[0]===r.sc[1]?-1:r.sc[0]>r.sc[1]?0:1;r.p.forEach(x=>tx(x,{t:'end',sc:r.sc,winner:w}));vres(r,w)}
 else r.p.forEach(x=>tx(x,{t:'round',win,sc:r.sc,round:r.round}))}
wss.on('connection',ws=>{ws.name='Player';ws.on('message',async d=>{let m;try{m=JSON.parse(d)}catch(e){return}
 if(m.t==='hello')ws.name=clean(m.name);
 else if(m.t==='vmatch'&&!ws.room&&!ws.pid){const me=await va('/session/verify',{session:m.session}),mt=await va('/matches/'+encodeURIComponent(String(m.matchId||'')));
  if(me.http!==200||mt.http!==200||mt.status!=='playing'||!(mt.players||[]).some(q=>q.playerId===me.playerId))return tx(ws,{t:'err',msg:'Match not found'});
  ws.pid=me.playerId;ws.name=clean(me.name);const v=vrooms[mt.id]=(vrooms[mt.id]||[]).filter(x=>x.readyState===1&&x.pid!==ws.pid);v.push(ws);
  if(v.length===2){const r=pair(v[0],v[1]);r.vid=mt.id;r.nonce=mt.nonce;delete vrooms[mt.id]}else tx(ws,{t:'wait'})}
 else if(m.t==='queue'&&!ws.room){if(queue&&queue!==ws&&queue.readyState===1){const q=queue;queue=null;pair(q,ws)}else{queue=ws;tx(ws,{t:'wait'})}}
 else if(m.t==='create'&&!ws.room){const c=rid();rooms[c]=ws;ws.code=c;tx(ws,{t:'code',code:c})}
 else if(m.t==='join'&&!ws.room){const c=String(m.code||'').toUpperCase(),h=rooms[c];if(h&&h!==ws&&h.readyState===1){delete rooms[c];pair(h,ws)}else tx(ws,{t:'err',msg:'Code not found'})}
 else if(m.t==='pos'&&ws.room&&!ws.room.over){tx(ws.room.p[1-ws.idx],{t:'opp',x:+m.x||0,y:+m.y||0})}
 else if(m.t==='done'&&ws.room&&!ws.room.over){const r=ws.room;if(+m.round!==r.round||r.done[ws.idx])return;r.done[ws.idx]=1;
  if(m.scored)endRound(r,ws.idx);else if(r.done[0]&&r.done[1])endRound(r,-1)}});
 ws.on('close',()=>{if(queue===ws)queue=null;if(ws.code)delete rooms[ws.code];const r=ws.room;if(r&&!r.over){r.over=1;const o=r.p[1-ws.idx];if(o)tx(o,{t:'end',sc:r.sc,winner:o.idx,dc:1});vres(r,1-ws.idx)}})});
srv.listen(PORT,()=>console.log('Lagos Striker server on '+PORT));
