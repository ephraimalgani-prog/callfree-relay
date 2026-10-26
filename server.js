// Call Free relay server
// بروتوكول بسيط عبر WebSocket:
//  نصي (JSON):
//   -> {t:"hello", id, ph, name, token}        تسجيل الدخول
//   <- {t:"hello_ok"}
//   -> {t:"presence", hashes:[ph,...]}         من المتصل من جهات اتصالي؟
//   <- {t:"presence", online:[{id,ph,name}]}
//   -> {t:"to", to:<id>, msg:{...}}            تمرير إشارة/رسالة/جزء ملف
//   <- {t:"from", from:<id>, ph, name, msg}    (الخادم يختم هوية المرسل بنفسه)
//  ثنائي (صوت): [1 بايت طول المعرّف][المعرّف][بيانات]  ->  يُمرَّر مع معرّف المرسل
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const TOKEN = process.env.APP_TOKEN || '';          // اختياري لكنه موصى به
const MAX_FRAME = 256 * 1024;                        // أقصى حجم إطار
const MAX_MSGS_PER_SEC = 200;                        // حماية من الإغراق

const clients = new Map();                           // id -> {ws, ph, name}

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end(`Call Free relay OK — online: ${clients.size}\n`);
});
const wss = new WebSocketServer({ server, maxPayload: MAX_FRAME });

function sendJson(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

wss.on('connection', (ws) => {
  let me = null;
  let count = 0;
  const rate = setInterval(() => { count = 0; }, 1000);
  const authTimer = setTimeout(() => { if (!me) ws.close(); }, 10000);
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data, isBinary) => {
    if (++count > MAX_MSGS_PER_SEC) return;
    if (isBinary) {
      if (!me) return;
      const idLen = data[0];
      const to = data.subarray(1, 1 + idLen).toString('utf8');
      const target = clients.get(to);
      if (!target) return;
      const fromBuf = Buffer.from(me.id, 'utf8');
      target.ws.send(Buffer.concat([Buffer.from([fromBuf.length]), fromBuf, data.subarray(1 + idLen)]), { binary: true });
      return;
    }
    let m;
    try { m = JSON.parse(data.toString('utf8')); } catch { return; }

    if (m.t === 'hello') {
      if (TOKEN && m.token !== TOKEN) { sendJson(ws, { t: 'error', e: 'bad_token' }); return ws.close(); }
      if (typeof m.id !== 'string' || m.id.length < 8 || m.id.length > 64) return ws.close();
      const old = clients.get(m.id);
      if (old && old.ws !== ws) old.ws.close();      // جلسة جديدة تستبدل القديمة
      me = { id: m.id, ph: String(m.ph || ''), name: String(m.name || '').slice(0, 40), ws };
      clients.set(me.id, me);
      clearTimeout(authTimer);
      sendJson(ws, { t: 'hello_ok' });
      return;
    }
    if (!me) return;

    if (m.t === 'presence' && Array.isArray(m.hashes)) {
      const wanted = new Set(m.hashes.slice(0, 5000).map(String));
      const online = [];
      for (const c of clients.values()) {
        if (c.id !== me.id && wanted.has(c.ph)) online.push({ id: c.id, ph: c.ph, name: c.name });
      }
      sendJson(ws, { t: 'presence', online });
      return;
    }
    if (m.t === 'to' && typeof m.to === 'string' && m.msg) {
      const target = clients.get(m.to);
      if (target) sendJson(target.ws, { t: 'from', from: me.id, ph: me.ph, name: me.name, msg: m.msg });
      return;
    }
  });

  ws.on('close', () => {
    clearInterval(rate);
    clearTimeout(authTimer);
    if (me && clients.get(me.id)?.ws === ws) clients.delete(me.id);
  });
  ws.on('error', () => {});
});

// إغلاق الاتصالات الميتة
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => console.log(`Call Free relay listening on :${PORT}`));
