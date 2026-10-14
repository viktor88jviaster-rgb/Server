'use strict';
/**
 * Crypto Biker — сервер турниров (WebSocket, без внешних зависимостей).
 * Запуск:  node server.js        (Node.js 18+)
 * Переменные окружения: PORT, BOT_TOKEN, ALLOW_GUESTS (см. README.md)
 */
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

// ---------- настройки ----------
const PORT = +process.env.PORT || 8080;
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const ALLOW_GUESTS = process.env.ALLOW_GUESTS === '1';
const COUNTDOWN_MS = +process.env.COUNTDOWN_MS || 4500;      // время до старта после «матч найден»
const FILL_MS = { 2: +process.env.FILL_MS_2 || 8000, 5: +process.env.FILL_MS_5 || 12000 }; // через сколько добавлять ботов
const RESULT_TIMEOUT_MS = +process.env.RESULT_TIMEOUT_MS || 40000; // сколько ждать остальных после первого финиша
const MAX_RACE_MS = 180000;
const MAX_CONN_PER_IP = +process.env.MAX_CONN_PER_IP || 10;

// Трасса должна совпадать с клиентом (index.html): P=30, PRE=14, RACE_N=400
const P = 30, PRE = 14, RACE_N = 400;
const START_X = (PRE - 3) * P;
const FINISH_X = (PRE + RACE_N) * P;
const MIN_RACE_MS = process.env.MIN_RACE_MS !== undefined ? +process.env.MIN_RACE_MS : ((FINISH_X - START_X) / 1500) * 1000;

const VEHICLES = ['classic', 'bike', 'moped', 'harley', 'sport', 'quad'];
const BOT_BASE = { bike: 250, moped: 285, classic: 330, harley: 335, sport: 370, quad: 310 };
const BOT_NAMES = ['Alex', 'Max', 'Nick', 'Dan', 'Roman', 'Leo', 'Sam', 'Tim', 'Egor', 'Vlad', 'Kirill', 'Art', 'Igor', 'Paul', 'Lukas', 'Hugo', 'Jonas', 'Felix'];

// ---------- минимальный WebSocket (RFC 6455) ----------
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_PAYLOAD = 4096;

function encodeFrame(opcode, payload) {
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | opcode, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | opcode; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([head, payload]);
}

class Conn {
  constructor(sock, ip) {
    this.sock = sock; this.ip = ip; this.buf = Buffer.alloc(0); this.frag = null;
    this.open = true; this.alive = true; this.id = crypto.randomBytes(4).toString('hex');
    this.tokens = 40; this.lastRefill = Date.now();
    this.name = 'Racer'; this.veh = 'classic'; this.uid = null;
    this.mode = 0; this.match = null; this.player = null;
    sock.setNoDelay(true);
    sock.on('data', d => this._data(d));
    sock.on('close', () => this._closed());
    sock.on('error', () => this._closed());
  }
  _data(d) {
    this.alive = true;
    this.buf = Buffer.concat([this.buf, d]);
    if (this.buf.length > MAX_PAYLOAD * 4) return this.terminate();
    for (;;) {
      const f = this._parse();
      if (f === null) break;
      if (f === false) return this.terminate();
      this._frame(f);
      if (!this.open) return;
    }
  }
  _parse() {
    const b = this.buf;
    if (b.length < 2) return null;
    const fin = !!(b[0] & 0x80), opcode = b[0] & 0x0f, masked = !!(b[1] & 0x80);
    let len = b[1] & 0x7f, off = 2;
    if (len === 126) { if (b.length < 4) return null; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return null; const big = b.readBigUInt64BE(2); if (big > BigInt(MAX_PAYLOAD)) return false; len = Number(big); off = 10; }
    if (!masked || len > MAX_PAYLOAD) return false;           // клиентские кадры обязаны быть маскированы
    if (b.length < off + 4 + len) return null;
    const mask = b.slice(off, off + 4);
    const payload = Buffer.from(b.slice(off + 4, off + 4 + len));
    for (let i = 0; i < len; i++) payload[i] ^= mask[i & 3];
    this.buf = b.slice(off + 4 + len);
    return { fin, opcode, payload };
  }
  _frame(f) {
    if (f.opcode === 0x8) { this.close(); return; }
    if (f.opcode === 0x9) { this._raw(encodeFrame(0xA, f.payload)); return; }
    if (f.opcode === 0xA) return;
    let data = null;
    if (f.opcode === 0x1 || f.opcode === 0x0) {
      if (f.opcode === 0x1) this.frag = [];
      if (!this.frag) return this.terminate();
      this.frag.push(f.payload);
      if (this.frag.reduce((s, x) => s + x.length, 0) > MAX_PAYLOAD) return this.terminate();
      if (f.fin) { data = Buffer.concat(this.frag).toString('utf8'); this.frag = null; }
    } else return;                                            // бинарные кадры не поддерживаем
    if (data === null) return;
    // ограничение частоты сообщений
    const now = Date.now();
    this.tokens = Math.min(40, this.tokens + (now - this.lastRefill) / 1000 * 30); this.lastRefill = now;
    if (this.tokens < 1) return;
    this.tokens -= 1;
    let msg; try { msg = JSON.parse(data); } catch (_) { return; }
    if (msg && typeof msg === 'object') onMessage(this, msg);
  }
  _raw(buf) { if (this.open) { try { this.sock.write(buf); } catch (_) {} } }
  send(obj) { if (this.open) this._raw(encodeFrame(0x1, Buffer.from(JSON.stringify(obj)))); }
  ping() { this._raw(encodeFrame(0x9, Buffer.alloc(0))); }
  close() { if (!this.open) return; this._raw(encodeFrame(0x8, Buffer.alloc(0))); try { this.sock.end(); } catch (_) {} this._closed(); }
  terminate() { try { this.sock.destroy(); } catch (_) {} this._closed(); }
  _closed() {
    if (!this.open) return;
    this.open = false;
    conns.delete(this);
    ipCount.set(this.ip, Math.max(0, (ipCount.get(this.ip) || 1) - 1));
    onDisconnect(this);
  }
}

// ---------- состояние ----------
const conns = new Set();
const ipCount = new Map();
const queues = { 2: [], 5: [] };
const matches = new Map();

const clampName = s => {
  s = String(s == null ? '' : s).replace(/[\u0000-\u001f\u007f<>&"'`]/g, '').trim().slice(0, 16);
  return s || 'Racer';
};
const rnd = a => a[Math.floor(Math.random() * a.length)];

// ---------- проверка Telegram initData ----------
function verifyInitData(initData) {
  try {
    if (!BOT_TOKEN || !initData) return null;
    const params = new URLSearchParams(initData);
    const hash = params.get('hash');
    if (!hash) return null;
    params.delete('hash');
    const dcs = [...params.entries()].map(([k, v]) => k + '=' + v).sort().join('\n');
    const secret = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
    const calc = crypto.createHmac('sha256', secret).update(dcs).digest('hex');
    const a = Buffer.from(calc), b = Buffer.from(hash);
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;
    const age = Date.now() / 1000 - (+params.get('auth_date') || 0);
    if (age > 86400) return null;
    const user = JSON.parse(params.get('user') || 'null');
    return user && user.id ? user : null;
  } catch (_) { return null; }
}

// ---------- обработка сообщений ----------
function onMessage(c, m) {
  switch (m.t) {
    case 'find': {
      if (c.mode || c.match) return;                          // уже в очереди/матче
      const mode = m.mode === 5 ? 5 : 2;
      if (BOT_TOKEN) {
        const u = verifyInitData(m.init);
        if (u) { c.uid = u.id; c.name = clampName(u.first_name); }
        else if (!ALLOW_GUESTS) { c.send({ t: 'err', code: 'auth' }); return; }
        else c.name = clampName(m.name);
      } else c.name = clampName(m.name);
      c.veh = VEHICLES.includes(m.veh) ? m.veh : 'classic';
      // один Telegram-пользователь — одна очередь
      if (c.uid) for (const o of conns) if (o !== c && o.uid === c.uid && (o.mode || o.match)) { o.send({ t: 'err', code: 'dup' }); o.close(); }
      c.mode = mode; c.joinedAt = Date.now();
      queues[mode].push(c);
      c.send({ t: 'hello', id: c.id });
      broadcastQueue(mode);
      tryMatch(mode);
      break;
    }
    case 'cancel': leaveQueue(c); break;
    case 'p': {                                               // позиция в гонке
      const pl = c.player, mt = c.match;
      if (!pl || !mt || mt.done) return;
      const now = Date.now();
      if (now < mt.startAt - 200 || pl.fin != null) return;
      const x = +m.x, y = +m.y, a = +m.a;
      if (!isFinite(x) || !isFinite(y) || !isFinite(a)) return;
      if (now - pl.lastP < 40) return;
      const dt = Math.max(0.05, (now - pl.lastP) / 1000);
      if (x < 0 || x > FINISH_X + 600 || Math.abs(x - pl.x) > 2000 * dt + 250) return;   // анти-телепорт
      pl.x = x; pl.y = Math.max(-2000, Math.min(4000, y)); pl.a = Math.max(-10, Math.min(10, a)); pl.lastP = now;
      break;
    }
    case 'fin': {
      const pl = c.player, mt = c.match;
      if (!pl || !mt || mt.done || pl.fin != null) return;
      const now = Date.now();
      const t = now - mt.startAt;
      if (pl.x < FINISH_X * 0.97 || t < 0) { pl.fin = -1; }   // нечестный финиш → не засчитываем
      else pl.fin = Math.max(t, MIN_RACE_MS);
      pl.finAt = now;
      sendAll(mt, { t: 'fin', id: pl.id, time: pl.fin < 0 ? null : Math.round(pl.fin) });
      break;
    }
    case 'leave': leaveQueue(c); dropFromMatch(c); break;
  }
}

function leaveQueue(c) {
  if (!c.mode) return;
  const q = queues[c.mode], i = q.indexOf(c);
  if (i >= 0) q.splice(i, 1);
  const mode = c.mode; c.mode = 0;
  broadcastQueue(mode);
}
function dropFromMatch(c) {
  const mt = c.match, pl = c.player;
  if (!mt || !pl) return;
  pl.left = true;
  if (pl.fin == null) pl.fin = -1;
  c.match = null; c.player = null;
  if (!mt.players.some(p => !p.bot && !p.left)) matches.delete(mt.id);   // людей не осталось
}
function onDisconnect(c) { leaveQueue(c); dropFromMatch(c); }

function broadcastQueue(mode) {
  const q = queues[mode];
  for (const c of q) c.send({ t: 'q', mode, others: q.filter(o => o !== c).map(o => o.name) });
}

// ---------- матчи ----------
function tryMatch(mode) {
  const q = queues[mode];
  if (q.length >= mode) startMatch(q.splice(0, mode), mode);
  else if (q.length && Date.now() - q[0].joinedAt >= FILL_MS[mode]) startMatch(q.splice(0, q.length), mode);
}
function startMatch(humans, mode) {
  const id = crypto.randomBytes(4).toString('hex');
  const now = Date.now();
  const mt = { id, mode, seed: crypto.randomInt(1, 1e9), startAt: now + COUNTDOWN_MS, players: [], done: false, firstFinAt: 0 };
  const used = new Set(humans.map(h => h.name));
  for (const h of humans) {
    h.mode = 0;
    const pl = { id: h.id, name: h.name, veh: h.veh, bot: false, x: START_X, y: 0, a: 0, fin: null, lastP: now, conn: h };
    h.match = mt; h.player = pl; mt.players.push(pl);
  }
  while (mt.players.length < mode) {
    let nm; do { nm = rnd(BOT_NAMES) + '_' + (10 + crypto.randomInt(0, 90)); } while (used.has(nm)); used.add(nm);
    const veh = rnd(VEHICLES);
    mt.players.push({ id: 'b' + crypto.randomBytes(3).toString('hex'), name: nm, veh, bot: true, x: START_X, v: 0, base: BOT_BASE[veh] + (Math.random() - .5) * 60, ph: Math.random() * 6, stall: 0, fin: null });
  }
  matches.set(id, mt);
  const pub = mt.players.map(p => ({ id: p.id, name: p.name, veh: p.veh, bot: p.bot }));
  for (const h of humans) h.send({ t: 'match', id, mode, seed: mt.seed, you: h.id, players: pub, startAt: mt.startAt, now: Date.now() });
}
function sendAll(mt, obj) { for (const p of mt.players) if (!p.bot && !p.left && p.conn && p.conn.open) p.conn.send(obj); }

function finalize(mt) {
  if (mt.done) return;
  mt.done = true;
  const now = Date.now();
  const rows = mt.players.map(p => {
    let t = p.fin;
    if (t == null && p.bot) t = (now - mt.startAt) + (FINISH_X - p.x) / Math.max(150, p.v || 150) * 1000;   // прогноз для ботов
    if (t != null && t < 0) t = null;
    return { id: p.id, name: p.name, veh: p.veh, bot: p.bot, time: t == null ? null : Math.round(t) };
  });
  rows.sort((a, b) => (a.time == null) - (b.time == null) || a.time - b.time);
  rows.forEach((r, i) => { r.place = i + 1; });
  sendAll(mt, { t: 'res', rows });
  for (const p of mt.players) if (p.conn) { p.conn.match = null; p.conn.player = null; }
  setTimeout(() => matches.delete(mt.id), 2000);
}

// игровой цикл: 10 Гц
setInterval(() => {
  const now = Date.now();
  for (const mt of matches.values()) {
    if (mt.done) continue;
    const dt = 0.1, t = (now - mt.startAt) / 1000;
    for (const p of mt.players) {
      if (!p.bot || p.fin != null || t < 0) continue;
      if (p.stall > 0) p.stall -= dt; else if (Math.random() < dt * .045) p.stall = 1 + Math.random() * 1.2;
      let tv = p.base * (1 + .16 * Math.sin(t * .8 + p.ph));
      if (p.stall > 0) tv = 40;
      p.v += (tv - p.v) * Math.min(1, dt * 2.5);
      p.x += p.v * dt;
      if (p.x >= FINISH_X) p.fin = Math.round(now - mt.startAt);
    }
    if (t >= -0.5) sendAll(mt, { t: 'st', p: mt.players.map(p => p.bot ? [p.id, Math.round(p.x)] : [p.id, Math.round(p.x), Math.round(p.y), +(+p.a).toFixed(2)]) });
    const humans = mt.players.filter(p => !p.bot);
    const humansDone = humans.every(p => p.left || p.fin != null);
    const firstFin = humans.reduce((m, p) => (p.fin != null && p.fin >= 0 && p.finAt ? Math.min(m, p.finAt) : m), Infinity);
    if (humansDone || (firstFin !== Infinity && now - firstFin > RESULT_TIMEOUT_MS) || now - mt.startAt > MAX_RACE_MS ||
        mt.players.every(p => p.fin != null)) finalize(mt);
  }
  for (const m of [2, 5]) if (queues[m].length) tryMatch(m);
}, 100);

// проверка «живости» соединений
setInterval(() => {
  for (const c of conns) { if (!c.alive) { c.terminate(); continue; } c.alive = false; c.ping(); }
}, 20000);

// ---------- HTTP ----------
function findIndex() {
  for (const p of [path.join(__dirname, 'index.html'), path.join(__dirname, '..', 'index.html')]) if (fs.existsSync(p)) return p;
  return null;
}
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/health') { res.writeHead(200, { 'content-type': 'text/plain' }); return res.end('ok'); }
  if (url === '/stats') {
    res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*' });
    return res.end(JSON.stringify({ online: conns.size, queued: { 2: queues[2].length, 5: queues[5].length }, matches: matches.size }));
  }
  if (url === '/' || url === '/index.html') {
    const f = findIndex();
    if (!f) { res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' }); return res.end('Crypto Biker tournament server is running. WebSocket: /ws'); }
    const proto = (req.headers['x-forwarded-proto'] || (req.socket.encrypted ? 'https' : 'http')).toString().split(',')[0];
    const wsUrl = (proto === 'https' ? 'wss://' : 'ws://') + req.headers.host + '/ws';
    const html = fs.readFileSync(f, 'utf8').replace("const DEFAULT_SERVER='';", "const DEFAULT_SERVER='" + wsUrl + "';");
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
    return res.end(html);
  }
  res.writeHead(404, { 'content-type': 'text/plain' }); res.end('not found');
});
server.on('upgrade', (req, sock) => {
  const url = (req.url || '').split('?')[0];
  const key = req.headers['sec-websocket-key'];
  if (url !== '/ws' || !key || String(req.headers.upgrade).toLowerCase() !== 'websocket') { sock.end('HTTP/1.1 400 Bad Request\r\n\r\n'); return; }
  const ip = ((req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim()) || req.socket.remoteAddress || '?';
  if ((ipCount.get(ip) || 0) >= MAX_CONN_PER_IP) { sock.end('HTTP/1.1 429 Too Many Requests\r\n\r\n'); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  ipCount.set(ip, (ipCount.get(ip) || 0) + 1);
  const c = new Conn(sock, ip);
  conns.add(c);
});
server.listen(PORT, () => console.log('Crypto Biker tournament server on :' + PORT + (BOT_TOKEN ? ' (Telegram auth ON)' : ' (Telegram auth OFF)')));
process.on('SIGTERM', () => { server.close(); process.exit(0); });
