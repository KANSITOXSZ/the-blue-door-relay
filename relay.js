// Relay de THE BLUE DOOR (multijugador por internet). Sin dependencias: solo Node.js 18 o mas nuevo.
//   node relay.js        (puerto: variable de entorno PORT, por defecto 8765)
// Todos los jugadores se conectan aqui (solo conexiones de salida, sin abrir puertos en casa).
// Cada sala tiene un codigo de 4 letras. El host es el jugador 1; los demas entran con el codigo.
const http = require('http');
const crypto = require('crypto');

const ALF = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const MAX_CLIENTES = 7;
const salas = new Map(); // codigo -> { host, clientes: Map(id -> ws), sig }

const i32 = (n) => { const b = Buffer.alloc(4); b.writeInt32LE(n); return b; };

// ---------- Datos en la nube (un registro por nombre de usuario) ----------
// El primer dispositivo que guarda un nombre se queda con el. Si otro dispositivo usa el mismo
// nombre, recibe 409 y el juego guarda solo en ese dispositivo.
// Almacenamiento: si defines UPSTASH_REDIS_REST_URL y UPSTASH_REDIS_REST_TOKEN se guarda en Upstash
// (gratis y permanente). Si no, en archivos dentro de DATA_DIR (en Render gratis se BORRAN al reiniciar).
const fs = require('fs');
const path = require('path');
const https = require('https');
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'datos');
const UP_URL = process.env.UPSTASH_REDIS_REST_URL || '';
const UP_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || '';

function claveNombre(n) { return String(n || '').toUpperCase().replace(/[^A-Z0-9_]/g, '').slice(0, 10); }

function upstash(cmd) {
  return new Promise((ok, fail) => {
    const u = new URL(UP_URL);
    const body = JSON.stringify(cmd);
    const rq = https.request({ hostname: u.hostname, path: u.pathname || '/', method: 'POST', port: u.port || 443,
      headers: { Authorization: 'Bearer ' + UP_TOKEN, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) } }, (r) => {
      let t = ''; r.on('data', (d) => t += d); r.on('end', () => { try { ok(JSON.parse(t).result); } catch (e) { fail(e); } });
    });
    rq.on('error', fail); rq.setTimeout(10000, () => rq.destroy(new Error('timeout'))); rq.end(body);
  });
}
async function leerRegistro(clave) {
  if (UP_URL) { const v = await upstash(['GET', 'bd:' + clave]); return v ? JSON.parse(v) : null; }
  try { return JSON.parse(fs.readFileSync(path.join(DATA_DIR, clave + '.json'), 'utf8')); } catch (e) { return null; }
}
async function escribirRegistro(clave, reg) {
  if (UP_URL) { await upstash(['SET', 'bd:' + clave, JSON.stringify(reg)]); return; }
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(path.join(DATA_DIR, clave + '.json'), JSON.stringify(reg));
}

function json(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
  res.end(JSON.stringify(obj));
}

async function manejarNube(req, res, url) {
  try {
    if (req.method === 'GET') {
      const clave = claveNombre(url.searchParams.get('nombre'));
      const disp = String(url.searchParams.get('dispositivo') || '');
      if (!clave || !disp) return json(res, 400, { estado: 'faltan_datos' });
      const reg = await leerRegistro(clave);
      if (!reg) return json(res, 404, { estado: 'libre' });
      if (reg.dispositivo !== disp) return json(res, 409, { estado: 'ocupado' });
      return json(res, 200, { estado: 'ok', datos: reg.datos });
    }
    if (req.method === 'POST') {
      let cuerpo = '';
      req.on('data', (d) => { cuerpo += d; if (cuerpo.length > 65536) req.destroy(); });
      req.on('end', async () => {
        try {
          const o = JSON.parse(cuerpo);
          const clave = claveNombre(o.nombre);
          const disp = String(o.dispositivo || '');
          if (!clave || !disp || typeof o.datos !== 'string') return json(res, 400, { estado: 'faltan_datos' });
          const reg = await leerRegistro(clave);
          if (reg && reg.dispositivo !== disp) return json(res, 409, { estado: 'ocupado' });
          await escribirRegistro(clave, { dispositivo: disp, datos: o.datos, actualizado: Date.now() });
          return json(res, 200, { estado: 'guardado' });
        } catch (e) { return json(res, 500, { estado: 'error' }); }
      });
      return;
    }
    json(res, 405, { estado: 'metodo' });
  } catch (e) { json(res, 500, { estado: 'error' }); }
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/nube') { manejarNube(req, res, url); return; }
  res.writeHead(200); res.end('The Blue Door relay OK\n');
});

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }
  const acc = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  socket.setNoDelay(true);
  const ip = String(req.headers['x-forwarded-for'] || socket.remoteAddress || '').split(',')[0].trim().replace('::ffff:', '');
  nuevoCliente(socket, ip);
});

// Mini WebSocket (servidor): enmarcado, ping/pong y cierre
function crearWS(socket, onMsg, onClose) {
  let buf = Buffer.alloc(0), frag = [], fragOp = 0, avisado = false;
  const enviarFrame = (op, payload) => {
    if (socket.destroyed || !socket.writable) return;
    const len = payload.length; let h;
    if (len < 126) h = Buffer.from([0x80 | op, len]);
    else if (len < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(len, 2); }
    else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(len), 2); }
    socket.write(Buffer.concat([h, payload]));
  };
  const cerrar = () => { enviarFrame(0x8, Buffer.alloc(0)); setTimeout(() => socket.destroy(), 300); };
  const parse = () => {
    for (;;) {
      if (buf.length < 2) return;
      const fin = !!(buf[0] & 0x80), op = buf[0] & 0x0f, masked = !!(buf[1] & 0x80);
      let len = buf[1] & 0x7f, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      if (len > 4000000) { socket.destroy(); return; }
      const mlen = masked ? 4 : 0;
      if (buf.length < off + mlen + len) return;
      const payload = Buffer.from(buf.subarray(off + mlen, off + mlen + len));
      if (masked) { const m = buf.subarray(off, off + 4); for (let i = 0; i < payload.length; i++) payload[i] ^= m[i & 3]; }
      buf = buf.subarray(off + mlen + len);
      if (op === 0x8) { cerrar(); return; }
      if (op === 0x9) { enviarFrame(0xA, payload); continue; }
      if (op === 0xA) continue;
      if (op === 0x1 || op === 0x2) { fragOp = op; frag = [payload]; } else if (op === 0x0) { frag.push(payload); }
      if (fin && (op <= 0x2)) {
        const data = Buffer.concat(frag); frag = [];
        onMsg(fragOp === 0x1 ? data.toString('utf8') : data);
      }
    }
  };
  socket.on('data', (d) => { buf = Buffer.concat([buf, d]); parse(); });
  socket.on('error', () => socket.destroy());
  const ping = setInterval(() => enviarFrame(0x9, Buffer.alloc(0)), 25000);
  socket.on('close', () => { clearInterval(ping); if (!avisado) { avisado = true; onClose(); } });
  return {
    texto: (s) => enviarFrame(0x1, Buffer.from(s, 'utf8')),
    bin: (b) => enviarFrame(0x2, b),
    cerrar,
  };
}

function nuevoCliente(socket, ip) {
  let sala = null, codigo = null, id = 0, esHost = false;
  const ws = crearWS(socket, (m) => {
    if (!sala) {
      let d; try { d = JSON.parse(m); } catch { ws.cerrar(); return; }
      if (d.op === 'host') {
        do { codigo = Array.from({ length: 4 }, () => ALF[Math.floor(Math.random() * ALF.length)]).join(''); } while (salas.has(codigo));
        sala = { host: ws, clientes: new Map(), sig: 2 };
        salas.set(codigo, sala); esHost = true;
        ws.texto(JSON.stringify({ op: 'hosted', code: codigo }));
        console.log('sala creada', codigo);
      } else if (d.op === 'join') {
        const c = String(d.code || '').toUpperCase().trim();
        const s = salas.get(c);
        if (!s) { ws.texto(JSON.stringify({ op: 'err', m: 'mp_err_noroom' })); setTimeout(() => ws.cerrar(), 200); return; }
        if (s.clientes.size >= MAX_CLIENTES) { ws.texto(JSON.stringify({ op: 'err', m: 'mp_err_full' })); setTimeout(() => ws.cerrar(), 200); return; }
        sala = s; codigo = c; id = s.sig++;
        s.clientes.set(id, ws);
        ws.texto(JSON.stringify({ op: 'joined', id }));
        s.host.texto(JSON.stringify({ op: 'peer_in', id, ip }));
      } else { ws.cerrar(); }
      return;
    }
    if (typeof m !== 'string') {
      if (m.length < 4) return;
      if (esHost) {
        const dst = m.readInt32LE(0);
        const trama = Buffer.concat([i32(1), m.subarray(4)]);
        if (dst === 0) for (const c of sala.clientes.values()) c.bin(trama);
        else if (dst > 0) { const c = sala.clientes.get(dst); if (c) c.bin(trama); }
        else for (const [i, c] of sala.clientes) if (i !== -dst) c.bin(trama);
      } else {
        sala.host.bin(Buffer.concat([i32(id), m.subarray(4)]));
      }
    } else if (esHost) {
      try { const d = JSON.parse(m); if (d.op === 'kick') { const c = sala.clientes.get(d.id); if (c) c.cerrar(); } } catch {}
    }
  }, () => {
    clearTimeout(timer);
    if (!sala) return;
    if (esHost) {
      salas.delete(codigo);
      for (const c of sala.clientes.values()) { c.texto('{"op":"closed"}'); c.cerrar(); }
      console.log('sala cerrada', codigo);
    } else if (sala.clientes.delete(id)) {
      sala.host.texto(JSON.stringify({ op: 'peer_out', id }));
    }
  });
  const timer = setTimeout(() => { if (!sala) ws.cerrar(); }, 10000);
}

const puerto = parseInt(process.env.PORT || '8765', 10);
server.listen(puerto, '0.0.0.0', () => console.log('Relay escuchando en el puerto', puerto));
