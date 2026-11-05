// Ferry: LAN file sharing server with zero dependencies.
// Files stream to ./uploads, metadata lives in ./uploads/meta.json, open browsers get live updates over Server-Sent Events.

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const crypto = require('node:crypto');
const zlib = require('node:zlib');
const { planZip } = require('./zip');

// ---------- config ----------

const PORT = Number(process.env.PORT) || 8080;
const DIR = path.resolve(process.env.UPLOAD_DIR || path.join(__dirname, 'uploads'));
const META = path.join(DIR, 'meta.json');
const INDEX = path.join(__dirname, 'public', 'index.html');

// ---------- metadata store ----------

fs.mkdirSync(DIR, { recursive: true });

// file record: { id, name, size, crc, type, by, cid, at, folder?, path? }, newest first
let files = [];
try {
  files = JSON.parse(fs.readFileSync(META, 'utf8')).filter(f => fs.existsSync(path.join(DIR, f.id)));
} catch {
  // first run or unreadable meta.json: start empty
}

// writes are batched: a 1000-file folder upload would otherwise rewrite meta.json 1000 times
let saveTimer = null;

function flush() {
  clearTimeout(saveTimer);
  saveTimer = null;
  fs.writeFileSync(META, JSON.stringify(files, null, 1));
}

const save = () => { saveTimer ??= setTimeout(flush, 300); };

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => { flush(); process.exit(0); });
}

// ---------- live clients (SSE) ----------

const clients = new Map(); // response stream -> { id, name }

const send = (res, event, data) => res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);

function broadcast(event, data) {
  for (const res of clients.keys()) send(res, event, data);
}

// one entry per browser, even if it has several tabs open
function presence() {
  const people = new Map();
  for (const c of clients.values()) people.set(c.id, c.name);
  broadcast('presence', [...people].map(([id, name]) => ({ id, name })));
}

// comment frames keep idle connections from being dropped by proxies and sleeping Wi-Fi
setInterval(() => {
  for (const res of clients.keys()) res.write(': ping\n\n');
}, 25000);

// ---------- helpers ----------

const lanIPs = () => Object.values(os.networkInterfaces())
  .flat()
  .filter(i => i && i.family === 'IPv4' && !i.internal)
  .map(i => i.address);

function json(res, code, data) {
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

// strips control characters and slashes, trims, caps length
const clean = (s, max) => String(s || '').replace(/[\x00-\x1f\\/]/g, '').trim().slice(0, max);

// "Photos/../2024/a.jpg" -> "Photos/2024/a.jpg": keeps folder structure, never escapes the archive root
const cleanPath = s => String(s || '')
  .split('/')
  .map(seg => clean(seg, 255))
  .filter(seg => seg && seg !== '.' && seg !== '..')
  .join('/');

const FOLDER_ID = /^[a-z0-9]{1,40}$/i;
const MIME = /^[\w.+-]+\/[\w.+-]+$/;

function removeFiles(gone) {
  if (!gone.length) return;

  const ids = new Set(gone.map(f => f.id));
  files = files.filter(f => !ids.has(f.id));
  for (const f of gone) fs.rm(path.join(DIR, f.id), { force: true }, () => {});

  save();
  broadcast('removed', [...ids]);
}

// ---------- route handlers ----------

function serveIndex(req, res) {
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
  fs.createReadStream(INDEX).pipe(res);
}

function openEvents(req, res, q) {
  res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });

  clients.set(res, { id: clean(q.get('cid'), 40), name: clean(q.get('name'), 40) || 'Someone' });
  send(res, 'files', files);
  presence();

  req.on('close', () => {
    clients.delete(res);
    presence();
  });
}

// a browser renamed itself: update presence and every file it uploaded
function rename(req, res, q) {
  const cid = clean(q.get('cid'), 40);
  const name = clean(q.get('name'), 40);
  if (!cid || !name) return json(res, 400, { error: 'cid and name required' });

  for (const c of clients.values()) if (c.id === cid) c.name = name;

  let changed = false;
  for (const f of files) {
    if (f.cid === cid && f.by !== name) { f.by = name; changed = true; }
  }

  if (changed) { save(); broadcast('files', files); }
  presence();
  json(res, 200, { ok: true });
}

// POST /api/upload?name=&by=&cid=  with the raw file as the body
// folder uploads add &folder=<batch id>&path=<path inside it, e.g. Photos/2024/a.jpg>
function upload(req, res, q) {
  const id = crypto.randomBytes(8).toString('hex');
  const tmp = path.join(DIR, `${id}.part`);
  const out = fs.createWriteStream(tmp);

  // size and CRC are counted while streaming, so a later zip download never re-reads the file for them
  let size = 0;
  let crc = 0;
  let failed = false;

  const fail = () => {
    if (failed) return;
    failed = true;
    out.destroy();
    fs.rm(tmp, { force: true }, () => {});
  };

  req.on('data', chunk => {
    size += chunk.length;
    crc = zlib.crc32(chunk, crc);
  });
  req.on('aborted', fail);

  out.on('error', () => {
    fail();
    if (!res.headersSent) json(res, 500, { error: 'write failed' });
  });

  out.on('finish', () => {
    if (failed) return;
    fs.renameSync(tmp, path.join(DIR, id));

    const folder = q.get('folder');
    const rel = cleanPath(q.get('path'));
    const type = req.headers['content-type'] || '';

    const file = {
      id,
      name: clean(q.get('name'), 255) || 'file',
      size,
      crc,
      type: MIME.test(type) ? type : 'application/octet-stream',
      by: clean(q.get('by'), 40) || 'Someone',
      cid: clean(q.get('cid'), 40),
      at: Date.now(),
      // only kept when it really is "folder/file", so every folder file has a top-level folder name
      ...(folder && FOLDER_ID.test(folder) && rel.includes('/') && { folder, path: rel }),
    };

    files.unshift(file);
    save();
    broadcast('added', file);
    json(res, 200, file);
  });

  req.pipe(out);
}

// ?dl forces a download; without it the browser shows the file inline (images, PDFs, video)
function download(req, res, q, file) {
  res.writeHead(200, {
    'Content-Type': file.type,
    'Content-Length': file.size,
    'Content-Disposition': `${q.has('dl') ? 'attachment' : 'inline'}; filename*=UTF-8''${encodeURIComponent(file.name)}`,
    'X-Content-Type-Options': 'nosniff',
    'Content-Security-Policy': 'sandbox', // an uploaded .html opened inline cannot run script against the app
  });

  fs.createReadStream(path.join(DIR, file.id)).pipe(res);
}

// a folder downloads as one zip, streamed straight from the stored files with no temp copy
function downloadZip(req, res, folderId) {
  const entries = files.filter(f => f.folder === folderId).reverse(); // oldest first = upload order
  if (!entries.length) return json(res, 404, { error: 'not found' });

  const zipName = `${entries[0].path.split('/')[0]}.zip`;
  const { length, pipeTo } = planZip(entries.map(f => ({
    path: path.join(DIR, f.id),
    name: f.path,
    size: f.size,
    crc: f.crc,
    at: f.at,
  })));

  res.writeHead(200, {
    'Content-Type': 'application/zip',
    'Content-Length': length, // exact, so the browser shows real download progress
    'Content-Disposition': `attachment; filename*=UTF-8''${encodeURIComponent(zipName)}`,
  });

  pipeTo(res).catch(() => res.destroy()); // client cancelled or a file was deleted mid-stream
}

// ---------- router ----------

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://local');
  const p = url.pathname;
  const q = url.searchParams;
  const route = `${req.method} ${p}`;

  if (route === 'GET /') return serveIndex(req, res);
  if (route === 'GET /api/info') return json(res, 200, { ips: lanIPs(), port: PORT });
  if (route === 'GET /api/files') return json(res, 200, files);
  if (route === 'GET /api/events') return openEvents(req, res, q);
  if (route === 'POST /api/rename') return rename(req, res, q);
  if (route === 'POST /api/upload') return upload(req, res, q);

  const zip = p.match(/^\/zip\/([a-z0-9]{1,40})$/i);
  if (zip && req.method === 'GET') return downloadZip(req, res, zip[1]);

  const folder = p.match(/^\/api\/folders\/([a-z0-9]{1,40})$/i);
  if (folder && req.method === 'DELETE') {
    removeFiles(files.filter(f => f.folder === folder[1]));
    return json(res, 200, { ok: true });
  }

  // /f/<id> downloads a file, /api/files/<id> deletes one
  const one = p.match(/^\/(f|api\/files)\/([a-f0-9]{16})$/);
  const file = one && files.find(f => f.id === one[2]);

  if (one && !file) return json(res, 404, { error: 'not found' });
  if (one && one[1] === 'f' && req.method === 'GET') return download(req, res, q, file);
  if (one && one[1] === 'api/files' && req.method === 'DELETE') {
    removeFiles([file]);
    return json(res, 200, { ok: true });
  }

  json(res, 404, { error: 'not found' });
});

// ---------- start ----------

server.requestTimeout = 0; // allow multi-GB uploads over slow Wi-Fi

server.listen(PORT, '0.0.0.0', () => {
  console.log('\n  Ferry is running. Open on any device on this network:\n');
  for (const ip of lanIPs()) console.log(`    http://${ip}:${PORT}`);
  console.log(`    http://localhost:${PORT}  (this machine)\n`);
  console.log(`  Files are stored in ${DIR}\n`);
});
