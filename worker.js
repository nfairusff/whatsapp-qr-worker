// worker.js — WhatsApp verification worker (verification-only)
// Every request requires X-Worker-Secret. Never exposed to browsers.

import http from 'http';
import makeWASocket, {
  useMultiFileAuthState,
  DisconnectReason,
} from '@whiskeysockets/baileys';
import { createClient } from '@supabase/supabase-js';
import fs from 'fs/promises';

/* ------------------------------------------------------------------ */
/* Config                                                              */
/* ------------------------------------------------------------------ */

const PORT = process.env.PORT || 8080;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const WORKER_SHARED_SECRET = process.env.WORKER_SHARED_SECRET;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('FATAL: missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}
if (!WORKER_SHARED_SECRET || WORKER_SHARED_SECRET.length < 32) {
  console.error('FATAL: WORKER_SHARED_SECRET missing or too short (min 32 chars)');
  process.exit(1);
}

const SESSION_TIMEOUT_MS = 90_000;
const MAX_RESTARTS = 1;
const RESTART_DELAY_MS = 1500;
const MAX_ACTIVE_SESSIONS = 20;

console.log(`Worker booting. supabase=${new URL(SUPABASE_URL).hostname}`);

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const activeSessions = new Map();

/* ------------------------------------------------------------------ */
/* Audit log                                                           */
/* ------------------------------------------------------------------ */

function audit(sessionId, event, extra = {}) {
  console.log(JSON.stringify({
    t: new Date().toISOString(),
    sessionId,
    event,
    ...extra,
  }));
}

/* ------------------------------------------------------------------ */
/* DB helpers                                                          */
/* ------------------------------------------------------------------ */

function parsePayload(raw) {
  if (raw == null) return {};
  if (typeof raw === 'string') {
    try { return JSON.parse(raw); } catch { return {}; }
  }
  if (typeof raw === 'object') return raw;
  return {};
}

async function readRow(sessionId) {
  const { data, error } = await supabase
    .from('LaporanAkun')
    .select('jenisPermohonan, isiPermohonan, statusLaporan')
    .eq('jenisPermohonan', sessionId)
    .limit(1)
    .maybeSingle();
  if (error) return null;
  return data;
}

async function updateRow(sessionId, fields) {
  const { error } = await supabase
    .from('LaporanAkun')
    .update(fields)
    .eq('jenisPermohonan', sessionId);
  if (error) {
    audit(sessionId, 'db_update_failed', { code: error.code, message: error.message });
    return false;
  }
  return true;
}

/* ------------------------------------------------------------------ */
/* Session lifecycle                                                   */
/* ------------------------------------------------------------------ */

function getSession(sessionId) {
  return activeSessions.get(sessionId) ?? null;
}

async function destroySession(sessionId, { endSocket = true, removeDir = true } = {}) {
  const entry = activeSessions.get(sessionId);
  if (!entry) return;

  if (entry.timeout) clearTimeout(entry.timeout);
  activeSessions.delete(sessionId);

  if (endSocket) {
    try { await entry.sock.end(undefined); } catch { /* ignore */ }
  }
  if (removeDir) {
    try { await fs.rm(entry.dir, { recursive: true, force: true }); } catch { /* ignore */ }
  }
  audit(sessionId, 'session_destroyed', { lifetimeMs: Date.now() - entry.startedAt });
}

async function startPairingSession(sessionId, restarts = 0) {
  if (restarts === 0) {
    if (activeSessions.size >= MAX_ACTIVE_SESSIONS) {
      audit(sessionId, 'rejected_concurrency_limit', { active: activeSessions.size });
      return;
    }
    const row = await readRow(sessionId);
    if (!row) {
      audit(sessionId, 'rejected_row_missing');
      return;
    }
    if (row.statusLaporan !== 'pending') {
      audit(sessionId, 'rejected_bad_status', { status: row.statusLaporan });
      return;
    }
    if (!await updateRow(sessionId, { statusLaporan: 'starting' })) {
      audit(sessionId, 'rejected_mark_starting_failed');
      return;
    }
    audit(sessionId, 'session_starting');
  } else {
    const existing = getSession(sessionId);
    if (existing) {
      if (existing.timeout) clearTimeout(existing.timeout);
      try { await existing.sock.end(undefined); } catch { /* ignore */ }
      activeSessions.delete(sessionId);
    }
    audit(sessionId, 'session_restarting', { restart: restarts });
  }

  const sessionDir = `./sessions/${sessionId}`;
  await fs.mkdir(sessionDir, { recursive: true });

  const { state, saveCreds } = await useMultiFileAuthState(sessionDir);

  const sock = makeWASocket({
    auth: state,
    printQRInTerminal: false,
    browser: ['Ubuntu', 'Chrome', '20.0.0'],
  });

  const entry = {
    sock,
    dir: sessionDir,
    timeout: null,
    paired: false,
    completed: false,
    restarts,
    startedAt: Date.now(),
  };

  entry.timeout = setTimeout(async () => {
    const current = getSession(sessionId);
    if (!current || current.completed || current.paired) return;
    audit(sessionId, 'session_timeout');
    await updateRow(sessionId, { statusLaporan: 'expired' });
    await destroySession(sessionId);
  }, SESSION_TIMEOUT_MS);

  activeSessions.set(sessionId, entry);

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (update) => {
    const { connection, qr, lastDisconnect } = update;
    const current = getSession(sessionId);
    if (!current) return;

    if (qr && !current.paired) {
      const row = await readRow(sessionId);
      const payload = parsePayload(row?.isiPermohonan);
      await updateRow(sessionId, {
        statusLaporan: 'qr_ready',
        isiPermohonan: JSON.stringify({ ...payload, qr }),
      });
      audit(sessionId, 'qr_emitted', { qrLen: qr.length });
    }

    if (connection === 'open') {
      current.paired = true;
      if (current.timeout) {
        clearTimeout(current.timeout);
        current.timeout = null;
      }

      const jid = sock.user?.id;
      const phone = jid?.split(':')[0]?.split('@')[0];
      audit(sessionId, 'connection_open', { phone });

      if (!phone) {
        audit(sessionId, 'no_phone_in_jid');
        try { await sock.logout(); } catch { /* ignore */ }
        await updateRow(sessionId, { statusLaporan: 'abandoned' });
        await destroySession(sessionId);
        return;
      }

      const row = await readRow(sessionId);
      const payload = parsePayload(row?.isiPermohonan);
      await updateRow(sessionId, {
        statusLaporan: 'paired',
        isiPermohonan: JSON.stringify({ ...payload, phone }),
      });

      try {
        const url = `${SUPABASE_URL}/functions/v1/REST-function?action=complete-pairing`;
        const res = await fetch(url, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
            apikey: SUPABASE_SERVICE_KEY,
            'X-Worker-Secret': WORKER_SHARED_SECRET,
          },
          body: JSON.stringify({ sessionId }),
        });
        audit(sessionId, 'complete_pairing_sent', { status: res.status });
      } catch (err) {
        audit(sessionId, 'complete_pairing_failed', { error: String(err) });
      }

      current.completed = true;

      try { await sock.logout(); } catch { /* ignore */ }
      await destroySession(sessionId, { endSocket: false });
      audit(sessionId, 'session_completed');
      return;
    }

    if (connection === 'close') {
      const code = lastDisconnect?.error?.output?.statusCode;
      if (current.completed) return;

      if (code === 515) {
        if (current.restarts >= MAX_RESTARTS) {
          audit(sessionId, 'max_restarts_exceeded');
          await updateRow(sessionId, { statusLaporan: 'abandoned' });
          await destroySession(sessionId);
          return;
        }
        const next = current.restarts + 1;
        audit(sessionId, 'restart_515', { attempt: next });
        setTimeout(() => {
          startPairingSession(sessionId, next).catch((err) => {
            audit(sessionId, 'restart_threw', { error: String(err) });
          });
        }, RESTART_DELAY_MS);
        return;
      }

      if (code === DisconnectReason.loggedOut) {
        audit(sessionId, 'logged_out_by_user');
        await updateRow(sessionId, { statusLaporan: 'abandoned' });
        await destroySession(sessionId);
        return;
      }

      audit(sessionId, 'close_unexpected', { code });
      await updateRow(sessionId, { statusLaporan: 'abandoned' });
      await destroySession(sessionId);
    }
  });
}

/* ------------------------------------------------------------------ */
/* HTTP server                                                         */
/* ------------------------------------------------------------------ */

function requireSecret(req) {
  const provided = req.headers['x-worker-secret'];
  return typeof provided === 'string' && provided.length === WORKER_SHARED_SECRET.length &&
    crypto.timingSafeEqual(Buffer.from(provided), Buffer.from(WORKER_SHARED_SECRET));
}

function readBody(req) {
  return new Promise((resolve) => {
    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', () => resolve(body));
  });
}

const server = http.createServer(async (req, res) => {
  if (req.method === 'GET' && (req.url === '/' || req.url === '/health')) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', active: activeSessions.size }));
    return;
  }

  if (req.method === 'POST' && req.url === '/start-session') {
    if (!requireSecret(req)) {
      res.writeHead(401);
      res.end('Unauthorized');
      return;
    }

    const raw = await readBody(req);
    let sessionId;
    try { sessionId = JSON.parse(raw || '{}').sessionId; } catch {
      res.writeHead(400); res.end('Invalid JSON'); return;
    }

    if (typeof sessionId !== 'string' || sessionId.length !== 64) {
      res.writeHead(400); res.end('Invalid sessionId'); return;
    }

    res.writeHead(202, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true }));

    startPairingSession(sessionId, 0).catch((err) => {
      audit(sessionId, 'start_threw', { error: String(err) });
    });
    return;
  }

  if (req.method === 'POST' && req.url === '/cancel-session') {
    if (!requireSecret(req)) {
      res.writeHead(401); res.end('Unauthorized'); return;
    }
    const raw = await readBody(req);
    let sessionId;
    try { sessionId = JSON.parse(raw || '{}').sessionId; } catch {
      res.writeHead(400); res.end('Invalid JSON'); return;
    }
    if (typeof sessionId === 'string' && activeSessions.has(sessionId)) {
      audit(sessionId, 'cancel_requested');
      await updateRow(sessionId, { statusLaporan: 'abandoned' });
      await destroySession(sessionId);
    }
    res.writeHead(204); res.end();
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Worker listening on 0.0.0.0:${PORT}`);
});

process.on('SIGTERM', async () => {
  console.log('SIGTERM — cleaning up sessions');
  for (const id of [...activeSessions.keys()]) {
    await destroySession(id);
  }
  process.exit(0);
});
