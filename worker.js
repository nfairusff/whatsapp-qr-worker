// worker.js — WhatsApp QR pairing worker
// Deployed as a web service  (Suga / any Docker host)

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

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

/**
 * KEEP_WA_LINKED=false (default)
 *   Verification-only. After capturing the phone number, the worker
 *   calls sock.logout() to unlink the device and deletes the creds.
 *   Matches the schema where activationMedia = verified phone number.
 *
 * KEEP_WA_LINKED=true
 *   Keep the WhatsApp link alive so the worker can send messages later.
 *   The socket stays open and creds stay on disk.
 */
const KEEP_WA_LINKED = process.env.KEEP_WA_LINKED === 'true';

// How long to wait for creds to flush before reconnecting after a 515.
const RESTART_DELAY_MS = 1500;

// Max 515 reconnects before giving up.
const MAX_RESTARTS = 3;

// Time the user has to scan the QR.
const SESSION_TIMEOUT_MS = 120_000;

console.log(
  `Worker booting. target=${new URL(SUPABASE_URL).hostname} keepLinked=${KEEP_WA_LINKED}`,
);

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// sessionId -> { sock, dir, timeout, paired, completed, restarts }
const activeSessions = new Map();

/* ------------------------------------------------------------------ */
/* DB helpers                                                          */
/* ------------------------------------------------------------------ */

function parsePayload(raw) {
  if (raw == null) return {};
  if (typeof raw === 'string') {
    try { return JSON.parse(raw); } catch (err) {
      console.warn('parsePayload: malformed JSON:', err.message);
      return {};
    }
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

  if (error) {
    console.error(`[${sessionId}] readRow error:`, error.message, error.details ?? '');
    return null;
  }
  return data;
}

async function updateRow(sessionId, fields) {
  const { error } = await supabase
    .from('LaporanAkun')
    .update(fields)
    .eq('jenisPermohonan', sessionId);

  if (error) {
    console.error(
      `[${sessionId}] updateRow FAILED`,
      JSON.stringify({
        message: error.message,
        details: error.details,
        hint: error.hint,
        code: error.code,
        fields: Object.keys(fields),
      }),
    );
    return { ok: false, error };
  }
  return { ok: true };
}

/* ------------------------------------------------------------------ */
/* Session state                                                       */
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
}

/* ------------------------------------------------------------------ */
/* Pairing session                                                     */
/* ------------------------------------------------------------------ */

async function startPairingSession(sessionId, restarts = 0) {
  const existing = getSession(sessionId);
  if (existing) {
    console.log(`[${sessionId}] restarting session (restart #${restarts})`);
    if (existing.timeout) clearTimeout(existing.timeout);
    try { await existing.sock.end(undefined); } catch { /* ignore */ }
    activeSessions.delete(sessionId);
    // Don't delete the dir — the creds on disk are exactly what we need
    // to reconnect without re-scanning.
  } else {
    // Fresh session — verify the row exists.
    const row = await readRow(sessionId);
    if (!row) {
      console.warn(`[${sessionId}] row not found, aborting`);
      return;
    }
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
  };

  entry.timeout = setTimeout(async () => {
    const current = getSession(sessionId);
    if (!current || current.completed || current.paired) return;
    console.log(`[${sessionId}] timeout, cleaning up`);
    await updateRow(sessionId, { statusLaporan: 'expired' });
    await destroySession(sessionId);
  }, SESSION_TIMEOUT_MS);

  activeSessions.set(sessionId, entry);

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (update) => {
    const { connection, qr, lastDisconnect } = update;

    const current = getSession(sessionId);
    if (!current) return; // destroyed by cancel/timeout

    /* ---- QR emitted ---- */
    if (qr && !current.paired) {
      console.log(`[${sessionId}] QR emitted (length=${qr.length})`);
      const row = await readRow(sessionId);
      const payload = parsePayload(row?.isiPermohonan);
      const nextJson = JSON.stringify({ ...payload, qr });
      const write = await updateRow(sessionId, {
        statusLaporan: 'qr_ready',
        isiPermohonan: nextJson,
      });
      if (write.ok) {
        console.log(`[${sessionId}] qr_ready written (${nextJson.length} bytes)`);
      }
    }

    /* ---- Pairing complete ---- */
    if (connection === 'open') {
      current.paired = true;
      if (current.timeout) {
        clearTimeout(current.timeout);
        current.timeout = null;
      }

      const jid = sock.user?.id;
      const phone = jid?.split(':')[0]?.split('@')[0];
      console.log(`[${sessionId}] connection open, phone=${phone}`);

      if (!phone) {
        console.error(`[${sessionId}] no phone in JID, aborting`);
        try { await sock.logout(); } catch { /* ignore */ }
        await destroySession(sessionId);
        return;
      }

      const row = await readRow(sessionId);
      const payload = parsePayload(row?.isiPermohonan);

      await updateRow(sessionId, {
        statusLaporan: 'paired',
        isiPermohonan: JSON.stringify({ ...payload, phone }),
      });

      // Ask the Edge Function to create the akunPengguna row and flip
      // the session to 'Aktif'.
      try {
        const url = `${SUPABASE_URL}/functions/v1/REST-function?action=complete-pairing`;
        const res = await fetch(url, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
            apikey: SUPABASE_SERVICE_KEY,
          },
          body: JSON.stringify({ sessionId }),
        });
        const text = await res.text();
        console.log(`[${sessionId}] complete-pairing -> ${res.status} ${text}`);
      } catch (err) {
        console.error(`[${sessionId}] complete-pairing fetch failed:`, err);
      }

      current.completed = true;

      if (KEEP_WA_LINKED) {
        console.log(`[${sessionId}] keeping WA link alive (KEEP_WA_LINKED=true)`);
        // Socket stays open, creds stay on disk. Session stays in
        // activeSessions so SIGTERM can shut it down cleanly.
      } else {
        console.log(`[${sessionId}] unlinking device (verification-only)`);
        try { await sock.logout(); } catch { /* ignore */ }
        await destroySession(sessionId);
      }
      return;
    }

    /* ---- Connection dropped ---- */
    if (connection === 'close') {
      const code = lastDisconnect?.error?.output?.statusCode;

      if (current.completed) {
        // We already finished. A close after that is either the logout
        // we triggered ourselves, or a benign post-completion drop.
        console.log(`[${sessionId}] close after completion (code ${code}), ignoring`);
        return;
      }

      // 515 = "restart required". This is the signal WhatsApp sends
      // right after a successful QR scan. It is NOT a failure.
      if (code === 515) {
        if (current.restarts >= MAX_RESTARTS) {
          console.error(
            `[${sessionId}] exceeded max restarts (${MAX_RESTARTS}), abandoning`,
          );
          await updateRow(sessionId, { statusLaporan: 'abandoned' });
          await destroySession(sessionId);
          return;
        }
        const nextRestarts = current.restarts + 1;
        console.log(
          `[${sessionId}] 515 restart required, reconnecting (#${nextRestarts}/${MAX_RESTARTS})`,
        );
        // Give creds.update time to flush to disk before we reconnect.
        setTimeout(() => {
          startPairingSession(sessionId, nextRestarts).catch((err) => {
            console.error(`[${sessionId}] restart failed:`, err);
          });
        }, RESTART_DELAY_MS);
        return;
      }

      // 401 = explicitly logged out. Terminal.
      if (code === DisconnectReason.loggedOut) {
        console.log(`[${sessionId}] logged out (code 401), abandoning`);
        await updateRow(sessionId, { statusLaporan: 'abandoned' });
        await destroySession(sessionId);
        return;
      }

      // Anything else before pairing = abandoned.
      console.log(`[${sessionId}] close (code ${code}), abandoning`);
      await updateRow(sessionId, { statusLaporan: 'abandoned' });
      await destroySession(sessionId);
    }
  });
}

/* ------------------------------------------------------------------ */
/* HTTP server                                                         */
/* ------------------------------------------------------------------ */

const server = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  if (req.method === 'GET' && (req.url === '/' || req.url === '/health')) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      active: activeSessions.size,
      keepLinked: KEEP_WA_LINKED,
      port: PORT,
    }));
    return;
  }

  if (req.method === 'POST' && req.url === '/start-session') {
    let body = '';
    req.on('data', (chunk) => (body += chunk));
    req.on('end', async () => {
      try {
        const { sessionId } = JSON.parse(body || '{}');
        if (!sessionId) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'sessionId is required' }));
          return;
        }

        res.writeHead(202, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, sessionId }));

        startPairingSession(sessionId, 0).catch((err) => {
          console.error(`[${sessionId}] startPairingSession failed:`, err);
        });
      } catch {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Invalid JSON' }));
      }
    });
    return;
  }

  if (req.method === 'POST' && req.url === '/cancel-session') {
    let body = '';
    req.on('data', (chunk) => (body += chunk));
    req.on('end', async () => {
      try {
        const { sessionId } = JSON.parse(body || '{}');
        if (sessionId && activeSessions.has(sessionId)) {
          await updateRow(sessionId, { statusLaporan: 'abandoned' });
          await destroySession(sessionId);
        }
        res.writeHead(204);
        res.end();
      } catch {
        res.writeHead(400);
        res.end();
      }
    });
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Worker listening on 0.0.0.0:${PORT}`);
});

/* ------------------------------------------------------------------ */
/* Graceful shutdown                                                   */
/* ------------------------------------------------------------------ */

process.on('SIGTERM', async () => {
  console.log('SIGTERM — cleaning up sessions');
  const ids = [...activeSessions.keys()];
  for (const id of ids) {
    await destroySession(id);
  }
  process.exit(0);
});
