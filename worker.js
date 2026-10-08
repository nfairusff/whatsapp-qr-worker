// worker.js — WhatsApp QR pairing worker
// Deployed as a web service (Suga / any Docker host)

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

console.log('Worker booting. SUPABASE_URL host:', new URL(SUPABASE_URL).hostname);

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// sessionId -> { sock, dir, timeout }
const activeSessions = new Map();

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

function parsePayload(raw) {
  if (raw == null) return {};
  if (typeof raw === 'string') {
    try {
      return JSON.parse(raw);
    } catch (err) {
      console.warn('parsePayload: malformed JSON, treating as empty:', err.message);
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

/**
 * Returns { ok: true } or { ok: false, error }.
 * Callers MUST check, otherwise writes fail silently.
 */
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

async function cleanupSession(sessionId, { endSocket = true } = {}) {
  const entry = activeSessions.get(sessionId);
  if (!entry) return;

  if (entry.timeout) clearTimeout(entry.timeout);
  activeSessions.delete(sessionId);

  if (endSocket) {
    try { await entry.sock.end(undefined); } catch { /* ignore */ }
  }
  try { await fs.rm(entry.dir, { recursive: true, force: true }); } catch { /* ignore */ }
}

/* ------------------------------------------------------------------ */
/* Pairing session                                                     */
/* ------------------------------------------------------------------ */

async function startPairingSession(sessionId) {
  if (activeSessions.has(sessionId)) {
    console.log(`[${sessionId}] already active, skipping`);
    return;
  }

  const row = await readRow(sessionId);
  if (!row) {
    console.warn(`[${sessionId}] row not found, aborting`);
    return;
  }

  const sessionDir = `./sessions/${sessionId}`;
  await fs.mkdir(sessionDir, { recursive: true });

  const { state, saveCreds } = await useMultiFileAuthState(sessionDir);

  const sock = makeWASocket({
    auth: state,
    printQRInTerminal: false,
    browser: ['Ubuntu', 'Chrome', '20.0.0'],
  });

  // Register BEFORE wiring listeners so any sync close event can find us.
  const timeout = setTimeout(async () => {
    console.log(`[${sessionId}] timeout, cleaning up`);
    await updateRow(sessionId, { statusLaporan: 'expired' });
    await cleanupSession(sessionId);
  }, 120_000);

  activeSessions.set(sessionId, { sock, dir: sessionDir, timeout });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (update) => {
    const { connection, qr, lastDisconnect } = update;

    /* ---- QR emitted ---- */
    if (qr) {
      console.log(`[${sessionId}] QR emitted (length=${qr.length})`);

      const current = await readRow(sessionId);
      const payload = parsePayload(current?.isiPermohonan);
      const next = { ...payload, qr };
      const nextJson = JSON.stringify(next);

      console.log(
        `[${sessionId}] writing isiPermohonan bytes=${nextJson.length} hasQr=${'qr' in next}`,
      );

      const write = await updateRow(sessionId, {
        statusLaporan: 'qr_ready',
        isiPermohonan: nextJson,
      });

      if (!write.ok) {
        // At this point statusLaporan may still have landed (PostgREST
        // applies updates atomically, so it shouldn't, but the log will
        // tell us exactly what went wrong).
        console.error(`[${sessionId}] QR write failed — see updateRow log above`);
        return;
      }

      // Verify the QR actually persisted.
      const verify = await readRow(sessionId);
      const verified = parsePayload(verify?.isiPermohonan);
      console.log(
        `[${sessionId}] verify: status=${verify?.statusLaporan} hasQr=${!!verified.qr} qrLen=${verified.qr?.length ?? 0}`,
      );
    }

    /* ---- Pairing complete ---- */
    if (connection === 'open') {
      await cleanupSession(sessionId, { endSocket: false });

      const jid = sock.user?.id;
      const phone = jid?.split(':')[0]?.split('@')[0];
      console.log(`[${sessionId}] paired with phone ${phone}`);

      if (!phone) {
        console.error(`[${sessionId}] no phone in JID, aborting`);
        try { await sock.logout(); } catch { /* ignore */ }
        await fs.rm(sessionDir, { recursive: true, force: true });
        activeSessions.delete(sessionId);
        return;
      }

      const current = await readRow(sessionId);
      const payload = parsePayload(current?.isiPermohonan);

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
          },
          body: JSON.stringify({ sessionId }),
        });
        const text = await res.text();
        console.log(`[${sessionId}] complete-pairing -> ${res.status} ${text}`);
      } catch (err) {
        console.error(`[${sessionId}] complete-pairing fetch failed:`, err);
      }

      try { await sock.logout(); } catch { /* ignore */ }
      await fs.rm(sessionDir, { recursive: true, force: true });
      activeSessions.delete(sessionId);
    }

    /* ---- Connection dropped ---- */
    if (connection === 'close') {
      const code = lastDisconnect?.error?.output?.statusCode;
      const stillActive = activeSessions.has(sessionId);
      if (code !== DisconnectReason.loggedOut && stillActive) {
        console.log(`[${sessionId}] connection closed (code ${code})`);
        await updateRow(sessionId, { statusLaporan: 'abandoned' });
        await cleanupSession(sessionId);
      }
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

        startPairingSession(sessionId).catch((err) => {
          console.error(`[${sessionId}] startPairingSession failed:`, err);
        });
      } catch (err) {
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
          await cleanupSession(sessionId);
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
    await cleanupSession(id);
  }
  process.exit(0);
});
