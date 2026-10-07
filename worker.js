// worker.js — WhatsApp QR pairing worker
// Deployed as a web service (Suga / any Docker host)

import http from 'http';
import makeWASocket, {
  useMultiFileAuthState,
  DisconnectReason,
} from '@whiskeysockets/baileys';
import { createClient } from '@supabase/supabase-js';
import fs from 'fs/promises';

const PORT = process.env.PORT || 8080;

// --- Supabase config (set as environment variables) ---
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// --- Active sessions map (sessionId -> { sock, dir, timeout }) ---
const activeSessions = new Map();

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

function parsePayload(raw) {
  if (raw == null) return {};
  if (typeof raw === 'string') {
    try { return JSON.parse(raw); } catch { return {}; }
  }
  return raw;
}

async function readRow(sessionId) {
  const { data, error } = await supabase
    .from('LaporanAkun')
    .select('jenisPermohonan, isiPermohonan, statusLaporan')
    .eq('jenisPermohonan', sessionId)
    .single();
  if (error || !data) return null;
  return data;
}

async function updateRow(sessionId, fields) {
  await supabase
    .from('LaporanAkun')
    .update(fields)
    .eq('jenisPermohonan', sessionId);
}

/* ------------------------------------------------------------------ */
/* Start a pairing session                                             */
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

  sock.ev.on('creds.update', saveCreds);

  // Hard timeout — 2 minutes for the user to scan
  const timeout = setTimeout(async () => {
    console.log(`[${sessionId}] timeout, cleaning up`);
    await updateRow(sessionId, { statusLaporan: 'expired' });
    try { await sock.end(undefined); } catch { /* ignore */ }
    await fs.rm(sessionDir, { recursive: true, force: true });
    activeSessions.delete(sessionId);
  }, 120_000);

  sock.ev.on('connection.update', async (update) => {
    const { connection, qr, lastDisconnect } = update;

    /* ---- QR emitted ---- */
    if (qr) {
      console.log(`[${sessionId}] QR emitted`);
      const current = await readRow(sessionId);
      const payload = parsePayload(current?.isiPermohonan);
      await updateRow(sessionId, {
        statusLaporan: 'qr_ready',
        isiPermohonan: JSON.stringify({ ...payload, qr }),
      });
    }

    /* ---- Pairing complete ---- */
    if (connection === 'open') {
      clearTimeout(timeout);

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

      // Write phone + status='paired'
      await updateRow(sessionId, {
        statusLaporan: 'paired',
        isiPermohonan: JSON.stringify({ ...payload, phone }),
      });

      // Ask the Edge Function to finalize activation
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

      // Close and clean up
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
        try { await sock.end(undefined); } catch { /* ignore */ }
        await fs.rm(sessionDir, { recursive: true, force: true });
        activeSessions.delete(sessionId);
      }
    }
  });

  activeSessions.set(sessionId, { sock, dir: sessionDir, timeout });
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

  /* ---- health check (root + /health) ---- */
  // Suga (and most platforms) probe '/' to check container health.
  // Returning 200 on both prevents unnecessary restarts.
  if (req.method === 'GET' && (req.url === '/' || req.url === '/health')) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      active: activeSessions.size,
    }));
    return;
  }

  /* ---- start a pairing session ---- */
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

        // Fire-and-forget; the response is already sent
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

  res.writeHead(404);
  res.end('Not found');
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('PORT env value:', process.env.PORT, '| using:', PORT);
  console.log(`Worker listening on 0.0.0.0:${PORT}`);
});

/* ------------------------------------------------------------------ */
/* Cleanup on shutdown                                                 */
/* ------------------------------------------------------------------ */

process.on('SIGTERM', async () => {
  console.log('SIGTERM — cleaning up sessions');
  for (const [sessionId, entry] of activeSessions) {
    try { await entry.sock.end(undefined); } catch { /* ignore */ }
    try { await fs.rm(entry.dir, { recursive: true, force: true }); } catch { /* ignore */ }
  }
  process.exit(0);
});
