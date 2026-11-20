import express from 'express';
import http from 'http';
import https from 'https';
import { createReadStream, createWriteStream, unlinkSync, existsSync, mkdirSync, openAsBlob, readdirSync, rmSync } from 'fs';
import { readFile, writeFile, unlink, stat, statfs, readdir, open as openFile } from 'fs/promises';
import { createDecipheriv, createCipheriv, randomUUID, randomBytes, createHash, timingSafeEqual } from 'crypto';
import { join, isAbsolute } from 'path';
import { Readable, Transform } from 'stream';
import { pipeline } from 'stream/promises';
import { execFile } from 'child_process';
import { promisify } from 'util';
import sharp from 'sharp';

const execFileAsync = promisify(execFile);

const app = express();
const PORT = process.env.PORT || 3000;
const BOT_TOKEN = process.env.TG_BOT_TOKEN;
const AUTH_SECRET = process.env.AUTH_SECRET;
const TG_API = process.env.TG_LOCAL_API || 'https://api.telegram.org';
const TEMP_DIR = process.env.TEMP_DIR || '/tmp/tg-s3-processor';

// Which automatic media jobs run after an upload: 'all' (default), 'images' (no video transcoding) or 'none'.
// Video transcoding is the only job whose memory use grows with the file, so small servers should use 'images' or 'none'.
const MEDIA_JOBS = (process.env.MEDIA_JOBS || 'all').toLowerCase();
if (!['all', 'images', 'none'].includes(MEDIA_JOBS)) {
  console.error(`FATAL: MEDIA_JOBS must be all, images or none (got "${MEDIA_JOBS}")`);
  process.exit(1);
}
function isJobTypeEnabled(jobType) {
  if (MEDIA_JOBS === 'none') return false;
  if (MEDIA_JOBS === 'images') return jobType !== 'video_transcode';
  return true;
}

// Timeout constants for TG API calls (server-side protection against hangs)
const TIMEOUT_API = 30_000;            // 30s for getFile and similar API method calls
const TIMEOUT_TRANSFER = 10 * 60_000;  // 10 min for file download/upload operations (up to 2GB)

// Validate required env vars at startup to fail fast
const REQUIRED_ENV = { TG_BOT_TOKEN: BOT_TOKEN, AUTH_SECRET };
for (const [name, value] of Object.entries(REQUIRED_ENV)) {
  if (!value) { console.error(`FATAL: ${name} is not set`); process.exit(1); }
}

if (!existsSync(TEMP_DIR)) mkdirSync(TEMP_DIR, { recursive: true });

// Files in TEMP_DIR at startup belong to a run that crashed or was killed mid-job: nothing can use them now.
for (const name of readdirSync(TEMP_DIR)) {
  try { rmSync(join(TEMP_DIR, name), { recursive: true, force: true }); } catch {}
}

// --- Disk quota ---
// The processor's files (TEMP_DIR) plus the Bot API's copies of this bot's files (BOT_FILES_DIR, the bot's folder
// in a --local Bot API's data directory) must stay under DISK_QUOTA_GB, and the disk must keep MIN_FREE_DISK_GB free.
// Work that would not fit is refused before it starts (503 SlowDown), instead of filling the disk half-way.
const GB = 1e9;
const DISK_QUOTA = Math.floor((Number(process.env.DISK_QUOTA_GB) || 6) * GB);
const MIN_FREE_DISK = Math.floor((Number(process.env.MIN_FREE_DISK_GB) || 4) * GB);
const BOT_FILES_DIR = join(process.env.BOT_API_DIR || '/data', BOT_TOKEN);
// Where to measure the shared disk: the bot's folder, else the Bot API data directory, else TEMP_DIR
const sharedDiskDir = () => [BOT_FILES_DIR, process.env.BOT_API_DIR || '/data', TEMP_DIR].find(d => existsSync(d));
let diskReserved = 0; // bytes promised to work in progress that is not yet on disk

class QuotaError extends Error {
  constructor(message) { super(message); this.status = 503; this.code = 'SlowDown'; this.retryAfter = 60; }
}

async function dirBytes(dir) {
  let total = 0;
  let entries;
  try { entries = await readdir(dir, { withFileTypes: true }); } catch { return 0; }
  for (const e of entries) {
    const p = join(dir, e.name);
    if (e.isDirectory()) total += await dirBytes(p);
    else if (e.isFile()) { try { total += (await stat(p)).size; } catch {} }
  }
  return total;
}

async function freeBytes(dir) {
  try { const s = await statfs(dir); return s.bavail * s.bsize; } catch { return Infinity; }
}

const fmtGB = b => `${(b / GB).toFixed(2)} GB`;

async function separateTempDisk() {
  try { return (await stat(TEMP_DIR)).dev !== (await stat(sharedDiskDir())).dev; } catch { return false; }
}

/** Reserves `bytes` of the quota for an operation, or throws QuotaError. Returns a release function. */
async function reserveDisk(bytes, what) {
  const used = (await dirBytes(TEMP_DIR)) + (await dirBytes(BOT_FILES_DIR));
  if (used + diskReserved + bytes > DISK_QUOTA) {
    throw new QuotaError(`tg-s3 disk quota: ${what} needs ${fmtGB(bytes)}, ${fmtGB(used + diskReserved)} of ${fmtGB(DISK_QUOTA)} already in use. Retry later.`);
  }
  // MIN_FREE_DISK protects the shared disk (where the Bot API keeps its copies). When TEMP_DIR is a separate,
  // fixed-size file system of its own, it only has to hold the work itself.
  const sharedFree = await freeBytes(sharedDiskDir());
  if (sharedFree - bytes < MIN_FREE_DISK) {
    throw new QuotaError(`Not enough free disk for ${what}: ${fmtGB(sharedFree)} free, ${fmtGB(bytes)} needed, ${fmtGB(MIN_FREE_DISK)} must stay free.`);
  }
  if (await separateTempDisk()) {
    const tempFree = await freeBytes(TEMP_DIR);
    if (tempFree < bytes) throw new QuotaError(`The temp file system has ${fmtGB(tempFree)} free, ${what} needs ${fmtGB(bytes)}. Retry later.`);
  }
  diskReserved += bytes;
  let released = false;
  return () => { if (!released) { released = true; diskReserved = Math.max(0, diskReserved - bytes); } };
}

/** Deletes the Bot API's copy of a downloaded file once it is no longer needed (only inside this bot's folder). */
async function dropBotCopy(localPath) {
  if (!localPath || !localPath.startsWith(BOT_FILES_DIR + '/')) return;
  try { await unlink(localPath); } catch { /* read-only mount or already gone: the cleanup timer handles it */ }
}

// Auth middleware (timing-safe comparison to prevent secret extraction via timing oracle)
function authMiddleware(req, res, next) {
  const auth = req.headers.authorization;
  if (!auth) return res.status(401).json({ error: 'Unauthorized' });
  const expected = `Bearer ${AUTH_SECRET}`;
  if (auth.length !== expected.length ||
      !timingSafeEqual(Buffer.from(auth), Buffer.from(expected))) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}

// Auth BEFORE body parsing to prevent unauthenticated 50MB JSON DoS
app.use(authMiddleware);
app.use(express.json({ limit: '50mb' }));

// Disk quota status (for monitoring and debugging)
app.get('/api/disk', async (_req, res) => {
  const temp = await dirBytes(TEMP_DIR);
  const botCopies = await dirBytes(BOT_FILES_DIR);
  res.json({
    quotaBytes: DISK_QUOTA, usedBytes: temp + botCopies, tempBytes: temp, botCopiesBytes: botCopies,
    reservedBytes: diskReserved, minFreeBytes: MIN_FREE_DISK,
    sharedDiskFreeBytes: await freeBytes(sharedDiskDir()), tempDiskFreeBytes: await freeBytes(TEMP_DIR),
    separateTempDisk: await separateTempDisk(),
  });
});

// --- Job queue (in-memory) ---
const jobs = new Map();
const JOB_TTL_MS = 30 * 60 * 1000; // 30 minutes

// Periodically purge completed/failed jobs older than TTL
setInterval(() => {
  const now = Date.now();
  for (const [id, job] of jobs) {
    if ((job.status === 'completed' || job.status === 'failed' || job.status === 'skipped') && job.finishedAt && now - job.finishedAt > JOB_TTL_MS) {
      jobs.delete(id);
    }
  }
}, 5 * 60 * 1000); // Every 5 minutes

// Jobs run one at a time: each holds a decoded image or an ffmpeg process, so running them in parallel multiplies
// the peak memory by the number of uploads that arrive together.
let jobChain = Promise.resolve();

app.post('/api/jobs', async (req, res) => {
  const { bucket, key, tg_file_id, job_type } = req.body;
  const jobId = randomUUID();

  if (!isJobTypeEnabled(job_type)) {
    jobs.set(jobId, { status: 'skipped', bucket, key, tg_file_id, job_type, results: null, error: null, finishedAt: Date.now() });
    return res.json({ jobId, status: 'skipped' });
  }

  jobs.set(jobId, { status: 'queued', bucket, key, tg_file_id, job_type, results: null, error: null });
  res.json({ jobId, status: 'queued' });

  jobChain = jobChain.then(() => processJob(jobId).catch(err => {
    const job = jobs.get(jobId);
    if (job) { job.status = 'failed'; job.error = err.message; job.finishedAt = Date.now(); }
  }));
});

app.get('/api/jobs/:id', (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: 'Job not found' });
  res.json({ jobId: req.params.id, ...job });
});

// --- Proxy endpoints ---

// Download file from TG via Local Bot API
app.post('/api/proxy/get', async (req, res) => {
  try {
    const { file_id } = req.body;
    const tgRes = await fetchTgFile(file_id);
    if (!tgRes.ok) return res.status(502).json({ error: `TG download failed: ${tgRes.status}` });

    res.set('Content-Type', tgRes.headers.get('content-type') || 'application/octet-stream');
    if (tgRes.headers.get('content-length')) res.set('Content-Length', tgRes.headers.get('content-length'));

    const reader = tgRes.body.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) { res.end(); return; }
      const ok = res.write(Buffer.from(value));
      if (!ok) await new Promise(resolve => res.once('drain', resolve));
    }
  } catch (err) {
    if (!res.headersSent) res.status(500).json({ error: err.message });
  }
});

// Upload file to TG via Local Bot API (streams to temp file to avoid OOM on large files)
app.post('/api/proxy/put', async (req, res) => {
  const tempPath = join(TEMP_DIR, `put-${randomUUID()}`);
  try {
    const chatId = req.headers['x-chat-id'] || req.query.chat_id;
    if (!chatId) return res.status(400).json({ error: 'Missing X-Chat-Id' });
    const filename = req.headers['x-filename'] || req.query.filename || 'file';
    const contentType = req.headers['x-content-type'] || req.query.content_type || 'application/octet-stream';
    const messageThreadId = req.headers['x-message-thread-id'] || req.query.message_thread_id;

    // Stream body to temp file with backpressure to keep memory bounded
    const ws = createWriteStream(tempPath);
    await new Promise((resolve, reject) => {
      ws.on('error', reject);
      req.on('error', reject);
      req.on('data', chunk => {
        if (!ws.write(chunk)) {
          req.pause();
          ws.once('drain', () => req.resume());
        }
      });
      req.on('end', () => ws.end(resolve));
    });

    // Upload from temp file to TG via multipart form
    let fileBlob;
    try {
      const { openAsBlob } = await import('node:fs');
      fileBlob = await openAsBlob(tempPath, { type: contentType });
    } catch {
      const fileBuffer = await readFile(tempPath);
      fileBlob = new Blob([fileBuffer], { type: contentType });
    }

    const form = new FormData();
    form.append('chat_id', chatId);
    form.append('document', fileBlob, filename);
    form.append('disable_content_type_detection', 'true'); // keep .webp etc. a document, not a sticker
    if (messageThreadId) form.append('message_thread_id', messageThreadId);

    const tgRes = await fetch(`${TG_API}/bot${BOT_TOKEN}/sendDocument`, {
      method: 'POST',
      body: form,
      signal: AbortSignal.timeout(TIMEOUT_TRANSFER),
    });

    if (!tgRes.ok) {
      const text = await tgRes.text();
      return res.status(502).json({ error: `TG upload failed: ${text}` });
    }

    const data = await tgRes.json();
    const doc = data.result?.document;
    if (!doc) return res.status(502).json({ error: 'TG response missing document field' });

    res.json({
      tgChatId: chatId,
      tgMessageId: data.result.message_id,
      tgFileId: doc.file_id,
      tgFileUniqueId: doc.file_unique_id,
    });
  } catch (err) {
    if (!res.headersSent) res.status(500).json({ error: err.message });
  } finally {
    try { unlinkSync(tempPath); } catch {}
  }
});

// Range read from TG file
app.post('/api/proxy/range', async (req, res) => {
  try {
    const { file_id, start, end } = req.body;
    const tgRes = await fetchTgFile(file_id, { start, end });

    res.status(tgRes.status);
    res.set('Content-Type', tgRes.headers.get('content-type') || 'application/octet-stream');
    if (tgRes.headers.get('content-length')) res.set('Content-Length', tgRes.headers.get('content-length'));
    if (tgRes.headers.get('content-range')) res.set('Content-Range', tgRes.headers.get('content-range'));

    const reader = tgRes.body.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) { res.end(); return; }
      const ok = res.write(Buffer.from(value));
      if (!ok) await new Promise(resolve => res.once('drain', resolve));
    }
  } catch (err) {
    if (!res.headersSent) res.status(500).json({ error: err.message });
  }
});

// Decodes the aws-chunked body format S3 clients use with STREAMING-* payload hashes:
// "<hex size>[;chunk-signature=...]\r\n<data>\r\n" repeated, ending with a 0-size chunk and optional trailers.
// Chunk signatures are not checked (TLS protects the transfer); the decoded data is hashed and verified instead.
class AwsChunkedDecoder extends Transform {
  constructor() {
    super();
    this.buf = Buffer.alloc(0);
    this.remaining = 0;   // data bytes left in the current chunk
    this.needCrlf = false; // a chunk's data ended; its trailing CRLF is next
    this.done = false;
  }
  _transform(chunk, _enc, cb) {
    if (this.done) return cb();
    this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk;
    try {
      for (;;) {
        if (this.remaining > 0) {
          if (this.buf.length === 0) break;
          const take = Math.min(this.remaining, this.buf.length);
          this.push(this.buf.subarray(0, take));
          this.buf = this.buf.subarray(take);
          this.remaining -= take;
          if (this.remaining === 0) this.needCrlf = true;
          continue;
        }
        if (this.needCrlf) {
          if (this.buf.length < 2) break;
          if (this.buf[0] !== 13 || this.buf[1] !== 10) throw new Error('aws-chunked: missing CRLF after chunk data');
          this.buf = this.buf.subarray(2);
          this.needCrlf = false;
          continue;
        }
        const eol = this.buf.indexOf('\r\n');
        if (eol < 0) {
          if (this.buf.length > 4096) throw new Error('aws-chunked: chunk header too long');
          break;
        }
        const size = parseInt(this.buf.subarray(0, eol).toString('latin1').split(';')[0].trim(), 16);
        if (!Number.isFinite(size) || size < 0) throw new Error('aws-chunked: bad chunk size');
        this.buf = this.buf.subarray(eol + 2);
        if (size === 0) { this.done = true; break; } // trailers, if any, are ignored
        this.remaining = size;
      }
      cb();
    } catch (err) { cb(err); }
  }
  _flush(cb) {
    cb(this.done || (this.remaining === 0 && !this.needCrlf && this.buf.length === 0) ? null : new Error('aws-chunked: body ended mid-chunk'));
  }
}

// POSTs a multipart form whose last field is a file streamed from disk. node:http streams the body; fetch() keeps
// the whole request body in memory (1.5 GB for a 1.5 GB file), which got the processor killed at its memory limit.
function postFileForm(url, fields, fileField, filePath, filename, contentType) {
  const boundary = `----tgs3${randomUUID()}`;
  const safeName = String(filename).replace(/"/g, '%22').replace(/\r/g, '%0D').replace(/\n/g, '%0A');
  let head = '';
  for (const [name, value] of Object.entries(fields)) {
    if (value === undefined || value === null || value === '') continue;
    head += `--${boundary}\r\nContent-Disposition: form-data; name="${name}"\r\n\r\n${value}\r\n`;
  }
  head += `--${boundary}\r\nContent-Disposition: form-data; name="${fileField}"; filename="${safeName}"\r\nContent-Type: ${contentType}\r\n\r\n`;
  const headBuf = Buffer.from(head, 'utf8');
  const tailBuf = Buffer.from(`\r\n--${boundary}--\r\n`);
  return new Promise((resolve, reject) => {
    stat(filePath).then(st => {
      const target = new URL(url);
      const req = (target.protocol === 'https:' ? https : http).request(target, {
        method: 'POST',
        headers: {
          'Content-Type': `multipart/form-data; boundary=${boundary}`,
          'Content-Length': String(headBuf.length + st.size + tailBuf.length),
        },
        timeout: TIMEOUT_TRANSFER,
      }, res => {
        let text = '';
        res.setEncoding('utf8');
        res.on('data', c => { text += c; });
        res.on('end', () => resolve({ status: res.statusCode, text }));
        res.on('error', reject);
      });
      req.on('timeout', () => req.destroy(new Error('Telegram upload timed out')));
      req.on('error', reject);
      async function* body() { yield headBuf; yield* createReadStream(filePath); yield tailBuf; }
      pipeline(Readable.from(body()), req).catch(reject);
    }, reject);
  });
}

// Sends a file from disk as a document through the Bot API, waiting out Telegram's flood limit (429) up to
// maxWaitSec per attempt. Returns { ok: true, data } or { ok: false, status, code, error, retryAfter }.
async function tgSendDocument({ chatId, filePath, filename, contentType, threadId }, { maxWaitSec = 30, attempts = 3 } = {}) {
  let last;
  for (let attempt = 0; attempt < attempts; attempt++) {
    const r = await postFileForm(`${TG_API}/bot${BOT_TOKEN}/sendDocument`, {
      chat_id: chatId,
      message_thread_id: threadId ? String(threadId) : undefined,
      disable_content_type_detection: 'true', // keep .webp etc. a document, not a sticker
    }, 'document', filePath, filename, contentType || 'application/octet-stream');
    if (r.status >= 200 && r.status < 300) return { ok: true, data: JSON.parse(r.text) };
    const text = r.text;
    if (r.status !== 429) return { ok: false, status: 502, code: 'InternalError', error: `TG upload failed (${r.status}): ${text.slice(0, 300)}` };
    let retryAfter = 5;
    try { retryAfter = JSON.parse(text).parameters?.retry_after ?? retryAfter; } catch {}
    last = { ok: false, status: 503, code: 'SlowDown', error: `Telegram rate limit, retry after ${retryAfter}s`, retryAfter };
    if (retryAfter > maxWaitSec || attempt === attempts - 1) break;
    await new Promise(r => setTimeout(r, retryAfter * 1000 + 250));
  }
  return last;
}

// File upload: stream body to disk, compute ETag, optionally encrypt, upload to TG.
// The Worker forwards request bodies here without reading them, so checksums cost the Worker no CPU.
// Used for PutObject and UploadPart of any size up to 2GB.
// Optional headers: X-Body-Encoding: aws-chunked (decode first), X-Content-MD5 (base64), X-Content-SHA256 (hex),
// X-Expected-Size (decoded size). A mismatch returns 400 with an S3 error code in `code`.
app.post('/api/proxy/put-full', async (req, res) => {
  const rawPath = join(TEMP_DIR, `putraw-${randomUUID()}`);
  const encPath = join(TEMP_DIR, `putenc-${randomUUID()}`);
  let releaseDisk = () => {};
  try {
    const chatId = req.headers['x-chat-id'];
    let filename = req.headers['x-filename'] || 'file';
    try { filename = decodeURIComponent(filename); } catch { /* sent unencoded */ }
    const contentType = req.headers['x-content-type'] || 'application/octet-stream';
    const messageThreadId = req.headers['x-message-thread-id'];
    const sseKey = req.headers['x-sse-key']; // SSE-C key (base64)
    const sseS3Key = req.headers['x-sse-s3-key']; // SSE-S3 master key (base64)
    const expectedMd5 = req.headers['x-content-md5'];
    const expectedSha256 = (req.headers['x-content-sha256'] || '').toLowerCase();
    const expectedSize = req.headers['x-expected-size'] ? Number(req.headers['x-expected-size']) : null;
    const chunked = req.headers['x-body-encoding'] === 'aws-chunked';

    if (!chatId) return res.status(400).json({ code: 'InvalidRequest', error: 'Missing X-Chat-Id' });

    // 0. Reserve disk: the body, an encrypted copy if any, and the Bot API's own copy while it uploads
    const declared = expectedSize ?? (Number(req.headers['content-length']) || null);
    const limit = declared ?? 2 * 1024 ** 3;
    try {
      releaseDisk = await reserveDisk(limit * ((sseKey || sseS3Key) ? 3 : 2), `an upload of ${fmtGB(limit)}`);
    } catch (err) {
      if (err instanceof QuotaError) return res.status(503).json({ code: err.code, error: err.message, retryAfter: err.retryAfter });
      throw err;
    }

    // 1. Stream body to temp file with backpressure, computing MD5 (and SHA-256 when asked) on the way
    const hash = createHash('md5');
    const sha = expectedSha256 ? createHash('sha256') : null;
    let size = 0;
    const meter = new Transform({
      transform(chunk, _enc, cb) {
        if (size + chunk.length > limit) return cb(new Error(`Body is larger than the declared ${limit} bytes.`));
        hash.update(chunk);
        if (sha) sha.update(chunk);
        size += chunk.length;
        cb(null, chunk);
      },
    });
    const stages = chunked ? [req, new AwsChunkedDecoder(), meter] : [req, meter];
    try {
      await pipeline(...stages, createWriteStream(rawPath));
    } catch (err) {
      return res.status(400).json({ code: 'IncompleteBody', error: err.message });
    }

    const digestBuf = hash.digest();
    const md5Hex = digestBuf.toString('hex');
    const md5Base64 = digestBuf.toString('base64');
    const etag = `"${md5Hex}"`;

    // 2. Validate size and checksums if provided
    if (expectedSize !== null && size !== expectedSize) {
      return res.status(400).json({ code: 'IncompleteBody', error: `Expected ${expectedSize} bytes, received ${size}.` });
    }
    if (expectedMd5 && md5Base64 !== expectedMd5) {
      return res.status(400).json({ code: 'BadDigest', error: 'The Content-MD5 you specified did not match what we received.' });
    }
    if (sha && sha.digest('hex') !== expectedSha256) {
      return res.status(400).json({ code: 'XAmzContentSHA256Mismatch', error: "The provided 'x-amz-content-sha256' header does not match what was computed." });
    }
    if (size === 0) {
      return res.status(400).json({ code: 'InvalidRequest', error: 'Empty bodies are not uploaded to Telegram.' });
    }

    // 3. Optionally encrypt (stream to another temp file)
    let uploadPath = rawPath;
    const encryptKey = sseKey || sseS3Key;
    if (encryptKey && size > 0) {
      await streamEncryptFile(rawPath, encPath, encryptKey);
      uploadPath = encPath;
    }

    // 4. Upload to TG via sendDocument (the request is still open, so flood waits stay short)
    const sent = await tgSendDocument(
      { chatId, filePath: uploadPath, filename, contentType, threadId: messageThreadId },
      { maxWaitSec: 20, attempts: 3 },
    );
    if (!sent.ok) return res.status(sent.status).json({ code: sent.code, error: sent.error, retryAfter: sent.retryAfter });

    const data = sent.data;
    const doc = data.result?.document;
    if (!doc) return res.status(502).json({ code: 'InternalError', error: 'TG response missing document field' });

    res.json({
      etag,
      tgChatId: chatId,
      tgMessageId: data.result.message_id,
      tgFileId: doc.file_id,
      tgFileUniqueId: doc.file_unique_id,
      size,
    });
  } catch (err) {
    if (!res.headersSent) res.status(500).json({ error: err.message });
  } finally {
    try { unlinkSync(rawPath); } catch {}
    try { unlinkSync(encPath); } catch {}
    releaseDisk();
  }
});

// Stream-encrypt a file: output format = [12-byte IV][ciphertext][16-byte GCM auth tag]
// Compatible with Web Crypto AES-GCM used by Worker's sse.ts
async function streamEncryptFile(inputPath, outputPath, keyBase64) {
  const iv = randomBytes(12);
  const keyBuf = Buffer.from(keyBase64, 'base64');
  if (keyBuf.length !== 32) throw new Error(`AES-256 key must be 32 bytes, got ${keyBuf.length}`);
  const cipher = createCipheriv('aes-256-gcm', keyBuf, iv);

  const rs = createReadStream(inputPath);
  const ws = createWriteStream(outputPath);

  await new Promise((resolve, reject) => {
    ws.on('error', reject);
    rs.on('error', reject);
    cipher.on('error', reject);
    ws.write(iv); // IV header
    rs.pipe(cipher);
    cipher.on('data', chunk => {
      const ok = ws.write(chunk);
      if (!ok) { cipher.pause(); ws.once('drain', () => cipher.resume()); }
    });
    cipher.on('end', () => {
      ws.write(cipher.getAuthTag()); // 16-byte auth tag
      ws.end(resolve);
    });
  });
}

// Download, decrypt, and stream back (for encrypted files >20MB that Worker can't buffer)
// Uses temp files + streaming decryption to avoid holding entire file in memory.
// AES-256-GCM format: [12-byte IV][ciphertext][16-byte auth tag]
app.post('/api/proxy/get-decrypt', async (req, res) => {
  const encPath = join(TEMP_DIR, `enc-${randomUUID()}`);
  const decPath = join(TEMP_DIR, `dec-${randomUUID()}`);
  try {
    const { file_id, key_base64, range_start, range_end } = req.body;
    if (!file_id || !key_base64) {
      return res.status(400).json({ error: 'Missing file_id or key_base64' });
    }

    // 1. Download encrypted file from TG to temp file (streaming, no memory buffering)
    const tgRes = await fetchTgFile(file_id);
    if (!tgRes.ok) return res.status(502).json({ error: `TG download failed: ${tgRes.status}` });

    const ws = createWriteStream(encPath);
    const reader = tgRes.body.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const ok = ws.write(Buffer.from(value));
      if (!ok) await new Promise(resolve => ws.once('drain', resolve));
    }
    await new Promise((resolve, reject) => { ws.end(resolve); ws.on('error', reject); });

    // 2. Read IV (first 12 bytes) and auth tag (last 16 bytes) from encrypted file
    const encStat = await stat(encPath);
    const encSize = encStat.size;
    if (encSize < 28) return res.status(500).json({ error: 'Encrypted data too short' });

    const fh = await openFile(encPath, 'r');
    const ivBuf = Buffer.alloc(12);
    await fh.read(ivBuf, 0, 12, 0);
    const authTagBuf = Buffer.alloc(16);
    await fh.read(authTagBuf, 0, 16, encSize - 16);
    await fh.close();

    // 3. Stream-decrypt ciphertext to another temp file
    const keyBuf = Buffer.from(key_base64, 'base64');
    if (keyBuf.length !== 32) return res.status(400).json({ error: `AES-256 key must be 32 bytes, got ${keyBuf.length}` });
    const decipher = createDecipheriv('aes-256-gcm', keyBuf, ivBuf);
    decipher.setAuthTag(authTagBuf);

    // Ciphertext is between IV and auth tag
    const cipherStream = createReadStream(encPath, { start: 12, end: encSize - 17 });
    const decStream = createWriteStream(decPath);

    try {
      await pipeline(cipherStream, decipher, decStream);
    } catch {
      return res.status(403).json({ error: 'Decryption failed. The provided encryption key does not match.' });
    }

    // 4. Serve decrypted file (with optional range support)
    const decStat = await stat(decPath);
    const decSize = decStat.size;

    if (range_start !== undefined && range_end !== undefined) {
      const start = parseInt(range_start, 10);
      const end = Math.min(parseInt(range_end, 10), decSize - 1);
      if (!Number.isFinite(start) || !Number.isFinite(end) || start < 0 || start > end || start >= decSize) {
        return res.status(416).json({ error: 'Range not satisfiable' });
      }
      res.set('Content-Range', `bytes ${start}-${end}/${decSize}`);
      res.set('Content-Length', (end - start + 1).toString());
      res.set('X-Decrypted-Size', decSize.toString());
      res.status(206);
      const rangeStream = createReadStream(decPath, { start, end });
      await pipeline(rangeStream, res);
    } else {
      res.set('Content-Length', decSize.toString());
      res.set('X-Decrypted-Size', decSize.toString());
      const fullStream = createReadStream(decPath);
      await pipeline(fullStream, res);
    }
  } catch (err) {
    if (!res.headersSent) res.status(500).json({ error: err.message });
  } finally {
    try { unlinkSync(encPath); } catch {}
    try { unlinkSync(decPath); } catch {}
  }
});

// Consolidate multipart parts: download all parts from TG, combine, optionally encrypt, upload as single file.
// Throws an Error with .status for request errors. Can take minutes for large files.
async function runConsolidate(body) {
  const { file_ids, chat_id, filename, content_type, message_thread_id, sse_key, sse_s3_key } = body;
  if (!file_ids || !Array.isArray(file_ids) || !chat_id) {
    throw Object.assign(new Error('Missing file_ids or chat_id'), { status: 400 });
  }
  if (file_ids.length > 10000) {
    throw Object.assign(new Error('file_ids exceeds maximum of 10000 parts'), { status: 400 });
  }
  const tempPath = join(TEMP_DIR, `consolidate-${randomUUID()}`);
  const encPath = join(TEMP_DIR, `consolidate-enc-${randomUUID()}`);
  // Disk needed: the joined file, an encrypted copy if any, and the Bot API's copy while it uploads.
  // Each part's Bot API copy is deleted as soon as it is read, so parts add at most one part at a time.
  const totalSize = Number(body.total_size) || 2 * 1024 ** 3;
  const releaseDisk = await reserveDisk(totalSize * ((sse_key || sse_s3_key) ? 3 : 2), `joining ${file_ids.length} parts (${fmtGB(totalSize)})`);
  try {
    // Download all parts and stream them into one temp file
    const hash = createHash('md5');
    const writeStream = createWriteStream(tempPath);
    const writeError = new Promise((_, reject) => writeStream.on('error', reject));
    writeError.catch(() => {});
    for (const fileId of file_ids) {
      const tgRes = await fetchTgFile(fileId);
      if (!tgRes.ok || !tgRes.body) throw new Error(`Download part failed: ${tgRes.status}`);
      const reader = tgRes.body.getReader();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        const buf = Buffer.from(value);
        hash.update(buf);
        if (!writeStream.write(buf)) await Promise.race([new Promise(r => writeStream.once('drain', r)), writeError]);
      }
      await dropBotCopy(tgRes.tgLocalPath);
    }
    await Promise.race([new Promise(resolve => writeStream.end(resolve)), writeError]);
    const etag = hash.digest('hex');

    // Optionally encrypt the consolidated file (SSE-C or SSE-S3)
    const encryptKey = sse_key || sse_s3_key;
    let uploadPath = tempPath;
    if (encryptKey) {
      await streamEncryptFile(tempPath, encPath, encryptKey);
      uploadPath = encPath;
    }

    // Upload the combined file from disk. This runs in the background, so long flood waits are fine.
    const sent = await tgSendDocument(
      { chatId: chat_id, filePath: uploadPath, filename, contentType: content_type, threadId: message_thread_id },
      { maxWaitSec: 300, attempts: 5 },
    );
    if (!sent.ok) throw new Error(sent.error);
    const doc = sent.data.result?.document;
    if (!doc) throw new Error('TG response missing document field');

    return {
      tgChatId: chat_id,
      tgMessageId: sent.data.result.message_id,
      tgFileId: doc.file_id,
      tgFileUniqueId: doc.file_unique_id,
      etag,
    };
  } finally {
    try { unlinkSync(tempPath); } catch {}
    try { unlinkSync(encPath); } catch {}
    releaseDisk();
  }
}

// Synchronous form (kept for older Workers). Large files outlast Cloudflare's ~100 s request limit; use the job form.
app.post('/api/proxy/consolidate', async (req, res) => {
  try {
    res.json(await runConsolidate(req.body));
  } catch (err) {
    if (!res.headersSent) res.status(err.status || 500).json({ error: err.message });
  }
});

// Background form: POST starts (or rejoins) the job for `job_key`; GET waits up to `wait` seconds (max 55) for it.
// A retried CompleteMultipartUpload reuses the running or finished job instead of joining the parts twice.
const consolidateJobs = new Map(); // job_key -> { status: 'running'|'done'|'failed', result, error, promise, finishedAt }
const CONSOLIDATE_JOB_TTL_MS = 60 * 60 * 1000;

function publicJob(key, job) {
  return { jobKey: key, status: job.status, result: job.result ?? null, error: job.error ?? null };
}

app.post('/api/proxy/consolidate-job', (req, res) => {
  const key = req.body?.job_key;
  if (!key || typeof key !== 'string') return res.status(400).json({ error: 'Missing job_key' });
  let job = consolidateJobs.get(key);
  if (!job || job.status === 'failed') {
    job = { status: 'running' };
    job.promise = runConsolidate(req.body)
      .then(result => { job.status = 'done'; job.result = result; })
      .catch(err => { job.status = 'failed'; job.error = err.message; console.error(`consolidate ${key} failed:`, err.message); })
      .finally(() => { job.finishedAt = Date.now(); });
    consolidateJobs.set(key, job);
  }
  res.json(publicJob(key, job));
});

app.get('/api/proxy/consolidate-job/:key', async (req, res) => {
  const key = req.params.key;
  const job = consolidateJobs.get(key);
  if (!job) return res.status(404).json({ error: 'No such job' });
  const waitMs = Math.min(Math.max(Number(req.query.wait) || 0, 0), 55) * 1000;
  if (job.status === 'running' && waitMs > 0) {
    await Promise.race([job.promise, new Promise(r => setTimeout(r, waitMs))]);
  }
  res.json(publicJob(key, job));
});

setInterval(() => {
  const now = Date.now();
  for (const [key, job] of consolidateJobs) {
    if (job.finishedAt && now - job.finishedAt > CONSOLIDATE_JOB_TTL_MS) consolidateJobs.delete(key);
  }
}, 5 * 60 * 1000);

// Image resize endpoint (synchronous, for Worker to call directly)
app.get('/api/image/resize', async (req, res) => {
  try {
    const { tg_file_id, width, format, quality } = req.query;
    const tgRes = await fetchTgFile(tg_file_id);
    if (!tgRes.ok) return res.status(502).json({ error: 'TG download failed' });

    const inputBuffer = Buffer.from(await tgRes.arrayBuffer());
    let img = sharp(inputBuffer);

    if (width) {
      const w = Math.min(Math.max(parseInt(width, 10) || 0, 1), 8192);
      img = img.resize(w, null, { withoutEnlargement: true });
    }

    const fmt = format || 'jpeg';
    const q = quality ? parseInt(quality, 10) : undefined;
    switch (fmt) {
      case 'webp': img = img.webp({ quality: q || 80 }); break;
      case 'png': img = img.png(); break;
      case 'avif': img = img.avif({ quality: q || 65 }); break;
      default: img = img.jpeg({ quality: q || 90, mozjpeg: true }); break;
    }

    const output = await img.toBuffer();
    const mimeMap = { jpeg: 'image/jpeg', webp: 'image/webp', png: 'image/png', avif: 'image/avif' };

    res.set('Content-Type', mimeMap[fmt] || 'image/jpeg');
    res.set('Content-Length', output.length.toString());
    res.send(output);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Job processing ---

async function processJob(jobId) {
  const job = jobs.get(jobId);
  job.status = 'processing';

  const inputPath = join(TEMP_DIR, `${jobId}-input`);
  const outputPath = join(TEMP_DIR, `${jobId}-output.mp4`);
  const posterPath = join(TEMP_DIR, `${jobId}-poster.jpg`);
  const results = [];

  try {
    // Stream the source file from TG to disk instead of holding it in memory (videos can be up to 2GB)
    const tgRes = await fetchTgFile(job.tg_file_id);
    if (!tgRes.ok || !tgRes.body) throw new Error(`TG download failed: ${tgRes.status}`);
    await pipeline(Readable.fromWeb(tgRes.body), createWriteStream(inputPath));

    switch (job.job_type) {
      case 'image_convert': {
        // HEIC -> JPEG full size
        const fullJpeg = await sharp(inputPath).jpeg({ quality: 90, mozjpeg: true }).toBuffer();
        results.push(await uploadDerivative(job, 'full.jpg', fullJpeg, 'image/jpeg'));

        // WebP thumbnail 400px
        const thumb400 = await sharp(inputPath)
          .resize(400, null, { withoutEnlargement: true })
          .webp({ quality: 80 })
          .toBuffer();
        results.push(await uploadDerivative(job, 'thumb_400.webp', thumb400, 'image/webp'));

        // WebP thumbnail 200px
        const thumb200 = await sharp(inputPath)
          .resize(200, null, { withoutEnlargement: true })
          .webp({ quality: 75 })
          .toBuffer();
        results.push(await uploadDerivative(job, 'thumb_200.webp', thumb200, 'image/webp'));

        // Extract metadata
        const metadata = await sharp(inputPath).metadata();
        const metaJson = JSON.stringify({
          width: metadata.width, height: metadata.height,
          format: metadata.format, density: metadata.density,
          hasAlpha: metadata.hasAlpha, space: metadata.space,
        });
        results.push(await uploadDerivative(job, 'metadata.json', Buffer.from(metaJson), 'application/json'));
        break;
      }

      case 'video_transcode': {
        // Transcode to H.264 MP4
        await execFileAsync('ffmpeg', [
          '-i', inputPath,
          '-c:v', 'libx264', '-preset', 'medium', '-crf', '23',
          '-c:a', 'aac', '-b:a', '128k',
          '-movflags', '+faststart',
          '-y', outputPath,
        ]);

        // Extract poster frame
        await execFileAsync('ffmpeg', [
          '-i', inputPath,
          '-ss', '00:00:01', '-vframes', '1',
          '-y', posterPath,
        ]);

        // Upload the MP4 from disk; it can be as large as the source
        results.push(await uploadDerivative(job, 'video.mp4', { path: outputPath }, 'video/mp4'));

        if (existsSync(posterPath)) {
          const posterBuffer = await readFile(posterPath);
          results.push(await uploadDerivative(job, 'poster.jpg', posterBuffer, 'image/jpeg'));
        }
        break;
      }

      case 'live_photo': {
        // Input expected as HEIC; MOV should be uploaded separately with a paired key
        const jpeg = await sharp(inputPath).jpeg({ quality: 90 }).toBuffer();
        results.push(await uploadDerivative(job, 'still.jpg', jpeg, 'image/jpeg'));

        const thumb = await sharp(inputPath)
          .resize(400, null, { withoutEnlargement: true })
          .webp({ quality: 80 })
          .toBuffer();
        results.push(await uploadDerivative(job, 'thumb_400.webp', thumb, 'image/webp'));
        break;
      }
    }
  } finally {
    for (const p of [inputPath, outputPath, posterPath]) {
      try { unlinkSync(p); } catch {}
    }
  }

  job.status = 'completed';
  job.results = results;
  job.finishedAt = Date.now();
}

// `source` is a Buffer, or { path } for a file on disk that should not be loaded into memory
async function uploadDerivative(job, derivativeName, source, contentType) {
  const key = `${job.key}._derivatives/${derivativeName}`;

  const form = new FormData();
  if (!process.env.DEFAULT_CHAT_ID) throw new Error('DEFAULT_CHAT_ID is not configured');
  const blob = Buffer.isBuffer(source)
    ? new Blob([source], { type: contentType })
    : await openAsBlob(source.path, { type: contentType });
  form.append('chat_id', process.env.DEFAULT_CHAT_ID);
  form.append('document', blob, derivativeName);
  form.append('disable_content_type_detection', 'true'); // keep .webp etc. a document, not a sticker
  form.append('caption', key);

  const tgRes = await fetch(`${TG_API}/bot${BOT_TOKEN}/sendDocument`, {
    method: 'POST',
    body: form,
    signal: AbortSignal.timeout(TIMEOUT_TRANSFER),
  });

  if (!tgRes.ok) {
    const text = await tgRes.text();
    throw new Error(`Failed to upload derivative ${derivativeName}: ${text}`);
  }

  const data = await tgRes.json();
  const doc = data.result?.document;
  if (!doc) throw new Error(`TG response missing document field for ${derivativeName}`);

  return {
    key,
    contentType,
    size: blob.size,
    tgFileId: doc.file_id,
    tgMessageId: data.result.message_id,
  };
}

async function getFilePath(fileId) {
  const res = await fetch(`${TG_API}/bot${BOT_TOKEN}/getFile`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ file_id: fileId }),
    signal: AbortSignal.timeout(TIMEOUT_API),
  });
  if (!res.ok) throw new Error(`getFile failed: ${res.status}`);
  const data = await res.json();
  if (!data.ok || !data.result?.file_path) throw new Error('No file_path in getFile response');
  return data.result.file_path;
}

// Download a Telegram file and return a fetch Response, optionally for a byte range.
// A Local Bot API server started with --local does not serve files over HTTP: getFile returns an absolute path
// on its own disk instead. In that case the file is read from disk, so the Bot API's data directory must be
// mounted into this container at the same path (see docker-compose.yml).
async function fetchTgFile(fileId, range) {
  const filePath = await getFilePath(fileId);
  if (!isAbsolute(filePath)) {
    const headers = range ? { 'Range': `bytes=${range.start}-${range.end}` } : {};
    return fetch(`${TG_API}/file/bot${BOT_TOKEN}/${filePath}`, { headers, signal: AbortSignal.timeout(TIMEOUT_TRANSFER) });
  }

  let size;
  try {
    size = (await stat(filePath)).size;
  } catch (err) {
    throw new Error(`The Bot API runs in --local mode and returned ${filePath}, which this container cannot read ` +
      `(${err.code}). Mount the Bot API data directory into the processor at the same path.`);
  }
  const headers = { 'Content-Type': 'application/octet-stream' };
  if (!range) {
    headers['Content-Length'] = String(size);
    const full = new Response(Readable.toWeb(createReadStream(filePath)), { status: 200, headers });
    full.tgLocalPath = filePath; // lets callers delete the Bot API's copy when done with it
    return full;
  }
  const start = Number(range.start);
  const end = Math.min(Number(range.end), size - 1);
  if (!Number.isInteger(start) || !Number.isInteger(end) || start < 0 || start > end) {
    return new Response(null, { status: 416, headers: { 'Content-Range': `bytes */${size}` } });
  }
  headers['Content-Length'] = String(end - start + 1);
  headers['Content-Range'] = `bytes ${start}-${end}/${size}`;
  const part = new Response(Readable.toWeb(createReadStream(filePath, { start, end })), { status: 206, headers });
  part.tgLocalPath = filePath;
  return part;
}

// HOST limits which interface the processor listens on. Leave it unset inside Docker's default bridge network;
// set it to 127.0.0.1 when the container uses the host network and a local tunnel connector reaches it.
const HOST = process.env.HOST || '0.0.0.0';
app.listen(PORT, HOST, () => {
  console.log(`tg-s3-processor running on ${HOST}:${PORT}`);
});
