import type { Env } from '../types';
import { VPS_PROXY_TIMEOUT, VPS_LONG_TIMEOUT } from '../constants';

export interface MediaJobRequest {
  bucket: string;
  key: string;
  tgFileId: string;
  jobType: 'image_convert' | 'video_transcode' | 'live_photo';
}

export interface MediaJobResponse {
  jobId: string;
  status: 'queued' | 'processing' | 'completed' | 'failed';
  results?: Array<{
    key: string;
    contentType: string;
    size: number;
    tgFileId: string;
    tgMessageId: number;
  }>;
  error?: string;
}

const VPS_MAX_RETRIES = 2;

export interface ConsolidateJob {
  jobKey: string;
  status: 'running' | 'done' | 'failed';
  result: { tgChatId: string; tgMessageId: number; tgFileId: string; tgFileUniqueId: string; etag: string } | null;
  error: string | null;
}

/** A VPS request that failed; `code` is an S3 error code when the VPS gave one (BadDigest, SlowDown, ...). */
export class VpsError extends Error {
  constructor(message: string, readonly status: number, readonly code: string, readonly retryAfter?: number) {
    super(message);
  }

  static async from(res: Response, what: string): Promise<VpsError> {
    const text = await res.text();
    let code = 'InternalError';
    let message = `VPS ${what} failed (${res.status}): ${text.slice(0, 300)}`;
    let retryAfter: number | undefined;
    try {
      const j = JSON.parse(text) as { code?: string; error?: string; retryAfter?: number };
      if (j.code) code = j.code;
      if (j.error) message = j.error;
      if (typeof j.retryAfter === 'number') retryAfter = j.retryAfter;
    } catch { /* not JSON (for example a Cloudflare error page) */ }
    return new VpsError(message, res.status, code, retryAfter);
  }

  /** The S3 error response for this failure: client errors keep their code, the rest become 5xx. */
  toS3Status(): number {
    if (this.status === 400) return 400;
    if (this.code === 'SlowDown' || this.status === 503) return 503;
    return 500;
  }
}

export class VpsClient {
  private baseUrl: string;
  private secret: string;

  constructor(env: Env) {
    this.baseUrl = env.VPS_URL!;
    this.secret = env.VPS_SECRET!;
  }

  private headers(): Record<string, string> {
    return {
      'Authorization': `Bearer ${this.secret}`,
      'Content-Type': 'application/json',
    };
  }

  /** Retry on fast transient failures (connection refused, DNS, quick 502/503). */
  private async withRetry<T>(fn: () => Promise<T>): Promise<T> {
    let lastError: Error | null = null;
    for (let attempt = 0; attempt < VPS_MAX_RETRIES; attempt++) {
      try {
        return await fn();
      } catch (e) {
        lastError = e as Error;
        // Only retry on network/timeout errors (not HTTP 4xx application errors)
        const isNetworkError = e instanceof TypeError; // fetch() network failure
        const isTimeout = e instanceof DOMException && e.name === 'AbortError';
        if ((isNetworkError || isTimeout) && attempt < VPS_MAX_RETRIES - 1) {
          await new Promise(r => setTimeout(r, 500 * (attempt + 1)));
          continue;
        }
        throw e;
      }
    }
    throw lastError!;
  }

  async submitJob(job: MediaJobRequest): Promise<MediaJobResponse> {
    return this.withRetry(async () => {
      const res = await fetch(`${this.baseUrl}/api/jobs`, {
        method: 'POST',
        headers: this.headers(),
        body: JSON.stringify({
          bucket: job.bucket,
          key: job.key,
          tg_file_id: job.tgFileId,
          job_type: job.jobType,
        }),
        signal: AbortSignal.timeout(VPS_PROXY_TIMEOUT),
      });
      if (!res.ok) throw new Error(`VPS job submit failed: ${res.status}`);
      return res.json();
    });
  }

  async getJobStatus(jobId: string): Promise<MediaJobResponse> {
    return this.withRetry(async () => {
      const res = await fetch(`${this.baseUrl}/api/jobs/${jobId}`, {
        headers: this.headers(),
        signal: AbortSignal.timeout(VPS_PROXY_TIMEOUT),
      });
      if (!res.ok) throw new Error(`VPS job query failed: ${res.status}`);
      return res.json();
    });
  }

  async proxyGet(fileId: string): Promise<Response> {
    return this.withRetry(async () => {
      const res = await fetch(`${this.baseUrl}/api/proxy/get`, {
        method: 'POST',
        headers: this.headers(),
        body: JSON.stringify({ file_id: fileId }),
        signal: AbortSignal.timeout(VPS_PROXY_TIMEOUT),
      });
      if (!res.ok) throw new Error(`VPS proxy get failed: ${res.status}`);
      return res;
    });
  }

  async proxyRange(fileId: string, start: number, end: number): Promise<Response> {
    return this.withRetry(async () => {
      const res = await fetch(`${this.baseUrl}/api/proxy/range`, {
        method: 'POST',
        headers: this.headers(),
        body: JSON.stringify({ file_id: fileId, start, end }),
        signal: AbortSignal.timeout(VPS_PROXY_TIMEOUT),
      });
      if (!res.ok) throw new Error(`VPS proxy range failed: ${res.status}`);
      return res;
    });
  }

  async imageResize(fileId: string, width?: string | null, format?: string | null, quality?: string | null): Promise<Response> {
    return this.withRetry(async () => {
      const params = new URLSearchParams();
      params.set('tg_file_id', fileId);
      if (width) params.set('width', width);
      if (format) params.set('format', format);
      if (quality) params.set('quality', quality);
      const res = await fetch(`${this.baseUrl}/api/image/resize?${params}`, {
        headers: { 'Authorization': `Bearer ${this.secret}` },
        signal: AbortSignal.timeout(VPS_PROXY_TIMEOUT),
      });
      if (!res.ok) throw new Error(`VPS image resize failed: ${res.status}`);
      return res;
    });
  }

  /**
   * Stream a request body to the VPS, which hashes, verifies and uploads it to TG. The Worker only forwards
   * bytes, so this costs it almost no CPU regardless of size. Throws VpsError with the VPS's S3 error code.
   */
  async proxyPutFull(
    body: ReadableStream<Uint8Array>,
    chatId: string, filename: string, contentType: string,
    contentLength: number,
    options?: {
      messageThreadId?: number | null;
      sseKeyBase64?: string;
      sseS3KeyBase64?: string;
      contentMd5?: string;
      contentSha256?: string;   // hex SHA-256 of the decoded body, verified by the VPS
      expectedSize?: number;    // decoded size, verified by the VPS
      awsChunked?: boolean;     // body is in aws-chunked format; the VPS decodes it
    },
  ): Promise<Response> {
    // No retry: body stream can only be consumed once
    const headers: Record<string, string> = {
      'Authorization': `Bearer ${this.secret}`,
      'Content-Type': 'application/octet-stream',
      'X-Chat-Id': chatId,
      // Header values must be Latin-1; keys may be any Unicode, so send the name percent-encoded
      'X-Filename': encodeURIComponent(filename),
      'X-Content-Type': contentType,
    };
    if (contentLength > 0) headers['Content-Length'] = contentLength.toString();
    if (options?.messageThreadId) headers['X-Message-Thread-Id'] = options.messageThreadId.toString();
    if (options?.sseKeyBase64) headers['X-SSE-Key'] = options.sseKeyBase64;
    if (options?.sseS3KeyBase64) headers['X-SSE-S3-Key'] = options.sseS3KeyBase64;
    if (options?.contentMd5) headers['X-Content-MD5'] = options.contentMd5;
    if (options?.contentSha256) headers['X-Content-SHA256'] = options.contentSha256;
    if (options?.expectedSize !== undefined) headers['X-Expected-Size'] = options.expectedSize.toString();
    if (options?.awsChunked) headers['X-Body-Encoding'] = 'aws-chunked';

    const res = await fetch(`${this.baseUrl}/api/proxy/put-full`, {
      method: 'POST',
      headers,
      body,
      signal: AbortSignal.timeout(VPS_LONG_TIMEOUT),
    });
    if (!res.ok) throw await VpsError.from(res, 'put-full');
    return res;
  }

  /** Start (or rejoin) a background consolidation on the VPS, keyed by the multipart upload ID. */
  async consolidateJobStart(
    jobKey: string, fileIds: string[], totalSize: number, chatId: string, filename: string, contentType: string,
    messageThreadId?: number | null,
    options?: { sseKeyBase64?: string; sseS3KeyBase64?: string },
  ): Promise<ConsolidateJob> {
    return this.withRetry(async () => {
      const body: Record<string, unknown> = {
        job_key: jobKey, file_ids: fileIds, total_size: totalSize, chat_id: chatId, filename, content_type: contentType,
        message_thread_id: messageThreadId ?? undefined,
      };
      if (options?.sseKeyBase64) body.sse_key = options.sseKeyBase64;
      if (options?.sseS3KeyBase64) body.sse_s3_key = options.sseS3KeyBase64;
      const res = await fetch(`${this.baseUrl}/api/proxy/consolidate-job`, {
        method: 'POST', headers: this.headers(), body: JSON.stringify(body),
        signal: AbortSignal.timeout(VPS_PROXY_TIMEOUT),
      });
      if (!res.ok) throw await VpsError.from(res, 'consolidate-job start');
      return res.json() as Promise<ConsolidateJob>;
    });
  }

  /** Wait up to waitSec (max 55, under Cloudflare's ~100 s limit) for a consolidation. Null if the VPS lost the job. */
  async consolidateJobWait(jobKey: string, waitSec: number): Promise<ConsolidateJob | null> {
    return this.withRetry(async () => {
      const res = await fetch(`${this.baseUrl}/api/proxy/consolidate-job/${encodeURIComponent(jobKey)}?wait=${waitSec}`, {
        headers: this.headers(),
        signal: AbortSignal.timeout((waitSec + 20) * 1000),
      });
      if (res.status === 404) return null;
      if (!res.ok) throw await VpsError.from(res, 'consolidate-job wait');
      return res.json() as Promise<ConsolidateJob>;
    });
  }

  /** Download and decrypt an encrypted file on the VPS side (streaming, no Worker memory buffering). */
  async proxyGetDecrypt(fileId: string, keyBase64: string, rangeStart?: number, rangeEnd?: number): Promise<Response> {
    return this.withRetry(async () => {
      const body: Record<string, unknown> = { file_id: fileId, key_base64: keyBase64 };
      if (rangeStart !== undefined && rangeEnd !== undefined) {
        body.range_start = rangeStart;
        body.range_end = rangeEnd;
      }
      const res = await fetch(`${this.baseUrl}/api/proxy/get-decrypt`, {
        method: 'POST',
        headers: this.headers(),
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(VPS_LONG_TIMEOUT),
      });
      if (!res.ok) throw new Error(`VPS proxy get-decrypt failed: ${res.status}`);
      return res;
    });
  }

  async proxyPut(data: ArrayBuffer, chatId: string, filename: string, contentType: string, messageThreadId?: number | null): Promise<Response> {
    return this.withRetry(async () => {
      const headers: Record<string, string> = {
        'Authorization': `Bearer ${this.secret}`,
        'Content-Type': contentType,
        'X-Chat-Id': chatId,
        'X-Filename': filename,
        'X-Content-Type': contentType,
      };
      if (messageThreadId) headers['X-Message-Thread-Id'] = messageThreadId.toString();
      const res = await fetch(`${this.baseUrl}/api/proxy/put`, {
        method: 'POST',
        headers,
        body: data,
        signal: AbortSignal.timeout(VPS_LONG_TIMEOUT),
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`VPS proxy put failed (${res.status}): ${text}`);
      }
      return res;
    });
  }

  async consolidate(
    fileIds: string[], chatId: string, filename: string, contentType: string,
    messageThreadId?: number | null,
    options?: { sseKeyBase64?: string; sseS3KeyBase64?: string },
  ): Promise<Response> {
    return this.withRetry(async () => {
      const body: Record<string, unknown> = {
        file_ids: fileIds,
        chat_id: chatId,
        filename,
        content_type: contentType,
        message_thread_id: messageThreadId ?? undefined,
      };
      if (options?.sseKeyBase64) body.sse_key = options.sseKeyBase64;
      if (options?.sseS3KeyBase64) body.sse_s3_key = options.sseS3KeyBase64;

      const res = await fetch(`${this.baseUrl}/api/proxy/consolidate`, {
        method: 'POST',
        headers: this.headers(),
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(VPS_LONG_TIMEOUT),
      });
      if (!res.ok) throw new Error(`VPS consolidate failed: ${res.status}`);
      return res;
    });
  }
}
