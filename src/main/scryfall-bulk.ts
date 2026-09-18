import { Worker } from 'node:worker_threads';
import { join } from 'node:path';
import type { ScryfallBulkCard } from './scryfall-bootstrap.js';

export const SCRYFALL_USER_AGENT =
  'Mimir/0.1 (https://github.com/JovinJovinsson/mimir; local catalogue)';
const ACCEPT = 'application/json';

const BULK_DATA_BASE = 'https://api.scryfall.com/bulk-data';

/**
 * Internal, normalised manifest shape used by the rest of Mimir.
 *
 * Scryfall migrated bulk card exports in July 2026 from a JSON-array
 * `download_uri` to gzip-compressed JSONL exposed as
 * `jsonl_download_uri`. We normalise both API generations to the legacy
 * field names here so the bootstrap orchestration does not need to care which
 * Scryfall generation it is talking to.
 */
export interface BulkDataManifest {
  type: string;
  download_uri: string;
  updated_at: string;
  size: number;
}

interface RawBulkDataManifest {
  type?: string;
  updated_at?: string;
  download_uri?: string;
  jsonl_download_uri?: string;
  size?: number;
  compressed_size?: number;
}

export interface HttpError extends Error {
  status: number;
  retryAfterMs?: number;
}

function httpError(message: string, status: number, retryAfterMs?: number): HttpError {
  const err = new Error(message) as HttpError;
  err.status = status;
  if (retryAfterMs != null) err.retryAfterMs = retryAfterMs;
  return err;
}

function politeHeaders(): Record<string, string> {
  return { 'User-Agent': SCRYFALL_USER_AGENT, Accept: ACCEPT };
}

export async function fetchBulkDataManifest(
  bulkType: 'default_cards' | 'oracle_cards' | 'all_cards',
  fetchImpl: typeof fetch = fetch,
): Promise<BulkDataManifest> {
  const slug = bulkType.replace(/_/g, '-');
  const res = await fetchImpl(`${BULK_DATA_BASE}/${slug}`, { headers: politeHeaders() });
  if (!res.ok) {
    throw httpError(
      `Scryfall bulk-data manifest ${slug} returned ${res.status}`,
      res.status,
      parseRetryAfter(res),
    );
  }

  const data = (await res.json()) as RawBulkDataManifest;
  const downloadUri = data.jsonl_download_uri ?? data.download_uri;
  if (!downloadUri) {
    throw new Error(
      'Scryfall bulk-data manifest missing jsonl_download_uri/download_uri',
    );
  }

  return {
    type: data.type ?? bulkType,
    download_uri: downloadUri,
    updated_at: data.updated_at ?? '',
    size: data.compressed_size ?? data.size ?? 0,
  };
}

/**
 * Legacy helper retained for tests and callers that explicitly provide an old
 * JSON-array bulk URI. The application bootstrap uses downloadBulkJson(), whose
 * worker supports both the current .jsonl.gz format and the old JSON array.
 */
export async function fetchBulkData(
  downloadUri: string,
  fetchImpl: typeof fetch = fetch,
): Promise<ScryfallBulkCard[]> {
  const res = await fetchImpl(downloadUri, { headers: politeHeaders() });
  if (!res.ok) {
    throw httpError(
      `Scryfall bulk-data download returned ${res.status}`,
      res.status,
      parseRetryAfter(res),
    );
  }
  const data = (await res.json()) as ScryfallBulkCard[];
  if (!Array.isArray(data)) {
    throw new Error('Scryfall bulk-data payload was not an array');
  }
  return data;
}

export type DownloadProgressFn = (downloadedBytes: number, totalBytes: number | null) => void;

// Spawns a Worker thread so downloading, gzip decompression and bulk parsing
// stay off the Electron main thread. The worker accepts both:
//   - current Scryfall gzip-compressed JSONL (.jsonl.gz)
//   - legacy Scryfall JSON arrays (.json)
// Cards are posted back in batches so the renderer remains responsive.
export function downloadBulkJson(
  downloadUri: string,
  onProgress?: DownloadProgressFn,
): Promise<ScryfallBulkCard[]> {
  return new Promise((resolve, reject) => {
    const worker = new Worker(join(__dirname, 'bulk-parse-worker.js'), {
      workerData: { downloadUri },
    });

    const allCards: ScryfallBulkCard[] = [];
    let settled = false;

    const resolveOnce = (): void => {
      if (settled) return;
      settled = true;
      resolve(allCards);
    };

    const rejectOnce = (err: Error): void => {
      if (settled) return;
      settled = true;
      reject(err);
    };

    worker.on('message', (msg: WorkerMsg) => {
      switch (msg.type) {
        case 'progress':
          onProgress?.(msg.downloaded, msg.total);
          break;
        case 'cards':
          for (const card of msg.cards) allCards.push(card);
          break;
        case 'done':
          resolveOnce();
          void worker.terminate();
          break;
        case 'error':
          rejectOnce(httpError(msg.message, msg.status ?? 0));
          void worker.terminate();
          break;
      }
    });

    worker.on('error', rejectOnce);
    worker.on('exit', (code) => {
      if (!settled && code !== 0) {
        rejectOnce(new Error(`Bulk-parse worker exited with code ${code}`));
      }
    });
  });
}

type WorkerMsg =
  | { type: 'progress'; downloaded: number; total: number | null }
  | { type: 'cards'; cards: ScryfallBulkCard[] }
  | { type: 'done' }
  | { type: 'error'; message: string; status?: number };

function parseRetryAfter(res: Response): number | undefined {
  const header = res.headers?.get?.('Retry-After');
  if (!header) return undefined;
  const seconds = Number(header);
  if (Number.isFinite(seconds)) return seconds * 1000;
  const date = Date.parse(header);
  if (Number.isFinite(date)) return Math.max(0, date - Date.now());
  return undefined;
}

export class RateLimiter {
  private nextAvailableAt = 0;
  constructor(private readonly minIntervalMs: number) {}

  async acquire(): Promise<void> {
    const now = Date.now();
    const wait = Math.max(0, this.nextAvailableAt - now);
    this.nextAvailableAt = Math.max(now, this.nextAvailableAt) + this.minIntervalMs;
    if (wait > 0) await sleep(wait);
  }
}

export interface RetryOptions {
  maxRetries: number;
  baseDelayMs?: number;
  sleep?: (ms: number) => Promise<void>;
}

export async function retryOn429<T>(
  attempt: () => Promise<T>,
  opts: RetryOptions,
): Promise<T> {
  const sleepFn = opts.sleep ?? sleep;
  const base = opts.baseDelayMs ?? 1000;
  let lastErr: unknown;
  for (let i = 0; i <= opts.maxRetries; i++) {
    try {
      return await attempt();
    } catch (err) {
      lastErr = err;
      const status = (err as HttpError | undefined)?.status;
      if (status !== 429) throw err;
      if (i === opts.maxRetries) throw err;
      const retryAfter = (err as HttpError).retryAfterMs;
      const backoff = retryAfter ?? base * 2 ** i;
      await sleepFn(backoff);
    }
  }
  throw lastErr;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
