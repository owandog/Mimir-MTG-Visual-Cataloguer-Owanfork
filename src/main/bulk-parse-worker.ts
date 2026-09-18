import { parentPort, workerData } from 'node:worker_threads';
import { get as httpsGet } from 'node:https';
import type { IncomingMessage } from 'node:http';
import { createGunzip } from 'node:zlib';
import { createInterface } from 'node:readline';
import type { ScryfallBulkCard } from './scryfall-bootstrap.js';

const SCRYFALL_USER_AGENT =
  'Mimir/0.1 (https://github.com/JovinJovinsson/mimir; local catalogue)';
const BATCH_SIZE = 10_000;
const PROGRESS_INTERVAL_BYTES = 1024 * 1024; // 1 MB

function politeHeaders(): Record<string, string> {
  return {
    'User-Agent': SCRYFALL_USER_AGENT,
    Accept: 'application/json;q=0.9,*/*;q=0.8',
  };
}

const { downloadUri } = workerData as { downloadUri: string };

let lastProgressAt = 0;
let finished = false;

function postError(err: unknown, status?: number): void {
  if (finished) return;
  finished = true;
  parentPort!.postMessage({
    type: 'error',
    message: err instanceof Error ? err.message : String(err),
    ...(status != null ? { status } : {}),
  });
}

function postDone(): void {
  if (finished) return;
  finished = true;
  parentPort!.postMessage({ type: 'done' });
}

function postCards(batch: ScryfallBulkCard[]): void {
  if (batch.length > 0) {
    parentPort!.postMessage({ type: 'cards', cards: batch });
  }
}

function reportProgress(downloaded: number, total: number | null, force = false): void {
  if (force || downloaded - lastProgressAt >= PROGRESS_INTERVAL_BYTES) {
    lastProgressAt = downloaded;
    parentPort!.postMessage({ type: 'progress', downloaded, total });
  }
}

function attachCompressedProgress(res: IncomingMessage): {
  getDownloaded: () => number;
  total: number | null;
} {
  const raw = res.headers['content-length'];
  const total = raw ? Number(raw) : null;
  let downloaded = 0;

  res.on('data', (chunk: Buffer) => {
    downloaded += chunk.length;
    reportProgress(downloaded, total);
  });

  return { getDownloaded: () => downloaded, total };
}

function parseJsonlGzip(res: IncomingMessage): void {
  const progress = attachCompressedProgress(res);
  const gunzip = createGunzip();
  const lines = createInterface({ input: gunzip, crlfDelay: Infinity });

  let batch: ScryfallBulkCard[] = [];
  let lineNumber = 0;

  const flush = (): void => {
    postCards(batch);
    batch = [];
  };

  lines.on('line', (line) => {
    if (finished) return;
    lineNumber++;
    const trimmed = line.trim();
    if (!trimmed) return;

    try {
      batch.push(JSON.parse(trimmed) as ScryfallBulkCard);
      if (batch.length >= BATCH_SIZE) flush();
    } catch (err) {
      postError(
        new Error(
          `Invalid Scryfall JSONL at line ${lineNumber}: ${err instanceof Error ? err.message : String(err)}`,
        ),
      );
      lines.close();
      res.destroy();
    }
  });

  lines.on('close', () => {
    if (finished) return;
    flush();
    reportProgress(progress.getDownloaded(), progress.total, true);
    postDone();
  });

  gunzip.on('error', postError);
  res.on('error', postError);
  res.pipe(gunzip);
}

// Legacy parser for pre-July-2026 Scryfall JSON-array downloads.
// Kept as a compatibility fallback for mirrors/cached manifests.
function parseLegacyJsonArray(res: IncomingMessage): void {
  const rawTotal = res.headers['content-length'];
  const total = rawTotal ? Number(rawTotal) : null;
  let downloaded = 0;
  const chunks: Buffer[] = [];

  res.on('data', (chunk: Buffer) => {
    chunks.push(chunk);
    downloaded += chunk.length;
    reportProgress(downloaded, total);
  });

  res.on('error', postError);

  res.on('end', () => {
    if (finished) return;
    reportProgress(downloaded, total, true);

    try {
      const parsed = JSON.parse(Buffer.concat(chunks).toString('utf8')) as unknown;
      if (!Array.isArray(parsed)) {
        throw new Error('Scryfall bulk-data payload was not a JSON array');
      }

      let batch: ScryfallBulkCard[] = [];
      for (const card of parsed as ScryfallBulkCard[]) {
        batch.push(card);
        if (batch.length >= BATCH_SIZE) {
          postCards(batch);
          batch = [];
        }
      }
      postCards(batch);
      postDone();
    } catch (err) {
      postError(err);
    }
  });
}

const req = httpsGet(downloadUri, { headers: politeHeaders() }, (res) => {
  if (res.statusCode !== 200) {
    const status = res.statusCode ?? 0;
    res.resume();
    postError(new Error(`Scryfall bulk-data download returned ${status}`), status);
    return;
  }

  const contentType = String(res.headers['content-type'] ?? '').toLowerCase();
  const contentEncoding = String(res.headers['content-encoding'] ?? '').toLowerCase();
  const looksJsonlGzip =
    downloadUri.toLowerCase().endsWith('.jsonl.gz') ||
    contentType.includes('jsonl') ||
    contentType.includes('gzip') ||
    contentEncoding.includes('gzip');

  if (looksJsonlGzip) {
    parseJsonlGzip(res);
  } else {
    parseLegacyJsonArray(res);
  }
});

req.on('error', postError);
