import { describe, it, expect, vi } from 'vitest';
import {
  fetchBulkDataManifest,
  fetchBulkData,
  RateLimiter,
  retryOn429,
  SCRYFALL_USER_AGENT,
} from '../src/main/scryfall-bulk.js';

function legacyManifestResponse(): typeof fetch {
  return vi.fn(async (url: string) => {
    if (String(url).endsWith('/bulk-data/default-cards')) {
      return {
        ok: true,
        status: 200,
        json: async () => ({
          object: 'bulk_data',
          type: 'default_cards',
          download_uri: 'https://data.scryfall.io/default-cards/default-cards.json',
          updated_at: '2026-06-13T00:00:00.000Z',
          size: 1234,
        }),
      } as Response;
    }
    throw new Error(`unexpected url: ${url}`);
  }) as unknown as typeof fetch;
}

function jsonlManifestResponse(): typeof fetch {
  return vi.fn(async (url: string) => {
    if (String(url).endsWith('/bulk-data/default-cards')) {
      return {
        ok: true,
        status: 200,
        json: async () => ({
          object: 'bulk_data',
          type: 'default_cards',
          jsonl_download_uri:
            'https://data.scryfall.io/default-cards/default-cards-20260918.jsonl.gz',
          updated_at: '2026-09-18T09:00:00.000Z',
          compressed_size: 987654,
        }),
      } as Response;
    }
    throw new Error(`unexpected url: ${url}`);
  }) as unknown as typeof fetch;
}

describe('fetchBulkDataManifest', () => {
  it('normalises the current Scryfall JSONL manifest', async () => {
    const fetchImpl = jsonlManifestResponse();
    const manifest = await fetchBulkDataManifest('default_cards', fetchImpl);
    expect(manifest.type).toBe('default_cards');
    expect(manifest.download_uri).toMatch(/default-cards-20260918\.jsonl\.gz/);
    expect(manifest.size).toBe(987654);
  });

  it('keeps compatibility with the legacy download_uri manifest', async () => {
    const fetchImpl = legacyManifestResponse();
    const manifest = await fetchBulkDataManifest('default_cards', fetchImpl);
    expect(manifest.download_uri).toMatch(/default-cards\.json/);
    expect(manifest.size).toBe(1234);
  });

  it('prefers jsonl_download_uri if both generations are present', async () => {
    const fetchImpl = vi.fn(async () =>
      ({
        ok: true,
        status: 200,
        json: async () => ({
          type: 'default_cards',
          download_uri: 'https://example.invalid/old.json',
          jsonl_download_uri: 'https://example.invalid/current.jsonl.gz',
          updated_at: '2026-09-18T09:00:00.000Z',
          size: 100,
          compressed_size: 50,
        }),
      }) as Response,
    ) as unknown as typeof fetch;

    const manifest = await fetchBulkDataManifest('default_cards', fetchImpl);
    expect(manifest.download_uri).toBe('https://example.invalid/current.jsonl.gz');
    expect(manifest.size).toBe(50);
  });

  it('fails clearly when neither download URI is provided', async () => {
    const fetchImpl = vi.fn(async () =>
      ({
        ok: true,
        status: 200,
        json: async () => ({
          type: 'default_cards',
          updated_at: '2026-09-18T09:00:00.000Z',
        }),
      }) as Response,
    ) as unknown as typeof fetch;

    await expect(
      fetchBulkDataManifest('default_cards', fetchImpl),
    ).rejects.toThrow(/jsonl_download_uri\/download_uri/);
  });

  it('sends the descriptive User-Agent', async () => {
    let seenUA = '';
    const fetchImpl = vi.fn(async (_url: string, init?: RequestInit) => {
      const headers = init?.headers as Record<string, string> | undefined;
      if (headers) seenUA = headers['User-Agent'] ?? '';
      return {
        ok: true,
        status: 200,
        json: async () => ({
          object: 'bulk_data',
          type: 'default_cards',
          jsonl_download_uri: 'https://data.scryfall.io/x.jsonl.gz',
          updated_at: '',
          compressed_size: 0,
        }),
      } as Response;
    }) as unknown as typeof fetch;
    await fetchBulkDataManifest('default_cards', fetchImpl);
    expect(seenUA).toBe(SCRYFALL_USER_AGENT);
    expect(seenUA).toMatch(/Mimir/);
  });
});

describe('fetchBulkData', () => {
  it('downloads and parses a legacy JSON array of cards', async () => {
    const fetchImpl = vi.fn(async () =>
      ({
        ok: true,
        status: 200,
        json: async () => [
          { id: 'a', name: 'A', set: 's', set_name: 'S', collector_number: '1' },
          { id: 'b', name: 'B', set: 's', set_name: 'S', collector_number: '2' },
        ],
      }) as Response,
    ) as unknown as typeof fetch;

    const cards = await fetchBulkData(
      'https://data.scryfall.io/default-cards/x.json',
      fetchImpl,
    );
    expect(cards).toHaveLength(2);
    expect(cards[0]?.id).toBe('a');
  });
});

describe('RateLimiter', () => {
  it('spaces calls at least minIntervalMs apart', async () => {
    const limiter = new RateLimiter(50);
    const now = () => Date.now();

    const t0 = now();
    await limiter.acquire();
    await limiter.acquire();
    await limiter.acquire();
    const elapsed = now() - t0;
    expect(elapsed).toBeGreaterThanOrEqual(100);
  });
});

describe('retryOn429', () => {
  it('retries once on a 429 response and succeeds the second time', async () => {
    let attempt = 0;
    const sleeps: number[] = [];
    const result = await retryOn429(
      async () => {
        attempt++;
        if (attempt === 1) {
          const err = new Error('429') as Error & { status: number; retryAfterMs?: number };
          err.status = 429;
          err.retryAfterMs = 10;
          throw err;
        }
        return 'ok';
      },
      { maxRetries: 3, sleep: async (ms) => { sleeps.push(ms); } },
    );
    expect(result).toBe('ok');
    expect(attempt).toBe(2);
    expect(sleeps[0]).toBeGreaterThanOrEqual(10);
  });

  it('gives up after maxRetries and surfaces the last error', async () => {
    const err = new Error('429') as Error & { status: number };
    err.status = 429;
    await expect(
      retryOn429(
        async () => {
          throw err;
        },
        { maxRetries: 2, sleep: async () => {} },
      ),
    ).rejects.toBe(err);
  });

  it('does not retry non-429 errors', async () => {
    let attempt = 0;
    const err = new Error('500') as Error & { status: number };
    err.status = 500;
    await expect(
      retryOn429(
        async () => {
          attempt++;
          throw err;
        },
        { maxRetries: 5, sleep: async () => {} },
      ),
    ).rejects.toBe(err);
    expect(attempt).toBe(1);
  });
});
