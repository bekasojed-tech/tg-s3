import type { Env } from '../types';

// R2's free tier includes 10 GB of storage. The cache stops growing at this budget, leaving headroom
// for objects written between recounts. Override with the R2_CACHE_BUDGET_GB variable.
const DEFAULT_BUDGET_GB = 8;
// A recount lists the bucket 1000 objects per page; stop after this many pages (100k objects).
const RECOUNT_MAX_PAGES = 100;

export function r2BudgetBytes(env: Env): number {
  const gb = Number(env.R2_CACHE_BUDGET_GB);
  return Math.floor((Number.isFinite(gb) && gb > 0 ? gb : DEFAULT_BUDGET_GB) * 1e9);
}

/**
 * Reserves space for an object about to be written to the R2 cache.
 * Returns false when the write would take the cache over budget, or when the usage table is missing:
 * the guard fails closed, so a missing migration means no caching rather than unlimited caching.
 */
export async function reserveR2Space(env: Env, size: number): Promise<boolean> {
  try {
    const row = await env.DB.prepare(
      'UPDATE r2_cache_usage SET total_bytes = total_bytes + ?1, object_count = object_count + 1 ' +
      'WHERE id = 1 AND total_bytes + ?1 <= ?2 RETURNING total_bytes',
    ).bind(size, r2BudgetBytes(env)).first();
    return row !== null;
  } catch (e) {
    console.error('R2 budget: reserve failed, skipping cache write:', e);
    return false;
  }
}

/** Gives back space reserved for a write that did not happen. */
export async function releaseR2Space(env: Env, size: number): Promise<void> {
  try {
    await env.DB.prepare(
      'UPDATE r2_cache_usage SET total_bytes = MAX(0, total_bytes - ?1), object_count = MAX(0, object_count - 1) WHERE id = 1',
    ).bind(size).run();
  } catch { /* the next recount corrects it */ }
}

/**
 * Replaces the running total with the real size of the bucket. Deletions (stale entries, lifecycle expiry,
 * deleted objects) never decrement the counter, so between recounts it can only be too high, never too low.
 * An incomplete listing leaves the counter unchanged for the same reason.
 */
export async function recountR2Usage(env: Env): Promise<{ totalBytes: number; objectCount: number; complete: boolean }> {
  if (!env.CACHE) return { totalBytes: 0, objectCount: 0, complete: true };
  let totalBytes = 0;
  let objectCount = 0;
  let cursor: string | undefined;
  let pages = 0;
  do {
    const listed = await env.CACHE.list({ cursor, limit: 1000 });
    for (const obj of listed.objects) {
      totalBytes += obj.size;
      objectCount++;
    }
    cursor = listed.truncated ? listed.cursor : undefined;
    pages++;
  } while (cursor && pages < RECOUNT_MAX_PAGES);

  const complete = cursor === undefined;
  if (complete) {
    await env.DB.prepare(
      "UPDATE r2_cache_usage SET total_bytes = ?1, object_count = ?2, recounted_at = datetime('now') WHERE id = 1",
    ).bind(totalBytes, objectCount).run();
  }
  return { totalBytes, objectCount, complete };
}
