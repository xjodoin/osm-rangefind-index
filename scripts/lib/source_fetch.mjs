export const SOURCE_USER_AGENT = "osm-rangefind-index/0.1 (+https://github.com/xjodoin/osm-rangefind-index)";
export const DEFAULT_SOURCE_TIMEOUT_MS = 30_000;

export async function fetchSource(
  url,
  init = {},
  {
    fetchImpl = globalThis.fetch,
    timeoutMs = DEFAULT_SOURCE_TIMEOUT_MS
  } = {}
) {
  const timeout = Math.max(1, Number(timeoutMs) || DEFAULT_SOURCE_TIMEOUT_MS);
  const controller = new AbortController();
  const timer = setTimeout(() => {
    controller.abort(new Error(`Source request timed out after ${timeout} ms: ${url}`));
  }, timeout);

  const headers = new Headers(init.headers);
  if (!headers.has("user-agent")) headers.set("user-agent", SOURCE_USER_AGENT);
  const signal = init.signal
    ? AbortSignal.any([init.signal, controller.signal])
    : controller.signal;

  try {
    return await fetchImpl(url, { ...init, headers, signal });
  } catch (error) {
    if (controller.signal.aborted && !init.signal?.aborted) {
      throw new Error(`Source request timed out after ${timeout} ms: ${url}`, { cause: error });
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}
