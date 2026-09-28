import http from 'node:http';

/** Returned by a handler that has written the response itself (SSE, static files, hangs). */
export const HANDLED = Symbol('handled');

export class HttpError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

export function sendJson(res, status, data, headers = {}) {
  res.writeHead(status, { 'content-type': 'application/json', ...headers });
  res.end(JSON.stringify(data));
}

async function readJson(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  if (!chunks.length) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    throw new HttpError(400, 'Invalid JSON body');
  }
}

function compile(path) {
  const pattern = path.replace(/\//g, '\\/').replace(/:(\w+)/g, '(?<$1>[^/]+)');
  return new RegExp(`^${pattern}$`);
}

/**
 * Minimal JSON router on top of node:http. A handler receives
 * { req, res, params, query, body } and returns a JSON-serialisable value,
 * or HANDLED when it wrote the response itself.
 */
export function createHttpApp({ fallback } = {}) {
  const routes = [];
  const add = (method) => (path, handler) => routes.push({ method, regex: compile(path), handler });

  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    const route = routes.find((r) => r.method === req.method && r.regex.test(url.pathname));
    try {
      if (!route) {
        if (fallback && (await fallback(req, res, url)) === HANDLED) return;
        throw new HttpError(404, `No route for ${req.method} ${url.pathname}`);
      }
      const params = url.pathname.match(route.regex).groups ?? {};
      const body = ['POST', 'PUT', 'PATCH'].includes(req.method) ? await readJson(req) : undefined;
      const query = Object.fromEntries(url.searchParams);
      const result = await route.handler({ req, res, params, query, body });
      if (result === HANDLED || res.headersSent) return;
      sendJson(res, 200, result ?? { ok: true });
    } catch (err) {
      if (res.headersSent) return res.destroy();
      sendJson(res, err.status ?? 500, { error: err.message });
    }
  });

  return {
    server,
    get: add('GET'),
    post: add('POST'),
    put: add('PUT'),
    delete: add('DELETE'),
    listen: (port) => new Promise((resolve) => server.listen(port, '0.0.0.0', resolve)),
  };
}

const ERROR_LABELS = {
  UND_ERR_SOCKET: 'CONNECTION_RESET',
  ECONNRESET: 'CONNECTION_RESET',
  ECONNREFUSED: 'CONNECTION_REFUSED',
  UND_ERR_CONNECT_TIMEOUT: 'TIMEOUT',
  UND_ERR_HEADERS_TIMEOUT: 'TIMEOUT',
};

/** Normalises a fetch() failure into a short label such as TIMEOUT or CONNECTION_RESET. */
export function describeFetchError(err) {
  if (err?.name === 'TimeoutError' || err?.name === 'AbortError') return 'TIMEOUT';
  const code = err?.cause?.code ?? err?.code;
  if (code) return ERROR_LABELS[code] ?? code;
  return err?.message ?? 'UNKNOWN_ERROR';
}

/** fetch() wrapper for service-to-service JSON calls. */
export async function requestJson(url, { method = 'GET', body, timeoutMs = 5000 } = {}) {
  const res = await fetch(url, {
    method,
    headers: body === undefined ? undefined : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = { error: text };
  }
  if (!res.ok) throw new HttpError(res.status, data?.error ?? `HTTP ${res.status} from ${url}`);
  return data;
}
