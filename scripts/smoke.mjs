// End-to-end smoke test against a running stack (docker compose up).
// Runs every scenario through the dashboard API and prints its result.
//   node scripts/smoke.mjs [baseUrl]
const BASE = process.argv[2] ?? `http://127.0.0.1:${process.env.DASHBOARD_HOST_PORT ?? 8090}`;

const RUNS = [
  ['happy-path', { clientId: 'amazon', count: 3 }],
  ['client-down', { clientId: 'flipkart', count: 3, recoverAfterSec: 6 }],
  ['backoff-then-down', { clientId: 'flipkart', count: 2, failureStyle: 'HTTP_503', recoverAfterSec: 3 }],
  ['rate-limited', { clientId: 'blinkit', count: 10, limitPerSec: 2, strategy: 'reactive' }],
  ['rate-limited', { clientId: 'blinkit', count: 10, limitPerSec: 2, strategy: 'token-bucket' }],
  ['rate-limited', { clientId: 'blinkit', count: 10, limitPerSec: 2, strategy: 'adaptive' }],
  ['transient', { clientId: 'flipkart', failures: 2, failureStyle: 'HTTP_503' }],
  ['isolation', { count: 3 }],
];

async function call(path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`${path}: ${data.error ?? res.status}`);
  return data;
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

let failures = 0;
await call('/api/settings/reset', {});
for (const [id, params] of RUNS) {
  await call('/api/reset', {});
  await sleep(1500);
  const started = Date.now();
  await call(`/api/scenarios/${id}`, params);
  let run;
  do {
    await sleep(1000);
    run = (await call('/api/state')).scenario;
  } while (run?.status === 'running' && Date.now() - started < 300_000);

  const label = `${id}${params.strategy ? ` (${params.strategy})` : ''}`.padEnd(32);
  if (run?.status === 'finished' && run.summary.delivered === run.summary.total) {
    const s = run.summary;
    const extra = s.http429 !== undefined ? ` · ${s.http429} × 429` : '';
    console.log(`PASS ${label} ${s.delivered}/${s.total} delivered in ${(s.durationMs / 1000).toFixed(1)} s · ${s.attempts} calls${extra}`);
  } else {
    failures += 1;
    console.log(`FAIL ${label} status=${run?.status} ${run?.error ?? JSON.stringify(run?.summary)}`);
  }
}
await call('/api/reset', {});
await call('/api/settings/reset', {});
process.exit(failures ? 1 : 0);
