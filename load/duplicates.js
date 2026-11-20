import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const accounts = JSON.parse(open('./accounts.json')).accounts;
const rate = Number(__ENV.RATE || 30);
const duration = __ENV.DURATION || '1m';
const baseURL = __ENV.BASE_URL || 'http://localhost:8080';
const apiKey = __ENV.API_KEY;

const replayObserved = new Rate('idempotent_replay_observed');

export const options = {
  scenarios: {
    duplicates: {
      executor: 'constant-arrival-rate',
      rate: rate,
      timeUnit: '1s',
      duration: duration,
      preAllocatedVUs: Math.max(10, Math.ceil(rate / 2)),
      maxVUs: Math.max(50, rate * 3),
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.001'],
    'http_req_duration{name:duplicate}': ['p(95)<300', 'p(99)<600'],
    idempotent_replay_observed: ['rate>0.95'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  if (!apiKey) throw new Error('API_KEY is required');
  return { count: accounts.length };
}

export default function () {
  const index = (__VU - 1) % accounts.length;
  const source = accounts[index];
  const destination = accounts[(index + 1) % accounts.length];
  const key = `dup-${__VU}-${__ITER}-${Date.now()}`;
  const payload = JSON.stringify({
    source_account_id: source.id,
    destination_account_id: destination.id,
    amount: '0.01',
    currency: source.currency,
    description: 'k6 duplicate load',
  });

  for (let attempt = 0; attempt < 3; attempt++) {
    const res = http.post(`${baseURL}/v1/transfers`, payload, {
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
        'Idempotency-Key': key,
      },
      tags: { name: 'duplicate' },
      timeout: '10s',
    });
    const ok = check(res, { 'duplicate answered 201': (r) => r.status === 201 });
    if (attempt > 0) {
      replayObserved.add(res.headers['Idempotent-Replayed'] === 'true');
    }
    if (!ok) {
      console.error(`duplicate request failed: status=${res.status} body=${res.body}`);
    }
  }
  sleep(0.01);
}
