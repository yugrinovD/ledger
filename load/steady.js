import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const accounts = JSON.parse(open('./accounts.json')).accounts;
const rate = Number(__ENV.RATE || 100);
const duration = __ENV.DURATION || '1m';
const baseURL = __ENV.BASE_URL || 'http://localhost:8080';
const apiKey = __ENV.API_KEY;

const transferOK = new Rate('transfer_ok');

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: rate,
      timeUnit: '1s',
      duration: duration,
      preAllocatedVUs: Math.max(20, Math.ceil(rate / 4)),
      maxVUs: Math.max(100, rate * 2),
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.001'],
    'http_req_duration{name:transfer}': ['p(95)<250', 'p(99)<500'],
    transfer_ok: ['rate>0.999'],
  },
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  if (!apiKey) throw new Error('API_KEY is required');
  if (!accounts || accounts.length === 0) throw new Error('accounts.json is empty; run make seed');
  return { count: accounts.length };
}

export default function () {
  const index = (__VU - 1) % accounts.length;
  const source = accounts[index];
  let destination = accounts[Math.floor(Math.random() * accounts.length)];
  if (destination.id === source.id) {
    destination = accounts[(index + 1) % accounts.length];
  }
  const key = `load-${__VU}-${__ITER}-${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
  const payload = JSON.stringify({
    source_account_id: source.id,
    destination_account_id: destination.id,
    amount: '0.01',
    currency: source.currency,
    description: 'k6 steady load',
  });
  const res = http.post(`${baseURL}/v1/transfers`, payload, {
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${apiKey}`,
      'Idempotency-Key': key,
    },
    tags: { name: 'transfer' },
    timeout: '10s',
  });
  const ok = check(res, { 'transfer 201': (r) => r.status === 201 });
  transferOK.add(ok);
  if (!ok) {
    console.error(`transfer failed: status=${res.status} body=${res.body}`);
  }
  sleep(0.01);
}
