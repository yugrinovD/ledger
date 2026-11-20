import http from 'k6/http';
import { check } from 'k6';

const accounts = JSON.parse(open('./accounts.json')).accounts;
const baseURL = __ENV.BASE_URL || 'http://localhost:8080';
const apiKey = __ENV.API_KEY;

export const options = {
  scenarios: {
    reads: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 500),
      timeUnit: '1s',
      duration: __ENV.DURATION || '1m',
      preAllocatedVUs: 100,
      maxVUs: 500,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.0005'],
    'http_req_duration{name:read}': ['p(95)<50', 'p(99)<100'],
  },
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  if (!apiKey) throw new Error('API_KEY is required');
  return { count: accounts.length };
}

export default function () {
  const account = accounts[Math.floor(Math.random() * accounts.length)];
  const path = Math.random() < 0.5
    ? `/v1/accounts/${account.id}/balance`
    : `/v1/accounts/${account.id}/entries?limit=20`;
  const res = http.get(`${baseURL}${path}`, {
    headers: { Authorization: `Bearer ${apiKey}` },
    tags: { name: 'read' },
    timeout: '10s',
  });
  check(res, { 'read 200': (r) => r.status === 200 });
}
