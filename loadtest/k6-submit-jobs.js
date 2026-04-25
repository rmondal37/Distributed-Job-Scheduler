import http from 'k6/http';
import { check, sleep } from 'k6';
import exec from 'k6/execution';

const baseUrl = __ENV.BASE_URL || 'http://localhost:8080';
const jobType = __ENV.JOB_TYPE || 'REPORT';
const priority = Number(__ENV.PRIORITY || '1');
const maxRetries = Number(__ENV.MAX_RETRIES || '3');
const thinkTimeMs = Number(__ENV.THINK_TIME_MS || '0');

export const options = {
  scenarios: {
    submit_jobs: {
      executor: 'ramping-arrival-rate',
      startRate: Number(__ENV.START_RATE || '10'),
      timeUnit: '1s',
      preAllocatedVUs: Number(__ENV.PRE_ALLOCATED_VUS || '20'),
      maxVUs: Number(__ENV.MAX_VUS || '200'),
      stages: [
        { target: Number(__ENV.STAGE1_TARGET || '25'), duration: __ENV.STAGE1_DURATION || '1m' },
        { target: Number(__ENV.STAGE2_TARGET || '50'), duration: __ENV.STAGE2_DURATION || '2m' },
        { target: Number(__ENV.STAGE3_TARGET || '75'), duration: __ENV.STAGE3_DURATION || '2m' },
        { target: Number(__ENV.STAGE4_TARGET || '0'), duration: __ENV.STAGE4_DURATION || '30s' }
      ]
    }
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<1000'],
    checks: ['rate>0.99']
  }
};

export default function () {
  const sequence = exec.scenario.iterationInTest;
  const virtualUser = exec.vu.idInTest;
  const payload = JSON.stringify({
    idempotencyKey: `k6-${jobType.toLowerCase()}-${virtualUser}-${sequence}-${Date.now()}`,
    type: jobType,
    payload: {
      source: 'k6',
      requestedBy: 'loadtest',
      sequence,
      virtualUser
    },
    priority,
    maxRetries
  });

  const response = http.post(`${baseUrl}/jobs`, payload, {
    headers: {
      'Content-Type': 'application/json'
    }
  });

  check(response, {
    'job submission returned 201': (res) => res.status === 201,
    'job submission returned an id': (res) => {
      try {
        const body = JSON.parse(res.body);
        return Boolean(body.id);
      } catch (error) {
        return false;
      }
    }
  });

  if (thinkTimeMs > 0) {
    sleep(thinkTimeMs / 1000);
  }
}
