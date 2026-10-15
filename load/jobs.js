import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Trend } from "k6/metrics";

const baseUrl = (__ENV.BASE_URL || "http://app-node:3000").replace(/\/$/, "");
const configuredWorkflow = __ENV.WORKFLOW || "mixed";
const itemCount = Number(__ENV.ITEM_COUNT || 100);
const externalLatencyMs = Number(__ENV.EXTERNAL_LATENCY_MS || 0);
const failureEvery = Number(__ENV.FAILURE_EVERY || 0);
const pollIntervalSeconds = Number(__ENV.POLL_INTERVAL_MS || 500) / 1000;

const enqueueLatency = new Trend("benchmark_enqueue_ms", true);
const completionLatency = new Trend("benchmark_completion_ms", true);
const endToEndLatency = new Trend("benchmark_end_to_end_ms", true);
const itemsPerSecond = new Trend("benchmark_items_per_second");
const completedJobs = new Counter("benchmark_jobs_completed");
const functionalErrors = new Counter("benchmark_functional_errors");

const profiles = {
  smoke: { vus: 1, iterations: 3, thresholds: {} },
  quick: {
    vus: 20,
    duration: "60s",
    thresholds: {},
  },
  stress: {
    stages: [
      { duration: "10s", target: 10 },
      { duration: "20s", target: 30 },
      { duration: "20s", target: 60 },
      { duration: "45s", target: 100 },
      { duration: "10s", target: 0 },
    ],
    gracefulRampDown: "60s",
    gracefulStop: "180s",
    thresholds: {},
  },
  soak: {
    vus: 50,
    duration: "180s",
    thresholds: {},
  },
};

const profile = profiles[__ENV.PROFILE || "stress"] || profiles.stress;
const options = {
  thresholds: {
    http_req_failed: ["rate<0.01"],
    checks: ["rate>0.99"],
    benchmark_functional_errors: ["count==0"],
    ...profile.thresholds,
  },
};

if (profile.stages) {
  options.scenarios = {
    benchmark: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: profile.stages,
      gracefulRampDown: profile.gracefulRampDown,
      gracefulStop: profile.gracefulStop,
    },
  };
} else {
  options.vus = profile.vus;
  if (profile.iterations) options.iterations = profile.iterations;
  if (profile.duration) options.duration = profile.duration;
}

export { options };

function workflowForIteration() {
  if (configuredWorkflow !== "mixed") return configuredWorkflow;
  return ["approval", "enrichment", "synchronization"][(__VU + __ITER) % 3];
}

function expectedFailures(total) {
  if (failureEvery <= 0) return 0;
  return Math.floor(total / failureEvery);
}

export default function () {
  const workflow = workflowForIteration();
  const tags = { workflow };
  functionalErrors.add(0, tags);
  const payload = JSON.stringify({
    workflow,
    itemCount,
    externalLatencyMs,
    failureEvery,
  });
  const startedAt = Date.now();
  const enqueueResponse = http.post(`${baseUrl}/api/benchmark/jobs`, payload, {
    headers: { "Content-Type": "application/json" },
    tags: { ...tags, phase: "enqueue" },
    timeout: "30s",
  });

  enqueueLatency.add(enqueueResponse.timings.duration, tags);
  const accepted = check(
    enqueueResponse,
    { "job accepted": (response) => response.status === 202 },
    tags,
  );
  if (!accepted) {
    functionalErrors.add(1, { ...tags, phase: "enqueue" });
    return;
  }

  let jobId;
  try {
    jobId = enqueueResponse.json("jobId");
  } catch (_error) {
    functionalErrors.add(1, { ...tags, phase: "enqueue-body" });
    return;
  }
  if (!jobId) {
    functionalErrors.add(1, { ...tags, phase: "missing-job-id" });
    return;
  }

  const deadline = Date.now() + 300000;
  let finalJob = null;
  while (Date.now() < deadline) {
    sleep(pollIntervalSeconds);
    const statusResponse = http.get(`${baseUrl}/api/benchmark/jobs/${jobId}`, {
      tags: { ...tags, phase: "poll" },
      timeout: "30s",
    });
    if (statusResponse.status !== 200) {
      functionalErrors.add(1, { ...tags, phase: "poll-status" });
      continue;
    }

    let job;
    try {
      job = statusResponse.json();
    } catch (_error) {
      functionalErrors.add(1, { ...tags, phase: "poll-body" });
      continue;
    }
    if (job.status === "completed" || job.status === "completed_with_errors" || job.status === "failed") {
      finalJob = job;
      break;
    }
  }

  if (!finalJob) {
    functionalErrors.add(1, { ...tags, phase: "job-timeout" });
    check(null, { "job reached a terminal state": () => false }, tags);
    return;
  }

  completedJobs.add(1, tags);
  const accountedItems = Number(finalJob.completedItems) + Number(finalJob.failedItems);
  const expectedFailedItems = expectedFailures(itemCount);
  const resultsAreConsistent = accountedItems === itemCount
    && Number(finalJob.failedItems) === expectedFailedItems
    && Number(finalJob.completedItems) === itemCount - expectedFailedItems;
  const terminalStatusIsConsistent = failureEvery > 0
    ? finalJob.status === "completed_with_errors"
    : finalJob.status === "completed";

  const metricsAreValid = check(
    finalJob,
    {
      "job item counts match request": () => resultsAreConsistent,
      "job terminal status matches injected failures": () => terminalStatusIsConsistent,
    },
    tags,
  );
  if (!metricsAreValid) functionalErrors.add(1, { ...tags, phase: "job-result" });

  const durationMs = Number(finalJob.durationMs);
  completionLatency.add(durationMs, tags);
  endToEndLatency.add(Date.now() - startedAt, tags);
  if (durationMs > 0) itemsPerSecond.add((itemCount * 1000) / durationMs, tags);
}

export function handleSummary(data) {
  const summaryPath = __ENV.SUMMARY_PATH || "/results/summary.json";
  return { [summaryPath]: JSON.stringify(data, null, 2) };
}
