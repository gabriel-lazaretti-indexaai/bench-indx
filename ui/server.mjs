import { createServer } from "node:http";
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { readdir, readFile, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const WEB_ROOT = path.join(ROOT, "ui", "public");
const RESULTS_DIR = path.join(ROOT, "results");
const HOST = "127.0.0.1";
const PORT = Number(process.env.BENCHMARK_UI_PORT || 4179);
const RUNTIMES = ["node", "dotnet", "go"];
const FLOWS = ["approval", "enrichment", "synchronization"];
const PROFILES = ["quick", "stress"];
const clients = new Set();
let imagesReady = false;
let runReserved = false;

const state = {
  status: "idle",
  runId: null,
  workflow: null,
  profile: null,
  total: 0,
  completed: 0,
  currentRuntime: null,
  currentFlow: null,
  stage: "Aguardando execução",
  progressPercent: 0,
  startedAt: null,
  finishedAt: null,
  error: null,
  results: [],
  logs: [],
};

function snapshot() {
  return { ...state, results: [...state.results], logs: [...state.logs] };
}

function publish(event, payload) {
  const message = `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;
  for (const client of clients) client.write(message);
}

function update(patch) {
  Object.assign(state, patch);
  publish("state", snapshot());
}

function addLog(message, level = "info") {
  const entry = { time: new Date().toISOString(), message, level };
  state.logs.push(entry);
  state.logs = state.logs.slice(-80);
  publish("log", entry);
}

function sendJson(response, status, payload) {
  response.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
  });
  response.end(JSON.stringify(payload));
}

function readRequestBody(request) {
  return new Promise((resolve, reject) => {
    let body = "";
    request.on("data", (chunk) => {
      body += chunk;
      if (body.length > 4096) {
        reject(new Error("A solicitação excede o limite permitido."));
        request.destroy();
      }
    });
    request.on("end", () => {
      try {
        resolve(JSON.parse(body || "{}"));
      } catch {
        reject(new Error("Envie um corpo JSON válido."));
      }
    });
    request.on("error", reject);
  });
}

function captureCommand(command, args) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { cwd: ROOT, env: process.env });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => { stdout += chunk; });
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.on("error", reject);
    child.on("close", (code) => resolve({ code: code ?? 1, stdout, stderr }));
  });
}

function runCommand(command, args, { quiet = false } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { cwd: ROOT, env: process.env });
    let lastLines = [];
    const consume = (stream) => {
      const lines = createInterface({ input: stream });
      lines.on("line", (rawLine) => {
        const line = rawLine.replace(/\u001b\[[0-9;]*m/g, "").trim();
        if (!line) return;
        lastLines.push(line);
        lastLines = lastLines.slice(-30);
        if (!quiet) addLog(line, stream === child.stderr ? "error" : "info");
        const progress = line.match(/\[\s*(\d+)%\s*\]/);
        if (progress) {
          state.progressPercent = Number(progress[1]);
          publish("progress", {
            progressPercent: state.progressPercent,
            currentRuntime: state.currentRuntime,
            currentFlow: state.currentFlow,
          });
        }
      });
    };
    consume(child.stdout);
    consume(child.stderr);
    child.on("error", reject);
    child.on("close", (code, signal) => resolve({
      code: code ?? 1,
      signal,
      tail: lastLines.join("\n"),
    }));
  });
}

function metric(metrics, name, key) {
  const values = metrics?.[name]?.values ?? metrics?.[name] ?? {};
  const value = values[key];
  return typeof value === "number" ? value : null;
}

function thresholdStatus(metrics) {
  const statuses = [];
  for (const item of Object.values(metrics || {})) {
    for (const result of Object.values(item?.thresholds || {})) {
      if (typeof result?.ok === "boolean") statuses.push(result.ok);
      else if (typeof result === "boolean") statuses.push(!result);
    }
  }
  return statuses.length === 0 || statuses.every(Boolean) ? "PASS" : "FAIL";
}

function memoryMiB(value) {
  const match = value.match(/^\s*([\d.]+)\s*([KMGT]?i?B)/i);
  if (!match) return null;
  const amount = Number(match[1]);
  const unit = match[2].toLowerCase();
  const factors = { b: 1 / 1048576, kb: 1 / 1024, kib: 1 / 1024, mb: 1, mib: 1,
    gb: 1024, gib: 1024, tb: 1048576, tib: 1048576 };
  return amount * (factors[unit] ?? 1);
}

async function resourcePeaks(summaryPath) {
  const csvPath = summaryPath.replace(".summary.json", ".resources.csv");
  let text;
  try {
    text = await readFile(csvPath, "utf8");
  } catch {
    return { cpuPeak: null, memoryPeakMiB: null };
  }
  const cpus = [];
  const memories = [];
  for (const line of text.split(/\r?\n/)) {
    const cleanLine = line.replace(/\u001b\[[0-?]*[ -/]*[@-~]/g, "").trim();
    const sample = cleanLine.match(/^([\d.]+)%\s*,\s*([^,]+?)\s*\/\s*[^,]+$/);
    if (!sample) continue;
    const cpuValue = Number(sample[1]);
    const mem = memoryMiB(sample[2]);
    if (Number.isFinite(cpuValue)) cpus.push(cpuValue);
    if (mem !== null) memories.push(mem);
  }
  return {
    cpuPeak: cpus.length ? Math.max(...cpus) : null,
    memoryPeakMiB: memories.length ? Math.max(...memories) : null,
  };
}

async function parseSummaryFile(fileName) {
  const fullPath = path.join(RESULTS_DIR, fileName);
  const data = JSON.parse(await readFile(fullPath, "utf8"));
  const match = fileName.match(/^(\d{8}T\d{6}Z)-(node|dotnet|go)-(approval|enrichment|synchronization|mixed)-(smoke|quick|stress)-lat(\d+)-fail(\d+)\.summary\.json$/);
  if (!match) return null;
  const [, timestamp, runtime, workflow, profile, latency, failureEvery] = match;
  const metrics = data.metrics || {};
  const resources = await resourcePeaks(fullPath);
  return {
    timestamp,
    runtime,
    workflow,
    profile,
    latencyMs: Number(latency),
    failureEvery: Number(failureEvery),
    jobsCompleted: metric(metrics, "benchmark_jobs_completed", "count"),
    jobsPerSecond: metric(metrics, "benchmark_jobs_completed", "rate"),
    enqueueP95Ms: metric(metrics, "benchmark_enqueue_ms", "p(95)"),
    completionP95Ms: metric(metrics, "benchmark_completion_ms", "p(95)"),
    endToEndP95Ms: metric(metrics, "benchmark_end_to_end_ms", "p(95)"),
    itemsPerSecond: metric(metrics, "benchmark_items_per_second", "avg"),
    httpRequestsPerSecond: metric(metrics, "http_reqs", "rate"),
    functionalErrors: metric(metrics, "benchmark_functional_errors", "count"),
    checksRate: metric(metrics, "checks", "rate"),
    httpFailureRate: metric(metrics, "http_req_failed", "rate"),
    thresholdStatus: thresholdStatus(metrics),
    ...resources,
    source: fileName,
  };
}

async function readResults() {
  let names;
  try {
    names = await readdir(RESULTS_DIR);
  } catch {
    return [];
  }
  const files = names.filter((name) => name.endsWith(".summary.json"));
  const rows = await Promise.all(files.map((file) => parseSummaryFile(file)));
  return rows.filter(Boolean).sort((a, b) => b.timestamp.localeCompare(a.timestamp));
}

async function assertNoRunningApps() {
  const result = await captureCommand("docker", ["compose", "ps", "--services", "--filter", "status=running"]);
  if (result.code !== 0) {
    throw new Error(result.stderr.trim() || "Docker Compose não respondeu. Confira se o Docker está ativo.");
  }
  const active = result.stdout.split(/\r?\n/).filter((service) => /^app-(node|dotnet|go)$/.test(service));
  if (active.length) {
    throw new Error(`Encerre os serviços ativos antes de iniciar uma comparação: ${active.join(", ")}.`);
  }
}

async function requireSuccess(command, args, label) {
  update({ stage: label, progressPercent: 0 });
  const result = await runCommand(command, args);
  if (result.code !== 0) throw new Error(`${label} falhou. ${result.tail}`);
  return result;
}

async function compressRawResult(fileName) {
  const rawPath = path.join(RESULTS_DIR, fileName.replace(".summary.json", ".raw.json"));
  try {
    await stat(rawPath);
  } catch {
    return;
  }
  const result = await captureCommand("gzip", ["-1", rawPath]);
  if (result.code !== 0) addLog("Não foi possível compactar o arquivo bruto; ele continua disponível sem compressão.", "warning");
}

async function runScenario(runtime, workflow, profile) {
  update({ currentRuntime: runtime, currentFlow: workflow, progressPercent: 0, stage: `Preparando ${runtime} / ${workflow}` });
  let appStarted = false;
  try {
    const up = await runCommand("docker", ["compose", "up", "-d", "--wait", `app-${runtime}`]);
    if (up.code !== 0) throw new Error(up.tail || `Não foi possível iniciar ${runtime}.`);
    appStarted = true;
    const reset = await runCommand("bash", ["scripts/reset-state.sh"]);
    if (reset.code !== 0) throw new Error(reset.tail || "Não foi possível limpar o estado do benchmark.");
    const before = new Set((await readdir(RESULTS_DIR)).filter((name) => name.endsWith(".summary.json")));
    update({ stage: `Executando ${FLOW_LABELS[workflow]} em ${RUNTIME_LABELS[runtime]}`, progressPercent: 0 });
    const run = await runCommand("bash", ["scripts/run-profile.sh", runtime, workflow, profile, "0", "0", "100"]);
    const after = (await readdir(RESULTS_DIR)).filter((name) => name.endsWith(".summary.json") && !before.has(name));
    const expected = `-${runtime}-${workflow}-${profile}-lat0-fail0.summary.json`;
    const fileName = after.filter((name) => name.includes(expected)).sort().at(-1);
    if (!fileName) throw new Error(run.tail || "A execução terminou sem gerar um resumo k6.");
    const result = await parseSummaryFile(fileName);
    result.exitCode = run.code;
    result.runId = state.runId;
    state.results.push(result);
    publish("result", result);
    update({ progressPercent: 100, stage: `${RUNTIME_LABELS[runtime]} concluído` });
    await compressRawResult(fileName);
    const summary = await runCommand("python3", ["scripts/summarize.py"], { quiet: true });
    if (summary.code !== 0) addLog("O resumo Markdown não foi atualizado; os arquivos JSON do k6 continuam disponíveis.", "warning");
    if (run.code !== 0 || result.thresholdStatus !== "PASS") {
      addLog(`${RUNTIME_LABELS[runtime]} terminou com threshold ou exit code de falha.`, "warning");
      return false;
    }
    addLog(`${RUNTIME_LABELS[runtime]} concluiu ${result.jobsCompleted ?? 0} jobs; ${result.jobsPerSecond?.toFixed(2) ?? "n/d"} jobs/s.`, "success");
    return true;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    addLog(message, "error");
    state.results.push({ runtime, workflow, profile, error: message, thresholdStatus: "FAIL", runId: state.runId });
    publish("result", state.results.at(-1));
    return false;
  } finally {
    if (appStarted) {
      const stop = await runCommand("docker", ["compose", "stop", `app-${runtime}`], { quiet: true });
      if (stop.code !== 0) addLog(`Não foi possível parar app-${runtime}; encerre pelo terminal se necessário.`, "warning");
    }
    state.completed += 1;
    update({ currentRuntime: null, progressPercent: 0, stage: `Concluídos ${state.completed} de ${state.total} cenários` });
  }
}

async function executeComparison(workflow, profile) {
  let hadFailure = false;
  try {
    if (!imagesReady) {
      await requireSuccess("docker", ["compose", "build", "app-node", "app-dotnet", "app-go"], "Construindo serviços");
      imagesReady = true;
    }
    await requireSuccess("docker", ["compose", "up", "-d", "--wait", "postgres", "redis"], "Iniciando PostgreSQL e Redis");
    const flows = workflow === "all" ? FLOWS : [workflow];
    for (const runtime of RUNTIMES) {
      for (const flow of flows) {
        const succeeded = await runScenario(runtime, flow, profile);
        if (!succeeded) hadFailure = true;
      }
    }
    update({
      status: hadFailure ? "completed_with_errors" : "completed",
      currentFlow: null,
      currentRuntime: null,
      stage: hadFailure ? "Concluído com falhas para revisar" : "Comparação concluída",
      finishedAt: new Date().toISOString(),
    });
    publish("done", snapshot());
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    addLog(message, "error");
    update({ status: "failed", error: message, stage: "Execução interrompida", finishedAt: new Date().toISOString() });
    publish("done", snapshot());
  } finally {
    runReserved = false;
  }
}

const staticFiles = {
  "/": ["index.html", "text/html; charset=utf-8"],
  "/app.css": ["app.css", "text/css; charset=utf-8"],
  "/app.js": ["app.js", "text/javascript; charset=utf-8"],
};

const server = createServer(async (request, response) => {
  const url = new URL(request.url || "/", `http://${HOST}:${PORT}`);
  if (request.method === "GET" && url.pathname === "/api/state") {
    return sendJson(response, 200, snapshot());
  }
  if (request.method === "GET" && url.pathname === "/api/results") {
    try {
      return sendJson(response, 200, { results: await readResults() });
    } catch (error) {
      return sendJson(response, 500, { error: error instanceof Error ? error.message : String(error) });
    }
  }
  if (request.method === "GET" && url.pathname === "/api/events") {
    response.writeHead(200, {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    });
    response.write(`event: snapshot\ndata: ${JSON.stringify(snapshot())}\n\n`);
    clients.add(response);
    request.on("close", () => clients.delete(response));
    return;
  }
  if (request.method === "POST" && url.pathname === "/api/runs") {
    if (state.status === "running" || runReserved) return sendJson(response, 409, { error: "Já existe uma comparação em andamento." });
    let body;
    try {
      body = await readRequestBody(request);
    } catch (error) {
      return sendJson(response, 400, { error: error instanceof Error ? error.message : String(error) });
    }
    const workflow = body.workflow;
    const profile = body.profile;
    if (!(workflow === "all" || FLOWS.includes(workflow)) || !PROFILES.includes(profile)) {
      return sendJson(response, 400, { error: "Selecione um fluxo e um perfil válidos." });
    }
    runReserved = true;
    try {
      await assertNoRunningApps();
    } catch (error) {
      runReserved = false;
      return sendJson(response, 409, { error: error instanceof Error ? error.message : String(error) });
    }
    const total = (workflow === "all" ? FLOWS.length : 1) * RUNTIMES.length;
    Object.assign(state, {
      status: "running",
      runId: `${Date.now()}`,
      workflow,
      profile,
      total,
      completed: 0,
      currentRuntime: null,
      currentFlow: null,
      stage: "Preparando a comparação",
      progressPercent: 0,
      startedAt: new Date().toISOString(),
      finishedAt: null,
      error: null,
      results: [],
      logs: [],
    });
    addLog(`Comparação iniciada: ${workflow === "all" ? "todos os fluxos" : FLOW_LABELS[workflow]}, perfil ${profile}.`);
    publish("state", snapshot());
    setImmediate(() => { void executeComparison(workflow, profile); });
    return sendJson(response, 202, { state: snapshot() });
  }
  if (request.method === "GET" && staticFiles[url.pathname]) {
    const [fileName, contentType] = staticFiles[url.pathname];
    try {
      const content = await readFile(path.join(WEB_ROOT, fileName));
      response.writeHead(200, { "Content-Type": contentType, "Cache-Control": "no-cache" });
      return response.end(content);
    } catch {
      return sendJson(response, 404, { error: "Arquivo não encontrado." });
    }
  }
  return sendJson(response, 404, { error: "Rota não encontrada." });
});

server.listen(PORT, HOST, () => {
  console.log(`Benchmark UI disponível em http://${HOST}:${PORT}`);
});
