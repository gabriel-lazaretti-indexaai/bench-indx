const runtimeNames = { node: "Node.js + NestJS", dotnet: ".NET", go: "Go" };
const runtimeShort = { node: "Node.js", dotnet: ".NET", go: "Go" };
const runtimeColors = { node: "node", dotnet: "dotnet", go: "go" };
const flows = ["approval", "enrichment", "synchronization"];
const flowNames = {
  approval: "Aprovação",
  enrichment: "Enriquecimento",
  synchronization: "Sincronização",
};

let selectedProfile = "stress";
let selectedMetric = "jobsPerSecond";
let historicalResults = [];
let runState = null;
let logItems = [];

const connection = document.querySelector("#connection");
const connectionLabel = document.querySelector("#connection-label");
const eventList = document.querySelector("#event-list");

function formatNumber(value, digits = 2) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "-";
  return new Intl.NumberFormat("pt-BR", { maximumFractionDigits: digits, minimumFractionDigits: digits }).format(value);
}

function formatMilliseconds(value) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "-";
  return `${formatNumber(value, 0)} ms`;
}

function activeResults() {
  const combined = new Map();
  for (const result of historicalResults) {
    if (result.profile !== selectedProfile || !flows.includes(result.workflow)) continue;
    const key = `${result.workflow}:${result.runtime}`;
    if (!combined.has(key)) combined.set(key, { ...result, current: false });
  }
  for (const result of runState?.results || []) {
    if (result.profile !== selectedProfile || !flows.includes(result.workflow)) continue;
    combined.set(`${result.workflow}:${result.runtime}`, { ...result, current: true });
  }
  return combined;
}

function selectedRows() {
  const results = activeResults();
  return flows.flatMap((flow) => ["node", "dotnet", "go"].map((runtime) => results.get(`${flow}:${runtime}`) || {
    runtime, workflow: flow, profile: selectedProfile, jobsPerSecond: null, completionP95Ms: null,
    endToEndP95Ms: null, cpuPeak: null, memoryPeakMiB: null, thresholdStatus: null,
  }));
}

function renderChart() {
  const chart = document.querySelector("#chart");
  chart.replaceChildren();
  const rows = activeResults();
  const metric = selectedMetric;
  const isLatency = metric === "completionP95Ms";
  const vals = [...rows.values()].map((row) => row[metric]).filter((value) => typeof value === "number");
  const maxValue = isLatency ? 1200 : 30;
  document.querySelector("#chart-unit").textContent = isLatency ? "duração do job · p95 em ms" : "jobs concluídos / segundo";
  document.querySelector("#chart-scale").textContent = `escala: 0–${isLatency ? "1.200 ms" : "30 jobs/s"}`;

  for (const flow of flows) {
    const group = document.createElement("section");
    group.className = "chart-flow";
    const title = document.createElement("h3");
    title.className = "chart-flow-title";
    title.textContent = flowNames[flow];
    group.append(title);
    for (const runtime of ["node", "dotnet", "go"]) {
      const row = rows.get(`${flow}:${runtime}`);
      const value = row?.[metric];
      const line = document.createElement("div");
      line.className = "chart-bar-row";
      const label = document.createElement("span");
      label.className = "chart-runtime";
      const dot = document.createElement("i");
      dot.className = runtimeColors[runtime];
      label.append(dot, document.createTextNode(runtimeShort[runtime]));
      const track = document.createElement("span");
      track.className = "chart-track";
      const fill = document.createElement("span");
      fill.className = `chart-fill ${runtimeColors[runtime]}`;
      fill.style.width = typeof value === "number" ? `${Math.min(100, value / maxValue * 100)}%` : "0%";
      track.append(fill);
      const number = document.createElement("span");
      number.className = `chart-value${typeof value === "number" ? "" : " is-missing"}`;
      number.textContent = typeof value === "number" ? (isLatency ? formatMilliseconds(value) : formatNumber(value)) : "aguardando";
      line.append(label, track, number);
      group.append(line);
    }
    chart.append(group);
  }
}

function renderTable() {
  const body = document.querySelector("#results-body");
  body.replaceChildren();
  const rows = selectedRows();
  for (const row of rows) {
    const tr = document.createElement("tr");
    if (row.current) tr.classList.add("is-current");
    const cells = [
      { value: flowNames[row.workflow] },
      { value: runtimeNames[row.runtime], className: "runtime-cell" },
      { value: formatNumber(row.jobsPerSecond), className: "metric" },
      { value: formatMilliseconds(row.completionP95Ms), className: "metric" },
      { value: formatMilliseconds(row.endToEndP95Ms), className: "metric" },
      { value: typeof row.cpuPeak === "number" ? `${formatNumber(row.cpuPeak, 1)}%` : "-", className: "metric" },
      { value: typeof row.memoryPeakMiB === "number" ? `${formatNumber(row.memoryPeakMiB, 1)} MiB` : "-", className: "metric" },
      { value: row.error ? "Falhou" : row.thresholdStatus || "-", className: row.thresholdStatus === "PASS" ? "check-pass" : row.thresholdStatus === "FAIL" ? "check-fail" : "" },
    ];
    for (const cell of cells) {
      const td = document.createElement("td");
      td.textContent = cell.value;
      if (cell.className) td.className = cell.className;
      tr.append(td);
    }
    body.append(tr);
  }
  document.querySelector("#table-profile").textContent = `Perfil: ${selectedProfile === "stress" ? "estresse" : "rápido"}`;
  const liveCount = (runState?.results || []).filter((row) => row.profile === selectedProfile && !row.error).length;
  document.querySelector("#results-caption").textContent = runState?.status === "running"
    ? `${liveCount} resultado(s) já recebidos nesta comparação; a tabela é atualizada ao fim de cada runtime.`
    : "Últimos resultados salvos para o perfil selecionado. Os dados da nova execução substituem as referências assim que chegam.";
}

function addLog(item) {
  logItems.push(item);
  logItems = logItems.slice(-80);
  eventList.replaceChildren();
  if (logItems.length === 0) {
    const empty = document.createElement("p");
    empty.className = "empty-event";
    empty.textContent = "O progresso do Docker e do k6 aparecerá aqui.";
    eventList.append(empty);
  } else {
    for (const entry of logItems.slice(-28)) {
      const line = document.createElement("p");
      line.className = `event-line${entry.level === "error" || entry.level === "warning" ? " is-error" : entry.level === "success" ? " is-success" : ""}`;
      const time = document.createElement("time");
      time.textContent = new Date(entry.time).toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
      const text = document.createElement("span");
      text.textContent = entry.message;
      line.append(time, text);
      eventList.append(line);
    }
    eventList.scrollTop = eventList.scrollHeight;
  }
  document.querySelector("#event-count").textContent = String(logItems.length);
}

function renderState() {
  if (!runState) return;
  const running = runState.status === "running";
  const failed = runState.status === "failed" || runState.status === "completed_with_errors";
  const statusPanel = document.querySelector("#run-status");
  statusPanel.classList.toggle("is-running", running);
  statusPanel.classList.toggle("is-complete", !running && !failed && runState.status !== "idle");
  statusPanel.classList.toggle("is-error", failed);
  document.querySelector("#status-heading").textContent = running
    ? `${flowNames[runState.currentFlow] || (runState.workflow === "all" ? "Matriz completa" : "Comparação")} em andamento`
    : runState.status === "completed" ? "Comparação concluída"
      : failed ? "Execução precisa de atenção" : "Aguardando uma comparação";
  document.querySelector("#run-counter").textContent = runState.total ? `${runState.completed} / ${runState.total}` : "-- / --";
  document.querySelector("#stage-label").textContent = runState.stage || "Aguardando execução";
  document.querySelector("#current-label").textContent = running && runState.currentRuntime
    ? `${runtimeNames[runState.currentRuntime]}${runState.progressPercent ? ` · ${runState.progressPercent}%` : ""}`
    : runState.profile ? `Perfil ${runState.profile === "stress" ? "estresse" : "rápido"}` : "Runner local";
  const totalProgress = runState.total ? (runState.completed + (running ? (runState.progressPercent || 0) / 100 : 0)) / runState.total * 100 : 0;
  document.querySelector("#progress-fill").style.width = `${Math.min(100, totalProgress)}%`;
  document.querySelector("#status-label").textContent = running ? "Executando" : failed ? "Revisar" : runState.status === "completed" ? "Concluído" : "Pronto";
  document.querySelectorAll("[data-flow], #run-all, [data-profile]").forEach((button) => { button.disabled = running; });
  renderChart();
  renderTable();
}

async function startRun(workflow) {
  const message = document.querySelector("#action-message");
  message.hidden = true;
  message.textContent = "";
  try {
    const response = await fetch("/api/runs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ workflow, profile: selectedProfile }),
    });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "Não foi possível iniciar a comparação.");
    runState = data.state;
    logItems = [...(runState.logs || [])];
    addLog({ time: new Date().toISOString(), message: "Pedido aceito pelo runner local.", level: "success" });
    renderState();
  } catch (error) {
    message.textContent = error instanceof Error ? error.message : String(error);
    message.hidden = false;
  }
}

document.querySelectorAll("[data-flow]").forEach((button) => button.addEventListener("click", () => startRun(button.dataset.flow)));
document.querySelector("#run-all").addEventListener("click", () => startRun("all"));
document.querySelectorAll("[data-profile]").forEach((button) => button.addEventListener("click", () => {
  selectedProfile = button.dataset.profile;
  document.querySelectorAll("[data-profile]").forEach((option) => {
    const selected = option.dataset.profile === selectedProfile;
    option.classList.toggle("is-selected", selected);
    option.setAttribute("aria-pressed", String(selected));
  });
  renderChart();
  renderTable();
}));
document.querySelectorAll("[data-metric]").forEach((button) => button.addEventListener("click", () => {
  selectedMetric = button.dataset.metric;
  document.querySelectorAll("[data-metric]").forEach((option) => {
    const selected = option.dataset.metric === selectedMetric;
    option.classList.toggle("is-selected", selected);
    option.setAttribute("aria-pressed", String(selected));
  });
  renderChart();
}));

async function loadInitialData() {
  try {
    const [stateResponse, resultResponse] = await Promise.all([fetch("/api/state"), fetch("/api/results")]);
    runState = await stateResponse.json();
    const resultData = await resultResponse.json();
    historicalResults = resultData.results || [];
    logItems = [...(runState.logs || [])];
    addLog({ time: new Date().toISOString(), message: `${historicalResults.length} resumo(s) local(is) carregado(s).`, level: "info" });
    renderState();
  } catch {
    document.querySelector("#action-message").textContent = "Não foi possível carregar os resultados locais.";
    document.querySelector("#action-message").hidden = false;
  }
}

const events = new EventSource("/api/events");
events.addEventListener("open", () => {
  connection.classList.add("is-connected");
  connection.classList.remove("is-disconnected");
  connectionLabel.textContent = "Runner conectado";
});
events.addEventListener("error", () => {
  connection.classList.remove("is-connected");
  connection.classList.add("is-disconnected");
  connectionLabel.textContent = "Reconectando";
});
events.addEventListener("snapshot", (event) => {
  runState = JSON.parse(event.data);
  logItems = [...(runState.logs || [])];
  addLog({ time: new Date().toISOString(), message: "Canal de eventos ativo.", level: "info" });
  renderState();
});
events.addEventListener("state", (event) => {
  runState = JSON.parse(event.data);
  logItems = [...(runState.logs || [])];
  addLog({ time: new Date().toISOString(), message: runState.stage || "Estado atualizado.", level: "info" });
  renderState();
});
events.addEventListener("progress", (event) => {
  const progress = JSON.parse(event.data);
  if (!runState) return;
  runState.progressPercent = progress.progressPercent;
  renderState();
});
events.addEventListener("log", (event) => addLog(JSON.parse(event.data)));
events.addEventListener("result", (event) => {
  const result = JSON.parse(event.data);
  if (!runState) return;
  runState.results = [...(runState.results || []).filter((item) => !(item.runtime === result.runtime && item.workflow === result.workflow)), result];
  renderChart();
  renderTable();
});
events.addEventListener("done", (event) => {
  runState = JSON.parse(event.data);
  logItems = [...(runState.logs || [])];
  addLog({ time: new Date().toISOString(), message: runState.stage || "Execução finalizada.", level: runState.status === "completed" ? "success" : "warning" });
  renderState();
  fetch("/api/results").then((response) => response.json()).then((data) => {
    historicalResults = data.results || [];
    renderChart();
    renderTable();
  }).catch(() => {});
});

loadInitialData();
