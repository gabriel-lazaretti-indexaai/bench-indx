#!/usr/bin/env python3
"""Build a compact Markdown report from k6 summaries and docker-stats samples."""

from __future__ import annotations

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"


def metric_value(metrics: dict, name: str, key: str) -> float | None:
    metric = metrics.get(name)
    if not metric:
        return None
    values = metric.get("values", metric)
    value = values.get(key)
    return float(value) if isinstance(value, (int, float)) else None


def threshold_status(metrics: dict) -> str:
    statuses = []
    for metric in metrics.values():
        for result in metric.get("thresholds", {}).values():
            if isinstance(result, dict) and isinstance(result.get("ok"), bool):
                statuses.append(result["ok"])
            elif isinstance(result, bool):
                statuses.append(not result)
    if not statuses:
        checks = metrics.get("checks", {})
        request_failures = metrics.get("http_req_failed", {})
        functional_errors = metrics.get("benchmark_functional_errors", {})
        if "value" in checks:
            statuses.append(checks["value"] > 0.99)
        if "value" in request_failures:
            statuses.append(request_failures["value"] < 0.01)
        if "count" in functional_errors:
            statuses.append(functional_errors["count"] == 0)
    if not statuses:
        return "n/a"
    return "FAIL" if not all(statuses) else "PASS"


def format_number(value: float | None, digits: int = 2) -> str:
    return "—" if value is None else f"{value:.{digits}f}"


def memory_mib(sample: str) -> float | None:
    match = re.match(r"\s*([\d.]+)\s*([KMGT]?i?B)", sample)
    if not match:
        return None
    amount = float(match.group(1))
    unit = match.group(2).lower()
    factors = {
        "b": 1 / (1024 * 1024),
        "kb": 1 / 1024,
        "kib": 1 / 1024,
        "mb": 1,
        "mib": 1,
        "gb": 1024,
        "gib": 1024,
        "tb": 1024 * 1024,
        "tib": 1024 * 1024,
    }
    return amount * factors.get(unit, 1)


def resource_peaks(summary_path: Path) -> tuple[float | None, float | None]:
    resource_path = RESULTS / f"{summary_path.name.removesuffix('.summary.json')}.resources.csv"
    if not resource_path.exists():
        return None, None
    cpu_values = []
    memory_values = []
    for line in resource_path.read_text().splitlines():
        parts = line.strip().split(",", 1)
        if len(parts) != 2:
            continue
        cpu_match = re.search(r"([\d.]+)%", parts[0])
        if cpu_match:
            cpu_values.append(float(cpu_match.group(1)))
        used = parts[1].split("/", 1)[0].strip()
        mib = memory_mib(used)
        if mib is not None:
            memory_values.append(mib)
    return max(cpu_values, default=None), max(memory_values, default=None)


def main() -> None:
    summaries = sorted(RESULTS.glob("*.summary.json"))
    if not summaries:
        raise SystemExit(f"No k6 summary files found under {RESULTS}")

    rows = []
    for path in summaries:
        data = json.loads(path.read_text())
        metrics = data.get("metrics", {})
        stem_parts = path.name.removesuffix(".summary.json").split("-")
        language = stem_parts[1] if len(stem_parts) > 1 else "?"
        workflow = stem_parts[2] if len(stem_parts) > 2 else "?"
        profile = stem_parts[3] if len(stem_parts) > 3 else "?"
        latency = stem_parts[4].removeprefix("lat") if len(stem_parts) > 4 else "?"
        failures = stem_parts[5].removeprefix("fail") if len(stem_parts) > 5 else "?"
        cpu_peak, memory_peak = resource_peaks(path)
        rows.append(
            {
                "language": language,
                "workflow": workflow,
                "profile": profile,
                "latency": latency,
                "failures": failures,
                "jobs": metric_value(metrics, "benchmark_jobs_completed", "count"),
                "jobs_per_second": metric_value(metrics, "benchmark_jobs_completed", "rate"),
                "enqueue_p95": metric_value(metrics, "benchmark_enqueue_ms", "p(95)"),
                "completion_p95": metric_value(metrics, "benchmark_completion_ms", "p(95)"),
                "end_to_end_p95": metric_value(metrics, "benchmark_end_to_end_ms", "p(95)"),
                "items_s_avg": metric_value(metrics, "benchmark_items_per_second", "avg"),
                "requests_s": metric_value(metrics, "http_reqs", "rate"),
                "cpu_peak": cpu_peak,
                "memory_peak": memory_peak,
                "errors": metric_value(metrics, "benchmark_functional_errors", "count"),
                "thresholds": threshold_status(metrics),
                "source": path.name,
            }
        )

    report = [
        "# Resultados do benchmark",
        "",
        "Os arquivos `.summary.json` guardam as métricas resumidas do k6; o tráfego bruto fica em `.raw.json.gz` após uma matriz completa (execuções individuais deixam `.raw.json`). CPU e memória são máximos observados nas amostras do `docker stats`; latências são percentis reportados pelo k6.",
        "",
        "| Runtime | Fluxo | Perfil | Latência externa | Falha a cada | Jobs | Jobs/s | Enqueue p95 (ms) | Job p95 (ms) | E2E p95 (ms) | Média itens/s por job | HTTP req/s | CPU máx. | Memória máx. (MiB) | Erros funcionais | Thresholds | Arquivo |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in rows:
        report.append(
            "| {language} | {workflow} | {profile} | {latency} ms | {failures} | {jobs} | {jobs_per_second} | {enqueue_p95} | {completion_p95} | {end_to_end_p95} | {items_s_avg} | {requests_s} | {cpu_peak}% | {memory_peak} | {errors} | {thresholds} | `{source}` |".format(
                language=row["language"],
                workflow=row["workflow"],
                profile=row["profile"],
                latency=row["latency"],
                failures=row["failures"],
                jobs=format_number(row["jobs"], 0),
                jobs_per_second=format_number(row["jobs_per_second"]),
                enqueue_p95=format_number(row["enqueue_p95"]),
                completion_p95=format_number(row["completion_p95"]),
                end_to_end_p95=format_number(row["end_to_end_p95"]),
                items_s_avg=format_number(row["items_s_avg"]),
                requests_s=format_number(row["requests_s"]),
                cpu_peak=format_number(row["cpu_peak"]),
                memory_peak=format_number(row["memory_peak"]),
                errors=format_number(row["errors"], 0),
                thresholds=row["thresholds"],
                source=row["source"],
            )
        )
    report.extend(
        [
            "",
            "Para comparação direta, filtre linhas com o mesmo fluxo, perfil, latência externa e regra de falha. O perfil `smoke` valida contrato/contadores; use `stress` ou `quick` para comparar capacidade.",
            "",
        ]
    )
    output_path = RESULTS / "summary.md"
    output_path.write_text("\n".join(report))
    print(output_path)


if __name__ == "__main__":
    main()
