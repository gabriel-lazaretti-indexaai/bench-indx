# Resultados do benchmark

Os arquivos `.summary.json` guardam as métricas resumidas do k6; o tráfego bruto fica em `.raw.json.gz` após uma matriz completa (execuções individuais deixam `.raw.json`). CPU e memória são máximos observados nas amostras do `docker stats`; latências são percentis reportados pelo k6.

| Runtime | Fluxo | Perfil | Latência externa | Falha a cada | Jobs | Jobs/s | Enqueue p95 (ms) | Job p95 (ms) | E2E p95 (ms) | Média itens/s por job | HTTP req/s | CPU máx. | Memória máx. (MiB) | Erros funcionais | Thresholds | Arquivo |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| node | approval | smoke | 0 ms | 0 | 3 | 1.44 | 25.39 | 231.00 | 1155.70 | 453.32 | 6.23 | 3.84% | 54.93 | 0 | PASS | `20261013T202051Z-node-approval-smoke-lat0-fail0.summary.json` |
| node | approval | smoke | 5 ms | 10 | 3 | 2.41 | 12.20 | 216.80 | 417.70 | 478.12 | 7.23 | 1.37% | 55.47 | 0 | PASS | `20261013T202054Z-node-approval-smoke-lat5-fail10.summary.json` |
| node | approval | stress | 0 ms | 0 | 2322 | 22.08 | 94.36 | 1003.00 | 4383.90 | 209.73 | 243.14 | 46.33% | 110.40 | 0 | PASS | `20261013T202057Z-node-approval-stress-lat0-fail0.summary.json` |
| node | enrichment | smoke | 0 ms | 0 | 3 | 2.09 | 8.49 | 72.90 | 937.00 | 1382.55 | 6.97 | 0.10% | 108.80 | 0 | PASS | `20261013T202243Z-node-enrichment-smoke-lat0-fail0.summary.json` |
| node | enrichment | smoke | 5 ms | 10 | 3 | 3.60 | 9.48 | 101.80 | 392.10 | 993.46 | 8.39 | 0.10% | 108.10 | 0 | PASS | `20261013T202245Z-node-enrichment-smoke-lat5-fail10.summary.json` |
| node | enrichment | stress | 0 ms | 0 | 2128 | 19.94 | 96.03 | 1084.00 | 4711.65 | 204.52 | 236.61 | 38.83% | 113.00 | 0 | PASS | `20261013T202247Z-node-enrichment-stress-lat0-fail0.summary.json` |
| node | synchronization | smoke | 0 ms | 0 | 3 | 3.62 | 9.44 | 75.70 | 388.50 | 1351.84 | 8.45 | 0.20% | 111.10 | 0 | PASS | `20261013T202435Z-node-synchronization-smoke-lat0-fail0.summary.json` |
| node | synchronization | smoke | 5 ms | 10 | 3 | 3.64 | 6.42 | 79.80 | 389.00 | 1271.37 | 8.49 | 0.20% | 111.10 | 0 | PASS | `20261013T202437Z-node-synchronization-smoke-lat5-fail10.summary.json` |
| node | synchronization | stress | 0 ms | 0 | 2170 | 20.50 | 89.47 | 1076.55 | 4689.00 | 204.32 | 239.75 | 37.15% | 119.30 | 0 | PASS | `20261013T202439Z-node-synchronization-stress-lat0-fail0.summary.json` |
| node | mixed | quick | 5 ms | 0 | 1279 | 21.08 | 85.22 | 877.00 | 1188.00 | 188.92 | 109.89 | 38.40% | 119.60 | 0 | PASS | `20261013T202626Z-node-mixed-quick-lat5-fail0.summary.json` |
| dotnet | approval | smoke | 0 ms | 0 | 3 | 4.39 | 36.63 | 72.60 | 258.60 | 1422.80 | 8.77 | 27.17% | 60.55 | 0 | PASS | `20261013T202729Z-dotnet-approval-smoke-lat0-fail0.summary.json` |
| dotnet | approval | smoke | 5 ms | 10 | 3 | 4.75 | 9.47 | 99.90 | 212.70 | 1006.73 | 9.50 | 0.00% | 62.95 | 0 | PASS | `20261013T202731Z-dotnet-approval-smoke-lat5-fail10.summary.json` |
| dotnet | approval | stress | 0 ms | 0 | 2139 | 20.37 | 89.50 | 886.00 | 4888.00 | 205.60 | 247.87 | 76.12% | 96.10 | 0 | PASS | `20261013T202732Z-dotnet-approval-stress-lat0-fail0.summary.json` |
| dotnet | enrichment | smoke | 0 ms | 0 | 3 | 4.75 | 8.51 | 69.80 | 210.90 | 1463.90 | 9.50 | 0.19% | 91.91 | 0 | PASS | `20261013T202918Z-dotnet-enrichment-smoke-lat0-fail0.summary.json` |
| dotnet | enrichment | smoke | 5 ms | 10 | 3 | 4.75 | 8.83 | 99.90 | 211.00 | 1006.73 | 9.50 | 0.19% | 93.39 | 0 | PASS | `20261013T202920Z-dotnet-enrichment-smoke-lat5-fail10.summary.json` |
| dotnet | enrichment | stress | 0 ms | 0 | 2081 | 19.79 | 90.29 | 898.00 | 4893.00 | 206.13 | 247.44 | 93.40% | 98.12 | 0 | PASS | `20261013T202921Z-dotnet-enrichment-stress-lat0-fail0.summary.json` |
| dotnet | synchronization | smoke | 0 ms | 0 | 3 | 4.79 | 7.77 | 73.50 | 210.70 | 1423.74 | 9.58 | 7.11% | 97.22 | 0 | PASS | `20261013T203109Z-dotnet-synchronization-smoke-lat0-fail0.summary.json` |
| dotnet | synchronization | smoke | 5 ms | 10 | 3 | 4.81 | 6.35 | 77.80 | 208.90 | 1304.54 | 9.61 | 0.39% | 96.77 | 0 | PASS | `20261013T203111Z-dotnet-synchronization-smoke-lat5-fail10.summary.json` |
| dotnet | synchronization | stress | 0 ms | 0 | 1875 | 17.79 | 88.19 | 1016.00 | 5417.00 | 189.65 | 242.64 | 66.94% | 115.20 | 0 | PASS | `20261013T203112Z-dotnet-synchronization-stress-lat0-fail0.summary.json` |
| dotnet | mixed | quick | 5 ms | 0 | 1155 | 18.99 | 86.36 | 985.40 | 1397.00 | 130.39 | 104.14 | 89.78% | 107.50 | 0 | PASS | `20261013T203259Z-dotnet-mixed-quick-lat5-fail0.summary.json` |
| go | approval | smoke | 0 ms | 0 | 3 | 4.74 | 10.10 | 75.90 | 212.70 | 1339.66 | 9.49 | 1.10% | 16.22 | 0 | PASS | `20261013T203402Z-go-approval-smoke-lat0-fail0.summary.json` |
| go | approval | smoke | 5 ms | 10 | 3 | 4.73 | 11.66 | 104.80 | 213.70 | 964.71 | 9.45 | 1.10% | 17.12 | 0 | PASS | `20261013T203404Z-go-approval-smoke-lat5-fail10.summary.json` |
| go | approval | stress | 0 ms | 0 | 2600 | 24.73 | 95.98 | 379.00 | 3590.00 | 377.79 | 271.27 | 102.73% | 21.35 | 0 | PASS | `20261013T203405Z-go-approval-stress-lat0-fail0.summary.json` |
| go | enrichment | smoke | 0 ms | 0 | 3 | 4.78 | 7.80 | 72.80 | 209.00 | 1395.59 | 9.57 | 1.38% | 19.05 | 0 | PASS | `20261013T203551Z-go-enrichment-smoke-lat0-fail0.summary.json` |
| go | enrichment | smoke | 5 ms | 10 | 3 | 4.78 | 8.23 | 102.00 | 210.80 | 986.93 | 9.56 | 1.38% | 19.38 | 0 | PASS | `20261013T203553Z-go-enrichment-smoke-lat5-fail10.summary.json` |
| go | enrichment | stress | 0 ms | 0 | 2642 | 25.15 | 56.56 | 321.95 | 3795.95 | 380.00 | 272.73 | 100.12% | 21.49 | 0 | PASS | `20261013T203554Z-go-enrichment-stress-lat0-fail0.summary.json` |
| go | synchronization | smoke | 0 ms | 0 | 3 | 4.84 | 6.33 | 69.00 | 208.70 | 1471.23 | 9.67 | 13.20% | 18.77 | 0 | PASS | `20261013T203740Z-go-synchronization-smoke-lat0-fail0.summary.json` |
| go | synchronization | smoke | 5 ms | 10 | 3 | 4.83 | 5.84 | 78.70 | 207.90 | 1304.98 | 9.66 | 1.66% | 19.05 | 0 | PASS | `20261013T203742Z-go-synchronization-smoke-lat5-fail10.summary.json` |
| go | synchronization | stress | 0 ms | 0 | 2615 | 24.88 | 56.68 | 336.00 | 3999.00 | 375.54 | 273.55 | 102.71% | 22.13 | 0 | PASS | `20261013T203744Z-go-synchronization-stress-lat0-fail0.summary.json` |
| go | mixed | quick | 5 ms | 0 | 1582 | 25.91 | 55.09 | 378.85 | 884.00 | 343.04 | 119.05 | 99.57% | 19.69 | 0 | PASS | `20261013T203930Z-go-mixed-quick-lat5-fail0.summary.json` |

Para comparação direta, filtre linhas com o mesmo fluxo, perfil, latência externa e regra de falha. O perfil `smoke` valida contrato/contadores; use `stress` ou `quick` para comparar capacidade.
