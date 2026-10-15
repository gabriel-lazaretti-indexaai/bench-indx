# Benchmark de runtimes dos fluxos da Indexa

Este projeto isolado compara Node.js + NestJS, ASP.NET Core/.NET 10 e Go em cargas de aprovação, enriquecimento e sincronização do catálogo. O código do benchmark fica separado dos repositórios da Indexa e não altera os projetos originais.

O contrato e as fronteiras do que é medido estão em [`BENCHMARK-SCOPE.md`](BENCHMARK-SCOPE.md). Os fluxos reais consultados e as exclusões estão descritos ali.

## Dependências

- Docker Engine com Compose.
- `curl` e Bash no host.
- Os serviços, PostgreSQL, Redis e k6 rodam em containers; o SDK .NET não precisa ser instalado no host.

## UI de comparação ao vivo

```bash
./scripts/start-ui.sh
```

Abra `http://127.0.0.1:4179`. Escolha o perfil rápido (20 VUs por 60 s) ou de estresse (rampa até 100 VUs), depois clique em aprovação, enriquecimento ou sincronização para comparar esse fluxo nos três runtimes. **Rodar matriz completa** executa os nove pares fluxo/runtime do perfil escolhido.

A UI mostra logs e progresso do k6 por Server-Sent Events, atualiza a tabela/gráfico assim que cada runtime termina, carrega summaries existentes e grava os novos artefatos em `results/`. Execuções são sequenciais e limpam o banco e a stream antes de cada runtime. O servidor escuta somente em `127.0.0.1`, permite uma execução de cada vez e recusa iniciar se algum serviço `app-*` do Compose já estiver rodando. Pare a UI com `Ctrl+C`; encerre os containers do benchmark com `docker compose down`.

## Rodar uma matriz completa

```bash
./scripts/run-matrix.sh
python3 ./scripts/summarize.py
```

O runner constrói as imagens em modo de produção, inicia PostgreSQL e Redis, executa cada runtime em série, valida o resultado funcional de cada fluxo, injeta latência/falhas em uma rodada curta, aplica carga crescente por fluxo e roda carga mista. Os limites de container, pools e concorrências são iguais entre linguagens. Cada cenário limpa as tabelas e a stream Redis antes da execução. Mesmo quando um perfil falha thresholds, a matriz continua para produzir resultados dos outros runtimes; no fim o comando retorna código diferente de zero e registra os cenários com falha.

Os summaries do k6, o tráfego bruto, as amostras de CPU/memória e as versões dos ambientes ficam em `results/`. O runner da matriz comprime o JSON bruto como `.raw.json.gz` ao final; execuções individuais deixam `.raw.json`. Erros HTTP, falhas nos checks e inconsistências funcionais aparecem como thresholds falhados nos summaries; a matriz continua, registra os cenários e retorna erro ao final.

## Rodar um cenário isolado

```bash
docker compose up -d --wait postgres redis app-go
./scripts/reset-state.sh
./scripts/run-profile.sh go approval stress 0 0 100
python3 ./scripts/summarize.py
```

Os argumentos de `run-profile.sh` são: runtime (`node|dotnet|go`), fluxo (`approval|enrichment|synchronization|mixed`), perfil (`smoke|quick|stress|soak`), latência externa em milissegundos, falhar a cada N itens e itens por job.

Para parar os containers sem apagar os dados locais do benchmark:

```bash
docker compose down
```

Para remover também os volumes de PostgreSQL e Redis deste projeto:

```bash
docker compose down -v
```

## Como ler os resultados

- `benchmark_enqueue_ms`: latência do request que cria job, itens e outbox.
- `benchmark_completion_ms`: tempo de processamento medido pelo job no serviço.
- `benchmark_end_to_end_ms`: tempo observado pelo gerador desde o enqueue até o estado terminal, incluindo polling.
- `benchmark_items_per_second`: itens por segundo do worker por job.
- `http_req_duration`: latência HTTP do enqueue e da consulta de progresso.
- `*.resources.csv`: amostras do container durante a carga, com CPU e uso de memória.
- `*.raw.json.gz`: tráfego bruto comprimido pela matriz; use `gzip -dk arquivo.raw.json.gz` para descompactar sem remover o original.

Compare o mesmo fluxo, perfil, quantidade de itens e parâmetros. O custo de polling também aparece no resultado porque mede a consulta de progresso que a UI/cliente usaria durante um processo assíncrono. A máquina local limita os números; os resultados servem para comparação relativa sob esta configuração.

## Estrutura

- `node/`: serviço de referência Node.js + NestJS.
- `dotnet/`: serviço ASP.NET Core.
- `go/`: serviço Go.
- `schema/init.sql`: schema compartilhado por todas as implementações.
- `load/jobs.js`: workload k6 que envia jobs e acompanha o progresso.
- `scripts/`: reset de estado, execução de perfis, matriz completa e resumo.
- `results/`: resultados de referência do benchmark, incluindo summaries, amostras de recursos e capturas k6 comprimidas.
