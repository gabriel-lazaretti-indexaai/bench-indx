# Escopo do benchmark

## Objetivo

Comparar Node.js com NestJS, ASP.NET Core no .NET e Go em três fluxos sintéticos inspirados nos módulos atuais da Indexa. A pasta fica fora dos repositórios `app-backend` e `app-frontend`.

## Referência no backend atual

- Aprovação: `approvals.controller` → `CreateApprovalUseCase` cria aprovação, itens, progresso e subtarefas → `APPROVALS_START` → `StartApprovalUseCase` divide o trabalho em lotes de 100 → processador por anúncio → `ApproveChannelOverrideProductUseCase` chama o adapter de canal e atualiza o progresso.
- Enriquecimento: `enrichment.controller` → `CreateEnrichmentUseCase` cria enriquecimento, itens e subtarefas em lotes de 500 → `ENRICHMENT_START` → `StartEnrichmentUseCase` envia lotes de 100 → processador de produto/anúncio → geração, persistência e atualização de progresso.
- Sincronização do catálogo: `synchronizations.dash.controller` → `CreateSynchronizationUseCase` grava `Synchronization`, `Progress` e audit log → `SICRONIZATION_START` → `StartSynchronizationUseCase` → `ChannelsService.sync()` → `SyncUseCase` → adapter específico do canal, que pagina e persiste os anúncios.
- A fila real passa por `QueueService.publish()` → tabela `outbox_messages` → `OutboxDispatcherScheduler` → BullMQ/Redis. O benchmark reproduz a etapa de outbox e a execução assíncrona com Redis Streams. Não pretende comparar BullMQ contra outras filas.
- `product-sync` é outro fluxo: sincroniza IDs externos individuais e aplica estratégia de merge. Ele não faz parte deste benchmark inicial.

## Perfis comparados

1. **Aprovação**: cria uma solicitação de anúncio pendente, chama um adapter de publicação determinístico e persiste o resultado por item.
2. **Enriquecimento**: cria itens de anúncio, executa a mesma transformação determinística de título, slug, descrição e atributos e persiste o resultado. O provedor de IA é simulado; sem credenciais ou chamadas externas.
3. **Sincronização**: importa registros sintéticos em páginas de 100, normaliza os campos e faz upsert no catálogo persistido. O adapter e a origem externa são fixtures locais.

Os três perfis aceitam latência externa artificial e falha determinística a cada N itens. O perfil padrão usa latência zero e sem falhas para medir capacidade; cenários adicionais ativam latência e falhas para medir espera, progresso e recuperação funcional.

## Contrato HTTP

### `POST /api/benchmark/jobs`

```json
{
  "workflow": "approval",
  "itemCount": 500,
  "externalLatencyMs": 0,
  "failureEvery": 0
}
```

`workflow` aceita `approval`, `enrichment` ou `synchronization`. `itemCount` aceita de 1 a 5.000. `externalLatencyMs` aceita de 0 a 200. `failureEvery` vale zero para desativar falhas ou um inteiro positivo para falhar os itens cujo índice é múltiplo dele. A resposta é `202` com `jobId` e status `queued`.

### `GET /api/benchmark/jobs/{jobId}`

Retorna `jobId`, `workflow`, `status`, `totalItems`, `completedItems`, `failedItems`, `createdAt`, `startedAt`, `completedAt` e `durationMs`. O status final é `completed` se não houve falhas e `completed_with_errors` quando houve falhas injetadas.

### `GET /health`

Retorna `200` e informa runtime/framework e versões do serviço para identificar cada execução.

## Contrato de processamento

- O request cria o job, os itens sintéticos, o progresso e a mensagem de outbox em uma transação PostgreSQL.
- Um dispatcher publica jobs na stream Redis comum e marca a mensagem de outbox como despachada. A publicação pode ser repetida; o worker deve tratar itens já concluídos de forma idempotente.
- Workers leem a mesma stream/consumer group, usam o mesmo tamanho de página (100), concorrência de jobs (8), concorrência por item (8) e pool PostgreSQL (20).
- Atualizações de produto, item e progresso são persistidas em PostgreSQL. Um job só é confirmado na stream depois de atingir estado terminal.
- `approval` persiste status de publicação; `enrichment` persiste os campos gerados; `synchronization` insere/atualiza produtos da página sintética.
- Os adapters artificiais aplicam a mesma latência e regra de falha nos três runtimes. Falhas de item não cancelam o job; os itens restantes continuam e a contagem final precisa refletir sucessos e falhas.

## Metodologia de carga

- Limpar as tabelas de benchmark e as mensagens da stream antes de cada cenário, preservando o schema e o consumer group Redis.
- Construir os três serviços em modo release e executá-los serialmente, com o mesmo limite de CPU/memória e configurações de pool/worker.
- Rodar cenários por fluxo e um perfil misto. Os cenários smoke aquecem o processo antes do cenário de estresse de cada fluxo; os resultados smoke também são guardados.
- O gerador k6 envia jobs e consulta até cada job chegar a estado terminal. Mede latência de enqueue, latência total do job, taxa de conclusão, itens/s e erros HTTP/funcionais.
- Registrar runtime/versões, configurações, limites de recursos, resultado bruto do k6 e amostras de CPU/memória do container. Não comparar resultados de containers concorrendo entre si.
- Rodar pelo menos uma rodada sem latência externa, uma rodada com latência de 5 ms por chamada simulada e uma rodada com falha determinística; incluir uma carga sustentada e uma rampa de concorrência.

## O que o resultado significa

O benchmark compara implementações pequenas com semântica e infraestrutura comuns, não mede a Indexa inteira nem prevê sozinho o custo de migrar. Os stubs retiram a variabilidade de lojas, canais e modelos generativos. As regras específicas de cada adapter, os fan-outs completos, o rate limit real por conta, o custo do provedor de IA e os efeitos secundários posteriores à aprovação estão fora do perfil comum. Resultados são relativos a esta máquina e às versões registradas pelo runner.
