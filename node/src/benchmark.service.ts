import {
  Injectable,
  Logger,
  NotFoundException,
  OnApplicationShutdown,
  OnModuleInit,
} from "@nestjs/common";
import Redis from "ioredis";
import { Pool, PoolClient } from "pg";
import { CreateBenchmarkJobDto, Workflow } from "./benchmark.dto";

const STREAM_NAME = "benchmark:jobs";
const CONSUMER_GROUP = "benchmark-workers";
const JOB_CONCURRENCY = 8;
const ITEM_CONCURRENCY = 8;
const PAGE_SIZE = 100;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

interface JobRow {
  id: string;
  workflow: Workflow;
  status: string;
  total_items: number;
  completed_items: number;
  failed_items: number;
  external_latency_ms: number;
  failure_every: number;
}

interface ItemRow {
  item_index: number;
  external_id: string;
  product_id: string | null;
  source_payload: unknown;
}

interface StreamMessage {
  id: string;
  jobId: string;
}

interface SourcePayload {
  externalId: string;
  sku: string;
  title: string;
  description: string;
  category: string;
  attributes: Record<string, unknown>;
}

@Injectable()
export class BenchmarkService implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(BenchmarkService.name);
  private readonly pool = new Pool({
    connectionString:
      process.env.DATABASE_URL ??
      "postgres://benchmark:benchmark@localhost:5432/benchmark",
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
  });
  private readonly redis = new Redis(
    process.env.REDIS_URL ?? "redis://localhost:6379",
    { maxRetriesPerRequest: null },
  );
  private stopping = false;
  private staleMessageCursor = "0-0";
  private dispatcherTask?: Promise<void>;
  private consumerTask?: Promise<void>;

  async onModuleInit(): Promise<void> {
    await this.pool.query("SELECT 1");
    await this.redis.ping();
    try {
      await this.redis.xgroup("CREATE", STREAM_NAME, CONSUMER_GROUP, "0", "MKSTREAM");
    } catch (error) {
      if (!(error instanceof Error) || !error.message.includes("BUSYGROUP")) {
        throw error;
      }
    }

    this.dispatcherTask = this.runOutboxDispatcher();
    this.consumerTask = this.runStreamConsumer();
  }

  async onApplicationShutdown(): Promise<void> {
    this.stopping = true;
    await Promise.allSettled([this.dispatcherTask, this.consumerTask].filter(Boolean));
    await this.redis.quit();
    await this.pool.end();
  }

  async createJob(input: CreateBenchmarkJobDto): Promise<{ jobId: string; status: string }> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const jobResult = await client.query<{ id: string }>(
        `INSERT INTO benchmark_jobs
          (workflow, total_items, external_latency_ms, failure_every)
         VALUES ($1, $2, $3, $4)
         RETURNING id`,
        [input.workflow, input.itemCount, input.externalLatencyMs, input.failureEvery],
      );
      const jobId = jobResult.rows[0].id;

      if (input.workflow === "synchronization") {
        await client.query(
          `INSERT INTO benchmark_job_items
            (job_id, item_index, external_id, source_payload)
           SELECT
             $1::uuid,
             item.item_index,
             $1::uuid::text || ':' || item.item_index::text,
             jsonb_build_object(
               'externalId', $1::uuid::text || ':' || item.item_index::text,
               'sku', 'SKU-' || item.item_index::text,
               'title', 'Imported Product ' || item.item_index::text,
               'description', 'Imported description for product ' || item.item_index::text,
               'category', 'category-' || (((item.item_index - 1) % 10) + 1)::text,
               'attributes', jsonb_build_object(
                 'source', 'fixture',
                 'page', ceil(item.item_index::numeric / 100)::integer
               )
             )
           FROM generate_series(1, $2::integer) AS item(item_index)`,
          [jobId, input.itemCount],
        );
      } else {
        await client.query(
          `INSERT INTO benchmark_products
            (external_id, sku, name, description, category)
           SELECT
             $1::uuid::text || ':' || item.item_index::text,
             'SKU-' || item.item_index::text,
             'Product ' || item.item_index::text,
             'Synthetic description for product ' || item.item_index::text,
             'category-' || (((item.item_index - 1) % 10) + 1)::text
           FROM generate_series(1, $2::integer) AS item(item_index)`,
          [jobId, input.itemCount],
        );
        await client.query(
          `INSERT INTO benchmark_job_items (job_id, item_index, external_id, product_id)
           SELECT $1::uuid, item.item_index, product.external_id, product.id
           FROM generate_series(1, $2::integer) AS item(item_index)
           JOIN benchmark_products product
             ON product.external_id = $1::uuid::text || ':' || item.item_index::text`,
          [jobId, input.itemCount],
        );
      }

      await client.query(
        `INSERT INTO benchmark_outbox (job_id, payload)
         VALUES ($1::uuid, jsonb_build_object('jobId', $1::uuid::text))`,
        [jobId],
      );
      await client.query("COMMIT");
      return { jobId, status: "queued" };
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async getJob(jobId: string) {
    if (!UUID_PATTERN.test(jobId)) {
      throw new NotFoundException("Benchmark job not found");
    }

    const result = await this.pool.query<JobRow & {
      created_at: Date;
      started_at: Date | null;
      completed_at: Date | null;
    }>(
      `SELECT id, workflow, status, total_items, completed_items, failed_items,
              created_at, started_at, completed_at
       FROM benchmark_jobs
       WHERE id = $1`,
      [jobId],
    );
    const job = result.rows[0];
    if (!job) {
      throw new NotFoundException("Benchmark job not found");
    }

    const startedAt = job.started_at ? new Date(job.started_at).getTime() : null;
    const completedAt = job.completed_at ? new Date(job.completed_at).getTime() : null;
    const durationMs =
      startedAt === null
        ? null
        : Math.max(0, (completedAt ?? Date.now()) - startedAt);

    return {
      jobId: job.id,
      workflow: job.workflow,
      status: job.status,
      totalItems: job.total_items,
      completedItems: job.completed_items,
      failedItems: job.failed_items,
      createdAt: new Date(job.created_at).toISOString(),
      startedAt: job.started_at ? new Date(job.started_at).toISOString() : null,
      completedAt: job.completed_at ? new Date(job.completed_at).toISOString() : null,
      durationMs,
    };
  }

  private async runOutboxDispatcher(): Promise<void> {
    while (!this.stopping) {
      try {
        await this.dispatchPendingOutbox();
      } catch (error) {
        this.logger.error("Outbox dispatch failed", this.errorMessage(error));
      }
      await this.delay(250);
    }
  }

  private async dispatchPendingOutbox(): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const pending = await client.query<{ id: string; job_id: string }>(
        `SELECT id, job_id
         FROM benchmark_outbox
         WHERE dispatched_at IS NULL
         ORDER BY id
         LIMIT 50
         FOR UPDATE SKIP LOCKED`,
      );

      for (const row of pending.rows) {
        await this.redis.xadd(
          STREAM_NAME,
          "*",
          "jobId",
          row.job_id,
          "outboxId",
          row.id,
        );
        await client.query(
          "UPDATE benchmark_outbox SET dispatched_at = clock_timestamp() WHERE id = $1",
          [row.id],
        );
      }
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  private async runStreamConsumer(): Promise<void> {
    const consumerName = `${process.env.HOSTNAME ?? "node"}-${process.pid}`;
    while (!this.stopping) {
      try {
        const reclaimed = await this.claimStaleMessages(consumerName);
        const messages =
          reclaimed.length > 0 ? reclaimed : await this.readNewMessages(consumerName);
        if (messages.length === 0) {
          continue;
        }

        await Promise.all(
          messages.slice(0, JOB_CONCURRENCY).map((message) =>
            this.consumeMessage(message),
          ),
        );
      } catch (error) {
        this.logger.error("Redis stream consumer failed", this.errorMessage(error));
        await this.delay(1000);
      }
    }
  }

  private async claimStaleMessages(consumerName: string): Promise<StreamMessage[]> {
    const response = (await this.redis.xautoclaim(
      STREAM_NAME,
      CONSUMER_GROUP,
      consumerName,
      "5000",
      this.staleMessageCursor,
      "COUNT",
      String(JOB_CONCURRENCY),
    )) as [string, [string, string[]][]];
    this.staleMessageCursor = response?.[0] ?? "0-0";
    return this.parseStreamEntries(response?.[1]);
  }

  private async readNewMessages(consumerName: string): Promise<StreamMessage[]> {
    const response = (await this.redis.xreadgroup(
      "GROUP",
      CONSUMER_GROUP,
      consumerName,
      "COUNT",
      String(JOB_CONCURRENCY),
      "BLOCK",
      "1000",
      "STREAMS",
      STREAM_NAME,
      ">",
    )) as [string, [string, string[]][]][] | null;
    return this.parseStreamEntries(response?.[0]?.[1]);
  }

  private parseStreamEntries(entries?: [string, string[]][]): StreamMessage[] {
    if (!Array.isArray(entries)) {
      return [];
    }

    return entries.map(([id, fields]) => {
      const values = new Map<string, string>();
      for (let index = 0; index + 1 < fields.length; index += 2) {
        values.set(fields[index], fields[index + 1]);
      }
      return { id, jobId: values.get("jobId") ?? "" };
    });
  }

  private async consumeMessage(message: StreamMessage): Promise<void> {
    if (!UUID_PATTERN.test(message.jobId)) {
      this.logger.warn(`Acknowledging malformed benchmark stream entry ${message.id}`);
      await this.redis.xack(STREAM_NAME, CONSUMER_GROUP, message.id);
      return;
    }

    try {
      const terminal = await this.processJob(message.jobId);
      if (terminal) {
        await this.redis.xack(STREAM_NAME, CONSUMER_GROUP, message.id);
      }
    } catch (error) {
      this.logger.error(
        `Benchmark job ${message.jobId} failed during processing`,
        this.errorMessage(error),
      );
    }
  }

  private async processJob(jobId: string): Promise<boolean> {
    const lockClient = await this.pool.connect();
    let hasLock = false;
    try {
      const lockResult = await lockClient.query<{ acquired: boolean }>(
        "SELECT pg_try_advisory_lock(hashtextextended($1, 0)) AS acquired",
        [jobId],
      );
      hasLock = lockResult.rows[0].acquired;
      if (!hasLock) {
        return false;
      }

      const jobResult = await this.pool.query<JobRow>(
        `SELECT id, workflow, status, total_items, completed_items, failed_items,
                external_latency_ms, failure_every
         FROM benchmark_jobs
         WHERE id = $1`,
        [jobId],
      );
      const job = jobResult.rows[0];
      if (!job || this.isTerminal(job.status)) {
        return true;
      }

      await this.pool.query(
        `UPDATE benchmark_jobs
         SET status = 'running', started_at = COALESCE(started_at, clock_timestamp())
         WHERE id = $1 AND status = 'queued'`,
        [jobId],
      );
      await this.pool.query(
        `UPDATE benchmark_job_items
         SET status = 'pending', updated_at = clock_timestamp()
         WHERE job_id = $1 AND status = 'processing'`,
        [jobId],
      );

      while (true) {
        const page = await this.pool.query<ItemRow>(
          `SELECT item_index, external_id, product_id, source_payload
           FROM benchmark_job_items
           WHERE job_id = $1 AND status = 'pending'
           ORDER BY item_index
           LIMIT $2`,
          [jobId, PAGE_SIZE],
        );
        if (page.rows.length === 0) {
          break;
        }
        await this.processPage(job, page.rows);
      }

      const finalized = await this.pool.query<{
        status: string;
        completed_items: number;
        failed_items: number;
      }>(
        `WITH item_counts AS (
           SELECT
             count(*) FILTER (WHERE status = 'completed')::integer AS completed_items,
             count(*) FILTER (WHERE status = 'failed')::integer AS failed_items
           FROM benchmark_job_items
           WHERE job_id = $1
         )
         UPDATE benchmark_jobs job
         SET completed_items = item_counts.completed_items,
             failed_items = item_counts.failed_items,
             status = CASE
               WHEN item_counts.completed_items + item_counts.failed_items < job.total_items
                 THEN 'running'
               WHEN item_counts.failed_items > 0 THEN 'completed_with_errors'
               ELSE 'completed'
             END,
             completed_at = CASE
               WHEN item_counts.completed_items + item_counts.failed_items >= job.total_items
                 THEN clock_timestamp()
               ELSE NULL
             END
         FROM item_counts
         WHERE job.id = $1
         RETURNING job.status, job.completed_items, job.failed_items`,
        [jobId],
      );
      const finalJob = finalized.rows[0];
      if (
        !finalJob ||
        finalJob.completed_items + finalJob.failed_items !== job.total_items ||
        !this.isTerminal(finalJob.status)
      ) {
        throw new Error(`Job ${jobId} did not reach a terminal item state`);
      }
      return true;
    } finally {
      if (hasLock) {
        await lockClient
          .query("SELECT pg_advisory_unlock(hashtextextended($1, 0))", [jobId])
          .catch((error: unknown) => {
            this.logger.warn(`Could not release job lock ${jobId}: ${this.errorMessage(error)}`);
          });
      }
      lockClient.release();
    }
  }

  private async processPage(job: JobRow, items: ItemRow[]): Promise<void> {
    if (job.workflow === "synchronization" && job.external_latency_ms > 0) {
      await this.delay(job.external_latency_ms);
    }

    const errors: unknown[] = [];
    let nextIndex = 0;
    const workerCount = Math.min(ITEM_CONCURRENCY, items.length);
    await Promise.all(
      Array.from({ length: workerCount }, async () => {
        while (nextIndex < items.length) {
          const item = items[nextIndex++];
          try {
            await this.processItem(job, item);
          } catch (error) {
            errors.push(error);
          }
        }
      }),
    );
    if (errors.length > 0) {
      throw errors[0];
    }
  }

  private async processItem(job: JobRow, item: ItemRow): Promise<void> {
    const claimed = await this.pool.query(
      `UPDATE benchmark_job_items
       SET status = 'processing', updated_at = clock_timestamp()
       WHERE job_id = $1 AND item_index = $2 AND status = 'pending'
       RETURNING item_index`,
      [job.id, item.item_index],
    );
    if (claimed.rowCount === 0) {
      return;
    }

    if (job.workflow !== "synchronization" && job.external_latency_ms > 0) {
      await this.delay(job.external_latency_ms);
    }
    if (job.failure_every > 0 && item.item_index % job.failure_every === 0) {
      await this.persistFailure(job.id, item.item_index);
      return;
    }

    switch (job.workflow) {
      case "approval":
        await this.persistApproval(job.id, item);
        return;
      case "enrichment":
        await this.persistEnrichment(job.id, item);
        return;
      case "synchronization":
        await this.persistSynchronization(job.id, item);
        return;
    }
  }

  private async persistApproval(jobId: string, item: ItemRow): Promise<void> {
    if (!item.product_id) {
      throw new Error(`Approval item ${item.item_index} has no product`);
    }
    const result = { status: "approved", externalId: item.external_id };
    await this.persistSuccessfulItem(jobId, item.item_index, item.product_id, result, async (client) => {
      await client.query(
        `UPDATE benchmark_products
         SET approval_status = 'approved', updated_at = clock_timestamp()
         WHERE id = $1`,
        [item.product_id],
      );
    });
  }

  private async persistEnrichment(jobId: string, item: ItemRow): Promise<void> {
    if (!item.product_id) {
      throw new Error(`Enrichment item ${item.item_index} has no product`);
    }
    const suffix = item.item_index;
    const title = `Optimized Product ${suffix}`;
    const enrichment = {
      title,
      slug: `optimized-product-${suffix}`,
      description: `Enriched description for product ${suffix}`,
      attributes: {
        benchmark: true,
        material: suffix % 2 === 0 ? "cotton" : "linen",
        color: suffix % 3 === 0 ? "blue" : "black",
      },
    };

    await this.persistSuccessfulItem(
      jobId,
      item.item_index,
      item.product_id,
      enrichment,
      async (client) => {
        await client.query(
          `UPDATE benchmark_products
           SET name = $2,
               description = $3,
               attributes = $4::jsonb,
               enrichment = $4::jsonb,
               updated_at = clock_timestamp()
           WHERE id = $1`,
          [
            item.product_id,
            enrichment.title,
            enrichment.description,
            JSON.stringify(enrichment),
          ],
        );
      },
    );
  }

  private async persistSynchronization(jobId: string, item: ItemRow): Promise<void> {
    const payload = this.readSourcePayload(item.source_payload);
    if (payload.externalId !== item.external_id) {
      throw new Error(`Synchronization item ${item.item_index} has an invalid external ID`);
    }
    const normalized = {
      externalId: payload.externalId.trim(),
      sku: payload.sku.trim(),
      name: payload.title.trim(),
      description: payload.description.trim(),
      category: payload.category.trim().toLowerCase(),
      attributes: payload.attributes,
    };
    const result = await this.pool.connect();
    try {
      await result.query("BEGIN");
      const productResult = await result.query<{ id: string }>(
        `INSERT INTO benchmark_products
          (external_id, sku, name, description, category, attributes, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6::jsonb, clock_timestamp())
         ON CONFLICT (external_id) DO UPDATE SET
           sku = EXCLUDED.sku,
           name = EXCLUDED.name,
           description = EXCLUDED.description,
           category = EXCLUDED.category,
           attributes = EXCLUDED.attributes,
           updated_at = clock_timestamp()
         RETURNING id`,
        [
          normalized.externalId,
          normalized.sku,
          normalized.name,
          normalized.description,
          normalized.category,
          JSON.stringify(normalized.attributes),
        ],
      );
      await this.completeItemInTransaction(
        result,
        jobId,
        item.item_index,
        productResult.rows[0].id,
        normalized,
      );
      await result.query("COMMIT");
    } catch (error) {
      await result.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      result.release();
    }
  }

  private async persistSuccessfulItem(
    jobId: string,
    itemIndex: number,
    productId: string,
    value: Record<string, unknown>,
    updateProduct: (client: PoolClient) => Promise<void>,
  ): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await updateProduct(client);
      await this.completeItemInTransaction(client, jobId, itemIndex, productId, value);
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  private async completeItemInTransaction(
    client: PoolClient,
    jobId: string,
    itemIndex: number,
    productId: string,
    result: Record<string, unknown>,
  ): Promise<void> {
    const itemUpdate = await client.query(
      `UPDATE benchmark_job_items
       SET status = 'completed', product_id = $3, result = $4::jsonb,
           error_message = NULL, updated_at = clock_timestamp()
       WHERE job_id = $1 AND item_index = $2 AND status = 'processing'
       RETURNING item_index`,
      [jobId, itemIndex, productId, JSON.stringify(result)],
    );
    if ((itemUpdate.rowCount ?? 0) > 0) {
      await client.query(
        "UPDATE benchmark_jobs SET completed_items = completed_items + 1 WHERE id = $1",
        [jobId],
      );
    }
  }

  private async persistFailure(jobId: string, itemIndex: number): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query(
        `UPDATE benchmark_job_items
         SET status = 'failed', error_message = 'Injected deterministic failure',
             updated_at = clock_timestamp()
         WHERE job_id = $1 AND item_index = $2 AND status = 'processing'
         RETURNING item_index`,
        [jobId, itemIndex],
      );
      if ((result.rowCount ?? 0) > 0) {
        await client.query(
          "UPDATE benchmark_jobs SET failed_items = failed_items + 1 WHERE id = $1",
          [jobId],
        );
      }
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  private readSourcePayload(value: unknown): SourcePayload {
    let payload: unknown = value;
    if (typeof payload === "string") {
      try {
        payload = JSON.parse(payload) as unknown;
      } catch {
        throw new Error("Synchronization source payload is invalid JSON");
      }
    }
    if (!this.isRecord(payload)) {
      throw new Error("Synchronization source payload is not an object");
    }
    const stringFields = ["externalId", "sku", "title", "description", "category"] as const;
    for (const field of stringFields) {
      if (typeof payload[field] !== "string") {
        throw new Error(`Synchronization source payload field ${field} is invalid`);
      }
    }
    if (!this.isRecord(payload.attributes)) {
      throw new Error("Synchronization source payload attributes are invalid");
    }
    return payload as unknown as SourcePayload;
  }

  private isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value);
  }

  private isTerminal(status: string): boolean {
    return ["completed", "completed_with_errors", "failed"].includes(status);
  }

  private delay(milliseconds: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
  }

  private errorMessage(error: unknown): string {
    return error instanceof Error ? error.stack ?? error.message : String(error);
  }
}
