using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace BenchmarkService;

public sealed class BenchmarkJobs(NpgsqlDataSource dataSource)
{
    public async Task<Guid> CreateAsync(CreateJobRequest request, CancellationToken cancellationToken)
    {
        var jobId = Guid.NewGuid();
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (var createJob = new NpgsqlCommand("""
            INSERT INTO benchmark_jobs (id, workflow, total_items, external_latency_ms, failure_every)
            VALUES (@jobId, @workflow, @itemCount, @latency, @failureEvery)
            """, connection, transaction))
        {
            createJob.Parameters.AddWithValue("jobId", jobId);
            createJob.Parameters.AddWithValue("workflow", request.Workflow);
            createJob.Parameters.AddWithValue("itemCount", request.ItemCount);
            createJob.Parameters.AddWithValue("latency", request.ExternalLatencyMs);
            createJob.Parameters.AddWithValue("failureEvery", request.FailureEvery);
            await createJob.ExecuteNonQueryAsync(cancellationToken);
        }

        if (request.Workflow is "approval" or "enrichment")
        {
            await InsertSyntheticProductsAsync(connection, transaction, jobId, request.ItemCount, cancellationToken);
            await InsertProductItemsAsync(connection, transaction, jobId, request.ItemCount, cancellationToken);
        }
        else
        {
            await InsertSynchronizationItemsAsync(connection, transaction, jobId, request.ItemCount, cancellationToken);
        }

        var payload = JsonSerializer.Serialize(new { jobId, workflow = request.Workflow });
        await using (var createOutbox = new NpgsqlCommand("""
            INSERT INTO benchmark_outbox (job_id, event_type, payload)
            VALUES (@jobId, 'benchmark.job.created', @payload)
            """, connection, transaction))
        {
            createOutbox.Parameters.AddWithValue("jobId", jobId);
            createOutbox.Parameters.AddWithValue("payload", NpgsqlDbType.Jsonb, payload);
            await createOutbox.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return jobId;
    }

    public async Task<BenchmarkJobResponse?> GetAsync(Guid jobId, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("""
            SELECT id, workflow, status, total_items, completed_items, failed_items,
                   created_at, started_at, completed_at,
                   CASE WHEN started_at IS NULL THEN NULL
                        ELSE (EXTRACT(EPOCH FROM (COALESCE(completed_at, clock_timestamp()) - started_at)) * 1000)::bigint
                   END AS duration_ms
            FROM benchmark_jobs
            WHERE id = @jobId
            """);
        command.Parameters.AddWithValue("jobId", jobId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new BenchmarkJobResponse(
            reader.GetGuid(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetInt32(3),
            reader.GetInt32(4),
            reader.GetInt32(5),
            reader.GetFieldValue<DateTimeOffset>(6),
            reader.IsDBNull(7) ? null : reader.GetFieldValue<DateTimeOffset>(7),
            reader.IsDBNull(8) ? null : reader.GetFieldValue<DateTimeOffset>(8),
            reader.IsDBNull(9) ? null : reader.GetInt64(9));
    }

    private static async Task InsertSyntheticProductsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        Guid jobId,
        int itemCount,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("""
            INSERT INTO benchmark_products (external_id, sku, name, description, category, attributes)
            SELECT @jobId::text || ':' || item_index::text,
                   'SKU-' || item_index::text,
                   'Product ' || item_index::text,
                   'Synthetic description for product ' || item_index::text,
                   'category-' || (((item_index - 1) % 10) + 1)::text,
                   '{}'::jsonb
            FROM generate_series(1, @itemCount) AS item_index
            """, connection, transaction);
        command.Parameters.AddWithValue("jobId", jobId);
        command.Parameters.AddWithValue("itemCount", itemCount);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task InsertProductItemsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        Guid jobId,
        int itemCount,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("""
            INSERT INTO benchmark_job_items (job_id, item_index, external_id, product_id)
            SELECT @jobId,
                   item_index,
                   @jobId::text || ':' || item_index::text,
                   product.id
            FROM generate_series(1, @itemCount) AS item_index
            JOIN benchmark_products AS product
              ON product.external_id = @jobId::text || ':' || item_index::text
            """, connection, transaction);
        command.Parameters.AddWithValue("jobId", jobId);
        command.Parameters.AddWithValue("itemCount", itemCount);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task InsertSynchronizationItemsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        Guid jobId,
        int itemCount,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("""
            INSERT INTO benchmark_job_items (job_id, item_index, external_id, source_payload)
            SELECT @jobId,
                   item_index,
                   @jobId::text || ':' || item_index::text,
                   jsonb_build_object(
                       'externalId', @jobId::text || ':' || item_index::text,
                       'sku', 'SKU-' || item_index::text,
                       'title', 'Imported Product ' || item_index::text,
                       'description', 'Imported description for product ' || item_index::text,
                       'category', 'category-' || (((item_index - 1) % 10) + 1)::text,
                       'attributes', jsonb_build_object(
                           'source', 'fixture',
                           'page', ceil(item_index::numeric / 100)::integer
                       )
                   )
            FROM generate_series(1, @itemCount) AS item_index
            """, connection, transaction);
        command.Parameters.AddWithValue("jobId", jobId);
        command.Parameters.AddWithValue("itemCount", itemCount);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
