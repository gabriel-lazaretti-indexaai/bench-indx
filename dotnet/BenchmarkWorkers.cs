using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
using StackExchange.Redis;

namespace BenchmarkService;

public sealed class BenchmarkWorkers(
    NpgsqlDataSource dataSource,
    IConnectionMultiplexer redis,
    BenchmarkOptions options,
    ILogger<BenchmarkWorkers> logger) : BackgroundService
{
    private static readonly TimeSpan PendingMessageIdleTime = TimeSpan.FromSeconds(30);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var database = redis.GetDatabase();
        try
        {
            await database.StreamCreateConsumerGroupAsync(
                BenchmarkOptions.StreamName,
                BenchmarkOptions.ConsumerGroup,
                "0-0",
                createStream: true);
        }
        catch (RedisServerException exception) when (exception.Message.Contains("BUSYGROUP", StringComparison.Ordinal))
        {
            // Another service instance has already created the shared consumer group.
        }

        var consumers = Enumerable.Range(0, options.WorkerConcurrency)
            .Select(index => ConsumeAsync(database, $"{Environment.MachineName}-{Guid.NewGuid():N}-{index}", stoppingToken));
        await Task.WhenAll(consumers);
    }

    private async Task ConsumeAsync(IDatabase database, string consumerName, CancellationToken stoppingToken)
    {
        var claimCursor = "0-0";
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var (claimed, nextClaimCursor) = await ClaimPendingAsync(database, consumerName, claimCursor);
                claimCursor = nextClaimCursor;
                if (claimed.Count > 0)
                {
                    foreach (var message in claimed)
                    {
                        await ProcessAndAcknowledgeAsync(database, message, stoppingToken);
                    }

                    continue;
                }

                var entries = await database.StreamReadGroupAsync(
                    BenchmarkOptions.StreamName,
                    BenchmarkOptions.ConsumerGroup,
                    consumerName,
                    StreamPosition.NewMessages,
                    count: 1);

                if (entries.Length == 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(100), stoppingToken);
                    continue;
                }

                foreach (var entry in entries)
                {
                    if (TryGetJobId(entry, out var jobId))
                    {
                        await ProcessAndAcknowledgeAsync(database, new StreamJobMessage(entry.Id.ToString(), jobId), stoppingToken);
                    }
                    else
                    {
                        logger.LogError("Ignoring malformed benchmark stream entry {MessageId} without a valid jobId.", entry.Id);
                        await database.StreamAcknowledgeAsync(BenchmarkOptions.StreamName, BenchmarkOptions.ConsumerGroup, entry.Id);
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Benchmark stream consumer {ConsumerName} failed; the message will be retried.", consumerName);
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            }
        }
    }

    private async Task<(List<StreamJobMessage> Messages, string NextCursor)> ClaimPendingAsync(
        IDatabase database,
        string consumerName,
        string cursor)
    {
        var result = await database.ExecuteAsync(
            "XAUTOCLAIM",
            BenchmarkOptions.StreamName,
            BenchmarkOptions.ConsumerGroup,
            consumerName,
            (long)PendingMessageIdleTime.TotalMilliseconds,
            cursor,
            "COUNT",
            options.PageSize);

        var response = (RedisResult[]?)result;
        if (response is null || response.Length < 2)
        {
            return ([], "0-0");
        }

        var nextCursor = (string?)response[0] ?? "0-0";
        var claimedEntries = (RedisResult[]?)response[1];
        if (claimedEntries is null)
        {
            return ([], nextCursor);
        }

        var messages = new List<StreamJobMessage>(claimedEntries.Length);
        foreach (var claimedEntry in claimedEntries)
        {
            var entry = (RedisResult[]?)claimedEntry;
            if (entry is null || entry.Length < 2)
            {
                continue;
            }

            var messageId = (string?)entry[0];
            var fields = (RedisResult[]?)entry[1];
            var jobIdText = FindField(fields, "jobId");
            if (messageId is not null && Guid.TryParse(jobIdText, out var jobId))
            {
                messages.Add(new StreamJobMessage(messageId, jobId));
            }
        }

        return (messages, nextCursor);
    }

    private static string? FindField(RedisResult[]? fields, string fieldName)
    {
        if (fields is null)
        {
            return null;
        }

        for (var index = 0; index + 1 < fields.Length; index += 2)
        {
            if (string.Equals((string?)fields[index], fieldName, StringComparison.Ordinal))
            {
                return (string?)fields[index + 1];
            }
        }

        return null;
    }

    private static bool TryGetJobId(StreamEntry entry, out Guid jobId)
    {
        foreach (var field in entry.Values)
        {
            if (field.Name == "jobId" && Guid.TryParse(field.Value.ToString(), out jobId))
            {
                return true;
            }
        }

        jobId = default;
        return false;
    }

    private async Task ProcessAndAcknowledgeAsync(
        IDatabase database,
        StreamJobMessage message,
        CancellationToken cancellationToken)
    {
        try
        {
            if (await ProcessJobAsync(message.JobId, cancellationToken))
            {
                await database.StreamAcknowledgeAsync(
                    BenchmarkOptions.StreamName,
                    BenchmarkOptions.ConsumerGroup,
                    message.MessageId);
            }
        }
        catch (Exception exception) when (exception is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            logger.LogError(exception, "Benchmark job {JobId} failed during processing; stream message {MessageId} remains pending.", message.JobId, message.MessageId);
        }
    }

    private async Task<bool> ProcessJobAsync(Guid jobId, CancellationToken cancellationToken)
    {
        await using var lockConnection = await dataSource.OpenConnectionAsync(cancellationToken);
        var lockAcquired = await TryAcquireJobLockAsync(lockConnection, jobId, cancellationToken);
        if (!lockAcquired)
        {
            return false;
        }

        try
        {
            var job = await LoadJobAsync(lockConnection, jobId, cancellationToken);
            if (job is null || IsTerminal(job.Value.Status))
            {
                return true;
            }

            await StartOrResumeJobAsync(lockConnection, jobId, cancellationToken);
            while (!cancellationToken.IsCancellationRequested)
            {
                var page = await LoadPendingPageAsync(jobId, cancellationToken);
                if (page.Count == 0)
                {
                    break;
                }

                if (job.Value.Workflow == "synchronization" && job.Value.LatencyMs > 0)
                {
                    await Task.Delay(job.Value.LatencyMs, cancellationToken);
                }

                await Parallel.ForEachAsync(
                    page,
                    new ParallelOptions
                    {
                        MaxDegreeOfParallelism = options.ItemConcurrency,
                        CancellationToken = cancellationToken
                    },
                    async (item, token) => await ProcessItemAsync(jobId, job.Value, item, token));
            }

            return await CompleteJobIfReadyAsync(lockConnection, jobId, cancellationToken);
        }
        finally
        {
            await ReleaseJobLockAsync(lockConnection, jobId, CancellationToken.None);
        }
    }

    private static async Task<bool> TryAcquireJobLockAsync(NpgsqlConnection connection, Guid jobId, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT pg_try_advisory_lock(hashtextextended(@jobId::text, 0))", connection);
        command.Parameters.AddWithValue("jobId", jobId);
        return (bool)(await command.ExecuteScalarAsync(cancellationToken) ?? false);
    }

    private static async Task ReleaseJobLockAsync(NpgsqlConnection connection, Guid jobId, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT pg_advisory_unlock(hashtextextended(@jobId::text, 0))", connection);
        command.Parameters.AddWithValue("jobId", jobId);
        await command.ExecuteScalarAsync(cancellationToken);
    }

    private static async Task<(string Workflow, int LatencyMs, int FailureEvery, string Status)?> LoadJobAsync(
        NpgsqlConnection connection,
        Guid jobId,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("""
            SELECT workflow, external_latency_ms, failure_every, status
            FROM benchmark_jobs
            WHERE id = @jobId
            """, connection);
        command.Parameters.AddWithValue("jobId", jobId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return (reader.GetString(0), reader.GetInt32(1), reader.GetInt32(2), reader.GetString(3));
    }

    private static bool IsTerminal(string status) => status is "completed" or "completed_with_errors" or "failed";

    private static async Task StartOrResumeJobAsync(NpgsqlConnection connection, Guid jobId, CancellationToken cancellationToken)
    {
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using (var startJob = new NpgsqlCommand("""
            UPDATE benchmark_jobs
            SET status = 'running', started_at = COALESCE(started_at, clock_timestamp())
            WHERE id = @jobId AND status IN ('queued', 'running')
            """, connection, transaction))
        {
            startJob.Parameters.AddWithValue("jobId", jobId);
            await startJob.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var resetInterruptedItems = new NpgsqlCommand("""
            UPDATE benchmark_job_items
            SET status = 'pending', updated_at = clock_timestamp()
            WHERE job_id = @jobId AND status = 'processing'
            """, connection, transaction))
        {
            resetInterruptedItems.Parameters.AddWithValue("jobId", jobId);
            await resetInterruptedItems.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    private async Task<List<PendingJobItem>> LoadPendingPageAsync(Guid jobId, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("""
            SELECT item_index, external_id, product_id, source_payload::text
            FROM benchmark_job_items
            WHERE job_id = @jobId AND status = 'pending'
            ORDER BY item_index
            LIMIT @pageSize
            """);
        command.Parameters.AddWithValue("jobId", jobId);
        command.Parameters.AddWithValue("pageSize", options.PageSize);

        var items = new List<PendingJobItem>(options.PageSize);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new PendingJobItem(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetInt64(2),
                reader.IsDBNull(3) ? null : reader.GetString(3)));
        }

        return items;
    }

    private async Task ProcessItemAsync(Guid jobId, (string Workflow, int LatencyMs, int FailureEvery, string Status) job, PendingJobItem item, CancellationToken cancellationToken)
    {
        if (!await TryMarkProcessingAsync(jobId, item.ItemIndex, cancellationToken))
        {
            return;
        }

        try
        {
            if (job.Workflow != "synchronization" && job.LatencyMs > 0)
            {
                await Task.Delay(job.LatencyMs, cancellationToken);
            }

            if (job.FailureEvery > 0 && item.ItemIndex % job.FailureEvery == 0)
            {
                throw new InjectedItemFailureException("Injected deterministic failure");
            }

            await PersistSuccessAsync(jobId, job.Workflow, item, cancellationToken);
        }
        catch (InjectedItemFailureException exception)
        {
            await PersistFailureAsync(jobId, item.ItemIndex, exception.Message, cancellationToken);
        }
    }

    private async Task<bool> TryMarkProcessingAsync(Guid jobId, int itemIndex, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("""
            UPDATE benchmark_job_items
            SET status = 'processing', updated_at = clock_timestamp()
            WHERE job_id = @jobId AND item_index = @itemIndex AND status = 'pending'
            RETURNING 1
            """);
        command.Parameters.AddWithValue("jobId", jobId);
        command.Parameters.AddWithValue("itemIndex", itemIndex);
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    private async Task PersistSuccessAsync(Guid jobId, string workflow, PendingJobItem item, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        long productId;
        string result;

        switch (workflow)
        {
            case "approval":
                productId = await UpdateApprovalAsync(connection, transaction, RequireProductId(item), cancellationToken);
                result = JsonSerializer.Serialize(new { status = "approved", externalId = item.ExternalId });
                break;
            case "enrichment":
                (productId, result) = await UpdateEnrichmentAsync(connection, transaction, item, cancellationToken);
                break;
            case "synchronization":
                (productId, result) = await UpsertSynchronizedProductAsync(connection, transaction, item, cancellationToken);
                break;
            default:
                throw new InvalidOperationException($"Unsupported benchmark workflow '{workflow}'.");
        }

        await using (var finishItem = new NpgsqlCommand("""
            UPDATE benchmark_job_items
            SET status = 'completed', product_id = @productId, result = @result, updated_at = clock_timestamp()
            WHERE job_id = @jobId AND item_index = @itemIndex AND status = 'processing'
            RETURNING 1
            """, connection, transaction))
        {
            finishItem.Parameters.AddWithValue("productId", productId);
            finishItem.Parameters.AddWithValue("result", NpgsqlDbType.Jsonb, result);
            finishItem.Parameters.AddWithValue("jobId", jobId);
            finishItem.Parameters.AddWithValue("itemIndex", item.ItemIndex);
            if (await finishItem.ExecuteScalarAsync(cancellationToken) is not null)
            {
                await IncrementJobProgressAsync(connection, transaction, jobId, completedIncrement: 1, failedIncrement: 0, cancellationToken);
            }
        }

        await transaction.CommitAsync(cancellationToken);
    }

    private static async Task<long> UpdateApprovalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, long productId, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("""
            UPDATE benchmark_products
            SET approval_status = 'approved', updated_at = clock_timestamp()
            WHERE id = @productId
            RETURNING id
            """, connection, transaction);
        command.Parameters.AddWithValue("productId", productId);
        return await command.ExecuteScalarAsync(cancellationToken) as long?
            ?? throw new InvalidOperationException($"Approval product '{productId}' does not exist.");
    }

    private static async Task<(long ProductId, string Result)> UpdateEnrichmentAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        PendingJobItem item,
        CancellationToken cancellationToken)
    {
        var index = item.ItemIndex;
        var targetProductId = RequireProductId(item);
        var title = $"Optimized Product {index}";
        var description = $"Enriched description for product {index}";
        var result = JsonSerializer.Serialize(new
        {
            title,
            slug = $"optimized-product-{index}",
            description,
            attributes = new
            {
                benchmark = true,
                material = index % 2 == 0 ? "cotton" : "linen",
                color = index % 3 == 0 ? "blue" : "black"
            }
        });

        await using var command = new NpgsqlCommand("""
            UPDATE benchmark_products
            SET name = @title,
                description = @description,
                attributes = @result,
                enrichment = @result,
                updated_at = clock_timestamp()
            WHERE id = @productId
            RETURNING id
            """, connection, transaction);
        command.Parameters.AddWithValue("title", title);
        command.Parameters.AddWithValue("description", description);
        command.Parameters.AddWithValue("result", NpgsqlDbType.Jsonb, result);
        command.Parameters.AddWithValue("productId", targetProductId);
        var productId = await command.ExecuteScalarAsync(cancellationToken);
        if (productId is not long id)
        {
            throw new InvalidOperationException($"Enrichment product '{targetProductId}' does not exist.");
        }

        return (id, result);
    }

    private static async Task<(long ProductId, string Result)> UpsertSynchronizedProductAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        PendingJobItem item,
        CancellationToken cancellationToken)
    {
        if (item.SourcePayload is null)
        {
            throw new InvalidOperationException($"Synchronization item {item.ItemIndex} has no source payload.");
        }

        await using var command = new NpgsqlCommand("""
            INSERT INTO benchmark_products (external_id, sku, name, description, category, attributes)
            SELECT btrim(payload->>'externalId'),
                   btrim(payload->>'sku'),
                   btrim(payload->>'title'),
                   btrim(payload->>'description'),
                   lower(btrim(payload->>'category')),
                   COALESCE(payload->'attributes', '{}'::jsonb)
            FROM (SELECT @sourcePayload::jsonb AS payload) AS source
            ON CONFLICT (external_id) DO UPDATE
            SET sku = EXCLUDED.sku,
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                category = EXCLUDED.category,
                attributes = EXCLUDED.attributes,
                updated_at = clock_timestamp()
            RETURNING id, jsonb_build_object(
                'externalId', external_id,
                'sku', sku,
                'name', name,
                'description', description,
                'category', category,
                'attributes', attributes
            )::text
            """, connection, transaction);
        command.Parameters.AddWithValue("sourcePayload", NpgsqlDbType.Jsonb, item.SourcePayload);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException($"Synchronization upsert returned no product for '{item.ExternalId}'.");
        }

        return (reader.GetInt64(0), reader.GetString(1));
    }

    private static long RequireProductId(PendingJobItem item) => item.ProductId
        ?? throw new InvalidOperationException($"Benchmark item {item.ItemIndex} has no product_id.");

    private async Task PersistFailureAsync(Guid jobId, int itemIndex, string errorMessage, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var failItem = new NpgsqlCommand("""
            UPDATE benchmark_job_items
            SET status = 'failed', error_message = @errorMessage, updated_at = clock_timestamp()
            WHERE job_id = @jobId AND item_index = @itemIndex AND status = 'processing'
            RETURNING 1
            """, connection, transaction);
        failItem.Parameters.AddWithValue("errorMessage", errorMessage);
        failItem.Parameters.AddWithValue("jobId", jobId);
        failItem.Parameters.AddWithValue("itemIndex", itemIndex);
        if (await failItem.ExecuteScalarAsync(cancellationToken) is not null)
        {
            await IncrementJobProgressAsync(connection, transaction, jobId, completedIncrement: 0, failedIncrement: 1, cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    private static async Task IncrementJobProgressAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        Guid jobId,
        int completedIncrement,
        int failedIncrement,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("""
            UPDATE benchmark_jobs
            SET completed_items = completed_items + @completedIncrement,
                failed_items = failed_items + @failedIncrement
            WHERE id = @jobId
            """, connection, transaction);
        command.Parameters.AddWithValue("completedIncrement", completedIncrement);
        command.Parameters.AddWithValue("failedIncrement", failedIncrement);
        command.Parameters.AddWithValue("jobId", jobId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<bool> CompleteJobIfReadyAsync(NpgsqlConnection connection, Guid jobId, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("""
            WITH item_counts AS (
                SELECT count(*) FILTER (WHERE status = 'completed')::integer AS completed_count,
                       count(*) FILTER (WHERE status = 'failed')::integer AS failed_count,
                       count(*) FILTER (WHERE status IN ('pending', 'processing'))::integer AS remaining_count
                FROM benchmark_job_items
                WHERE job_id = @jobId
            )
            UPDATE benchmark_jobs AS job
            SET completed_items = item_counts.completed_count,
                failed_items = item_counts.failed_count,
                status = CASE WHEN item_counts.failed_count > 0 THEN 'completed_with_errors' ELSE 'completed' END,
                completed_at = clock_timestamp()
            FROM item_counts
            WHERE job.id = @jobId AND item_counts.remaining_count = 0 AND job.status = 'running'
            RETURNING job.status
            """, connection);
        command.Parameters.AddWithValue("jobId", jobId);
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    private sealed class InjectedItemFailureException(string message) : Exception(message);
}
