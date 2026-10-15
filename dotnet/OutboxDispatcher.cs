using Npgsql;
using StackExchange.Redis;

namespace BenchmarkService;

public sealed class OutboxDispatcher(
    NpgsqlDataSource dataSource,
    IConnectionMultiplexer redis,
    BenchmarkOptions options,
    ILogger<OutboxDispatcher> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var database = redis.GetDatabase();
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var dispatched = await DispatchBatchAsync(database, stoppingToken);
                if (dispatched == 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(100), stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Failed to dispatch pending benchmark outbox messages.");
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            }
        }
    }

    private async Task<int> DispatchBatchAsync(IDatabase redisDatabase, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var pending = new List<(long OutboxId, Guid JobId)>();

        await using (var select = new NpgsqlCommand("""
            SELECT id, job_id
            FROM benchmark_outbox
            WHERE dispatched_at IS NULL
            ORDER BY id
            LIMIT @pageSize
            FOR UPDATE SKIP LOCKED
            """, connection, transaction))
        {
            select.Parameters.AddWithValue("pageSize", options.PageSize);
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                pending.Add((reader.GetInt64(0), reader.GetGuid(1)));
            }
        }

        foreach (var row in pending)
        {
            await redisDatabase.StreamAddAsync(
                BenchmarkOptions.StreamName,
                [new NameValueEntry("jobId", row.JobId.ToString()), new NameValueEntry("outboxId", row.OutboxId)]);
        }

        if (pending.Count > 0)
        {
            await using var markDispatched = new NpgsqlCommand("""
                UPDATE benchmark_outbox
                SET dispatched_at = clock_timestamp()
                WHERE id = ANY(@outboxIds)
                """, connection, transaction);
            markDispatched.Parameters.AddWithValue("outboxIds", pending.Select(row => row.OutboxId).ToArray());
            await markDispatched.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return pending.Count;
    }
}
