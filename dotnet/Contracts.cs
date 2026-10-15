namespace BenchmarkService;

public sealed record CreateJobRequest(string Workflow, int ItemCount, int ExternalLatencyMs = 0, int FailureEvery = 0)
{
    public string? Validate()
    {
        if (Workflow is not ("approval" or "enrichment" or "synchronization"))
        {
            return "workflow must be approval, enrichment, or synchronization.";
        }

        if (ItemCount is < 1 or > 5000)
        {
            return "itemCount must be between 1 and 5000.";
        }

        if (ExternalLatencyMs is < 0 or > 200)
        {
            return "externalLatencyMs must be between 0 and 200.";
        }

        return FailureEvery < 0 ? "failureEvery must be zero or a positive integer." : null;
    }
}

public sealed record BenchmarkJobResponse(
    Guid JobId,
    string Workflow,
    string Status,
    int TotalItems,
    int CompletedItems,
    int FailedItems,
    DateTimeOffset CreatedAt,
    DateTimeOffset? StartedAt,
    DateTimeOffset? CompletedAt,
    long? DurationMs);

public sealed record PendingJobItem(int ItemIndex, string ExternalId, long? ProductId, string? SourcePayload);

public sealed record StreamJobMessage(string MessageId, Guid JobId);
