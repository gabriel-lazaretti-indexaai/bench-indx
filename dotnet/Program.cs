using BenchmarkService;
using Npgsql;
using StackExchange.Redis;
using System.Runtime.InteropServices;

var builder = WebApplication.CreateBuilder(args);

var options = BenchmarkOptions.FromEnvironment();
builder.WebHost.UseUrls($"http://0.0.0.0:{options.Port}");
builder.Services.AddSingleton(options);
builder.Services.AddSingleton(_ =>
{
    var connectionString = BenchmarkOptions.ToPostgresConnectionString(options.DatabaseUrl, options.DatabasePoolMax);
    return NpgsqlDataSource.Create(connectionString);
});
builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
    ConnectionMultiplexer.Connect(BenchmarkOptions.ToRedisConfiguration(options.RedisUrl)));
builder.Services.AddSingleton<BenchmarkJobs>();
builder.Services.AddHostedService<OutboxDispatcher>();
builder.Services.AddHostedService<BenchmarkWorkers>();

var app = builder.Build();

app.MapGet("/health", () => Results.Ok(new
{
    status = "ok",
    runtime = ".NET",
    runtimeVersion = Environment.Version.ToString(),
    framework = RuntimeInformation.FrameworkDescription
}));
app.MapPost("/api/benchmark/jobs", async (CreateJobRequest request, BenchmarkJobs jobs, CancellationToken cancellationToken) =>
{
    var validationError = request.Validate();
    if (validationError is not null)
    {
        return Results.ValidationProblem(new Dictionary<string, string[]> { ["request"] = [validationError] });
    }

    var jobId = await jobs.CreateAsync(request, cancellationToken);
    return Results.Accepted($"/api/benchmark/jobs/{jobId}", new { jobId, status = "queued" });
});
app.MapGet("/api/benchmark/jobs/{jobId:guid}", async (Guid jobId, BenchmarkJobs jobs, CancellationToken cancellationToken) =>
{
    var job = await jobs.GetAsync(jobId, cancellationToken);
    return job is null ? Results.NotFound() : Results.Ok(job);
});

app.Run();
