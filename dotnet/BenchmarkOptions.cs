using Npgsql;
using StackExchange.Redis;

namespace BenchmarkService;

public sealed record BenchmarkOptions(
    int Port,
    string DatabaseUrl,
    string RedisUrl,
    int WorkerConcurrency,
    int ItemConcurrency,
    int PageSize,
    int DatabasePoolMax)
{
    public const string StreamName = "benchmark:jobs";
    public const string ConsumerGroup = "benchmark-workers";

    public static BenchmarkOptions FromEnvironment() => new(
        ReadInt("PORT", 3000, 1, 65535),
        Environment.GetEnvironmentVariable("DATABASE_URL") ?? "postgres://benchmark:benchmark@localhost:5432/benchmark",
        Environment.GetEnvironmentVariable("REDIS_URL") ?? "redis://localhost:6379",
        ReadInt("WORKER_CONCURRENCY", 8, 1, 128),
        ReadInt("ITEM_CONCURRENCY", 8, 1, 128),
        ReadInt("PAGE_SIZE", 100, 1, 1000),
        ReadInt("DB_POOL_MAX", 20, 1, 200));

    public static string ToPostgresConnectionString(string value, int poolMax)
    {
        if (Uri.TryCreate(value, UriKind.Absolute, out var uri) &&
            (uri.Scheme == "postgres" || uri.Scheme == "postgresql"))
        {
            var builder = new NpgsqlConnectionStringBuilder
            {
                Host = uri.Host,
                Port = uri.IsDefaultPort ? 5432 : uri.Port,
                Username = Uri.UnescapeDataString(uri.UserInfo.Split(':', 2)[0]),
                Password = uri.UserInfo.Contains(':')
                    ? Uri.UnescapeDataString(uri.UserInfo.Split(':', 2)[1])
                    : string.Empty,
                Database = Uri.UnescapeDataString(uri.AbsolutePath.TrimStart('/')),
                MaxPoolSize = poolMax,
                Pooling = true
            };
            return builder.ConnectionString;
        }

        var connectionString = new NpgsqlConnectionStringBuilder(value)
        {
            MaxPoolSize = poolMax,
            Pooling = true
        };
        return connectionString.ConnectionString;
    }

    public static ConfigurationOptions ToRedisConfiguration(string value)
    {
        if (!Uri.TryCreate(value, UriKind.Absolute, out var uri) ||
            (uri.Scheme != "redis" && uri.Scheme != "rediss"))
        {
            return ConfigurationOptions.Parse(value);
        }

        var configuration = new ConfigurationOptions
        {
            AbortOnConnectFail = false,
            Ssl = uri.Scheme == "rediss"
        };
        configuration.EndPoints.Add(uri.Host, uri.IsDefaultPort ? 6379 : uri.Port);
        if (!string.IsNullOrEmpty(uri.UserInfo))
        {
            var userInfo = uri.UserInfo.Split(':', 2);
            if (userInfo.Length == 2)
            {
                configuration.User = Uri.UnescapeDataString(userInfo[0]);
                configuration.Password = Uri.UnescapeDataString(userInfo[1]);
            }
            else
            {
                configuration.Password = Uri.UnescapeDataString(userInfo[0]);
            }
        }

        var databasePath = uri.AbsolutePath.Trim('/');
        if (int.TryParse(databasePath, out var database))
        {
            configuration.DefaultDatabase = database;
        }

        return configuration;
    }

    private static int ReadInt(string name, int fallback, int minimum, int maximum)
    {
        var raw = Environment.GetEnvironmentVariable(name);
        if (raw is null)
        {
            return fallback;
        }

        if (!int.TryParse(raw, out var value) || value < minimum || value > maximum)
        {
            throw new InvalidOperationException($"{name} must be an integer between {minimum} and {maximum}.");
        }

        return value;
    }
}
