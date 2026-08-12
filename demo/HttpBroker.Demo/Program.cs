using System.Diagnostics;
using HttpBroker.Client;
using HttpBroker.Server;

namespace HttpBroker.Demo;

/// <summary>Demo CLI. Commands:
///   serve    --urls http://host:port --data ./data        run the broker
///   produce  --url ... --topic demo --count 1000          push text messages
/// </summary>
public static class Program
{
    private static readonly Dictionary<string, string> Opts = new(StringComparer.Ordinal);

    public static async Task<int> Main(string[] args)
    {
        if (args.Length == 0) { PrintUsage(); return 1; }

        var command = args[0];
        ParseOpts(args.Skip(1));

        try
        {
            return command switch
            {
                "serve" => await ServeAsync(),
                "produce" => await ProduceAsync(),
                _ => PrintUsage(),
            };
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"error: {ex.Message}");
            return 1;
        }
    }

    private static void ParseOpts(IEnumerable<string> args)
    {
        Opts.Clear();
        var list = args.ToList();
        for (int i = 0; i < list.Count; i++)
        {
            if (list[i].StartsWith("--"))
            {
                var name = list[i][2..];
                var value = "";
                if (i + 1 < list.Count && !list[i + 1].StartsWith("--")) value = list[++i];
                Opts[name] = value;
            }
        }
    }

    private static string Opt(string name, string def = "")
        => Opts.TryGetValue(name, out var v) && v.Length > 0 ? v : def;

    private static int OptInt(string name, int def)
        => int.TryParse(Opt(name), out var v) ? v : def;

    private static long OptLong(string name, long def)
        => long.TryParse(Opt(name), out var v) ? v : def;

    // ---------- serve ----------

    private static async Task<int> ServeAsync()
    {
        var args = new[] { "--urls", Opt("urls", $"http://127.0.0.1:{HttpBroker.Core.Engine.BrokerEngine.DefaultPort}"), "--data", Opt("data", "data") };
        Console.WriteLine($"[broker] http-native message broker starting on {args[1]} (data: {args[3]})");
        await BrokerHost.Build(args).RunAsync();
        return 0;
    }

    // ---------- produce ----------

    private static async Task<int> ProduceAsync()
    {
        using var client = new BrokerClient(Opt("url", "http://127.0.0.1:8123"));
        var topic = Opt("topic", "demo");
        var count = OptLong("count", 1000);
        var size = OptLong("size", 64);
        var payload = new string('x', (int)size);

        var sw = Stopwatch.StartNew();
        for (long i = 0; i < count; i++)
            await client.ProduceAsync(topic, [$"{i:D8}: {payload}"]);
        sw.Stop();

        Console.WriteLine($"produced {count} messages to '{topic}' in {sw.Elapsed.TotalSeconds:F2}s ({count / sw.Elapsed.TotalSeconds:N0} msg/s)");
        return 0;
    }

    private static int PrintUsage()
    {
        Console.WriteLine("""
            HttpBroker demo
              serve    --urls http://127.0.0.1:8123 --data ./data
              produce  --url http://127.0.0.1:8123 --topic demo --count 1000 [--size 64]
            """);
        return 1;
    }
}
