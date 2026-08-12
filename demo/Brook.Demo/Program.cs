using System.Collections.Concurrent;
using System.Diagnostics;
using Brook.Client;
using Brook.Server;

namespace Brook.Demo;

/// <summary>Demo CLI. Commands:
///   serve    --urls http://host:port --data ./data        run the broker
///   produce  --url ... --topic demo --count 1000          push text messages
///   consume  --url ... --topic demo --group g1            print + commit, at-least-once
///   bench    --url ... --topic bench --messages 100k --size 128 --batch 100 --workers 8
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
                "consume" => await ConsumeAsync(),
                "bench" => await BenchAsync(),
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
        var args = new[] { "--urls", Opt("urls", $"http://127.0.0.1:{Brook.Core.Engine.BrokerEngine.DefaultPort}"), "--data", Opt("data", "data") };
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

    // ---------- consume ----------

    private static async Task<int> ConsumeAsync()
    {
        using var client = new BrokerClient(Opt("url", "http://127.0.0.1:8123"));
        var topic = Opt("topic", "demo");
        var group = Opt("group", "g1");
        var commitEvery = OptInt("commit-every", 100);
        var print = Opt("print", "true") != "false";

        var committed = await client.GetCommittedOffsetAsync(group, topic);
        Console.WriteLine($"[consumer] group '{group}' resuming from offset {committed} on '{topic}'. Ctrl+C to stop.");

        long last = committed;
        long seen = 0;
        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        while (!cts.IsCancellationRequested)
        {
            try
            {
                await using var stream = await client.OpenStreamAsync(topic, group, last, cts.Token);
                while (true)
                {
                    var msg = await stream.NextAsync(cts.Token);
                    if (msg is null) break; // broker closed the stream gracefully
                    last = msg.Offset + 1;
                    seen++;
                    if (print && seen <= 20)
                        Console.WriteLine($"  [{msg.Offset}] {msg.Payload}");
                    if (commitEvery > 0 && seen % commitEvery == 0)
                        await client.CommitOffsetAsync(group, topic, last);
                }
            }
            catch (ResetException)
            {
                // Slow-consumer reset: replay from committed offset. At-least-once in action.
                committed = await client.GetCommittedOffsetAsync(group, topic);
                last = committed;
                Console.WriteLine($"  [!] stream reset (channel overflow) - replaying from {committed}");
            }
            catch (OperationCanceledException) { break; }
        }

        await client.CommitOffsetAsync(group, topic, last);
        Console.WriteLine($"[consumer] done. saw {seen} messages, committed offset {last}");
        return 0;
    }

    // ---------- bench ----------

    private static async Task<int> BenchAsync()
    {
        using var client = new BrokerClient(Opt("url", "http://127.0.0.1:8123"));
        var topic = Opt("topic", "bench");
        var total = OptLong("messages", 100_000);
        var size = OptInt("size", 128);
        var batch = OptInt("batch", 100);
        var workers = OptInt("workers", 8);
        var payload = new string('x', size);

        Console.WriteLine($"[bench] {total:N0} messages x {size} B, batch {batch}, workers {workers} on '{topic}'");
        var sw = Stopwatch.StartNew();
        var latencies = new ConcurrentBag<double>();
        var remaining = total;

        await Task.WhenAll(Enumerable.Range(0, workers).Select(async _ =>
        {
            while (true)
            {
                var n = (int)Math.Min(batch, Interlocked.Read(ref remaining));
                if (n <= 0) break;
                Interlocked.Add(ref remaining, -n);

                var batchMsgs = Enumerable.Range(0, n).Select(i => payload).ToArray();
                var t = Stopwatch.StartNew();
                await client.ProduceAsync(topic, batchMsgs);
                t.Stop();
                latencies.Add(t.Elapsed.TotalMicroseconds / n);
            }
        }));
        sw.Stop();

        var sorted = latencies.OrderBy(x => x).ToArray();
        Console.WriteLine($"[bench] {total:N0} msgs in {sw.Elapsed.TotalSeconds:F2}s = {total / sw.Elapsed.TotalSeconds:N0} msg/s");
        Console.WriteLine($"[bench] batch latency p50 {Pct(sorted, 0.50):F0} µs · p99 {Pct(sorted, 0.99):F0} µs");
        return 0;
    }

    private static double Pct(double[] sorted, double q)
        => sorted.Length == 0 ? 0 : sorted[(int)((sorted.Length - 1) * q)];

    private static int PrintUsage()
    {
        Console.WriteLine("""
            Brook demo
              serve    --urls http://127.0.0.1:8123 --data ./data
              produce  --url http://127.0.0.1:8123 --topic demo --count 1000 [--size 64]
              consume  --url http://127.0.0.1:8123 --topic demo --group g1 [--commit-every 100]
              bench    --url http://127.0.0.1:8123 --topic bench --messages 100000 [--size 128] [--batch 100] [--workers 8]
            """);
        return 1;
    }
}
