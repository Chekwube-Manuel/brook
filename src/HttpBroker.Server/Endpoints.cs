using System.Text;
using System.Text.Json;
using HttpBroker.Core.Engine;
using HttpBroker.Core.Log;
using HttpBroker.Core.Model;

namespace HttpBroker.Server;

/// <summary>
/// HTTP endpoints. Wire format is NDJSON for the stream and JSON for everything else.
///   POST /v1/topics/{topic}/messages              produce (single object or array)
///   GET  /v1/topics/{topic}/stream?group=&offset=  consume (HTTP/2 server-stream)
///   PUT  /v1/groups/{group}/topics/{topic}/offset  commit offset
///   GET  /v1/groups/{group}/topics/{topic}/offset  read committed offset
///   GET  /healthz
/// </summary>
public static class Endpoints
{
    public static void Map(WebApplication app)
    {
        app.MapGet("/healthz", () => Results.Text("ok"));
        app.MapPost("/v1/topics/{topic}/messages", Produce);
        app.MapGet("/v1/topics/{topic}/stream", Stream);
        app.MapPut("/v1/groups/{group}/topics/{topic}/offset", CommitOffset);
        app.MapGet("/v1/groups/{group}/topics/{topic}/offset", ReadOffset);
    }

    // ---------- produce ----------

    private static async Task Produce(HttpContext ctx, string topic, BrokerEngine engine)
    {
        try
        {
            using var doc = await JsonDocument.ParseAsync(ctx.Request.Body);
            var root = doc.RootElement;
            var batch = new List<byte[]>(8);

            void Add(JsonElement item)
            {
                var payload = item.ValueKind == JsonValueKind.String ? item.GetString()
                    : item.TryGetProperty("payload", out var p) ? p.GetString()
                    : null;
                if (payload is null)
                    throw new ArgumentException("Each message needs a string 'payload'.");
                batch.Add(Encoding.UTF8.GetBytes(payload));
            }

            if (root.ValueKind == JsonValueKind.Array)
                foreach (var item in root.EnumerateArray()) Add(item);
            else if (root.ValueKind is JsonValueKind.Object or JsonValueKind.String)
                Add(root);
            else
                throw new ArgumentException("Body must be a message object or an array of them.");

            var sw = System.Diagnostics.Stopwatch.StartNew();
            var (first, last) = engine.Produce(topic, batch);
            sw.Stop();

            await Results.Json(new
            {
                topic,
                firstOffset = first,
                lastOffset = last,
                count = last - first + 1,
                latencyUs = sw.Elapsed.TotalMicroseconds,
                durability = engine.GetTopicConfig(topic)?.Durability.ToString(),
            }).ExecuteAsync(ctx);
        }
        catch (ArgumentException ex)
        {
            await Error(ctx, 400, ex.Message);
        }
        catch (Exception ex)
        {
            await Error(ctx, 500, ex.Message);
        }
    }

    // ---------- consume (HTTP/2 server stream, NDJSON) ----------

    private static async Task Stream(HttpContext ctx, string topic, BrokerEngine engine,
        string? group = null, long? offset = null)
    {
        try
        {
            var oldest = engine.OldestOffset(topic);
            var requested = offset ?? (group is not null ? engine.GetCommittedOffset(group, topic) : 0);
            if (requested < oldest)
            {
                ctx.Response.Headers["X-Oldest-Offset"] = oldest.ToString();
                await Error(ctx, 410, $"Offset {requested} expired; rewind to {oldest}.");
                return;
            }

            var sub = engine.Subscribe(topic);
            try
            {
                ctx.Response.StatusCode = 200;
                ctx.Response.ContentType = "application/x-ndjson";
                await ctx.Response.StartAsync(ctx.RequestAborted);

                // Phase 1: replay the gap [requested, EndOffset) straight from the log.
                await foreach (var m in engine.ReadReplayAsync(topic, requested, sub.EndOffset, ctx.RequestAborted))
                    await WriteLineAsync(ctx, m);

                // Phase 2: drain the fan-out channel (anything appended after subscribe).
                while (await sub.Channel.WaitToReadAsync(ctx.RequestAborted))
                {
                    if (sub.Overflowed)
                    {
                        // Slow consumer: channel filled, we refuse to drop data silently.
                        // Hard reset — client replays from its last committed offset.
                        ctx.Abort();
                        return;
                    }
                    while (sub.Channel.TryRead(out var m))
                        await WriteLineAsync(ctx, m);
                }
            }
            finally
            {
                engine.Unsubscribe(topic, sub);
            }
        }
        catch (OperationCanceledException) { /* client went away */ }
        catch (Exception ex)
        {
            if (!ctx.Response.HasStarted) await Error(ctx, 500, ex.Message);
            else ctx.Abort();
        }
    }

    private static async Task WriteLineAsync(HttpContext ctx, BrokerMessage m)
    {
        var line = JsonSerializer.Serialize(new
        {
            offset = m.Offset,
            timestamp = m.TimestampMs,
            payload = m.PayloadText,
        });
        var bytes = Encoding.UTF8.GetBytes(line + "\n");
        await ctx.Response.Body.WriteAsync(bytes, ctx.RequestAborted);
        await ctx.Response.Body.FlushAsync(ctx.RequestAborted);
    }

    // ---------- consumer groups ----------

    private static async Task CommitOffset(HttpContext ctx, string group, string topic, BrokerEngine engine)
    {
        try
        {
            BrokerEngine.ValidateName(group, "group");
            using var doc = await JsonDocument.ParseAsync(ctx.Request.Body);
            if (!doc.RootElement.TryGetProperty("offset", out var p) || !p.TryGetInt64(out var requested))
                throw new ArgumentException("Body needs an integer 'offset' (the next offset to consume).");
            if (requested < 0) throw new ArgumentException("Offset must be >= 0.");

            await engine.CommitOffsetAsync(group, topic, requested);
            await Results.Json(new { group, topic, committed = requested }).ExecuteAsync(ctx);
        }
        catch (ArgumentException ex) { await Error(ctx, 400, ex.Message); }
        catch (Exception ex) { await Error(ctx, 500, ex.Message); }
    }

    private static async Task ReadOffset(HttpContext ctx, string group, string topic, BrokerEngine engine)
    {
        try
        {
            BrokerEngine.ValidateName(group, "group");
            await Results.Json(new { group, topic, offset = engine.GetCommittedOffset(group, topic) }).ExecuteAsync(ctx);
        }
        catch (ArgumentException ex) { await Error(ctx, 400, ex.Message); }
        catch (Exception ex) { await Error(ctx, 500, ex.Message); }
    }

    private static async Task Error(HttpContext ctx, int status, string message)
    {
        ctx.Response.StatusCode = status;
        ctx.Response.ContentType = "application/json";
        await ctx.Response.WriteAsJsonAsync(new { error = message });
    }
}
