using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Brook.Core.Codec;

namespace Brook.Client;

/// <summary>
/// Thin client over the broker's HTTP API. Supports content negotiation:
///   - JSON (default): human-readable, works everywhere
///   - Binary (opt-in): length-prefixed records, ~2x faster on small payloads
/// 
/// Set useBinary=true in the constructor or on individual Produce/Stream calls
/// to negotiate binary encoding with the broker.
/// </summary>
public sealed class BrokerClient : IDisposable
{
    internal readonly HttpClient Http;
    private readonly bool _ownsHttp;
    private readonly bool _preferBinary;

    public Uri BaseAddress => Http.BaseAddress!;

    /// <summary>Create a client. If useBinary=true, prefer binary encoding for produce/consume.</summary>
    public BrokerClient(string baseUrl, HttpClient? http = null, bool useBinary = false)
    {
        _ownsHttp = http is null;
        _preferBinary = useBinary;
        Http = http ?? new HttpClient();
        Http.BaseAddress = new Uri(baseUrl.EndsWith('/') ? baseUrl : baseUrl + "/");
        Http.DefaultRequestVersion = System.Net.HttpVersion.Version20;
        Http.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrHigher;
        
        // Set default Accept based on preference
        Http.DefaultRequestHeaders.Accept.Clear();
        if (useBinary)
            Http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/octet-stream"));
        Http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        
        Http.Timeout = Timeout.InfiniteTimeSpan; // streams stay open
    }

    public void Dispose()
    {
        if (_ownsHttp) Http.Dispose();
    }

    // ---------- Produce ----------

    /// <summary>Produce text messages (JSON encoding).</summary>
    public Task<ProduceResult> ProduceAsync(string topic, IEnumerable<string> messages, CancellationToken ct = default)
    {
        var payloads = messages.Select(m => new { payload = m }).ToArray();
        var content = new StringContent(JsonSerializer.Serialize(payloads), Encoding.UTF8, "application/json");
        return ProduceCoreAsync(topic, content, ct);
    }

    /// <summary>Produce binary messages (JSON encoding, base64-wrapped).</summary>
    public Task<ProduceResult> ProduceAsync(string topic, IReadOnlyList<byte[]> messages, CancellationToken ct = default)
    {
        var payloads = messages.Select(m => new { payload = Convert.ToBase64String(m) }).ToArray();
        var content = new StringContent(JsonSerializer.Serialize(payloads), Encoding.UTF8, "application/json");
        return ProduceCoreAsync(topic, content, ct);
    }

    /// <summary>Produce with binary encoding (length-prefixed records).</summary>
    public Task<ProduceResult> ProduceBinaryAsync(string topic, IEnumerable<byte[]> messages, CancellationToken ct = default)
    {
        var ms = new MemoryStream();
        BinaryCodec.EncodeBatchTo(ms, messages);
        ms.Position = 0;
        var content = new StreamContent(ms);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        return ProduceCoreAsync(topic, content, ct);
    }

    private async Task<ProduceResult> ProduceCoreAsync(string topic, HttpContent content, CancellationToken ct)
    {
        using var resp = await Http.PostAsync($"/v1/topics/{Uri.EscapeDataString(topic)}/messages", content, ct);
        await ThrowIfErrorAsync(resp, ct);
        return JsonSerializer.Deserialize<ProduceResult>(await resp.Content.ReadAsStringAsync(ct), Json.Options)!;
    }

    // ---------- Consume ----------

    /// <summary>Open a consume stream (NDJSON by default, or binary if client prefers).</summary>
    public Task<ConsumerStream> OpenStreamAsync(string topic, string? group = null, long? offset = null, CancellationToken ct = default)
    {
        var query = new List<string>();
        if (group is not null) query.Add($"group={Uri.EscapeDataString(group)}");
        if (offset is not null) query.Add($"offset={offset}");
        var qs = query.Count > 0 ? "?" + string.Join("&", query) : "";
        return ConsumerStream.OpenAsync(Http, $"/v1/topics/{Uri.EscapeDataString(topic)}/stream{qs}", useBinary: _preferBinary, ct);
    }

    /// <summary>Open a consume stream explicitly requesting binary encoding.</summary>
    public Task<ConsumerStream> OpenStreamBinaryAsync(string topic, string? group = null, long? offset = null, CancellationToken ct = default)
    {
        var query = new List<string>();
        if (group is not null) query.Add($"group={Uri.EscapeDataString(group)}");
        if (offset is not null) query.Add($"offset={offset}");
        var qs = query.Count > 0 ? "?" + string.Join("&", query) : "";
        return ConsumerStream.OpenAsync(Http, $"/v1/topics/{Uri.EscapeDataString(topic)}/stream{qs}", useBinary: true, ct);
    }

    /// <summary>Commit the next offset a group should consume. At-least-once lives here:
    /// commit AFTER you have durably (idempotently) processed the message.</summary>
    public async Task CommitOffsetAsync(string group, string topic, long nextOffset, CancellationToken ct = default)
    {
        var body = new StringContent(JsonSerializer.Serialize(new { offset = nextOffset }), Encoding.UTF8, "application/json");
        using var resp = await Http.PutAsync(
            $"/v1/groups/{Uri.EscapeDataString(group)}/topics/{Uri.EscapeDataString(topic)}/offset", body, ct);
        await ThrowIfErrorAsync(resp, ct);
    }

    public async Task<long> GetCommittedOffsetAsync(string group, string topic, CancellationToken ct = default)
    {
        using var resp = await Http.GetAsync(
            $"/v1/groups/{Uri.EscapeDataString(group)}/topics/{Uri.EscapeDataString(topic)}/offset", ct);
        await ThrowIfErrorAsync(resp, ct);
        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync(ct));
        return doc.RootElement.GetProperty("offset").GetInt64();
    }

    public async Task SweepAsync(CancellationToken ct = default)
    {
        using var resp = await Http.PostAsync("/v1/admin/sweep", null, ct);
        await ThrowIfErrorAsync(resp, ct);
    }

    public async Task ConfigureTopicAsync(string topic, object config, CancellationToken ct = default)
    {
        var body = new StringContent(JsonSerializer.Serialize(config), Encoding.UTF8, "application/json");
        using var resp = await Http.PutAsync($"/v1/topics/{Uri.EscapeDataString(topic)}", body, ct);
        await ThrowIfErrorAsync(resp, ct);
    }

    internal static async Task ThrowIfErrorAsync(HttpResponseMessage resp, CancellationToken ct)
    {
        if (resp.IsSuccessStatusCode) return;
        var text = await resp.Content.ReadAsStringAsync(ct);
        throw new BrokerRequestException((int)resp.StatusCode, text, resp.Headers);
    }
}

public sealed class ProduceResult
{
    public string Topic { get; set; } = "";
    public long FirstOffset { get; set; }
    public long LastOffset { get; set; }
    public long Count { get; set; }
    public double LatencyUs { get; set; }
    public string? Durability { get; set; }
}

internal static class Json
{
    public static readonly JsonSerializerOptions Options = new() { PropertyNameCaseInsensitive = true };
}

public sealed class BrokerRequestException(int statusCode, string body, System.Net.Http.Headers.HttpResponseHeaders headers)
    : Exception($"Broker returned {(int)statusCode}: {body}")
{
    public int StatusCode { get; } = statusCode;
    public long? OldestOffset { get; } = headers.TryGetValues("X-Oldest-Offset", out var v)
        && long.TryParse(v.FirstOrDefault(), out var o) ? o : null;
}
