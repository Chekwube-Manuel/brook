using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace HttpBroker.Client;

/// <summary>
/// Thin client over the broker's HTTP API. The wire format is JSON so any language —
/// C#, Go, Node, or curl — can talk to the same endpoints. HTTP/2 multiplexing means
/// many concurrent produce/consume calls share one connection to the broker.
/// </summary>
public sealed class BrokerClient : IDisposable
{
    internal readonly HttpClient Http;
    private readonly bool _ownsHttp;

    public Uri BaseAddress => Http.BaseAddress!;

    public BrokerClient(string baseUrl, HttpClient? http = null)
    {
        _ownsHttp = http is null;
        Http = http ?? new HttpClient();
        Http.BaseAddress = new Uri(baseUrl.EndsWith('/') ? baseUrl : baseUrl + "/");
        Http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        Http.Timeout = Timeout.InfiniteTimeSpan; // streams stay open
    }

    public void Dispose()
    {
        if (_ownsHttp) Http.Dispose();
    }

    public Task<ProduceResult> ProduceAsync(string topic, IEnumerable<string> messages, CancellationToken ct = default)
    {
        var payloads = messages.Select(m => new { payload = m }).ToArray();
        var content = new StringContent(JsonSerializer.Serialize(payloads), Encoding.UTF8, "application/json");
        return ProduceCoreAsync(topic, content, ct);
    }

    public Task<ProduceResult> ProduceAsync(string topic, IReadOnlyList<byte[]> messages, CancellationToken ct = default)
    {
        var payloads = messages.Select(m => new { payload = Convert.ToBase64String(m) }).ToArray();
        var content = new StringContent(JsonSerializer.Serialize(payloads), Encoding.UTF8, "application/json");
        return ProduceCoreAsync(topic, content, ct);
    }

    private async Task<ProduceResult> ProduceCoreAsync(string topic, HttpContent content, CancellationToken ct)
    {
        using var resp = await Http.PostAsync($"/v1/topics/{Uri.EscapeDataString(topic)}/messages", content, ct);
        await ThrowIfErrorAsync(resp, ct);
        return JsonSerializer.Deserialize<ProduceResult>(await resp.Content.ReadAsStringAsync(ct), Json.Options)!;
    }

    /// <summary>Open a consume stream. The returned reader adapts the NDJSON response;
    /// the broker keeps the connection open and pushes as new messages arrive.</summary>
    public Task<ConsumerStream> OpenStreamAsync(string topic, string? group = null, long? offset = null, CancellationToken ct = default)
    {
        var query = new List<string>();
        if (group is not null) query.Add($"group={Uri.EscapeDataString(group)}");
        if (offset is not null) query.Add($"offset={offset}");
        var qs = query.Count > 0 ? "?" + string.Join("&", query) : "";
        return ConsumerStream.OpenAsync(Http, $"/v1/topics/{Uri.EscapeDataString(topic)}/stream{qs}", ct);
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
