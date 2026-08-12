using System.Net;
using System.Text;
using System.Text.Json;

namespace Brook.Client;

/// <summary>
/// A consume stream: the broker replays [offset, end) from the log, then pushes
/// new messages as they arrive over the one open HTTP/2 connection.
/// A reset (slow-consumer overflow) surfaces as <see cref="ResetException"/> —
/// the at-least-once recovery move is to reconnect from your last committed offset.
/// </summary>
public sealed class ConsumerStream : IAsyncDisposable
{
    private readonly HttpResponseMessage _response;
    private readonly StreamReader _reader;

    private ConsumerStream(HttpResponseMessage response)
    {
        _response = response;
        _reader = new StreamReader(response.Content.ReadAsStream());
    }

    public static async Task<ConsumerStream> OpenAsync(HttpClient http, string url, CancellationToken ct)
    {
        using var req = new HttpRequestMessage(HttpMethod.Get, url)
        {
            Version = HttpVersion.Version20,                  // h2c: the broker is HTTP/2 only
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher,
        };
        var response = await http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct);
        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync(ct);
            throw new BrokerRequestException((int)response.StatusCode, body, response.Headers);
        }
        return new ConsumerStream(response);
    }

    public async Task<Message?> NextAsync(CancellationToken ct = default)
    {
        try
        {
            var line = await _reader.ReadLineAsync(ct);
            return line is null ? null : JsonSerializer.Deserialize<Message>(line, Json.Options);
        }
        catch (IOException ex)
        {
            throw new ResetException(ex.Message);
        }
    }

    public async ValueTask DisposeAsync()
    {
        _reader.Dispose();
        _response.Dispose();
    }

    public sealed class Message
    {
        public long Offset { get; set; }
        public long Timestamp { get; set; }
        public string Payload { get; set; } = "";
        public byte[] PayloadBytes => Encoding.UTF8.GetBytes(Payload);
    }
}

/// <summary>The broker aborted the stream (slow consumer ran the channel dry and we
/// refuse to drop data). Reconnect from your committed offset to resume.</summary>
public sealed class ResetException(string message) : Exception(message);
