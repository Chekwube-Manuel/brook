using HttpBroker.Client;
using HttpBroker.Server;
using Microsoft.AspNetCore.Builder;
using Xunit;

namespace HttpBroker.Tests;

/// <summary>
/// Real end-to-end: boots an actual Kestrel broker on an ephemeral port and drives it
/// over HTTP/2 with the client library. Proves the wire protocol, not just the engine.
/// </summary>
public class EndToEndTests : IAsyncLifetime
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "hfb-e2e-" + Guid.NewGuid().ToString("N"));
    private WebApplication _app = null!;
    private BrokerClient _client = null!;
    private string _url = "";

    public async Task InitializeAsync()
    {
        _app = BrokerHost.Build(["--urls", "http://127.0.0.1:0", "--data", _dataDir]);
        await _app.StartAsync();
        _url = _app.Urls.First(u => u.StartsWith("http://127.0.0.1:"));
        _client = new BrokerClient(_url);
    }

    private static string Payload(int i) => $"hello-{i:D6}";

    [Fact]
    public async Task ProduceThenConsumeInOrderOverHttp()
    {
        const int n = 500;

        for (int i = 0; i < n; i++)
            await _client.ProduceAsync("e2e", [Payload(i)]);

        var got = new List<string>();
        await using (var stream = await _client.OpenStreamAsync("e2e", offset: 0))
        {
            for (int i = 0; i < n; i++)
            {
                var msg = await stream.NextAsync();
                Assert.NotNull(msg);
                got.Add(msg!.Payload);
            }
        }

        Assert.Equal(Enumerable.Range(0, n).Select(Payload), got);
    }

    [Fact]
    public async Task BatchProduceReturnsOffsetsAndStreamResumesFromCommitted()
    {
        const string group = "batch-group";

        // 100 messages in one batch POST
        var result = await _client.ProduceAsync("batch", Enumerable.Range(0, 100).Select(Payload));
        Assert.Equal(0, result.FirstOffset);
        Assert.Equal(99, result.LastOffset);
        Assert.Equal(100, result.Count);

        // consume 40, commit 40
        await using (var stream = await _client.OpenStreamAsync("batch", group, offset: 0))
            for (int i = 0; i < 40; i++) await stream.NextAsync();
        await _client.CommitOffsetAsync(group, "batch", 40);

        // a new consumer in the same group resumes exactly at 40
        var resumed = (await _client.GetCommittedOffsetAsync(group, "batch"));
        Assert.Equal(40, resumed);

        var tail = new List<string>();
        await using (var stream = await _client.OpenStreamAsync("batch", group, resumed))
            for (int i = 0; i < 60; i++)
            {
                var msg = await stream.NextAsync();
                Assert.NotNull(msg);
                tail.Add(msg!.Payload);
            }

        Assert.Equal(Enumerable.Range(40, 60).Select(Payload), tail);
    }

    [Fact]
    public async Task ConfigureAndDescribeTopic()
    {
        await _client.ConfigureTopicAsync("cfg-topic", new { durability = "fsync", retention = new { maxBytes = 1_000_000 } });
        var got = await _client.Http.GetStringAsync($"{_url}/v1/topics/cfg-topic");
        Assert.Contains("\"durability\":\"Fsync\"", got);
        Assert.Contains("\"maxBytes\":1000000", got);
    }

    [Fact]
    public async Task ExpiredOffsetReturns410WithRewindHint()
    {
        // small segments + retention so the sweep deletes early segments
        await _client.ConfigureTopicAsync("tiny", new { durability = "none", segmentMaxBytes = 64, retention = new { maxBytes = 64 } });

        for (int i = 0; i < 200; i++) await _client.ProduceAsync("tiny", [$"m{i}"]);

        await _client.SweepAsync();

        var ex = await Assert.ThrowsAsync<BrokerRequestException>(async () =>
        {
            await using var s = await _client.OpenStreamAsync("tiny", offset: 0);
        });

        Assert.Equal(410, ex.StatusCode);
        Assert.NotNull(ex.OldestOffset);
        Assert.True(ex.OldestOffset > 0);
    }

    public async Task DisposeAsync()
    {
        _client.Dispose();
        await _app.StopAsync();
        await _app.DisposeAsync();
        try { if (Directory.Exists(_dataDir)) Directory.Delete(_dataDir, recursive: true); }
        catch { /* best-effort */ }
    }
}