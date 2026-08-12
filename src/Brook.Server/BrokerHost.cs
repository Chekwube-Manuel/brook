using Brook.Core.Engine;
using Microsoft.AspNetCore.Server.Kestrel.Core;

namespace Brook.Server;

/// <summary>Builds and maps the Kestrel host. Exposed as a plain method (not top-level
/// statements) so tests can boot a real in-process broker on an ephemeral port.</summary>
public static class BrokerHost
{
    public static WebApplication Build(string[] args)
    {
        var (url, dataDir, rest) = ParseArgs(args);

        var builder = WebApplication.CreateBuilder(rest);
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            // HTTP/2 over cleartext (h2c, prior knowledge). .NET clients negotiate it
            // automatically; curl needs --http2-prior-knowledge. Kept HTTP/2-only so the
            // streaming protocol is never silently downgraded to HTTP/1.1.
            var uri = new Uri(url);
            var port = uri.Port == 0 ? 0 : uri.Port;
            var options = (ListenOptions lo) => { lo.Protocols = HttpProtocols.Http2; };
            if (uri.Host is "localhost" or "127.0.0.1")
                kestrel.Listen(System.Net.IPAddress.Loopback, port, options);
            else if (uri.Host == "[::1]")
                kestrel.Listen(System.Net.IPAddress.IPv6Loopback, port, options);
            else
                kestrel.ListenAnyIP(port, options);
        });
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.Services.AddSingleton(new BrokerEngine(dataDir));

        var app = builder.Build();
        Endpoints.Map(app);
        return app;
    }

    private static (string Url, string DataDir, string[] RemainingArgs) ParseArgs(string[] args)
    {
        string Take(string flag, string def)
        {
            var i = Array.IndexOf(args, "--" + flag);
            if (i >= 0 && i + 1 < args.Length) return args[i + 1];
            return Environment.GetEnvironmentVariable("BROOK_" + flag.ToUpperInvariant()) ?? def;
        }

        var url = Take("urls", $"http://127.0.0.1:{BrokerEngine.DefaultPort}");
        var dataDir = Path.GetFullPath(Take("data", "data"));
        var rest = new List<string>();
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i] is "--urls" or "--data") { i++; continue; }
            if (args[i].StartsWith("--urls=") || args[i].StartsWith("--data=")) continue;
            rest.Add(args[i]);
        }
        return (url, dataDir, [.. rest]);
    }
}
