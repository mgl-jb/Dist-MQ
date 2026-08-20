using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace DistMq.Storage.Tests;

/// <summary>
/// Runs Azurite, the Azure Storage emulator, for the duration of the test collection.
/// </summary>
/// <remarks>
/// The Azure code paths — append-position conditions, lease fencing, transaction limits —
/// are where the subtle failures live, so they are exercised against a real
/// implementation of the storage protocol rather than a mock. Each run gets its own
/// data directory and its own ports, so parallel runs do not collide.
/// </remarks>
public sealed class AzuriteFixture : IAsyncLifetime
{
    private Process? _process;
    private string? _dataDirectory;

    public string ConnectionString { get; private set; } = string.Empty;

    public ValueTask InitializeAsync()
    {
        var executable = ResolveAzurite();
        _dataDirectory = Path.Combine(Path.GetTempPath(), $"distmq-azurite-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dataDirectory);

        var blobPort = FreePort();
        var queuePort = FreePort();
        var tablePort = FreePort();

        var startInfo = new ProcessStartInfo(executable)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };

        foreach (var argument in new[]
        {
            "--silent",
            "--skipApiVersionCheck",
            "--location", _dataDirectory,
            "--blobHost", "127.0.0.1", "--blobPort", blobPort.ToString(),
            "--queueHost", "127.0.0.1", "--queuePort", queuePort.ToString(),
            "--tableHost", "127.0.0.1", "--tablePort", tablePort.ToString(),
        })
        {
            startInfo.ArgumentList.Add(argument);
        }

        _process = Process.Start(startInfo)
            ?? throw new InvalidOperationException("Could not start Azurite.");

        // The well-known emulator account. Azurite accepts nothing else.
        const string account = "devstoreaccount1";
        const string key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
        ConnectionString =
            $"DefaultEndpointsProtocol=http;AccountName={account};AccountKey={key};" +
            $"BlobEndpoint=http://127.0.0.1:{blobPort}/{account};" +
            $"QueueEndpoint=http://127.0.0.1:{queuePort}/{account};" +
            $"TableEndpoint=http://127.0.0.1:{tablePort}/{account};";

        WaitUntilListening(blobPort);
        WaitUntilListening(tablePort);
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        if (_process is { HasExited: false })
        {
            _process.Kill(entireProcessTree: true);
            _process.WaitForExit(10_000);
        }

        _process?.Dispose();

        if (_dataDirectory is not null && Directory.Exists(_dataDirectory))
        {
            try
            {
                Directory.Delete(_dataDirectory, recursive: true);
            }
            catch (IOException)
            {
                // A temp directory left behind is not worth failing a test run over.
            }
        }

        return ValueTask.CompletedTask;
    }

    private static string ResolveAzurite()
    {
        var candidates = new List<string>();

        if (Environment.GetEnvironmentVariable("PATH") is { } pathVariable)
        {
            candidates.AddRange(pathVariable
                .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries)
                .Select(directory => Path.Combine(directory, "azurite")));
        }

        candidates.Add("/opt/node22/bin/azurite");
        candidates.Add("/usr/local/bin/azurite");

        var found = candidates.FirstOrDefault(File.Exists);
        if (found is not null)
        {
            return found;
        }

        throw new InvalidOperationException(
            "Azurite was not found on PATH. The storage conformance suite runs against the real " +
            "Azure Storage protocol, so it is required: install it with 'npm install -g azurite'.");
    }

    private static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private void WaitUntilListening(int port)
    {
        var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(60);
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (_process is { HasExited: true })
            {
                throw new InvalidOperationException(
                    $"Azurite exited with code {_process.ExitCode} before it started listening. " +
                    $"{_process.StandardError.ReadToEnd()}");
            }

            try
            {
                using var probe = new TcpClient();
                probe.Connect(IPAddress.Loopback, port);
                return;
            }
            catch (SocketException)
            {
                Thread.Sleep(100);
            }
        }

        throw new TimeoutException($"Azurite did not start listening on port {port}.");
    }
}

[CollectionDefinition(Name)]
public class AzuriteCollection : ICollectionFixture<AzuriteFixture>
{
    public const string Name = "azurite";
}
