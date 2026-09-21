namespace Mofucat.ReactiveHub;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

// In-process SignalR server. The port is kept across StopAsync() / StartAsync()
public sealed class HubServer : IAsyncDisposable
{
    private WebApplication? app;

    public int Port { get; private set; }

    public Uri Url => new($"http://127.0.0.1:{Port}/hub");

    public ConnectionTracker Tracker { get; } = new();

    public static async Task<HubServer> CreateAsync()
    {
        var server = new HubServer();
        await server.StartAsync();
        return server;
    }

    public async Task StartAsync()
    {
        var builder = WebApplication.CreateSlimBuilder([]);
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls($"http://127.0.0.1:{Port}");
        builder.Services.AddSingleton(Tracker);
        builder.Services.AddSignalR(options =>
        {
            options.KeepAliveInterval = TimeSpan.FromSeconds(1);
            options.ClientTimeoutInterval = TimeSpan.FromSeconds(2);
        });

        app = builder.Build();
        app.MapHub<TestHub>("/hub");
        await app.StartAsync();

        Port = new Uri(app.Urls.First()).Port;
    }

    public async Task StopAsync()
    {
        if (app is null)
        {
            return;
        }

        await app.StopAsync();
        await app.DisposeAsync();
        app = null;
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
    }

    // Waits until the server has seen the number of connection events
    public async Task<IReadOnlyList<string>> WaitForEventsAsync(int count)
    {
        var endTime = DateTime.UtcNow + TestSettings.Timeout;
        while (DateTime.UtcNow < endTime)
        {
            var events = Tracker.Events;
            if (events.Count >= count)
            {
                return events;
            }

            await Task.Delay(10);
        }

        throw new TimeoutException($"Events not reached. count=[{count}]");
    }
}

// Records connected / disconnected events on the server side
public sealed class ConnectionTracker
{
    private readonly Lock sync = new();

    private readonly List<string> events = [];

    public IReadOnlyList<string> Events
    {
        get
        {
            lock (sync)
            {
                return [.. events];
            }
        }
    }

    public void Add(string value)
    {
        lock (sync)
        {
            events.Add(value);
        }
    }
}

// Hub methods must be instance methods
#pragma warning disable CA1822
public sealed class TestHub : Hub
{
    private readonly ConnectionTracker tracker;

    public TestHub(ConnectionTracker tracker)
    {
        this.tracker = tracker;
    }

    public override Task OnConnectedAsync()
    {
        tracker.Add($"connected:{Context.ConnectionId}");
        return base.OnConnectedAsync();
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        tracker.Add($"disconnected:{Context.ConnectionId}");
        return base.OnDisconnectedAsync(exception);
    }

    public string Echo(string text) => text;

    public int Add(int a, int b) => a + b;

    public string Header(string name) => Context.GetHttpContext()?.Request.Headers[name].ToString() ?? string.Empty;

    public Task Broadcast(string text) => Clients.All.SendAsync("Message", text);

    public Task Ping() => Clients.Caller.SendAsync("Message", "pong");

    public void Fail() => throw new HubException("failed");

    // Closes the connection without allowing reconnect (the client sees Closed)
    public void Abort() => Context.Abort();
}
#pragma warning restore CA1822
