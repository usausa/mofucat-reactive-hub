namespace Example;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

public sealed class ExampleServer
{
    private readonly int port;

    private WebApplication? app;

    public ExampleServer(int port)
    {
        this.port = port;
    }

    public Task StartAsync()
    {
        if (app is not null)
        {
            return Task.CompletedTask;
        }

        var builder = WebApplication.CreateSlimBuilder([]);
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
        builder.Services.AddSignalR();
        builder.Services.AddHostedService<TickService>();

        app = builder.Build();
        app.MapHub<ExampleHub>("/hub");
        return app.StartAsync();
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
}

public sealed class ExampleHub : Hub
{
#pragma warning disable CA1822
    public string Echo(string text) => text;
#pragma warning restore CA1822
}

// Broadcasts a counter every second
public sealed class TickService : BackgroundService
{
    private readonly IHubContext<ExampleHub> context;

    public TickService(IHubContext<ExampleHub> context)
    {
        this.context = context;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var count = 0;
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            await context.Clients.All.SendAsync("Tick", ++count, stoppingToken);
        }
    }
}
