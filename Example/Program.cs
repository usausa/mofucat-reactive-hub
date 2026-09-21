using System.Reactive.Disposables;

using Example;

using Mofucat.ReactiveHub;

// In-process server and a client in one console

const int port = 5300;
var url = new Uri($"http://127.0.0.1:{port}/hub");

var server = new ExampleServer(port);
await server.StartAsync();

await using var hub = new ReactiveHubConnection(
    serverTimeout: TimeSpan.FromSeconds(10),
    keepAliveInterval: TimeSpan.FromSeconds(5),
    retryDelays: [TimeSpan.Zero, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3)]);

using var status = hub.Status.Subscribe(x => Console.WriteLine($"Status: {x.Kind} id=[{x.ConnectionId}] error=[{x.Error?.Message}]"));
using var ticks = hub.On<int>("Tick").Subscribe(x => Console.WriteLine($"Tick: {x}"));

// Subscribe = connect, dispose = disconnect
using var connection = new SerialDisposable();
connection.Disposable = hub.Connect(url).Subscribe(_ => { }, ex => Console.WriteLine($"Error: {ex.Message}"));

Console.WriteLine("[S] Stop server / [R] Restart server / [E] Echo / [C] Connect again / [D] Disconnect / [Q] Quit");

var running = true;
while (running)
{
    switch (Console.ReadKey(true).Key)
    {
        case ConsoleKey.S:
            await server.StopAsync();
            Console.WriteLine("Server stopped");
            break;
        case ConsoleKey.R:
            await server.StartAsync();
            Console.WriteLine("Server started");
            break;
        case ConsoleKey.E:
            try
            {
                Console.WriteLine($"Echo: {await hub.InvokeAsync<string>("Echo", "hello")}");
            }
            catch (InvalidOperationException ex)
            {
                Console.WriteLine($"Echo failed: {ex.Message}");
            }
            break;
        case ConsoleKey.C:
            connection.Disposable = hub.Connect(url).Subscribe(_ => { }, ex => Console.WriteLine($"Error: {ex.Message}"));
            break;
        case ConsoleKey.D:
            connection.Disposable = null;
            break;
        case ConsoleKey.Q:
            running = false;
            break;
    }
}

await server.StopAsync();
