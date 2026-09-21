# Mofucat.ReactiveHub

[![NuGet](https://img.shields.io/nuget/v/Mofucat.ReactiveHub.svg)](https://www.nuget.org/packages/Mofucat.ReactiveHub)

Keeps a SignalR `HubConnection` alive with Rx.

## ReactiveHubConnection

```csharp
using System.Reactive.Linq;

using Mofucat.ReactiveHub;

await using var hub = new ReactiveHubConnection(
    serverTimeout: TimeSpan.FromSeconds(30),
    keepAliveInterval: TimeSpan.FromSeconds(15));

// Received messages (follows the current connection)
using var messages = hub.On<string>("Message").Subscribe(x => Console.WriteLine(x));

// Subscribe = connect, dispose = disconnect
using var connection = hub.Connect(new Uri("http://localhost:5000/hub"))
    .Subscribe(x => Console.WriteLine($"{x.Kind} {x.ConnectionId} {x.Error?.Message}"));

// Send / invoke if connected (false if not connected)
await hub.TrySendAsync("Broadcast", "hello");
await hub.TryInvokeAsync("Register", [deviceId, name]);

// Invoke with result (InvalidOperationException if not connected)
var echo = await hub.InvokeAsync<string>("Echo", "hello");
```

### Behavior

* `Connect()` starts connecting when subscribed and keeps reconnecting until the subscription is disposed.
* Initial connect failures are retried with backoff (`retryDelays`, default `0, 2, 5, 10, 30, 60` seconds, the last value repeats). `resume` cuts the wait short (e.g. network restored).
* Disconnections after connecting are handled by the automatic reconnect of `HubConnection` with the same delays. `Reconnecting` / `Connected` are reported.
* When the connection is closed (reconnect given up / closed by the server), it starts over from the initial connect.
* `Connect()` is last-wins. Subscribing again completes the previous observer, stops the previous connection and starts a new one after the stop has completed (never two connections at once). `On()` and invocations follow the new connection.
* `Status` publishes the latest status to any subscriber (`Disconnected` while not connected).
* Statuses and messages are notified on background threads.
* `Dispose()` stops the connection without waiting, `DisposeAsync()` waits for the stop.

### Options

| Parameter | Description |
|-----------|-------------|
| `serverTimeout` / `keepAliveInterval` | `HubConnection.ServerTimeout` / `KeepAliveInterval` |
| `retryDelays` | Delays of the initial connect retry and the automatic reconnect |
| `scheduler` | Scheduler of the retry timer (for tests) |
| `Connect(url, configure)` | `HttpConnectionOptions` (access token, headers, transports) |
| `Connect(url, build)` | `IHubConnectionBuilder` (protocol, logging, stateful reconnect) |
| `Connect(url, resume)` | `IObservable<Unit>` that cuts the retry wait short |

### Recipes

Register again after every (re)connect:

```csharp
using var registration = hub.Status
    .Where(x => x.Kind == HubStatusKind.Connected)
    .SelectMany(_ => hub.TryInvokeAsync("Register", deviceId).AsTask())
    .Subscribe();
```

Keep connected while any subscriber exists:

```csharp
var shared = hub.Connect(url).Publish().RefCount();
```

Retry immediately when the network is restored (.NET MAUI):

```csharp
var resume = Observable.FromEventPattern<ConnectivityChangedEventArgs>(
        h => Connectivity.Current.ConnectivityChanged += h,
        h => Connectivity.Current.ConnectivityChanged -= h)
    .Where(x => x.EventArgs.NetworkAccess == NetworkAccess.Internet)
    .Select(_ => Unit.Default);

using var connection = hub.Connect(url, resume: resume).Subscribe();
```
