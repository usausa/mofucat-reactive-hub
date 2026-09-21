// ReSharper disable AccessToDisposedClosure
namespace Mofucat.ReactiveHub;

using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Subjects;

using Microsoft.AspNetCore.SignalR;

public sealed class ReactiveHubConnectionTests
{
    private static readonly TimeSpan[] FastRetryDelays = [TimeSpan.Zero, TimeSpan.FromMilliseconds(100)];

    private static readonly TimeSpan Quiet = TimeSpan.FromMilliseconds(300);

    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    // ------------------------------------------------------------
    // Connect
    // ------------------------------------------------------------

    [Fact]
    public async Task ConnectWhenServerAvailable()
    {
        await using var server = await HubServer.CreateAsync();
        await using var hub = new ReactiveHubConnection(retryDelays: FastRetryDelays);
        using var recorder = new Recorder<HubStatus>();

        Assert.False(hub.IsConnected);
        Assert.Null(hub.ConnectionId);

        using var subscription = hub.Connect(server.Url).Subscribe(recorder);

        var connecting = await recorder.NextAsync();
        Assert.Equal(HubStatusKind.Connecting, connecting.Kind);
        Assert.Null(connecting.ConnectionId);
        Assert.Null(connecting.Error);

        var connected = await recorder.NextAsync();
        Assert.Equal(HubStatusKind.Connected, connected.Kind);
        Assert.NotNull(connected.ConnectionId);
        Assert.Null(connected.Error);
        Assert.True(hub.IsConnected);
        Assert.Equal(connected.ConnectionId, hub.ConnectionId);
    }

    [Fact]
    public async Task ConnectRetriesUntilServerAvailable()
    {
        await using var server = await HubServer.CreateAsync();
        await server.StopAsync();
        await using var hub = new ReactiveHubConnection(retryDelays: FastRetryDelays);
        using var recorder = new Recorder<HubStatus>();

        using var subscription = hub.Connect(server.Url).Subscribe(recorder);

        Assert.Equal(HubStatusKind.Connecting, (await recorder.NextAsync()).Kind);

        var failed = await recorder.NextAsync();
        Assert.Equal(HubStatusKind.Connecting, failed.Kind);
        Assert.NotNull(failed.Error);
        Assert.False(hub.IsConnected);

        await server.StartAsync();

        var connected = await recorder.WaitForAsync(HubStatusKind.Connected);
        Assert.NotNull(connected.ConnectionId);
        Assert.True(hub.IsConnected);
    }

    [Fact]
    public async Task DisposingSubscriptionDisconnects()
    {
        await using var server = await HubServer.CreateAsync();
        await using var hub = new ReactiveHubConnection(retryDelays: FastRetryDelays);
        using var recorder = new Recorder<HubStatus>();

        var subscription = hub.Connect(server.Url).Subscribe(recorder);
        var connected = await recorder.WaitForAsync(HubStatusKind.Connected);

        subscription.Dispose();

        Assert.False(hub.IsConnected);
        Assert.Null(hub.ConnectionId);
        Assert.Null(await recorder.NextOrDefaultAsync(Quiet));

        var events = await server.WaitForEventsAsync(2);
        Assert.Equal([$"connected:{connected.ConnectionId}", $"disconnected:{connected.ConnectionId}"], events);
    }

    [Fact]
    public async Task ConnectIsLastWins()
    {
        await using var server = await HubServer.CreateAsync();
        await using var hub = new ReactiveHubConnection(retryDelays: FastRetryDelays);
        using var recorder1 = new Recorder<HubStatus>();
        using var recorder2 = new Recorder<HubStatus>();

        var subscription1 = hub.Connect(server.Url).Subscribe(recorder1);
        var first = await recorder1.WaitForAsync(HubStatusKind.Connected);

        using var subscription2 = hub.Connect(server.Url).Subscribe(recorder2);

        // The previous observer completes
        await recorder1.Completed;

        var second = await recorder2.WaitForAsync(HubStatusKind.Connected);
        Assert.NotEqual(first.ConnectionId, second.ConnectionId);
        Assert.True(hub.IsConnected);
        Assert.Equal(second.ConnectionId, hub.ConnectionId);

        // The new connection starts after the previous one has stopped
        var events = await server.WaitForEventsAsync(3);
        Assert.Equal([$"connected:{first.ConnectionId}", $"disconnected:{first.ConnectionId}", $"connected:{second.ConnectionId}"], events);

        // Disposing the replaced subscription does nothing
        subscription1.Dispose();

        Assert.True(hub.IsConnected);
        Assert.Null(await recorder2.NextOrDefaultAsync(Quiet));
    }

    [Fact]
    public async Task ConfigureAndBuildAreApplied()
    {
        await using var server = await HubServer.CreateAsync();
        await using var hub = new ReactiveHubConnection(retryDelays: FastRetryDelays);
        using var recorder = new Recorder<HubStatus>();

        var built = 0;
        using var subscription = hub.Connect(
                server.Url,
                configure: options => options.Headers["X-Test"] = "test",
                build: _ => built++)
            .Subscribe(recorder);
        await recorder.WaitForAsync(HubStatusKind.Connected);

        Assert.Equal(1, built);
        Assert.Equal("test", await hub.InvokeAsync<string>("Header", "X-Test", Cancel));
    }

    // ------------------------------------------------------------
    // Reconnect
    // ------------------------------------------------------------

    [Fact]
    public async Task ServerRestartReconnects()
    {
        await using var server = await HubServer.CreateAsync();
        await using var hub = new ReactiveHubConnection(retryDelays: FastRetryDelays);
        using var recorder = new Recorder<HubStatus>();

        using var subscription = hub.Connect(server.Url).Subscribe(recorder);
        var first = await recorder.WaitForAsync(HubStatusKind.Connected);

        await server.StopAsync();

        var reconnecting = await recorder.WaitForAsync(HubStatusKind.Reconnecting);
        Assert.Null(reconnecting.ConnectionId);
        Assert.False(hub.IsConnected);

        await server.StartAsync();

        var second = await recorder.WaitForAsync(HubStatusKind.Connected);
        Assert.NotNull(second.ConnectionId);
        Assert.NotEqual(first.ConnectionId, second.ConnectionId);
        Assert.True(hub.IsConnected);
        Assert.Equal(second.ConnectionId, hub.ConnectionId);
    }

    [Fact]
    public async Task ClosedRestartsFromInitialConnect()
    {
        await using var server = await HubServer.CreateAsync();
        await using var hub = new ReactiveHubConnection(retryDelays: FastRetryDelays);
        using var recorder = new Recorder<HubStatus>();

        using var subscription = hub.Connect(server.Url).Subscribe(recorder);
        var first = await recorder.WaitForAsync(HubStatusKind.Connected);

        // The server closes the connection without allowing reconnect
        Assert.True(await hub.TrySendAsync("Abort", Cancel));

        var connecting = await recorder.NextAsync();
        Assert.Equal(HubStatusKind.Connecting, connecting.Kind);

        var second = await recorder.WaitForAsync(HubStatusKind.Connected);
        Assert.NotEqual(first.ConnectionId, second.ConnectionId);
        Assert.True(hub.IsConnected);
    }

    // ------------------------------------------------------------
    // Retry
    // ------------------------------------------------------------

    [Fact]
    public async Task RetryFollowsDelaysAndResume()
    {
        await using var server = await HubServer.CreateAsync();
        await server.StopAsync();

        using var scheduler = new SignalingTestScheduler();
        TimeSpan[] delays = [TimeSpan.Zero, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5)];
        await using var hub = new ReactiveHubConnection(retryDelays: delays, scheduler: scheduler);
        using var recorder = new Recorder<HubStatus>();

        // Signals when the retry subscribes to resume
        using var resumeSubscribed = new SemaphoreSlim(0);
        using var resume = new Subject<Unit>();
        var resumeSource = Observable.Create<Unit>(observer =>
        {
            var disposable = resume.Subscribe(observer);
            resumeSubscribed.Release();
            return disposable;
        });

        using var subscription = hub.Connect(server.Url, resume: resumeSource).Subscribe(recorder);

        Assert.Equal(HubStatusKind.Connecting, (await recorder.NextAsync()).Kind);

        // 1st attempt fails, retry after 0 (the test scheduler runs it on the next tick)
        Assert.NotNull((await recorder.NextAsync()).Error);
        await scheduler.WaitScheduledAsync(Cancel);
        Assert.True(await resumeSubscribed.WaitAsync(TestSettings.Timeout, Cancel));
        scheduler.AdvanceBy(1);

        // 2nd attempt fails, retry after 2 seconds
        Assert.NotNull((await recorder.NextAsync()).Error);
        await scheduler.WaitScheduledAsync(Cancel);
        Assert.True(await resumeSubscribed.WaitAsync(TestSettings.Timeout, Cancel));
        scheduler.AdvanceBy(TimeSpan.FromSeconds(1).Ticks);
        Assert.Null(await recorder.NextOrDefaultAsync(Quiet));
        scheduler.AdvanceBy(TimeSpan.FromSeconds(1).Ticks);

        // 3rd attempt fails, retry after 5 seconds
        Assert.NotNull((await recorder.NextAsync()).Error);
        await scheduler.WaitScheduledAsync(Cancel);
        Assert.True(await resumeSubscribed.WaitAsync(TestSettings.Timeout, Cancel));
        scheduler.AdvanceBy(TimeSpan.FromSeconds(4).Ticks);
        Assert.Null(await recorder.NextOrDefaultAsync(Quiet));
        scheduler.AdvanceBy(TimeSpan.FromSeconds(1).Ticks);

        // 4th attempt fails, retry after 5 seconds again (capped at the last delay)
        Assert.NotNull((await recorder.NextAsync()).Error);
        await scheduler.WaitScheduledAsync(Cancel);
        Assert.True(await resumeSubscribed.WaitAsync(TestSettings.Timeout, Cancel));
        scheduler.AdvanceBy(TimeSpan.FromSeconds(4).Ticks);
        Assert.Null(await recorder.NextOrDefaultAsync(Quiet));

        // Resume cuts the wait short
        await server.StartAsync();
        resume.OnNext(Unit.Default);

        var connected = await recorder.WaitForAsync(HubStatusKind.Connected);
        Assert.NotNull(connected.ConnectionId);
        Assert.True(hub.IsConnected);
    }

    // ------------------------------------------------------------
    // Receive
    // ------------------------------------------------------------

    [Fact]
    public async Task OnReceivesMessages()
    {
        await using var server = await HubServer.CreateAsync();
        await using var hub = new ReactiveHubConnection(retryDelays: FastRetryDelays);
        using var messages = new Recorder<string>();
        using var recorder = new Recorder<HubStatus>();

        // Subscribed before connecting
        using var receiving = hub.On<string>("Message").Subscribe(messages);

        using var subscription = hub.Connect(server.Url).Subscribe(recorder);
        await recorder.WaitForAsync(HubStatusKind.Connected);

        Assert.True(await hub.TryInvokeAsync("Broadcast", "hello", Cancel));
        Assert.Equal("hello", await messages.NextAsync());

        Assert.True(await hub.TrySendAsync("Ping", Cancel));
        Assert.Equal("pong", await messages.NextAsync());
    }

    [Fact]
    public async Task OnFollowsConnectionReplacement()
    {
        await using var server = await HubServer.CreateAsync();
        await using var hub = new ReactiveHubConnection(retryDelays: FastRetryDelays);
        using var messages = new Recorder<string>();
        using var recorder1 = new Recorder<HubStatus>();
        using var recorder2 = new Recorder<HubStatus>();

        using var receiving = hub.On<string>("Message").Subscribe(messages);

        using var subscription1 = hub.Connect(server.Url).Subscribe(recorder1);
        await recorder1.WaitForAsync(HubStatusKind.Connected);

        Assert.True(await hub.TryInvokeAsync("Broadcast", "first", Cancel));
        Assert.Equal("first", await messages.NextAsync());

        using var subscription2 = hub.Connect(server.Url).Subscribe(recorder2);
        await recorder2.WaitForAsync(HubStatusKind.Connected);

        Assert.True(await hub.TryInvokeAsync("Broadcast", "second", Cancel));
        Assert.Equal("second", await messages.NextAsync());
    }

    // ------------------------------------------------------------
    // Send
    // ------------------------------------------------------------

    [Fact]
    public async Task InvokeReturnsResult()
    {
        await using var server = await HubServer.CreateAsync();
        await using var hub = new ReactiveHubConnection(retryDelays: FastRetryDelays);
        using var recorder = new Recorder<HubStatus>();

        using var subscription = hub.Connect(server.Url).Subscribe(recorder);
        await recorder.WaitForAsync(HubStatusKind.Connected);

        Assert.Equal("hello", await hub.InvokeAsync<string>("Echo", "hello", Cancel));
        Assert.Equal(3, await hub.InvokeAsync<int>("Add", [1, 2], Cancel));
    }

    [Fact]
    public async Task InvokeWhenNotConnected()
    {
        await using var hub = new ReactiveHubConnection(retryDelays: FastRetryDelays);

        Assert.False(await hub.TrySendAsync("Ping", Cancel));
        Assert.False(await hub.TryInvokeAsync("Ping", Cancel));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await hub.InvokeAsync<string>("Echo", "hello", Cancel));
    }

    [Fact]
    public async Task InvokeFailurePropagates()
    {
        await using var server = await HubServer.CreateAsync();
        await using var hub = new ReactiveHubConnection(retryDelays: FastRetryDelays);
        using var recorder = new Recorder<HubStatus>();

        using var subscription = hub.Connect(server.Url).Subscribe(recorder);
        await recorder.WaitForAsync(HubStatusKind.Connected);

        await Assert.ThrowsAsync<HubException>(async () => await hub.TryInvokeAsync("Fail", Cancel));
        Assert.True(hub.IsConnected);
    }

    // ------------------------------------------------------------
    // Status
    // ------------------------------------------------------------

    [Fact]
    public async Task StatusPublishesLatest()
    {
        await using var server = await HubServer.CreateAsync();
        await using var hub = new ReactiveHubConnection(retryDelays: FastRetryDelays);
        using var statuses = new Recorder<HubStatus>();
        using var recorder = new Recorder<HubStatus>();

        using var watching = hub.Status.Subscribe(statuses);
        Assert.Equal(HubStatusKind.Disconnected, (await statuses.NextAsync()).Kind);

        var subscription = hub.Connect(server.Url).Subscribe(recorder);
        Assert.Equal(HubStatusKind.Connecting, (await statuses.NextAsync()).Kind);
        var connected = await statuses.WaitForAsync(HubStatusKind.Connected);
        Assert.Equal((await recorder.WaitForAsync(HubStatusKind.Connected)).ConnectionId, connected.ConnectionId);

        subscription.Dispose();
        Assert.Equal(HubStatusKind.Disconnected, (await statuses.NextAsync()).Kind);

        // A late subscriber receives the latest status
        using var late = new Recorder<HubStatus>();
        using var watchingLate = hub.Status.Subscribe(late);
        Assert.Equal(HubStatusKind.Disconnected, (await late.NextAsync()).Kind);
    }

    // ------------------------------------------------------------
    // Dispose
    // ------------------------------------------------------------

    [Fact]
    public async Task DisposeStopsConnection()
    {
        await using var server = await HubServer.CreateAsync();
        using var statuses = new Recorder<HubStatus>();
        using var recorder = new Recorder<HubStatus>();
        using var messages = new Recorder<string>();

        var hub = new ReactiveHubConnection(retryDelays: FastRetryDelays);
        try
        {
            using var watching = hub.Status.Subscribe(statuses);
            using var receiving = hub.On<string>("Message").Subscribe(messages);
            var subscription = hub.Connect(server.Url).Subscribe(recorder);
            var connected = await recorder.WaitForAsync(HubStatusKind.Connected);

            await hub.DisposeAsync();

            // The observers complete
            await recorder.Completed;
            await statuses.Completed;
            await messages.Completed;

            // The connection has stopped
            var events = await server.WaitForEventsAsync(2);
            Assert.Equal([$"connected:{connected.ConnectionId}", $"disconnected:{connected.ConnectionId}"], events);
            Assert.False(hub.IsConnected);
            Assert.Null(hub.ConnectionId);

            Assert.Throws<ObjectDisposedException>(() => hub.Connect(server.Url));
            Assert.Throws<ObjectDisposedException>(() => hub.On<string>("Message"));
            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await hub.TrySendAsync("Ping", Cancel));
            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await hub.TryInvokeAsync("Ping", Cancel));
            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await hub.InvokeAsync<string>("Echo", "hello", Cancel));

            // Disposing the old subscription does nothing
            subscription.Dispose();
        }
        finally
        {
            // Disposing again does nothing
            await hub.DisposeAsync();
        }
    }

    [Fact]
    public void DisposeIsIdempotent()
    {
        var hub = new ReactiveHubConnection(retryDelays: FastRetryDelays);

        hub.Dispose();
        hub.Dispose();

        Assert.False(hub.IsConnected);
        Assert.Throws<ObjectDisposedException>(() => hub.On<string>("Message"));
    }

    [Fact]
    public void EmptyRetryDelaysIsRejected()
    {
        Assert.Throws<ArgumentException>(() => new ReactiveHubConnection(retryDelays: []));
    }
}
