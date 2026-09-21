namespace Mofucat.ReactiveHub;

using System.Net.WebSockets;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;

using Microsoft.AspNetCore.Http.Connections.Client;
using Microsoft.AspNetCore.SignalR.Client;

// Keeps a SignalR HubConnection alive with Rx.
// - Connect() starts connecting when subscribed and keeps reconnecting until the subscription is disposed.
//   Initial connect failures are retried with backoff (resume cuts the wait short), disconnections after
//   connecting are left to the automatic reconnect of HubConnection (same backoff), and when the connection
//   is closed (reconnect given up / closed by the server) it starts over from the initial connect.
// - Connect() is last-wins. A new subscription stops the previous connection and completes the previous observer.
// - On() / TryInvokeAsync() / InvokeAsync() / IsConnected use the current connection, so they keep working
//   across connection replacement.
// - Status publishes the latest status (Disconnected while nothing is subscribed).
// - Dispose() stops the running connection. Connect() / On() / invocations throw ObjectDisposedException afterwards.
public sealed class ReactiveHubConnection : IDisposable, IAsyncDisposable
{
    private static readonly TimeSpan[] DefaultRetryDelays =
    [
        TimeSpan.Zero,
        TimeSpan.FromSeconds(2),
        TimeSpan.FromSeconds(5),
        TimeSpan.FromSeconds(10),
        TimeSpan.FromSeconds(30),
        TimeSpan.FromSeconds(60)
    ];

    private static readonly HubStatus Disconnected = new(HubStatusKind.Disconnected, null, null);

    private readonly IReadOnlyList<TimeSpan> retryDelays;

    private readonly TimeSpan? serverTimeout;

    private readonly TimeSpan? keepAliveInterval;

    private readonly IScheduler scheduler;

    private readonly IRetryPolicy reconnectPolicy;

    private readonly Lock sync = new();

    // Current connection (exists only while Connect() is subscribed)
    private readonly BehaviorSubject<HubConnection?> connections = new(null);

    // Latest status
    private readonly BehaviorSubject<HubStatus> status = new(Disconnected);

    // Running session (last-wins)
    private Session? current;

    // Stop of the previous sessions (the next session waits for this before connecting)
    private Task stopping = Task.CompletedTask;

    private bool disposed;

    public IObservable<HubStatus> Status => status.AsObservable();

    public bool IsConnected => connections.TryGetValue(out var connection) && (connection?.State == HubConnectionState.Connected);

    public string? ConnectionId => connections.TryGetValue(out var connection) ? connection?.ConnectionId : null;

    // ------------------------------------------------------------
    // Constructor
    // ------------------------------------------------------------

    public ReactiveHubConnection(
        TimeSpan? serverTimeout = null,
        TimeSpan? keepAliveInterval = null,
        IReadOnlyList<TimeSpan>? retryDelays = null,
        IScheduler? scheduler = null)
    {
        this.serverTimeout = serverTimeout;
        this.keepAliveInterval = keepAliveInterval;
        this.retryDelays = retryDelays ?? DefaultRetryDelays;
        if (this.retryDelays.Count == 0)
        {
            throw new ArgumentException("Retry delays cannot be empty", nameof(retryDelays));
        }
        this.scheduler = scheduler ?? Scheduler.Default;
        reconnectPolicy = new BackoffRetryPolicy(this.retryDelays);
    }

    // Stops the running session (the observer completes) without waiting for the stop
    public void Dispose()
    {
        _ = DisposeCore();
    }

    // Stops the running session and waits for the connection to stop
    public async ValueTask DisposeAsync()
    {
        await DisposeCore().ConfigureAwait(false);
    }

    private Task DisposeCore()
    {
        Session? session;
        Task stopped;
        lock (sync)
        {
            if (disposed)
            {
                return stopping;
            }

            disposed = true;
            session = current;
            current = null;
            if (session is not null)
            {
                session.Active = false;
                stopping = Task.WhenAll(stopping, session.Stopped);
            }
            stopped = stopping;

            connections.OnNext(null);
            connections.OnCompleted();
            status.OnNext(Disconnected);
            status.OnCompleted();
        }

        session?.Dispose();
        connections.Dispose();
        status.Dispose();
        return stopped;
    }

    // ------------------------------------------------------------
    // Connect
    // ------------------------------------------------------------

    // Keeps the connection alive while subscribed. Subscribing again replaces the previous subscription.
    public IObservable<HubStatus> Connect(
        Uri url,
        Action<HttpConnectionOptions>? configure = null,
        Action<IHubConnectionBuilder>? build = null,
        IObservable<Unit>? resume = null)
    {
        ArgumentNullException.ThrowIfNull(url);
        ObjectDisposedException.ThrowIf(disposed, this);

        return Observable.Create<HubStatus>(observer =>
        {
            var session = new Session(CreateConnection(url, configure, build));
            Session? previous;
            lock (sync)
            {
                if (disposed)
                {
                    session.Dispose();
                }

                ObjectDisposedException.ThrowIf(disposed, this);

                previous = current;
                current = session;
                if (previous is not null)
                {
                    previous.Active = false;
                    stopping = Task.WhenAll(stopping, previous.Stopped);
                }

                connections.OnNext(session.Connection);
                session.Start(
                    Connect(session.Connection, stopping, resume ?? Observable.Never<Unit>()).Do(x => Publish(session, x)),
                    observer);
            }

            // Complete the previous observer and stop the previous connection (the new connection waits for the stop)
            previous?.Dispose();

            return Disposable.Create(() => Release(session));
        });
    }

    // Publishes the status of the active session
    private void Publish(Session session, HubStatus value)
    {
        lock (sync)
        {
            if (session.Active)
            {
                status.OnNext(value);
            }
        }
    }

    // Releases the subscription. Stops the session if it is the current one (no-op if replaced or disposed)
    private void Release(Session session)
    {
        lock (sync)
        {
            if (!ReferenceEquals(current, session))
            {
                return;
            }

            current = null;
            session.Active = false;
            stopping = Task.WhenAll(stopping, session.Stopped);
            connections.OnNext(null);
            status.OnNext(Disconnected);
        }

        session.Dispose();
    }

    private HubConnection CreateConnection(Uri url, Action<HttpConnectionOptions>? configure, Action<IHubConnectionBuilder>? build)
    {
        var builder = (configure is null ? new HubConnectionBuilder().WithUrl(url) : new HubConnectionBuilder().WithUrl(url, configure))
            .WithAutomaticReconnect(reconnectPolicy);
        build?.Invoke(builder);

        var connection = builder.Build();
        if (serverTimeout is { } timeout)
        {
            connection.ServerTimeout = timeout;
        }
        if (keepAliveInterval is { } interval)
        {
            connection.KeepAliveInterval = interval;
        }

        return connection;
    }

    private IObservable<HubStatus> Connect(HubConnection connection, Task ready, IObservable<Unit> resume) =>
        Observable.Create<HubStatus>(observer =>
        {
            // Bridge the events (Func<T, Task>) of HubConnection to observables
            var closed = new Subject<Exception?>();
            var reconnecting = new Subject<Exception?>();
            var reconnected = new Subject<string?>();
            Task OnClosed(Exception? exception)
            {
                closed.OnNext(exception);
                return Task.CompletedTask;
            }

            Task OnReconnecting(Exception? exception)
            {
                reconnecting.OnNext(exception);
                return Task.CompletedTask;
            }

            Task OnReconnected(string? connectionId)
            {
                reconnected.OnNext(connectionId);
                return Task.CompletedTask;
            }

            connection.Closed += OnClosed;
            connection.Reconnecting += OnReconnecting;
            connection.Reconnected += OnReconnected;

            // Single connect attempt (after the previous connection has stopped). A failure is reported before retrying
            var attempt = Observable.FromAsync(async cancel =>
                {
                    await ready.WaitAsync(cancel).ConfigureAwait(false);
                    await connection.StartAsync(cancel).ConfigureAwait(false);
                })
                .Select(_ => new HubStatus(HubStatusKind.Connected, connection.ConnectionId, null))
                .Catch(static (Exception ex) => Observable.Return(new HubStatus(HubStatusKind.Connecting, null, ex)).Concat(Observable.Throw<HubStatus>(ex)));

            // Initial connect: retry with backoff (resume cuts the wait short)
            var connect = attempt.RetryWhen(errors => errors
                .Select(static (_, index) => index)
                .SelectMany(index => Observable.Timer(retryDelays[Math.Min(index, retryDelays.Count - 1)], scheduler)
                    .Select(static _ => Unit.Default)
                    .Amb(resume.Take(1))));

            // Connected -> Closed -> start over from the initial connect
            var session = connect
                .Concat(closed.Take(1).Select(static ex => new HubStatus(HubStatusKind.Connecting, null, ex)))
                .Repeat();

            var subscription = Observable.Merge(
                    session,
                    reconnecting.Select(static ex => new HubStatus(HubStatusKind.Reconnecting, null, ex)),
                    reconnected.Select(static id => new HubStatus(HubStatusKind.Connected, id, null)))
                .StartWith(new HubStatus(HubStatusKind.Connecting, null, null))
                .Subscribe(observer);

            return new CompositeDisposable(
                subscription,
                Disposable.Create(() =>
                {
                    connection.Closed -= OnClosed;
                    connection.Reconnecting -= OnReconnecting;
                    connection.Reconnected -= OnReconnected;
                }),
                closed,
                reconnecting,
                reconnected);
        });

    // ------------------------------------------------------------
    // Receive
    // ------------------------------------------------------------

    // Received messages. The handler is registered only while subscribed
    public IObservable<T> On<T>(string methodName)
    {
        ArgumentNullException.ThrowIfNull(methodName);
        ObjectDisposedException.ThrowIf(disposed, this);

        return connections
            .Select(connection => connection is null
                ? Observable.Empty<T>()
                : Observable.Create<T>(observer => connection.On<T>(methodName, observer.OnNext)))
            .Switch();
    }

    // ------------------------------------------------------------
    // Send
    // ------------------------------------------------------------

    // Sends if connected (returns false without sending if not connected)
    public ValueTask<bool> TrySendAsync(string methodName, CancellationToken cancellationToken = default) =>
        TrySendAsync(methodName, [], cancellationToken);

    public ValueTask<bool> TrySendAsync(string methodName, object? arg, CancellationToken cancellationToken = default) =>
        TrySendAsync(methodName, [arg], cancellationToken);

    public async ValueTask<bool> TrySendAsync(string methodName, object?[] args, CancellationToken cancellationToken = default)
    {
        var connection = GetConnectedConnection();
        if (connection is null)
        {
            return false;
        }

        await connection.SendCoreAsync(methodName, args, cancellationToken).ConfigureAwait(false);
        return true;
    }

    // Invokes if connected (returns false without invoking if not connected)
    public ValueTask<bool> TryInvokeAsync(string methodName, CancellationToken cancellationToken = default) =>
        TryInvokeAsync(methodName, [], cancellationToken);

    public ValueTask<bool> TryInvokeAsync(string methodName, object? arg, CancellationToken cancellationToken = default) =>
        TryInvokeAsync(methodName, [arg], cancellationToken);

    public async ValueTask<bool> TryInvokeAsync(string methodName, object?[] args, CancellationToken cancellationToken = default)
    {
        var connection = GetConnectedConnection();
        if (connection is null)
        {
            return false;
        }

        await connection.InvokeCoreAsync(methodName, args, cancellationToken).ConfigureAwait(false);
        return true;
    }

    // Invokes and returns the result (throws InvalidOperationException if not connected)
    public ValueTask<TResult> InvokeAsync<TResult>(string methodName, CancellationToken cancellationToken = default) =>
        InvokeAsync<TResult>(methodName, [], cancellationToken);

    public ValueTask<TResult> InvokeAsync<TResult>(string methodName, object? arg, CancellationToken cancellationToken = default) =>
        InvokeAsync<TResult>(methodName, [arg], cancellationToken);

    public async ValueTask<TResult> InvokeAsync<TResult>(string methodName, object?[] args, CancellationToken cancellationToken = default)
    {
        var connection = GetConnectedConnection() ?? throw new InvalidOperationException("The hub is not connected.");
        return await connection.InvokeCoreAsync<TResult>(methodName, args, cancellationToken).ConfigureAwait(false);
    }

    private HubConnection? GetConnectedConnection()
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        return connections.TryGetValue(out var connection) && (connection?.State == HubConnectionState.Connected) ? connection : null;
    }

    // ------------------------------------------------------------
    // Session
    // ------------------------------------------------------------

    // One subscription of Connect() = lifetime of one HubConnection
    private sealed class Session : IDisposable
    {
        // Stop of the observation (completes the observer)
        private readonly Subject<Unit> stop = new();

        private readonly TaskCompletionSource stopped = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private IDisposable? subscription;

        public HubConnection Connection { get; }

        // Completes when the connection has stopped and been disposed
        public Task Stopped => stopped.Task;

        // Whether the status is published (guarded by the lock of the owner)
        public bool Active { get; set; } = true;

        public Session(HubConnection connection)
        {
            Connection = connection;
        }

        public void Start(IObservable<HubStatus> source, IObserver<HubStatus> observer)
        {
            subscription = source.TakeUntil(stop).Subscribe(observer);
        }

        // Stops the observation (the observer completes), then stops and disposes the connection
        public void Dispose()
        {
            stop.OnNext(Unit.Default);
            stop.Dispose();
            subscription?.Dispose();
            _ = StopAsync();
        }

        private async Task StopAsync()
        {
            try
            {
                await Connection.StopAsync().ConfigureAwait(false);
                await Connection.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is InvalidOperationException or IOException or WebSocketException)
            {
                // Ignore failures while disconnecting
            }
            finally
            {
                stopped.SetResult();
            }
        }
    }

    // Automatic reconnect policy (never gives up, the interval is capped at the last delay)
    private sealed class BackoffRetryPolicy : IRetryPolicy
    {
        private readonly IReadOnlyList<TimeSpan> delays;

        public BackoffRetryPolicy(IReadOnlyList<TimeSpan> delays)
        {
            this.delays = delays;
        }

        public TimeSpan? NextRetryDelay(RetryContext retryContext) =>
            delays[(int)Math.Min(retryContext.PreviousRetryCount, delays.Count - 1)];
    }
}
