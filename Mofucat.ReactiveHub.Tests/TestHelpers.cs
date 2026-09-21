namespace Mofucat.ReactiveHub;

using System.Reactive.Concurrency;
using System.Threading.Channels;

using Microsoft.Reactive.Testing;

public static class TestSettings
{
    public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
}

// Records notifications and lets the test await them one by one
public sealed class Recorder<T> : IObserver<T>, IDisposable
{
    private readonly Channel<T> channel = Channel.CreateUnbounded<T>();

    private readonly TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task Completed => completed.Task.WaitAsync(TestSettings.Timeout);

    public void Dispose()
    {
        channel.Writer.TryComplete();
    }

    public void OnNext(T value) => channel.Writer.TryWrite(value);

    public void OnError(Exception error)
    {
        completed.TrySetException(error);
        channel.Writer.TryComplete(error);
    }

    public void OnCompleted()
    {
        completed.TrySetResult();
        channel.Writer.TryComplete();
    }

    public async Task<T> NextAsync()
    {
        using var cts = new CancellationTokenSource(TestSettings.Timeout);
        return await channel.Reader.ReadAsync(cts.Token);
    }

    // Returns default if nothing arrives within the wait
    public async Task<T?> NextOrDefaultAsync(TimeSpan wait)
    {
        using var cts = new CancellationTokenSource(wait);
        try
        {
            return await channel.Reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            return default;
        }
    }
}

public static class RecorderExtensions
{
    // Skips until the status of the kind arrives
    public static async Task<HubStatus> WaitForAsync(this Recorder<HubStatus> recorder, HubStatusKind kind)
    {
        while (true)
        {
            var status = await recorder.NextAsync();
            if (status.Kind == kind)
            {
                return status;
            }
        }
    }
}

// TestScheduler that signals when something is scheduled (the test advances the clock only after that)
public sealed class SignalingTestScheduler : TestScheduler, IDisposable
{
    private readonly SemaphoreSlim scheduled = new(0);

    public void Dispose()
    {
        scheduled.Dispose();
    }

    // ReSharper disable once NullnessAnnotationConflictWithJetBrainsAnnotations
    public override IDisposable ScheduleAbsolute<TState>(TState state, long dueTime, Func<IScheduler, TState, IDisposable> action)
    {
        var disposable = base.ScheduleAbsolute(state, dueTime, action);
        scheduled.Release();
        return disposable;
    }

    public async Task WaitScheduledAsync(CancellationToken cancellationToken)
    {
        if (!await scheduled.WaitAsync(TestSettings.Timeout, cancellationToken))
        {
            throw new TimeoutException("Nothing scheduled.");
        }
    }
}
