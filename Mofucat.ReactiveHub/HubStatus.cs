namespace Mofucat.ReactiveHub;

public enum HubStatusKind
{
    Disconnected,
    Connecting,
    Connected,
    Reconnecting
}

// Error is the reason of the latest connect failure or disconnection
public sealed record HubStatus(HubStatusKind Kind, string? ConnectionId, Exception? Error);
