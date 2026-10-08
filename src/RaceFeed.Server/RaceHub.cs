using Microsoft.AspNetCore.SignalR;

namespace RaceFeed.Server;

/// <summary>Strongly typed client methods: a typo becomes a compile error, not a silent no-op.</summary>
public interface IRaceClient
{
    Task Lap(LapEvent lap);
    Task PitConfirmed(string car, int pitOnLap);
}

public sealed class RaceHub : Hub<IRaceClient>
{
    public Task WatchAll() => Groups.AddToGroupAsync(Context.ConnectionId, "all");

    public Task WatchCar(string car) => Groups.AddToGroupAsync(Context.ConnectionId, $"car-{car}");

    /// <summary>Client-to-server call: the thing SSE can't do on the same connection.</summary>
    public Task RequestPit(string car, int currentLap) => Clients.Caller.PitConfirmed(car, currentLap + 1);
}

/// <summary>Forwards every new lap from the feed to SignalR groups.</summary>
public sealed class SignalRBridge(LapFeed feed, IHubContext<RaceHub, IRaceClient> hub) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var lap in feed.Subscribe(feed.CurrentSeq, stoppingToken))
        {
            await hub.Clients.Group("all").Lap(lap);
            await hub.Clients.Group($"car-{lap.Car}").Lap(lap);
        }
    }
}
