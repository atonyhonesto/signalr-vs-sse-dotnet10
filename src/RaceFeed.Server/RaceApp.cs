using System.Net.ServerSentEvents;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;

namespace RaceFeed.Server;

public static class RaceApp
{
    public static WebApplication Build(string[] args, RaceOptions? options = null)
    {
        var builder = WebApplication.CreateBuilder(args);
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.Services.AddSingleton<LapFeed>();
        builder.Services.AddSingleton(options ?? new RaceOptions());
        builder.Services.AddSingleton<RaceSimulator>();
        builder.Services.AddSignalR();
        builder.Services.AddHostedService<SignalRBridge>();

        var app = builder.Build();

        // Server-Sent Events: plain HTTP GET, text/event-stream, server -> client only.
        // Without Last-Event-ID a client gets new laps; with it, the laps it missed first.
        app.MapGet("/sse/laps", (LapFeed feed, HttpRequest request, string? car, CancellationToken ct) =>
        {
            var after = long.TryParse(request.Headers["Last-Event-ID"], out var id) ? id : feed.CurrentSeq;
            return TypedResults.ServerSentEvents(Laps(feed.Subscribe(after, ct), car));
        });

        // SignalR: negotiates WebSockets (falls back to SSE or long polling), both directions.
        app.MapHub<RaceHub>("/hubs/race");

        app.MapGet("/race/status", (LapFeed feed) => new { subscribers = feed.SubscriberCount, seq = feed.CurrentSeq });
        app.MapPost("/race/start", (RaceSimulator sim) => sim.Start() ? Results.Accepted() : Results.Conflict());
        return app;
    }

    private static async IAsyncEnumerable<SseItem<LapEvent>> Laps(IAsyncEnumerable<LapEvent> source, string? car)
    {
        await foreach (var lap in source)
        {
            if (car is null || lap.Car == car)
                yield return new SseItem<LapEvent>(lap, "lap") { EventId = lap.Seq.ToString() };
        }
    }

    /// <summary>The address Kestrel actually bound (useful with port 0).</summary>
    public static string Address(this WebApplication app) =>
        app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
}
