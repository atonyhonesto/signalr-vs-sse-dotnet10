using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Http.Json;
using System.Net.ServerSentEvents;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR.Client;
using RaceFeed.Server;

namespace RaceFeed.Demo;

/// <summary>An SSE client is just an HTTP GET whose body never ends, parsed with System.Net.ServerSentEvents.</summary>
public static class SseClient
{
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    public static async Task<(List<LapEvent> Laps, string? ContentType)> ReadAsync(
        HttpClient http, string url, int take, string? lastEventId = null, CancellationToken ct = default)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        if (lastEventId is not null) request.Headers.Add("Last-Event-ID", lastEventId);   // what a browser's EventSource sends on reconnect
        using var response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
        response.EnsureSuccessStatusCode();
        await using var stream = await response.Content.ReadAsStreamAsync(ct);
        var laps = new List<LapEvent>();
        var parser = SseParser.Create(stream, (eventType, data) => JsonSerializer.Deserialize<LapEvent>(data, Web)!);
        await foreach (var item in parser.EnumerateAsync(ct))
        {
            laps.Add(item.Data);
            if (laps.Count == take) break;                 // leaving the loop closes the connection
        }
        return (laps, response.Content.Headers.ContentType?.MediaType);
    }
}

/// <summary>A SignalR connection that collects laps for one group.</summary>
public sealed class SignalRWatcher : IAsyncDisposable
{
    private readonly ConcurrentQueue<LapEvent> _laps = new();

    private SignalRWatcher(HubConnection connection) => Connection = connection;

    public HubConnection Connection { get; }
    public IReadOnlyCollection<LapEvent> Laps => _laps;

    public static async Task<SignalRWatcher> ConnectAsync(string baseUrl, string? car)
    {
        var connection = new HubConnectionBuilder().WithUrl($"{baseUrl}/hubs/race").Build();
        var watcher = new SignalRWatcher(connection);
        connection.On<LapEvent>("Lap", lap => watcher._laps.Enqueue(lap));
        await connection.StartAsync();
        await (car is null ? connection.InvokeAsync("WatchAll") : connection.InvokeAsync("WatchCar", car));
        return watcher;
    }

    /// <summary>Client -> server -> client on the same connection.</summary>
    public async Task<(string Car, int PitOnLap, TimeSpan RoundTrip)> RequestPitAsync(string car, int currentLap)
    {
        var reply = new TaskCompletionSource<(string, int)>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var _ = Connection.On<string, int>("PitConfirmed", (c, lap) => reply.TrySetResult((c, lap)));
        var sw = Stopwatch.StartNew();
        await Connection.InvokeAsync("RequestPit", car, currentLap);
        var (confirmedCar, pitLap) = await reply.Task.WaitAsync(TimeSpan.FromSeconds(10));
        return (confirmedCar, pitLap, sw.Elapsed);
    }

    public ValueTask DisposeAsync() => Connection.DisposeAsync();
}

/// <summary>Starts the server on a free port and lets callers wait until every client is subscribed.</summary>
public sealed class RaceHarness : IAsyncDisposable
{
    private RaceHarness(WebApplication app, string url)
    {
        App = app;
        Url = url;
        Http = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
    }

    public WebApplication App { get; }
    public string Url { get; }
    public HttpClient Http { get; }

    public static async Task<RaceHarness> StartAsync(RaceOptions? options = null)
    {
        var app = RaceApp.Build([], options);
        app.Urls.Add("http://127.0.0.1:0");
        await app.StartAsync();
        return new RaceHarness(app, app.Address());
    }

    /// <summary>Wait for N feed subscribers (each SSE client is one; the SignalR bridge is one more).</summary>
    public async Task WaitForSubscribersAsync(int count)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            var status = await Http.GetFromJsonAsync<JsonElement>($"{Url}/race/status");
            if (status.GetProperty("subscribers").GetInt32() >= count) return;
            await Task.Delay(20);
        }
        throw new TimeoutException($"expected {count} subscribers");
    }

    public async Task StartRaceAsync() => (await Http.PostAsync($"{Url}/race/start", null)).EnsureSuccessStatusCode();

    public static async Task WaitUntilAsync(Func<bool> condition, int seconds = 15)
    {
        var deadline = DateTime.UtcNow.AddSeconds(seconds);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("condition not met");
            await Task.Delay(20);
        }
    }

    public async ValueTask DisposeAsync()
    {
        Http.Dispose();
        await App.StopAsync();
        await App.DisposeAsync();
    }
}
