using RaceFeed.Demo;
using RaceFeed.Server;

namespace RaceFeed.Tests;

public class FeedTests
{
    private static readonly RaceOptions Options = new() { Cars = ["5", "24"], Laps = 4, IntervalMs = 20 };
    private const int Total = 8;

    [Fact]
    public async Task Sse_streams_every_lap_with_event_ids()
    {
        await using var race = await RaceHarness.StartAsync(Options);
        var read = SseClient.ReadAsync(race.Http, $"{race.Url}/sse/laps", Total);
        await race.WaitForSubscribersAsync(2);
        await race.StartRaceAsync();
        var (laps, contentType) = await read;
        Assert.Equal("text/event-stream", contentType);
        Assert.Equal(Enumerable.Range(1, Total).Select(i => (long)i), laps.Select(l => l.Seq));
    }

    [Fact]
    public async Task Sse_filters_on_the_server()
    {
        await using var race = await RaceHarness.StartAsync(Options);
        var read = SseClient.ReadAsync(race.Http, $"{race.Url}/sse/laps?car=24", Options.Laps);
        await race.WaitForSubscribersAsync(2);
        await race.StartRaceAsync();
        var (laps, _) = await read;
        Assert.All(laps, l => Assert.Equal("24", l.Car));
        Assert.Equal(new[] { 1, 2, 3, 4 }, laps.Select(l => l.Lap));
    }

    [Fact]
    public async Task Sse_resume_with_last_event_id_has_no_gaps()
    {
        await using var race = await RaceHarness.StartAsync(Options);
        var first = SseClient.ReadAsync(race.Http, $"{race.Url}/sse/laps", 3);
        await race.WaitForSubscribersAsync(2);
        await race.StartRaceAsync();
        var (part1, _) = await first;
        var (part2, _) = await SseClient.ReadAsync(race.Http, $"{race.Url}/sse/laps", Total - 3, part1[^1].Seq.ToString());
        Assert.Equal(Enumerable.Range(1, Total).Select(i => (long)i), part1.Concat(part2).Select(l => l.Seq));
    }

    [Fact]
    public async Task SignalR_groups_and_client_to_server_calls()
    {
        await using var race = await RaceHarness.StartAsync(Options);
        await using var all = await SignalRWatcher.ConnectAsync(race.Url, null);
        await using var car5 = await SignalRWatcher.ConnectAsync(race.Url, "5");
        await race.WaitForSubscribersAsync(1);
        await race.StartRaceAsync();
        await RaceHarness.WaitUntilAsync(() => all.Laps.Count == Total && car5.Laps.Count == Options.Laps);
        Assert.All(car5.Laps, l => Assert.Equal("5", l.Car));

        var pit = await car5.RequestPitAsync("5", 2);
        Assert.Equal(("5", 3), (pit.Car, pit.PitOnLap));
    }

    [Fact]
    public async Task Race_can_only_start_once()
    {
        await using var race = await RaceHarness.StartAsync(Options);
        await race.StartRaceAsync();
        var again = await race.Http.PostAsync($"{race.Url}/race/start", null);
        Assert.Equal(System.Net.HttpStatusCode.Conflict, again.StatusCode);
    }
}
