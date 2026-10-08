using RaceFeed.Server;

namespace RaceFeed.Demo;

internal static class DemoProgram
{
    private static async Task Main()
    {
        var options = new RaceOptions();                       // 4 cars x 5 laps, one lap every 40 ms
        var total = options.Cars.Length * options.Laps;
        await using var race = await RaceHarness.StartAsync(options);
        Console.WriteLine($"Race feed on {race.Url}: {options.Cars.Length} cars x {options.Laps} laps\n");

        // Connect everyone before the green flag.
        var sseAll = SseClient.ReadAsync(race.Http, $"{race.Url}/sse/laps", total);
        var sse24 = SseClient.ReadAsync(race.Http, $"{race.Url}/sse/laps?car=24", options.Laps);
        var sseDrops = SseClient.ReadAsync(race.Http, $"{race.Url}/sse/laps", 6);    // will "lose signal" after 6
        await using var hubAll = await SignalRWatcher.ConnectAsync(race.Url, null);
        await using var hub24 = await SignalRWatcher.ConnectAsync(race.Url, "24");
        await race.WaitForSubscribersAsync(4);                 // 3 SSE streams + the SignalR bridge
        await race.StartRaceAsync();

        var (firstPart, _) = await sseDrops;
        var lastId = firstPart[^1].Seq.ToString();
        var (secondPart, _) = await SseClient.ReadAsync(race.Http, $"{race.Url}/sse/laps", total - firstPart.Count, lastId);
        var resumed = firstPart.Concat(secondPart).Select(l => l.Seq).ToList();

        var (all, contentType) = await sseAll;
        var (only24, _) = await sse24;
        await RaceHarness.WaitUntilAsync(() => hubAll.Laps.Count == total && hub24.Laps.Count == options.Laps);
        var pit = await hub24.RequestPitAsync("24", 3);

        Console.WriteLine("Transport  Client                Laps  Notes");
        Console.WriteLine($"SSE        all cars              {all.Count,4}  {contentType}, event ids {all[0].Seq}..{all[^1].Seq}");
        Console.WriteLine($"SSE        ?car=24               {only24.Count,4}  filtered on the server: cars {string.Join(",", only24.Select(l => l.Car).Distinct())}");
        Console.WriteLine($"SSE        dropped and resumed   {firstPart.Count,2}+{secondPart.Count,-2} reconnected with Last-Event-ID {lastId}: " +
                          $"{(resumed.SequenceEqual(Enumerable.Range(1, total).Select(i => (long)i)) ? "no gaps, no duplicates" : "GAP")}");
        Console.WriteLine($"SignalR    group all             {hubAll.Laps.Count,4}  pushed over the negotiated transport (WebSockets by default)");
        Console.WriteLine($"SignalR    group car-24          {hub24.Laps.Count,4}  cars {string.Join(",", hub24.Laps.Select(l => l.Car).Distinct())}");
        Console.WriteLine($"SignalR    RequestPit(24, 3)        -  server replied PitConfirmed(car {pit.Car}, lap {pit.PitOnLap}) " +
                          "on the same connection: SSE has no client-to-server channel");
    }
}
