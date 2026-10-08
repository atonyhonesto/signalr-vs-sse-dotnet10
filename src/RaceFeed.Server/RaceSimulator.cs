namespace RaceFeed.Server;

public sealed class RaceOptions
{
    public string[] Cars { get; set; } = ["5", "24", "48", "77"];
    public int Laps { get; set; } = 5;
    public int IntervalMs { get; set; } = 40;
}

/// <summary>Publishes laps when the race is started (POST /race/start), so clients can connect first.</summary>
public sealed class RaceSimulator(LapFeed feed, RaceOptions options)
{
    private int _started;

    public bool Start()
    {
        if (Interlocked.Exchange(ref _started, 1) == 1) return false;
        _ = Task.Run(async () =>
        {
            var rng = new Random(7);
            for (var lap = 1; lap <= options.Laps; lap++)
            {
                foreach (var car in options.Cars)
                {
                    feed.Publish(car, lap, Math.Round(40.5 + rng.NextDouble(), 3));
                    await Task.Delay(options.IntervalMs);
                }
            }
        });
        return true;
    }
}
