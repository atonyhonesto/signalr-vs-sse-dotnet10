namespace RaceFeed.Server;

/// <summary>One completed lap. Seq is the feed's sequence number, used as the SSE event id.</summary>
public sealed record LapEvent(long Seq, string Car, int Lap, double LapTimeS, DateTimeOffset At);
