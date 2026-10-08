using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace RaceFeed.Server;

/// <summary>
/// The single source of laps. Keeps history (so SSE clients can resume with Last-Event-ID)
/// and fans each new lap out to every live subscriber through its own channel.
/// </summary>
public sealed class LapFeed
{
    private readonly Lock _gate = new();
    private readonly List<LapEvent> _history = [];
    private readonly List<Channel<LapEvent>> _subscribers = [];
    private long _seq;

    public long CurrentSeq
    {
        get { lock (_gate) return _seq; }
    }

    public int SubscriberCount
    {
        get { lock (_gate) return _subscribers.Count; }
    }

    public LapEvent Publish(string car, int lap, double lapTimeS)
    {
        lock (_gate)
        {
            var e = new LapEvent(++_seq, car, lap, lapTimeS, DateTimeOffset.UtcNow);
            _history.Add(e);
            foreach (var s in _subscribers) s.Writer.TryWrite(e);
            return e;
        }
    }

    /// <summary>Everything after <paramref name="afterSeq"/>: history first, then live, with no gap between them.</summary>
    public async IAsyncEnumerable<LapEvent> Subscribe(long afterSeq, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var channel = Channel.CreateUnbounded<LapEvent>(new UnboundedChannelOptions { SingleReader = true });
        List<LapEvent> replay;
        lock (_gate)                                  // snapshot + register atomically: nothing slips between
        {
            replay = _history.Where(e => e.Seq > afterSeq).ToList();
            _subscribers.Add(channel);
        }
        try
        {
            foreach (var e in replay) yield return e;
            await foreach (var e in channel.Reader.ReadAllAsync(ct)) yield return e;
        }
        finally
        {
            lock (_gate) _subscribers.Remove(channel);
        }
    }
}
