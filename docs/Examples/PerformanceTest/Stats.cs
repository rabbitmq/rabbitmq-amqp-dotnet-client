using System.Globalization;

namespace PerformanceTest;

public class Stats
{
    private long _consumed;
    private long _published;
    private long _failed;
    private long _accepted;

    private DateTime _start = DateTime.MinValue;
    private DateTime _end = DateTime.MinValue;
    private bool _isRunning = false;

    private static string FormatNumberWithDots(double number)
    {
        return number.ToString("#,0", new CultureInfo("it-IT"));
    }

    public void Start()
    {
        _start = DateTime.Now;
        _isRunning = true;
    }

    public void Stop()
    {
        _isRunning = false;
    }

    public bool IsRunning()
    {
        return _isRunning;
    }

    public long Consumed => Interlocked.Read(ref _consumed);

    public long Published => Interlocked.Read(ref _published);

    public long Failed => Interlocked.Read(ref _failed);

    public long Accepted => Interlocked.Read(ref _accepted);

    public void IncrementConsumed()
    {
        Interlocked.Increment(ref _consumed);
    }

    public void IncrementPublished()
    {
        Interlocked.Increment(ref _published);
    }

    public void IncrementFailed()
    {
        Interlocked.Increment(ref _failed);
    }

    public void IncrementAccepted()
    {
        Interlocked.Increment(ref _accepted);
    }

    public double AcceptedPerSeconds()
    {
        return Accepted / (_end - _start).TotalSeconds;
    }

    public double ConsumedPerSeconds()
    {
        return Consumed / (_end - _start).TotalSeconds;
    }

    public double PublishedPerSeconds()
    {
        return Published / (_end - _start).TotalSeconds;
    }

    public string Report(bool full = false)
    {
        if (_isRunning)
        {
            _end = DateTime.Now;
        }

        string report = $"Published: {FormatNumberWithDots(PublishedPerSeconds())} msg/s, " +
                        $"Accepted: {FormatNumberWithDots(AcceptedPerSeconds())} msg/s," +
                        $"Consumed: {FormatNumberWithDots(ConsumedPerSeconds())} msg/s - ";

        if (full)
        {
            report += $" {{ Published: {FormatNumberWithDots(Published)}," +
                      $"Accepted: {FormatNumberWithDots(Accepted)}, " +
                      $"Consumed: {FormatNumberWithDots(Consumed)}," +
                      $"Failed: {FormatNumberWithDots(Failed)} in {(_end - _start).TotalSeconds} seconds }}";
        }

        return report;
    }
}
