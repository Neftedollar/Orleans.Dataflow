namespace Orleans.Dataflow.Hosting;

/// <summary>
/// How a client watches the runs it started.
/// </summary>
/// <remarks>
/// <para>
/// Completion is observed by polling, and this is the one knob that choice needs. Polling is deliberate
/// for phase 1: an observer is best-effort by design in Orleans, so a completion delivered by one would
/// have to be backed by a poll anyway, and a design whose fallback is its whole mechanism is simpler
/// honestly stated than dressed up.
/// </para>
/// <para>
/// The cost is stated rather than hidden: a run's completion is observed up to one poll interval after it
/// happens, and a client watching many runs makes one call per run per interval.
/// </para>
/// </remarks>
public sealed class OrleansDataflowClientOptions
{
    /// <summary>The interval a client polls a run's status at when nothing else is configured.</summary>
    /// <remarks>
    /// Short, because the runs a poll is watching are usually short too, and because a test cluster that
    /// waited a second per completion would spend its time waiting rather than testing. It is a default
    /// rather than a constant precisely so a deployment whose runs last hours can widen it.
    /// </remarks>
    public static readonly TimeSpan DefaultPollInterval = TimeSpan.FromMilliseconds(20);

    private TimeSpan _pollInterval = DefaultPollInterval;

    /// <summary>Gets or sets how often a run's status is polled while waiting for it to end.</summary>
    /// <value>A positive interval; <see cref="DefaultPollInterval"/> unless set.</value>
    /// <exception cref="ArgumentOutOfRangeException">The value is not positive.</exception>
    public TimeSpan PollInterval
    {
        get => _pollInterval;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero);

            _pollInterval = value;
        }
    }
}
