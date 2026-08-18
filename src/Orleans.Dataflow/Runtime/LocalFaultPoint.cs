using Orleans.Dataflow.Authoring;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// One fault point of one run: the arming it was declared with, whatever a test armed it with since, and
/// the accounting of what it has seen and thrown.
/// </summary>
/// <remarks>
/// <para>
/// The same shape every control of this runtime has — one object per materialization, reached by name as
/// soon as the run exists — with one thing none of the others has: it is useful before anybody resolves it.
/// A run starts as soon as it is materialized, so a test that could only arm a fault point through its
/// control would be racing the elements it wanted to fail; the declared arming is what makes "fail the
/// second element" a fact of the graph rather than a matter of scheduling, and the control is for re-arming
/// a run whose elements a test is already pacing.
/// </para>
/// <para>
/// <b>Arming at run time counts from the next arrival</b>, where the declared arming counts from the first
/// of the run. That is the reading a test wants in both cases: a graph says "the second element ever", and a
/// test holding a probe says "the next one".
/// </para>
/// <para>
/// <see cref="Pass"/> runs on the segment's own thread and is the only member that does. Everything else is
/// safe from any thread at any point in the run's life, which is what a control has to be; the counters are
/// read without synchronization and are therefore readings of a moment, to be asserted on once the run has
/// come to rest rather than spun on.
/// </para>
/// <para>
/// A fault point's counter is <b>not</b> stage state. A supervision scope that restarts its stages rebuilds
/// every stage inside it from its own factory, and the factory of a fault point closes over this object
/// rather than making one: the counting a test declared survives a restart, so "fail the second arrival"
/// means the second arrival and not the second arrival since the last restart.
/// </para>
/// </remarks>
/// <param name="mode">The mode this run's fault point starts in, as its document declares.</param>
/// <param name="firstFailure">The one-based arrival the declared mode first throws at.</param>
/// <param name="fault">What to throw, over the one-based position of the arrival that is throwing.</param>
internal sealed class LocalFaultPoint(LocalFaultMode mode, int firstFailure, Func<long, Exception> fault)
{
    private readonly Lock _gate = new();
    private long _seen;
    private long _thrown;
    private LocalFaultMode _mode = mode;
    private long _from = firstFailure;

    /// <summary>Gets the number of elements this fault point has been handed.</summary>
    /// <value>Every arrival, whether it passed or threw; a retry's re-offer is an arrival of its own.</value>
    internal long ElementsSeen => Interlocked.Read(ref _seen);

    /// <summary>Gets the number of times this fault point has thrown.</summary>
    internal long FaultsThrown => Interlocked.Read(ref _thrown);

    /// <summary>Arms this fault point from the next arrival onwards.</summary>
    /// <param name="armed">When to throw.</param>
    /// <param name="firstFailure">
    /// How many arrivals from now the first failing one is; one is the very next element.
    /// </param>
    /// <remarks>
    /// Takes effect at the next element rather than retroactively, exactly as closing a valve does: an
    /// element that has already passed is downstream and is not called back. Arming twice keeps the second
    /// arming and nothing of the first.
    /// </remarks>
    internal void Arm(LocalFaultMode armed, long firstFailure)
    {
        lock (_gate)
        {
            _mode = armed;
            _from = Interlocked.Read(ref _seen) + firstFailure;
        }
    }

    /// <summary>Counts one arrival and throws if this fault point is armed for it.</summary>
    /// <exception cref="Exception">
    /// Whatever the bound factory answered for this arrival, which is the author's own instance and travels
    /// to the run — or to the supervision scope this stage stands inside — unwrapped.
    /// </exception>
    /// <remarks>
    /// The arrival is counted before the arming is read and before anything is thrown, so the count is what
    /// the stage was handed rather than what it let through: a test asserting that a retrying scope offered
    /// one element three times reads three here and two in <see cref="FaultsThrown"/>.
    /// </remarks>
    internal void Pass()
    {
        long arrival = Interlocked.Increment(ref _seen);
        LocalFaultMode armed;
        long from;

        lock (_gate)
        {
            armed = _mode;
            from = _from;
        }

        bool throwing = armed switch
        {
            LocalFaultMode.Once => arrival == from,
            LocalFaultMode.Always => arrival >= from,
            _ => false,
        };

        if (!throwing)
        {
            return;
        }

        _ = Interlocked.Increment(ref _thrown);

        throw fault(arrival);
    }

    /// <summary>Returns a one-line diagnostic summary of this fault point.</summary>
    /// <returns>Text of the form <c>fault point (Once at 2, seen 3, thrown 1)</c>.</returns>
    /// <remarks>Never throws, and answers for a moment that may already have passed.</remarks>
    public override string ToString()
    {
        LocalFaultMode armed;
        long from;

        lock (_gate)
        {
            armed = _mode;
            from = _from;
        }

        return string.Create(
            System.Globalization.CultureInfo.InvariantCulture,
            $"fault point ({armed} at {from}, seen {ElementsSeen}, thrown {FaultsThrown})");
    }
}
