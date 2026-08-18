namespace Orleans.Dataflow.Testing;

/// <summary>
/// The runtime control of one fault point of one run: the switch a test re-arms, and the accounting of what
/// the point has seen and thrown.
/// </summary>
/// <remarks>
/// <para>
/// Resolved by name from <see cref="RunHandle.GetValueAsync{TResult}"/> like every other control, and
/// available as soon as the run exists rather than when it ends. What separates it from every other control
/// is that the stage it belongs to is already doing its job before anybody resolves it: a run starts as soon
/// as it is materialized, so the arming a test wrote into the graph is what makes "fail the second element"
/// a fact rather than a race, and this control is for the second half of a test — re-arming a run whose
/// elements the test is already pacing through a source probe.
/// </para>
/// <para>
/// <b>Re-arming counts from the next arrival</b>, where the declared arming counts from the first of the
/// run. That is the reading a test wants in both places: a graph says "the second element ever", and a test
/// holding a probe says "the next one".
/// </para>
/// <para>
/// Every member is safe to call from any thread at any point in the run's life. The two counters are
/// readings of a moment rather than synchronization points: assert on them once the run has come to rest,
/// and never spin on them.
/// </para>
/// </remarks>
public interface IFaultPoint
{
    /// <summary>Gets the number of elements this fault point has been handed.</summary>
    /// <value>Every arrival, whether it passed or threw.</value>
    /// <remarks>
    /// A retrying supervision scope re-offers the element to its first stage, so a re-offer is an arrival of
    /// its own: a scope that offered one element three times leaves three here. That is what makes "the
    /// scope really did retry" a number rather than an inference.
    /// </remarks>
    long ElementsSeen { get; }

    /// <summary>Gets the number of times this fault point has thrown.</summary>
    /// <value>One per arrival its arming named, whatever happened to the failure afterwards.</value>
    /// <remarks>
    /// Counted where the failure was raised and not where it was answered, so a supervision scope that
    /// contained three failures and a run that reported one both leave three here.
    /// </remarks>
    long FaultsThrown { get; }

    /// <summary>Arms this fault point from the next arrival onwards.</summary>
    /// <param name="mode">When to throw.</param>
    /// <param name="firstFailure">
    /// How many arrivals from now the first failing one is; one, the default, is the very next element.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="mode"/> is not a declared member of its enumeration, or
    /// <paramref name="firstFailure"/> is below one.
    /// </exception>
    /// <remarks>
    /// Takes effect at the next element rather than retroactively, exactly as closing a valve does: an
    /// element that has already passed is downstream and is not called back. Arming twice keeps the second
    /// arming and nothing of the first, and arming a fault point of a run that has stopped does nothing at
    /// all.
    /// </remarks>
    void Arm(FaultPointMode mode, long firstFailure = 1);

    /// <summary>Stops this fault point throwing, from the next arrival onwards.</summary>
    /// <remarks>
    /// <see cref="Arm"/> with <see cref="FaultPointMode.Never"/>, spelled so that a test healing a point
    /// reads as healing one. Idempotent.
    /// </remarks>
    void Disarm();
}
