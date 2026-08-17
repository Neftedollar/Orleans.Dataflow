namespace Orleans.Dataflow.Runtime;

/// <summary>
/// What one ingress queue tells a watcher about the run reading it.
/// </summary>
/// <remarks>
/// <para>
/// A queue's own contract is about producers: what an offer answers, and when the queue stops accepting.
/// Neither says anything about the reader, because nothing an ordinary producer does depends on when the
/// run happens to take an element. A rendezvous does: a probe's emit completes when the run has taken the
/// element and not when the queue accepted it, which is the difference between "I handed it over" and "it
/// is in a buffer somewhere".
/// </para>
/// <para>
/// Both members are called from the run's own thread, at most one at a time, and an implementation must
/// therefore return promptly and never throw: it is running inside the pull loop of a live run.
/// <see cref="Ended"/> may arrive more than once, because a run learns twice that it will read no more —
/// once where its segment stops and once where it settles — and an implementation is expected to be
/// idempotent rather than to be called carefully.
/// </para>
/// </remarks>
internal interface ILocalQueueObserver
{
    /// <summary>Reports that the run has taken one element out of the queue.</summary>
    void Taken();

    /// <summary>Reports that the run will never take another.</summary>
    void Ended();
}
