using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;

namespace Orleans.Dataflow.Testing;

/// <summary>
/// The sources a test starts a graph from.
/// </summary>
/// <remarks>
/// <para>
/// A test source is an ordinary source of the local vocabulary: it composes with every operator, buffer,
/// and asynchronous stage the authoring API offers, closes with every sink, and produces a document the
/// stage catalog validates. Nothing about a graph changes because a probe stands at its head — which is
/// the point, since a probe exists to measure the graph an author actually wrote.
/// </para>
/// <para>
/// The factories live on a non-generic companion class beside <see cref="Source"/>, so that the type
/// argument is written only where it cannot be inferred.
/// </para>
/// </remarks>
public static class TestSource
{
    /// <summary>The capacity of the queue behind a probe: room for exactly the element being handed over.</summary>
    /// <remarks>
    /// One and not zero, because a queue of no elements cannot accept one, and not more than one, because
    /// room for a second would let a test run ahead of the graph it is measuring by exactly as many
    /// elements as the room allowed. What makes the handover a rendezvous rather than a buffer of one is
    /// that the emit waits for the run to take the element, not merely for the queue to accept it.
    /// </remarks>
    private const int Handover = 1;

    /// <summary>Starts a source that hands the run elements a test emits, one at a time.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="controlName">The author-stable name to expose the probe under.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="controlName"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="controlName"/> is not a valid result slot identifier.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The probe is a per-run control reached by name, exactly as an ingress queue is: closing the graph
    /// declares a result slot under <paramref name="controlName"/>,
    /// <see cref="RunnableGraph.Control{TControl}"/> turns that name back into a typed
    /// <see cref="ResultSlot{TResult}"/> of <see cref="ISourceProbe{T}"/>, and
    /// <see cref="RunHandle.GetValueAsync{TResult}"/> resolves it against one run. Two runs of one graph
    /// have two probes, and the control resolves at the start of a run rather than at its end, because a
    /// test emits into a run that is already running.
    /// </para>
    /// <para>
    /// What a probe adds to a queue is demand: an emit completes when the run has taken the element, and
    /// <see cref="ISourceProbe{T}.PullsObserved"/> reports how much the run asked for. Together they make
    /// both halves of the demand protocol assertable from the producing end — the test cannot outrun the
    /// run, and the run never receives what was not emitted.
    /// </para>
    /// </remarks>
    public static Source<T> Probe<T>(string controlName)
    {
        ResultSlotId control = LocalOptionGuard.SlotName(controlName, nameof(controlName));
        Func<LocalIngressQueue, object> facade = static queue => new SourceProbe<T>(queue);

        return new Source<T>(LocalStageChain.Of(
            LocalStageDescriptor.Queue(
                LocalOptionGuard.Buffer(new BufferOptions { Capacity = Handover }, nameof(Handover)),
                control,
                typeof(ISourceProbe<T>),
                facade)));
    }
}
