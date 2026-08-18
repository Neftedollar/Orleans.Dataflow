namespace Orleans.Dataflow.Grains;

/// <summary>
/// What one run offers a bridge grain so that something outside the run can reach the run's ingress.
/// </summary>
/// <remarks>
/// <para>
/// An Orleans grain observer, created by the run itself with
/// <see cref="IGrainFactory.CreateObjectReference{TGrainObserverInterface}"/> and handed to a grain that
/// external code can address by name. That indirection is what a bridge is: the ingress is an object in
/// the memory of whichever silo is executing the run, and a grain reference is the only address a caller
/// anywhere in the cluster can hold for it.
/// </para>
/// <para>
/// It is created off any grain context, on the run's own source thread, which is the one place it can be:
/// Orleans refuses <c>CreateObjectReference</c> from inside a grain, and the run's engine threads are not
/// inside one. That was probed rather than assumed, and it is the same fact the phase-2 stream source rests
/// on — a run executes beside the grains of its silo rather than on one of their turns.
/// </para>
/// <para>
/// <b>Best effort, and observably so.</b> Every push answers with what became of it, so a caller learns
/// that a run stopped listening instead of guessing. Nothing here is durable, nothing is replayed, and a
/// reference whose run has ended answers <see cref="DataflowPushOutcome.Closed"/> for as long as the bridge
/// grain lives.
/// </para>
/// </remarks>
public interface IDataflowPushReceiver : IGrainObserver
{
    /// <summary>Offers one element to the run's bounded ingress.</summary>
    /// <param name="element">The element, which must satisfy Orleans serialization.</param>
    /// <returns>What became of the element.</returns>
    /// <remarks>
    /// Under the backpressure policy the returned task does not complete until the run makes room, so the
    /// caller waits exactly as long as the run's own bound implies. Under a dropping policy it completes at
    /// once and says what was dropped.
    /// </remarks>
    Task<DataflowPushOutcome> PushAsync(object? element);
}

/// <summary>
/// What became of one element pushed at a run.
/// </summary>
/// <remarks>
/// The wire form of the engine's own offer outcome, and deliberately a separate type: the engine's is an
/// engine's own public value and this one is part of a grain contract that has to stay stable across a hop.
/// The four cases are the four states an ingress can be in, and none of them is an exception — a producer
/// that had to tell them apart from <c>catch</c> blocks would be writing its control flow in the wrong
/// construct.
/// </remarks>
[GenerateSerializer]
public enum DataflowPushOutcome
{
    /// <summary>The element was admitted to the run's ingress.</summary>
    Accepted = 0,

    /// <summary>The ingress was full and the declared overflow policy discarded this element.</summary>
    Dropped = 1,

    /// <summary>The run is no longer accepting: it ended, was drained, or was never listening.</summary>
    Closed = 2,

    /// <summary>The run's ingress was failed, so the run is ending with that failure.</summary>
    Failed = 3,
}
