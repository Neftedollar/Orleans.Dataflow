namespace Orleans.Dataflow.Testing;

/// <summary>
/// The runtime control of one marking sink of one run: how far its side effect has actually got.
/// </summary>
/// <remarks>
/// <para>
/// A sink's commit mark, read from a test. It is the number a checkpoint stores for this sink, so
/// what a test asserts here is exactly what a resume of the run would find written down — which is the whole
/// point of exposing it rather than leaving a test to count callbacks itself.
/// </para>
/// <para>
/// <b>The mark advances after the callback and never before it.</b> A callback that throws leaves the mark
/// where it was, so the number always describes work that finished. That direction is not a detail: a mark
/// that moved first would make a resume skip an element whose commit never happened, and a duplicate window
/// would become a loss window.
/// </para>
/// <para>
/// <b>It counts committed deliveries, not distinct elements.</b> The two agree for a graph that neither
/// drops nor multiplies elements between a source and this sink; they part company across a resume, because
/// a replayed element is a second delivery of one element, and the mark is restored across a resume so that
/// the count is the run's rather than the attempt's.
/// </para>
/// <para>
/// Resolved by name from <see cref="RunHandle.GetValueAsync{TResult}"/> like every other control, and
/// available as soon as the run exists. Safe to read from any thread; it is a reading of a moment rather
/// than a synchronization point, so assert on it once the run has come to rest and never spin on it.
/// </para>
/// </remarks>
public interface IMarkingSink
{
    /// <summary>Gets how many elements this sink's side effect has completed for.</summary>
    /// <value>The running count across the run and every resume of it.</value>
    long Mark { get; }
}
