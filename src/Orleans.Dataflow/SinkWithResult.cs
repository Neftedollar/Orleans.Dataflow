using System.Globalization;
using Orleans.Dataflow.Authoring;

namespace Orleans.Dataflow;

/// <summary>
/// A reusable description of what terminates a graph, declaring one result.
/// </summary>
/// <typeparam name="TIn">The element type the sink consumes.</typeparam>
/// <typeparam name="TResult">The type of the result the sink declares.</typeparam>
/// <remarks>
/// <para>
/// The carrier is what keeps the result type out of <see cref="RunnableGraph"/> and out of every stream
/// shape, per ADR 0004 section 3. The result type travels on the sink, is picked up by <c>To</c>, and ends
/// on the <see cref="ResultSlot{TResult}"/> that closing the graph produces.
/// </para>
/// <para>
/// This type is not a <see cref="Sink{TIn}"/>. It converts to one explicitly, through
/// <see cref="ToSink"/> or the equivalent cast, and the conversion discards the result declaration. An
/// implicit conversion would let a result vanish silently, which is exactly the accident the mandatory slot
/// name on the result-bearing <c>To</c> overloads exists to prevent.
/// </para>
/// <para>
/// Attaching one carrier to two graphs declares two slots, one per graph and one per name; a slot belongs
/// to the document that declared it, never to the sink value.
/// </para>
/// </remarks>
public sealed class SinkWithResult<TIn, TResult>
{
    /// <summary>Initializes a new instance of the <see cref="SinkWithResult{TIn, TResult}"/> class.</summary>
    /// <param name="stages">The occurrences this sink contributes, in authoring order.</param>
    internal SinkWithResult(IReadOnlyList<LocalStageDescriptor> stages) => Stages = stages;

    /// <summary>Gets the occurrences this sink contributes to a graph, in authoring order.</summary>
    internal IReadOnlyList<LocalStageDescriptor> Stages { get; }

    /// <summary>Converts a result-bearing sink into a plain one, discarding the result declaration.</summary>
    /// <param name="sink">The sink to convert.</param>
    /// <returns>A sink that consumes the same elements and declares no result.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sink"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// Explicit, so that discarding a result is something the author wrote rather than something overload
    /// resolution did. <see cref="ToSink"/> is the same operation spelled as a method.
    /// </remarks>
    public static explicit operator Sink<TIn>(SinkWithResult<TIn, TResult> sink)
    {
        ArgumentNullException.ThrowIfNull(sink);

        return sink.ToSink();
    }

    /// <summary>Returns this sink without its result declaration.</summary>
    /// <returns>A sink that consumes the same elements and declares no result.</returns>
    /// <remarks>
    /// The graph still runs the fold; it simply exposes no slot for the final state, so nothing can ask for
    /// it. This is the named form of the explicit conversion.
    /// </remarks>
    public Sink<TIn> ToSink() => new(Stages);

    /// <summary>Returns a one-line diagnostic summary of this sink.</summary>
    /// <returns>Text of the form <c>sink with result (1 stages)</c>.</returns>
    /// <remarks>The count is formatted with the invariant culture, and the method never throws.</remarks>
    public override string ToString() =>
        string.Create(CultureInfo.InvariantCulture, $"sink with result ({Stages.Count} stages)");
}
