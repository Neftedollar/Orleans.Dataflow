namespace Orleans.Dataflow.Runtime;

/// <summary>
/// One fused segment of a compiled plan: where its elements come from, what it does to each of them, and
/// whether it terminates the run.
/// </summary>
/// <remarks>
/// <para>
/// A segment is one loop on one thread. Adjacent synchronous stages are fused into
/// <see cref="Stages"/> and applied to an element one after another with nothing between them, which is
/// exactly the checkpoint 1 execution model; a chain with no boundary in it compiles to one segment and
/// runs precisely as it did before buffers existed.
/// </para>
/// <para>
/// Exactly one of <see cref="Elements"/> and <see cref="Async"/> is set, and only for the segments that
/// have a head of their own: the first segment of a plan pulls from a sequence, a segment that begins at
/// an asynchronous stage drives that stage, and every other segment simply reads its input channel.
/// <see cref="Terminal"/> is set on the last segment and only when the graph's terminal has something to
/// do with an element.
/// </para>
/// </remarks>
internal sealed class LocalSegment
{
    /// <summary>Initializes a new instance of the <see cref="LocalSegment"/> class.</summary>
    /// <param name="elements">The factory of the sequence to pull from, or <see langword="null"/>.</param>
    /// <param name="async">The asynchronous stage that heads this segment, or <see langword="null"/>.</param>
    /// <param name="stages">The fused synchronous stages, in flow order.</param>
    /// <param name="terminal">What the graph's terminal does with an element, or <see langword="null"/>.</param>
    internal LocalSegment(
        LocalSource? elements,
        LocalAsyncStage? async,
        IReadOnlyList<LocalElementStage> stages,
        LocalTerminal? terminal)
    {
        Elements = elements;
        Async = async;
        Stages = stages;
        Terminal = terminal;
    }

    /// <summary>Gets the factory of the sequence this segment pulls from.</summary>
    /// <value>
    /// The factory that opens the source for one run — the very sequence the author handed to
    /// <see cref="Source.From{T}"/>, or an enumeration built for this run over a queue, a channel, or an
    /// asynchronous sequence; <see langword="null"/> for every segment that reads a boundary instead.
    /// </value>
    /// <remarks>
    /// Nothing is opened here. A run invokes the factory and obtains its own enumerator at its first pull,
    /// which is what makes two materializations of one graph two independent enumerations and what keeps a
    /// run stopped before its first element from touching its source at all.
    /// </remarks>
    internal LocalSource? Elements { get; }

    /// <summary>Gets the asynchronous stage that heads this segment.</summary>
    /// <value>The stage, or <see langword="null"/> when this segment has no asynchronous head.</value>
    internal LocalAsyncStage? Async { get; }

    /// <summary>Gets the fused synchronous stages this segment applies, in flow order.</summary>
    /// <value>
    /// The mappings and filters, which is empty for a segment that only moves elements from its head to
    /// its terminal or to the next boundary.
    /// </value>
    internal IReadOnlyList<LocalElementStage> Stages { get; }

    /// <summary>Gets what the graph's terminal does with an element that reaches it.</summary>
    /// <value>
    /// The terminal on the last segment of a plan, or <see langword="null"/> when this is not the last
    /// segment, when the terminal discards its elements, and when the terminal is an asynchronous callback
    /// sink — whose work is the callback <see cref="Async"/> already drives, leaving nothing to do with the
    /// nothing it emits.
    /// </value>
    internal LocalTerminal? Terminal { get; }
}
