namespace Orleans.Dataflow.Runtime;

/// <summary>
/// One fused segment of a compiled plan: where its elements come from, what it does to each of them, where
/// they go, and whether it ends a branch of the graph.
/// </summary>
/// <remarks>
/// <para>
/// A segment is one loop on one thread. Adjacent synchronous stages are fused into
/// <see cref="Stages"/> and applied to an element one after another with nothing between them, which is
/// exactly the checkpoint 1 execution model; a chain with no boundary in it compiles to one segment and
/// runs precisely as it did before buffers existed, whether it is the whole graph or one branch of one.
/// </para>
/// <para>
/// At most one of <see cref="Elements"/>, <see cref="Async"/>, and <see cref="FanOut"/> is set, and only
/// for the segments that have a head of their own: the segment at the head of the graph pulls from a
/// sequence, a segment that begins at an asynchronous stage drives that stage, a junction segment is the
/// junction and nothing else, and every other segment simply reads its input channel.
/// <see cref="Terminal"/> is set on a segment that ends a branch and only when the sink there has something
/// to do with an element.
/// </para>
/// <para>
/// <see cref="Inputs"/> and <see cref="Outputs"/> are what make the plan a graph rather than a line. A
/// segment's position in the plan named both of them when a plan was one chain; now a segment says which
/// channels it reads and which it writes, the head reads none, an ending writes none, and a junction writes
/// several. Nothing else about a channel changed — the policies, the offer discipline, and the closing on
/// completion are the boundary machinery they always were, and only the way one is found is new.
/// </para>
/// </remarks>
internal sealed class LocalSegment
{
    /// <summary>Initializes a new instance of the <see cref="LocalSegment"/> class.</summary>
    /// <param name="elements">The factory of the sequence to pull from, or <see langword="null"/>.</param>
    /// <param name="async">The asynchronous stage that heads this segment, or <see langword="null"/>.</param>
    /// <param name="fanOut">The junction this segment is, or <see langword="null"/>.</param>
    /// <param name="stages">The fused synchronous stages, in flow order.</param>
    /// <param name="terminal">What the branch's terminal does with an element, or <see langword="null"/>.</param>
    /// <param name="inputs">The channels this segment reads, which is one of them or none.</param>
    /// <param name="outputs">The channels this segment writes, which is none, one, or a junction's legs.</param>
    /// <param name="ending">The ending this segment settles, or minus one when it is not the end of a branch.</param>
    internal LocalSegment(
        LocalSource? elements,
        LocalAsyncStage? async,
        LocalFanOut? fanOut,
        IReadOnlyList<LocalElementStage> stages,
        LocalTerminal? terminal,
        IReadOnlyList<int> inputs,
        IReadOnlyList<int> outputs,
        int ending)
    {
        Elements = elements;
        Async = async;
        FanOut = fanOut;
        Stages = stages;
        Terminal = terminal;
        Inputs = inputs;
        Outputs = outputs;
        Ending = ending;
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

    /// <summary>Gets the junction this segment is.</summary>
    /// <value>The strategy, or <see langword="null"/> for every segment that is not a junction.</value>
    /// <remarks>
    /// A junction never fuses with anything, so a segment that has one has no stages and no terminal: its
    /// whole work is to read one channel and place what it read into several, under the rule its strategy
    /// states. That is why the junction is here and not in <see cref="Stages"/> — a fused stage is a
    /// function from an element to an element, and a junction is a shape of loop.
    /// </remarks>
    internal LocalFanOut? FanOut { get; }

    /// <summary>Gets the fused synchronous stages this segment applies, in flow order.</summary>
    /// <value>
    /// The mappings and filters, which is empty for a segment that only moves elements from its head to
    /// its terminal or to the next boundary, and for every junction.
    /// </value>
    internal IReadOnlyList<LocalElementStage> Stages { get; }

    /// <summary>Gets what the branch's terminal does with an element that reaches it.</summary>
    /// <value>
    /// The terminal on a segment that ends a branch, or <see langword="null"/> when this segment feeds
    /// something below it, when the terminal discards its elements, and when the terminal is an
    /// asynchronous callback sink — whose work is the callback <see cref="Async"/> already drives, leaving
    /// nothing to do with the nothing it emits.
    /// </value>
    internal LocalTerminal? Terminal { get; }

    /// <summary>Gets the channels this segment reads from.</summary>
    /// <value>
    /// The one channel a downstream segment reads, or an empty list for the segment at the head of the
    /// graph, which reads a sequence instead.
    /// </value>
    /// <remarks>
    /// A list rather than a single value because the fan-in pumps read several, and because the propagation
    /// of a completed stream walks exactly this list: a segment that stops closes every channel it was
    /// reading, which is what releases a producer parked in a full one.
    /// </remarks>
    internal IReadOnlyList<int> Inputs { get; }

    /// <summary>Gets the channels this segment writes into.</summary>
    /// <value>
    /// One channel for an ordinary segment, one per leg for a junction, and an empty list for a segment
    /// that ends a branch.
    /// </value>
    internal IReadOnlyList<int> Outputs { get; }

    /// <summary>Gets the ending this segment settles.</summary>
    /// <value>
    /// The position of this branch's <see cref="LocalEnding"/> in the plan, or minus one for every segment
    /// that has something below it.
    /// </value>
    /// <remarks>
    /// Every segment that writes into no channel has one, including the segment of a sink that discards its
    /// elements and therefore has no <see cref="Terminal"/> at all: an ending is a place a branch stops,
    /// and whether anything is accumulated there is a separate question.
    /// </remarks>
    internal int Ending { get; }
}
