namespace Orleans.Dataflow.Benchmarks;

/// <summary>Builds one measured graph over a probe's numbers.</summary>
/// <param name="elements">How many elements the graph carries.</param>
/// <param name="probe">The source of the numbers, which samples the heap as it emits them.</param>
/// <returns>The closed graph.</returns>
internal delegate RunnableGraph GraphFactory(long elements, HeapProbe probe);

/// <summary>
/// One shape the harness measures, with the bound it is measured against.
/// </summary>
/// <param name="Name">What the scenario is called, in the output and on the command line.</param>
/// <param name="Bound">What limits the memory this shape holds, in one phrase.</param>
/// <param name="Build">Builds the graph.</param>
/// <param name="Ceiling">
/// The most elements this shape may be asked for whatever the command line says, or zero for no limit.
/// </param>
/// <remarks>
/// The bound is carried beside the shape rather than written only in the documentation because it is the
/// thing the number has to be read against: a peak heap of a megabyte means nothing on its own and means
/// everything beside "one element in flight" or "a declared capacity of 1024".
/// </remarks>
internal sealed record GraphScenario(string Name, string Bound, GraphFactory Build, long Ceiling = 0)
{
    /// <summary>The capacity the buffered scenario declares.</summary>
    internal const int BufferCapacity = 1024;

    /// <summary>The concurrency the asynchronous mapping scenario declares.</summary>
    internal const int MapConcurrency = 4;

    /// <summary>The concurrency the grain-call-shaped sink scenario declares.</summary>
    /// <remarks>
    /// Eight, which is an ordinary <c>maxInFlight</c> for a grain call. What is being measured is the
    /// shape — a terminal that awaits a call per element under a declared concurrency — and the call is
    /// faked locally on purpose: see <see cref="Grade"/>.
    /// </remarks>
    internal const int CallConcurrency = 8;

    /// <summary>How many keys the grouping scenario keeps active.</summary>
    internal const int ActiveKeys = 16;

    /// <summary>The most elements the collecting control is ever asked for.</summary>
    /// <remarks>
    /// A million, and it is the only scenario with a ceiling because it is the only one that keeps what it
    /// is given: at sixty-odd bytes an element, ten million would be a six-hundred-megabyte heap, and a
    /// harness that made the machine swap to prove that a collecting sink collects would be proving it
    /// twice. Every other shape holds a constant, so <c>--elements</c> costs them time and nothing else.
    /// The row prints the count it actually ran, so a capped run says so rather than implying otherwise.
    /// </remarks>
    internal const long CollectCeiling = 1_000_000;

    /// <summary>Reports how many elements one scenario runs over when the command line asks for a count.</summary>
    /// <param name="requested">What the command line asked for.</param>
    /// <returns>The count this scenario runs.</returns>
    internal long Elements(long requested) => Ceiling == 0 ? requested : Math.Min(requested, Ceiling);

    /// <summary>What these numbers are worth, printed at the head of every report.</summary>
    /// <remarks>
    /// <para>
    /// Honesty-grade, which is a real grade and not a disclaimer. What the harness is built to answer is
    /// whether the runtime holds a bounded amount of memory under a stream far longer than any bound it
    /// declares, and roughly what it costs to push an element through — the first to within a factor, the
    /// second to within an order of magnitude. It is not built to compare two implementations of the same
    /// stage or to detect a ten-percent regression, and a number from it should never be quoted as if it
    /// were.
    /// </para>
    /// <para>
    /// The grain-call sink is the one shape that is faked. A real one crosses a grain call, and the cost
    /// of an Orleans call in an in-process cluster is a measurement of Orleans, not of this runtime; what
    /// the local fake preserves is the part this runtime owns — a terminal that keeps a declared number of
    /// calls in flight and no more. Cluster memory is not measured here at all, and docs/BENCHMARKS.md
    /// says so in as many words.
    /// </para>
    /// </remarks>
    internal const string Grade =
        "honesty-grade: bounds to within a factor, throughput to within an order of magnitude";

    /// <summary>Gets every scenario the harness measures, in the order it reports them.</summary>
    /// <remarks>
    /// <para>
    /// Six shapes that between them cover every way this runtime is allowed to hold more than one element:
    /// nothing (a fused chain), a declared buffer, a declared concurrency in the middle, a junction that
    /// hands the same element to two consumers, a declared number of live keys, and a terminal that keeps
    /// calls in flight. Every one of them ends in something that discards — a fold to a number, a count —
    /// so that what the peak measures is the runtime's own holding and not the author's.
    /// </para>
    /// <para>
    /// The seventh is the control, and it is the one shape that is <em>supposed</em> to grow: a collecting
    /// sink under a declared maximum. Its peak rises with that maximum and therefore with the run length,
    /// which is what makes the other six worth believing — an instrument that reported "bounded" for
    /// everything would report it for this too.
    /// </para>
    /// </remarks>
    internal static IReadOnlyList<GraphScenario> All { get; } =
    [
        new(
            "fused-chain",
            "one element in flight; the chain declares no boundary",
            static (elements, probe) => Source.From(probe.Numbers(elements))
                .Select(static value => value * 2)
                .Where(static value => value > 0)
                .Select(static value => value + 1)
                .To(static s => s.Aggregate(0L, static (sum, value) => sum + value).ToSink())),
        new(
            "buffered-boundary",
            $"{BufferCapacity} elements; the capacity the author declared",
            static (elements, probe) => Source.From(probe.Numbers(elements))
                .Select(static value => value * 2)
                .Buffer(new BufferOptions { Capacity = BufferCapacity, OverflowPolicy = OverflowPolicy.Backpressure })
                .Select(static value => value + 1)
                .To(static s => s.Aggregate(0L, static (sum, value) => sum + value).ToSink())),
        new(
            "async-map-parallelism-4",
            $"{MapConcurrency} calls in flight; the concurrency the author declared",
            static (elements, probe) => Source.From(probe.Numbers(elements))
                .SelectAsync(
                    new ParallelismOptions { MaxConcurrency = MapConcurrency },
                    static (value, _) => Task.FromResult(value * 2))
                .To(static s => s.Aggregate(0L, static (sum, value) => sum + value).ToSink())),
        new(
            "broadcast-two-sinks",
            "one element per leg; a junction holds an element until every leg has taken it",
            static (elements, probe) => Source.From(probe.Numbers(elements))
                .BroadcastTo(
                    Flow.For<long>().To(static s => s.Aggregate(0L, static (sum, value) => sum + value).ToSink()),
                    Flow.For<long>().To(static s => s.Count().ToSink()))),
        new(
            "bounded-group-by",
            $"{ActiveKeys} live keys; the maximum the author declared, over a stream that never exceeds it",
            static (elements, probe) => Source.From(probe.Numbers(elements))
                .GroupBy(
                    new GroupByOptions { MaxActiveKeys = ActiveKeys, OverflowPolicy = ActiveKeyOverflowPolicy.Fail },
                    static value => value % ActiveKeys,
                    Flow.For<long>().Select(static value => value + 1))
                .To(static s => s.Aggregate(0L, static (sum, value) => sum + value).ToSink())),
        new(
            "grain-call-sink-shape",
            $"{CallConcurrency} calls in flight; the shape of a grain-call sink, faked locally",
            static (elements, probe) => Source.From(probe.Numbers(elements))
                .To(static s => s.ForEachAsync(
                    new ParallelismOptions { MaxConcurrency = CallConcurrency },
                    static (_, _) => Task.CompletedTask))),
        new(
            "declared-collect-control",
            "the declared maximum, which is the run length: this one is meant to grow",
            static (elements, probe) => Source.From(probe.Numbers(elements))
                .To(_ => Sink.Collect<long>(new CollectOptions { MaxElements = checked((int)elements) }).ToSink()),
            CollectCeiling),
    ];
}
