using System.Collections.Concurrent;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// Authoring values are immutable and reusable, and composing one never disturbs it.
/// </summary>
/// <remarks>
/// <para>
/// This is the property AGENTS.md states as a product boundary and the one that decides whether a flow can
/// be shared across a codebase. The tests are written as before-and-after comparisons of what a value
/// builds, rather than as inspections of what a value holds: a value that still builds byte-identical
/// documents after being composed into three graphs was not modified by any of them.
/// </para>
/// <para>
/// "Immutable" and "thread-safe" are two claims and the second one needs threads. The single-threaded tests
/// establish that composing a value does not disturb it; one test here composes and closes a single shared
/// value from eight threads at once and asks what the four hundred documents are, which is the only shape in
/// which a value with hidden per-instance state actually shows itself.
/// </para>
/// <para>
/// Two graphs built from the same reusable flow number their occurrences from <c>stage-0001</c> independently,
/// so their node identifiers overlap. They are different documents and nothing relates them, so that is not
/// a collision; the fragment algebra's import scoping exists for the case where two copies of one fragment
/// meet inside a single graph, which flat numbering at closure never produces.
/// </para>
/// </remarks>
public sealed class ReuseAndImmutabilityTests
{
    [Fact]
    public void OneFlowInTwoGraphsProducesTwoIndependentDocuments()
    {
        Flow<OrderCreated, OrderDocument> normalize =
            Flow.For<OrderCreated>().Where(order => order.IsValid).Select(OrderDocument.FromEvent);

        Source<OrderCreated> orders = Source.From(OrderEvents);

        RunnableGraph discarded = orders.Via(normalize).To(Sink.Ignore<OrderDocument>());
        RunnableGraph counted = orders
            .Via(normalize)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> _);

        Assert.NotSame(discarded.Document, counted.Document);
        Assert.NotEqual(discarded.Fingerprint, counted.Fingerprint);

        // The node identifiers overlap across the two documents, which is what flat numbering means. The
        // two documents are unrelated, so nothing is ambiguous about it.
        Assert.Equal(["stage-0001", "stage-0002", "stage-0003", "stage-0004"], NodeIds(discarded.Document));
        Assert.Equal(["stage-0001", "stage-0002", "stage-0003", "stage-0004"], NodeIds(counted.Document));
    }

    [Fact]
    public void ComposingASourceLeavesTheSourceUnchanged()
    {
        Source<OrderCreated> orders = Source.From(OrderEvents);

        byte[] before = GraphDocumentSerializer.Serialize(orders.To(Sink.Ignore<OrderCreated>()).Document);

        _ = orders.Where(order => order.IsValid);
        _ = orders.Select(OrderDocument.FromEvent);
        _ = orders.Buffer(new BufferOptions { Capacity = 4, OverflowPolicy = OverflowPolicy.DropOldest });
        _ = orders.SelectAsync(
            new ParallelismOptions { MaxConcurrency = 2 },
            (order, _) => Task.FromResult(order.Total));
        _ = orders.SelectAsyncUnordered(
            new ParallelismOptions { MaxConcurrency = 2 },
            (order, _) => Task.FromResult(order.Total));
        _ = orders.Via(Flow.For<OrderCreated>().Where(order => order.IsValid));
        _ = orders.To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> _);

        byte[] after = GraphDocumentSerializer.Serialize(orders.To(Sink.Ignore<OrderCreated>()).Document);

        Assert.Equal(before, after);
    }

    [Fact]
    public void ComposingAFlowLeavesTheFlowUnchanged()
    {
        Flow<OrderCreated, OrderCreated> valid = Flow.For<OrderCreated>().Where(order => order.IsValid);

        byte[] before = Bytes(valid);

        _ = valid.Select(OrderDocument.FromEvent);
        _ = valid.Where(order => order.Total > 5m);
        _ = valid.Buffer(new BufferOptions { Capacity = 4, OverflowPolicy = OverflowPolicy.Fail });
        _ = valid.SelectAsync(
            new ParallelismOptions { MaxConcurrency = 2 },
            (order, _) => Task.FromResult(order.Total));
        _ = valid.SelectAsyncUnordered(
            new ParallelismOptions { MaxConcurrency = 2 },
            (order, _) => Task.FromResult(order.Total));
        _ = valid.Via(Flow.For<OrderCreated>().Where(order => order.Total > 5m));

        Assert.Equal(before, Bytes(valid));

        static byte[] Bytes(Flow<OrderCreated, OrderCreated> flow) =>
            GraphDocumentSerializer.Serialize(
                Source.From(OrderEvents).Via(flow).To(Sink.Ignore<OrderCreated>()).Document);
    }

    [Fact]
    public void ComposingASinkLeavesTheSinkUnchanged()
    {
        SinkWithResult<OrderCreated, long> counting =
            Sink.Aggregate<OrderCreated, long>(0L, (count, _) => count + 1);

        GraphFingerprint before = Source.From(OrderEvents).To(counting, "processed").Graph.Fingerprint;

        _ = counting.ToSink();
        _ = Source.From(OrderEvents).To(counting, "other");

        Assert.Equal(before, Source.From(OrderEvents).To(counting, "processed").Graph.Fingerprint);
    }

    [Fact]
    public void OneFlowUsedTwiceInOneGraphContributesTwoDistinctOccurrences()
    {
        // Composing the same reusable value twice into one graph is the case that would collide if node
        // identifiers were allocated when a value is created instead of when a graph is closed.
        Flow<OrderCreated, OrderCreated> valid = Flow.For<OrderCreated>().Where(order => order.IsValid);

        RunnableGraph graph = Source.From(OrderEvents)
            .Via(valid)
            .Via(valid)
            .To(Sink.Ignore<OrderCreated>());

        Assert.Equal(["stage-0001", "stage-0002", "stage-0003", "stage-0004"], NodeIds(graph.Document));
        Assert.Equal(["from-enumerable", "where", "where", "ignore"], StageIds(graph.Document));
        Assert.Equal(
            [
                "stage-0001#out -> stage-0002#in",
                "stage-0002#out -> stage-0003#in",
                "stage-0003#out -> stage-0004#in",
            ],
            Edges(graph.Document));
    }

    [Fact]
    public void OneSourceHeadsAnyNumberOfGraphs()
    {
        Source<OrderDocument> head = Source.From(OrderEvents).Select(OrderDocument.FromEvent);

        RunnableGraph[] graphs =
        [
            head.To(Sink.Ignore<OrderDocument>()),
            head.Where(order => order.Total > 5m).To(Sink.Ignore<OrderDocument>()),
            head.To(s => s.Aggregate(0m, (total, order) => total + order.Total), "total", out ResultSlot<decimal> _),
        ];

        Assert.Equal(3, graphs.Distinct().Count());
        Assert.Equal(3, graphs.Select(graph => graph.Fingerprint).Distinct().Count());
    }

    [Fact]
    public async Task OneFlowComposedAndClosedByEightThreadsAtOnceBuildsOneDocumentEveryTime()
    {
        // The other half of the property the tests above establish. They prove that composing a value does
        // not disturb it, one thread at a time; this proves the same thing while several threads are inside
        // the value at once, which is what "instances are thread-safe" actually claims and what a value with
        // hidden per-instance state would fail. Four hundred closures of one shared flow produce four
        // hundred documents, and an immutable value has exactly one answer for what they are.
        Flow<OrderCreated, OrderDocument> normalize =
            Flow.For<OrderCreated>().Where(order => order.IsValid).Select(OrderDocument.FromEvent);

        ConcurrentBag<string> documents = [];
        ConcurrentBag<GraphFingerprint> fingerprints = [];
        ConcurrentBag<Exception> failures = [];

        string alone = Bytes(Closed());
        int inside = 0;
        int peak = 0;

        Parallel.For(0, 8, new ParallelOptions { MaxDegreeOfParallelism = 8 }, _ =>
        {
            try
            {
                for (int iteration = 0; iteration < 50; iteration++)
                {
                    Peak(Interlocked.Increment(ref inside));

                    try
                    {
                        RunnableGraph graph = Closed();

                        documents.Add(Bytes(graph));
                        fingerprints.Add(graph.Fingerprint);
                    }
                    finally
                    {
                        _ = Interlocked.Decrement(ref inside);
                    }
                }
            }
            catch (Exception failure)
            {
                // Recorded rather than thrown, so that a concurrency defect is reported as the exception it
                // was instead of as an AggregateException the assertion below would never reach.
                failures.Add(failure);
            }
        });

        Assert.Empty(failures);
        Assert.Equal(400, documents.Count);

        // Measured rather than assumed, because a scheduler that had run the eight workers one after another
        // would make everything below true of a value nothing was ever concurrent with. How many overlapped
        // is the scheduler's business and is deliberately not asserted; that some did is what this test is.
        Assert.True(peak > 1, $"the eight workers never overlapped, so nothing here was concurrent; peak was {peak}.");

        // Byte-identical, not merely equivalent: the composition is the only thing that ran concurrently, so
        // a second distinct document would be one thread having observed another's half-written state. And
        // the one document is the one a single thread builds, before and after the storm, which is this
        // file's before-and-after comparison asked across eight threads instead of one.
        Assert.Equal(alone, Assert.Single(documents.Distinct()));
        Assert.Equal(alone, Bytes(Closed()));
        Assert.Single(fingerprints.Distinct());

        // And the same shared value materialized three times over, because a blueprint that were secretly
        // per-run would show up here rather than in the bytes. All three runs are started before any of them
        // is awaited, and each resolves its own number: whether the three overlapped is the scheduler's
        // business, but three results that are three different values cannot have come from shared state.
        LocalDataflowHost host = new();
        IReadOnlyList<OrderCreated>[] batches =
        [
            [new("a-1", 1m)],
            [new("b-1", 2m), new("b-2", 3m)],
            [new("c-1", 4m), new("c-2", 5m), new("c-3", 6m)],
        ];
        RunnableGraph[] graphs = new RunnableGraph[batches.Length];
        ResultSlot<decimal>[] totals = new ResultSlot<decimal>[batches.Length];

        for (int batch = 0; batch < batches.Length; batch++)
        {
            graphs[batch] = Source.From(batches[batch])
                .Via(normalize)
                .To(s => s.Aggregate(0m, (total, document) => total + document.Total), "total", out totals[batch]);
        }

        CancellationToken token = TestContext.Current.CancellationToken;
        RunHandle[] runs = await Task.WhenAll(graphs.Select(graph => host.MaterializeAsync(graph, token).AsTask()));

        try
        {
            await Task.WhenAll(runs.Select(run => run.Completion));

            Assert.Equal(
                [1m, 5m, 15m],
                await Task.WhenAll(runs.Select((run, index) => run.GetValueAsync(totals[index], token))));
        }
        finally
        {
            foreach (RunHandle run in runs)
            {
                await run.DisposeAsync();
            }
        }

        RunnableGraph Closed() =>
            Source.From(OrderEvents)
                .Via(normalize)
                .Where(document => document.Total > 5m)
                .To(s => s.Aggregate(0m, (total, document) => total + document.Total), "total", out ResultSlot<decimal> _);

        static string Bytes(RunnableGraph graph) =>
            Convert.ToBase64String(GraphDocumentSerializer.Serialize(graph.Document));

        void Peak(int now)
        {
            int seen = Volatile.Read(ref peak);

            while (now > seen)
            {
                int was = Interlocked.CompareExchange(ref peak, now, seen);

                if (was == seen)
                {
                    return;
                }

                seen = was;
            }
        }
    }

    [Fact]
    public void TheSequenceASourceReadsIsNeverEnumeratedWhileAGraphIsBuilt()
    {
        // Building a graph starts no work, and that has to include not touching the author's sequence.
        CountingSequence elements = new();

        _ = Source.From(elements)
            .Where(value => value > 0)
            .Select(value => value * 2)
            .To(s => s.Aggregate(0, (sum, value) => sum + value), "total", out ResultSlot<int> _);

        Assert.Equal(0, elements.EnumerationCount);
    }

    /// <summary>A sequence that records how often it was enumerated.</summary>
    private sealed class CountingSequence : IEnumerable<int>
    {
        /// <summary>Gets the number of enumerators handed out so far.</summary>
        internal int EnumerationCount { get; private set; }

        /// <inheritdoc/>
        public IEnumerator<int> GetEnumerator()
        {
            EnumerationCount++;

            return Enumerable.Empty<int>().GetEnumerator();
        }

        /// <inheritdoc/>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
