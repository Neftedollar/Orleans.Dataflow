using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;
using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.DurableFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// The local proof of ADR 0007's resume: a run that dies, a new materialization of the same graph over the
/// checkpoint it left, and every promise the model makes checked against a number.
/// </summary>
/// <remarks>
/// <para>
/// <b>This is the model's proof and not the row's.</b> Nothing here crosses a process, a silo, or a machine:
/// the "crash" is an injected failure that kills the attempt, and what survives it is the store. That is
/// exactly the half a local runtime can prove — the checkpoint model, the cursor seam, the durable-state
/// seam, the commit-mark seam, and the arithmetic that relates them — and the other half, a run outliving
/// the process that was running it, is M5.3's with an Orleans cluster under it.
/// </para>
/// <para>
/// <b>Every number is read back out of the store.</b> A test that asserted against what the run believed it
/// had written would prove the run consistent with itself, which is not the claim.
/// </para>
/// <para>
/// <b>The duplicate window is measured, not bounded.</b> "At-least-once between commit marks" is an
/// arithmetic statement — the elements between the checkpoint's cursor and the mark at the crash are
/// delivered twice — and the assertions here are the subtraction, by value, rather than a range.
/// </para>
/// </remarks>
public sealed class ResumeTests
{
    [Fact]
    public async Task AResumedRunReopensItsSourceWhereTheCheckpointLeftItAndReplaysExactlyTheDuplicateWindow()
    {
        InMemoryCheckpointStore store = new();
        List<int> first = [];
        List<int> second = [];

        RunnableGraph crashing = Committing(first);

        await using (RunHandle attempt = await Host.MaterializeDurableAsync(
            crashing,
            Durable(store, "replay", everyElements: 3),
            TestToken))
        {
            _ = await Assert.ThrowsAsync<FaultInjectedException>(async () => await attempt.Completion);

            IMarkingSink marked = await attempt.GetValueAsync(
                crashing.Control<IMarkingSink>("mark"),
                TestToken);

            Assert.Equal(8L, marked.Mark);
        }

        LocalCheckpoint checkpoint = await StoredAsync(store, "replay", TestToken);

        // The capture at element six is the last one the run reached: the ninth element threw before it was
        // delivered, so it never counted towards the bound and no capture was ever due for it.
        Assert.Equal(6L, Cursor(checkpoint));
        Assert.Equal(6L, Mark(checkpoint));
        Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8], first);

        RunnableGraph resumed = Committing(second);

        await using (RunHandle attempt = await Host.MaterializeFromCheckpointAsync(
            resumed,
            Durable(store, "replay", everyElements: 3),
            TestToken))
        {
            await attempt.Completion;

            IMarkingSink marked = await attempt.GetValueAsync(
                resumed.Control<IMarkingSink>("mark"),
                TestToken);

            // The mark is restored and continues, so a second crash's checkpoint would describe more work
            // than the first's rather than starting over.
            Assert.Equal(12L, marked.Mark);
        }

        // The source reopened at the stored cursor, so the resumed attempt starts at element seven.
        Assert.Equal([7, 8, 9, 10, 11, 12], second);

        // The duplicate window is exactly [stored cursor, mark at the crash]: two elements, by value, and
        // not one more. Nothing is lost, because this graph holds nothing between the source and the sink.
        Assert.Equal([7, 8], first.Skip(6));
        Assert.Equal([7, 8], first.Intersect(second));
        Assert.Equal([.. Enumerable.Range(1, 12)], first.Union(second).Order());
        Assert.Equal(14, first.Count + second.Count);

        // And the resumed attempt's own checkpoints continue the run rather than restarting it: its cursor
        // counts from where it reopened, not from its own first element.
        LocalCheckpoint continued = await StoredAsync(store, "replay", TestToken);

        Assert.Equal(12L, Cursor(continued));
        Assert.Equal(12L, Mark(continued));
    }

    [Fact]
    public async Task ASequenceShorterThanTheStoredPositionFailsTheResumeByName()
    {
        InMemoryCheckpointStore store = new();
        RunnableGraph graph = Committing([]);

        _ = await store.WriteAsync(
            Anonymous,
            RunId.Create("shrunk"),
            LocalCheckpointDocument.Write(
                graph.Fingerprint,
                graph.Document.Revision,
                new Dictionary<NodeId, CanonicalJsonValue>
                {
                    [NodeId.Create("stage-0001")] = CanonicalJsonValue.Parse("""{"index":99}"""),
                },
                new Dictionary<NodeId, CanonicalJsonValue>(),
                new Dictionary<NodeId, CanonicalJsonValue>()),
            expectedETag: null,
            TestToken);

        await using RunHandle run = await Host.MaterializeFromCheckpointAsync(
            graph,
            Durable(store, "shrunk", everyElements: 3),
            TestToken);

        InvalidOperationException refused =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion);

        // The adapter's declared requirement, refused rather than silently started from somewhere else: a
        // sequence cursor re-enumerates the very sequence the author handed over, so a sequence that has
        // shrunk is a resume that cannot be honoured. It fails the run rather than materialization, because
        // a source is not opened until the first pull.
        Assert.Contains("reopen at element 99", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AStoredStateAScopeCannotReadIsRefusedByName()
    {
        InMemoryCheckpointStore store = new();
        RunnableGraph graph = Counting([]);
        NodeId scope = graph.Document.Nodes
            .Single(node => node.Stage.ToString() == "local/durable@v1")
            .Id;

        _ = await store.WriteAsync(
            Anonymous,
            RunId.Create("garbled-state"),
            LocalCheckpointDocument.Write(
                graph.Fingerprint,
                graph.Document.Revision,
                new Dictionary<NodeId, CanonicalJsonValue>(),
                new Dictionary<NodeId, CanonicalJsonValue>
                {
                    [scope] = CanonicalJsonValue.Parse("""{"stages":[{},{}]}"""),
                },
                new Dictionary<NodeId, CanonicalJsonValue>()),
            expectedETag: null,
            TestToken);

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeFromCheckpointAsync(
                graph,
                Durable(store, "garbled-state", everyElements: 3),
                TestToken));

        // A scope's export is positional over its declared chain, so a chain of a different length is a
        // checkpoint of a different graph — refused before the run's first element rather than applied to
        // whichever stages happened to line up.
        Assert.Contains("2 stage states for a durable scope of 1", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DurableStateSurvivesAResumeAndEverythingElseResetsProvedByValues()
    {
        InMemoryCheckpointStore store = new();
        List<(long Sum, long Seen)> first = [];
        List<(long Sum, long Seen)> second = [];

        RunnableGraph crashing = Counting(first);

        await using (RunHandle attempt = await Host.MaterializeDurableAsync(
            crashing,
            Durable(store, "state", everyElements: 3),
            TestToken))
        {
            _ = await Assert.ThrowsAsync<FaultInjectedException>(async () => await attempt.Completion);
        }

        LocalCheckpoint checkpoint = await StoredAsync(store, "state", TestToken);

        // The scope's state and the cursor were captured at one safe point, so the two describe the same
        // moment: twenty-one is the sum of the first six elements and six is how many the source had
        // delivered.
        Assert.Equal(6L, Cursor(checkpoint));
        Assert.Equal(21L, Total(checkpoint));

        RunnableGraph resumed = Counting(second);

        await using (RunHandle attempt = await Host.MaterializeFromCheckpointAsync(
            resumed,
            Durable(store, "state", everyElements: 3),
            TestToken))
        {
            await attempt.Completion;
        }

        // Proved by values rather than by counts, which is the only proof that separates "restored" from
        // "started again": the first element of the resumed attempt carries a running sum of twenty-eight,
        // which is the stored twenty-one plus element seven. A scope that had reset would have said seven.
        Assert.Equal(28L, second[0].Sum);

        // And the scan *outside* the scope reset, which is the other half of the same contract: it counts
        // one for the first element of the resumed attempt, not seven.
        Assert.Equal(1L, second[0].Seen);
        Assert.Equal(8L, first[^1].Seen);

        // The durable total ends at the true sum of the whole stream, even though the sink saw two elements
        // twice: a checkpoint's state and its cursor are one moment, so the replay adds each element to the
        // scope's state exactly once. The at-least-once window is the sink's and not the scope's.
        Assert.Equal(78L, second[^1].Sum);
        Assert.Equal(6, second.Count);
    }

    [Fact]
    public async Task AResumeAgainstADifferentGraphIsRefusedByName()
    {
        InMemoryCheckpointStore store = new();
        RunnableGraph crashing = Committing([]);

        await using (RunHandle attempt = await Host.MaterializeDurableAsync(
            crashing,
            Durable(store, "mismatch", everyElements: 3),
            TestToken))
        {
            _ = await Assert.ThrowsAsync<FaultInjectedException>(async () => await attempt.Completion);
        }

        RunnableGraph other = Source.From(Enumerable.Range(1, 12))
            .To(TestSink.Marking<int>("mark", static _ => { }));

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeFromCheckpointAsync(
                other,
                Durable(store, "mismatch", everyElements: 3),
                TestToken));

        // The v1 rule said by name: same-revision resume only, and a checkpoint of another document is
        // refused rather than partly applied.
        Assert.Contains(crashing.Fingerprint.ToString(), refused.Message, StringComparison.Ordinal);
        Assert.Contains(other.Fingerprint.ToString(), refused.Message, StringComparison.Ordinal);
        Assert.Contains("same revision only", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AResumeOfARunTheStoreHasNeverHeardOfIsRefusedByName()
    {
        InMemoryCheckpointStore store = new();
        RunnableGraph graph = Committing([]);

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeFromCheckpointAsync(
                graph,
                Durable(store, "never-ran", everyElements: 3),
                TestToken));

        Assert.Contains("holds nothing", refused.Message, StringComparison.Ordinal);
        Assert.Contains("never-ran", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ACheckpointNamingANodeThisGraphHasNoSeamForIsRefusedByName()
    {
        InMemoryCheckpointStore store = new();
        RunnableGraph graph = Committing([]);

        _ = await store.WriteAsync(
            Anonymous,
            RunId.Create("foreign"),
            LocalCheckpointDocument.Write(
                graph.Fingerprint,
                graph.Document.Revision,
                new Dictionary<NodeId, CanonicalJsonValue>
                {
                    [NodeId.Create("stage-9999")] = CanonicalJsonValue.Parse("""{"index":1}"""),
                },
                new Dictionary<NodeId, CanonicalJsonValue>(),
                new Dictionary<NodeId, CanonicalJsonValue>()),
            expectedETag: null,
            TestToken);

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeFromCheckpointAsync(
                graph,
                Durable(store, "foreign", everyElements: 3),
                TestToken));

        Assert.Contains("stage-9999", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AStoredDocumentThisRuntimeCannotReadIsRefusedByName()
    {
        InMemoryCheckpointStore store = new();
        RunnableGraph graph = Committing([]);

        _ = await store.WriteAsync(
            Anonymous,
            RunId.Create("garbled"),
            CanonicalJsonValue.Parse("""{"nonsense":true}"""),
            expectedETag: null,
            TestToken);

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeFromCheckpointAsync(
                graph,
                Durable(store, "garbled", everyElements: 3),
                TestToken));

        Assert.Contains("nonsense", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AResumedAttemptFencesTheStaleOneOutOfTheStore()
    {
        InMemoryCheckpointStore store = new();
        RunnableGraph crashing = Committing([]);

        await using (RunHandle attempt = await Host.MaterializeDurableAsync(
            crashing,
            Durable(store, "fencing", everyElements: 3),
            TestToken))
        {
            _ = await Assert.ThrowsAsync<FaultInjectedException>(async () => await attempt.Completion);
        }

        StoredCheckpoint stale = (await store.ReadAsync(Anonymous, RunId.Create("fencing"), TestToken))!.Value;
        RunnableGraph resumed = Committing([]);

        await using (RunHandle attempt = await Host.MaterializeFromCheckpointAsync(
            resumed,
            Durable(store, "fencing", everyElements: 3),
            TestToken))
        {
            await attempt.Completion;
        }

        // The resumed attempt presented the ETag it read and the store moved on, so the crashed attempt's
        // own ETag is now the stale one — which is the whole of what "resume is the same run continuing"
        // buys against a stale writer that is somehow still alive.
        _ = await Assert.ThrowsAsync<CheckpointConflictException>(
            async () => await store.WriteAsync(
                Anonymous,
                RunId.Create("fencing"),
                stale.Document,
                stale.ETag,
                TestToken));
    }

    [Fact]
    public async Task AResumedRunOfAGraphWithNoCursorStartsFromNow()
    {
        InMemoryCheckpointStore store = new();
        List<int> committed = [];

        // A queue is a source with no cursor: it contributes nothing to a checkpoint and resumes from now,
        // which is stated per source in the adapter table rather than generalized.
        RunnableGraph graph = Source
            .Queue<int>(new BufferOptions { Capacity = 8, OverflowPolicy = OverflowPolicy.Backpressure }, "in")
            .To(TestSink.Marking<int>("mark", committed.Add));

        await using (RunHandle attempt = await Host.MaterializeDurableAsync(
            graph,
            Durable(store, "queued", everyElements: 2),
            TestToken))
        {
            IIngressQueue<int> queue = await attempt.GetValueAsync(
                graph.Control<IIngressQueue<int>>("in"),
                TestToken);

            _ = await queue.OfferAsync(1, TestToken);
            _ = await queue.OfferAsync(2, TestToken);

            while (attempt.Checkpoints == 0L)
            {
                TestToken.ThrowIfCancellationRequested();

                await Task.Yield();
            }

            await attempt.ShutdownAsync();
            await attempt.Completion;
        }

        LocalCheckpoint checkpoint = await StoredAsync(store, "queued", TestToken);

        Assert.Empty(checkpoint.Cursors);
        Assert.Equal(2L, Mark(checkpoint));
        Assert.Equal([1, 2], committed);
    }

    [Fact]
    public async Task AnElementHeldBetweenACursorAndItsMarkIsLostByAResumeAndTheCheckpointSaysHowMany()
    {
        InMemoryCheckpointStore store = new();
        List<IReadOnlyList<int>> first = [];
        List<IReadOnlyList<int>> second = [];

        // A batch is the smallest thing that holds elements between a cursor and a mark: the source has
        // delivered them and the sink has not committed them, and a resume replays from the cursor.
        RunnableGraph crashing = Batching(first);

        await using (RunHandle attempt = await Host.MaterializeDurableAsync(
            crashing,
            Durable(store, "in-flight", everyElements: 4),
            TestToken))
        {
            _ = await Assert.ThrowsAsync<FaultInjectedException>(async () => await attempt.Completion);
        }

        LocalCheckpoint checkpoint = await StoredAsync(store, "in-flight", TestToken);

        // Eight elements delivered, one group of five committed: the checkpoint says both numbers, which is
        // the whole reason it carries cursors and marks rather than one of them.
        Assert.Equal(8L, Cursor(checkpoint));
        Assert.Equal(1L, Mark(checkpoint));
        Assert.Equal([[1, 2, 3, 4, 5]], first);

        RunnableGraph resumed = Batching(second);

        await using (RunHandle attempt = await Host.MaterializeFromCheckpointAsync(
            resumed,
            Durable(store, "in-flight", everyElements: 4),
            TestToken))
        {
            await attempt.Completion;
        }

        // Elements six, seven, and eight are gone, and this is v1's honest boundary rather than a defect:
        // the batch is not inside a durable scope, so it reset; the cursor had counted the elements it was
        // holding; and a resume replays from the cursor. The number is exactly cursor minus committed
        // elements — eight minus five — and it is a measurement the checkpoint itself hands over, not a
        // surprise. A graph that must not lose them puts the batch in a durable scope, or a marking sink
        // where the elements actually land.
        Assert.Equal([[9, 10]], second);
        Assert.Equal(3, 8 - (first[0].Count * 1));
        Assert.DoesNotContain(second.SelectMany(group => group), element => element is 6 or 7 or 8);
    }

    /// <summary>The graph the loss-window measurement runs: a batch between the cursor and the mark.</summary>
    /// <param name="committed">The list the sink's side effect appends each committed group to.</param>
    /// <returns>The closed graph.</returns>
    /// <remarks>
    /// Ten elements, a crash at the ninth arrival, and groups of five, so that a capture at element eight
    /// finds the batch holding three elements the sink has never seen. Everything about that is
    /// deterministic: the chain is fused, so the batch's contents at any element are arithmetic.
    /// </remarks>
    private static RunnableGraph Batching(List<IReadOnlyList<int>> committed) =>
        Source.From(Enumerable.Range(1, 10))
            .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 9))
            .Grouped(5)
            .To(TestSink.Marking<IReadOnlyList<int>>("mark", committed.Add));

    /// <summary>The graph the replay proof runs: twelve elements, a crash at the ninth, one marking sink.</summary>
    /// <param name="committed">The list the sink's side effect appends to.</param>
    /// <returns>The closed graph.</returns>
    /// <remarks>
    /// Built twice per test — once for the attempt that dies and once for the attempt that resumes — and the
    /// two are the same graph, because a fingerprint is content and a lambda is not content. The declared
    /// arming names the ninth <em>arrival of the run</em>, so the resumed attempt, which replays six
    /// elements, never reaches it.
    /// </remarks>
    private static RunnableGraph Committing(List<int> committed) =>
        Source.From(Enumerable.Range(1, 12))
            .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 9))
            .To(TestSink.Marking<int>("mark", committed.Add));

    /// <summary>The graph the state proof runs: a durable running sum and a scan outside the scope.</summary>
    /// <param name="committed">The list the sink's side effect appends to.</param>
    /// <returns>The closed graph.</returns>
    /// <remarks>
    /// The two scans are the whole experiment. The one inside the scope carries a state codec and therefore
    /// a state a checkpoint can hold; the one outside it counts what <em>this attempt</em> has seen and is
    /// rebuilt from its seed by every materialization. Reading both out of one element is what lets one
    /// assertion separate "restored" from "started again".
    /// </remarks>
    private static RunnableGraph Counting(List<(long Sum, long Seen)> committed) =>
        Source.From(Enumerable.Range(1, 12))
            .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 9))
            .Durable(Flow.For<int>().Scan(0L, (sum, value) => sum + value, WriteTotal, ReadTotal))
            .Scan((Sum: 0L, Seen: 0L), (state, sum) => (sum, state.Seen + 1))
            .To(TestSink.Marking<(long Sum, long Seen)>("mark", committed.Add));
}
