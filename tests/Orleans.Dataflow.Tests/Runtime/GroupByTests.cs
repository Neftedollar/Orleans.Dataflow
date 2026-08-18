using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.JunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What a keyed stage promises: one substream per key with its own state, emissions merged as they happen,
/// a bound that is the contract, and an end of stream that flushes every key still open.
/// </summary>
/// <remarks>
/// <para>
/// The claims are asserted as exact sequences rather than as multisets wherever a sequence is a fact. A
/// keyed stage is fused, so nothing about the order of its output is a scheduling question: the elements
/// go downstream in the order the elements that produced them arrived, which is what "unordered across keys
/// and ordered within one" means for a stage that runs on one thread. A test that asserted a multiset would
/// pass for an implementation that had lost that.
/// </para>
/// <para>
/// Per-key isolation is asserted by making the two keys' states observably different — a scan whose sums
/// would collide if the states were shared, and a batch whose groups would interleave — because a stage that
/// shared one state would still emit the right number of elements.
/// </para>
/// </remarks>
public sealed class GroupByTests
{
    [Fact]
    public async Task EachKeyGetsItsOwnStateAndTheEmissionsMerge()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4, 5, 6])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value % 2,
                Flow.For<int>().Scan(0, (running, value) => running + value))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Two running sums that never met: the odds fold 1, 3, 5 into 1, 4, 9 and the evens fold 2, 4, 6
        // into 2, 6, 12. One shared state would have produced 1, 3, 6, 10, 15, 21 — the right number of
        // elements and the wrong operator.
        Assert.Equal([1, 2, 4, 6, 9, 12], observed);
    }

    [Fact]
    public async Task EmissionIsUnorderedAcrossKeysAndInOrderWithinOne()
    {
        List<string> observed = [];

        RunnableGraph graph = Source.From(["a1", "b1", "a2", "a3", "b2"])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value[0],
                Flow.For<string>().Select(value => value.ToUpperInvariant()))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The merge-map sentence pair, read of a keyed stage. Across keys the output is the arrival
        // interleaving and nothing else; within a key it is that key's own order.
        Assert.Equal(["A1", "B1", "A2", "A3", "B2"], observed);
        Assert.Equal(["A1", "A2", "A3"], observed.Where(value => value[0] is 'A'));
        Assert.Equal(["B1", "B2"], observed.Where(value => value[0] is 'B'));
    }

    [Fact]
    public async Task AnIdentityGroupFlowPassesEveryKeysElementsThrough()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .GroupBy(new GroupByOptions { MaxActiveKeys = 2 }, value => value % 2, Flow.For<int>())
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2, 3, 4], observed);
    }

    [Fact]
    public async Task AKeyPastTheBoundFailsTheRunNamingTheBoundAndTheKey()
    {
        RunnableGraph graph = Source.From([1, 2, 3])
            .GroupBy(new GroupByOptions { MaxActiveKeys = 2 }, value => value, Flow.For<int>())
            .To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        TrackedKeyOverflowException failed =
            await Assert.ThrowsAsync<TrackedKeyOverflowException>(async () => await run.Completion);

        // The bound is the number the author chose and the key is usually the whole diagnosis, so both are
        // in the sentence.
        Assert.Contains("at most 2 keys", failed.Message, StringComparison.Ordinal);
        Assert.Contains("'3'", failed.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AKeyAlreadyActiveCostsNothingNew()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([7, 7, 7, 7])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 1 },
                value => value,
                Flow.For<int>().Scan(0, (running, value) => running + value))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // A stream of one key runs inside a bound of one, however long it is: the bound counts keys with a
        // substream open and not elements.
        Assert.Equal([7, 14, 21, 28], observed);
    }

    [Fact]
    public async Task EvictingFlushesWhatTheIdlestKeyWasHoldingAndForgetsIt()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.From([10, 20, 11, 12, 30])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2, OverflowPolicy = ActiveKeyOverflowPolicy.EvictIdle },
                value => value / 10,
                Flow.For<int>().Grouped(3))
            .To(s => s.ForEach(group => observed.Add(group)));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Key 1 fills its group at 12 and emits it; key 3 then arrives against a full table, and the idlest
        // key is 2, which has waited since the second element. Its partial group walks downstream at that
        // moment — the wave-2 residue discipline, applied per key — and then it is forgotten. The end of the
        // stream flushes what is left: key 1 holds nothing and key 3 holds one element.
        Assert.Equal([[10, 11, 12], [20], [30]], observed.Select(group => group.ToArray()));
    }

    [Fact]
    public async Task AnEvictedKeyStartsAgainFromItsSeedAndAppearsTwiceDownstream()
    {
        List<string> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 1])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2, OverflowPolicy = ActiveKeyOverflowPolicy.EvictIdle },
                value => value,
                Flow.For<int>().Scan(0, (running, value) => running + value).Select(sum => $"{sum}"))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Key 1 is evicted when 3 arrives, so the second 1 folds into a fresh state and emits 1 again rather
        // than 2. That is what eviction costs, stated as an assertion rather than as a footnote.
        Assert.Equal(["1", "2", "3", "1"], observed);
    }

    [Fact]
    public async Task AnElementRefreshesItsKeySoTheIdlestIsTheOneThatHasWaitedLongest()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 1, 3])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2, OverflowPolicy = ActiveKeyOverflowPolicy.EvictIdle },
                value => value,
                Flow.For<int>().Scan(0, (running, value) => running + value))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Key 1 arrived first and is touched again by the third element, so the key evicted when 3 arrives
        // is 2 and not 1. Idleness is when a key last had an element, which is the only reading under which
        // "evict idle" means anything different from "evict oldest".
        Assert.Equal([1, 2, 2, 3], observed);
    }

    [Fact]
    public async Task TheEndOfTheStreamFlushesEveryOpenKeyInTheOrderItsKeyFirstArrived()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.From([30, 10, 20, 11, 21, 31])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 3 },
                value => value / 10,
                Flow.For<int>().Grouped(5))
            .To(s => s.ForEach(group => observed.Add(group)));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // No group fills, so every emission is a residue and the order is the order the keys first arrived
        // in: key 3 came first even though its second element is the last one of the stream, and key 1 came
        // before key 2 even though both were last touched in the same pass.
        Assert.Equal([[30, 31], [10, 11], [20, 21]], observed.Select(group => group.ToArray()));
    }

    [Fact]
    public async Task EveryStageOfAKeysFlowIsFlushedInFlowOrder()
    {
        List<IReadOnlyList<IReadOnlyList<int>>> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4, 5])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value % 2,
                Flow.For<int>().Grouped(2).Grouped(2))
            .To(s => s.ForEach(group => observed.Add(group)));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The odd key holds [1,3] and then 5; flushing the first batch hands [5] to the second, which is
        // then holding [[1,3],[5]] and hands that over. Asking every stage in flow order and pushing each
        // residue through the stages below it is what makes that come out whole.
        Assert.Equal(
            [[[1, 3], [5]], [[2, 4]]],
            observed.Select(outer => outer.Select(inner => inner.ToArray()).ToArray()));
    }

    [Fact]
    public async Task AStageInsideAGroupFlowThatEndsItsStreamEndsThatKeyAndNotTheRun()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4, 5, 6, 7, 8])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value % 2,
                Flow.For<int>().Take(2))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Each key takes two elements and then ends; the run goes on and delivers the other key's. Every
        // later element of an ended key is dropped, and the run completes rather than being completed by the
        // first key to reach its bound.
        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1, 2, 3, 4], observed);
    }

    [Fact]
    public async Task AKeyThatEndedHandsOverWhatItWasHoldingAtThatMoment()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.From([1, 3, 5, 7, 2])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value % 2,
                Flow.For<int>().Grouped(2).Take(2))
            .To(s => s.ForEach(group => observed.Add(group)));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The odd key's take is spent on its second group, so its substream ends there. Its batch is asked
        // for a residue at that moment and is empty, and the spent take would have refused one anyway —
        // which is the engine's own rule read one level down.
        Assert.Equal([[1, 3], [5, 7], [2]], observed.Select(group => group.ToArray()));
    }

    [Fact]
    public async Task AKeyThatEndedKeepsItsPlaceAgainstTheBound()
    {
        RunnableGraph graph = Source.From([1, 1, 1, 2, 3])
            .GroupBy(new GroupByOptions { MaxActiveKeys = 2 }, value => value, Flow.For<int>().Take(1))
            .To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // Key 1's substream ended on its first element and its place is still its own, so the third distinct
        // key overflows the bound of two. Remembering that a key ended is what keeps it ended, and what it
        // costs is the place.
        _ = await Assert.ThrowsAsync<TrackedKeyOverflowException>(async () => await run.Completion);
    }

    [Fact]
    public async Task AKeyOfNullIsAKeyLikeAnyOther()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value % 2 == 0 ? "even" : null,
                Flow.For<int>().Scan(0, (running, value) => running + value))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // A key function may answer null and a nullable key type has one, so null is a key with a substream
        // of its own rather than a case the table cannot hold.
        Assert.Equal([1, 2, 4, 6], observed);
    }

    [Fact]
    public async Task EveryRunOfAKeyedGraphStartsWithAnEmptyTable()
    {
        List<int> first = [];
        List<int> second = [];
        List<int> observed = first;

        RunnableGraph graph = Source.From([1, 2])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value % 2,
                Flow.For<int>().Scan(0, (running, value) => running + value))
            .To(s => s.ForEach(value => observed.Add(value)));

        await using (RunHandle one = await Host.MaterializeAsync(graph, TestToken))
        {
            await one.Completion;
        }

        observed = second;

        await using (RunHandle two = await Host.MaterializeAsync(graph, TestToken))
        {
            await two.Completion;
        }

        // Fresh per materialization, exactly as a scan's own state is: the table, the substreams, and every
        // stage inside them.
        Assert.Equal([1, 2], first);
        Assert.Equal(first, second);
    }

    [Fact]
    public async Task AKeyedStageFusesAndPullsNoFurtherThanTheElementInItsHand()
    {
        RecordingEnumerable<int> source = new(1, 2, 3, 4, 5, 6);
        Gate held = new();

        RunnableGraph graph = Source.From(source)
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value % 2,
                Flow.For<int>().Select(value => value))
            .To(s => s.ForEach(value =>
            {
                if (value is 2)
                {
                    held.Wait();
                }
            }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(held.Reached, "the run is inside the callback holding the second element");

        // A keyed stage is a fused stage and not a boundary, so the source has run exactly as far as the
        // element the callback is holding. A stage that opened a segment of its own would have run one
        // further, into the handoff in front of it.
        Assert.Equal(2, source.Pulls);

        held.Open();

        await run.Completion;
    }

    [Fact]
    public async Task ShutdownFlushesEveryKeyThatWasStillOpen()
    {
        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value % 2,
                Flow.For<int>().Grouped(3))
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "groups", out ResultSlot<IReadOnlyList<IReadOnlyList<int>>> groups);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await run.Completion;
        await run.ShutdownAsync();

        // A shutdown ends the stream as running out does, so the elements every key had admitted are handed
        // over rather than abandoned. Asserted on a graph that has already ended for the same reason the
        // batching suite asserts it: what the two share is the residue walk.
        Assert.Equal(
            [[1, 3], [2, 4]],
            (await run.GetValueAsync(groups, TestToken)).Select(group => group.ToArray()));
    }

    [Fact]
    public async Task ShutdownMidStreamHandsOverWhatEveryKeyHadAdmitted()
    {
        RunnableGraph graph = TestSource.Probe<int>("in")
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value % 2,
                Flow.For<int>().Grouped(3))
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "groups", out ResultSlot<IReadOnlyList<IReadOnlyList<int>>> groups);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISourceProbe<int> source = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("in"), TestToken);

        await source.EmitAsync(1, TestToken);
        await source.EmitAsync(2, TestToken);
        await source.EmitAsync(3, TestToken);

        await run.ShutdownAsync();
        await run.Completion;

        // Two open groups at the moment of the stop, and both of them arrive: the odd key's [1,3] and the
        // even key's [2]. An emit into a source probe completes when the run has taken the element, so what
        // was admitted is exact rather than estimated.
        Assert.Equal(
            [[1, 3], [2]],
            (await run.GetValueAsync(groups, TestToken)).Select(group => group.ToArray()));
    }

    [Fact]
    public async Task CancellationAbandonsWhatEveryKeyWasHolding()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = TestSource.Probe<int>("in")
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value % 2,
                Flow.For<int>().Grouped(3))
            .To(s => s.ForEach(group => observed.Add(group)));

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISourceProbe<int> source = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("in"), TestToken);

        await source.EmitAsync(1, TestToken);
        await source.EmitAsync(2, TestToken);

        await run.DisposeAsync();

        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
        Assert.Empty(observed);
    }

    [Fact]
    public async Task APauseHoldsEveryKeysStateAcrossTheResume()
    {
        RunnableGraph graph = Source.From([1, 2, 3, 4, 5, 6])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value % 2,
                Flow.For<int>().Scan(0, (running, value) => running + value))
            .To(TestSink.Probe<int>("out"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISinkProbe<int> sink = await run.GetValueAsync(graph.Control<ISinkProbe<int>>("out"), TestToken);

        Assert.Equal(1, await sink.ReceiveAsync(TestToken));
        Assert.Equal(2, await sink.ReceiveAsync(TestToken));

        // The double pause: the first may be answered by an ordinary park on the way to a wait, so the
        // second is the wait's own. A keyed stage parks between elements like any other fused stage, and
        // what it is holding — one substream per key — is held rather than in flight.
        await Reaches(run.PauseAsync(TestToken), "the run reaches quiescence between two elements");
        await Reaches(run.ResumeAsync(), "the run moves again");
        await Reaches(run.PauseAsync(TestToken), "the run reaches quiescence a second time");

        Assert.True(run.IsPaused);

        await Reaches(run.ResumeAsync(), "the run moves again");

        // Both states survived the hold: the sums carry on rather than restarting from the seed.
        Assert.Equal(4, await sink.ReceiveAsync(TestToken));
        Assert.Equal(6, await sink.ReceiveAsync(TestToken));
        Assert.Equal(9, await sink.ReceiveAsync(TestToken));
        Assert.Equal(12, await sink.ReceiveAsync(TestToken));
        await sink.ExpectCompletedAsync(TestToken);

        await run.Completion;
    }

    [Fact]
    public async Task ASpentBoundBelowCutsTheEndOfStreamFlushShort()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value % 2,
                Flow.For<int>().Grouped(3))
            .Take(1)
            .To(s => s.ForEach(group => observed.Add(group)));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Both keys hand over at the end of the stream and the take is spent on the first of them, so the
        // second never leaves: the run's rule that a residue walk stops at the first residue ending the
        // stream, read over an answer that carries several of them.
        Assert.Equal([[1, 3]], observed.Select(group => group.ToArray()));
    }

    [Fact]
    public async Task APauseInTheMiddleOfTheEndOfStreamFlushHoldsTheResiduesThatHaveNotLeft()
    {
        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value % 2,
                Flow.For<int>().Grouped(3))
            .To(TestSink.Probe<IReadOnlyList<int>>("out"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISinkProbe<IReadOnlyList<int>> sink =
            await run.GetValueAsync(graph.Control<ISinkProbe<IReadOnlyList<int>>>("out"), TestToken);

        // No group fills, so nothing at all leaves until the stream ends and both keys hand over at once.
        // The run is then walking a sequence of residues, which is the path a flattening stage's sequence
        // takes: the token and the pause gate are examined between two of them.
        Assert.Equal([1, 3], await sink.ReceiveAsync(TestToken));

        await Reaches(run.PauseAsync(TestToken), "the run comes to rest between two residues");
        await Reaches(run.ResumeAsync(), "the run moves again");
        await Reaches(run.PauseAsync(TestToken), "the run comes to rest a second time");

        Assert.True(run.IsPaused);

        await Reaches(run.ResumeAsync(), "the run moves again");

        Assert.Equal([2, 4], await sink.ReceiveAsync(TestToken));
        await sink.ExpectCompletedAsync(TestToken);

        await run.Completion;
    }

    [Fact]
    public async Task AFailingKeyFunctionIsTheRunsOwnFailureUnwrapped()
    {
        InvalidTimeZoneException failure = new("the key function said so");

        RunnableGraph graph = Source.From([1, 2, 3])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value is 2 ? throw failure : value % 2,
                Flow.For<int>())
            .To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(
            failure,
            await Assert.ThrowsAsync<InvalidTimeZoneException>(async () => await run.Completion));
    }

    [Fact]
    public async Task TwoKeyedStagesInOneChainKeepTheirOwnTables()
    {
        List<string> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4, 5, 6])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value % 2,
                Flow.For<int>().Grouped(2).Select(group => group.Sum()))
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 4 },
                value => value % 3,
                Flow.For<int>().Grouped(2).Select(group => string.Join('+', group)))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The first stage pairs each key's elements and sums them, so 4 and 6 leave it during the stream and
        // 5 and 6 leave it as residues when the stream ends. The second stage sees all four as ordinary
        // elements and completes a group of its own out of the residues — which is the whole of what "each
        // residue travels through the stages below the one that gave it" means with two keyed stages fused
        // in one segment. Then the second stage's own keys are flushed, in the order they arrived.
        Assert.Equal(["6+6", "4", "5"], observed);
    }

    [Fact]
    public async Task AGroupFlowThatTakesNothingClosesEveryKeyAtItsFirstElement()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .GroupBy(new GroupByOptions { MaxActiveKeys = 2 }, value => value % 2, Flow.For<int>().Take(0))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // A take of nothing inside a group flow ends every key's substream on the element that opened it,
        // and the run itself is untouched: it reads the whole stream and delivers nothing.
        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Empty(observed);
    }

    [Fact]
    public async Task AnEndedKeyThatIsThenEvictedStartsAgain()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 1, 2, 3, 1])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2, OverflowPolicy = ActiveKeyOverflowPolicy.EvictIdle },
                value => value,
                Flow.For<int>().Take(1))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Key 1's substream ends on its first element and its second element is dropped while it keeps its
        // place. Key 3 then evicts it — an ended substream is forgotten without being flushed twice — and
        // the last element of key 1 opens a fresh one, which takes its one element again.
        Assert.Equal([1, 2, 3, 1], observed);
    }

    [Fact]
    public async Task ADistinctInsideAGroupFlowBoundsItsOwnKeysPerKey()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([10, 20, 10, 20, 11, 21])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value / 10,
                Flow.For<int>().Distinct(new DistinctOptions { MaxTrackedKeys = 2 }))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Two bounds, one inside the other, and each counts its own thing: two active keys outside, and two
        // remembered elements inside each of the two substreams. Neither run of the inner bound is the
        // other's, which is why four distinct elements pass a bound of two.
        Assert.Equal([10, 20, 11, 21], observed);
    }

    [Fact]
    public async Task OneGroupFlowComposedIntoTwoGraphsIsTwoSetsOfSubstreams()
    {
        List<int> first = [];
        List<int> second = [];

        Flow<int, int> group = Flow.For<int>().Scan(0, (running, value) => running + value);

        RunnableGraph one = Source.From([1, 2])
            .GroupBy(new GroupByOptions { MaxActiveKeys = 2 }, value => value % 2, group)
            .To(s => s.ForEach(first.Add));
        RunnableGraph two = Source.From([3, 4])
            .GroupBy(new GroupByOptions { MaxActiveKeys = 2 }, value => value % 2, group)
            .To(s => s.ForEach(second.Add));

        await using (RunHandle run = await Host.MaterializeAsync(one, TestToken))
        {
            await run.Completion;
        }

        await using (RunHandle run = await Host.MaterializeAsync(two, TestToken))
        {
            await run.Completion;
        }

        // A flow is an immutable value and composing it reads it, so one group flow in two graphs is two
        // sets of substreams rather than one shared by both.
        Assert.Equal([1, 2], first);
        Assert.Equal([3, 4], second);
    }

    [Fact]
    public async Task AnElementAClockProducedIsAnOrdinaryElementToAKeyedStage()
    {
        LocalDataflowHost host = TimingFixtures.Timed(out TestClock clock);
        List<int> observed = [];

        RunnableGraph graph = Source.Queue<int>(new BufferOptions { Capacity = 4 }, "in")
            .GroupedWithin(10, TimingFixtures.Second)
            .Select(group => group.Sum())
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value % 2,
                Flow.For<int>().Scan(0, (running, value) => running + value))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        IIngressQueue<int> queue = await run.GetValueAsync(graph.Control<IIngressQueue<int>>("in"), TestToken);

        // One element per window, because that is the only shape with a rendezvous. Acceptance into the
        // ingress is not arrival at the stage, and the one fact a test can await before moving the clock is
        // the window's timer being armed — which its first element's arrival is what does. A second element
        // in the same window would have no arming of its own to wait for, and an advance racing it closes
        // the window without it.
        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(3, TestToken));

        await clock.WaitForTimersAsync(1, TestToken);
        await clock.AdvanceAsync(1, TimingFixtures.Second, TestToken);
        await TimingFixtures.Reaches(() => observed.Count == 1, "the first window closing", TestToken);

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(7, TestToken));

        await clock.WaitForTimersAsync(1, TestToken);
        await clock.AdvanceAsync(1, TimingFixtures.Second, TestToken);
        await TimingFixtures.Reaches(() => observed.Count == 2, "the second window closing", TestToken);

        queue.Complete();
        await run.Completion;

        // Both elements reached the keyed stage from a timer's wake rather than from a pull, and the key's
        // state carried across them: 3, then 3 + 7 under the same key. A clock-reading stage may not stand
        // *inside* a group flow, and one standing above a keyed stage is an ordinary producer of ordinary
        // elements — the segment's own thread does the batching, the walk, and the keying, exactly as wave
        // 2 says it does.
        Assert.Equal([3, 10], observed);
    }

    [Fact]
    public async Task TwoKeyedBranchesJoinedByAMergeKeepTheirOwnTables()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value % 2,
                Flow.For<int>().Scan(0, (running, value) => running + value))
            .Merge(Source.From([10, 20, 30, 40])
                .GroupBy(
                    new GroupByOptions { MaxActiveKeys = 2 },
                    value => value % 20,
                    Flow.For<int>().Scan(0, (running, value) => running + value)))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // A merge promises the multiset rather than the interleaving, so the assertion is the multiset: each
        // branch's own sequence is exactly what it would have been alone, and one keyed stage's table is
        // nothing to the other's.
        Assert.Equal([1, 2, 4, 6, 10, 20, 40, 60], observed.Order());
    }

    [Fact]
    public async Task AKeyedStageComposesWithTheStagesAroundIt()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4, 5, 6, 7, 8])
            .Where(value => value < 7)
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value % 2,
                Flow.For<int>().Grouped(2).Select(group => group.Sum()))
            .Take(2)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // A group leaves the keyed stage as an ordinary element, so a take below it ends the run on the one
        // that reaches its bound: the odd key's 1+3 and the even key's 2+4, and nothing after them.
        Assert.Equal([4, 6], observed);
    }
}
