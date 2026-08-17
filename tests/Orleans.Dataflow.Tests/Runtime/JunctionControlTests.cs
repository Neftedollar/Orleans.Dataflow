using System.Threading.Channels;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.JunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// The control plane across branching topologies: what pausing, resuming, shutting down, cancelling, and
/// failing do to a graph whose segments are junctions rather than a line of stages.
/// </summary>
/// <remarks>
/// <para>
/// This file is the junction-era sibling of <see cref="PauseTests"/>, and it keeps that file's discipline
/// exactly. Nothing waits on a clock. "The pause has not taken effect yet" is asserted only where it is a
/// fact — a run inside an author's callback is one no pause can be quiescent for — and "nothing moved while
/// the run was held" is asserted by what the run did afterwards, where the alternative would have produced
/// a different sequence. Every test resumes or stops the run it paused, because a run left paused is a run
/// whose completion never arrives.
/// </para>
/// <para>
/// <b>Where the held states come from.</b> Each junction has a state only it can be in — a partition
/// holding an element routed to a leg with no room, a broadcast that cannot pull because one leg is full, a
/// zip holding a column, a combine-latest remembering every input's latest — and reaching one on purpose
/// needs a consumer that stops consuming without an author's callback blocking quiescence by design. That
/// is what the demand-aware probes are for, and it is why the case checkpoint 4 deferred is here: a probe
/// sink holds the run's element on the run's own thread inside one of this runtime's own waits, which is
/// exactly the state a pause has to be able to be quiescent about.
/// </para>
/// <para>
/// <b>The double pause is an idiom and not a stutter.</b> A pause asked for while a segment is still on its
/// way to a wait may well be answered by an ordinary park at that segment's safe point, which proves
/// nothing about the wait. Pausing, resuming, and pausing again — the very idiom the M2 suite uses for a
/// source that parks on nothing at all — leaves the run in a state from which the only way to be quiet is
/// the wait itself, because nothing between the resume and the second request can move.
/// </para>
/// <para>
/// <b>The bounds are read the way every bounded-memory test in this suite reads them.</b>
/// <see cref="ISourceProbe{T}.PullsObserved"/> is how far a held source got, and a run whose junction held
/// one element more than its contract allows would have pulled one more. The counts are asserted once the
/// run has come to rest, never sampled at a moment that might have been one step early.
/// </para>
/// </remarks>
public sealed class JunctionControlTests
{
    [Fact]
    public async Task APauseTakesEffectOnAPartitionHoldingARoutedElement()
    {
        // The case checkpoint 4 deferred, and the reason it was deferred: filling a leg means its consumer
        // is stuck, and the only ways to keep a consumer stuck are an author's callback — which blocks
        // quiescence by design, so a test built that way would be asserting a pause the contract says
        // cannot happen — and a probe's rendezvous, which is this checkpoint's.
        //
        // The state is arranged by necessity rather than by timing. The probe sink holds 2 because nobody
        // has received it; the leg's channel holds 4 because the sink is not reading; so the partition,
        // which reads first and waits second, is holding 6 with nowhere to put it. That is a fact by the
        // time the last emit returns: an emit completes when the run has taken the element, the source
        // segment takes the next element only after placing the one before it, and the junction's input
        // channel holds one — so 3 could only have been taken after 8 was placed, and 8 could only have
        // been placed after the partition took 6.
        Lock counting = new();
        List<int> odd = [];
        int routed = 0;

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Emitter("stage-1"),
                    Node("stage-2", "partition"),
                    Receiver("stage-3"),
                    Node("stage-4", "for-each"),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                [Control("emitted", "stage-1"), Control("even", "stage-3")]),
            Bindings(
                ("stage-1", Emitting<int>("emitted")),
                (
                    "stage-2",
                    Routing(value =>
                    {
                        _ = Interlocked.Increment(ref routed);

                        return value % 2;
                    })),
                ("stage-3", Receiving<int>("even")),
                (
                    "stage-4",
                    Calling(value =>
                    {
                        lock (counting)
                        {
                            odd.Add(value);
                        }
                    }))),
            Controls(("emitted", typeof(ISourceProbe<int>)), ("even", typeof(ISinkProbe<int>))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISourceProbe<int> source = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);
        ISinkProbe<int> even = await run.GetValueAsync(graph.Control<ISinkProbe<int>>("even"), TestToken);

        await source.EmitAsync(2, TestToken);
        await source.EmitAsync(4, TestToken);
        await source.EmitAsync(6, TestToken);
        await source.EmitAsync(8, TestToken);
        await source.EmitAsync(3, TestToken);

        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a routed run");
        await Reaches(run.ResumeAsync(), "the run moves again");
        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a partition holding a routed element");

        Assert.True(run.IsPaused);

        // Nothing between the resume and this request could move — every consumer is holding what it has
        // and no receive has been issued — so the junction is in the one wait it can be in: holding an
        // element it has already routed, waiting for that element's own leg.
        Assert.Equal(3, Volatile.Read(ref routed));

        // Head-of-line, one element deep, through the pause: the odd element is queued behind an element
        // routed to a leg with no room, so the leg it belongs on has received nothing at all. The routing
        // function ran once for each of the three elements the junction took and not once for the element
        // behind them, which is the read-once rule seen through a pause.
        lock (counting)
        {
            Assert.Empty(odd);
        }

        await Reaches(run.ResumeAsync(), "the run moves again");

        // The held element is delivered once, unchanged, and in its place: a pause is a hold and not a
        // step, so 6 is neither lost nor repeated nor overtaken by the element behind it.
        Assert.Equal(2, await even.ReceiveAsync(TestToken));
        Assert.Equal(4, await even.ReceiveAsync(TestToken));
        Assert.Equal(6, await even.ReceiveAsync(TestToken));
        Assert.Equal(8, await even.ReceiveAsync(TestToken));

        source.Complete();

        await even.ExpectCompletedAsync(TestToken);
        await Reaches(run.Completion, "the run ends once both legs have drained");

        lock (counting)
        {
            Assert.Equal([3], odd);
        }

        Assert.Equal(5, Volatile.Read(ref routed));
    }

    [Fact]
    public async Task APauseTakesEffectOnABroadcastThatCannotPullBecauseOneLegIsFull()
    {
        // The fan-out half of the same claim, and the opposite bound: a broadcast asks for room before it
        // pulls, so a broadcast that cannot pull is holding nothing at all. The element it would have taken
        // is still in its input channel, which is what the source's pull count says.
        Lock counting = new();
        List<int> fast = [];

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Emitter("stage-1"),
                    Node("stage-2", "broadcast"),
                    Receiver("stage-3"),
                    Node("stage-4", "for-each"),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                [Control("emitted", "stage-1"), Control("slow", "stage-3")]),
            Bindings(
                ("stage-1", Emitting<int>("emitted")),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", Receiving<int>("slow")),
                (
                    "stage-4",
                    Calling(value =>
                    {
                        lock (counting)
                        {
                            fast.Add(value);
                        }
                    }))),
            Controls(("emitted", typeof(ISourceProbe<int>)), ("slow", typeof(ISinkProbe<int>))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISourceProbe<int> source = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);
        ISinkProbe<int> slow = await run.GetValueAsync(graph.Control<ISinkProbe<int>>("slow"), TestToken);

        await source.EmitAsync(1, TestToken);
        await source.EmitAsync(2, TestToken);
        await source.EmitAsync(3, TestToken);
        await source.EmitAsync(4, TestToken);

        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a branching run");
        await Reaches(run.ResumeAsync(), "the run moves again");
        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a broadcast waiting for room on a leg");

        Assert.True(run.IsPaused);

        // Four elements taken and no fifth pull: one at the probe sink, one in the slow leg's channel, one
        // in the junction's input channel, one in the source segment's hand — and nothing at all inside the
        // junction, which is what "ask for room, then pull" buys.
        Assert.Equal(4L, source.PullsObserved);

        await Reaches(run.ResumeAsync(), "the run moves again");

        Assert.Equal(1, await slow.ReceiveAsync(TestToken));
        Assert.Equal(2, await slow.ReceiveAsync(TestToken));
        Assert.Equal(3, await slow.ReceiveAsync(TestToken));
        Assert.Equal(4, await slow.ReceiveAsync(TestToken));

        source.Complete();

        await slow.ExpectCompletedAsync(TestToken);
        await Reaches(run.Completion, "the resumed run reaches its end");

        // Slowest-consumer backpressure paces the fast leg too, and every element reached both legs: a
        // pause across a fan-out delivers the same stream twice and loses nothing on either.
        lock (counting)
        {
            Assert.Equal([1, 2, 3, 4], fast);
        }
    }

    [Fact]
    public async Task APauseTakesEffectOnABalanceWaitingForAnyLegToHaveRoom()
    {
        // The other splitting wait, and a different one in the engine: a broadcast needs every leg and
        // waits for them one after another, while a balance needs one and waits on all of them at once.
        // A wait-any that did not report itself would hang a pause exactly here, which is why the run is
        // arranged so that both legs are full and there is no other way for it to be quiet.
        Lock counting = new();
        List<int> received = [];

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Emitter("stage-1"),
                    Node("stage-2", "balance"),
                    Receiver("stage-3"),
                    Receiver("stage-4"),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                [Control("emitted", "stage-1"), Control("first", "stage-3"), Control("second", "stage-4")]),
            Bindings(
                ("stage-1", Emitting<int>("emitted")),
                ("stage-2", LocalStageDescriptor.Balance()),
                ("stage-3", Receiving<int>("first")),
                ("stage-4", Receiving<int>("second"))),
            Controls(
                ("emitted", typeof(ISourceProbe<int>)),
                ("first", typeof(ISinkProbe<int>)),
                ("second", typeof(ISinkProbe<int>))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISourceProbe<int> source = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);
        ISinkProbe<int> first = await run.GetValueAsync(graph.Control<ISinkProbe<int>>("first"), TestToken);
        ISinkProbe<int> second = await run.GetValueAsync(graph.Control<ISinkProbe<int>>("second"), TestToken);

        for (int element = 1; element <= 6; element++)
        {
            await source.EmitAsync(element, TestToken);
        }

        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a distributing run");
        await Reaches(run.ResumeAsync(), "the run moves again");
        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a balance with no willing leg");

        Assert.True(run.IsPaused);

        // Two legs holding two elements each, one in the junction's input channel, one in the source
        // segment's hand, and nothing inside the junction: a balance that had taken an element it could
        // not place would have pulled once more.
        Assert.Equal(6L, source.PullsObserved);

        await Reaches(run.ResumeAsync(), "the run moves again");

        source.Complete();

        // Which leg an element went to is the one thing a balance never promises, so both legs are drained
        // at once and the union is the claim. Draining them one after the other would be the test's own
        // deadlock rather than the engine's: the leg left unread holds the element that keeps the run from
        // ending, and the run's end is what releases the other drain.
        await Reaches(
            Task.WhenAll(Drained(first, received, counting), Drained(second, received, counting)),
            "both legs drain");

        await Reaches(run.Completion, "the run ends once every leg has drained");

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);

        lock (counting)
        {
            Assert.Equal([.. Enumerable.Range(1, 6)], received.Order());
        }
    }

    [Fact]
    public async Task APauseTakesEffectOnAConcatAsleepOnTheInputWhoseTurnItIs()
    {
        // The joining wait that is not a wait-any: a concat reads one input to its end and does not touch
        // the next one until then, so it sleeps on one channel while another has something ready. That is
        // the state this pause has to be quiescent about, and the element waiting on the input behind it is
        // what proves the junction really was asleep on the active one rather than merely idle.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Emitter("stage-1"),
                    Emitter("stage-2"),
                    Node("stage-3", "concat"),
                    Receiver("stage-4"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                [Control("first", "stage-1"), Control("second", "stage-2"), Control("joined", "stage-4")]),
            Bindings(
                ("stage-1", Emitting<int>("first")),
                ("stage-2", Emitting<int>("second")),
                ("stage-3", LocalStageDescriptor.Concat()),
                ("stage-4", Receiving<int>("joined"))),
            Controls(
                ("first", typeof(ISourceProbe<int>)),
                ("second", typeof(ISourceProbe<int>)),
                ("joined", typeof(ISinkProbe<int>))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISourceProbe<int> first = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("first"), TestToken);
        ISourceProbe<int> second = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("second"), TestToken);
        ISinkProbe<int> joined = await run.GetValueAsync(graph.Control<ISinkProbe<int>>("joined"), TestToken);

        await second.EmitAsync(20, TestToken);

        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a concatenating run");
        await Reaches(run.ResumeAsync(), "the run moves again");
        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a concat asleep on its active input");

        Assert.True(run.IsPaused);

        await Reaches(run.ResumeAsync(), "the run moves again");
        await first.EmitAsync(10, TestToken);

        // The active input's element is what a concat delivers, however long the input behind it has been
        // ready: order across inputs is this junction's whole contract, and a pause did not disturb it.
        Assert.Equal(10, await joined.ReceiveAsync(TestToken));

        first.Complete();

        Assert.Equal(20, await joined.ReceiveAsync(TestToken));

        second.Complete();

        await joined.ExpectCompletedAsync(TestToken);
        await Reaches(run.Completion, "the run ends when the last input has");
    }

    [Fact]
    public async Task APauseTakesEffectOnAMergeAsleepOnEveryInputAndItResumesWithBoth()
    {
        // A joining junction with nothing to read is asleep in a wait of this runtime's own rather than
        // standing at a park point, and a wait that did not report itself would leave the pause waiting
        // forever for the very quiet it caused. The run is arranged so there is no other way to be quiet:
        // both inputs are probes nobody has emitted into, so every segment is inside one of those waits.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Emitter("stage-1"),
                    Emitter("stage-2"),
                    Node("stage-3", "merge"),
                    Receiver("stage-4"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                [Control("left", "stage-1"), Control("right", "stage-2"), Control("joined", "stage-4")]),
            Bindings(
                ("stage-1", Emitting<int>("left")),
                ("stage-2", Emitting<int>("right")),
                ("stage-3", LocalStageDescriptor.Merge()),
                ("stage-4", Receiving<int>("joined"))),
            Controls(
                ("left", typeof(ISourceProbe<int>)),
                ("right", typeof(ISourceProbe<int>)),
                ("joined", typeof(ISinkProbe<int>))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISourceProbe<int> left = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("left"), TestToken);
        ISourceProbe<int> right = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("right"), TestToken);
        ISinkProbe<int> joined = await run.GetValueAsync(graph.Control<ISinkProbe<int>>("joined"), TestToken);

        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a joining run");
        await Reaches(run.ResumeAsync(), "the run moves again");
        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a junction asleep on every input");

        Assert.True(run.IsPaused);

        await Reaches(run.ResumeAsync(), "the run moves again");

        await left.EmitAsync(1, TestToken);
        await right.EmitAsync(2, TestToken);

        // A merge promises nothing about the order across its inputs, so the multiset is the claim: both
        // arrive, and a run held with its junction asleep is a run that wakes up rather than one that
        // missed the arrivals that woke it.
        List<int> received = [await joined.ReceiveAsync(TestToken), await joined.ReceiveAsync(TestToken)];

        left.Complete();
        right.Complete();

        await joined.ExpectCompletedAsync(TestToken);
        await Reaches(run.Completion, "the resumed run ends when both inputs have");

        Assert.Equal([1, 2], received.Order());
    }

    [Fact]
    public async Task AMergeAbsorbsOnlyItsChannelsAndItsHandAcrossAPause()
    {
        // The merge's held-element bound stated as a number and proven through a pause. A merge secures
        // room before it reads, so what it holds between elements is nothing at all and what it holds
        // while placing is one element; everything else the run absorbed is in a channel or in a segment's
        // hand. With the sink holding the first element, the run takes exactly four and asks for no fifth.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Emitter("stage-1"),
                    Emitter("stage-2"),
                    Node("stage-3", "merge"),
                    Receiver("stage-4"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                [Control("left", "stage-1"), Control("right", "stage-2"), Control("joined", "stage-4")]),
            Bindings(
                ("stage-1", Emitting<int>("left")),
                ("stage-2", Emitting<int>("right")),
                ("stage-3", LocalStageDescriptor.Merge()),
                ("stage-4", Receiving<int>("joined"))),
            Controls(
                ("left", typeof(ISourceProbe<int>)),
                ("right", typeof(ISourceProbe<int>)),
                ("joined", typeof(ISinkProbe<int>))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISourceProbe<int> left = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("left"), TestToken);
        ISourceProbe<int> right = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("right"), TestToken);
        ISinkProbe<int> joined = await run.GetValueAsync(graph.Control<ISinkProbe<int>>("joined"), TestToken);

        await left.EmitAsync(1, TestToken);
        await left.EmitAsync(2, TestToken);
        await left.EmitAsync(3, TestToken);
        await left.EmitAsync(4, TestToken);

        // A fact and not a hope: the source segment cannot take a fifth element until it has placed the
        // fourth, and it cannot place the fourth until the junction takes the third, which it cannot do
        // until the sink is asked for something.
        Task fifth = left.EmitAsync(5, TestToken).AsTask();

        Assert.False(fifth.IsCompleted);

        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a saturated joining run");
        await Reaches(run.ResumeAsync(), "the run moves again");
        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a merge waiting for room below it");

        Assert.True(run.IsPaused);
        Assert.False(fifth.IsCompleted);
        Assert.Equal(4L, left.PullsObserved);

        await Reaches(run.ResumeAsync(), "the run moves again");

        List<int> received = [];

        for (int element = 0; element < 5; element++)
        {
            received.Add(await joined.ReceiveAsync(TestToken));
        }

        await fifth;

        left.Complete();
        right.Complete();

        await joined.ExpectCompletedAsync(TestToken);
        await Reaches(run.Completion, "the run ends when both inputs have");

        // One input, so per-input order is the whole of what a merge promises here, and it survives the
        // hold: nothing was lost, repeated, or reordered by being paused.
        Assert.Equal([1, 2, 3, 4, 5], received);
    }

    [Fact]
    public async Task APausedZipHoldsItsPartialRowAndBuildsItFromTheHeldColumn()
    {
        // The row-building junction's held state through a pause, which is the one bound only this pump
        // has: a column already read, a row that cannot be completed, and a wait on the input that would
        // complete it. What proves the column survived the hold is the row itself — the element emitted
        // after the resume carries the very column read before the pause.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Emitter("stage-1"),
                    Emitter("stage-2"),
                    Node("stage-3", "zip"),
                    Receiver("stage-4"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                [Control("left", "stage-1"), Control("right", "stage-2"), Control("rows", "stage-4")]),
            Bindings(
                ("stage-1", Emitting<int>("left")),
                ("stage-2", Emitting<int>("right")),
                ("stage-3", LocalStageDescriptor.Zip(Rows())),
                ("stage-4", Receiving<string>("rows"))),
            Controls(
                ("left", typeof(ISourceProbe<int>)),
                ("right", typeof(ISourceProbe<int>)),
                ("rows", typeof(ISinkProbe<string>))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISourceProbe<int> left = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("left"), TestToken);
        ISourceProbe<int> right = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("right"), TestToken);
        ISinkProbe<string> rows = await run.GetValueAsync(graph.Control<ISinkProbe<string>>("rows"), TestToken);

        await left.EmitAsync(1, TestToken);
        await left.EmitAsync(2, TestToken);
        await left.EmitAsync(3, TestToken);

        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a row-building run");
        await Reaches(run.ResumeAsync(), "the run moves again");
        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a zip holding a partial row");

        Assert.True(run.IsPaused);

        // Three elements taken from the input that is running and no fourth pull: one column in the
        // junction, one element in the input's channel, one in the segment's hand. The N−1 of the table is
        // the first of those three, and a junction that had filled a second row would have pulled again.
        Assert.Equal(3L, left.PullsObserved);

        // And exactly one on the input that has produced nothing: a segment waiting for an element has
        // already spent the pull it is waiting on, which is the demand meter's own reading of "asked for
        // one more than it was given".
        Assert.Equal(1L, right.PullsObserved);

        await Reaches(run.ResumeAsync(), "the run moves again");
        await right.EmitAsync(10, TestToken);

        // The column read before the pause is the column of the row emitted after it.
        Assert.Equal("1-10", await rows.ReceiveAsync(TestToken));

        await right.EmitAsync(20, TestToken);

        Assert.Equal("2-20", await rows.ReceiveAsync(TestToken));

        right.Complete();
        left.Complete();

        // A zip completes as soon as an input it still needs has ended, and the column it was holding for
        // the row that can never exist is discarded rather than delivered.
        await rows.ExpectCompletedAsync(TestToken);
        await Reaches(run.Completion, "the zip ends when the input it needs does");
    }

    [Fact]
    public async Task APausedCombineLatestStillRemembersEveryInputsLatestElement()
    {
        // The other row-building junction's held state, which is not a partial row but a memory: N
        // elements, one per input, held for as long as the junction runs. The proof that the memory
        // survived the hold is the row emitted after the resume from an arrival on one input alone — it
        // carries the other input's element, which was read before the pause.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Emitter("stage-1"),
                    Emitter("stage-2"),
                    Node("stage-3", "combine-latest"),
                    Receiver("stage-4"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                [Control("left", "stage-1"), Control("right", "stage-2"), Control("rows", "stage-4")]),
            Bindings(
                ("stage-1", Emitting<int>("left")),
                ("stage-2", Emitting<int>("right")),
                ("stage-3", LocalStageDescriptor.CombineLatest(Rows())),
                ("stage-4", Receiving<string>("rows"))),
            Controls(
                ("left", typeof(ISourceProbe<int>)),
                ("right", typeof(ISourceProbe<int>)),
                ("rows", typeof(ISinkProbe<string>))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISourceProbe<int> left = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("left"), TestToken);
        ISourceProbe<int> right = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("right"), TestToken);
        ISinkProbe<string> rows = await run.GetValueAsync(graph.Control<ISinkProbe<string>>("rows"), TestToken);

        await left.EmitAsync(1, TestToken);
        await right.EmitAsync(10, TestToken);

        // Receiving the first row is what makes the junction's state a fact: nothing is emitted until every
        // input has produced once, so a row in hand means both slots are filled.
        Assert.Equal("1-10", await rows.ReceiveAsync(TestToken));

        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a combining run");
        await Reaches(run.ResumeAsync(), "the run moves again");
        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a combine-latest asleep on its inputs");

        Assert.True(run.IsPaused);

        await Reaches(run.ResumeAsync(), "the run moves again");
        await right.EmitAsync(20, TestToken);

        // One arrival, one row, and the row carries the element the other input produced before the pause.
        Assert.Equal("1-20", await rows.ReceiveAsync(TestToken));

        left.Complete();
        right.Complete();

        await rows.ExpectCompletedAsync(TestToken);
        await Reaches(run.Completion, "the run ends when every input has");
    }

    [Fact]
    public async Task APausedCycleKeepsCirculatingFromWhereItStopped()
    {
        // A loop has no source to hold it: what it is holding when a pause takes effect is its own
        // circulating stream. The element in this loop carries its lap count, so what the exit observes is
        // a contiguous run of numbers — and a hold that lost the circulating element, or delivered it
        // twice, would break that contiguity at exactly the lap the pause landed on.
        Lock counting = new();
        List<int> laps = [];
        TaskCompletionSource circulating = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource resumed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int released = int.MaxValue;

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "merge"),
                    Node("stage-3", "select"),
                    Node("stage-4", "broadcast"),
                    Node("stage-5", "for-each"),
                    Buffer("stage-6", 4, "drop-oldest"),
                ],
                [
                    Into("stage-1", "stage-2", 0),
                    Edge("stage-2", "stage-3"),
                    Edge("stage-3", "stage-4"),
                    Leg("stage-4", 0, "stage-5"),
                    Leg("stage-4", 1, "stage-6"),
                    Into("stage-6", "stage-2", 1),
                ],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(0))),
                ("stage-2", LocalStageDescriptor.Merge()),
                ("stage-3", LocalStageDescriptor.Select((Func<int, int>)(value => value + 1))),
                ("stage-4", LocalStageDescriptor.Broadcast()),
                (
                    "stage-5",
                    Calling(value =>
                    {
                        lock (counting)
                        {
                            laps.Add(value);
                        }

                        if (value == 20)
                        {
                            circulating.TrySetResult();
                        }

                        if (value > Volatile.Read(ref released) + 10)
                        {
                            resumed.TrySetResult();
                        }
                    })),
                ("stage-6", Buffering(4))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(circulating.Task, "the loop is circulating");
        await Reaches(run.PauseAsync(TestToken), "the loop comes to rest");

        Assert.True(run.IsPaused);

        // Read while the run is at rest, so the lap the loop stopped on is a fact rather than a sample:
        // nothing moves between here and the resume, which is what makes "ten more laps than this one"
        // a claim about the run having started again.
        lock (counting)
        {
            Volatile.Write(ref released, laps.Count);
        }

        await Reaches(run.ResumeAsync(), "the loop moves again");
        await Reaches(resumed.Task, "the loop keeps circulating after the resume");

        await run.ShutdownAsync();
        await Reaches(run.Completion, "the shutdown ends the loop");

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);

        int[] observed;

        lock (counting)
        {
            observed = [.. laps];
        }

        // Contiguous from the first lap to the last, across the pause that landed in the middle of it: the
        // one element this loop carries was still the one it was carrying when the run moved again.
        Assert.Equal([.. Enumerable.Range(1, observed.Length)], observed);
    }

    [Fact]
    public async Task PausingAndResumingABranchingRunRepeatedlyLosesNoElement()
    {
        // The storm, generalized from the linear suite to a diamond. Every cycle asks for quiescence while
        // elements are genuinely in flight on both legs, and every one of them has to be answered: a
        // junction whose wait went unreported would hang here rather than fail, which is what the deadline
        // in Reaches is for. What the run produced afterwards is the assertion — an exact sequence of rows,
        // one per element, is a claim no run that lost, repeated, or reordered an element could satisfy.
        const int Elements = 40;

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Emitter("stage-1"),
                    Node("stage-2", "broadcast"),
                    Node("stage-3", "select"),
                    Node("stage-4", "select"),
                    Node("stage-5", "zip"),
                    Collect("stage-6", 128),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Leg("stage-2", 1, "stage-4"),
                    Into("stage-3", "stage-5", 0),
                    Into("stage-4", "stage-5", 1),
                    Edge("stage-5", "stage-6"),
                ],
                [Control("emitted", "stage-1"), Slot("rows", "stage-6")]),
            Bindings(
                ("stage-1", Emitting<int>("emitted")),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", LocalStageDescriptor.Select((Func<int, int>)(value => value))),
                ("stage-4", LocalStageDescriptor.Select((Func<int, int>)(value => value * 10))),
                ("stage-5", LocalStageDescriptor.Zip(Rows())),
                ("stage-6", CollectingRows(128))),
            Controls(("emitted", typeof(ISourceProbe<int>))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISourceProbe<int> source = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);

        for (int element = 1; element <= Elements; element++)
        {
            await source.EmitAsync(element, TestToken);
            await Reaches(run.PauseAsync(TestToken), $"the pause takes effect at element {element}");

            Assert.True(run.IsPaused);

            await Reaches(run.ResumeAsync(), $"the run moves again after element {element}");
        }

        source.Complete();

        await Reaches(run.Completion, "the stormed run reaches its end");

        string[] rows = await run.GetValueAsync(Result<string[]>(graph, "rows"), TestToken);

        Assert.Equal(
            [.. Enumerable.Range(1, Elements).Select(value => $"{value}-{value * 10}")],
            rows);
    }

    [Fact]
    public async Task APauseOfABranchingRunWaitsForACallbackOnAnotherBranch()
    {
        // The one counter that is not about segments, checked across a branch rather than along a chain. A
        // callback in flight is an author's code executing, so a pause has not taken effect while one is
        // running however parked the branches around it are — and this is a fact rather than a hope, which
        // is what lets the claim be asserted at all. The branch the callback is on is not the branch the
        // pause would otherwise be quiet about, which is the part that is new here.
        TaskCompletionSource held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "cycle"),
                    Node("stage-2", "broadcast"),
                    Node("stage-3", "select-async", "local-parallelism-parameters", """{"maxConcurrency":1}"""),
                    Node("stage-4", "ignore"),
                    Node("stage-5", "ignore"),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Leg("stage-2", 1, "stage-5"),
                    Edge("stage-3", "stage-4"),
                ],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.Cycle(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                (
                    "stage-3",
                    LocalStageDescriptor.SelectAsync(
                        new ParallelismOptions { MaxConcurrency = 1 },
                        (Func<int, CancellationToken, Task<int>>)(async (value, token) =>
                        {
                            entered.TrySetResult();

                            await held.Task.WaitAsync(token).ConfigureAwait(false);

                            return value;
                        }))),
                ("stage-4", LocalStageDescriptor.Ignore()),
                ("stage-5", LocalStageDescriptor.Ignore())));

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable releasing = Completing(held);

        await Reaches(entered.Task, "the callback on one branch is running");

        Task paused = run.PauseAsync(TestToken);

        Assert.False(paused.IsCompleted);

        held.TrySetResult();

        await Reaches(paused, "the pause takes effect once the callback has finished");

        Assert.True(run.IsPaused);

        await Reaches(run.ResumeAsync(), "the run moves again");
        await run.DisposeAsync();

        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
    }

    [Fact]
    public async Task APausedChannelSinkBelowAJunctionHoldsItsElementAndTheRunComesToRest()
    {
        // A channel sink's write is this runtime's own wait on a channel the author owns, and a wait that
        // said nothing left the pause waiting forever on a segment nothing but a consumer could free. The
        // hole was not junction-specific and its regression lives with the linear suite; what this adds is
        // the shape that made it worth hunting for — the sink at the end of one leg while the rest of the
        // graph is quiet, which is where a branching run would have hung.
        Channel<int> channel = Channel.CreateBounded<int>(1);

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "cycle"),
                    Node("stage-2", "broadcast"),
                    Node("stage-3", "to-channel"),
                    Node("stage-4", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.Cycle(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", LocalStageDescriptor.ToChannel(channel.Writer)),
                ("stage-4", LocalStageDescriptor.Ignore())));

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // One element out of the channel and one in it leaves the sink holding a third with nowhere to put
        // it, which is the only state this leg can be in from here on.
        Assert.Equal(1, await channel.Reader.ReadAsync(TestToken));

        await Reaches(run.PauseAsync(TestToken), "the pause takes effect with a channel sink on one leg");

        Assert.True(run.IsPaused);

        await Reaches(run.ResumeAsync(), "the run moves again");
        await run.DisposeAsync();

        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
    }

    [Fact]
    public async Task AStormOnAGraphMixingEveryJunctionKindLosesNoElement()
    {
        // The storm again, on a graph in which a split, a join, a second split, and a row-building join all
        // stand at once: the elements are partitioned by parity, merged back, broadcast, and zipped with a
        // transformed copy of themselves. Every pause has to reach quiescence through all four pumps, and
        // what the run produced afterwards is the assertion — a merge promises the multiset rather than the
        // interleaving, so the multiset of rows is what a run that lost or duplicated an element could not
        // satisfy.
        const int Elements = 60;

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Emitter("stage-1"),
                    Node("stage-2", "partition"),
                    Node("stage-3", "select"),
                    Node("stage-4", "select"),
                    Node("stage-5", "merge"),
                    Node("stage-6", "broadcast"),
                    Node("stage-7", "select"),
                    Node("stage-8", "zip"),
                    Collect("stage-9", 512),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Leg("stage-2", 1, "stage-4"),
                    Into("stage-3", "stage-5", 0),
                    Into("stage-4", "stage-5", 1),
                    Edge("stage-5", "stage-6"),
                    Leg("stage-6", 0, "stage-7"),
                    Rejoins("stage-6", 1, "stage-8", 1),
                    Into("stage-7", "stage-8", 0),
                    Edge("stage-8", "stage-9"),
                ],
                [Control("emitted", "stage-1"), Slot("rows", "stage-9")]),
            Bindings(
                ("stage-1", Emitting<int>("emitted")),
                ("stage-2", Routing(value => value % 2)),
                ("stage-3", LocalStageDescriptor.Select((Func<int, int>)(value => value))),
                ("stage-4", LocalStageDescriptor.Select((Func<int, int>)(value => value))),
                ("stage-5", LocalStageDescriptor.Merge()),
                ("stage-6", LocalStageDescriptor.Broadcast()),
                ("stage-7", LocalStageDescriptor.Select((Func<int, int>)(value => value * 10))),
                ("stage-8", LocalStageDescriptor.Zip(Rows())),
                ("stage-9", CollectingRows(512))),
            Controls(("emitted", typeof(ISourceProbe<int>))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISourceProbe<int> source = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);

        for (int element = 1; element <= Elements; element++)
        {
            await source.EmitAsync(element, TestToken);
            await Reaches(run.PauseAsync(TestToken), $"the pause takes effect at element {element}");

            Assert.True(run.IsPaused);

            await Reaches(run.ResumeAsync(), $"the run moves again after element {element}");
        }

        source.Complete();

        await Reaches(run.Completion, "the stormed mixed graph reaches its end");

        string[] rows = await run.GetValueAsync(Result<string[]>(graph, "rows"), TestToken);

        // Each row pairs an element's transformed copy with itself, because a broadcast gives the same
        // element to both legs and a zip takes one from each: the split and the join realign without skew
        // however many times the run was held on the way.
        Assert.Equal(
            [.. Enumerable.Range(1, Elements).Select(value => $"{value * 10}-{value}").Order()],
            rows.Order());
    }

    [Fact]
    public async Task ShuttingDownADiamondDeliversEverythingItAdmitted()
    {
        // Shutdown across a branching topology is the same promise it is across a line: stop admitting and
        // deliver what is already inside. Everything admitted is a fact here rather than an estimate,
        // because an emit completes when the run has taken the element — so six emits are six elements the
        // run owns, and the assertion is that all six leave through the join.
        Gate gate = new();
        Lock counting = new();
        List<string> observed = [];

        RunnableGraph graph = Diamond(gate, counting, observed);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        ISourceProbe<int> source = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);

        for (int element = 1; element <= 6; element++)
        {
            await source.EmitAsync(element, TestToken);
        }

        await Reaches(gate.Reached, "the sink is holding the first row");

        Task shutdown = run.ShutdownAsync().AsTask();

        gate.Open();

        await Reaches(shutdown, "the shutdown drains the diamond");

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);

        // Every admitted element left through the join, and a drain discards nothing: the declared buffers
        // on both legs were delivered rather than abandoned.
        lock (counting)
        {
            Assert.Equal([.. Enumerable.Range(1, 6).Select(value => $"{value}-{value * 10}")], observed);
        }

        Assert.Equal(0L, run.DroppedElements);
    }

    [Fact]
    public async Task CancellingADiamondAbandonsWhatItsBuffersWereHolding()
    {
        // The other half of the accounting, on the same graph and with the same held row, so the drain and
        // the abandonment differ by nothing but the request. Cancellation is examined before the next
        // element, so the row inside the author's callback is finished and no row behind it is started.
        Gate gate = new();
        Lock counting = new();
        List<string> observed = [];

        RunnableGraph graph = Diamond(gate, counting, observed);

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        ISourceProbe<int> source = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);

        for (int element = 1; element <= 6; element++)
        {
            await source.EmitAsync(element, TestToken);
        }

        await Reaches(gate.Reached, "the sink is holding the first row");

        Task disposal = run.DisposeAsync().AsTask();

        gate.Open();

        await Reaches(disposal, "the cancellation abandons the diamond");

        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);

        // One row folded and nothing behind it: what the legs' buffers were holding is abandoned, which is
        // the whole of the difference from the shutdown of the same graph in the same state.
        lock (counting)
        {
            Assert.Equal(["1-10"], observed);
        }
    }

    [Fact]
    public async Task ShuttingDownAGraphMixingJunctionKindsDrainsEveryOneOfThem()
    {
        // Checkpoint 4 proved a shutdown mid-cycle and every earlier checkpoint proved one across its own
        // junction; what is left is a graph in which several kinds stand at once. A partition splits the
        // stream by parity, each leg is transformed on its own, and a merge puts the halves back together
        // behind a buffer — so the drain has to walk a split, a join, and a declared boundary in one run.
        Gate gate = new();
        Lock counting = new();
        List<int> observed = [];

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Emitter("stage-1"),
                    Node("stage-2", "partition"),
                    Node("stage-3", "select"),
                    Node("stage-4", "select"),
                    Node("stage-5", "merge"),
                    Buffer("stage-6", 8),
                    Node("stage-7", "for-each"),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Leg("stage-2", 1, "stage-4"),
                    Into("stage-3", "stage-5", 0),
                    Into("stage-4", "stage-5", 1),
                    Edge("stage-5", "stage-6"),
                    Edge("stage-6", "stage-7"),
                ],
                [Control("emitted", "stage-1")]),
            Bindings(
                ("stage-1", Emitting<int>("emitted")),
                ("stage-2", Routing(value => value % 2)),
                ("stage-3", LocalStageDescriptor.Select((Func<int, int>)(value => value))),
                ("stage-4", LocalStageDescriptor.Select((Func<int, int>)(value => value))),
                ("stage-5", LocalStageDescriptor.Merge()),
                ("stage-6", Buffering(8)),
                (
                    "stage-7",
                    Calling(value =>
                    {
                        gate.Wait();

                        lock (counting)
                        {
                            observed.Add(value);
                        }
                    }))),
            Controls(("emitted", typeof(ISourceProbe<int>))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        ISourceProbe<int> source = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);

        for (int element = 1; element <= 8; element++)
        {
            await source.EmitAsync(element, TestToken);
        }

        await Reaches(gate.Reached, "the sink is holding the first element");

        Task shutdown = run.ShutdownAsync().AsTask();

        gate.Open();

        await Reaches(shutdown, "the shutdown drains every junction of the graph");

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);

        int[] delivered;

        lock (counting)
        {
            delivered = [.. observed];
        }

        // A merge promises the multiset and not the interleaving, so the multiset is what is asserted: the
        // eight elements the run admitted are the eight it delivered, and a drain that had abandoned a leg
        // would be missing one parity of them.
        Assert.Equal([.. Enumerable.Range(1, 8)], delivered.Order());
        Assert.Equal(0L, run.DroppedElements);
    }

    [Fact]
    public async Task AFailureInOneBranchReachesItsSiblingAsCancellationAndNotAsCompletion()
    {
        // ADR 0005's first shared rule read across a fan-out rather than across a fan-in: a failure on one
        // branch is the run's failure, and every other branch learns about it as an abandonment. Proving
        // that it is an abandonment and not an ordinary end takes a branch that can tell the two apart,
        // which is an asynchronous callback holding the run's own token: it is told, and what it is told is
        // which of the two happened.
        InvalidOperationException failure = new("the branch refuses");
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<string> sibling = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource held = new(TaskCreationOptions.RunContinuationsAsynchronously);

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "cycle"),
                    Node("stage-2", "broadcast"),
                    Node("stage-3", "select"),
                    Node(
                        "stage-4",
                        "select-async",
                        "local-parallelism-parameters",
                        """{"maxConcurrency":1}"""),
                    Node("stage-5", "ignore"),
                    Node("stage-6", "ignore"),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Leg("stage-2", 1, "stage-4"),
                    Edge("stage-3", "stage-5"),
                    Edge("stage-4", "stage-6"),
                ],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.Cycle(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                (
                    "stage-3",
                    LocalStageDescriptor.Select((Func<int, int>)(_ =>
                    {
                        // The sibling has to be inside its callback before this branch fails, so that what
                        // the callback observes is the failure and not a race with its own admission.
                        entered.Task.GetAwaiter().GetResult();

                        throw failure;
                    }))),
                (
                    "stage-4",
                    LocalStageDescriptor.SelectAsync(
                        new ParallelismOptions { MaxConcurrency = 1 },
                        (Func<int, CancellationToken, Task<int>>)(async (value, token) =>
                        {
                            entered.TrySetResult();

                            try
                            {
                                await held.Task.WaitAsync(token).ConfigureAwait(false);

                                sibling.TrySetResult("completed");
                            }
                            catch (OperationCanceledException)
                            {
                                sibling.TrySetResult("cancelled");

                                throw;
                            }

                            return value;
                        }))),
                ("stage-5", LocalStageDescriptor.Ignore()),
                ("stage-6", LocalStageDescriptor.Ignore())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable releasing = Completing(held);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));

        // The sibling was told, and it was told the right thing: an author's callback on a healthy branch
        // sees the run's cancellation rather than a stream that quietly ended under it.
        Assert.Equal("cancelled", await sibling.Task);
    }

    [Theory]
    [InlineData("broadcast")]
    [InlineData("balance")]
    [InlineData("partition")]
    [InlineData("unzip")]
    [InlineData("merge")]
    [InlineData("concat")]
    [InlineData("interleave")]
    [InlineData("zip")]
    [InlineData("combine-latest")]
    [InlineData("cycle")]
    public async Task DisposingMidFlightEndsEveryJunctionKindAndReleasesWhatItHeld(string junction)
    {
        // The M2 disposal discipline, one junction kind at a time. Three claims, and the first of them is
        // the one that would hang rather than fail: disposal returns, which it can only do once every
        // segment thread has left its loop, so a pump that could not be woken from its own wait would
        // never let this test finish. Then the outcome is the cancellation that was asked for, and every
        // enumerator the run obtained was released — including the per-lap ones an endless source hands
        // out, which is where a branching run has more of them than a linear one ever did.
        Gate gate = new();
        RecordingEnumerable<int> left = new(1);
        RecordingEnumerable<int> right = new(2);
        RunnableGraph graph = Junction(junction, gate, left, right);

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(gate.Reached, "the run reaches the sink");

        Task disposal = run.DisposeAsync().AsTask();

        gate.Open();

        await Reaches(disposal, "the disposal of a branching run returns");

        Assert.Equal(TaskStatus.RanToCompletion, disposal.Status);
        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
        Assert.Equal(left.Enumerations, left.Releases);
        Assert.Equal(right.Enumerations, right.Releases);

        // Idempotent for a branching run exactly as for a linear one: nothing is left to cancel and the
        // outcome does not change.
        await run.DisposeAsync();

        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
    }

    [Fact]
    public async Task DemandThroughAFanInIsOnePullPerElementAndNoPrefetch()
    {
        // The demand claim of the probes, read through a junction rather than along a chain. The bound
        // worth reading is the same one every probe test reads — a run asks for at most one more element
        // than it has been given — and a junction that read ahead of its downstream demand would exceed it
        // on the very input it was reading ahead on.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Emitter("stage-1"),
                    Emitter("stage-2"),
                    Node("stage-3", "merge"),
                    Receiver("stage-4"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                [Control("left", "stage-1"), Control("right", "stage-2"), Control("joined", "stage-4")]),
            Bindings(
                ("stage-1", Emitting<int>("left")),
                ("stage-2", Emitting<int>("right")),
                ("stage-3", LocalStageDescriptor.Merge()),
                ("stage-4", Receiving<int>("joined"))),
            Controls(
                ("left", typeof(ISourceProbe<int>)),
                ("right", typeof(ISourceProbe<int>)),
                ("joined", typeof(ISinkProbe<int>))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISourceProbe<int> left = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("left"), TestToken);
        ISourceProbe<int> right = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("right"), TestToken);
        ISinkProbe<int> joined = await run.GetValueAsync(graph.Control<ISinkProbe<int>>("joined"), TestToken);

        for (int round = 1; round <= 4; round++)
        {
            await left.EmitAsync(round, TestToken);

            Assert.Equal(round, await joined.ReceiveAsync(TestToken));

            await right.EmitAsync(round * 10, TestToken);

            Assert.Equal(round * 10, await joined.ReceiveAsync(TestToken));

            Assert.InRange(left.PullsObserved, round, round + 1);
            Assert.InRange(right.PullsObserved, round, round + 1);
        }

        left.Complete();
        right.Complete();

        await joined.ExpectCompletedAsync(TestToken);
        await Reaches(run.Completion, "the run ends when both inputs have");
    }

    [Fact]
    public async Task ADroppingBoundaryBelowARowJunctionDropsRatherThanPacingIt()
    {
        // Checkpoint 3 recorded that the overflow policies are inherited by a junction's own boundaries and
        // untested there; this is the row-building half of that. A buffer written immediately below a
        // junction is that junction's output channel, so the policy the author declared is the policy the
        // junction's offer applies — and a dropping one answers rather than pacing the pump, which is what
        // makes the rows keep being built while the sink is held.
        Gate gate = new();
        Lock counting = new();
        List<string> observed = [];
        RecordingEnumerable<int> left = new(1, 2, 3, 4, 5, 6, 7, 8);
        RecordingEnumerable<int> right = new(10, 20, 30, 40, 50, 60, 70, 80);

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "zip"),
                    Buffer("stage-4", 2, "drop-newest"),
                    Node("stage-5", "for-each"),
                ],
                [
                    Into("stage-1", "stage-3", 0),
                    Into("stage-2", "stage-3", 1),
                    Edge("stage-3", "stage-4"),
                    Edge("stage-4", "stage-5"),
                ],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(left)),
                ("stage-2", LocalStageDescriptor.FromEnumerable(right)),
                ("stage-3", LocalStageDescriptor.Zip(Rows())),
                ("stage-4", Buffering(2)),
                (
                    "stage-5",
                    CallingRows(row =>
                    {
                        gate.Wait();

                        lock (counting)
                        {
                            observed.Add(row);
                        }
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(gate.Reached, "the sink is holding the first row");
        await Reaches(left.Released, "the left input runs out while the sink is held");
        await Reaches(right.Released, "the right input runs out while the sink is held");

        gate.Open();

        await Reaches(run.Completion, "the run completes once the sink is released");

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);

        int delivered;

        lock (counting)
        {
            delivered = observed.Count;
        }

        // Eight rows were built and every one of them is accounted for: delivered, or counted as dropped.
        // A dropping boundary never loses an element silently, and it never paced the junction either —
        // the inputs ran to their end with the sink still holding its first row.
        Assert.Equal(8L, delivered + run.DroppedElements);

        // Three is the floor rather than the answer, and it is an argument rather than a measurement: both
        // inputs ran out while the sink was held, and an input's segment ends only once every element it
        // had has been offered into a channel of one, so the junction had built at least six rows by then.
        // Of those, one was in the sink's hand and at most two in the declared buffer. How many more were
        // dropped depends on how far the junction ran ahead of a sink that had just been released, which is
        // a scheduling question this test has no business answering.
        Assert.True(
            run.DroppedElements >= 3L,
            $"a boundary of two below a held sink drops at least three of eight rows, and dropped {run.DroppedElements}");
    }

    [Fact]
    public async Task AFailingBoundaryBelowARowJunctionFaultsTheRun()
    {
        // The other end of the same rule: a boundary whose policy is to fail does fail, and the failure
        // travels from the junction's own offer to the run's outcome like any other. The sink is a probe
        // rather than a gate on purpose — a run whose failure has to reach a segment sitting inside an
        // author's callback cannot settle until that callback returns, while a probe holds the element in
        // one of this runtime's own waits, which the failure's cancellation releases.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "zip"),
                    Buffer("stage-4", 1, "fail"),
                    Receiver("stage-5"),
                ],
                [
                    Into("stage-1", "stage-3", 0),
                    Into("stage-2", "stage-3", 1),
                    Edge("stage-3", "stage-4"),
                    Edge("stage-4", "stage-5"),
                ],
                [Control("rows", "stage-5")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3, 4, 5, 6))),
                ("stage-2", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(10, 20, 30, 40, 50, 60))),
                ("stage-3", LocalStageDescriptor.Zip(Rows())),
                ("stage-4", Buffering(1)),
                ("stage-5", Receiving<string>("rows"))),
            Controls(("rows", typeof(ISinkProbe<string>))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISinkProbe<string> rows = await run.GetValueAsync(graph.Control<ISinkProbe<string>>("rows"), TestToken);

        BufferOverflowException overflowed =
            await Assert.ThrowsAsync<BufferOverflowException>(() => run.Completion);

        Assert.Contains("1", overflowed.Message, StringComparison.Ordinal);

        // The consumer on the far side of the junction is told what happened rather than left waiting, and
        // it is told with the run's own exception instance.
        Assert.Same(overflowed, await rows.ExpectFailedAsync(TestToken));
    }

    /// <summary>Receives from one probe sink until the run it belongs to has ended.</summary>
    /// <param name="probe">The probe to drain.</param>
    /// <param name="into">The list every element is added to, shared with the other legs' drains.</param>
    /// <param name="guard">The lock that list is guarded by.</param>
    /// <returns>The task that completes when the run has ended.</returns>
    /// <remarks>
    /// The termination is the loop's exit condition rather than an accident: no wait of a probe survives
    /// the run it belongs to, so a receive issued into a run that is ending is answered with a
    /// <see cref="ProbeTerminatedException"/> naming the outcome instead of being left pending. That is
    /// what makes draining several legs at once terminate at all.
    /// </remarks>
    private static async Task Drained(ISinkProbe<int> probe, List<int> into, Lock guard)
    {
        while (true)
        {
            int element;

            try
            {
                element = await probe.ReceiveAsync(TestToken).ConfigureAwait(false);
            }
            catch (ProbeTerminatedException)
            {
                return;
            }

            lock (guard)
            {
                into.Add(element);
            }
        }
    }

    /// <summary>Builds the diamond both drain tests are asserted against.</summary>
    /// <param name="gate">The hold the sink applies to its first row.</param>
    /// <param name="counting">The lock guarding <paramref name="observed"/>.</param>
    /// <param name="observed">The rows the sink folded, in order.</param>
    /// <returns>The graph, declaring the source probe under the name <c>emitted</c>.</returns>
    /// <remarks>
    /// One split, two branches with a declared buffer each, and one join: the shape whose drain has to
    /// deliver what two boundaries were holding through a junction that builds a row from both of them.
    /// The buffers are what make the claim about the drain rather than about the handoffs — without them
    /// the sink's own hold would be the only thing the run was carrying.
    /// </remarks>
    private static RunnableGraph Diamond(Gate gate, Lock counting, List<string> observed) =>
        Graph(
            Declaring(
                [
                    Emitter("stage-1"),
                    Node("stage-2", "broadcast"),
                    Buffer("stage-3", 4),
                    Buffer("stage-4", 4),
                    Node("stage-5", "select"),
                    Node("stage-6", "zip"),
                    Node("stage-7", "for-each"),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Leg("stage-2", 1, "stage-4"),
                    Edge("stage-4", "stage-5"),
                    Into("stage-3", "stage-6", 0),
                    Into("stage-5", "stage-6", 1),
                    Edge("stage-6", "stage-7"),
                ],
                [Control("emitted", "stage-1")]),
            Bindings(
                ("stage-1", Emitting<int>("emitted")),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", Buffering(4)),
                ("stage-4", Buffering(4)),
                ("stage-5", LocalStageDescriptor.Select((Func<int, int>)(value => value * 10))),
                ("stage-6", LocalStageDescriptor.Zip(Rows())),
                (
                    "stage-7",
                    CallingRows(row =>
                    {
                        gate.Wait();

                        lock (counting)
                        {
                            observed.Add(row);
                        }
                    }))),
            Controls(("emitted", typeof(ISourceProbe<int>))));

    /// <summary>Builds a run of one junction kind that keeps moving until something stops it.</summary>
    /// <param name="junction">The junction to stand in the middle, by its stage identifier text.</param>
    /// <param name="gate">The hold the sink applies to its first element.</param>
    /// <param name="left">The first endless source, whose enumerations this test counts.</param>
    /// <param name="right">The second endless source, used by the joining shapes.</param>
    /// <returns>The graph.</returns>
    /// <remarks>
    /// Endless sources on purpose: a run that would end by itself proves nothing about a disposal reaching
    /// a pump mid-flight. The splitting shapes take one source and two terminals, the joining shapes take
    /// two sources and one, and the cycle is the shape that has no source of its own at all — its stream is
    /// what is circulating inside it, which is why a stop is the only thing that ends it.
    /// </remarks>
    private static RunnableGraph Junction(
        string junction,
        Gate gate,
        RecordingEnumerable<int> left,
        RecordingEnumerable<int> right)
    {
        Action<int> holding = _ => gate.Wait();

        if (junction is "cycle")
        {
            return Graph(
                Declaring(
                    [
                        Node("stage-1", "cycle"),
                        Node("stage-2", "merge"),
                        Node("stage-3", "broadcast"),
                        Node("stage-4", "for-each"),
                        Buffer("stage-5", 4, "drop-oldest"),
                    ],
                    [
                        Into("stage-1", "stage-2", 0),
                        Edge("stage-2", "stage-3"),
                        Leg("stage-3", 0, "stage-4"),
                        Leg("stage-3", 1, "stage-5"),
                        Into("stage-5", "stage-2", 1),
                    ],
                    []),
                Bindings(
                    ("stage-1", LocalStageDescriptor.Cycle(left)),
                    ("stage-2", LocalStageDescriptor.Merge()),
                    ("stage-3", LocalStageDescriptor.Broadcast()),
                    ("stage-4", Calling(holding)),
                    ("stage-5", Buffering(4))));
        }

        if (junction is "broadcast" or "balance" or "partition" or "unzip")
        {
            LocalStageDescriptor splitting = junction switch
            {
                "broadcast" => LocalStageDescriptor.Broadcast(),
                "balance" => LocalStageDescriptor.Balance(),
                "partition" => Routing(_ => 0),
                _ => LocalStageDescriptor.Unzip(
                    (Func<int, int>)(value => value),
                    (Func<int, int>)(value => value)),
            };

            GraphEdge[] legs = junction is "unzip"
                ? [Half("stage-2", "left", "stage-3"), Half("stage-2", "right", "stage-4")]
                : [Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")];

            return Graph(
                Declaring(
                    [
                        Node("stage-1", "cycle"),
                        Node("stage-2", junction),
                        Node("stage-3", "for-each"),
                        Node("stage-4", "for-each"),
                    ],
                    [Edge("stage-1", "stage-2"), .. legs],
                    []),
                Bindings(
                    ("stage-1", LocalStageDescriptor.Cycle(left)),
                    ("stage-2", splitting),
                    ("stage-3", Calling(holding)),
                    ("stage-4", Calling(_ => { }))));
        }

        StageNode node = junction is "interleave" ? Interleaving("stage-3", 1) : Node("stage-3", junction);
        LocalStageDescriptor joining = junction switch
        {
            "merge" => LocalStageDescriptor.Merge(),
            "concat" => LocalStageDescriptor.Concat(),
            "interleave" => LocalStageDescriptor.Interleave(1),
            "zip" => LocalStageDescriptor.Zip(Rows()),
            _ => LocalStageDescriptor.CombineLatest(Rows()),
        };

        LocalStageDescriptor sink = junction is "zip" or "combine-latest"
            ? CallingRows(_ => gate.Wait())
            : Calling(holding);

        return Graph(
            Declaring(
                [
                    Node("stage-1", "cycle"),
                    Node("stage-2", "cycle"),
                    node,
                    Node("stage-4", "for-each"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.Cycle(left)),
                ("stage-2", LocalStageDescriptor.Cycle(right)),
                ("stage-3", joining),
                ("stage-4", sink)));
    }
}
