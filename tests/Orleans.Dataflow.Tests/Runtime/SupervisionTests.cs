using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.JunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What each of the four supervision forms promises, proved by the values a run produces rather than by the
/// number of them.
/// </summary>
/// <remarks>
/// <para>
/// The pair that carries the milestone is the first two tests. They are the <em>same graph</em> over the
/// same elements with the same injected failure, differing in one enumeration member, and their outputs
/// differ in what a scan inside the scope was holding: <c>[1, 4, 8]</c> when the state is kept and
/// <c>[1, 3, 7]</c> when it resets. A test that asserted "three elements arrived" would pass for both and
/// would have proved nothing about either.
/// </para>
/// <para>
/// The failure is injected by a fault point rather than by a throwing lambda, because the arming is in the
/// document: which element fails is a fact of the graph under test, so the two runs above fail at the same
/// element by construction and not by the two tests agreeing to count the same way.
/// </para>
/// </remarks>
public sealed class SupervisionTests
{
    [Fact]
    public async Task ResumeDropsTheFailingElementAndKeepsTheScopesState()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>()
                    .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2))
                    .Scan(0, (running, value) => running + value))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The second element is dropped and the running sum never saw it, so the third folds onto 1 and the
        // fourth onto 4. The sums are the state, and the state survived.
        Assert.Equal([1, 4, 8], observed);
        Assert.Equal(1, run.SupervisedFailures);
        Assert.Equal(0, run.PoisonElements);
    }

    [Fact]
    public async Task RestartStageDropsTheFailingElementAndResetsTheScopesState()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.RestartStage },
                Flow.For<int>()
                    .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2))
                    .Scan(0, (running, value) => running + value))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // One enumeration member away from the test above, and the running sum starts again from its seed:
        // the third element folds onto 0 and the fourth onto 3.
        Assert.Equal([1, 3, 7], observed);
        Assert.Equal(1, run.SupervisedFailures);
    }

    [Fact]
    public async Task RestartStageResetsEveryStageOfTheScopeAndNotOnlyTheFirst()
    {
        List<int> observed = [];

        // A batch is the sharpest reading of "reset": it abandons the group it had open, so the elements
        // that were in it never arrive anywhere. The scope batches by two, and the failure lands while a
        // group is half full.
        RunnableGraph graph = Source.From([1, 2, 3, 4, 5])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.RestartStage },
                Flow.For<int>()
                    .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2))
                    .Grouped(2)
                    .Select(group => group.Sum()))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Element 1 opens a group; element 2 fails and the open group is abandoned with it; elements 3 and 4
        // fill a fresh group and 5 is handed over at the end of the stream. A resuming scope would have
        // emitted 1 + 3 first.
        Assert.Equal([7, 5], observed);
    }

    [Fact]
    public async Task ResumeKeepsAHalfFilledBatchAcrossTheFailure()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4, 5])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>()
                    .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2))
                    .Grouped(2)
                    .Select(group => group.Sum()))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The contrast with the test above, on one enumeration member: the group holding 1 is still open
        // when 3 arrives, so 1 and 3 leave together and 4 and 5 follow.
        Assert.Equal([4, 9], observed);
    }

    [Fact]
    public async Task RetryOffersTheElementAgainUntilItSucceeds()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Retry, MaxAttempts = 3 },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2)))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The second arrival is the second element's first attempt; its re-offer is the third arrival and
        // passes. Nothing is lost, which is the whole difference from resuming.
        Assert.Equal([1, 2, 3], observed);
        Assert.Equal(1, run.SupervisedFailures);
        Assert.Equal(0, run.PoisonElements);
    }

    [Fact]
    public async Task ARetriedElementIsOfferedToTheScopesFirstStageAgain()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Retry, MaxAttempts = 3 },
                Flow.For<int>()
                    .Scan(0, (running, value) => running + value)
                    .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2)))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The declared semantics, asserted rather than footnoted: the scan is above the fault point, so the
        // re-offer folds the element a second time and the run sees 1 and then 1 + 2 + 2. This is why a
        // retrying scope is kept small and why the exhaustion answer can escalate to a restart.
        Assert.Equal([1, 5], observed);
    }

    [Fact]
    public async Task RetryExhaustionFailsTheRunByDefault()
    {
        RunnableGraph graph = Source.From([1, 2, 3])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Retry, MaxAttempts = 2 },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(FaultPointMode.Always, firstFailure: 1)))
            .To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        FaultInjectedException failed =
            await Assert.ThrowsAsync<FaultInjectedException>(async () => await run.Completion);

        // The exception is the last attempt's own instance and nothing wraps it, which is the rule the
        // engine has had since M2 read through a scope that ran out of attempts.
        Assert.Equal(2, failed.Arrival);
        Assert.Equal(2, run.SupervisedFailures);
        Assert.Equal(1, run.PoisonElements);
    }

    [Fact]
    public async Task RetryExhaustionEscalatesToResume()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Supervised(
                new SupervisionOptions
                {
                    Form = SupervisionForm.Retry,
                    MaxAttempts = 2,
                    OnExhaustion = RetryExhaustion.Resume,
                },
                Flow.For<int>()
                    .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 1))
                    .Scan(0, (running, value) => running + value))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Element 1 fails once and its re-offer succeeds, so it is folded; elements 2 and 3 follow. Nothing
        // is exhausted here — this test is the escalation's baseline, and the next one is the escalation.
        Assert.Equal([1, 3, 6], observed);
        Assert.Equal(0, run.PoisonElements);
    }

    [Fact]
    public async Task AnExhaustedRetryThatResumesDropsThePoisonElementAndKeepsTheState()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Supervised(
                new SupervisionOptions
                {
                    Form = SupervisionForm.Retry,
                    MaxAttempts = 2,
                    OnExhaustion = RetryExhaustion.Resume,
                },
                Flow.For<int>()
                    .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 1))
                    .Scan(0, (running, value) => running + value)
                    .Via(TestFlow.FaultPoint<int>(FaultPointMode.Always, firstFailure: 2)))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The second fault point is below the scan and fails from its second arrival onwards, so elements 2
        // and 3 exhaust their two attempts each and are dropped — while the scan, which saw each of them
        // twice, keeps everything it folded.
        Assert.Equal([1], observed);
        Assert.Equal(2, run.PoisonElements);
        Assert.Equal(5, run.SupervisedFailures);
    }

    [Fact]
    public async Task AnExhaustedRetryThatRestartsResetsTheScope()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Supervised(
                new SupervisionOptions
                {
                    Form = SupervisionForm.Retry,
                    MaxAttempts = 2,
                    OnExhaustion = RetryExhaustion.RestartStage,
                },
                Flow.For<int>()
                    .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2))
                    .Scan(0, (running, value) => running + value))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Element 1 passes and is folded to 1. Element 2's first attempt fails at the fault point, which is
        // above the scan, so the scan never saw it; its re-offer passes and folds to 3. Nothing is
        // exhausted, so the state stands and element 3 folds to 6.
        Assert.Equal([1, 3, 6], observed);
        Assert.Equal(0, run.PoisonElements);
    }

    [Fact]
    public async Task RecoverEmitsTheFallbackAndEndsTheStreamSuccessfully()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Recover },
                Flow.For<int>()
                    .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 3))
                    .Select(value => value * 10),
                fallback: -1)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The fallback travels downstream and the stream ends after it: the fourth element never arrives,
        // and the run reports success rather than the failure that ended it. That is the boundary the
        // matrix demands be distinct from switching to an alternate source.
        Assert.Equal([10, 20, -1], observed);
        Assert.Equal(1, run.SupervisedFailures);
    }

    [Fact]
    public async Task RecoverResolvesTheResultAsAnOrdinaryCompletionWould()
    {
        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Recover },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 3)),
                fallback: 100)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // 1 + 2 + the fallback. The slot resolves rather than faulting, which is what "ends the stream
        // successfully" means to everything downstream of the scope.
        Assert.Equal(103L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task AScopeThatSeesNoFailureIsTheChainItWraps()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>().Select(value => value * 2).Where(value => value != 4))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([2, 6], observed);
        Assert.Equal(0, run.SupervisedFailures);
        Assert.Equal(0, run.PoisonElements);
    }

    [Fact]
    public async Task AScopeHandsOverWhatItsChainHeldWhenTheStreamEnds()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>().Grouped(2).Select(group => group.Sum()))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The end of the stream reaches the scope's stages exactly as it reaches a segment's, so the partial
        // group leaves rather than being lost with the scope.
        Assert.Equal([3, 3], observed);
    }

    [Fact]
    public async Task AStageInsideAScopeThatEndsItsStreamEndsTheScopesStream()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>().Take(2))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // A scope is a stage of a chain, so a bound inside it is a bound on the chain: everything above the
        // scope stops and the run completes, exactly as a top-level take does.
        Assert.Equal([1, 2], observed);
    }

    [Fact]
    public async Task ARetryOfOneAttemptAppliesItsExhaustionAnswerToTheFirstFailure()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Supervised(
                new SupervisionOptions
                {
                    Form = SupervisionForm.Retry,
                    MaxAttempts = 1,
                    OnExhaustion = RetryExhaustion.Resume,
                },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2)))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // One attempt is legal and means no re-offer, which is a resuming scope written the long way. It is
        // admitted because a graph generated from configuration may legitimately turn the retries down to
        // none, and the poison count is what says the element was given up on rather than merely dropped.
        Assert.Equal([1, 3], observed);
        Assert.Equal(1, run.PoisonElements);
        Assert.Equal(1, run.SupervisedFailures);
    }

    [Fact]
    public async Task AScopeWhoseChainWantsNoElementEndsTheStreamAtItsFirst()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Supervised(new SupervisionOptions { Form = SupervisionForm.Resume }, Flow.For<int>().Take(0))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // A scope does not forward its chain's "completes before any element", so the source is pulled once
        // and the stream ends on that element rather than before it. Stated because it is the one place a
        // scope is observably not the chain it wraps.
        Assert.Empty(observed);
    }

    [Fact]
    public async Task TwoFaultPointsInOneGraphAreTwoControls()
    {
        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .Via(TestFlow.FaultPoint<int>("first", FaultPointMode.Never, firstFailure: 1))
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2)))
            .Via(TestFlow.FaultPoint<int>("second", FaultPointMode.Never, firstFailure: 1))
            .To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        IFaultPoint first = await run.GetValueAsync(graph.Control<IFaultPoint>("first"), TestToken);
        IFaultPoint second = await run.GetValueAsync(graph.Control<IFaultPoint>("second"), TestToken);

        // Three fault points in one chain: two named and one inside the scope with no name at all. The two
        // controls resolve to two objects with two counters, and the third — which nothing can reach —
        // still did its job, which the element the scope dropped is the evidence of.
        Assert.NotSame(first, second);
        Assert.Equal(4, first.ElementsSeen);
        Assert.Equal(3, second.ElementsSeen);
    }

    [Fact]
    public async Task AScopeFusesAndPullsNoFurtherThanTheElementInItsHand()
    {
        RecordingEnumerable<int> source = new(1, 2, 3, 4, 5, 6);
        Gate held = new();

        RunnableGraph graph = Source.From(source)
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
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

        // A scope is a fused stage and not a boundary, so the source has run exactly as far as the element
        // the callback is holding. A stage that opened a segment of its own would have run one further,
        // into the handoff in front of it — which is the accounting every bounded-memory claim in this
        // suite makes.
        Assert.Equal(2, source.Pulls);

        held.Open();

        await run.Completion;
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public async Task AScopeThatSeesNoFailureProducesWhatTheSameChainProducesUnwrapped(int shape)
    {
        // The invariant rather than a fact: whatever the chain does, the scope around it does the same. It
        // is written as a comparison so that it cannot pass by agreeing with a number somebody wrote down —
        // the chains chosen are the ones where a scope's own seams meet the run's, including two that end
        // their own stream part way and two that hand over a residue at the end of it.
        Flow<int, int> chain = Chain(shape);

        RunnableGraph wrapped = Source.From([1, 2, 3, 4, 5])
            .Supervised(new SupervisionOptions { Form = SupervisionForm.Resume }, chain)
            .To(s => s.Collect(new CollectOptions { MaxElements = 32 }), "seen", out ResultSlot<IReadOnlyList<int>> inside);

        RunnableGraph bare = Source.From([1, 2, 3, 4, 5])
            .Via(chain)
            .To(s => s.Collect(new CollectOptions { MaxElements = 32 }), "seen", out ResultSlot<IReadOnlyList<int>> outside);

        await using RunHandle supervised = await Host.MaterializeAsync(wrapped, TestToken);
        await using RunHandle plain = await Host.MaterializeAsync(bare, TestToken);

        await supervised.Completion;
        await plain.Completion;

        Assert.Equal(
            await plain.GetValueAsync(outside, TestToken),
            await supervised.GetValueAsync(inside, TestToken));
        Assert.Equal(0, supervised.SupervisedFailures);
    }

    [Fact]
    public async Task OneSupervisedFlowValueComposedTwiceIsTwoScopesWithTwoStates()
    {
        List<int> first = [];
        List<int> second = [];

        // A flow is an immutable reusable value and a scope is a stage of one, so composing it twice has to
        // give two scopes with two instances of everything inside them — the scan's state and the fault
        // point's arrival counter alike.
        Flow<int, int> supervised = Flow.For<int>()
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>()
                    .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2))
                    .Scan(0, (running, value) => running + value));

        RunnableGraph one = Source.From([1, 2, 3]).Via(supervised).To(s => s.ForEach(first.Add));
        RunnableGraph other = Source.From([1, 2, 3]).Via(supervised).To(s => s.ForEach(second.Add));

        await using RunHandle running = await Host.MaterializeAsync(one, TestToken);
        await running.Completion;

        await using RunHandle rerunning = await Host.MaterializeAsync(other, TestToken);
        await rerunning.Completion;

        Assert.Equal([1, 4], first);
        Assert.Equal(first, second);
    }

    [Fact]
    public async Task RecoverOnTheVeryFirstElementEmitsOnlyTheFallback()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Recover },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 1)),
                fallback: -1)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([-1], observed);
    }

    [Fact]
    public async Task RecoverThroughADeclaredBufferDeliversTheFallbackAndCompletes()
    {
        List<int> observed = [];

        // A boundary below the scope is where an early completion has somewhere to go wrong: the fallback
        // has to cross the buffer and the segments below it have to drain rather than be cut off.
        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Recover },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 3)),
                fallback: -1)
            .Buffer(new BufferOptions { Capacity = 4 })
            .Select(value => value * 2)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([2, 4, -2], observed);
    }

    [Fact]
    public async Task TwoScopesInOneChainKeepTheirOwnPolicies()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2)))
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Retry, MaxAttempts = 2 },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2)))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The first scope drops its second arrival — element 2 — and the second scope retries its own
        // second arrival, which is element 4. Two scopes, two counters of arrivals, two answers.
        Assert.Equal([1, 3, 4], observed);
        Assert.Equal(2, run.SupervisedFailures);
    }

    /// <summary>Builds one of the chains the wrap-versus-bare comparison runs over.</summary>
    /// <param name="shape">Which chain to build.</param>
    /// <returns>The chain.</returns>
    /// <remarks>
    /// Chosen for where a scope's seams are rather than for coverage: an identity, a chain that holds a
    /// residue to the end of the stream, one that ends its own stream part way, one that does both, and one
    /// whose batch is still open when a bound below it closes.
    /// </remarks>
    private static Flow<int, int> Chain(int shape) => shape switch
    {
        0 => Flow.For<int>(),
        1 => Flow.For<int>().Grouped(2).Select(group => group.Sum()),
        2 => Flow.For<int>().Take(3).Select(value => value * 10),
        3 => Flow.For<int>().Grouped(2).Select(group => group.Sum()).Take(1),
        _ => Flow.For<int>().Take(3).Grouped(2).Select(group => group.Sum()),
    };
}
