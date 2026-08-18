using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;
using static Orleans.Dataflow.Tests.Runtime.TimingFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// Where a supervision scope composes, where it refuses to, and what a retrying scope's waits are worth
/// against a clock the test moves by hand.
/// </summary>
/// <remarks>
/// <para>
/// A scope is a stage, so "it composes with everything that already composes" is a claim rather than a
/// definition, and the compositions asserted here are the ones ADR 0007 named: a scope on a junction leg,
/// two scopes in one chain, and a fault point inside a scope driving all of it. What is refused is refused
/// by name — a scope inside a group flow and a scope inside a scope — and the refusals say why.
/// </para>
/// <para>
/// The backoff assertions are written the way every timing test in this suite is: the clock never advances
/// by itself, so the run is advanced to one tick short of the rung, asserted to have done nothing, and then
/// advanced the last tick. What each attempt is measured by is the fault point's own factory, which is
/// called at the moment of the throw and records the clock's reading there.
/// </para>
/// </remarks>
public sealed class SupervisionCompositionTests
{
    [Fact]
    public async Task AScopeOnAJunctionLegSupervisesThatLegAndNotItsSibling()
    {
        RunnableGraph graph = Source.From([1, 2, 3]).BroadcastTo(
            Flow.For<int>()
                .Supervised(
                    new SupervisionOptions { Form = SupervisionForm.Resume },
                    Flow.For<int>().Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2)))
                .To(s => s.Aggregate(0L, (sum, value) => sum + value), "supervised", out ResultSlot<long> supervised),
            Flow.For<int>()
                .To(s => s.Aggregate(0L, (sum, value) => sum + value), "plain", out ResultSlot<long> plain));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The supervised leg lost its second element and the sibling kept all three. ADR 0005's "failure
        // wins" is exactly what a scope weakens, and it weakens it for the leg the scope is on.
        Assert.Equal(4L, await run.GetValueAsync(supervised, TestToken));
        Assert.Equal(6L, await run.GetValueAsync(plain, TestToken));
        Assert.Equal(1, run.SupervisedFailures);
    }

    [Fact]
    public async Task AFailureOnAnUnsupervisedLegStillFailsTheWholeRun()
    {
        RunnableGraph graph = Source.From([1, 2, 3]).BroadcastTo(
            Flow.For<int>()
                .Supervised(new SupervisionOptions { Form = SupervisionForm.Resume }, Flow.For<int>())
                .To(s => s.Count(), "supervised", out _),
            Flow.For<int>()
                .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2))
                .To(s => s.Count(), "plain", out _));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // The contrast, sideways: a scope on one leg is not a policy for the graph, so the sibling's failure
        // reaches the run unchanged.
        _ = await Assert.ThrowsAsync<FaultInjectedException>(async () => await run.Completion);
    }

    [Fact]
    public void AScopeIsRefusedInsideAGroupFlowByName()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Source.From([1, 2]).GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value,
                Flow.For<int>().Supervised(
                    new SupervisionOptions { Form = SupervisionForm.Resume },
                    Flow.For<int>())));

        // A scope reads the run's clock, so it falls under the clause a group flow has always had: one
        // instance per key of a thing that wants a run of its own is not something a fused stage can hold.
        Assert.Equal("group", refused.ParamName);
        Assert.Contains("'local/supervised@v1' at position 1", refused.Message, StringComparison.Ordinal);
        Assert.Contains("a stage that reads the clock", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AScopeInsideAScopeIsRefusedByName()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Source.From([1, 2]).Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>().Supervised(
                    new SupervisionOptions { Form = SupervisionForm.RestartStage },
                    Flow.For<int>())));

        // A policy inside a policy is a real feature with a contract of its own to state — which answer
        // wins, what a restart of the outer one does to the inner one's state — and it is not this one.
        Assert.Equal("scope", refused.ParamName);
        Assert.Contains("'local/supervised@v1' at position 1", refused.Message, StringComparison.Ordinal);
        Assert.Contains("a nested scope and a group-by are refused", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AFlatteningStageIsRefusedInsideAScopeWithItsOwnReason()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Source.From([1, 2]).Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>().SelectMany(value => new[] { value, value })));

        // The refusal a reader has to be able to find, because letting it through would be supervision that
        // silently did not apply: the sequence is read after the scope's own method has returned.
        Assert.Equal("scope", refused.ParamName);
        Assert.Contains(
            "a failure inside it would fall outside the scope it appears to be in",
            refused.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task AGroupByInsideAScopeIsRefusedAndAScopeAroundOneIsNot()
    {
        _ = Assert.Throws<ArgumentException>(
            () => Source.From([1, 2]).Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>().GroupBy(
                    new GroupByOptions { MaxActiveKeys = 2 },
                    value => value,
                    Flow.For<int>())));

        List<int> observed = [];

        // The composition that is not refused, and it is the useful one: the keyed stage stands beside the
        // scope rather than inside it, so a failure downstream of the grouping is contained.
        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .GroupBy(new GroupByOptions { MaxActiveKeys = 2 }, value => value % 2, Flow.For<int>())
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 3)))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2, 4], observed);
    }

    [Fact]
    public async Task RetryWaitsTheDeclaredLadderOnTheRunsClock()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        DateTimeOffset start = clock.GetUtcNow();
        List<TimeSpan> attempts = [];

        RunnableGraph graph = Source.From([1])
            .Supervised(
                new SupervisionOptions
                {
                    Form = SupervisionForm.Retry,
                    MaxAttempts = 3,
                    Backoff = [Second, Second + Second],
                },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(
                    FaultPointMode.Always,
                    firstFailure: 1,
                    _ =>
                    {
                        attempts.Add(clock.GetUtcNow() - start);

                        return new FaultInjectedException("injected");
                    })))
            .To(Sink.Ignore<int>());

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await Reaches(() => attempts.Count == 1, "the first attempt", TestToken);
        await clock.AdvanceAsync(1, Second - Instant, TestToken);

        // Not one tick before the rung the document declares.
        Assert.Single(attempts);

        clock.Advance(Instant);

        await Reaches(() => attempts.Count == 2, "the second attempt", TestToken);
        await clock.AdvanceAsync(1, Second + Second, TestToken);
        await Reaches(() => attempts.Count == 3, "the third attempt", TestToken);

        _ = await Assert.ThrowsAsync<FaultInjectedException>(async () => await run.Completion);

        Assert.Equal([TimeSpan.Zero, Second, Second + Second + Second], attempts);
        Assert.Equal(1, run.PoisonElements);
    }

    [Fact]
    public async Task TheLastRungOfTheLadderRepeats()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        DateTimeOffset start = clock.GetUtcNow();
        List<TimeSpan> attempts = [];

        RunnableGraph graph = Source.From([1])
            .Supervised(
                new SupervisionOptions
                {
                    Form = SupervisionForm.Retry,
                    MaxAttempts = 4,
                    Backoff = [Second],
                    OnExhaustion = RetryExhaustion.Resume,
                },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(
                    FaultPointMode.Always,
                    firstFailure: 1,
                    _ =>
                    {
                        attempts.Add(clock.GetUtcNow() - start);

                        return new FaultInjectedException("injected");
                    })))
            .To(Sink.Ignore<int>());

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        for (int rung = 1; rung <= 3; rung++)
        {
            int reached = rung;

            await Reaches(() => attempts.Count == reached, "the next attempt", TestToken);
            await clock.AdvanceAsync(1, Second, TestToken);
        }

        await run.Completion;

        // A ladder shorter than the attempt count is legal and reads as "and then this long every time".
        Assert.Equal([TimeSpan.Zero, Second, Second + Second, Second + Second + Second], attempts);
    }

    [Fact]
    public async Task AnEmptyLadderRetriesAtOnce()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        DateTimeOffset start = clock.GetUtcNow();
        List<TimeSpan> attempts = [];

        RunnableGraph graph = Source.From([1])
            .Supervised(
                new SupervisionOptions
                {
                    Form = SupervisionForm.Retry,
                    MaxAttempts = 3,
                    OnExhaustion = RetryExhaustion.Resume,
                },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(
                    FaultPointMode.Always,
                    firstFailure: 1,
                    _ =>
                    {
                        attempts.Add(clock.GetUtcNow() - start);

                        return new FaultInjectedException("injected");
                    })))
            .To(Sink.Ignore<int>());

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Nothing was ever waited for and nothing armed a timer, so the run finished without the clock
        // moving at all — which is the honest encoding of a retry an author declared without a wait.
        Assert.Equal([TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero], attempts);
        Assert.Equal(0, clock.PendingTimers);
    }

    [Fact]
    public async Task APauseDuringABackoffWaitHoldsTheRunAndAResumeReleasesIt()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<TimeSpan> attempts = [];
        DateTimeOffset start = clock.GetUtcNow();

        RunnableGraph graph = Source.From([1])
            .Supervised(
                new SupervisionOptions
                {
                    Form = SupervisionForm.Retry,
                    MaxAttempts = 2,
                    Backoff = [Second],
                    OnExhaustion = RetryExhaustion.Resume,
                },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(
                    FaultPointMode.Always,
                    firstFailure: 1,
                    _ =>
                    {
                        attempts.Add(clock.GetUtcNow() - start);

                        return new FaultInjectedException("injected");
                    })))
            .To(Sink.Ignore<int>());

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await Reaches(() => attempts.Count == 1, "the first attempt", TestToken);
        await clock.WaitForTimersAsync(1, TestToken);

        // A backoff wait is one of this runtime's own, so it reports itself idle and a pause of a run that
        // is sitting in one takes effect at once rather than after the rung.
        await run.PauseAsync(TestToken);

        clock.Advance(Second + Second);

        await Reaches(() => clock.PendingTimers == 0, "the backoff wait firing", TestToken);

        // The wait is over and the run is still held: a park is where the segment comes to rest, so the
        // re-offer cannot happen until the pause is withdrawn however far the clock has moved.
        Assert.True(run.IsPaused);
        Assert.Single(attempts);

        await run.ResumeAsync();
        await run.Completion;

        // The wait was already over when the pause released, so the second attempt happens at the moment
        // the clock had reached — the park is a safe point, not a second deadline.
        Assert.Equal(2, attempts.Count);
    }

    [Fact]
    public async Task AShutdownDuringABackoffWaitReleasesItAndTheElementIsDelivered()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Retry, MaxAttempts = 2, Backoff = [Second] },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 1)))
            .To(s => s.ForEach(observed.Add));

        DateTimeOffset start = clock.GetUtcNow();

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await clock.WaitForTimersAsync(1, TestToken);
        await run.ShutdownAsync();
        await run.Completion;

        // The graceful stop released the wait and the re-offer happened without the rest of the rung being
        // paid, so the element in hand was delivered — the rule the delay and the throttle already follow,
        // read over a retry. The clock never moved, so nothing here was waited out.
        Assert.Equal([1], observed);
        Assert.Equal(start, clock.GetUtcNow());
    }

    [Fact]
    public async Task ACancellationDuringABackoffWaitEndsTheRunCancelled()
    {
        using CancellationTokenSource cancellation = new();
        LocalDataflowHost host = Timed(out TestClock clock);

        RunnableGraph graph = Source.From([1, 2, 3])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Retry, MaxAttempts = 2, Backoff = [Second] },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 1)))
            .To(Sink.Ignore<int>());

        await using RunHandle run = await host.MaterializeAsync(graph, cancellation.Token);

        await clock.WaitForTimersAsync(1, TestToken);
        await cancellation.CancelAsync();

        // A cancellation releases the wait and is raised rather than swallowed: the run ends cancelled and
        // the element in hand is abandoned, which is the other half of the pair the shutdown test above
        // asserts.
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run.Completion);

        Assert.Equal(0, run.PoisonElements);
    }

    [Fact]
    public async Task TheSameDocumentTwiceProducesTheSameStream()
    {
        RunnableGraph graph = Source.From([1, 2, 3, 4, 5])
            .Supervised(
                new SupervisionOptions
                {
                    Form = SupervisionForm.Retry,
                    MaxAttempts = 2,
                    OnExhaustion = RetryExhaustion.RestartStage,
                },
                Flow.For<int>()
                    .Via(TestFlow.FaultPoint<int>(FaultPointMode.Always, firstFailure: 4))
                    .Scan(0, (running, value) => running + value))
            .To(s => s.Collect(new CollectOptions { MaxElements = 16 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        await using RunHandle first = await Host.MaterializeAsync(graph, TestToken);
        await first.Completion;

        await using RunHandle second = await Host.MaterializeAsync(graph, TestToken);
        await second.Completion;

        IReadOnlyList<int> one = await first.GetValueAsync(seen, TestToken);
        IReadOnlyList<int> other = await second.GetValueAsync(seen, TestToken);

        // Two runs of one document, two fault points, two scopes, and one answer. Nothing about a scope is
        // scheduled or sampled, which is what makes a policy something a test can prove at all.
        Assert.Equal(one, other);
        Assert.Equal(first.SupervisedFailures, second.SupervisedFailures);
        Assert.Equal(first.PoisonElements, second.PoisonElements);
    }
}
