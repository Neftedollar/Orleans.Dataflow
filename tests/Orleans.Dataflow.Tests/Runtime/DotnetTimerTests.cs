using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.DotnetFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the run-scoped timer does: tick, stop when the run stops, and count what it produced rather than
/// what a clock did.
/// </summary>
/// <remarks>
/// <para>
/// Nothing here asserts a duration, and that is the point. A timer test that measured elapsed time would be
/// asserting the machine's load; what this stage promises is a sequence of indices, a bound, and the two
/// ways a run ends, and every one of those is checkable without a clock. The periods are small so the suite
/// is quick, and every assertion would hold at any period.
/// </para>
/// <para>
/// One test does wait for periods to pass, and it is the exception that states the rule: the claim that a
/// slow consumer loses no tick is empty unless the consumer really was held across several of them. The wait
/// is a rendezvous on a second <see cref="PeriodicTimer"/> of the same period rather than a delay of a
/// length chosen by hand — this stage builds its own timer, so there is no clock a test could move — and the
/// assertion that follows names a sequence of indices and no duration at all.
/// </para>
/// <para>
/// The pipelines below mix a registered source with lambda operators, which is the local host's own
/// affordance: the push vocabulary declares the same opaque element contract every local port declares, so
/// a timer heads a chain of ordinary operators with nothing in between.
/// </para>
/// </remarks>
public sealed class DotnetTimerTests
{
    [Fact]
    public async Task ABoundedTimerTicksItsIndicesFromZeroAndCompletes()
    {
        LocalDataflowHost host = TimerHost();
        RunnableGraph graph = Source
            .FromRegistered(
                DotnetStages.Timer(),
                "ticks",
                DotnetStages.TimerParameters(TimeSpan.FromMilliseconds(1), tickLimit: 4))
            .To(sink => sink.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<long>> seen);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await run.Completion;

        Assert.Equal([0L, 1L, 2L, 3L], await run.GetValueAsync(seen, TestToken));
    }

    [Fact]
    public async Task AnUnboundedTimerIsBoundedByWhateverStandsDownstream()
    {
        LocalDataflowHost host = TimerHost();
        RunnableGraph graph = Source
            .FromRegistered(
                DotnetStages.Timer(),
                "ticks",
                DotnetStages.TimerParameters(TimeSpan.FromMilliseconds(1)))
            .Take(3)
            .To(sink => sink.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<long>> seen);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await run.Completion;

        Assert.Equal([0L, 1L, 2L], await run.GetValueAsync(seen, TestToken));
    }

    [Fact]
    public async Task AConsumerSlowerThanThePeriodMissesNoTickBecauseThePullIsTheBackpressure()
    {
        // The row's claim, and the one place this stage differs from the local tick source: the timer is
        // awaited on the run's own source thread and has no ingress behind it, so a consumer that is slower
        // than the period makes the next tick later and never makes a tick vanish. TickSourceTests holds a
        // consumer across three intervals and the numbering jumps to say how many were skipped; this holds
        // one across three periods and the numbering is contiguous, because the index counts the ticks this
        // run emitted rather than the periods that elapsed.
        TimeSpan period = TimeSpan.FromMilliseconds(2);
        Gate gate = new();
        LocalDataflowHost host = TimerHost();
        RunnableGraph graph = Source
            .FromRegistered(
                DotnetStages.Timer(),
                "ticks",
                DotnetStages.TimerParameters(period, tickLimit: 5))
            .Select(tick =>
            {
                gate.Wait();

                return tick;
            })
            .To(sink => sink.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<long>> seen);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        // The run is inside the author's function holding the first tick, so the source is not awaiting the
        // timer at all while the periods below come due.
        await gate.Reached;

        // Three whole periods, waited for rather than slept through, and on the very instrument the adapter
        // uses. This file has no controlled clock because the stage builds a PeriodicTimer of its own, so
        // "the consumer was held across three periods" is made a fact by a second timer of the same period
        // reaching its third tick — and every assertion below would hold for any number of periods, which is
        // why none of them names one.
        using (PeriodicTimer elapsing = new(period))
        {
            for (int elapsed = 0; elapsed < 3; elapsed++)
            {
                Assert.True(await elapsing.WaitForNextTickAsync(TestToken));
            }
        }

        gate.Open();

        await run.Completion;

        // Contiguous from zero with no gap anywhere: nothing was queued while the consumer was held and
        // nothing was dropped, which is what "no ingress, no drops" means when the only pacing is the pull.
        Assert.Equal([0L, 1L, 2L, 3L, 4L], await run.GetValueAsync(seen, TestToken));
    }

    [Fact]
    public async Task ShutdownEndsTheTimerAndKeepsTheTicksSoFar()
    {
        Gate gate = new();
        LocalDataflowHost host = TimerHost();
        RunnableGraph graph = Source
            .FromRegistered(
                DotnetStages.Timer(),
                "ticks",
                DotnetStages.TimerParameters(TimeSpan.FromMilliseconds(1)))
            .To(
                sink => sink.Aggregate(0L, (count, tick) =>
                {
                    gate.Wait();

                    return count + 1L;
                }),
                "counted",
                out ResultSlot<long> counted);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        // The first tick has reached the fold, so the shutdown lands on a timer that has produced something
        // rather than on one that has not started. Nothing here waits for a length of time.
        await gate.Reached;

        Task shutdown = run.ShutdownAsync().AsTask();

        gate.Open();

        await shutdown;
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);

        // A drain keeps what the run had folded. The number is not fixed, because a shutdown races the next
        // tick by design; what is fixed is that the run completed and resolved a count of at least one.
        Assert.True(await run.GetValueAsync(counted, TestToken) >= 1L);
    }

    [Fact]
    public async Task CancellingAbandonsTheTimerAndResolvesNothing()
    {
        Gate gate = new();
        LocalDataflowHost host = TimerHost();
        RunnableGraph graph = Source
            .FromRegistered(
                DotnetStages.Timer(),
                "ticks",
                DotnetStages.TimerParameters(TimeSpan.FromMilliseconds(1)))
            .To(
                sink => sink.Aggregate(0L, (count, tick) =>
                {
                    gate.Wait();

                    return count + 1L;
                }),
                "counted",
                out ResultSlot<long> counted);

        RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await gate.Reached;

        gate.Open();

        await run.DisposeAsync();

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await run.GetValueAsync(counted, TestToken));
    }

    [Fact]
    public async Task ATimerCancelledBeforeItsFirstTickEndsWithoutOne()
    {
        using CancellationTokenSource cancelled = new();

        await cancelled.CancelAsync();

        LocalDataflowHost host = TimerHost();
        RunnableGraph graph = Source
            .FromRegistered(
                DotnetStages.Timer(),
                "ticks",
                DotnetStages.TimerParameters(TimeSpan.FromMilliseconds(1)))
            .To(sink => sink.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<long>> _);

        await using RunHandle run = await host.MaterializeAsync(graph, cancelled.Token);

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);
    }

    [Fact]
    public async Task ShutdownBetweenTicksEndsTheSequenceRatherThanWaitingForTheNextPeriod()
    {
        // A period far longer than the test, so that the shutdown lands squarely between two ticks: if the
        // stop token did not release the wait, this run would take a minute to end and the test would fail
        // on its own timeout rather than pass.
        LocalDataflowHost host = TimerHost();
        RunnableGraph graph = Source
            .FromRegistered(
                DotnetStages.Timer(),
                "ticks",
                DotnetStages.TimerParameters(TimeSpan.FromMinutes(1)))
            .To(sink => sink.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<long>> seen);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);
        await run.ShutdownAsync();
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Empty(await run.GetValueAsync(seen, TestToken));
    }

    [Fact]
    public void APeriodBelowOneMillisecondIsRefusedWhenTheParametersAreWritten()
    {
        _ = Assert.Throws<ArgumentOutOfRangeException>(
            "period",
            () => DotnetStages.TimerParameters(TimeSpan.Zero));
        _ = Assert.Throws<ArgumentOutOfRangeException>(
            "period",
            () => DotnetStages.TimerParameters(TimeSpan.FromMilliseconds(-1)));
        _ = Assert.Throws<ArgumentOutOfRangeException>(
            "tickLimit",
            () => DotnetStages.TimerParameters(TimeSpan.FromMilliseconds(1), tickLimit: -1));
    }

    [Fact]
    public async Task ATimerPayloadThisRuntimeCannotReadIsRefusedBeforeTheRunStarts()
    {
        LocalDataflowHost host = TimerHost();
        RunnableGraph graph = Source
            .FromRegistered(
                DotnetStages.Timer(),
                "ticks",
                CanonicalJsonValue.Parse("{\"periodMilliseconds\":0,\"tickLimit\":0}"))
            .To(sink => sink.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<long>> _);

        InvalidOperationException refusal = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await host.MaterializeAsync(graph, TestToken));

        Assert.Contains("periodMilliseconds", refusal.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AHostThatDidNotPublishTheVocabularyRefusesATimerByName()
    {
        LocalDataflowHost host = new();
        RunnableGraph graph = Source
            .FromRegistered(
                DotnetStages.Timer(),
                "ticks",
                DotnetStages.TimerParameters(TimeSpan.FromMilliseconds(1), tickLimit: 1))
            .To(sink => sink.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<long>> _);

        InvalidOperationException refusal = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await host.MaterializeAsync(graph, TestToken));

        Assert.Contains("dotnet/timer@v1", refusal.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePublishedTimerSpecificationDeclaresOneOutputPortAndNoCapability()
    {
        Assert.True(DotnetStages.Catalog.TryGetSpecification(
            DotnetStages.TimerStage,
            out StageSpecification? specification));

        Assert.Empty(specification!.InputPorts);
        Assert.Single(specification.OutputPorts);
        Assert.Empty(specification.RequiredCapabilities);
        Assert.Equal(DotnetStages.ElementContract, specification.OutputPorts[0].ElementContract);
    }
}
