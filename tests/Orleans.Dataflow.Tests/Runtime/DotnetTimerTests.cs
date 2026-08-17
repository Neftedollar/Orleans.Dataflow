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
