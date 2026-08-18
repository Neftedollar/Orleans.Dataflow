using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;
using static Orleans.Dataflow.Tests.Runtime.TimingFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What a supervision scope does <em>not</em> catch, which is the half of ADR 0007 that keeps the engine's
/// own rule readable.
/// </summary>
/// <remarks>
/// <para>
/// A policy is only worth something if its edges are exact. Three of them are asserted here, and each is a
/// different kind of claim. A failure <b>outside</b> every scope still fails the run — proved as a contrast,
/// one graph with the fault point inside the scope and one with it a stage earlier, so the only difference
/// is where the scope's brackets fall. A <b>cancellation</b> is not a failure and no form weakens it. And a
/// failure of the <b>machinery</b> rather than of an author's stage is a refusal at materialization, before
/// the run has an element to supervise at all.
/// </para>
/// <para>
/// The fourth is the one this engine had to decide for itself: a failure raised while a stream is
/// <em>ending</em>. There is no failing element to drop, nothing to re-offer, and no fallback question to
/// ask, so it is not supervised — and the test that says so injects the failure exactly there, in a batch's
/// projection of the partial group it hands over as the stream ends.
/// </para>
/// </remarks>
public sealed class SupervisionBoundaryTests
{
    [Fact]
    public async Task AFailureInsideAScopeIsContained()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2)))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 3], observed);
    }

    [Fact]
    public async Task AFailureOutsideEveryScopeStillFailsTheRun()
    {
        List<int> observed = [];

        // The contrast test, and the only difference from the one above is which side of the scope the
        // fault point stands on. Everything else — the source, the arming, the sink — is identical.
        RunnableGraph graph = Source.From([1, 2, 3])
            .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2))
            .Supervised(new SupervisionOptions { Form = SupervisionForm.Resume }, Flow.For<int>())
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        _ = await Assert.ThrowsAsync<FaultInjectedException>(async () => await run.Completion);

        Assert.Equal([1], observed);
        Assert.Equal(0, run.SupervisedFailures);
    }

    [Fact]
    public async Task AFailureBelowAScopeStillFailsTheRun()
    {
        RunnableGraph graph = Source.From([1, 2, 3])
            .Supervised(new SupervisionOptions { Form = SupervisionForm.Resume }, Flow.For<int>())
            .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2))
            .To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // The other side of the brackets, and the same answer: a scope answers for the chain it owns and
        // for nothing that happens to travel past it.
        _ = await Assert.ThrowsAsync<FaultInjectedException>(async () => await run.Completion);

        Assert.Equal(0, run.SupervisedFailures);
    }

    [Fact]
    public async Task CancellationIsNotCaughtByAScope()
    {
        using CancellationTokenSource cancellation = new();
        List<int> observed = [];

        RunnableGraph graph = TestSource.Probe<int>("emitted")
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>().Select(value => value))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);
        ISourceProbe<int> probe = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);

        await probe.EmitAsync(1, TestToken);
        await Reaches(() => observed.Count == 1, "the first element reaching the sink", TestToken);

        await cancellation.CancelAsync();

        // The run's own stop is not a failure, so no form weakens it: the run ends cancelled and the scope
        // contained nothing. A scope that caught cancellation would turn a stop into a stream that would not
        // stop.
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run.Completion);

        Assert.Equal(0, run.SupervisedFailures);
    }

    [Fact]
    public async Task ACancellingStageInsideAScopeIsNotSupervisedEither()
    {
        using CancellationTokenSource cancellation = new();

        // The same claim read from inside the scope rather than from outside it: the exception the scope
        // sees is an OperationCanceledException raised by the run's own token, and the scope hands it on
        // rather than dropping the element and carrying on.
        RunnableGraph graph = Source.From([1, 2, 3])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>().Select(value =>
                {
                    cancellation.Cancel();
                    cancellation.Token.ThrowIfCancellationRequested();

                    return value;
                }))
            .To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run.Completion);

        Assert.Equal(0, run.SupervisedFailures);
    }

    [Fact]
    public async Task AFailureRaisedAsTheStreamEndsIsNotSupervised()
    {
        // The projection throws only for a group smaller than the batch, which is a group that exists only
        // at the end of the stream: every failure this graph can raise is a residue's and never an
        // element's.
        RunnableGraph graph = Source.From([1, 2, 3])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>().Grouped(2).Select(group =>
                    group.Count == 2 ? group.Sum() : throw new BufferOverflowException("residue")))
            .To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        BufferOverflowException failed =
            await Assert.ThrowsAsync<BufferOverflowException>(async () => await run.Completion);

        // There is no failing element to drop and nothing to re-offer, so the scope has no answer to give
        // and the failure travels to the run. Stated in the documentation rather than discovered here.
        Assert.Equal("residue", failed.Message);
        Assert.Equal(0, run.SupervisedFailures);
    }

    [Fact]
    public async Task AMalformedScopePayloadIsRefusedAtMaterializationRatherThanSupervised()
    {
        RunnableGraph graph = Scoped("""{"form":"nonsense","scope":[]}""");

        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        // A machinery failure fails materialization: the run never starts, so there is no element for a
        // policy to answer for and no chance of a scope appearing to supervise its own construction.
        Assert.Contains("[invalid-parameters]", rejected.Message, StringComparison.Ordinal);
        Assert.Contains(
            "a supervision form is one of 'resume', 'restart-stage', 'retry', and 'recover'",
            rejected.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task AScopeChainThePlannerCannotBuildIsRefusedAtMaterialization()
    {
        RunnableGraph graph = Scoped(
            """{"form":"resume","scope":[{"stage":"local/never@v1","parameters":{}}]}""");

        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains(
            "a scope owns the execution of its chain element by element, so it holds element stages only",
            rejected.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task TwoPlanesDescribingDifferentScopesAreRefusedAtMaterialization()
    {
        RunnableGraph graph = Scoped(
            """{"form":"resume","scope":[{"stage":"local/take@v1","parameters":{"count":2}}]}""",
            LocalStageDescriptor.Skip(2));

        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains(
            "is declared as 'local/take@v1' and bound as 'local/skip@v1'",
            rejected.Message,
            StringComparison.Ordinal);
    }

    /// <summary>Builds a chain whose middle node is a scope carrying a payload written by hand.</summary>
    /// <param name="payload">The parameter payload as JSON text.</param>
    /// <param name="scope">The occurrences the binding declares the chain to be.</param>
    /// <returns>The graph, fingerprinted the way closing one would have fingerprinted it.</returns>
    /// <remarks>
    /// Every document here is unreachable through the authoring API, which writes the payload from the very
    /// descriptors it binds; building it by hand is the only way a refusal that exists for a hand-written
    /// document can be asserted at all.
    /// </remarks>
    private static RunnableGraph Scoped(string payload, params LocalStageDescriptor[] scope) =>
        Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "supervised", "local-supervision-parameters", payload),
                    Node("stage-3", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                (
                    "stage-2",
                    LocalStageDescriptor.Supervised(
                        new SupervisionOptions { Form = SupervisionForm.Resume },
                        fallback: null,
                        scope)),
                ("stage-3", LocalStageDescriptor.Ignore())));
}
