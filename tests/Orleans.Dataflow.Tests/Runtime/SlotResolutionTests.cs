using Orleans.Dataflow.Identity;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// Which slots a run accepts, and what a caller waiting on one can and cannot do.
/// </summary>
/// <remarks>
/// ADR 0004 section 4 binds a slot to two identities, and both are checked here separately, because the
/// point of having two is that either can be the one that disagrees. The fingerprint catches a slot of a
/// differently shaped graph; the instance identity catches a slot of an identically shaped one, which is
/// exactly the case a fingerprint alone cannot see, since a document records no delegate.
/// </remarks>
public sealed class SlotResolutionTests
{
    [Fact]
    public async Task TheDefaultSlotIsRejected()
    {
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2), out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        ArgumentException rejected = await Assert.ThrowsAsync<ArgumentException>(
            async () => await run.GetValueAsync(default(ResultSlot<long>), TestToken));

        Assert.Equal("slot", rejected.ParamName);
        Assert.Contains("names no result", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ASlotOfAStructurallyDifferentGraphIsRejectedByFingerprint()
    {
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2), out ResultSlot<long> _);

        RunnableGraph other = Source.From(new RecordingEnumerable<int>(1, 2))
            .Where(value => value > 0)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> otherTotal);

        Assert.NotEqual(graph.Fingerprint, other.Fingerprint);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ArgumentException rejected =
            await Assert.ThrowsAsync<ArgumentException>(async () => await run.GetValueAsync(otherTotal, TestToken));

        Assert.Equal("slot", rejected.ParamName);
        Assert.Contains("belongs to a different graph", rejected.Message, StringComparison.Ordinal);
        Assert.Contains(other.Fingerprint.ToString(), rejected.Message, StringComparison.Ordinal);
        Assert.Contains(graph.Fingerprint.ToString(), rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ASlotOfAnIdenticallyShapedGraphIsRejectedByInstanceIdentity()
    {
        // The same code twice. The two documents are byte-identical and share a fingerprint by design, and
        // the slots are still not interchangeable.
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2), out ResultSlot<long> _);
        RunnableGraph twin = Summing(new RecordingEnumerable<int>(1, 2), out ResultSlot<long> twinTotal);

        Assert.Equal(graph.Fingerprint, twin.Fingerprint);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ArgumentException rejected =
            await Assert.ThrowsAsync<ArgumentException>(async () => await run.GetValueAsync(twinTotal, TestToken));

        Assert.Equal("slot", rejected.ParamName);
        Assert.Contains("belongs to a different graph", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("another built instance", rejected.Message, StringComparison.Ordinal);

        // The instance identity is described, never printed: it is an implementation detail of the check
        // and means nothing to whoever reads the message.
        Assert.DoesNotContain(
            twin.AuthoringNonce.ToString(),
            rejected.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ASlotNamingNoResultOfThisGraphIsRejected()
    {
        // Both identities agree and the name still does not exist. Unreachable through the authoring API,
        // which only ever hands out slots it declared, so the slot has to be fabricated to get here.
        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2)).To(Sink.Ignore<int>());

        ResultSlot<long> fabricated =
            ResultSlot<long>.Create(ResultSlotId.Create("total"), graph.Fingerprint, graph.AuthoringNonce);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ArgumentException rejected =
            await Assert.ThrowsAsync<ArgumentException>(async () => await run.GetValueAsync(fabricated, TestToken));

        Assert.Equal("slot", rejected.ParamName);
        Assert.Contains("declares no result named 'total'", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ASlotOfAGraphWhoseResultWasDiscardedIsRejected()
    {
        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2))
            .To(Sink.Aggregate<int, long>(0L, (sum, value) => sum + value).ToSink());

        ResultSlot<long> fabricated =
            ResultSlot<long>.Create(ResultSlotId.Create("total"), graph.Fingerprint, graph.AuthoringNonce);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ArgumentException rejected =
            await Assert.ThrowsAsync<ArgumentException>(async () => await run.GetValueAsync(fabricated, TestToken));

        Assert.Contains("declares no result named 'total'", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheWaitTokenCancelsTheWaitAndLeavesTheRunAlone()
    {
        Gate gate = new();
        RecordingEnumerable<int> elements = new(1, 2, 3);
        RunnableGraph graph = Summing(elements, _ => gate.Wait(), out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        using CancellationTokenSource waiting = new();
        Task<long> abandoned = run.GetValueAsync(total, waiting.Token);

        await waiting.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => abandoned);

        // The run never noticed: it finishes and a second ask resolves normally.
        gate.Open();
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
        Assert.Equal(3, elements.Pulls);
    }

    [Fact]
    public async Task TwoWaitersAndCompletionObserveOneOutcome()
    {
        Gate gate = new();
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), _ => gate.Wait(), out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        Task<long> first = run.GetValueAsync(total, TestToken);
        Task<long> second = run.GetValueAsync(total, TestToken);
        Task completion = run.Completion;

        gate.Open();
        await Task.WhenAll(first, second, completion);

        Assert.Equal(6L, await first);
        Assert.Equal(6L, await second);
        Assert.Equal(TaskStatus.RanToCompletion, completion.Status);
    }

    [Fact]
    public async Task AResultAskedForBeforeTheRunStartedResolvesAllTheSame()
    {
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task ASlotResolvesAgainstEitherRunOfItsGraph()
    {
        // A slot carries no run identity, so one slot resolves against every run of the graph that
        // declared it.
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), out ResultSlot<long> total);

        await using RunHandle first = await Host.MaterializeAsync(graph, TestToken);
        await using RunHandle second = await Host.MaterializeAsync(graph, TestToken);

        Assert.Equal(6L, await first.GetValueAsync(total, TestToken));
        Assert.Equal(6L, await second.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task TheHandleDescribesItsGraphAndItsStatus()
    {
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal($"run of {graph.Fingerprint} ({TaskStatus.RanToCompletion})", run.ToString());
    }
}
