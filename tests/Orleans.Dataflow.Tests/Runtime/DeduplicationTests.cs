using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the two deduplicating operators promise, and what each of them costs: a declared bound and a
/// declared answer for reaching it, or one element of memory and no bound at all.
/// </summary>
/// <remarks>
/// <para>
/// The bound is the subject rather than the happy path. A deduplicator whose memory grows with the data is
/// the operator this vocabulary refuses to have, so the tests here are mostly about what happens at the
/// bound: the failing policy reports that the bound was wrong, and the evicting policy is the deliberate
/// weakening — an element whose key was evicted is emitted a second time, which is asserted rather than
/// described.
/// </para>
/// <para>
/// The keys are per run, so the graphs that carry state are materialized twice.
/// </para>
/// </remarks>
public sealed class DeduplicationTests
{
    [Fact]
    public async Task DistinctPassesTheFirstOccurrenceOfEveryElement()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 1, 3, 2, 1])
            .Distinct(new DistinctOptions { MaxTrackedKeys = 8 })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2, 3], observed);
    }

    [Fact]
    public async Task ARepeatCostsNoCapacityAtAll()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([7, 7, 7, 7, 7])
            .Distinct(new DistinctOptions { MaxTrackedKeys = 1 })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // A stream of one key forever runs inside a bound of one, because a repeat is recognized before
        // anything is added.
        Assert.Equal([7], observed);
    }

    [Fact]
    public async Task TheKeyPastTheBoundFailsTheRunUnderTheDefaultPolicy()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Distinct(new DistinctOptions { MaxTrackedKeys = 2 })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        TrackedKeyOverflowException refused =
            await Assert.ThrowsAsync<TrackedKeyOverflowException>(async () => await run.Completion);

        Assert.Contains("2", refused.Message, StringComparison.Ordinal);
        Assert.Equal([1, 2], observed);
    }

    [Fact]
    public async Task TheEvictingPolicyForgetsTheOldestKeyAndKeepsGoing()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .Distinct(new DistinctOptions { MaxTrackedKeys = 2, OverflowPolicy = KeyOverflowPolicy.EvictOldest })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1, 2, 3, 4], observed);
    }

    [Fact]
    public async Task AnEvictedKeyIsEmittedASecondTimeIfItArrivesAgain()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 1])
            .Distinct(new DistinctOptions { MaxTrackedKeys = 2, OverflowPolicy = KeyOverflowPolicy.EvictOldest })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The deliberate weakening, asserted rather than described: 3 evicts 1, so the second 1 is a key
        // this stage no longer remembers and it is emitted again. The stream is distinct over a window of
        // the last two keys and not over its history.
        Assert.Equal([1, 2, 3, 1], observed);
    }

    [Fact]
    public async Task EvictionIsByArrivalAndNotByLastUse()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 1, 3, 2])
            .Distinct(new DistinctOptions { MaxTrackedKeys = 2, OverflowPolicy = KeyOverflowPolicy.EvictOldest })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The repeated 1 does not refresh its key, so 3 still evicts 1 and 2 is still remembered when it
        // arrives again. Age is when a key was first remembered, which is the contract rather than an
        // accident of the data structure.
        Assert.Equal([1, 2, 3], observed);
    }

    [Fact]
    public async Task TheEvictingPolicyRunsAStreamOfManyKeysInsideItsBound()
    {
        long emitted = 0;

        RunnableGraph graph = Source.Range(1, 10_000)
            .Distinct(new DistinctOptions { MaxTrackedKeys = 4, OverflowPolicy = KeyOverflowPolicy.EvictOldest })
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "emitted", out ResultSlot<long> counted);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        emitted = await run.GetValueAsync(counted, TestToken);

        // Ten thousand distinct keys through a bound of four. Nothing faults, and the bound is what the
        // stage holds rather than what the stream carries.
        Assert.Equal(10_000L, emitted);
    }

    [Fact]
    public async Task EveryRunOfADistinctStartsWithNoKeysRemembered()
    {
        List<int> first = [];
        List<int> second = [];
        List<int> observed = first;

        RunnableGraph graph = Source.From([1, 1, 2])
            .Distinct(new DistinctOptions { MaxTrackedKeys = 8 })
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

        Assert.Equal([1, 2], first);
        Assert.Equal([1, 2], second);
    }

    [Fact]
    public void ADistinctRefusesAPolicyNoMemberDeclares()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            "options",
            () =>
            {
                _ = Source.From([1]).Distinct(
                    new DistinctOptions { MaxTrackedKeys = 1, OverflowPolicy = (KeyOverflowPolicy)42 });
            });
    }

    [Fact]
    public async Task DeduplicateConsecutiveCollapsesRunsAndNeverComparesAcrossThem()
    {
        List<string> observed = [];

        RunnableGraph graph = Source.From(["a", "a", "b", "b", "b", "a"])
            .DeduplicateConsecutive()
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The bounded deduplicator: one element of memory, so the last 'a' is a new run rather than a
        // repeat of the first one.
        Assert.Equal(["a", "b", "a"], observed);
    }

    [Fact]
    public async Task DeduplicateConsecutiveRunsAStreamOfDistinctElementsUntouched()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.Range(1, 5).DeduplicateConsecutive().To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2, 3, 4, 5], observed);
    }

    [Fact]
    public async Task DeduplicateConsecutiveEmitsTheFirstElementEvenWhenItIsTheTypesDefault()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([0, 0, 1]).DeduplicateConsecutive().To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The first element is emitted because it is the first and not because it differs from a remembered
        // default, which is the case a stage keeping only a value rather than "have I seen one" would lose.
        Assert.Equal([0, 1], observed);
    }

    [Fact]
    public async Task DeduplicateConsecutiveUsesTheElementTypesOwnEquality()
    {
        List<string> observed = [];

        RunnableGraph graph = Source.From([new string(['a']), new string(['a']), "b"])
            .DeduplicateConsecutive()
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Two distinct string instances that are equal, so the second is a repeat: equality is the element
        // type's own and never reference identity.
        Assert.Equal(["a", "b"], observed);
    }

    [Fact]
    public async Task EveryRunOfADeduplicateConsecutiveStartsRememberingNothing()
    {
        List<int> first = [];
        List<int> second = [];
        List<int> observed = first;

        RunnableGraph graph = Source.From([5, 5])
            .DeduplicateConsecutive()
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

        // The second run emits its first element rather than treating it as a repeat of the first run's
        // last one.
        Assert.Equal([5], first);
        Assert.Equal([5], second);
    }
}
