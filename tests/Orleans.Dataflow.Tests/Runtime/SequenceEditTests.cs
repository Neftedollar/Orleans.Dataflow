using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the sequence edits promise, and what they turn out to be: a concat junction under two names and a
/// two-legged partition that leaves the main line open.
/// </summary>
/// <remarks>
/// <para>
/// These operators add no stage to the vocabulary, which is the claim worth pinning first: the document a
/// prepend builds is the document a concat builds with its inputs swapped, and the document a divert builds
/// is a partition with an empty first leg. So the tests assert both halves — the elements a run delivers,
/// and the shape of the graph it delivers them through — because sugar that quietly grew a stage of its own
/// would pass the first and fail the second.
/// </para>
/// <para>
/// What they inherit is inherited whole: everything ADR 0005 says about a concat and about a partition holds
/// here, including the parts that cost something.
/// </para>
/// </remarks>
public sealed class SequenceEditTests
{
    [Fact]
    public async Task PrependEmitsTheOtherStreamFirst()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([3, 4]).Prepend(Source.From([1, 2])).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2, 3, 4], observed);
    }

    [Fact]
    public async Task PrependTakesAFixedRunOfElements()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([3, 4]).Prepend(0, 1, 2).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([0, 1, 2, 3, 4], observed);
    }

    [Fact]
    public async Task AppendEmitsTheOtherStreamLast()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2]).Append(Source.From([3, 4])).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2, 3, 4], observed);
    }

    [Fact]
    public async Task AppendTakesAFixedRunOfElements()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2]).Append(98, 99).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2, 98, 99], observed);
    }

    [Fact]
    public void AppendAndConcatAreTheSameGraph()
    {
        RunnableGraph appended = Source.From([1, 2]).Append(Source.From([3])).To(Sink.Ignore<int>());
        RunnableGraph concatenated = Source.From([1, 2]).Concat(Source.From([3])).To(Sink.Ignore<int>());

        // Not "behaves the same" but "is the same": one document, one fingerprint, and the two spellings are
        // a question of what an author is saying rather than of what the run does.
        Assert.Equal(
            GraphDocumentSerializer.Fingerprint(appended.Document),
            GraphDocumentSerializer.Fingerprint(concatenated.Document));
    }

    [Fact]
    public void PrependIsTheConcatWithItsInputsSwapped()
    {
        RunnableGraph prepended = Source.From([3]).Prepend(Source.From([1, 2])).To(Sink.Ignore<int>());
        RunnableGraph concatenated = Source.From([1, 2]).Concat(Source.From([3])).To(Sink.Ignore<int>());

        Assert.Equal(
            GraphDocumentSerializer.Fingerprint(prepended.Document),
            GraphDocumentSerializer.Fingerprint(concatenated.Document));
        Assert.Equal(
            ["from-enumerable", "from-enumerable", "concat", "ignore"],
            prepended.Document.Nodes.Select(node => node.Stage.Stage.Value));
    }

    [Fact]
    public void ThePrependedSourceReachesTheJunctionsFirstInput()
    {
        RunnableGraph graph = Source.From([3]).Prepend(Source.From([1, 2])).To(Sink.Ignore<int>());
        GraphDocument document = graph.Document;

        // Argument order is identity-bearing everywhere in this vocabulary, and here it is what "before"
        // means: the head is wired to in-0 and the receiver to in-1, which is the order a concat consumes.
        Assert.Contains(
            document.Edges,
            edge => edge.From.Node.Value == "stage-0001" && edge.To.Port.Value == "in-0");
        Assert.Contains(
            document.Edges,
            edge => edge.From.Node.Value == "stage-0002" && edge.To.Port.Value == "in-1");
    }

    [Fact]
    public async Task DivertToSendsTheAcceptedElementsToTheBranchAndTheRestOnward()
    {
        List<int> diverted = [];
        List<int> onward = [];

        RunnableGraph graph = Source.Range(1, 6)
            .DivertTo(value => value % 2 == 0, Flow.For<int>().To(s => s.ForEach(diverted.Add)))
            .To(s => s.ForEach(onward.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Every element goes to exactly one of the two, which is what makes this a partition rather than a
        // tap: unlike AlsoTo, nothing is duplicated.
        Assert.Equal([2, 4, 6], diverted);
        Assert.Equal([1, 3, 5], onward);
    }

    [Fact]
    public void DivertToIsAPartitionWithAnEmptyFirstLeg()
    {
        RunnableGraph graph = Source.Range(1, 3)
            .DivertTo(value => value > 1, Flow.For<int>().To(s => s.ForEach(_ => { })))
            .To(Sink.Ignore<int>());
        GraphDocument document = graph.Document;

        Assert.Equal(
            ["range", "partition", "for-each", "ignore"],
            document.Nodes.Select(node => node.Stage.Stage.Value));

        // The main line stays on the junction's first leg and the branch is on its second, which is what
        // keeps the receiver an expression rather than one branch of a closed graph.
        Assert.Contains(
            document.Edges,
            edge => edge.From.Port.Value == "out-0" && edge.To.Node.Value == "stage-0004");
        Assert.Contains(
            document.Edges,
            edge => edge.From.Port.Value == "out-1" && edge.To.Node.Value == "stage-0003");
    }

    [Fact]
    public async Task ADivertedBranchCanDeclareItsOwnResult()
    {
        RunnableGraph graph = Source.Range(1, 6)
            .DivertTo(
                value => value % 2 == 0,
                Flow.For<int>().To(s => s.Count(), "rejected", out ResultSlot<long> rejected))
            .To(s => s.Count(), "kept", out ResultSlot<long> kept);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The branch named its slot where its sink was written, and the graph declares it beside the main
        // line's when it is closed.
        Assert.Equal(3L, await run.GetValueAsync(rejected, TestToken));
        Assert.Equal(3L, await run.GetValueAsync(kept, TestToken));
    }

    [Fact]
    public async Task DivertToWithNothingAcceptedLeavesTheMainLineWhole()
    {
        List<int> diverted = [];
        List<int> onward = [];

        RunnableGraph graph = Source.Range(1, 4)
            .DivertTo(_ => false, Flow.For<int>().To(s => s.ForEach(diverted.Add)))
            .To(s => s.ForEach(onward.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Empty(diverted);
        Assert.Equal([1, 2, 3, 4], onward);
    }

    [Fact]
    public async Task AFailingPredicateFailsTheRunWithItsOwnException()
    {
        InvalidOperationException failure = new("cannot classify this one");

        RunnableGraph graph = Source.Range(1, 3)
            .DivertTo(_ => throw failure, Flow.For<int>().To(s => s.ForEach(_ => { })))
            .To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion));
    }

    [Fact]
    public async Task PrependAndAppendComposeWithTheOperatorsAroundThem()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([2, 3])
            .Prepend(1)
            .Append(4)
            .Select(value => value * 10)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Two junctions rather than one, which is what a chain of them honestly is, and the order the
        // author wrote is the order the elements arrive in.
        Assert.Equal([10, 20, 30, 40], observed);
    }

    [Fact]
    public void PrependingNothingStillBuildsTheJunction()
    {
        RunnableGraph graph = Source.From([1]).Prepend().To(Sink.Ignore<int>());

        // A graph's shape is what the author wrote. An empty prepend is a real concat over an empty source,
        // not a call that disappears, which is what keeps a fingerprint a statement about a program.
        Assert.Equal(
            ["from-enumerable", "from-enumerable", "concat", "ignore"],
            graph.Document.Nodes.Select(node => node.Stage.Stage.Value));
    }

    [Fact]
    public void TheSequenceEditsRefuseANullArgument()
    {
        Source<int> numbers = Source.From([1]);

        Assert.Throws<ArgumentNullException>("head", () => { _ = numbers.Prepend((Source<int>)null!); });
        Assert.Throws<ArgumentNullException>("tail", () => { _ = numbers.Append((Source<int>)null!); });
        Assert.Throws<ArgumentNullException>("elements", () => { _ = numbers.Prepend((int[])null!); });
        Assert.Throws<ArgumentNullException>("elements", () => { _ = numbers.Append((int[])null!); });
        Assert.Throws<ArgumentNullException>(
            "predicate",
            () => { _ = numbers.DivertTo(null!, Flow.For<int>().To(s => s.Ignore())); });
        Assert.Throws<ArgumentNullException>("side", () => { _ = numbers.DivertTo(_ => true, null!); });
    }
}
