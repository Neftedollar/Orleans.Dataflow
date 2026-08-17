using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Xunit;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What a closed graph says about a runtime control, and how an author gets a typed slot back for the name
/// they wrote.
/// </summary>
/// <remarks>
/// <para>
/// A control is a result slot: it is declared in the document, it is named, and it resolves through the one
/// <c>GetValueAsync</c> every result uses. ADR 0002 listed a queue control beside a fold result for exactly
/// this reason, and these tests are where that claim stops being a sentence — the document carries two
/// slots, on two ports, under two contracts, and one run resolves both.
/// </para>
/// <para>
/// The name is the only thing an author writes, so the two ways of getting it wrong are the two things
/// worth diagnostics: a name no control answers to, and the right name asked for as the wrong type. Both
/// are rejected before a run exists.
/// </para>
/// </remarks>
public sealed class ControlSlotTests
{
    [Fact]
    public void AQueueGraphDeclaresItsControlOnTheQueueNodesControlPort()
    {
        RunnableGraph graph = Queued()
            .To(s => s.Count(), "count", out ResultSlot<long> _);

        Assert.Equal(
            [ResultSlotId.Create("count"), ResultSlotId.Create("ingress")],
            graph.ResultSlots.Order());

        ResultSlotDefinition control =
            graph.Document.ResultSlots.Single(slot => slot.Id == ResultSlotId.Create("ingress"));

        Assert.Equal(NodeId.Create("stage-0001"), control.Producer.Node);
        Assert.Equal(PortId.Create("control"), control.Producer.Port);
        Assert.Equal(ContractId.Create("local-control"), control.ResultContract.Contract);
    }

    [Fact]
    public void AQueueNodeCarriesTheBufferPayloadUnderTheBufferContract()
    {
        RunnableGraph graph = Source
            .Queue<int>(new BufferOptions { Capacity = 3, OverflowPolicy = OverflowPolicy.DropOldest }, "ingress")
            .To(Sink.Ignore<int>());

        StageNode queue = graph.Document.Nodes.Single(node => node.Stage.Stage == StageId.Create("queue"));

        Assert.Equal(ContractId.Create("local-buffer-parameters"), queue.ParameterContract.Contract);
        Assert.Equal("""{"capacity":3,"overflowPolicy":"drop-oldest"}""", queue.Parameters.ToString());
    }

    [Fact]
    public void TwoQueueGraphsThatDifferOnlyInTheirBoundsHaveDifferentFingerprints()
    {
        RunnableGraph small = Source.Queue<int>(new BufferOptions { Capacity = 2 }, "ingress").To(Sink.Ignore<int>());
        RunnableGraph large = Source.Queue<int>(new BufferOptions { Capacity = 3 }, "ingress").To(Sink.Ignore<int>());
        RunnableGraph dropping = Source
            .Queue<int>(new BufferOptions { Capacity = 2, OverflowPolicy = OverflowPolicy.DropNewest }, "ingress")
            .To(Sink.Ignore<int>());
        RunnableGraph renamed = Source.Queue<int>(new BufferOptions { Capacity = 2 }, "inbox").To(Sink.Ignore<int>());
        RunnableGraph same = Source.Queue<int>(new BufferOptions { Capacity = 2 }, "ingress").To(Sink.Ignore<int>());

        Assert.NotEqual(small.Fingerprint, large.Fingerprint);
        Assert.NotEqual(small.Fingerprint, dropping.Fingerprint);
        Assert.NotEqual(small.Fingerprint, renamed.Fingerprint);
        Assert.Equal(small.Fingerprint, same.Fingerprint);
    }

    [Fact]
    public void TheControlSlotBindsToTheGraphThatDeclaredIt()
    {
        RunnableGraph graph = Queued().To(Sink.Ignore<int>());
        RunnableGraph twin = Queued().To(Sink.Ignore<int>());

        ResultSlot<IIngressQueue<int>> control = graph.Control<IIngressQueue<int>>("ingress");

        Assert.Equal(control, graph.Control<IIngressQueue<int>>("ingress"));
        Assert.Equal(graph.Fingerprint, control.Graph);

        // The two graphs have one shape and therefore one fingerprint; the slots are still different,
        // because a slot of a nondeployable graph binds to the instance that declared it as well.
        Assert.Equal(graph.Fingerprint, twin.Fingerprint);
        Assert.NotEqual(control, twin.Control<IIngressQueue<int>>("ingress"));
    }

    [Fact]
    public void AskingForAControlUnderTheWrongTypeNamesBothTypes()
    {
        RunnableGraph graph = Queued().To(Sink.Ignore<int>());

        ArgumentException failure =
            Assert.Throws<ArgumentException>(() => graph.Control<IIngressQueue<string>>("ingress"));

        Assert.Equal("name", failure.ParamName);
        Assert.Contains("IIngressQueue`1[System.Int32]", failure.Message, StringComparison.Ordinal);
        Assert.Contains("IIngressQueue`1[System.String]", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AskingForAControlByAnUnknownNameListsTheOnesTheGraphDeclares()
    {
        RunnableGraph graph = Queued().To(Sink.Ignore<int>());

        ArgumentException failure =
            Assert.Throws<ArgumentException>(() => graph.Control<IIngressQueue<int>>("inbox"));

        Assert.Equal("name", failure.ParamName);
        Assert.Contains("'inbox'", failure.Message, StringComparison.Ordinal);
        Assert.Contains("'ingress'", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AskingAGraphWithNoControlsSaysSo()
    {
        RunnableGraph graph = Source.Range(1, 2).To(Sink.Ignore<int>());

        ArgumentException failure =
            Assert.Throws<ArgumentException>(() => graph.Control<IIngressQueue<int>>("ingress"));

        Assert.Contains("declares no controls at all", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AControlNameThatIsNotAnIdentifierIsRejectedAndANullOneIsToo()
    {
        RunnableGraph graph = Queued().To(Sink.Ignore<int>());

        Assert.Equal("name", Assert.Throws<ArgumentException>(() => graph.Control<IIngressQueue<int>>("Not A Name")).ParamName);
        Assert.Equal("name", Assert.Throws<ArgumentNullException>(() => graph.Control<IIngressQueue<int>>(null!)).ParamName);
        Assert.Equal(
            "name",
            Assert.Throws<ArgumentNullException>(
                () => graph.TryGetControl(null!, out ResultSlot<IIngressQueue<int>> _)).ParamName);
    }

    [Fact]
    public void TheNonThrowingLookupAnswersNoRatherThanRaising()
    {
        RunnableGraph graph = Queued().To(Sink.Ignore<int>());

        Assert.True(graph.TryGetControl("ingress", out ResultSlot<IIngressQueue<int>> found));
        Assert.Equal(graph.Control<IIngressQueue<int>>("ingress"), found);

        Assert.False(graph.TryGetControl("inbox", out ResultSlot<IIngressQueue<int>> missing));
        Assert.True(missing.IsDefault);

        Assert.False(graph.TryGetControl("ingress", out ResultSlot<IIngressQueue<string>> mistyped));
        Assert.True(mistyped.IsDefault);

        // A name no graph could declare is a miss rather than a diagnostic: "no" is the whole answer this
        // method promises.
        Assert.False(graph.TryGetControl("Not A Name", out ResultSlot<IIngressQueue<int>> invalid));
        Assert.True(invalid.IsDefault);
    }

    [Fact]
    public void AControlNameThatCollidesWithTheResultNameIsRejectedWhenTheGraphIsClosed()
    {
        // The uniqueness rule is the document's own and covers both kinds of slot at once, which is exactly
        // why controls needed no rule of their own.
        ArgumentException failure = Assert.Throws<ArgumentException>(
            () => Source.Queue<int>(new BufferOptions { Capacity = 2 }, "total")
                .To(s => s.Count(), "total", out ResultSlot<long> _));

        Assert.Contains("repeats the result slot id 'total'", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AQueueGraphIsStillNondeployableAndEphemeral()
    {
        RunnableGraph graph = Queued().To(Sink.Ignore<int>());

        Assert.Contains(CapabilityToken.Nondeployable, graph.Document.Capabilities);
        Assert.Contains(CapabilityToken.EphemeralIdentity, graph.Document.Capabilities);
        Assert.Throws<ArgumentException>(
            () => graph.AsPipeline(GraphId.Create("ingest"), GraphRevision.Create(1)));
    }

    [Fact]
    public void AQueueSourceRejectsItsArgumentsBeforeAnythingIsBuilt()
    {
        Assert.Equal(
            "options",
            Assert.Throws<ArgumentNullException>(() => Source.Queue<int>(null!, "ingress")).ParamName);
        Assert.Equal(
            "controlName",
            Assert.Throws<ArgumentNullException>(
                () => Source.Queue<int>(new BufferOptions { Capacity = 1 }, null!)).ParamName);
        Assert.Equal(
            "controlName",
            Assert.Throws<ArgumentException>(
                () => Source.Queue<int>(new BufferOptions { Capacity = 1 }, "Not A Name")).ParamName);
        Assert.Equal(
            "options",
            Assert.Throws<ArgumentOutOfRangeException>(
                () => Source.Queue<int>(new BufferOptions { Capacity = 0 }, "ingress")).ParamName);
    }

    /// <summary>Starts a chain at a queue named <c>ingress</c>.</summary>
    /// <returns>The source, ready to be closed.</returns>
    private static Source<int> Queued() =>
        Source.Queue<int>(new BufferOptions { Capacity = 2 }, "ingress");
}
