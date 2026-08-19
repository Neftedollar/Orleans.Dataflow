using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;
using static Orleans.Dataflow.Tests.Api.RegisteredJunctionFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What a branching graph built entirely from registered stages closes into.
/// </summary>
/// <remarks>
/// The sibling of <c>JunctionAuthoringTests</c>, and deliberately its mirror image. That file asserts what
/// a local junction costs a graph — both capability tokens and an element-contract-mismatch at every seam —
/// and this one asserts their absence, on the same shape, once the junction itself is a registered stage.
/// The two together are the statement M4.5 makes: nothing changed about what a local junction is, and a
/// provider can now register one of its own.
/// </remarks>
public sealed class RegisteredJunctionTests
{
    [Fact]
    public void ARegisteredFanOutIsWiredAtTheStagesOwnPortNames()
    {
        // A local junction's legs are 'out-0' and 'out-1' because the local specifications say so. A
        // registered junction's are whatever the provider named them, read from the specification the
        // catalog published, which is why a document built here names 'left' and 'right'.
        RunnableGraph graph = RegisteredFanOut(out ResultSlot<long> _, out ResultSlot<long> _);

        Assert.Equal(
            [
                "normalize#out -> split#in",
                "orders-in#events -> normalize#in",
                "split#left -> count-left#elements",
                "split#right -> count-right#elements",
            ],
            Edges(graph.Document));
    }

    [Fact]
    public void AFullyRegisteredFanOutDeclaresNeitherToken()
    {
        // The sibling of JunctionAuthoringTests.AFanOutOfRegisteredStagesDeclaresBothTokensTheJunctionBrings
        // WithIt, and the whole point of this milestone. The same shape, the same branches, the same
        // author-chosen names — and the junction between them is registered rather than local, so there is
        // no local stage to require 'nondeployable' and no unnamed occurrence to require
        // 'ephemeral-identity'.
        RunnableGraph graph = RegisteredFanOut(out ResultSlot<long> _, out ResultSlot<long> _);

        Assert.Empty(Capabilities(graph.Document));
        Assert.DoesNotContain(NodeIds(graph.Document), id => id.StartsWith("stage-", StringComparison.Ordinal));
        Assert.Equal(
            ["count-left", "count-right", "normalize", "orders-in", "split"],
            NodeIds(graph.Document));
    }

    [Fact]
    public void AFullyRegisteredFanOutValidatesAgainstTheCatalogWithNoSeam()
    {
        // The other half of the same sentence. A local junction's ports declare 'local-opaque@v1', so every
        // edge between one and a registered stage is a correct element-contract-mismatch; a registered
        // junction's ports declare the provider's own contracts, so there is nothing to forgive.
        RunnableGraph graph = RegisteredFanOut(out ResultSlot<long> _, out ResultSlot<long> _);

        GraphValidationReport report = GraphCompiler.Validate(graph.Document, Catalog);

        Assert.True(report.IsValid, report.ToString());
    }

    [Fact]
    public void AFullyRegisteredFanOutIsAcceptedAsAPipeline()
    {
        // The consequence an author meets: AsPipeline rejects a graph declaring either token, and this graph
        // declares neither, so a branching pipeline is deployable for the first time.
        RunnableGraph graph = RegisteredFanOut(out ResultSlot<long> _, out ResultSlot<long> _);

        PipelineDefinition pipeline = graph.AsPipeline(GraphId.Create("orders"), GraphRevision.Create(3));

        Assert.Equal("orders", pipeline.Id.Value);
        Assert.Equal(["left", "right"], pipeline.Document.ResultSlots.Select(slot => slot.Id.Value));

        // The fingerprint is of the deployable document, so it differs from the anonymous graph's.
        Assert.NotEqual(graph.Fingerprint, pipeline.Fingerprint);
    }

    [Fact]
    public void EveryBranchOfARegisteredFanOutDeclaresItsOwnResult()
    {
        // Multiple results were reachable from M4.2, and were reachable only for a nondeployable graph. The
        // same claim, now on a document a cluster could be handed.
        RunnableGraph graph = RegisteredFanOut(out ResultSlot<long> left, out ResultSlot<long> right);

        Assert.Equal(graph.Fingerprint, left.Graph);
        Assert.Equal(graph.Fingerprint, right.Graph);
        Assert.Equal(
            ["count-left#total", "count-right#total"],
            graph.Document.ResultSlots.Select(slot => slot.Producer.ToString()));
    }

    [Fact]
    public void AFullyRegisteredFanInIsAcceptedAsAPipeline()
    {
        // The joining direction of the same claim, and it needs its own test because a fan-in composes
        // through a different operation: two shapes are placed beside each other and then combined, rather
        // than one being split.
        RunnableGraph graph = RegisteredFanIn(out ResultSlot<long> _);

        Assert.Empty(Capabilities(graph.Document));
        Assert.True(GraphCompiler.Validate(graph.Document, Catalog).IsValid);
        Assert.Equal(
            [
                "join#out -> count-out#elements",
                "normalize-primary#out -> join#primary",
                "normalize-secondary#out -> join#secondary",
                "orders-primary#events -> normalize-primary#in",
                "orders-secondary#events -> normalize-secondary#in",
            ],
            Edges(graph.Document));

        PipelineDefinition pipeline = graph.AsPipeline(GraphId.Create("orders"), GraphRevision.Create(1));

        Assert.Equal(1, pipeline.Revision.Value);
    }

    [Fact]
    public void AnUnlikeLeggedFanOutWiresEachLegAtItsOwnContract()
    {
        // The case that makes the claim sharp. One stage, three ports, three different contracts, and every
        // one of them validated — against the catalog here, and against the handle's own declarations when
        // the handle was created. Nothing overrides a specification's port contract because nothing needs
        // to: a provider that wants other contracts registers another stage.
        RunnableGraph graph = RegisteredUnzip(out ResultSlot<long> _, out ResultSlot<long> _);

        Assert.True(GraphCompiler.Validate(graph.Document, Catalog).IsValid);
        Assert.Contains("divide#documents -> count-documents#elements", Edges(graph.Document));
        Assert.Contains("divide#keys -> count-keys#elements", Edges(graph.Document));
        Assert.Empty(Capabilities(graph.Document));
    }

    [Fact]
    public void AnUnlikeInputFanInWiresEachInputAtItsOwnContract()
    {
        RunnableGraph graph = RegisteredZip(out ResultSlot<long> _);

        Assert.True(GraphCompiler.Validate(graph.Document, Catalog).IsValid);
        Assert.Contains("normalize-first#out -> pair#first", Edges(graph.Document));
        Assert.Contains("keys#out -> pair#second", Edges(graph.Document));
        Assert.Empty(Capabilities(graph.Document));
    }

    [Fact]
    public void TheLegAStagesPortsSortFirstIsTheOneTheFirstBranchIsWiredTo()
    {
        // The ordering rule, asserted where it can fail. The unlike-legged stage declares 'keys' before
        // 'documents' in the source that registers it, and a specification sorts its ports at construction,
        // so the first leg is 'documents'. A surface that wired legs by declaration order would put the
        // document branch on the key port and the graph compiler would say so.
        Assert.Equal(
            ["documents", "keys"],
            Divide.Specification.OutputPorts.Select(port => port.Id.Value));
        Assert.Equal("order-document@v1", Divide.Specification.OutputPorts[0].ElementContract.ToString());
    }

    [Fact]
    public void BranchOrderIsIdentityBearingForARegisteredFanOutToo()
    {
        // The same rule ADR 0006 fixed for a local fan-out: reordering the arguments of a junction call
        // reorders the occurrences and therefore the document. Nothing about a registered junction relaxes
        // it — the branches are appended in argument order, and the wiring follows the ports.
        RunnableGraph first = Source.FromRegistered(OrderSource, "orders-in", RegisteredFixtures.SourceParameters)
            .Via(Normalize, "normalize", RegisteredFixtures.NormalizeParameters)
            .FanOutTo(
                Split,
                "split",
                BroadcastParameters,
                Flow.For<OrderDocument>().To(CountSink, "count-a", RegisteredFixtures.CountParameters, "a", out ResultSlot<long> _),
                Flow.For<OrderDocument>().To(CountSink, "count-b", RegisteredFixtures.CountParameters, "b", out ResultSlot<long> _));

        RunnableGraph swapped = Source.FromRegistered(OrderSource, "orders-in", RegisteredFixtures.SourceParameters)
            .Via(Normalize, "normalize", RegisteredFixtures.NormalizeParameters)
            .FanOutTo(
                Split,
                "split",
                BroadcastParameters,
                Flow.For<OrderDocument>().To(CountSink, "count-b", RegisteredFixtures.CountParameters, "b", out ResultSlot<long> _),
                Flow.For<OrderDocument>().To(CountSink, "count-a", RegisteredFixtures.CountParameters, "a", out ResultSlot<long> _));

        Assert.NotEqual(first.Fingerprint, swapped.Fingerprint);
        Assert.Contains("split#left -> count-a#elements", Edges(first.Document));
        Assert.Contains("split#left -> count-b#elements", Edges(swapped.Document));
    }

    [Fact]
    public void TheJunctionsPayloadIsPartOfTheDocumentsIdentity()
    {
        // What a junction does is the provider's, and how this occurrence of it is configured is the
        // document's. Two graphs differing only in that payload are two graphs, which is what makes reading
        // 'mode' at materialization honest rather than a hidden setting.
        RunnableGraph broadcasting = RegisteredFanOut(out ResultSlot<long> _, out ResultSlot<long> _);
        RunnableGraph balancing = RegisteredFanOut(out ResultSlot<long> _, out ResultSlot<long> _, BalanceParameters);

        Assert.NotEqual(broadcasting.Fingerprint, balancing.Fingerprint);
    }

    [Fact]
    public void ALambdaBranchUnderARegisteredJunctionIsStillASeam()
    {
        // The mixing rule is unchanged and this is where it shows. A registered junction's legs carry real
        // contracts, and a local sink's input port carries 'local-opaque@v1', so the edge between them is an
        // element-contract-mismatch exactly as a lambda-to-registered edge has been since ADR 0004. What
        // M4.5 changed is that a fully registered graph no longer has such an edge, not that this one does.
        RunnableGraph graph = Source.FromRegistered(OrderSource, "orders-in", RegisteredFixtures.SourceParameters)
            .Via(Normalize, "normalize", RegisteredFixtures.NormalizeParameters)
            .FanOutTo(
                Split,
                "split",
                BroadcastParameters,
                Flow.For<OrderDocument>().To(CountSink, "count-left", RegisteredFixtures.CountParameters, "left", out ResultSlot<long> _),
                Flow.For<OrderDocument>().To(s => s.Count(), "right", out ResultSlot<long> _));

        GraphValidationReport report = GraphCompiler.Validate(graph.Document, MixedCatalog);

        Assert.False(report.IsValid);
        GraphValidationDiagnostic seam = Assert.Single(report.Diagnostics);
        Assert.Equal("element-contract-mismatch", seam.Rule);
        Assert.Contains("local-opaque@v1", seam.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AFanOutCallWithTheWrongNumberOfBranchesIsRefusedNamingBothNumbers()
    {
        // A junction's arity is what its stage declares, so this is an equality rather than the range a
        // local junction is checked against. Refusing it here is what stops a document from naming a port
        // no stage declares or leaving a declared one unconnected.
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Source.FromRegistered(OrderSource, "orders-in", RegisteredFixtures.SourceParameters)
                .Via(Normalize, "normalize", RegisteredFixtures.NormalizeParameters)
                .FanOutTo(
                    Split,
                    "split",
                    BroadcastParameters,
                    Flow.For<OrderDocument>().To(CountSink, "count-a", RegisteredFixtures.CountParameters, "a", out ResultSlot<long> _)));

        Assert.Contains("declares 2 output ports", refused.Message, StringComparison.Ordinal);
        Assert.Contains("1 branches", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AFanInCallWithTheWrongNumberOfSourcesIsRefusedCountingTheReceiver()
    {
        Source<OrderDocument> stream = Source.FromRegistered(OrderSource, "orders-in", RegisteredFixtures.SourceParameters)
            .Via(Normalize, "normalize", RegisteredFixtures.NormalizeParameters);

        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => stream.FanIn(Join, "join", MergeParameters));

        Assert.Contains("declares 2 input ports", refused.Message, StringComparison.Ordinal);
        Assert.Contains("joins 1 streams", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AJunctionHandleRefusesAStageWhosePortsCarryOtherContracts()
    {
        // Handle creation is where a mismatch between what the author believes and what the catalog says
        // becomes an exception, and for a junction that check runs per port rather than once.
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => RegisteredStage.FanOut(
                Catalog,
                RegisteredFixtures.Stage("split"),
                RegisteredFixtures.OrderDocumentContract,
                OrderKeyContract));

        Assert.Contains("the port 'left'", refused.Message, StringComparison.Ordinal);
        Assert.Contains("the port 'right'", refused.Message, StringComparison.Ordinal);
        Assert.Contains("breaks 2 invariants", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AJunctionHandleRefusesALinearStage()
    {
        // A flow is one input and one output, and a fan-out routes to at least two. Refusing it here is what
        // keeps "a junction" from meaning "whatever stage the author pointed at".
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => RegisteredStage.FanOut(
                Catalog,
                RegisteredFixtures.Stage("normalize"),
                RegisteredFixtures.OrderCreatedContract,
                RegisteredFixtures.OrderDocumentContract));

        Assert.Contains("routes to at least 2", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AJunctionHandleRefusesAStageThatDeclaresAResultPort()
    {
        // A result is read from a terminal and a junction is not one, so a stage declaring one is refused
        // rather than having its result quietly ignored.
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => RegisteredStage.FanIn(
                Catalog,
                RegisteredFixtures.Stage("count-sink"),
                RegisteredFixtures.OrderDocumentContract,
                RegisteredFixtures.OrderDocumentContract));

        Assert.Contains("result port", refused.Message, StringComparison.Ordinal);
        Assert.Contains("a junction is not one", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AJunctionHandleRefusesAStageNoCatalogRegisters()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => RegisteredStage.FanIn(
                Catalog,
                RegisteredFixtures.UnknownStage(),
                RegisteredFixtures.OrderDocumentContract,
                RegisteredFixtures.OrderDocumentContract));

        Assert.Contains("does not register the stage", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCompilerReadsARegisteredJunctionsPortsFromWhicheverCatalogItIsGiven()
    {
        // The multi-port half of validation needed nothing new, and this is the evidence rather than the
        // claim: the very document that validates against the catalog the stages were authored against is
        // reported edge by edge against a catalog whose 'split' declares other ports. Two edges name ports
        // that catalog does not declare as outputs, and two ports it does declare carry no edge.
        RunnableGraph graph = RegisteredFanOut(out ResultSlot<long> _, out ResultSlot<long> _);

        GraphValidationReport report = GraphCompiler.Validate(graph.Document, Renamed());

        Assert.False(report.IsValid);
        Assert.Equal(
            ["unknown-output-port", "unknown-output-port", "unconnected-output-port", "unconnected-output-port"],
            report.Diagnostics.Select(diagnostic => diagnostic.Rule));
    }

    [Fact]
    public void TheCompilerComparesEveryLegsContractSeparately()
    {
        // The same document against a catalog whose second leg carries another contract: one mismatch, on
        // that leg's edge alone. Without this the multi-port contract check could be passing because every
        // leg happened to carry one contract.
        RunnableGraph graph = RegisteredFanOut(out ResultSlot<long> _, out ResultSlot<long> _);

        GraphValidationReport report = GraphCompiler.Validate(graph.Document, Retyped());

        GraphValidationDiagnostic mismatch = Assert.Single(report.Diagnostics);
        Assert.Equal("element-contract-mismatch", mismatch.Rule);
        Assert.Contains("split#right", mismatch.Message, StringComparison.Ordinal);
        Assert.Contains("order-key@v1", mismatch.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AThreeLeggedFanOutIsWiredAtAllThreeOfItsPorts()
    {
        // Arity is read from the specification, so it has to be a number rather than "two". A surface that
        // assumed two would pass every other test in this file and fail here.
        RunnableGraph graph = RegisteredSpread(out ResultSlot<long> _, out ResultSlot<long> _, out ResultSlot<long> _);

        Assert.Equal(3, Spread.Legs);
        Assert.True(GraphCompiler.Validate(graph.Document, Catalog).IsValid);
        Assert.Contains("spread#leg-a -> count-a#elements", Edges(graph.Document));
        Assert.Contains("spread#leg-b -> count-b#elements", Edges(graph.Document));
        Assert.Contains("spread#leg-c -> count-c#elements", Edges(graph.Document));
        Assert.Empty(Capabilities(graph.Document));
    }

    [Fact]
    public void AThreeInputFanInJoinsTheReceiverAndBothArguments()
    {
        // The joining direction of the same claim, and the one that exercises the arithmetic: a call joins
        // the receiver plus its arguments, so three inputs means two arguments.
        RunnableGraph graph = RegisteredGather(out ResultSlot<long> _);

        Assert.Equal(3, Gather.Inputs);
        Assert.True(GraphCompiler.Validate(graph.Document, Catalog).IsValid);
        Assert.Contains("normalize-a#out -> gather#src-a", Edges(graph.Document));
        Assert.Contains("normalize-b#out -> gather#src-b", Edges(graph.Document));
        Assert.Contains("normalize-c#out -> gather#src-c", Edges(graph.Document));
        Assert.Empty(Capabilities(graph.Document));
    }

    [Fact]
    public void ANullJunctionHandleOrBranchIsRefusedBeforeAnythingIsComposed()
    {
        Source<OrderDocument> stream = Source.FromRegistered(OrderSource, "orders-in", RegisteredFixtures.SourceParameters)
            .Via(Normalize, "normalize", RegisteredFixtures.NormalizeParameters);
        Branch<OrderDocument> branch = Flow.For<OrderDocument>()
            .To(CountSink, "count", RegisteredFixtures.CountParameters, "counted", out ResultSlot<long> _);

        Assert.Throws<ArgumentNullException>(
            () => stream.FanOutTo<OrderDocument>(null!, "split", BroadcastParameters, branch, branch));
        Assert.Throws<ArgumentNullException>(
            () => stream.FanOutTo(Split, "split", BroadcastParameters, branch, null!));
        Assert.Throws<ArgumentNullException>(() => stream.FanIn<OrderDocument>(null!, "join", MergeParameters, stream));
        Assert.Throws<ArgumentNullException>(() => stream.FanIn(Join, "join", MergeParameters, null!));
        Assert.Throws<ArgumentNullException>(() => stream.FanIn(Join, null!, MergeParameters, stream));
    }

    [Fact]
    public void AJunctionOccurrenceNameAndPayloadAreCheckedAtTheAttachment()
    {
        // The same two rules every registered attachment applies, at the call the author wrote rather than
        // at the one that happens to close the graph afterwards.
        Source<OrderDocument> stream = Source.FromRegistered(OrderSource, "orders-in", RegisteredFixtures.SourceParameters)
            .Via(Normalize, "normalize", RegisteredFixtures.NormalizeParameters);

        ArgumentException named = Assert.Throws<ArgumentException>(
            () => stream.FanIn(Join, "not a name", MergeParameters, stream));
        ArgumentException paid = Assert.Throws<ArgumentException>(
            () => stream.FanIn(Join, "join", default, stream));

        Assert.Equal("occurrenceName", named.ParamName);
        Assert.Contains("parameters", paid.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Builds a catalog whose fan-out declares two ports under other names.</summary>
    /// <returns>The catalog.</returns>
    private static StageCatalog Renamed() =>
        Rewritten(Leg("port-a", "order-document"), Leg("port-b", "order-document"));

    /// <summary>Builds a catalog whose fan-out declares its second leg under another contract.</summary>
    /// <returns>The catalog.</returns>
    private static StageCatalog Retyped() => Rewritten(Leg("left", "order-document"), Leg("right", "order-key"));

    /// <summary>Builds a catalog identical to the fixture one but for the fan-out's output ports.</summary>
    /// <param name="ports">The output ports the rewritten fan-out declares.</param>
    /// <returns>The catalog.</returns>
    private static StageCatalog Rewritten(params OutputPortSpecification[] ports) =>
        StageCatalog.Create(
        [
            .. Catalog.Specifications.Where(
                specification => specification.Stage != RegisteredFixtures.Stage("split")),
            StageSpecification.FanOut(
                RegisteredFixtures.Stage("split"),
                ContractReference.Create(ContractId.Create("split-parameters"), 1),
                Port.In("in", Contract("order-document")),
                ports),
        ]);

    /// <summary>Builds one leg of the rewritten fan-out.</summary>
    /// <param name="port">The port name.</param>
    /// <param name="contract">The element contract identifier text.</param>
    /// <returns>The port specification.</returns>
    private static OutputPortSpecification Leg(string port, string contract) =>
        Port.Out(port, Contract(contract));
}
