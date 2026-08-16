using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;
using static Orleans.Dataflow.Tests.Api.RegisteredFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// The mixing rule: registered and lambda stages compose in one chain, and the closed document declares
/// exactly what it contains.
/// </summary>
/// <remarks>
/// <para>
/// Mixing is legal because it is useful — a registered stage inside a lambda harness is how one is tested
/// — and because refusing it would need a second vocabulary to refuse it in. What mixing must not do is
/// launder deployability: a document holding one lambda stage is nondeployable however many registered
/// stages surround it.
/// </para>
/// <para>
/// The tokens used to be unconditional, because every occurrence this API could build was a lambda. They
/// are now derived, and these tests are what makes "derived" mean something: each token has to appear
/// exactly when its cause is present and be absent otherwise.
/// </para>
/// <para>
/// Mixing has a limit these tests state rather than hide: every local port declares one opaque element
/// contract, because a local graph's element types live in the C# type system and nowhere else. An edge
/// from a registered stage to a lambda one therefore joins a real contract to that opaque one, and the
/// graph compiler reports it — correctly. A mixed graph is an authoring and materialization affordance,
/// not a document the definition plane can type-check end to end.
/// </para>
/// </remarks>
public sealed class RegisteredMixingTests
{
    [Fact]
    public void ALambdaStageBetweenTwoRegisteredOnesDeclaresBothLocalTokens()
    {
        RunnableGraph graph = Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .Select(OrderDocument.FromEvent)
            .To(IndexSink, "index-out", IndexParameters);

        Assert.Equal(["ephemeral-identity", "nondeployable"], Capabilities(graph.Document));
        Assert.Equal(["index-out", "orders-in", "stage-0002"], NodeIds(graph.Document));
    }

    [Fact]
    public void TheNumberingOfALambdaOccurrenceIsItsPositionInTheWholeChain()
    {
        // A registered occurrence numbers nothing, but it does occupy a position: 'stage-0002' is the
        // second occurrence of this chain and not the first lambda of it. That keeps a lambda-only graph
        // numbered exactly as it always was, which is what leaves every existing fingerprint alone.
        RunnableGraph graph = Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .Select(OrderDocument.FromEvent)
            .Where(order => order.Total > 0m)
            .To(IndexSink, "index-out", IndexParameters);

        Assert.Equal(
            ["index-out", "orders-in", "stage-0002", "stage-0003"],
            NodeIds(graph.Document));
    }

    [Fact]
    public void ARegisteredStageInsideALambdaHarnessIsStillALambdaGraph()
    {
        RunnableGraph graph = Source.From(OrderEvents)
            .Via(Normalize, "normalize", NormalizeParameters)
            .To(Sink.Ignore<OrderDocument>());

        Assert.Equal(["ephemeral-identity", "nondeployable"], Capabilities(graph.Document));
        Assert.Equal(["normalize", "stage-0001", "stage-0003"], NodeIds(graph.Document));
    }

    [Fact]
    public void AFullyRegisteredChainDeclaresNeitherLocalToken()
    {
        Assert.Empty(Indexed().Document.Capabilities);
    }

    [Fact]
    public void EachTokenAppearsExactlyWhenItsOwnCauseIsPresent()
    {
        // The conditional emission, swept rather than sampled: 'nondeployable' tracks the presence of a
        // local stage and 'ephemeral-identity' tracks the presence of a machine-made identifier. Each is
        // compared against its own cause read off the document, so a builder that emitted either for the
        // wrong reason would fail here even while the other one still looked right.
        foreach ((string name, RunnableGraph graph) in MixedGraphs())
        {
            bool numbered = graph.Document.Nodes.Any(
                node => node.Id.Value.StartsWith("stage-", StringComparison.Ordinal));
            bool local = graph.Document.Nodes.Any(node => node.Stage.Provider.Value == "local");

            Assert.Equal(
                numbered,
                graph.Document.Capabilities.Contains(CapabilityToken.EphemeralIdentity));
            Assert.Equal(
                local,
                graph.Document.Capabilities.Contains(CapabilityToken.Nondeployable));
            Assert.False(name is null);
        }
    }

    [Fact]
    public void EphemeralIdentityCanOnlyComeFromALambdaStageBecauseARegisteredOneMustBeNamed()
    {
        // Every registered attachment takes a name and requires it, and no lambda attachment takes one at
        // all. So through this surface the two tokens have exactly one cause between them — the presence
        // of a lambda stage — and therefore always appear together or not at all. They stay orthogonal in
        // the model (ADR 0004 section 6): a document with registered stages and machine-made names is
        // writable by hand and readable by the compiler, it is simply not authorable here. This is what
        // would fail if a later checkpoint gave lambda stages a name or registered ones a default one.
        foreach ((string name, RunnableGraph graph) in MixedGraphs())
        {
            bool ephemeral = graph.Document.Capabilities.Contains(CapabilityToken.EphemeralIdentity);
            bool nondeployable = graph.Document.Capabilities.Contains(CapabilityToken.Nondeployable);

            Assert.Equal(ephemeral, nondeployable);
            Assert.False(name is null);
        }
    }

    [Fact]
    public void ABufferMakesAGraphNondeployableEvenThoughItCarriesNoDelegate()
    {
        // The one local stage whose whole behavior is written down. It is still nondeployable, and that is
        // a statement about where it can run rather than about whether the author wrote a lambda for it:
        // 'local/buffer@v1' resolves in the local provider and nowhere else, so a document naming it is
        // executable only by the process that has that provider. Every local stage specification requires
        // the token, and the builder declares what its occurrences require.
        RunnableGraph graph = Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .Buffer(new BufferOptions { Capacity = 4 })
            .Via(Normalize, "normalize", NormalizeParameters)
            .To(IndexSink, "index-out", IndexParameters);

        Assert.Equal(["ephemeral-identity", "nondeployable"], Capabilities(graph.Document));
        Assert.Equal("local/buffer@v1", Assert.Single(
            graph.Document.Nodes,
            node => node.Id.Value == "stage-0002").Stage.ToString());
    }

    [Fact]
    public void AnAsynchronousStageIsNondeployableForBothReasonsAtOnce()
    {
        RunnableGraph graph = Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 2 },
                (order, _) => Task.FromResult(OrderDocument.FromEvent(order)))
            .To(IndexSink, "index-out", IndexParameters);

        Assert.Equal(["ephemeral-identity", "nondeployable"], Capabilities(graph.Document));
    }

    [Fact]
    public void TheSeamBetweenALambdaStageAndARegisteredOneIsAnElementContractMismatch()
    {
        // The limit of mixing, pinned rather than discovered later. A local port declares 'local-opaque@v1'
        // whatever flows through it, because a local graph's element types exist only in the C# type
        // system; a registered port declares a real contract. An edge across that seam joins two contracts
        // that are not equal, and contract equality is the whole of the definition plane's element rule, so
        // the compiler reports it. Nothing here is a compiler defect and nothing here is fixable by a
        // catalog: it is what "the local vocabulary is deliberately blind" costs when the two meet in one
        // document. The C# compiler is still what proves this chain's element typing.
        RunnableGraph graph = Source.From(OrderEvents)
            .Via(Normalize, "normalize", NormalizeParameters)
            .To(Sink.Ignore<OrderDocument>());

        GraphValidationReport report = GraphCompiler.Validate(graph.Document, MixedCatalog);

        Assert.False(report.IsValid);
        Assert.All(report.Diagnostics, diagnostic => Assert.Equal("element-contract-mismatch", diagnostic.Rule));
        Assert.Equal(2, report.Diagnostics.Count);

        // One seam on each side of the registered stage, and every one of them names the opaque contract
        // beside the real one it could not be reconciled with.
        Assert.All(
            report.Diagnostics,
            diagnostic => Assert.Contains("local-opaque@v1", diagnostic.Message, StringComparison.Ordinal));
        Assert.Contains(
            report.Diagnostics,
            diagnostic => diagnostic.Message.Contains("order-created@v1", StringComparison.Ordinal));
        Assert.Contains(
            report.Diagnostics,
            diagnostic => diagnostic.Message.Contains("order-document@v1", StringComparison.Ordinal));
    }

    [Fact]
    public void EveryOtherRuleIsSatisfiedOnBothSidesOfThatSeam()
    {
        // The mismatch is the only thing wrong: every stage resolves, every port is connected, every
        // payload passes its stage's own check, and every required capability is declared. Without this,
        // the claim above would be "mixed graphs fail validation" rather than "mixed graphs fail exactly
        // one rule, at exactly the seams, for exactly one reason".
        foreach ((string name, RunnableGraph graph) in MixedGraphs())
        {
            GraphValidationReport report = GraphCompiler.Validate(graph.Document, MixedCatalog);

            Assert.All(
                report.Diagnostics,
                diagnostic => Assert.Equal("element-contract-mismatch", diagnostic.Rule));
            Assert.Equal(Seams(graph), report.Diagnostics.Count);
            Assert.True(report.IsValid == (Seams(graph) == 0), name);
        }
    }

    [Fact]
    public void AMixedDocumentIsUnresolvableAgainstEitherCatalogAlone()
    {
        RunnableGraph graph = Source.From(OrderEvents)
            .Via(Normalize, "normalize", NormalizeParameters)
            .To(Sink.Ignore<OrderDocument>());

        GraphValidationReport local = GraphCompiler.Validate(graph.Document, LocalStageCatalog.Instance);
        GraphValidationReport registered = GraphCompiler.Validate(graph.Document, Catalog);

        Assert.False(local.IsValid);
        Assert.False(registered.IsValid);
        Assert.Equal("unknown-stage", Assert.Single(local.Diagnostics).Rule);
        Assert.Equal("normalize", Assert.Single(local.Diagnostics).Subject);
        Assert.Equal(2, registered.Diagnostics.Count(diagnostic => diagnostic.Rule == "unknown-stage"));

        // And the composite resolves every one of them, which is the part a merged catalog does fix.
        Assert.DoesNotContain(
            GraphCompiler.Validate(graph.Document, MixedCatalog).Diagnostics,
            diagnostic => diagnostic.Rule == "unknown-stage");
    }

    [Fact]
    public void TheCompositeCatalogEnumeratesBothVocabulariesInCanonicalOrder()
    {
        // The interface promises an order that is a property of the contents alone, and a wrapper that
        // concatenated its two sources would break that promise for exactly the catalogs a mixed graph
        // needs. 'local' precedes 'orleans-test' ordinally, so the local stages come first whichever
        // catalog was consulted first.
        Assert.Equal(
            LocalStageCatalog.Instance.Specifications.Count + Catalog.Specifications.Count,
            MixedCatalog.Specifications.Count);
        Assert.Equal(
            MixedCatalog.Specifications.Select(specification => specification.Stage),
            new CompositeStageCatalog(Catalog, LocalStageCatalog.Instance)
                .Specifications.Select(specification => specification.Stage));
    }

    [Fact]
    public void AMixedChainStillClosesOneLinearDocument()
    {
        RunnableGraph graph = Source.From(OrderEvents)
            .Where(order => order.IsValid)
            .Via(Normalize, "normalize", NormalizeParameters)
            .Via(Enrich, "enrich", CanonicalJsonValue.Parse("{}"))
            .Select(order => order.OrderId)
            .To(Sink.Ignore<string>());

        Assert.Equal(
            ["enrich", "normalize", "stage-0001", "stage-0002", "stage-0005", "stage-0006"],
            NodeIds(graph.Document));
        Assert.Equal(
            [
                "enrich#out -> stage-0005#in",
                "normalize#out -> enrich#in",
                "stage-0001#out -> stage-0002#in",
                "stage-0002#out -> normalize#in",
                "stage-0005#out -> stage-0006#in",
            ],
            Edges(graph.Document));
    }

    /// <summary>Counts the edges of a graph that join a local port to a registered one.</summary>
    /// <param name="graph">The closed graph.</param>
    /// <returns>The number of seams, which is the number of contract mismatches the compiler reports.</returns>
    /// <remarks>
    /// Derived from the document rather than from a number written beside each fixture, so that a graph
    /// added to the sweep later is counted rather than forgotten.
    /// </remarks>
    private static int Seams(RunnableGraph graph)
    {
        Dictionary<NodeId, bool> local = graph.Document.Nodes.ToDictionary(
            node => node.Id,
            node => node.Stage.Provider.Value == "local");

        return graph.Document.Edges.Count(edge => local[edge.From.Node] != local[edge.To.Node]);
    }

    /// <summary>Builds one graph of every mixing shape, with a name for diagnostics.</summary>
    /// <returns>The named graphs.</returns>
    /// <remarks>
    /// Every combination of the two kinds that changes what the document declares: neither, one at each
    /// position, both terminations, and a result.
    /// </remarks>
    private static IEnumerable<(string Name, RunnableGraph Graph)> MixedGraphs()
    {
        yield return ("registered only", Indexed());

        yield return ("registered only with a result", Counted(out ResultSlot<long> _));

        yield return (
            "registered source, lambda tail",
            Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
                .Select(OrderDocument.FromEvent)
                .To(Sink.Ignore<OrderDocument>()));

        yield return (
            "lambda source, registered tail",
            Source.From(OrderEvents)
                .Via(Normalize, "normalize", NormalizeParameters)
                .To(IndexSink, "index-out", IndexParameters));

        yield return (
            "lambda source, registered result",
            Source.From(OrderEvents)
                .Select(OrderDocument.FromEvent)
                .To(CountSink, "count-out", CountParameters, "processed", out ResultSlot<long> _));

        yield return (
            "lambda only",
            Source.From(OrderEvents).To(Sink.Ignore<OrderCreated>()));

        yield return (
            "lambda only with a result",
            Source.From(OrderEvents)
                .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> _));
    }
}
