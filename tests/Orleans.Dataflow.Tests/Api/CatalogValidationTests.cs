using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// The integration assertion: every graph the C# API can build is a graph the definition plane accepts.
/// </summary>
/// <remarks>
/// <para>
/// This is what makes the authoring API part of the system rather than beside it. The graph compiler
/// resolves every stage reference, checks every element and result contract, requires every port to be
/// connected, and requires every capability a stage needs to be declared; a graph that came out of
/// <c>To</c> has to survive all of it without exception.
/// </para>
/// <para>
/// The representative graphs are enumerated in one place and validated in one loop, so a graph shape added
/// to the API later has exactly one place to be added here.
/// </para>
/// </remarks>
public sealed class CatalogValidationTests
{
    [Fact]
    public void EveryRepresentativeGraphValidatesAgainstTheLocalCatalog()
    {
        foreach ((string name, RunnableGraph graph) in RepresentativeGraphs())
        {
            GraphValidationReport report = GraphCompiler.Validate(graph.Document, LocalStageCatalog.Instance);

            Assert.True(report.IsValid, $"{name}: {report}");
        }
    }

    [Fact]
    public void EveryChainLengthAndEveryTerminationValidates()
    {
        // The reachable shape space of this API is exactly one linear chain: a source, any number of
        // mappings, filters, buffers and asynchronous mappings, and one of the two terminations. Sweeping
        // it re-derives the claim that every expressible graph is valid, instead of restating a list of
        // graphs already known to be — including for the operators added after the list was written.
        for (int operators = 0; operators <= 12; operators++)
        {
            Source<long> source = Source.From<long>([1L, 2L, 3L]);

            for (int index = 0; index < operators; index++)
            {
                source = (index % 5) switch
                {
                    0 => source.Select(value => value + 1),
                    1 => source.Where(value => value > 0),
                    2 => source.Buffer(new BufferOptions { Capacity = index + 1 }),
                    3 => source.SelectAsync(
                        new ParallelismOptions { MaxConcurrency = index + 1 },
                        (value, _) => Task.FromResult(value)),
                    _ => source.SelectAsyncUnordered(
                        new ParallelismOptions { MaxConcurrency = index + 1 },
                        (value, _) => Task.FromResult(value)),
                };
            }

            RunnableGraph discarded = source.To(Sink.Ignore<long>());
            RunnableGraph counted = source.To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> _);

            Assert.Equal(operators + 2, discarded.Document.Nodes.Count);
            Assert.Equal(operators + 1, discarded.Document.Edges.Count);
            Assert.Equal(operators + 2, counted.Document.Nodes.Count);

            Assert.True(
                GraphCompiler.Validate(discarded.Document, LocalStageCatalog.Instance).IsValid,
                $"discarded chain of {operators}");
            Assert.True(
                GraphCompiler.Validate(counted.Document, LocalStageCatalog.Instance).IsValid,
                $"counted chain of {operators}");
        }
    }

    [Fact]
    public void TheSameGraphsAreRejectedByACatalogThatDoesNotKnowTheLocalStages()
    {
        // Without this, "every graph is valid" would be a claim about the compiler being lenient rather
        // than about the documents being right. The same documents, against a catalog that declares
        // nothing, have to fail — and fail by naming the stage they cannot resolve.
        StageCatalog empty = StageCatalog.Create([]);

        foreach ((string name, RunnableGraph graph) in RepresentativeGraphs())
        {
            GraphValidationReport report = GraphCompiler.Validate(graph.Document, empty);

            Assert.False(report.IsValid, name);
            Assert.All(report.Diagnostics, diagnostic => Assert.Equal("unknown-stage", diagnostic.Rule));
            Assert.Equal(graph.Document.Nodes.Count, report.Diagnostics.Count);
        }
    }

    [Fact]
    public void ALinearGraphNeverDeclaresMoreThanOneResultSlot()
    {
        // The reason the definition plane's duplicate-slot violation is unreachable from this API: a graph
        // is closed by exactly one To, and every To carries at most one slot name. Two slots in one graph
        // arrive with graphs that have more than one sink.
        foreach ((string name, RunnableGraph graph) in RepresentativeGraphs())
        {
            Assert.True(graph.Document.ResultSlots.Count <= 1, name);
            Assert.Equal(graph.Document.ResultSlots.Count, graph.ResultSlots.Count);
        }
    }

    [Fact]
    public void TheCatalogDeclaresExactlyTheEightLocalStages()
    {
        Assert.Equal(
            [
                LocalStage("buffer"),
                LocalStage("fold"),
                LocalStage("from-enumerable"),
                LocalStage("ignore"),
                LocalStage("select"),
                LocalStage("select-async"),
                LocalStage("select-async-unordered"),
                LocalStage("where"),
            ],
            LocalStageCatalog.Instance.Specifications.Select(specification => specification.Stage));
    }

    [Fact]
    public void EveryStageShapeTheVocabularyDeclaresResolvesInTheCatalog()
    {
        // Derived from the enumeration rather than from a list written here, so that a shape added later
        // without a stage reference, without a parameter contract, or without a specification fails this
        // test instead of failing a run. A list would only ever catch a change to the shapes it named.
        LocalStageKind[] kinds = Enum.GetValues<LocalStageKind>();

        Assert.Equal(LocalStageCatalog.Instance.Specifications.Count, kinds.Length);

        foreach (LocalStageKind kind in kinds)
        {
            Assert.True(
                LocalStageCatalog.Instance.TryGetSpecification(
                    LocalVocabulary.StageOf(kind),
                    out StageSpecification? specification),
                kind.ToString());
            Assert.Equal(LocalVocabulary.ParameterContractOf(kind), specification!.ParameterContract);
        }
    }

    [Fact]
    public void EveryLocalStageRequiresTheNondeployableCapability()
    {
        foreach (StageSpecification specification in LocalStageCatalog.Instance.Specifications)
        {
            Assert.Equal([CapabilityToken.Nondeployable], specification.RequiredCapabilities);
        }
    }

    [Fact]
    public void OnlyTheParameterizedStagesDeclareAParameterContractOfTheirOwnAndAValidator()
    {
        // The split that decides which stages a document can describe completely. A capacity and a
        // concurrency bound are numbers a document can state, so they have contracts and checks of their
        // own; every other stage is a delegate, and a delegate is never durable topology.
        Dictionary<string, string> contracts = LocalStageCatalog.Instance.Specifications.ToDictionary(
            specification => specification.Stage.Stage.Value,
            specification => specification.ParameterContract.Contract.Value,
            StringComparer.Ordinal);

        Assert.Equal(
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["buffer"] = "local-buffer-parameters",
                ["fold"] = "local-parameters",
                ["from-enumerable"] = "local-parameters",
                ["ignore"] = "local-parameters",
                ["select"] = "local-parameters",
                ["select-async"] = "local-parallelism-parameters",
                ["select-async-unordered"] = "local-parallelism-parameters",
                ["where"] = "local-parameters",
            },
            contracts);

        foreach (StageSpecification specification in LocalStageCatalog.Instance.Specifications)
        {
            bool parameterized = specification.ParameterContract != Contract("local-parameters");

            Assert.Equal(parameterized, specification.ParameterValidator is not null);
        }
    }

    [Fact]
    public void EveryElementPortDeclaresTheOneOpaqueLocalElementContract()
    {
        // The definition plane forbids CLR type names as contract identity, and a local graph's element
        // types exist only in the C# type system. One opaque contract for every local port is the honest
        // encoding of that, and it is why document-level contract checking proves nothing about a local
        // graph's element typing; the compiler proves that instead.
        foreach (StageSpecification specification in LocalStageCatalog.Instance.Specifications)
        {
            Assert.All(
                specification.InputPorts,
                port =>
                {
                    Assert.Equal("in", port.Id.Value);
                    Assert.Equal(Contract("local-opaque"), port.ElementContract);
                    Assert.False(port.IsOptional);
                });

            Assert.All(
                specification.OutputPorts,
                port =>
                {
                    Assert.Equal("out", port.Id.Value);
                    Assert.Equal(Contract("local-opaque"), port.ElementContract);
                    Assert.False(port.IsIgnorable);
                });

            Assert.All(
                specification.ResultPorts,
                port =>
                {
                    Assert.Equal("result", port.Id.Value);
                    Assert.Equal(Contract("local-fold-result"), port.ResultContract);
                });
        }
    }

    [Fact]
    public void OnlyTheFoldDeclaresAResultPort()
    {
        Dictionary<string, int> resultPorts = LocalStageCatalog.Instance.Specifications.ToDictionary(
            specification => specification.Stage.Stage.Value,
            specification => specification.ResultPorts.Count,
            StringComparer.Ordinal);

        Assert.Equal(
            new Dictionary<string, int>(StringComparer.Ordinal)
            {
                ["buffer"] = 0,
                ["fold"] = 1,
                ["from-enumerable"] = 0,
                ["ignore"] = 0,
                ["select"] = 0,
                ["select-async"] = 0,
                ["select-async-unordered"] = 0,
                ["where"] = 0,
            },
            resultPorts);
    }

    [Fact]
    public void TheCatalogIsOneSharedInstanceAndResolvesEveryStageItDeclares()
    {
        Assert.Same(LocalStageCatalog.Instance, LocalStageCatalog.Instance);

        foreach (StageSpecification specification in LocalStageCatalog.Instance.Specifications)
        {
            Assert.True(
                LocalStageCatalog.Instance.TryGetSpecification(specification.Stage, out StageSpecification? resolved));
            Assert.Same(specification, resolved);
        }
    }

    /// <summary>Builds one graph of every shape the authoring API can express, with a name for diagnostics.</summary>
    /// <returns>The named graphs.</returns>
    /// <remarks>
    /// Every combination that changes the document is here: with and without a result, with and without a
    /// composed flow, with an identity flow that contributes nothing, with a flow used twice, with a
    /// discarded result, and with a chain long enough to separate ordinal from numeric identifier order.
    /// </remarks>
    private static IEnumerable<(string Name, RunnableGraph Graph)> RepresentativeGraphs()
    {
        Flow<OrderCreated, OrderDocument> normalize =
            Flow.For<OrderCreated>().Where(order => order.IsValid).Select(OrderDocument.FromEvent);

        yield return ("source to ignore", Source.From(OrderEvents).To(Sink.Ignore<OrderCreated>()));

        yield return (
            "source to fold",
            Source.From(OrderEvents).To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> _));

        yield return (
            "source via flow to fold",
            Source.From(OrderEvents)
                .Via(normalize)
                .To(s => s.Aggregate(0m, (total, order) => total + order.Total), "total", out ResultSlot<decimal> _));

        yield return (
            "source select where to ignore",
            Source.From(OrderEvents)
                .Select(OrderDocument.FromEvent)
                .Where(order => order.Total > 5m)
                .To(Sink.Ignore<OrderDocument>()));

        yield return (
            "source via identity flow to ignore",
            Source.From(OrderEvents).Via(Flow.For<OrderCreated>()).To(Sink.Ignore<OrderCreated>()));

        yield return (
            "source via one flow twice to ignore",
            Source.From(OrderEvents)
                .Via(normalize)
                .Via(Flow.For<OrderDocument>().Where(order => order.Total > 5m))
                .Via(Flow.For<OrderDocument>().Where(order => order.Total > 5m))
                .To(Sink.Ignore<OrderDocument>()));

        yield return (
            "source to fold with the result discarded",
            Source.From(OrderEvents).To(Sink.Aggregate<OrderCreated, long>(0L, (count, _) => count + 1).ToSink()));

        yield return (
            "source buffered to ignore",
            Source.From(OrderEvents)
                .Buffer(new BufferOptions { Capacity = 4 })
                .To(Sink.Ignore<OrderCreated>()));

        yield return (
            "source buffered under every overflow policy to ignore",
            Source.From(OrderEvents)
                .Buffer(new BufferOptions { Capacity = 1, OverflowPolicy = OverflowPolicy.DropOldest })
                .Buffer(new BufferOptions { Capacity = 2, OverflowPolicy = OverflowPolicy.DropNewest })
                .Buffer(new BufferOptions { Capacity = 3, OverflowPolicy = OverflowPolicy.DropBuffer })
                .Buffer(new BufferOptions { Capacity = 4, OverflowPolicy = OverflowPolicy.Fail })
                .Buffer(new BufferOptions { Capacity = 5, OverflowPolicy = OverflowPolicy.Backpressure })
                .To(Sink.Ignore<OrderCreated>()));

        yield return (
            "source through an ordered asynchronous mapping to fold",
            Source.From(OrderEvents)
                .SelectAsync(
                    new ParallelismOptions { MaxConcurrency = 2 },
                    (order, _) => Task.FromResult(OrderDocument.FromEvent(order)))
                .To(s => s.Aggregate(0m, (total, order) => total + order.Total), "total", out ResultSlot<decimal> _));

        yield return (
            "source through an unordered asynchronous mapping to ignore",
            Source.From(OrderEvents)
                .SelectAsyncUnordered(
                    new ParallelismOptions { MaxConcurrency = 3 },
                    (order, _) => Task.FromResult(order.OrderId))
                .To(Sink.Ignore<string>()));

        yield return (
            "source through a buffered flow with both asynchronous mappings to fold",
            Source.From(OrderEvents)
                .Via(Flow.For<OrderCreated>()
                    .Buffer(new BufferOptions { Capacity = 8, OverflowPolicy = OverflowPolicy.DropOldest })
                    .SelectAsync(new ParallelismOptions { MaxConcurrency = 2 }, (order, _) => Task.FromResult(order.Total))
                    .SelectAsyncUnordered(new ParallelismOptions { MaxConcurrency = 1 }, (total, _) => Task.FromResult(total * 2m))
                    .Where(total => total > 0m)
                    .Buffer(new BufferOptions { Capacity = 2 }))
                .To(s => s.Aggregate(0m, (sum, total) => sum + total), "total", out ResultSlot<decimal> _));

        yield return ("twelve occurrences", LongChain());
    }

    /// <summary>Builds a chain long enough that ordinal identifier order differs from authoring order.</summary>
    /// <returns>The closed graph.</returns>
    private static RunnableGraph LongChain()
    {
        Flow<long, long> ten = Flow.For<long>();

        for (int index = 0; index < 10; index++)
        {
            ten = ten.Select(value => value + 1);
        }

        return Source.From<long>([1L, 2L])
            .Via(ten)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> _);
    }
}
