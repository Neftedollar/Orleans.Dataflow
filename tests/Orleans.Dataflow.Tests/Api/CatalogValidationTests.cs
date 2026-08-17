using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Testing;
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
        // operators, and one termination. Sweeping it re-derives the claim that every expressible graph is
        // valid, instead of restating a list of graphs already known to be — including for the operators
        // added after the list was written.
        for (int operators = 0; operators <= 24; operators++)
        {
            Source<long> source = Source.From<long>([1L, 2L, 3L]);

            for (int index = 0; index < operators; index++)
            {
                source = (index % 13) switch
                {
                    0 => source.Select(value => value + 1),
                    1 => source.Where(value => value > 0),
                    2 => source.Buffer(new BufferOptions { Capacity = index + 1 }),
                    3 => source.SelectAsync(
                        new ParallelismOptions { MaxConcurrency = index + 1 },
                        (value, _) => Task.FromResult(value)),
                    4 => source.SelectAsyncUnordered(
                        new ParallelismOptions { MaxConcurrency = index + 1 },
                        (value, _) => Task.FromResult(value)),
                    5 => source.Scan(0L, (sum, value) => sum + value),
                    6 => source.Take(index + 100),
                    7 => source.Skip(index),
                    8 => source.TakeWhile(value => value < long.MaxValue),
                    9 => source.TakeThrough(value => value < long.MaxValue),
                    10 => source.SelectValueTaskAsync(
                        new ParallelismOptions { MaxConcurrency = index + 1 },
                        (value, _) => ValueTask.FromResult(value)),
                    11 => source.SelectValueTaskAsyncUnordered(
                        new ParallelismOptions { MaxConcurrency = index + 1 },
                        (value, _) => ValueTask.FromResult(value)),
                    _ => source.Distinct(new DistinctOptions { MaxTrackedKeys = index + 1 }),
                };
            }

            foreach ((string name, RunnableGraph graph) in Terminations(source))
            {
                Assert.Equal(operators + 2, graph.Document.Nodes.Count);
                Assert.Equal(operators + 1, graph.Document.Edges.Count);
                Assert.True(
                    GraphCompiler.Validate(graph.Document, LocalStageCatalog.Instance).IsValid,
                    $"{name} chain of {operators}");
            }
        }
    }

    [Fact]
    public void EverySourceTheApiCanStartFromValidates()
    {
        // The other end of the same sweep. Every source is closed with the same discarding sink, so what
        // is under test is the source alone.
        foreach ((string name, RunnableGraph graph) in Sources())
        {
            GraphValidationReport report = GraphCompiler.Validate(graph.Document, LocalStageCatalog.Instance);

            Assert.True(report.IsValid, $"{name}: {report}");
            Assert.Equal(2, graph.Document.Nodes.Count);
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
    public void ALinearGraphNeverDeclaresMoreThanOneResultSlotAndAnyNumberOfControls()
    {
        // The reason the definition plane's duplicate-slot violation is unreachable from this API: a graph
        // is closed by exactly one To, and every To carries at most one slot name. Two results in one graph
        // arrive with graphs that have more than one sink.
        //
        // Controls are the other half of the same sentence and are not bounded by one, because they are not
        // declared by the closing call at all: a queue and a probe name theirs on the stage that produces
        // them, and a chain may hold more than one such stage. The port name is what tells the two apart,
        // which is why the ports are separate identities rather than one.
        foreach ((string name, RunnableGraph graph) in RepresentativeGraphs())
        {
            Assert.True(
                graph.Document.ResultSlots.Count(slot => slot.Producer.Port.Value == "result") <= 1,
                name);
            Assert.All(
                graph.Document.ResultSlots,
                slot => Assert.True(
                    slot.Producer.Port.Value is "result" or "control",
                    $"{name}: {slot.Producer.Port}"));
            Assert.Equal(graph.Document.ResultSlots.Count, graph.ResultSlots.Count);
        }
    }

    [Fact]
    public void TheCatalogDeclaresExactlyTheLocalStages()
    {
        // Spelled out rather than derived, and in the catalog's own canonical order. The catalog is built
        // from the enumeration of shapes, so a list derived the same way would agree with it whatever
        // either said; this list is the independent statement of what the vocabulary is.
        Assert.Equal(
            [
                LocalStage("buffer"),
                LocalStage("collect"),
                LocalStage("count"),
                LocalStage("cycle"),
                LocalStage("distinct"),
                LocalStage("empty"),
                LocalStage("failed"),
                LocalStage("first"),
                LocalStage("first-or-default"),
                LocalStage("fold"),
                LocalStage("for-each"),
                LocalStage("for-each-async"),
                LocalStage("from-async-enumerable"),
                LocalStage("from-async-factory"),
                LocalStage("from-channel"),
                LocalStage("from-enumerable"),
                LocalStage("from-factory"),
                LocalStage("from-task"),
                LocalStage("ignore"),
                LocalStage("last"),
                LocalStage("last-or-default"),
                LocalStage("never"),
                LocalStage("queue"),
                LocalStage("range"),
                LocalStage("repeat"),
                LocalStage("scan"),
                LocalStage("select"),
                LocalStage("select-async"),
                LocalStage("select-async-unordered"),
                LocalStage("select-value-task-async"),
                LocalStage("select-value-task-async-unordered"),
                LocalStage("single"),
                LocalStage("sink-probe"),
                LocalStage("skip"),
                LocalStage("skip-while"),
                LocalStage("take"),
                LocalStage("take-through"),
                LocalStage("take-while"),
                LocalStage("to-channel"),
                LocalStage("unfold"),
                LocalStage("unfold-async"),
                LocalStage("where"),
            ],
            LocalStageCatalog.Instance.Specifications.Select(specification => specification.Stage));
    }

    [Fact]
    public void EveryStageDeclaresThePortsItsPlaceInAChainImplies()
    {
        // The ports are derived from where a shape stands, so this is where that derivation is checked
        // against a list written by hand: a source consumes nothing, a sink produces nothing, and
        // everything else does both. A shape that moved between the three would keep validating and start
        // executing somewhere it cannot stand.
        string[] sources =
        [
            "cycle",
            "empty",
            "failed",
            "from-async-enumerable",
            "from-async-factory",
            "from-channel",
            "from-enumerable",
            "from-factory",
            "from-task",
            "never",
            "queue",
            "range",
            "repeat",
            "single",
            "unfold",
            "unfold-async",
        ];
        string[] sinks =
        [
            "collect",
            "count",
            "first",
            "first-or-default",
            "fold",
            "for-each",
            "for-each-async",
            "ignore",
            "last",
            "last-or-default",
            "sink-probe",
            "to-channel",
        ];

        foreach (StageSpecification specification in LocalStageCatalog.Instance.Specifications)
        {
            string stage = specification.Stage.Stage.Value;

            Assert.Equal(sources.Contains(stage) ? 0 : 1, specification.InputPorts.Count);
            Assert.Equal(sinks.Contains(stage) ? 0 : 1, specification.OutputPorts.Count);
        }
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
                ["collect"] = "local-collect-parameters",
                ["count"] = "local-parameters",
                ["cycle"] = "local-parameters",
                ["distinct"] = "local-distinct-parameters",
                ["empty"] = "local-parameters",
                ["failed"] = "local-parameters",
                ["first"] = "local-parameters",
                ["first-or-default"] = "local-parameters",
                ["fold"] = "local-parameters",
                ["for-each"] = "local-parameters",
                ["for-each-async"] = "local-parallelism-parameters",
                ["from-async-enumerable"] = "local-parameters",
                ["from-async-factory"] = "local-parameters",
                ["from-channel"] = "local-parameters",
                ["from-enumerable"] = "local-parameters",
                ["from-factory"] = "local-parameters",
                ["from-task"] = "local-parameters",
                ["ignore"] = "local-parameters",
                ["last"] = "local-parameters",
                ["last-or-default"] = "local-parameters",
                ["never"] = "local-parameters",
                ["queue"] = "local-buffer-parameters",
                ["range"] = "local-range-parameters",
                ["repeat"] = "local-count-parameters",
                ["scan"] = "local-parameters",
                ["select"] = "local-parameters",
                ["select-async"] = "local-parallelism-parameters",
                ["select-async-unordered"] = "local-parallelism-parameters",
                ["select-value-task-async"] = "local-parallelism-parameters",
                ["select-value-task-async-unordered"] = "local-parallelism-parameters",
                ["single"] = "local-parameters",
                ["sink-probe"] = "local-parameters",
                ["skip"] = "local-count-parameters",
                ["skip-while"] = "local-parameters",
                ["take"] = "local-count-parameters",
                ["take-through"] = "local-parameters",
                ["take-while"] = "local-parameters",
                ["to-channel"] = "local-parameters",
                ["unfold"] = "local-parameters",
                ["unfold-async"] = "local-parameters",
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

            // Three opaque result contracts and not one. 'local-fold-result' is the identity a fold's
            // result port has always declared, and a durable contract identifier is not renamed to cover
            // sinks that do not fold, so the sinks that arrived later declare the general one instead;
            // 'local-control' is a third identity because a control is not a result at all — its value
            // exists when the run starts rather than when it ends, and the port name says so.
            Assert.All(
                specification.ResultPorts,
                port =>
                {
                    (string expectedPort, string expectedContract) = specification.Stage.Stage.Value switch
                    {
                        "fold" => ("result", "local-fold-result"),
                        "queue" or "sink-probe" => ("control", "local-control"),
                        _ => ("result", "local-result"),
                    };

                    Assert.Equal(expectedPort, port.Id.Value);
                    Assert.Equal(Contract(expectedContract), port.ResultContract);
                });
        }
    }

    [Fact]
    public void OnlyTheResultBearingSinksDeclareAResultPort()
    {
        Dictionary<string, int> resultPorts = LocalStageCatalog.Instance.Specifications.ToDictionary(
            specification => specification.Stage.Stage.Value,
            specification => specification.ResultPorts.Count,
            StringComparer.Ordinal);

        Assert.Equal(
            new Dictionary<string, int>(StringComparer.Ordinal)
            {
                ["buffer"] = 0,
                ["collect"] = 1,
                ["count"] = 1,
                ["cycle"] = 0,
                ["distinct"] = 0,
                ["empty"] = 0,
                ["failed"] = 0,
                ["first"] = 1,
                ["first-or-default"] = 1,
                ["fold"] = 1,
                ["for-each"] = 0,
                ["for-each-async"] = 0,
                ["from-async-enumerable"] = 0,
                ["from-async-factory"] = 0,
                ["from-channel"] = 0,
                ["from-enumerable"] = 0,
                ["from-factory"] = 0,
                ["from-task"] = 0,
                ["ignore"] = 0,
                ["last"] = 1,
                ["last-or-default"] = 1,
                ["never"] = 0,
                ["queue"] = 1,
                ["range"] = 0,
                ["repeat"] = 0,
                ["scan"] = 0,
                ["select"] = 0,
                ["select-async"] = 0,
                ["select-async-unordered"] = 0,
                ["select-value-task-async"] = 0,
                ["select-value-task-async-unordered"] = 0,
                ["single"] = 0,
                ["sink-probe"] = 1,
                ["skip"] = 0,
                ["skip-while"] = 0,
                ["take"] = 0,
                ["take-through"] = 0,
                ["take-while"] = 0,
                ["to-channel"] = 0,
                ["unfold"] = 0,
                ["unfold-async"] = 0,
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
            "source through both value-task mappings to fold",
            Source.From(OrderEvents)
                .SelectValueTaskAsync(
                    new ParallelismOptions { MaxConcurrency = 2 },
                    (order, _) => ValueTask.FromResult(order.Total))
                .SelectValueTaskAsyncUnordered(
                    new ParallelismOptions { MaxConcurrency = 1 },
                    (total, _) => ValueTask.FromResult(total * 2m))
                .To(s => s.Aggregate(0m, (sum, total) => sum + total), "total", out ResultSlot<decimal> _));

        yield return (
            "probe source through a buffer to a probe sink",
            TestSource.Probe<OrderCreated>("emitted")
                .Buffer(new BufferOptions { Capacity = 2 })
                .To(TestSink.Probe<OrderCreated>("received")));

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

        yield return (
            "source through every counted operator to ignore",
            Source.From(OrderEvents)
                .Skip(1)
                .Take(2)
                .Distinct(new DistinctOptions { MaxTrackedKeys = 16 })
                .To(Sink.Ignore<OrderCreated>()));

        yield return (
            "source through every predicate operator to for-each",
            Source.From(OrderEvents)
                .TakeWhile(order => order.IsValid)
                .TakeThrough(order => order.Total < 100m)
                .SkipWhile(order => order.Total < 0m)
                .To(s => s.ForEach(_ => { })));

        yield return (
            "source scanned to first",
            Source.From(OrderEvents)
                .Scan(0m, (total, order) => total + order.Total)
                .To(s => s.First(), "head", out ResultSlot<decimal> _));

        yield return (
            "source to first-or-default",
            Source.From(OrderEvents).To(s => s.FirstOrDefault(), "head", out ResultSlot<OrderCreated?> _));

        yield return (
            "source to count",
            Source.From(OrderEvents).To(s => s.Count(), "counted", out ResultSlot<long> _));

        yield return (
            "source to an asynchronous callback sink",
            Source.From(OrderEvents)
                .To(s => s.ForEachAsync(
                    new ParallelismOptions { MaxConcurrency = 2 },
                    (_, _) => Task.CompletedTask)));

        foreach ((string name, RunnableGraph graph) in Sources())
        {
            yield return (name, graph);
        }
    }

    /// <summary>Builds one graph per source the API can start from, closed by the same discarding sink.</summary>
    /// <returns>The named graphs.</returns>
    private static IEnumerable<(string Name, RunnableGraph Graph)> Sources()
    {
        yield return ("probe source", TestSource.Probe<int>("emitted").To(Sink.Ignore<int>()));
        yield return ("empty source", Source.Empty<int>().To(Sink.Ignore<int>()));
        yield return ("single-element source", Source.Single(1).To(Sink.Ignore<int>()));
        yield return ("repeating source", Source.Repeat(1, 3).To(Sink.Ignore<int>()));
        yield return ("range source", Source.Range(-1, 4).To(Sink.Ignore<int>()));
        yield return ("task source", Source.FromTask(Task.FromResult(1)).To(Sink.Ignore<int>()));
        yield return (
            "failed source",
            Source.Failed<int>(new InvalidOperationException("no")).To(Sink.Ignore<int>()));
        yield return (
            "unfolded source",
            Source.Unfold(
                    0,
                    (int state, out int value, out int next) =>
                    {
                        value = state;
                        next = state + 1;

                        return state < 3;
                    })
                .To(Sink.Ignore<int>()));
    }

    /// <summary>Closes one chain with each termination the API offers.</summary>
    /// <param name="source">The chain to close.</param>
    /// <returns>The named graphs.</returns>
    private static IEnumerable<(string Name, RunnableGraph Graph)> Terminations(Source<long> source)
    {
        yield return ("discarded", source.To(Sink.Ignore<long>()));
        yield return (
            "folded",
            source.To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> _));
        yield return ("called", source.To(s => s.ForEach(_ => { })));
        yield return (
            "called asynchronously",
            source.To(s => s.ForEachAsync(new ParallelismOptions { MaxConcurrency = 1 }, (_, _) => Task.CompletedTask)));
        yield return ("first", source.To(s => s.First(), "head", out ResultSlot<long> _));
        yield return ("first or default", source.To(s => s.FirstOrDefault(), "head", out ResultSlot<long> _));
        yield return ("counted", source.To(s => s.Count(), "counted", out ResultSlot<long> _));
        yield return ("probed", source.To(TestSink.Probe<long>("received")));
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
