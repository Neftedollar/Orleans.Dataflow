using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;
using static Orleans.Dataflow.Tests.Api.RegisteredFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// The registered junctions, and the typed handles over them, the multi-port authoring tests are written
/// against.
/// </summary>
/// <remarks>
/// <para>
/// This is the half of a provider SDK that M4.5 opened: four junction stages beside the linear ones, with
/// real element contracts on every port. Two of them are like-legged — a fan-out whose legs both carry
/// documents, a fan-in whose inputs both do — and two are not, which is the case that matters most: one
/// stage whose ports declare three different contracts is what makes "no occurrence overrides its
/// specification" a demonstrated claim rather than an assertion.
/// </para>
/// <para>
/// The port names disagree with the local vocabulary's <c>out-0</c> and <c>in-0</c> everywhere a test can
/// see the difference, and they are deliberately not written in the order they are used: the unlike-legged
/// fan-out declares <c>documents</c> and <c>keys</c>, so the first leg is the one that sorts first and not
/// the one that was typed first. A surface that wired legs by declaration order rather than by the
/// specification's canonical order would pass every like-legged test here and fail that one.
/// </para>
/// <para>
/// Every junction's payload is read by the factory that runs it — <c>mode</c> decides whether the fan-out
/// broadcasts or balances and whether the fan-in merges or concatenates — so two graphs differing only in a
/// payload really are two graphs, and the run proves it rather than the document merely claiming it.
/// </para>
/// </remarks>
internal static class RegisteredJunctionFixtures
{
    /// <summary>The payload the like-legged fan-out carries when it is to broadcast.</summary>
    internal static readonly CanonicalJsonValue BroadcastParameters = SplitParameters(SplitMode.Broadcast);

    /// <summary>The payload the like-legged fan-out carries when it is to balance.</summary>
    internal static readonly CanonicalJsonValue BalanceParameters = SplitParameters(SplitMode.Balance);

    /// <summary>The payload the unlike-legged fan-out carries.</summary>
    internal static readonly CanonicalJsonValue DivideParameters = CanonicalJsonValue.Parse("{}");

    /// <summary>The payload the like-input fan-in carries when it is to merge.</summary>
    internal static readonly CanonicalJsonValue MergeParameters = JoinParameters(JoinMode.Merge);

    /// <summary>The payload the like-input fan-in carries when it is to concatenate.</summary>
    internal static readonly CanonicalJsonValue ConcatParameters = JoinParameters(JoinMode.Concat);

    /// <summary>The payload the unlike-input fan-in carries.</summary>
    internal static readonly CanonicalJsonValue PairParameters = CanonicalJsonValue.Parse("{}");

    /// <summary>The payload every <c>keys</c> occurrence carries.</summary>
    internal static readonly CanonicalJsonValue KeyParameters = CanonicalJsonValue.Parse("{}");

    /// <summary>The payload that makes the three-legged fan-out claim two legs.</summary>
    internal static readonly CanonicalJsonValue HalvesParameters = SplitParameters(SplitMode.Halves);

    /// <summary>The payload the three-legged fan-out carries.</summary>
    internal static readonly CanonicalJsonValue SpreadParameters = SplitParameters(SplitMode.Broadcast);

    /// <summary>The payload the three-input fan-in carries.</summary>
    internal static readonly CanonicalJsonValue GatherParameters = JoinParameters(JoinMode.Merge);

    /// <summary>Writes the payload of one fan-out occurrence.</summary>
    /// <param name="mode">What the junction is to do with an element.</param>
    /// <returns>The canonical payload.</returns>
    /// <remarks>
    /// The typed parameter builder, at the size the pattern comes in for a one-member payload: the member
    /// name is spelled in <see cref="JunctionModePayload"/> and nowhere else, and an author writing a mode
    /// this vocabulary does not have is stopped by the C# compiler rather than by a validator. What the
    /// validator is still for is a document that was not written through here — one hand-authored, one from
    /// another version, one from another provider entirely.
    /// </remarks>
    internal static CanonicalJsonValue SplitParameters(SplitMode mode) =>
        JunctionModePayload.WriteSplit(mode);

    /// <summary>Writes the payload of one fan-in occurrence.</summary>
    /// <param name="mode">How the junction is to join its inputs.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue JoinParameters(JoinMode mode) => JunctionModePayload.WriteJoin(mode);

    /// <summary>Gets the catalog a deployment would register these stages in.</summary>
    /// <value>The linear fixture stages and the junctions together, which is one provider's vocabulary.</value>
    internal static IStageCatalog Catalog { get; } = Build();

    /// <summary>Gets a catalog that resolves both the local vocabulary and every registered stage.</summary>
    internal static IStageCatalog MixedCatalog { get; } =
        new CompositeStageCatalog(LocalStageCatalog.Instance, Catalog);

    /// <summary>Gets the declaration that <c>order-key@v1</c> is carried by <see cref="OrderKey"/>.</summary>
    internal static ElementContract<OrderKey> OrderKeyContract { get; } =
        ElementContract.For<OrderKey>("order-key", 1);

    /// <summary>Gets the declaration that <c>order-pair@v1</c> is carried by <see cref="OrderPair"/>.</summary>
    internal static ElementContract<OrderPair> OrderPairContract { get; } =
        ElementContract.For<OrderPair>("order-pair", 1);

    /// <summary>Gets the handle of the registered source, resolved through this catalog.</summary>
    internal static RegisteredSource<OrderCreated> OrderSource { get; } =
        RegisteredStage.Source(Catalog, Stage("order-source"), OrderCreatedContract);

    /// <summary>Gets the handle of the registered flow, resolved through this catalog.</summary>
    internal static RegisteredFlow<OrderCreated, OrderDocument> Normalize { get; } =
        RegisteredStage.Flow(Catalog, Stage("normalize"), OrderCreatedContract, OrderDocumentContract);

    /// <summary>Gets the handle of the registered sink that declares a result, resolved through this catalog.</summary>
    internal static RegisteredSinkWithResult<OrderDocument, long> CountSink { get; } =
        RegisteredStage.SinkWithResult(Catalog, Stage("count-sink"), OrderDocumentContract, OrderCountContract);

    /// <summary>Gets the handle of the registered sink that counts keys.</summary>
    internal static RegisteredSinkWithResult<OrderKey, long> KeyCountSink { get; } =
        RegisteredStage.SinkWithResult(Catalog, Stage("key-count-sink"), OrderKeyContract, OrderCountContract);

    /// <summary>Gets the handle of the registered sink that counts pairs.</summary>
    internal static RegisteredSinkWithResult<OrderPair, long> PairCountSink { get; } =
        RegisteredStage.SinkWithResult(Catalog, Stage("pair-count-sink"), OrderPairContract, OrderCountContract);

    /// <summary>Gets the handle of the registered flow that projects a document onto its key.</summary>
    internal static RegisteredFlow<OrderDocument, OrderKey> Keys { get; } =
        RegisteredStage.Flow(Catalog, Stage("keys"), OrderDocumentContract, OrderKeyContract);

    /// <summary>Gets the handle of the registered fan-out whose two legs carry one contract.</summary>
    internal static RegisteredFanOut<OrderDocument, OrderDocument> Split { get; } =
        RegisteredStage.FanOut(Catalog, Stage("split"), OrderDocumentContract, OrderDocumentContract);

    /// <summary>Gets the handle of the registered fan-out whose two legs carry different contracts.</summary>
    /// <value>
    /// The first leg is <c>documents</c> and the second is <c>keys</c>, because that is how the
    /// specification's ports sort — not how they were written.
    /// </value>
    internal static RegisteredFanOut<OrderDocument, OrderDocument, OrderKey> Divide { get; } =
        RegisteredStage.FanOut(
            Catalog,
            Stage("divide"),
            OrderDocumentContract,
            OrderDocumentContract,
            OrderKeyContract);

    /// <summary>Gets the handle of the registered flow whose factory builds a junction instead.</summary>
    /// <value>
    /// A linear stage by its catalog entry, which is what an author sees, and a fan-out by its factory,
    /// which is what a run would get. Only the planner can tell them apart.
    /// </value>
    internal static RegisteredFlow<OrderDocument, OrderDocument> Miscast { get; } =
        RegisteredStage.Flow(Catalog, Stage("enrich-miscast"), OrderDocumentContract, OrderDocumentContract);

    /// <summary>Gets the handle of the registered fan-out with three legs.</summary>
    /// <value>
    /// The one fixture junction whose arity is neither two nor a coincidence: an arity read from the
    /// specification has to be a number, and a surface that assumed two would pass every other test here.
    /// </value>
    internal static RegisteredFanOut<OrderDocument, OrderDocument> Spread { get; } =
        RegisteredStage.FanOut(Catalog, Stage("spread"), OrderDocumentContract, OrderDocumentContract);

    /// <summary>Gets the handle of the registered fan-in with three inputs.</summary>
    internal static RegisteredFanIn<OrderDocument, OrderDocument> Gather { get; } =
        RegisteredStage.FanIn(Catalog, Stage("gather"), OrderDocumentContract, OrderDocumentContract);

    /// <summary>Gets the handle of the registered fan-in whose two inputs carry one contract.</summary>
    internal static RegisteredFanIn<OrderDocument, OrderDocument> Join { get; } =
        RegisteredStage.FanIn(Catalog, Stage("join"), OrderDocumentContract, OrderDocumentContract);

    /// <summary>Gets the handle of the registered fan-in whose two inputs carry different contracts.</summary>
    internal static RegisteredFanIn<OrderDocument, OrderKey, OrderPair> Pair { get; } =
        RegisteredStage.FanIn(
            Catalog,
            Stage("pair"),
            OrderDocumentContract,
            OrderKeyContract,
            OrderPairContract);

    /// <summary>Builds the fully registered branching graph the deployability tests read.</summary>
    /// <param name="left">When this method returns, the slot the first leg's count resolves under.</param>
    /// <param name="right">When this method returns, the slot the second leg's count resolves under.</param>
    /// <param name="parameters">The junction's payload, which decides what it does with an element.</param>
    /// <returns>The closed graph: source, normalize, junction, and a counting sink per leg, all named.</returns>
    /// <remarks>
    /// Every occurrence is registered and every one carries a name the author chose, so the closed document
    /// declares no capability token at all. That is the whole claim of the multi-port half of M4.5, and it
    /// is what the M4.2 sibling test — the one asserting that a local junction costs a graph both tokens —
    /// is now paired with.
    /// </remarks>
    internal static RunnableGraph RegisteredFanOut(
        out ResultSlot<long> left,
        out ResultSlot<long> right,
        CanonicalJsonValue? parameters = null) =>
        Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .Via(Normalize, "normalize", NormalizeParameters)
            .FanOutTo(
                Split,
                "split",
                parameters ?? BroadcastParameters,
                Flow.For<OrderDocument>().To(CountSink, "count-left", CountParameters, "left", out left),
                Flow.For<OrderDocument>().To(CountSink, "count-right", CountParameters, "right", out right));

    /// <summary>Builds the fully registered joining graph the deployability tests read.</summary>
    /// <param name="total">When this method returns, the slot the count resolves under.</param>
    /// <param name="parameters">The junction's payload, which decides how it joins.</param>
    /// <returns>The closed graph: two sources, two flows, the junction, and one counting sink, all named.</returns>
    internal static RunnableGraph RegisteredFanIn(
        out ResultSlot<long> total,
        CanonicalJsonValue? parameters = null) =>
        Source.FromRegistered(OrderSource, "orders-primary", SourceParameters)
            .Via(Normalize, "normalize-primary", NormalizeParameters)
            .FanIn(
                Join,
                "join",
                parameters ?? MergeParameters,
                Source.FromRegistered(OrderSource, "orders-secondary", SourceParameters)
                    .Via(Normalize, "normalize-secondary", NormalizeParameters))
            .To(CountSink, "count-out", CountParameters, "total", out total);

    /// <summary>Builds the fully registered graph whose junction has three legs.</summary>
    /// <param name="a">When this method returns, the first leg's slot.</param>
    /// <param name="b">When this method returns, the second leg's slot.</param>
    /// <param name="c">When this method returns, the third leg's slot.</param>
    /// <returns>The closed graph, all of it registered and all of it named.</returns>
    internal static RunnableGraph RegisteredSpread(
        out ResultSlot<long> a,
        out ResultSlot<long> b,
        out ResultSlot<long> c) =>
        Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .Via(Normalize, "normalize", NormalizeParameters)
            .FanOutTo(
                Spread,
                "spread",
                SpreadParameters,
                Flow.For<OrderDocument>().To(CountSink, "count-a", CountParameters, "a", out a),
                Flow.For<OrderDocument>().To(CountSink, "count-b", CountParameters, "b", out b),
                Flow.For<OrderDocument>().To(CountSink, "count-c", CountParameters, "c", out c));

    /// <summary>Builds the fully registered graph whose junction joins three streams.</summary>
    /// <param name="total">When this method returns, the slot the count resolves under.</param>
    /// <returns>The closed graph, all of it registered and all of it named.</returns>
    internal static RunnableGraph RegisteredGather(out ResultSlot<long> total) =>
        Stream("a")
            .FanIn(Gather, "gather", GatherParameters, Stream("b"), Stream("c"))
            .To(CountSink, "count-out", CountParameters, "total", out total);

    /// <summary>Builds one named registered stream of documents.</summary>
    /// <param name="name">The suffix that makes this stream's occurrence names its own.</param>
    /// <returns>The source.</returns>
    private static Source<OrderDocument> Stream(string name) =>
        Source.FromRegistered(OrderSource, $"orders-{name}", SourceParameters)
            .Via(Normalize, $"normalize-{name}", NormalizeParameters);

    /// <summary>Builds the fully registered graph whose junction splits a row into unlike halves.</summary>
    /// <param name="documents">When this method returns, the slot the document leg's count resolves under.</param>
    /// <param name="keys">When this method returns, the slot the key leg's count resolves under.</param>
    /// <returns>The closed graph, all of it registered and all of it named.</returns>
    internal static RunnableGraph RegisteredUnzip(out ResultSlot<long> documents, out ResultSlot<long> keys) =>
        Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .Via(Normalize, "normalize", NormalizeParameters)
            .FanOutTo(
                Divide,
                "divide",
                DivideParameters,
                Flow.For<OrderDocument>().To(CountSink, "count-documents", CountParameters, "documents", out documents),
                Flow.For<OrderKey>().To(KeyCountSink, "count-keys", CountParameters, "keys", out keys));

    /// <summary>Builds the fully registered graph whose junction pairs two unlike streams.</summary>
    /// <param name="total">When this method returns, the slot the pair count resolves under.</param>
    /// <returns>The closed graph, all of it registered and all of it named.</returns>
    /// <remarks>
    /// The key stream is produced by dividing a document stream and discarding the document leg into a
    /// counting sink, because a fixture source of keys would be a stage this vocabulary does not need. What
    /// the graph proves is the junction, and the shape it takes to get there is incidental.
    /// </remarks>
    internal static RunnableGraph RegisteredZip(out ResultSlot<long> total) =>
        Source.FromRegistered(OrderSource, "orders-first", SourceParameters)
            .Via(Normalize, "normalize-first", NormalizeParameters)
            .FanIn(
                Pair,
                "pair",
                PairParameters,
                Source.FromRegistered(OrderSource, "orders-second", SourceParameters)
                    .Via(Normalize, "normalize-second", NormalizeParameters)
                    .Via(Keys, "keys", KeyParameters))
            .To(PairCountSink, "count-out", CountParameters, "total", out total);

    /// <summary>Builds the catalog of linear fixture stages plus the junctions.</summary>
    /// <returns>The catalog.</returns>
    /// <remarks>
    /// Composed from <see cref="RegisteredFixtures.Catalog"/> rather than restating it, so that the linear
    /// stages a junction graph is built out of are the very stages the linear tests are written against.
    /// </remarks>
    private static StageCatalog Build() =>
        StageCatalog.Create(
        [
            .. RegisteredFixtures.Catalog.Specifications,
            StageSpecification.Create(
                Stage("keys"),
                [Input("in", "order-document")],
                [Output("out", "order-key")],
                [],
                Parameters("keys"),
                []),
            StageSpecification.Create(
                Stage("key-count-sink"),
                [Input("elements", "order-key")],
                [],
                [Result("total", "order-count")],
                Parameters("count-sink"),
                []),
            StageSpecification.Create(
                Stage("pair-count-sink"),
                [Input("elements", "order-pair")],
                [],
                [Result("total", "order-count")],
                Parameters("count-sink"),
                []),
            StageSpecification.Create(
                Stage("split"),
                [Input("in", "order-document")],
                [Output("left", "order-document"), Output("right", "order-document")],
                [],
                Parameters("split"),
                [],
                new JunctionModeValidator(joining: false)),
            StageSpecification.Create(
                Stage("divide"),
                [Input("in", "order-document")],
                [Output("keys", "order-key"), Output("documents", "order-document")],
                [],
                Parameters("divide"),
                []),
            StageSpecification.Create(
                Stage("join"),
                [Input("primary", "order-document"), Input("secondary", "order-document")],
                [Output("out", "order-document")],
                [],
                Parameters("join"),
                [],
                new JunctionModeValidator(joining: true)),
            StageSpecification.Create(
                Stage("pair"),
                [Input("first", "order-document"), Input("second", "order-key")],
                [Output("out", "order-pair")],
                [],
                Parameters("pair"),
                []),
            StageSpecification.Create(
                Stage("enrich-miscast"),
                [Input("in", "order-document")],
                [Output("out", "order-document")],
                [],
                Parameters("enrich"),
                []),
            StageSpecification.Create(
                Stage("spread"),
                [Input("in", "order-document")],
                [
                    Output("leg-a", "order-document"),
                    Output("leg-b", "order-document"),
                    Output("leg-c", "order-document"),
                ],
                [],
                Parameters("split"),
                [],
                new JunctionModeValidator(joining: false)),
            StageSpecification.Create(
                Stage("gather"),
                [
                    Input("src-a", "order-document"),
                    Input("src-b", "order-document"),
                    Input("src-c", "order-document"),
                ],
                [Output("out", "order-document")],
                [],
                Parameters("join"),
                [],
                new JunctionModeValidator(joining: true)),
        ]);

    /// <summary>Builds the parameter contract of one fixture stage.</summary>
    /// <param name="stage">The stage identifier text.</param>
    /// <returns>The contract reference, at major version 1.</returns>
    private static ContractReference Parameters(string stage) =>
        ContractReference.Create(ContractId.Create($"{stage}-parameters"), 1);

    /// <summary>Builds a required input port specification.</summary>
    /// <param name="port">The port name.</param>
    /// <param name="contract">The element contract identifier text.</param>
    /// <returns>The port specification.</returns>
    private static InputPortSpecification Input(string port, string contract) =>
        InputPortSpecification.Create(
            PortId.Create(port),
            ContractReference.Create(ContractId.Create(contract), 1));

    /// <summary>Builds a consumed output port specification.</summary>
    /// <param name="port">The port name.</param>
    /// <param name="contract">The element contract identifier text.</param>
    /// <returns>The port specification.</returns>
    private static OutputPortSpecification Output(string port, string contract) =>
        OutputPortSpecification.Create(
            PortId.Create(port),
            ContractReference.Create(ContractId.Create(contract), 1));

    /// <summary>Builds a result port specification.</summary>
    /// <param name="port">The port name.</param>
    /// <param name="contract">The result contract identifier text.</param>
    /// <returns>The port specification.</returns>
    private static ResultPortSpecification Result(string port, string contract) =>
        ResultPortSpecification.Create(
            PortId.Create(port),
            ContractReference.Create(ContractId.Create(contract), 1));

    /// <summary>The key of an order, as the unlike-legged junctions carry it.</summary>
    /// <param name="OrderId">The order identity.</param>
    internal sealed record class OrderKey(string OrderId);

    /// <summary>A document beside the key of another order, as the pairing junction builds it.</summary>
    /// <param name="Document">The document the first input contributed.</param>
    /// <param name="Key">The key the second input contributed.</param>
    internal sealed record class OrderPair(OrderDocument Document, OrderKey Key);
}
