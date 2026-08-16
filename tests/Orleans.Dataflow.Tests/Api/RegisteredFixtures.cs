using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// A catalog of registered stages, and the typed handles over it, that the registered-authoring tests are
/// written against.
/// </summary>
/// <remarks>
/// <para>
/// This is what a provider SDK would register at startup: five stages under one provider, with real
/// element and result contracts, real parameter contracts, one payload validator, and one stage that
/// requires a capability of its host. Nothing here is production code, and the local vocabulary is
/// deliberately untouched: the registered surface is not an extension of it.
/// </para>
/// <para>
/// The port names are chosen to disagree with the local vocabulary's <c>in</c>, <c>out</c>, and
/// <c>result</c> wherever a test can see the difference — the source produces on <c>events</c> and the
/// counting sink consumes on <c>elements</c> and yields on <c>total</c>. A builder that hard-coded the
/// local names would close a document whose edges name ports no stage declares, and the graph compiler
/// would say so.
/// </para>
/// <para>
/// The contract identifiers are spelled here rather than derived from the handles, so that an assertion
/// about a closed document is a statement about the catalog a deployment registered and not an echo of
/// whatever the authoring code happened to put there.
/// </para>
/// </remarks>
internal static class RegisteredFixtures
{
    /// <summary>The provider every fixture stage belongs to.</summary>
    internal const string Provider = "orleans-test";

    /// <summary>The payload the <c>order-source</c> stage's validator accepts.</summary>
    internal static readonly CanonicalJsonValue SourceParameters =
        CanonicalJsonValue.Parse("""{"topic":"orders"}""");

    /// <summary>A payload the <c>order-source</c> stage's validator rejects.</summary>
    internal static readonly CanonicalJsonValue BlankSourceParameters =
        CanonicalJsonValue.Parse("""{"topic":""}""");

    /// <summary>The payload every <c>normalize</c> occurrence carries.</summary>
    internal static readonly CanonicalJsonValue NormalizeParameters =
        CanonicalJsonValue.Parse("""{"culture":"invariant"}""");

    /// <summary>The payload every <c>index-sink</c> occurrence carries.</summary>
    internal static readonly CanonicalJsonValue IndexParameters =
        CanonicalJsonValue.Parse("""{"index":"orders","refresh":false}""");

    /// <summary>The payload every <c>count-sink</c> occurrence carries.</summary>
    internal static readonly CanonicalJsonValue CountParameters = CanonicalJsonValue.Parse("{}");

    /// <summary>The payload every <c>durable-sink</c> occurrence carries.</summary>
    internal static readonly CanonicalJsonValue DurableParameters =
        CanonicalJsonValue.Parse("""{"stateName":"orders"}""");

    /// <summary>Gets the catalog a deployment would register these stages in.</summary>
    internal static IStageCatalog Catalog { get; } = Build();

    /// <summary>Gets a catalog that resolves both the local vocabulary and the registered stages.</summary>
    /// <value>
    /// What a mixed graph has to be validated against: its lambda nodes resolve in one catalog and its
    /// registered nodes in the other, and neither catalog alone can answer for the whole document.
    /// </value>
    internal static IStageCatalog MixedCatalog { get; } =
        new CompositeStageCatalog(LocalStageCatalog.Instance, Catalog);

    /// <summary>Gets the declaration that <c>order-created@v1</c> is carried by <see cref="OrderCreated"/>.</summary>
    internal static ElementContract<OrderCreated> OrderCreatedContract { get; } =
        ElementContract.For<OrderCreated>("order-created", 1);

    /// <summary>Gets the declaration that <c>order-document@v1</c> is carried by <see cref="OrderDocument"/>.</summary>
    internal static ElementContract<OrderDocument> OrderDocumentContract { get; } =
        ElementContract.For<OrderDocument>("order-document", 1);

    /// <summary>Gets the declaration that <c>order-count@v1</c> is carried by a 64-bit integer.</summary>
    internal static ResultContract<long> OrderCountContract { get; } =
        ResultContract.For<long>("order-count", 1);

    /// <summary>Gets the handle of the registered source.</summary>
    internal static RegisteredSource<OrderCreated> OrderSource { get; } =
        RegisteredStage.Source(Catalog, Stage("order-source"), OrderCreatedContract);

    /// <summary>Gets the handle of the registered flow.</summary>
    internal static RegisteredFlow<OrderCreated, OrderDocument> Normalize { get; } =
        RegisteredStage.Flow(Catalog, Stage("normalize"), OrderCreatedContract, OrderDocumentContract);

    /// <summary>Gets the handle of the registered flow whose two ports carry one contract.</summary>
    /// <value>
    /// The one fixture stage a reusable flow can hold and be composed with itself, which is how the tests
    /// reach the case where one explicit name would be contributed twice.
    /// </value>
    internal static RegisteredFlow<OrderDocument, OrderDocument> Enrich { get; } =
        RegisteredStage.Flow(Catalog, Stage("enrich"), OrderDocumentContract, OrderDocumentContract);

    /// <summary>Gets the handle of the registered sink that declares no result.</summary>
    internal static RegisteredSink<OrderDocument> IndexSink { get; } =
        RegisteredStage.Sink(Catalog, Stage("index-sink"), OrderDocumentContract);

    /// <summary>Gets the handle of the registered sink that declares a result.</summary>
    internal static RegisteredSinkWithResult<OrderDocument, long> CountSink { get; } =
        RegisteredStage.SinkWithResult(Catalog, Stage("count-sink"), OrderDocumentContract, OrderCountContract);

    /// <summary>Gets the handle of the registered sink that requires a capability of its host.</summary>
    internal static RegisteredSink<OrderDocument> DurableSink { get; } =
        RegisteredStage.Sink(Catalog, Stage("durable-sink"), OrderDocumentContract);

    /// <summary>Builds a fixture stage reference.</summary>
    /// <param name="stage">The stage identifier text.</param>
    /// <returns>The reference under <see cref="Provider"/> at major version 1.</returns>
    internal static StageRef Stage(string stage) => Stage(stage, 1);

    /// <summary>Builds a fixture stage reference at a chosen major version.</summary>
    /// <param name="stage">The stage identifier text.</param>
    /// <param name="majorVersion">The compatibility major version.</param>
    /// <returns>The reference under <see cref="Provider"/>.</returns>
    /// <remarks>
    /// Only version 1 of anything is registered, so any other version is a reference the catalog does not
    /// resolve — which is the point of being able to spell one.
    /// </remarks>
    internal static StageRef Stage(string stage, int majorVersion) =>
        StageRef.Create(ProviderId.Create(Provider), StageId.Create(stage), majorVersion);

    /// <summary>Builds a reference to a stage no fixture catalog registers.</summary>
    /// <returns>The reference.</returns>
    internal static StageRef UnknownStage() => Stage("no-such-stage");

    /// <summary>Builds the fully registered chain the document tests read.</summary>
    /// <returns>The closed graph: source, normalize, index sink, all named.</returns>
    internal static RunnableGraph Indexed() =>
        Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .Via(Normalize, "normalize", NormalizeParameters)
            .To(IndexSink, "index-out", IndexParameters);

    /// <summary>Builds the fully registered chain that declares a result.</summary>
    /// <param name="slot">When this method returns, the slot that resolves the count.</param>
    /// <returns>The closed graph: source, normalize, counting sink, all named.</returns>
    internal static RunnableGraph Counted(out ResultSlot<long> slot) =>
        Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .Via(Normalize, "normalize", NormalizeParameters)
            .To(CountSink, "count-out", CountParameters, "processed", out slot);

    /// <summary>Builds the five specifications of the fixture catalog.</summary>
    /// <returns>The catalog.</returns>
    /// <remarks>
    /// Every stage carries a parameter contract of its own, so a payload written for one stage is a
    /// <c>parameter-contract-mismatch</c> on another rather than a payload that happens to pass.
    /// </remarks>
    private static StageCatalog Build() =>
        StageCatalog.Create(
        [
            StageSpecification.Create(
                Stage("order-source"),
                [],
                [Output("events", "order-created")],
                [],
                Parameters("order-source"),
                [],
                new TopicValidator()),
            StageSpecification.Create(
                Stage("normalize"),
                [Input("in", "order-created")],
                [Output("out", "order-document")],
                [],
                Parameters("normalize"),
                []),
            StageSpecification.Create(
                Stage("enrich"),
                [Input("in", "order-document")],
                [Output("out", "order-document")],
                [],
                Parameters("enrich"),
                []),
            StageSpecification.Create(
                Stage("index-sink"),
                [Input("in", "order-document")],
                [],
                [],
                Parameters("index-sink"),
                []),
            StageSpecification.Create(
                Stage("count-sink"),
                [Input("elements", "order-document")],
                [],
                [Result("total", "order-count")],
                Parameters("count-sink"),
                []),
            StageSpecification.Create(
                Stage("durable-sink"),
                [Input("in", "order-document")],
                [],
                [],
                Parameters("durable-sink"),
                [CapabilityToken.Create("durable-state")]),
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

    /// <summary>The parameter check the <c>order-source</c> stage registers.</summary>
    /// <remarks>
    /// One member, one rule, and no tolerance for anything else: a payload this stage did not write is not
    /// a payload it will run. The check exists so that the tests can prove the graph compiler runs a
    /// registered stage's validator over a node an author actually wrote.
    /// </remarks>
    private sealed class TopicValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters)
        {
            if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
            {
                return ["the payload is not a JSON object"];
            }

            JsonElement payload = parameters.ToElement();
            List<string> violations = [];

            if (!payload.TryGetProperty("topic", out JsonElement topic))
            {
                violations.Add("the member 'topic' is missing");
            }
            else if (topic.ValueKind is not JsonValueKind.String)
            {
                violations.Add("the member 'topic' is not a string");
            }
            else if (topic.GetString()!.Length == 0)
            {
                violations.Add("the member 'topic' is empty, and a topic names a stream");
            }

            foreach (JsonProperty member in payload.EnumerateObject())
            {
                if (!string.Equals(member.Name, "topic", StringComparison.Ordinal))
                {
                    violations.Add($"the member '{member.Name}' is not one this stage declares");
                }
            }

            return violations;
        }
    }
}
