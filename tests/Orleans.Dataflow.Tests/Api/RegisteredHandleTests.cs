using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;
using static Orleans.Dataflow.Tests.Api.RegisteredFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What a typed registered-stage handle checks before it exists.
/// </summary>
/// <remarks>
/// <para>
/// A handle is where the author's belief about a stage meets the catalog's statement about it. Everything
/// checkable is checked here — the stage is registered, its port multiplicities are the ones the handle
/// kind attaches, and its ports carry the declared contracts — so that a mismatch is an exception at the
/// declaration rather than a diagnostic at the far end of a chain or a document that fails at deployment.
/// </para>
/// <para>
/// The rejections are asserted by their message content rather than only by their type, because the whole
/// value of checking early is that the message says what disagreed with what.
/// </para>
/// </remarks>
public sealed class RegisteredHandleTests
{
    [Fact]
    public void EveryHandleKindCarriesItsSpecificationAndItsDeclaredContracts()
    {
        Assert.Equal(Stage("order-source"), OrderSource.Stage);
        Assert.Same(Specification("order-source"), OrderSource.Specification);
        Assert.Equal(OrderCreatedContract, OrderSource.Output);

        Assert.Equal(Stage("normalize"), Normalize.Stage);
        Assert.Equal(OrderCreatedContract, Normalize.Input);
        Assert.Equal(OrderDocumentContract, Normalize.Output);

        Assert.Equal(Stage("index-sink"), IndexSink.Stage);
        Assert.Equal(OrderDocumentContract, IndexSink.Input);

        Assert.Equal(Stage("count-sink"), CountSink.Stage);
        Assert.Equal(OrderDocumentContract, CountSink.Input);
        Assert.Equal(OrderCountContract, CountSink.Result);
    }

    [Fact]
    public void AHandleCarriesTheSpecificationAndNotTheCatalogItCameFrom()
    {
        // The settled open question, pinned where it can be seen: a handle built from the composite catalog
        // is the same handle as one built from the catalog that actually registers the stage, because a
        // specification is a value and the catalog was only ever the lookup. Nothing about the handle
        // remembers which catalog answered, and nothing about it can therefore claim to know whether some
        // graph validates.
        RegisteredFlow<OrderCreated, OrderDocument> throughComposite =
            RegisteredStage.Flow(MixedCatalog, Stage("normalize"), OrderCreatedContract, OrderDocumentContract);

        Assert.Same(Normalize.Specification, throughComposite.Specification);
        Assert.Equal(Normalize.Stage, throughComposite.Stage);
    }

    [Fact]
    public void AStageTheCatalogDoesNotRegisterIsRejectedNamingIt()
    {
        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => RegisteredStage.Source(Catalog, UnknownStage(), OrderCreatedContract));

        Assert.Contains("does not register the stage", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("orleans-test/no-such-stage@v1", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("breaks 1 invariant", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AStageRegisteredOnlyInAnotherCatalogDoesNotResolve()
    {
        // Resolution is the catalog's answer and not a name match: the local catalog registers 'select',
        // and asking the fixture catalog for it is an unknown stage.
        Assert.Throws<ArgumentException>(
            () => RegisteredStage.Flow(
                Catalog,
                ApiFixtures.LocalStage("select"),
                OrderCreatedContract,
                OrderDocumentContract));
    }

    [Fact]
    public void AFlowUsedAsASourceIsRejectedForItsInputPort()
    {
        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => RegisteredStage.Source(Catalog, Stage("normalize"), OrderDocumentContract));

        Assert.Contains(
            "the stage declares 1 input port, and a registered source attaches a stage with exactly 0",
            rejected.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void ASourceUsedAsAFlowIsRejectedForItsMissingInputPort()
    {
        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => RegisteredStage.Flow(
                Catalog,
                Stage("order-source"),
                OrderCreatedContract,
                OrderCreatedContract));

        Assert.Contains(
            "the stage declares 0 input ports, and a registered flow attaches a stage with exactly 1",
            rejected.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void ASinkUsedAsAFlowIsRejectedForItsMissingOutputPort()
    {
        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => RegisteredStage.Flow(
                Catalog,
                Stage("index-sink"),
                OrderDocumentContract,
                OrderDocumentContract));

        Assert.Contains(
            "the stage declares 0 output ports, and a registered flow attaches a stage with exactly 1",
            rejected.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void AResultBearingSinkUsedAsAPlainSinkIsRejectedForItsResultPort()
    {
        // The registered mirror of the rule that a SinkWithResult is not a Sink: a result nothing names is
        // a result nothing can read, and the handle refuses to pretend the port is not there.
        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => RegisteredStage.Sink(Catalog, Stage("count-sink"), OrderDocumentContract));

        Assert.Contains(
            "the stage declares 1 result port, and a registered sink attaches a stage with exactly 0",
            rejected.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void APlainSinkUsedAsAResultBearingOneIsRejectedForItsMissingResultPort()
    {
        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => RegisteredStage.SinkWithResult(
                Catalog,
                Stage("index-sink"),
                OrderDocumentContract,
                OrderCountContract));

        Assert.Contains(
            "the stage declares 0 result ports, and a registered sink with a result attaches a stage with exactly 1",
            rejected.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void AnInputContractTheStageDoesNotDeclareIsRejectedNamingBothContracts()
    {
        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => RegisteredStage.Flow(
                Catalog,
                Stage("normalize"),
                OrderDocumentContract,
                OrderDocumentContract));

        Assert.Contains(
            "the port 'in' accepts elements of contract 'order-created@v1', and the handle declares 'order-document@v1'",
            rejected.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void AnOutputContractTheStageDoesNotDeclareIsRejectedNamingBothContracts()
    {
        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => RegisteredStage.Source(Catalog, Stage("order-source"), OrderDocumentContract));

        Assert.Contains(
            "the port 'events' produces elements of contract 'order-created@v1', and the handle declares 'order-document@v1'",
            rejected.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void AResultContractTheStageDoesNotDeclareIsRejectedNamingBothContracts()
    {
        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => RegisteredStage.SinkWithResult(
                Catalog,
                Stage("count-sink"),
                OrderDocumentContract,
                ResultContract.For<long>("order-total", 1)));

        Assert.Contains(
            "the port 'total' yields a result of contract 'order-count@v1', and the handle declares 'order-total@v1'",
            rejected.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void AMajorVersionTheCatalogDoesNotRegisterIsAnUnknownStage()
    {
        // Resolution is exact: the same provider and stage at another major version is another reference,
        // because the two are allowed to declare different ports and different parameter contracts.
        Assert.Contains(
            "does not register the stage 'orleans-test/normalize@v2'",
            Assert.Throws<ArgumentException>(
                () => RegisteredStage.Flow(
                    Catalog,
                    Stage("normalize", 2),
                    OrderCreatedContract,
                    OrderDocumentContract)).Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void EveryViolationOfOneHandleIsReportedInOneException()
    {
        // Two things are wrong at once: 'normalize' consumes, which a source does not, and it produces
        // order documents, which is not what this handle claims. One call, one exception, both numbered.
        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => RegisteredStage.Source(Catalog, Stage("normalize"), OrderCreatedContract));

        Assert.Contains("breaks 2 invariants", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("1. the stage declares 1 input port", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("2. the port 'out' produces elements of", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void APortCountThatIsAlreadyWrongContributesNoContractComplaint()
    {
        // The gating rule the rest of this codebase follows: a contract is compared only against a port
        // that exists, so the report carries what is wrong rather than a follow-on that disappears on its
        // own once the reported one is fixed. 'index-sink' has no output port, so the output contract this
        // flow handle declares has nothing to disagree with.
        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => RegisteredStage.Flow(
                Catalog,
                Stage("index-sink"),
                OrderDocumentContract,
                OrderCountContractAsElement()));

        Assert.Contains("breaks 1 invariant", rejected.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("produces elements of", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ANullCatalogIsRejected()
    {
        Assert.Equal(
            "catalog",
            Assert.Throws<ArgumentNullException>(
                () => RegisteredStage.Source(null!, Stage("order-source"), OrderCreatedContract)).ParamName);
        Assert.Equal(
            "catalog",
            Assert.Throws<ArgumentNullException>(
                () => RegisteredStage.Flow(
                    null!,
                    Stage("normalize"),
                    OrderCreatedContract,
                    OrderDocumentContract)).ParamName);
        Assert.Equal(
            "catalog",
            Assert.Throws<ArgumentNullException>(
                () => RegisteredStage.Sink(null!, Stage("index-sink"), OrderDocumentContract)).ParamName);
        Assert.Equal(
            "catalog",
            Assert.Throws<ArgumentNullException>(
                () => RegisteredStage.SinkWithResult(
                    null!,
                    Stage("count-sink"),
                    OrderDocumentContract,
                    OrderCountContract)).ParamName);
    }

    [Fact]
    public void ADefaultStageReferenceIsRejectedUnderItsOwnParameter()
    {
        Assert.Equal(
            "stage",
            Assert.Throws<ArgumentException>(
                () => RegisteredStage.Source(Catalog, default, OrderCreatedContract)).ParamName);
        Assert.Equal(
            "stage",
            Assert.Throws<ArgumentException>(
                () => RegisteredStage.Sink(Catalog, default, OrderDocumentContract)).ParamName);
    }

    [Fact]
    public void ADefaultContractDeclarationIsRejectedUnderItsOwnParameter()
    {
        Assert.Equal(
            "output",
            Assert.Throws<ArgumentException>(
                () => RegisteredStage.Source<OrderCreated>(Catalog, Stage("order-source"), default)).ParamName);
        Assert.Equal(
            "input",
            Assert.Throws<ArgumentException>(
                () => RegisteredStage.Flow<OrderCreated, OrderDocument>(
                    Catalog,
                    Stage("normalize"),
                    default,
                    OrderDocumentContract)).ParamName);
        Assert.Equal(
            "output",
            Assert.Throws<ArgumentException>(
                () => RegisteredStage.Flow<OrderCreated, OrderDocument>(
                    Catalog,
                    Stage("normalize"),
                    OrderCreatedContract,
                    default)).ParamName);
        Assert.Equal(
            "input",
            Assert.Throws<ArgumentException>(
                () => RegisteredStage.Sink<OrderDocument>(Catalog, Stage("index-sink"), default)).ParamName);
        Assert.Equal(
            "result",
            Assert.Throws<ArgumentException>(
                () => RegisteredStage.SinkWithResult<OrderDocument, long>(
                    Catalog,
                    Stage("count-sink"),
                    OrderDocumentContract,
                    default)).ParamName);
    }

    [Fact]
    public void ACatalogThatResolvesAndThenSuppliesNothingNamesItsOwnDefect()
    {
        // The catalog is a public seam a federated implementation will fill later, possibly from an
        // assembly under no nullable-reference obligation. A broken seam is not an unregistered stage and
        // must not be reported as one: it is a defect in the registration, and no edit to these arguments
        // would fix it.
        InvalidOperationException rejected = Assert.Throws<InvalidOperationException>(
            () => RegisteredStage.Source(
                new Compilation.ContractBreakingCatalog(),
                Stage("order-source"),
                OrderCreatedContract));

        Assert.Contains("supplied no specification", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("orleans-test/order-source@v1", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AHandleIsAnImmutableValueThatEveryAttachmentReads()
    {
        // A handle has no position and no name, so attaching it changes nothing about it. Two graphs built
        // from one handle are independent documents, and the handle is the value it was before either.
        RunnableGraph first = Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .To(SinkOverCreated(), "index-out", IndexParameters);

        RunnableGraph second = Source.FromRegistered(OrderSource, "inbound", SourceParameters)
            .To(SinkOverCreated(), "index-out", IndexParameters);

        Assert.NotEqual(first.Fingerprint, second.Fingerprint);
        Assert.Equal(Stage("order-source"), OrderSource.Stage);
        Assert.Equal(OrderCreatedContract, OrderSource.Output);
    }

    [Fact]
    public void OneHandleAttachedTwiceUnderTwoNamesIsTwoOccurrencesOfOneStage()
    {
        RunnableGraph graph = Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .Via(Normalize, "normalize", NormalizeParameters)
            .Via(Enrich, "enrich-a", CanonicalJson("{}"))
            .Via(Enrich, "enrich-b", CanonicalJson("{}"))
            .To(IndexSink, "index-out", IndexParameters);

        Assert.Equal(["enrich-a", "enrich-b", "index-out", "normalize", "orders-in"], NodeIds(graph.Document));
        Assert.Equal(
            2,
            graph.Document.Nodes.Count(node => node.Stage == Stage("enrich")));
    }

    [Fact]
    public void AHandleRendersItsStageAndItsContracts()
    {
        Assert.Equal(
            "registered source orleans-test/order-source@v1 -> order-created@v1 as OrderCreated",
            OrderSource.ToString());
        Assert.Equal(
            "registered flow orleans-test/normalize@v1: order-created@v1 as OrderCreated -> order-document@v1 as OrderDocument",
            Normalize.ToString());
        Assert.Equal(
            "registered sink orleans-test/index-sink@v1 <- order-document@v1 as OrderDocument",
            IndexSink.ToString());
        Assert.Equal(
            "registered sink with result orleans-test/count-sink@v1 <- order-document@v1 as OrderDocument => order-count@v1 as Int64",
            CountSink.ToString());
    }

    /// <summary>Reads one fixture specification out of the catalog that registers it.</summary>
    /// <param name="stage">The stage identifier text.</param>
    /// <returns>The registered specification.</returns>
    private static StageSpecification Specification(string stage)
    {
        Assert.True(Catalog.TryGetSpecification(Stage(stage), out StageSpecification? specification));

        return specification!;
    }

    /// <summary>Declares the counting sink's result contract as an element contract instead.</summary>
    /// <returns>The declaration, which no fixture port carries.</returns>
    /// <remarks>
    /// A contract no port of the stage under test declares, so that a contract comparison would certainly
    /// fail if one were made at all.
    /// </remarks>
    private static ElementContract<long> OrderCountContractAsElement() =>
        ElementContract.For<long>("order-count", 1);

    /// <summary>Declares the index sink over the element type the fixture source produces.</summary>
    /// <returns>The handle.</returns>
    /// <remarks>
    /// The stage declares <c>order-document@v1</c>, and this binds that contract to the other CLR type, so
    /// that a two-occurrence chain straight from the source can be closed.
    /// </remarks>
    private static RegisteredSink<OrderCreated> SinkOverCreated() =>
        RegisteredStage.Sink(Catalog, Stage("index-sink"), ElementContract.For<OrderCreated>("order-document", 1));

    /// <summary>Parses a canonical payload.</summary>
    /// <param name="json">The JSON text.</param>
    /// <returns>The canonical value.</returns>
    private static CanonicalJsonValue CanonicalJson(string json) => CanonicalJsonValue.Parse(json);
}
