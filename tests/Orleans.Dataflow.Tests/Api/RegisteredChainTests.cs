using System.Text;
using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;
using static Orleans.Dataflow.Tests.Api.RegisteredFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What a chain of registered stages writes into its document, and what the definition plane makes of it.
/// </summary>
/// <remarks>
/// This is the deployable half of <see cref="DocumentShapeTests"/>. Every claim there about a lambda graph
/// has a counterpart here, and the differences are the whole point: the node identifiers are the author's
/// names, the payloads are real, the ports are the ones the catalog declares, and the document carries no
/// capability token at all.
/// </remarks>
public sealed class RegisteredChainTests
{
    [Fact]
    public void EveryOccurrenceIsDeclaredUnderTheNameItsAuthorGaveIt()
    {
        GraphDocument document = Indexed().Document;

        // Canonical node order is ordinal over identifier text, which for these names is not the authoring
        // order — and that is the price of names that survive an edit rather than a defect.
        Assert.Equal(["index-out", "normalize", "orders-in"], NodeIds(document));
        Assert.DoesNotContain(NodeIds(document), id => id.StartsWith("stage-", StringComparison.Ordinal));
    }

    [Fact]
    public void EveryNodeNamesTheStageItsHandleResolved()
    {
        GraphDocument document = Indexed().Document;

        Assert.Equal(
            [Stage("index-sink"), Stage("normalize"), Stage("order-source")],
            document.Nodes.Select(node => node.Stage));
    }

    [Fact]
    public void TheEdgesNameThePortsTheCatalogDeclaresRatherThanTheLocalOnes()
    {
        // The fixture source produces on 'events' and nothing in this chain is called 'out' except the
        // flow's own output port. A builder that assumed the local vocabulary's port names would close a
        // document whose edges name ports no stage declares.
        Assert.Equal(
            ["normalize#out -> index-out#in", "orders-in#events -> normalize#in"],
            Edges(Indexed().Document));
    }

    [Fact]
    public void EveryNodeCarriesItsStagesParameterContractAndTheAuthorsPayloadBytes()
    {
        GraphDocument document = Indexed().Document;

        AssertPayload(document, "orders-in", "order-source-parameters", """{"topic":"orders"}""");
        AssertPayload(document, "normalize", "normalize-parameters", """{"culture":"invariant"}""");

        // Canonical form sorts an object's members, so the bytes stored are not the bytes written: the
        // author wrote 'index' before 'refresh' and canonical order happens to agree, but the assertion is
        // on the canonical text either way.
        AssertPayload(document, "index-out", "index-sink-parameters", """{"index":"orders","refresh":false}""");
    }

    [Fact]
    public void ANodeOfARegisteredStageDeclaresNoExecutionPolicy()
    {
        foreach (StageNode node in Indexed().Document.Nodes)
        {
            Assert.Null(node.ExecutionPolicyContract);
            Assert.Null(node.ExecutionPolicy);
        }
    }

    [Fact]
    public void AFullyRegisteredAndFullyNamedDocumentDeclaresNoCapabilityAtAll()
    {
        Assert.Empty(Indexed().Document.Capabilities);
        Assert.Empty(Counted(out ResultSlot<long> _).Document.Capabilities);
    }

    [Fact]
    public void AFullyRegisteredDocumentValidatesAgainstTheCatalogItWasAuthoredFrom()
    {
        GraphValidationReport report = GraphCompiler.Validate(Indexed().Document, Catalog);

        Assert.True(report.IsValid, report.ToString());
    }

    [Fact]
    public void AFullyRegisteredDocumentDoesNotValidateAgainstTheLocalCatalog()
    {
        // The other half of "it validates": against a catalog that registers none of these stages, every
        // node is an unknown stage. Without this, validity would be a claim about a lenient compiler.
        GraphValidationReport report = GraphCompiler.Validate(Indexed().Document, LocalStageCatalog.Instance);

        Assert.False(report.IsValid);
        Assert.All(report.Diagnostics, diagnostic => Assert.Equal("unknown-stage", diagnostic.Rule));
        Assert.Equal(3, report.Diagnostics.Count);
    }

    [Fact]
    public void APayloadTheStagesValidatorRejectsIsAnInvalidParametersDiagnostic()
    {
        RunnableGraph graph = Source.FromRegistered(OrderSource, "orders-in", BlankSourceParameters)
            .Via(Normalize, "normalize", NormalizeParameters)
            .To(IndexSink, "index-out", IndexParameters);

        GraphValidationReport report = GraphCompiler.Validate(graph.Document, Catalog);

        GraphValidationDiagnostic diagnostic = Assert.Single(report.Diagnostics);

        Assert.Equal("invalid-parameters", diagnostic.Rule);
        Assert.Equal("orders-in", diagnostic.Subject);
        Assert.Contains("the member 'topic' is empty", diagnostic.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void APayloadWrittenForAnotherStageIsAContractMismatchRatherThanASilentPass()
    {
        // Every fixture stage declares a parameter contract of its own, and a node declares the contract of
        // the stage its handle resolved. The mistake this pins is not reachable by writing the wrong bytes:
        // it needs a hand-built document, which is what proves the contract is really stored per node.
        GraphDocument document = GraphDocument.Create(
            GraphId.Create("hand-built"),
            GraphRevision.Create(1),
            [],
            [
                StageNode.Create(
                    NodeId.Create("orders-in"),
                    Stage("order-source"),
                    ContractReference.Create(ContractId.Create("normalize-parameters"), 1),
                    SourceParameters),
            ],
            [],
            []);

        GraphValidationReport report = GraphCompiler.Validate(document, Catalog);

        Assert.Contains(
            report.Diagnostics,
            diagnostic => diagnostic.Rule == "parameter-contract-mismatch");
    }

    [Fact]
    public void TheResultBearingSinkDeclaresItsSlotOnTheStagesOwnResultPort()
    {
        GraphDocument document = Counted(out ResultSlot<long> processed).Document;

        ResultSlotDefinition slot = Assert.Single(document.ResultSlots);

        Assert.Equal("processed", slot.Id.Value);
        Assert.Equal("count-out", slot.Producer.Node.Value);
        Assert.Equal("total", slot.Producer.Port.Value);
        Assert.Equal(ContractReference.Create(ContractId.Create("order-count"), 1), slot.ResultContract);
        Assert.Equal("processed", processed.Id.Value);
    }

    [Fact]
    public void TheOccurrenceNameAndTheSlotNameAreTwoNamespacesAndMayShareOneText()
    {
        // Both names are required and neither is derivable from the other, but they name different things
        // in different namespaces: one is a node of the graph and one is a result of it. Using one text for
        // both is therefore legal and unambiguous, which is worth pinning so that nobody later "fixes" it
        // into a uniqueness rule that has no reason to exist.
        RunnableGraph graph = Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .Via(Normalize, "normalize", NormalizeParameters)
            .To(CountSink, "processed", CountParameters, "processed", out ResultSlot<long> slot);

        ResultSlotDefinition declared = Assert.Single(graph.Document.ResultSlots);

        Assert.Equal("processed", declared.Id.Value);
        Assert.Equal("processed", declared.Producer.Node.Value);
        Assert.Equal("total", declared.Producer.Port.Value);
        Assert.Equal("processed", slot.Id.Value);
        Assert.True(GraphCompiler.Validate(graph.Document, Catalog).IsValid);
    }

    [Fact]
    public void TheSlotOfARegisteredGraphStillBindsToItsFingerprintAndItsInstance()
    {
        // A RunnableGraph is a RunnableGraph whatever its stages are: the slot binds by fingerprint and by
        // the built instance's nonce, exactly as a lambda graph's does. Binding by fingerprint and lineage
        // without a nonce is a property of a PipelineDefinition, which is a different value.
        RunnableGraph first = Counted(out ResultSlot<long> firstSlot);
        RunnableGraph second = Counted(out ResultSlot<long> secondSlot);

        Assert.Equal(first.Fingerprint, firstSlot.Graph);
        Assert.Equal(first.Fingerprint, second.Fingerprint);
        Assert.NotEqual(firstSlot, secondSlot);
        Assert.NotEqual(firstSlot.AuthoringNonce, secondSlot.AuthoringNonce);
    }

    [Fact]
    public void TheTupleFormAndTheOutFormCloseTheSameDocument()
    {
        (RunnableGraph Graph, ResultSlot<long> Slot) tuple =
            Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
                .Via(Normalize, "normalize", NormalizeParameters)
                .To(CountSink, "count-out", CountParameters, "processed");

        RunnableGraph fluent = Counted(out ResultSlot<long> fluentSlot);

        Assert.Equal(fluent.Document, tuple.Graph.Document);
        Assert.Equal(fluent.Fingerprint, tuple.Graph.Fingerprint);
        Assert.Equal(fluentSlot.Id, tuple.Slot.Id);
    }

    [Fact]
    public void ASinkThatRequiresACapabilityMakesTheDocumentDeclareIt()
    {
        // A registered stage may require something of its host, and the graph compiler rejects a document
        // that declares less than its stages require. The builder therefore declares the union of what its
        // occurrences require, and 'durable-state' is neither of the two local tokens.
        RunnableGraph graph = Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .Via(Normalize, "normalize", NormalizeParameters)
            .To(DurableSink, "state-out", DurableParameters);

        Assert.Equal(["durable-state"], Capabilities(graph.Document));
        Assert.True(GraphCompiler.Validate(graph.Document, Catalog).IsValid);
    }

    [Fact]
    public void ARegisteredDocumentRoundTripsThroughItsCanonicalBytes()
    {
        RunnableGraph graph = Counted(out ResultSlot<long> _);

        byte[] bytes = GraphDocumentSerializer.Serialize(graph.Document);
        GraphDocument restored = GraphDocumentSerializer.Deserialize(bytes);

        Assert.Equal(graph.Document, restored);
        Assert.Equal(bytes, GraphDocumentSerializer.Serialize(restored));
        Assert.Equal(graph.Fingerprint, GraphDocumentSerializer.Fingerprint(restored));
    }

    [Fact]
    public void TheSameChainAuthoredThroughAReusableFlowClosesTheSameBytes()
    {
        // Permuted authoring, in the only sense this surface admits one: the same three occurrences reached
        // by composing a reusable flow instead of chaining in place. The document is content, so the two
        // spellings are byte-identical.
        RunnableGraph direct = Indexed();

        RunnableGraph composed = Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .Via(Flow.For<OrderCreated>().Via(Normalize, "normalize", NormalizeParameters))
            .To(IndexSink, "index-out", IndexParameters);

        Assert.Equal(direct.Document, composed.Document);
        Assert.Equal(
            GraphDocumentSerializer.Serialize(direct.Document),
            GraphDocumentSerializer.Serialize(composed.Document));
        Assert.Equal(direct.Fingerprint, composed.Fingerprint);
    }

    [Fact]
    public void ChangingOnlyAPayloadChangesTheFingerprint()
    {
        // The registered counterpart of the lambda surface's blind spot: a registered stage's configuration
        // is in the document, so two graphs that differ only in it are two different graphs. Two lambda
        // graphs whose delegates differ are not.
        RunnableGraph first = Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .To(IndexSinkOverCreated(), "index-out", IndexParameters);

        RunnableGraph second = Source.FromRegistered(
                OrderSource,
                "orders-in",
                CanonicalJsonValue.Parse("""{"topic":"other"}"""))
            .To(IndexSinkOverCreated(), "index-out", IndexParameters);

        Assert.NotEqual(first.Fingerprint, second.Fingerprint);
    }

    [Fact]
    public void RenamingAnOccurrenceChangesTheFingerprint()
    {
        RunnableGraph named = Indexed();

        RunnableGraph renamed = Source.FromRegistered(OrderSource, "orders-inbound", SourceParameters)
            .Via(Normalize, "normalize", NormalizeParameters)
            .To(IndexSink, "index-out", IndexParameters);

        Assert.NotEqual(named.Fingerprint, renamed.Fingerprint);
    }

    [Fact]
    public void TheClosedDocumentEqualsOneWrittenOutByHandFromTheCatalogsOwnTexts()
    {
        // Ground truth re-derived rather than echoed. Every other assertion in this file reads one property
        // of the document the authoring surface produced; this one writes the whole document down from the
        // identifiers the catalog declares and requires the two to be equal, element for element. A
        // builder that dropped a node, wired an edge to the wrong port, or invented a capability would pass
        // the property assertions and fail here.
        GraphDocument built = GraphDocument.Create(
            GraphId.Create("anonymous"),
            GraphRevision.Create(1),
            [],
            [
                StageNode.Create(
                    NodeId.Create("orders-in"),
                    Stage("order-source"),
                    Contract("order-source-parameters"),
                    SourceParameters),
                StageNode.Create(
                    NodeId.Create("normalize"),
                    Stage("normalize"),
                    Contract("normalize-parameters"),
                    NormalizeParameters),
                StageNode.Create(
                    NodeId.Create("index-out"),
                    Stage("index-sink"),
                    Contract("index-sink-parameters"),
                    IndexParameters),
            ],
            [
                GraphEdge.Create(
                    PortAddress.Create(NodeId.Create("orders-in"), PortId.Create("events")),
                    PortAddress.Create(NodeId.Create("normalize"), PortId.Create("in"))),
                GraphEdge.Create(
                    PortAddress.Create(NodeId.Create("normalize"), PortId.Create("out")),
                    PortAddress.Create(NodeId.Create("index-out"), PortId.Create("in"))),
            ],
            []);

        Assert.Equal(built, Indexed().Document);
        Assert.Equal(GraphDocumentSerializer.Fingerprint(built), Indexed().Fingerprint);
    }

    [Fact]
    public void TheRepresentativeRegisteredGraphHasThePinnedFingerprint()
    {
        // The absolute pin for the registered surface, for the reason its lambda counterpart has one: a
        // change of encoding rather than of shape would move every authored fingerprint at once and leave
        // every relative assertion passing. A pipeline's slots bind by fingerprint and lineage, so this
        // value is exactly what a durable deployment would have written down.
        Assert.Equal(
            "sha256:18110b496f4cae5efe5f4facf567643f287e82e69146f1e09b21780d3b79352b",
            Indexed().Fingerprint.ToString());
    }

    [Fact]
    public void AnExplicitNameThatCollidesWithTheAutomaticNumberingIsRejected()
    {
        // The automatic numbering has no reserved namespace, and this is what that costs: an author who
        // names an occurrence 'stage-0002' can collide with the number a lambda occurrence would be given
        // at that very position. The collision is reported by the same rule that reports two explicit
        // names colliding, which is the honest outcome — there is one node identifier space and both kinds
        // of name live in it.
        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => Source.FromRegistered(OrderSource, "stage-0002", SourceParameters)
                .Select(OrderDocument.FromEvent)
                .To(IndexSink, "index-out", IndexParameters));

        Assert.Contains("disjoint node ids", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("'stage-0002'", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnExplicitNameInTheAutomaticShapeIsFineWhenNothingCollides()
    {
        // And the other half of the same fact: the numbering is not reserved, so a name that merely looks
        // like one is a perfectly good name as long as no occurrence is actually numbered onto it.
        RunnableGraph graph = Source.FromRegistered(OrderSource, "stage-0002", SourceParameters)
            .Via(Normalize, "normalize", NormalizeParameters)
            .To(IndexSink, "index-out", IndexParameters);

        Assert.Equal(["index-out", "normalize", "stage-0002"], NodeIds(graph.Document));
        Assert.Empty(graph.Document.Capabilities);
    }

    [Fact]
    public void TwoOccurrencesUnderOneNameAreRejectedNamingTheCollision()
    {
        // Deliberately not pre-checked at attachment: a repeated name is a property of the whole chain, and
        // the fragment algebra already rejects it when the chain is composed. One defect, one diagnostic.
        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
                .Via(Normalize, "orders-in", NormalizeParameters)
                .To(IndexSink, "index-out", IndexParameters));

        Assert.Contains("disjoint node ids", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("'orders-in'", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void OneReusableFlowCarryingANamedOccurrenceCannotBeComposedTwice()
    {
        // The consequence of an explicit name being an identity rather than a position, stated where an
        // author would meet it: a lambda flow used twice contributes two numbered occurrences, and a
        // registered one used twice contributes one name twice.
        Flow<OrderDocument, OrderDocument> enrich =
            Flow.For<OrderDocument>().Via(Enrich, "enrich", CanonicalJsonValue.Parse("{}"));

        // Once is a graph.
        RunnableGraph once = Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .Via(Normalize, "normalize", NormalizeParameters)
            .Via(enrich)
            .To(IndexSink, "index-out", IndexParameters);

        Assert.Equal(["enrich", "index-out", "normalize", "orders-in"], NodeIds(once.Document));

        // Twice is a collision, and the message names the identifier that collided.
        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
                .Via(Normalize, "normalize", NormalizeParameters)
                .Via(enrich)
                .Via(enrich)
                .To(IndexSink, "index-out", IndexParameters));

        Assert.Contains("'enrich'", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnOccurrenceNameThatBreaksTheGrammarIsRejectedUnderItsOwnParameter()
    {
        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => Source.FromRegistered(OrderSource, "Orders In", SourceParameters));

        Assert.Equal("occurrenceName", rejected.ParamName);
        Assert.Contains("Orders In", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnOccurrenceNameThatIsAPathIsRejectedBecauseAnOccurrenceNamesItself()
    {
        // A node identifier may be a path, but the path structure exists for import scoping, which is the
        // fragment algebra's business. An author names one occurrence.
        Assert.Equal(
            "occurrenceName",
            Assert.Throws<ArgumentException>(
                () => Source.FromRegistered(OrderSource, "orders/in", SourceParameters)).ParamName);
    }

    [Fact]
    public void EveryAttachmentSpellingValidatesItsOccurrenceNameUnderTheSameParameterName()
    {
        Source<OrderCreated> orders = Source.FromRegistered(OrderSource, "orders-in", SourceParameters);

        Assert.Equal(
            "occurrenceName",
            Assert.Throws<ArgumentException>(() => orders.Via(Normalize, "", NormalizeParameters)).ParamName);
        Assert.Equal(
            "occurrenceName",
            Assert.Throws<ArgumentException>(
                () => orders.Via(Normalize, "normalize", NormalizeParameters)
                    .To(IndexSink, "", IndexParameters)).ParamName);
        Assert.Equal(
            "occurrenceName",
            Assert.Throws<ArgumentException>(
                () => orders.Via(Normalize, "normalize", NormalizeParameters)
                    .To(CountSink, "", CountParameters, "processed")).ParamName);
        Assert.Equal(
            "occurrenceName",
            Assert.Throws<ArgumentException>(
                () => Flow.For<OrderCreated>().Via(Normalize, "", NormalizeParameters)).ParamName);
    }

    [Fact]
    public void ANullOccurrenceNameIsRejected()
    {
        Assert.Equal(
            "occurrenceName",
            Assert.Throws<ArgumentNullException>(
                () => Source.FromRegistered(OrderSource, null!, SourceParameters)).ParamName);
    }

    [Fact]
    public void ANullHandleIsRejected()
    {
        Assert.Equal(
            "source",
            Assert.Throws<ArgumentNullException>(
                () => Source.FromRegistered<OrderCreated>(null!, "orders-in", SourceParameters)).ParamName);

        Source<OrderCreated> orders = Source.FromRegistered(OrderSource, "orders-in", SourceParameters);

        Assert.Equal(
            "flow",
            Assert.Throws<ArgumentNullException>(
                () => orders.Via<OrderDocument>(null!, "normalize", NormalizeParameters)).ParamName);
        Assert.Equal(
            "sink",
            Assert.Throws<ArgumentNullException>(
                () => orders.Via(Normalize, "normalize", NormalizeParameters)
                    .To((RegisteredSink<OrderDocument>)null!, "index-out", IndexParameters)).ParamName);
    }

    [Fact]
    public void ADefaultPayloadIsRejectedAtTheAttachmentThatCarriesIt()
    {
        // The rule is the node model's, applied where the author wrote the mistake rather than at the close
        // that happens to follow it.
        Assert.Equal(
            "parameters",
            Assert.Throws<ArgumentException>(
                () => Source.FromRegistered(OrderSource, "orders-in", default)).ParamName);
    }

    [Fact]
    public void APayloadThatIsTheJsonNullValueIsRejected()
    {
        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => Source.FromRegistered(OrderSource, "orders-in", CanonicalJsonValue.Parse("null")));

        Assert.Equal("parameters", rejected.ParamName);
        Assert.Contains("JSON null value", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnInvalidSlotNameIsRejectedBeforeAnythingIsBuilt()
    {
        Source<OrderDocument> normalized = Source
            .FromRegistered(OrderSource, "orders-in", SourceParameters)
            .Via(Normalize, "normalize", NormalizeParameters);

        Assert.Equal(
            "slotName",
            Assert.Throws<ArgumentException>(
                () => normalized.To(CountSink, "count-out", CountParameters, "Processed Total")).ParamName);
        Assert.Equal(
            "slotName",
            Assert.Throws<ArgumentNullException>(
                () => normalized.To(CountSink, "count-out", CountParameters, null!)).ParamName);
    }

    [Fact]
    public async Task ARegisteredGraphIsRefusedByTheLocalRuntimeBeforeAnythingIsPlanned()
    {
        // The M2 seam, stated as behavior rather than as a comment. The refusal arrives earlier than the
        // binding table: the host validates every graph against the local catalog first, and a registered
        // stage is an unknown stage there, so the graph is refused before a plan is attempted. The run
        // planner's own message about unbound behavior is for a document that passes that check and still
        // has a node nothing is bound to, which is a hand-built document rather than one of these.
        LocalDataflowHost host = new();

        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await host.MaterializeAsync(Indexed(), TestContext.Current.CancellationToken));

        Assert.Contains(
            "does not validate against the local stage catalog",
            rejected.Message,
            StringComparison.Ordinal);
        Assert.Contains("[unknown-stage]", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("orleans-test/order-source@v1", rejected.Message, StringComparison.Ordinal);
    }

    /// <summary>Declares the index sink over the element type the fixture source produces.</summary>
    /// <returns>The handle.</returns>
    /// <remarks>
    /// Only for the payload-fingerprint test, which needs a two-occurrence chain and therefore a sink that
    /// accepts what the source produces. The stage really does declare <c>order-document@v1</c>, so this
    /// declares the same contract under the other CLR type, which is exactly the process-local assertion
    /// <see cref="ElementContract{T}"/> exists to make.
    /// </remarks>
    private static RegisteredSink<OrderCreated> IndexSinkOverCreated() =>
        RegisteredStage.Sink(Catalog, Stage("index-sink"), ElementContract.For<OrderCreated>("order-document", 1));

    /// <summary>Asserts the parameter contract and the canonical payload bytes of one node.</summary>
    /// <param name="document">The closed document.</param>
    /// <param name="nodeId">The node identifier text.</param>
    /// <param name="parameterContract">The contract identifier text the node must declare.</param>
    /// <param name="canonicalJson">The canonical text the payload must have, byte for byte.</param>
    private static void AssertPayload(
        GraphDocument document,
        string nodeId,
        string parameterContract,
        string canonicalJson)
    {
        StageNode node = Assert.Single(document.Nodes, candidate => candidate.Id.Value == nodeId);

        Assert.Equal(Contract(parameterContract), node.ParameterContract);
        Assert.Equal(Encoding.UTF8.GetBytes(canonicalJson), node.Parameters.CanonicalUtf8Bytes.ToArray());
    }
}
