using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Xunit;
using static Orleans.Dataflow.Tests.Compilation.CompilationFixtures;

namespace Orleans.Dataflow.Tests.Compilation;

/// <summary>
/// Tests for each catalog rule <see cref="GraphCompiler"/> implements, and for the gating that keeps one
/// broken element from producing a cascade of derived diagnostics.
/// </summary>
/// <remarks>
/// Every test asserts the whole diagnostic list rather than that it contains something, so a rule that
/// starts firing where it should not fails a test instead of passing one. The rule identifiers are
/// written as literals rather than taken from a constant, because they are the published contract: a test
/// that read them from the implementation could not notice the implementation renaming one.
/// </remarks>
public sealed class GraphCompilerTests
{
    [Fact]
    public void ARepresentativeValidGraphProducesAnEmptyReport()
    {
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("reader", "source"), Node("writer", "sink")],
                edges: [Edge("reader", "out", "writer", "in")],
                resultSlots: [Slot("count", "counter-result", "writer", "result")]));

        Assert.True(report.IsValid);
        Assert.Empty(report.Diagnostics);
        Assert.Equal("compilation-fixture@r1: valid", report.ToString());
    }

    [Fact]
    public void ADocumentWithNoNodesIsValidAgainstEveryCatalog()
    {
        Assert.True(Validate(Graph()).IsValid);
        Assert.True(GraphCompiler.Validate(Graph(), StageCatalog.Create([])).IsValid);
    }

    [Fact]
    public void AnOptionalInputAndAnIgnorableOutputMayAlsoBeConnected()
    {
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("reader", "ignorable-source"), Node("writer", "optional-sink")],
                edges: [Edge("reader", "out", "writer", "in")]));

        Assert.True(report.IsValid);
    }

    [Fact]
    public void UnknownStageIsReportedForANodeTheCatalogDoesNotRegister()
    {
        GraphValidationReport report = Validate(Graph(nodes: [Node("ghost", "missing")]));

        Assert.Equal(["unknown-stage"], Rules(report));
        Assert.Equal(["ghost"], Subjects(report));
        Assert.Contains("orleans-core/missing@v1", report.Diagnostics[0].Message, StringComparison.Ordinal);
    }

    [Fact]
    public void UnknownStageDoesNotFireForARegisteredStage() =>
        Assert.Empty(Validate(Graph(nodes: [Node("probe", "probe")])).Diagnostics);

    [Fact]
    public void AnUnknownStageSuppressesEveryOtherRuleForThatNode()
    {
        // The node declares a parameter contract no stage declares, produces nothing into its dangling
        // edge, and would require a capability the document does not declare. None of that is knowable
        // without a specification, so exactly one diagnostic is reported.
        GraphValidationReport report = Validate(
            Graph(
                nodes: [NodeWithContract("ghost", "missing", "wrong-parameters"), Node("writer", "optional-sink")],
                edges: [Edge("ghost", "out", "writer", "in")]));

        Assert.Equal(["unknown-stage"], Rules(report));
    }

    [Fact]
    public void AnEdgeBetweenTwoUnknownStagesReportsNothingBeyondTheTwoNodes()
    {
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("a", "missing"), Node("b", "also-missing")],
                edges: [Edge("a", "out", "b", "in")]));

        Assert.Equal(["unknown-stage", "unknown-stage"], Rules(report));
        Assert.Equal(["a", "b"], Subjects(report));
    }

    [Fact]
    public void EveryNodeIsReportedWhenTheCatalogIsEmpty()
    {
        GraphValidationReport report = GraphCompiler.Validate(
            Graph(
                nodes: [Node("reader", "source"), Node("writer", "sink")],
                edges: [Edge("reader", "out", "writer", "in")]),
            StageCatalog.Create([]));

        Assert.Equal(["unknown-stage", "unknown-stage"], Rules(report));
    }

    [Fact]
    public void ParameterContractMismatchIsReportedForANodeDeclaringAnotherContract()
    {
        GraphValidationReport report =
            Validate(Graph(nodes: [NodeWithContract("probe", "probe", "wrong-parameters")]));

        Assert.Equal(["parameter-contract-mismatch"], Rules(report));
        Assert.Equal(["probe"], Subjects(report));
        Assert.Contains("wrong-parameters@v1", report.Diagnostics[0].Message, StringComparison.Ordinal);
        Assert.Contains("probe-parameters@v1", report.Diagnostics[0].Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ParameterContractMismatchDoesNotFireForAMatchingContract() =>
        Assert.Empty(
            Validate(Graph(nodes: [NodeWithContract("probe", "probe", "probe-parameters")])).Diagnostics);

    [Fact]
    public void InvalidParametersReportsOneDiagnosticPerFragmentInTheValidatorsOrder()
    {
        RecordingValidator validator = new("the member 'a' is missing", "the member 'b' is missing");

        GraphValidationReport report = GraphCompiler.Validate(
            Graph(nodes: [Node("strict", "strict")]),
            Catalog(validator));

        Assert.Equal(["invalid-parameters", "invalid-parameters"], Rules(report));
        Assert.Equal(["strict", "strict"], Subjects(report));
        Assert.Contains("the member 'a' is missing", report.Diagnostics[0].Message, StringComparison.Ordinal);
        Assert.Contains("the member 'b' is missing", report.Diagnostics[1].Message, StringComparison.Ordinal);
        Assert.Equal(1, validator.CallCount);
    }

    [Fact]
    public void InvalidParametersDoesNotFireWhenTheValidatorAcceptsThePayload()
    {
        RecordingValidator validator = new();

        GraphValidationReport report = GraphCompiler.Validate(
            Graph(nodes: [Node("strict", "strict")]),
            Catalog(validator));

        Assert.True(report.IsValid);
        Assert.Equal(1, validator.CallCount);
        Assert.Equal("""{"value":1}""", validator.LastParameters.ToString());
    }

    [Fact]
    public void AWrongParameterContractSuppressesTheValidatorEntirely()
    {
        RecordingValidator validator = new("the payload is refused");

        GraphValidationReport report = GraphCompiler.Validate(
            Graph(nodes: [NodeWithContract("strict", "strict", "wrong-parameters")]),
            Catalog(validator));

        Assert.Equal(["parameter-contract-mismatch"], Rules(report));

        // Not merely "the validator reported nothing": it was never asked. A payload written for another
        // contract is not evidence about this contract.
        Assert.Equal(0, validator.CallCount);
    }

    [Fact]
    public void UnknownOutputPortIsReportedForAnEdgeOriginTheStageDoesNotDeclare()
    {
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("probe", "probe"), Node("writer", "sink")],
                edges: [Edge("probe", "nope", "writer", "in")]));

        Assert.Equal(["unknown-output-port"], Rules(report));
        Assert.Equal(["probe#nope"], Subjects(report));
    }

    [Fact]
    public void UnknownInputPortIsReportedForAnEdgeTargetTheStageDoesNotDeclare()
    {
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("reader", "source"), Node("probe", "probe")],
                edges: [Edge("reader", "out", "probe", "nope")]));

        Assert.Equal(["unknown-input-port"], Rules(report));
        Assert.Equal(["probe#nope"], Subjects(report));
    }

    [Fact]
    public void AnEdgeWhoseBothEndsAreUnknownPortsReportsBothAndComparesNoContracts()
    {
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("a", "probe"), Node("b", "probe")],
                edges: [Edge("a", "out", "b", "in")]));

        Assert.Equal(["unknown-output-port", "unknown-input-port"], Rules(report));
        Assert.Equal(["a#out", "b#in"], Subjects(report));
    }

    [Fact]
    public void ElementContractMismatchIsReportedForPortsWithDifferentContracts()
    {
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("reader", "source"), Node("writer", "typed-sink")],
                edges: [Edge("reader", "out", "writer", "in")]));

        Assert.Equal(["element-contract-mismatch"], Rules(report));
        Assert.Equal(["reader#out -> writer#in"], Subjects(report));
        Assert.Contains("order@v1", report.Diagnostics[0].Message, StringComparison.Ordinal);
        Assert.Contains("order-summary@v3", report.Diagnostics[0].Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ElementContractMismatchDoesNotFireForEqualContracts() =>
        Assert.Empty(
            Validate(
                Graph(
                    nodes: [Node("reader", "source"), Node("writer", "optional-sink")],
                    edges: [Edge("reader", "out", "writer", "in")])).Diagnostics);

    [Fact]
    public void UnknownResultPortIsReportedForASlotProducerTheStageDoesNotDeclare()
    {
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("probe", "probe")],
                resultSlots: [Slot("count", "counter-result", "probe", "nope")]));

        Assert.Equal(["unknown-result-port"], Rules(report));
        Assert.Equal(["count"], Subjects(report));
        Assert.Contains("probe#nope", report.Diagnostics[0].Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnUnknownResultPortSuppressesTheResultContractCheck()
    {
        // The slot's contract cannot disagree with a port that does not exist, so the report says the one
        // thing that is true rather than two things that describe one mistake.
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("probe", "probe")],
                resultSlots: [Slot("count", "unrelated-result", "probe", "nope")]));

        Assert.Equal(["unknown-result-port"], Rules(report));
    }

    [Fact]
    public void ResultContractMismatchIsReportedForASlotDeclaringAnotherContract()
    {
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("reader", "source"), Node("writer", "sink")],
                edges: [Edge("reader", "out", "writer", "in")],
                resultSlots: [Slot("count", "other-result", "writer", "result")]));

        Assert.Equal(["result-contract-mismatch"], Rules(report));
        Assert.Equal(["count"], Subjects(report));
        Assert.Contains("other-result@v1", report.Diagnostics[0].Message, StringComparison.Ordinal);
        Assert.Contains("counter-result@v1", report.Diagnostics[0].Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TwoSlotsSharingOneProducerAreReportedSeparatelyByName()
    {
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("probe", "probe")],
                resultSlots:
                [
                    Slot("first", "counter-result", "probe", "nope"),
                    Slot("second", "counter-result", "probe", "nope"),
                ]));

        // Sharing a producer is allowed by the model, so the subject has to be the slot rather than the
        // address; otherwise two diagnostics would be indistinguishable.
        Assert.Equal(["unknown-result-port", "unknown-result-port"], Rules(report));
        Assert.Equal(["first", "second"], Subjects(report));
    }

    [Fact]
    public void UnconnectedInputPortIsReportedForARequiredInputWithNoEdge()
    {
        GraphValidationReport report = Validate(Graph(nodes: [Node("writer", "sink")]));

        Assert.Equal(["unconnected-input-port"], Rules(report));
        Assert.Equal(["writer#in"], Subjects(report));
    }

    [Fact]
    public void UnconnectedInputPortDoesNotFireForAnOptionalInput() =>
        Assert.Empty(Validate(Graph(nodes: [Node("writer", "optional-sink")])).Diagnostics);

    [Fact]
    public void UnconnectedOutputPortIsReportedForANonIgnorableOutputWithNoEdge()
    {
        GraphValidationReport report = Validate(Graph(nodes: [Node("reader", "source")]));

        Assert.Equal(["unconnected-output-port"], Rules(report));
        Assert.Equal(["reader#out"], Subjects(report));
    }

    [Fact]
    public void UnconnectedOutputPortDoesNotFireForAnIgnorableOutput() =>
        Assert.Empty(Validate(Graph(nodes: [Node("reader", "ignorable-source")])).Diagnostics);

    [Fact]
    public void AResultPortNeedsNoEdgeOfItsOwn()
    {
        // 'sink' declares one required input and one result port. Wiring the input is enough; the result
        // is read through a slot, and connectivity says nothing about it.
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("reader", "source"), Node("writer", "sink")],
                edges: [Edge("reader", "out", "writer", "in")]));

        Assert.True(report.IsValid);
    }

    [Fact]
    public void AnEdgeToAnUnresolvedNodeStillConnectsItsOrigin()
    {
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("ghost", "missing"), Node("reader", "source")],
                edges: [Edge("reader", "out", "ghost", "in")]));

        Assert.Equal(["unknown-stage"], Rules(report));
    }

    [Fact]
    public void AnEdgeFromAnUnresolvedNodeStillConnectsItsTarget()
    {
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("ghost", "missing"), Node("writer", "sink")],
                edges: [Edge("ghost", "out", "writer", "in")]));

        Assert.Equal(["unknown-stage"], Rules(report));
    }

    [Fact]
    public void AnEdgeAtAnUndeclaredPortDoesNotSatisfyADeclaredPortOfTheSameNode()
    {
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("probe", "probe"), Node("writer", "sink")],
                edges: [Edge("probe", "nope", "writer", "other")]));

        Assert.Equal(["unknown-output-port", "unknown-input-port", "unconnected-input-port"], Rules(report));
        Assert.Equal(["probe#nope", "writer#other", "writer#in"], Subjects(report));
    }

    [Fact]
    public void UndeclaredCapabilityIsReportedForARequirementTheDocumentDoesNotDeclare()
    {
        GraphValidationReport report = Validate(Graph(nodes: [Node("capable", "capable")]));

        Assert.Equal(["undeclared-capability"], Rules(report));
        Assert.Equal(["nondeployable"], Subjects(report));
    }

    [Fact]
    public void UndeclaredCapabilityDoesNotFireWhenTheDocumentDeclaresIt() =>
        Assert.Empty(
            Validate(
                Graph(
                    nodes: [Node("capable", "capable")],
                    capabilities: [CapabilityToken.Nondeployable])).Diagnostics);

    [Fact]
    public void ACapabilityDeclaredButRequiredByNoStageIsNotADiagnostic() =>
        Assert.Empty(
            Validate(
                Graph(
                    nodes: [Node("probe", "probe")],
                    capabilities: [CapabilityToken.Nondeployable])).Diagnostics);

    [Fact]
    public void UndeclaredCapabilitiesAreReportedInOrdinalOrderRatherThanNodeOrder()
    {
        // The nodes contribute 'zulu' first and 'alpha' second, so a report that merely echoed the order
        // it met them in would come out in the other order.
        GraphValidationReport report = Validate(
            Graph(nodes: [Node("a-zulu", "needs-zulu"), Node("b-alpha", "needs-alpha")]));

        Assert.Equal(["undeclared-capability", "undeclared-capability"], Rules(report));
        Assert.Equal(["alpha", "zulu"], Subjects(report));
    }

    [Fact]
    public void OneStageUsedByManyNodesContributesOneCapabilityDiagnostic()
    {
        GraphValidationReport report = Validate(
            Graph(nodes: [Node("one", "capable"), Node("two", "capable"), Node("three", "capable")]));

        Assert.Equal(["undeclared-capability"], Rules(report));
    }

    [Fact]
    public void AnUnknownStageContributesNoCapabilityRequirement()
    {
        GraphValidationReport report = Validate(
            Graph(nodes: [Node("ghost", "missing"), Node("probe", "probe")]));

        Assert.Equal(["unknown-stage"], Rules(report));
    }

    [Fact]
    public void ConnectivityReportsInputsBeforeOutputsAndPortsInCanonicalOrder()
    {
        // The stage declares its ports in the reverse of canonical order, so a report that echoed the
        // registration order would come out as in-b, in-a, out-b, out-a.
        GraphValidationReport report = Validate(Graph(nodes: [Node("hub", "hub")]));

        Assert.Equal(
            [
                "unconnected-input-port",
                "unconnected-input-port",
                "unconnected-output-port",
                "unconnected-output-port",
            ],
            Rules(report));

        Assert.Equal(["hub#in-a", "hub#in-b", "hub#out-a", "hub#out-b"], Subjects(report));
    }

    [Fact]
    public void AnEdgeOriginatingAtADeclaredInputPortIsNotAnOutput()
    {
        // Port names are unique across the whole stage, so 'in' is an input and nothing else. Using it as
        // an origin names no output, and the input it does name is still unconnected.
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("probe", "probe"), Node("writer", "sink")],
                edges: [Edge("writer", "in", "probe", "in")]));

        Assert.Equal(["unknown-output-port", "unknown-input-port", "unconnected-input-port"], Rules(report));
        Assert.Equal(["writer#in", "probe#in", "writer#in"], Subjects(report));
    }

    [Fact]
    public void AnEdgeTerminatingAtADeclaredResultPortIsNotAnInput()
    {
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("reader", "source"), Node("writer", "sink")],
                edges: [Edge("reader", "out", "writer", "result")]));

        Assert.Equal(["unknown-input-port", "unconnected-input-port"], Rules(report));
        Assert.Equal(["writer#result", "writer#in"], Subjects(report));
    }

    [Fact]
    public void ASlotProducedByADeclaredInputPortIsNotAResultPort()
    {
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("reader", "source"), Node("writer", "sink")],
                edges: [Edge("reader", "out", "writer", "in")],
                resultSlots: [Slot("count", "counter-result", "writer", "in")]));

        Assert.Equal(["unknown-result-port"], Rules(report));
        Assert.Equal(["count"], Subjects(report));
    }

    [Fact]
    public void SubjectsRenderHierarchicalNodeIdentifiersInFull()
    {
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("orders/reader", "source"), Node("orders/writer", "typed-sink")],
                edges: [Edge("orders/reader", "out", "orders/writer", "in")]));

        Assert.Equal(["element-contract-mismatch"], Rules(report));
        Assert.Equal(["orders/reader#out -> orders/writer#in"], Subjects(report));
    }

    [Fact]
    public void ADeclaredExecutionPolicyIsNotValidatedInThisMilestone()
    {
        // The specification does not yet declare which policy contracts a stage accepts, so a node may
        // carry any policy without the compiler having an opinion. This is recorded as a limitation in
        // the design document, and pinned here so that adding the rule later is a deliberate change.
        GraphValidationReport report = Validate(Graph(nodes: [NodeWithExecutionPolicy("probe", "probe")]));

        Assert.True(report.IsValid);
    }

    [Fact]
    public void OnlyTheCapabilitiesTheDocumentOmitsAreReported()
    {
        GraphValidationReport report = Validate(
            Graph(
                nodes: [Node("a-zulu", "needs-zulu"), Node("b-alpha", "needs-alpha")],
                capabilities: [Capability("alpha")]));

        Assert.Equal(["undeclared-capability"], Rules(report));
        Assert.Equal(["zulu"], Subjects(report));
    }

    [Fact]
    public void ManyNodesOnOneSpecificationAreCheckedIndependently()
    {
        GraphValidationReport report = Validate(
            Graph(nodes: [Node("first", "sink"), Node("second", "sink"), Node("third", "probe")]));

        Assert.Equal(["unconnected-input-port", "unconnected-input-port"], Rules(report));
        Assert.Equal(["first#in", "second#in"], Subjects(report));
    }

    /// <summary>Validates a document against the fixture catalog.</summary>
    /// <param name="document">The document to validate.</param>
    /// <returns>The report.</returns>
    private static GraphValidationReport Validate(GraphDocument document) =>
        GraphCompiler.Validate(document, Catalog());

    /// <summary>Projects the rule identifiers of a report, in report order.</summary>
    /// <param name="report">The report to read.</param>
    /// <returns>The rule identifiers.</returns>
    private static IEnumerable<string> Rules(GraphValidationReport report) =>
        report.Diagnostics.Select(diagnostic => diagnostic.Rule);

    /// <summary>Projects the subjects of a report, in report order.</summary>
    /// <param name="report">The report to read.</param>
    /// <returns>The subjects.</returns>
    private static IEnumerable<string?> Subjects(GraphValidationReport report) =>
        report.Diagnostics.Select(diagnostic => diagnostic.Subject);
}
