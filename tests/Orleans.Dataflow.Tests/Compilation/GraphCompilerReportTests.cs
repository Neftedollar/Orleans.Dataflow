using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Xunit;
using static Orleans.Dataflow.Tests.Compilation.CompilationFixtures;

namespace Orleans.Dataflow.Tests.Compilation;

/// <summary>
/// Tests for the shape of a <see cref="GraphValidationReport"/>: the order diagnostics appear in, the
/// determinism of a report, the argument contract of <see cref="GraphCompiler.Validate"/>, and what
/// happens when a registered validator breaks its own contract.
/// </summary>
public sealed class GraphCompilerReportTests
{
    [Fact]
    public void DiagnosticsAppearInDocumentOrderAcrossEveryPhase()
    {
        RecordingValidator validator = new("the payload is refused");

        GraphValidationReport report = GraphCompiler.Validate(BrokenGraph(), Catalog(validator));

        // Nodes first, in node order; then edges; then result slots; then connectivity, in node order;
        // then capabilities. The one edge in this document is well formed at its resolved end and gated
        // at its unresolved one, so the edge phase contributes nothing and its absence is part of what
        // this order pins.
        Assert.Equal(
            [
                "unknown-stage",
                "parameter-contract-mismatch",
                "unknown-result-port",
                "unconnected-input-port",
                "undeclared-capability",
            ],
            report.Diagnostics.Select(diagnostic => diagnostic.Rule));

        Assert.Equal(
            ["a-ghost", "b-strict", "s1", "d-sink#in", "nondeployable"],
            report.Diagnostics.Select(diagnostic => diagnostic.Subject));

        Assert.Equal(0, validator.CallCount);
        Assert.False(report.IsValid);
    }

    [Fact]
    public void ValidatingTwiceProducesEqualReportsElementForElement()
    {
        StageCatalog catalog = Catalog();
        GraphDocument document = BrokenGraph();

        GraphValidationReport first = GraphCompiler.Validate(document, catalog);
        GraphValidationReport second = GraphCompiler.Validate(document, catalog);

        Assert.NotSame(first, second);
        Assert.Equal(first.Diagnostics, second.Diagnostics);
    }

    [Fact]
    public void IndependentlyBuiltInputsProduceEqualReports()
    {
        // The report is a function of the document and the catalog, not of the objects that carry them:
        // nothing about instance identity, registration order, or construction order reaches it.
        GraphValidationReport first = GraphCompiler.Validate(BrokenGraph(), Catalog());
        GraphValidationReport second = GraphCompiler.Validate(BrokenGraph(), Catalog());

        Assert.Equal(first.Diagnostics, second.Diagnostics);
        Assert.Equal(first.Document, second.Document);
    }

    [Fact]
    public void TheReportCarriesTheVeryDocumentThatWasValidated()
    {
        GraphDocument document = Graph(nodes: [Node("probe", "probe")]);

        Assert.Same(document, GraphCompiler.Validate(document, Catalog()).Document);
    }

    [Fact]
    public void AValidReportIsValidAndEmpty()
    {
        GraphValidationReport report = GraphCompiler.Validate(Graph(), Catalog());

        Assert.True(report.IsValid);
        Assert.Empty(report.Diagnostics);
        Assert.Equal("compilation-fixture@r1: valid", report.ToString());
    }

    [Fact]
    public void ToStringCountsTheDiagnosticsItDoesNotRenderThem()
    {
        GraphValidationReport one = GraphCompiler.Validate(Graph(nodes: [Node("writer", "sink")]), Catalog());
        GraphValidationReport many = GraphCompiler.Validate(BrokenGraph(), Catalog());

        Assert.Equal("compilation-fixture@r1: 1 diagnostic", one.ToString());
        Assert.Equal("compilation-fixture@r1: 5 diagnostics", many.ToString());
    }

    [Fact]
    public void DiagnosticsAreReadOnlyAndNotTheUnderlyingList()
    {
        GraphValidationReport report = GraphCompiler.Validate(BrokenGraph(), Catalog());

        Assert.IsNotType<List<GraphValidationDiagnostic>>(report.Diagnostics);

        IList<GraphValidationDiagnostic> diagnostics =
            Assert.IsAssignableFrom<IList<GraphValidationDiagnostic>>(report.Diagnostics);

        Assert.True(diagnostics.IsReadOnly);
        Assert.Throws<NotSupportedException>(
            () => diagnostics.Add(GraphValidationDiagnostic.Create("intruder", "a message")));
    }

    [Fact]
    public void ValidateRejectsANullDocument() =>
        Assert.Throws<ArgumentNullException>("document", () => GraphCompiler.Validate(null!, Catalog()));

    [Fact]
    public void ValidateRejectsANullCatalog() =>
        Assert.Throws<ArgumentNullException>("catalog", () => GraphCompiler.Validate(Graph(), null!));

    [Fact]
    public void AValidatorThatReturnsNoListAtAllIsReportedAsACatalogDefect()
    {
        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => GraphCompiler.Validate(
                Graph(nodes: [Node("strict", "strict")]),
                Catalog(new ContractBreakingValidator(returnsNull: true))));

        // The message names the stage rather than the node: no edit to the graph could fix this, so
        // whoever reads it has to be sent to the registration, not to the document.
        Assert.Contains("orleans-core/strict@v1", exception.Message, StringComparison.Ordinal);
        Assert.Contains("returned no list at all", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void AValidatorThatReturnsABlankFragmentIsReportedAsACatalogDefect(string? fragment)
    {
        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => GraphCompiler.Validate(
                Graph(nodes: [Node("strict", "strict")]),
                Catalog(new ContractBreakingValidator(returnsNull: false, fragment))));

        Assert.Contains("orleans-core/strict@v1", exception.Message, StringComparison.Ordinal);
        Assert.Contains("the fragment at index 0 is blank", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ACatalogThatResolvesToNoSpecificationIsReportedAsACatalogDefect()
    {
        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => GraphCompiler.Validate(
                Graph(nodes: [Node("probe", "probe")]),
                new ContractBreakingCatalog()));

        Assert.Contains("orleans-core/probe@v1", exception.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Builds a document that breaks one rule in each of the five phases.
    /// </summary>
    /// <returns>The document.</returns>
    /// <remarks>
    /// <para>
    /// <c>a-ghost</c> references a stage the catalog does not register; <c>b-strict</c> declares the
    /// wrong parameter contract for a stage that carries a validator; <c>c-capable</c> requires a
    /// capability the document does not declare; <c>d-sink</c> leaves a required input unconnected and
    /// carries a slot naming a result port it does not declare; and <c>e-source</c> feeds its output into
    /// the unresolved node, which connects that output without producing a diagnostic of its own.
    /// </para>
    /// <para>
    /// The node identifiers are named so that their canonical order is the order the phases visit them
    /// in, which makes the expected diagnostic sequence readable rather than a puzzle.
    /// </para>
    /// </remarks>
    private static GraphDocument BrokenGraph() =>
        Graph(
            nodes:
            [
                Node("a-ghost", "missing"),
                NodeWithContract("b-strict", "strict", "wrong-parameters"),
                Node("c-capable", "capable"),
                Node("d-sink", "sink"),
                Node("e-source", "source"),
            ],
            edges: [Edge("e-source", "out", "a-ghost", "in")],
            resultSlots: [Slot("s1", "counter-result", "d-sink", "nope")]);
}
