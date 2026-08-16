using System.Globalization;
using Orleans.Dataflow.Definition;

namespace Orleans.Dataflow.Compilation;

/// <summary>
/// The result of checking one graph document against one stage catalog.
/// </summary>
/// <remarks>
/// <para>
/// A report is a value: it carries the document it is about and every diagnostic found, and it says
/// nothing about what to do next. Refusing to run an invalid graph is a decision for the caller, made
/// where the consequences are known.
/// </para>
/// <para>
/// A report is complete rather than first-failure. Every rule that could be evaluated was evaluated, and
/// rules whose own inputs were already reported as broken were skipped, so the list is what is actually
/// wrong and not a cascade from one root cause.
/// </para>
/// <para>
/// A report is deterministic: the same document and the same catalog produce the same diagnostics in the
/// same order, element for element. Two silos validating one document therefore agree, and a report can
/// be compared, logged, and pinned in a test.
/// </para>
/// <para>
/// A report is only ever created by <see cref="GraphCompiler"/>. There is no public constructor, because
/// a report that nothing validated would be a claim about a document that no rule ever examined.
/// </para>
/// </remarks>
public sealed class GraphValidationReport
{
    /// <summary>
    /// Initializes a new instance of the <see cref="GraphValidationReport"/> class.
    /// </summary>
    /// <param name="document">The document that was validated.</param>
    /// <param name="diagnostics">The diagnostics found, already read-only and in report order.</param>
    internal GraphValidationReport(GraphDocument document, IReadOnlyList<GraphValidationDiagnostic> diagnostics)
    {
        Document = document;
        Diagnostics = diagnostics;
    }

    /// <summary>
    /// Gets the document this report is about.
    /// </summary>
    /// <value>The very instance that was validated.</value>
    /// <remarks>
    /// The document travels with the report so that a diagnostic's subject can be resolved back to the
    /// node, edge, or slot it names without the caller having to keep the two together by hand.
    /// </remarks>
    public GraphDocument Document { get; }

    /// <summary>
    /// Gets every rule the document breaks against the catalog it was checked with.
    /// </summary>
    /// <value>
    /// A read-only list in the deterministic order <see cref="GraphCompiler.Validate"/> documents; empty
    /// when the document is valid.
    /// </value>
    public IReadOnlyList<GraphValidationDiagnostic> Diagnostics { get; }

    /// <summary>
    /// Gets a value indicating whether the document broke no rule.
    /// </summary>
    /// <value><see langword="true"/> when <see cref="Diagnostics"/> is empty; otherwise <see langword="false"/>.</value>
    /// <remarks>
    /// Validity is computed from the diagnostics rather than stored beside them, so a valid report and an
    /// empty diagnostic list cannot disagree.
    /// </remarks>
    public bool IsValid => Diagnostics.Count == 0;

    /// <summary>
    /// Returns a one-line diagnostic summary of this report.
    /// </summary>
    /// <returns>
    /// Text of the form <c>orders-import@r7: valid</c> or <c>orders-import@r7: 3 diagnostics</c>.
    /// </returns>
    /// <remarks>
    /// The count is formatted with the invariant culture so that the text is identical under every
    /// ambient culture. The individual diagnostics are deliberately not rendered here: a report can carry
    /// one per node of a large graph, and a log line has no use for that. The method never throws.
    /// </remarks>
    public override string ToString() =>
        IsValid
            ? $"{Document.Id}@r{Document.Revision}: valid"
            : string.Create(
                CultureInfo.InvariantCulture,
                $"{Document.Id}@r{Document.Revision}: {Diagnostics.Count} diagnostic{(Diagnostics.Count == 1 ? string.Empty : "s")}");
}
