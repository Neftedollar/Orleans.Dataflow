using System.Globalization;
using System.Text;
using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// Turns a deployable document into a started run, using a host's catalog and its runtime factories in
/// place of the binding table a locally authored graph carries.
/// </summary>
/// <remarks>
/// <para>
/// The counterpart of <see cref="LocalDataflowHost"/> for the deployable plane, and the same three steps
/// in the same order: validate the document against the host's catalog, compile it into a plan, start the
/// run. What differs is only where behavior comes from — a factory registered per provider rather than a
/// delegate the author closed over — which is the whole of the runtime-factory seam.
/// </para>
/// <para>
/// The run this starts carries <see cref="Guid.Empty"/> as its authoring nonce, and that value is the
/// documented statement "this is a run of a pipeline". A pipeline's slots bind by fingerprint and lineage
/// without a per-instance nonce (ADR 0004 section 4), because registered behavior is in the document and
/// content identity therefore means something; a nonce would claim a distinction that does not exist. The
/// sentinel is what lets a handle tell a pipeline's slot from a built graph's and refuse each against the
/// other's run.
/// </para>
/// <para>
/// Internal, and the seam a same-repo host package consumes. It is not the M4 provider SDK: what is fixed
/// here is that materializing a document needs a catalog and a factory registry and nothing else, not the
/// public spelling by which a provider ships them.
/// </para>
/// </remarks>
internal static class PipelineMaterializer
{
    /// <summary>The greatest number of diagnostics one refusal spells out.</summary>
    /// <remarks>
    /// <para>
    /// A refusal is a message, and a message that is thrown across a grain boundary is bytes on the wire
    /// that the refused caller pays for. The report itself is unbounded — it holds one diagnostic per thing
    /// wrong with a document, and a document is an input — so a document engineered to be wrong in a hundred
    /// thousand places would otherwise answer with a hundred thousand sentences: measured at 23,600,112
    /// characters for a 200,000-node document, which is a refusal larger than the thing being refused.
    /// </para>
    /// <para>
    /// Twenty, and the count of what was left out is stated rather than the list silently ending. Twenty is
    /// generous for the case this text is written for — an author reconciling a document with a deployment's
    /// catalog, who reads the diagnostics and fixes them — and a document with more than twenty things wrong
    /// with it is one whose first twenty are where the work starts. The whole report is still there for a
    /// caller that has it: this bounds what crosses a wire, not what validation produced.
    /// </para>
    /// </remarks>
    internal const int ReportedDiagnosticLimit = 20;

    /// <summary>The authoring nonce every run of a pipeline carries.</summary>
    /// <remarks>
    /// Documented rather than incidental: a slot whose nonce is this value was declared by a
    /// <see cref="PipelineDefinition"/>, and a slot whose nonce is anything else was declared by a built
    /// <see cref="RunnableGraph"/> instance. The two are different worlds and a handle rejects each
    /// against the other, which only works because this value is reserved and
    /// <see cref="RunnableGraph"/> allocates its nonce with <see cref="Guid.NewGuid"/>.
    /// </remarks>
    internal static Guid PipelineNonce => Guid.Empty;

    /// <summary>Validates a document against a host and starts a run of it.</summary>
    /// <param name="document">The deployable document.</param>
    /// <param name="fingerprint">The fingerprint of that document's canonical bytes.</param>
    /// <param name="catalog">The host's stage catalog.</param>
    /// <param name="factories">The host's runtime factories, keyed by provider.</param>
    /// <param name="runIdentity">
    /// What this run is called in the deployment, which is the run grain's own key: a source that has to be
    /// addressable from outside the run composes its identity from it.
    /// </param>
    /// <param name="cancellationToken">A token that cancels the run this call starts.</param>
    /// <returns>The started run.</returns>
    /// <exception cref="ArgumentNullException">Any reference argument is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">
    /// The document does not validate against <paramref name="catalog"/>, or it is not the one linear
    /// chain of resolvable stages this runtime executes.
    /// </exception>
    /// <remarks>
    /// The run is started before this method returns, exactly as the local host's is, and an already
    /// cancelled token does not make the call throw: the run starts, observes the token before its first
    /// pull, and ends cancelled. Cancellation is an outcome of a run and never a failure of
    /// materialization, so a caller always receives something to await and dispose.
    /// </remarks>
    internal static LocalRun Start(
        GraphDocument document,
        GraphFingerprint fingerprint,
        IStageCatalog catalog,
        StageRuntimeRegistry factories,
        string runIdentity,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(document);
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(factories);
        ArgumentNullException.ThrowIfNull(runIdentity);

        GraphValidationReport report = GraphCompiler.Validate(document, catalog);

        if (!report.IsValid)
        {
            throw new InvalidOperationException(Describe(report));
        }

        // The system clock, and not an option of this seam. Every stage of this runtime that reads a clock
        // is a stage of the local vocabulary, and the local vocabulary has no binding here: a document
        // materialized through this path is registered stages only, so nothing it holds can ask the time.
        // When a registered stage ever needs one, it arrives through the runtime-factory seam beside the
        // tokens rather than through here.
        LocalRunPlan plan = LocalRunPlanner.Compile(
            document,
            new Dictionary<Identity.NodeId, Authoring.LocalStageDescriptor>(),
            new StageRuntimeBinder(catalog, factories),
            runIdentity,
            TimeProvider.System);

        return LocalRun.Start(plan, fingerprint, PipelineNonce, durable: null, resumed: false, cancellationToken);
    }

    /// <summary>Validates a document against a host and starts a run of it that writes checkpoints.</summary>
    /// <param name="document">The deployable document.</param>
    /// <param name="fingerprint">The fingerprint of that document's canonical bytes.</param>
    /// <param name="catalog">The host's stage catalog.</param>
    /// <param name="factories">The host's runtime factories, keyed by provider.</param>
    /// <param name="runIdentity">What this run is called in the deployment.</param>
    /// <param name="durable">Where this run's checkpoints go, what it is called, and when one is taken.</param>
    /// <param name="checkpoint">
    /// What a resume read back, or <see langword="null"/> for a durable run starting from the beginning.
    /// </param>
    /// <param name="etag">
    /// The ETag the resumed attempt presents at its first capture, or <see langword="null"/> when this run
    /// believes the store holds nothing for its identity.
    /// </param>
    /// <param name="cancellationToken">A token that cancels the run this call starts.</param>
    /// <returns>The started run.</returns>
    /// <exception cref="ArgumentNullException">Any required reference argument is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">
    /// The document does not validate, it is not one this runtime executes, or the checkpoint names a node
    /// this graph has no such seam for.
    /// </exception>
    /// <remarks>
    /// <para>
    /// <see cref="Start"/> with the two things a durable run adds, and deliberately nothing else: whatever a
    /// checkpoint carried is handed back to the plan's seams before the first element, and a capture loop
    /// runs beside the run on its declared timing. A fresh durable run and a resumed one differ in exactly
    /// those two inputs — what the seams were handed, and which ETag the first capture presents — which is
    /// the same statement <see cref="LocalDataflowHost"/> makes about its own two spellings.
    /// </para>
    /// <para>
    /// <b>Whether the checkpoint describes this graph is the caller's question and not this one's.</b> A
    /// host that reads a store has to be able to refuse a mismatch in its own vocabulary — by name, and
    /// across a wire where an exception chain does not survive — so the fingerprint and revision comparison
    /// stays where the store was read rather than being buried in a materialization failure.
    /// </para>
    /// </remarks>
    internal static LocalRun StartDurable(
        GraphDocument document,
        GraphFingerprint fingerprint,
        IStageCatalog catalog,
        StageRuntimeRegistry factories,
        string runIdentity,
        DurableRunOptions durable,
        LocalCheckpoint? checkpoint,
        string? etag,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(document);
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(factories);
        ArgumentNullException.ThrowIfNull(runIdentity);
        ArgumentNullException.ThrowIfNull(durable);

        GraphValidationReport report = GraphCompiler.Validate(document, catalog);

        if (!report.IsValid)
        {
            throw new InvalidOperationException(Describe(report));
        }

        LocalRunPlan plan = LocalRunPlanner.Compile(
            document,
            new Dictionary<Identity.NodeId, Authoring.LocalStageDescriptor>(),
            new StageRuntimeBinder(catalog, factories),
            runIdentity,
            TimeProvider.System);

        if (checkpoint is not null)
        {
            LocalResume.Restore(plan, checkpoint);
        }

        bool declared = durable.Interval is not null || durable.EveryElements is not null;

        return LocalRun.Start(
            plan,
            fingerprint,
            PipelineNonce,
            declared
                ? started => new LocalCheckpointer(
                    plan,
                    started.Pause,
                    plan.Clock,
                    durable,
                    fingerprint,
                    document.Revision,
                    document.Id,
                    etag,
                    started.Faulted,
                    started.StopToken)
                : null,
            resumed: checkpoint is not null,
            cancellationToken);
    }

    /// <summary>Renders a failed validation report as the message of the exception that refuses a document.</summary>
    /// <param name="report">The report, which is known to carry at least one diagnostic.</param>
    /// <returns>The message.</returns>
    /// <remarks>
    /// <para>
    /// Each diagnostic appears, in the report's own deterministic order, as its stable rule identifier, its
    /// subject when it names one, and its message. A caller reconciling a document with a host's catalog
    /// needs the report and not its first line, which is exactly why a rolling upgrade that removed a stage
    /// produces a readable refusal rather than one line about one node.
    /// </para>
    /// <para>
    /// <b>Up to <see cref="ReportedDiagnosticLimit"/> of them, and the rest are counted.</b> The full count
    /// is stated first, so the sentence a caller reads is honest about how much of the report it is holding;
    /// what is bounded is the text, which travels as the message of an exception across a wire.
    /// </para>
    /// </remarks>
    internal static string Describe(GraphValidationReport report)
    {
        ArgumentNullException.ThrowIfNull(report);

        StringBuilder message = new();

        message.Append(
            CultureInfo.InvariantCulture,
            $"The document does not validate against this host's stage catalog and cannot be materialized. {report.Diagnostics.Count} diagnostic");

        if (report.Diagnostics.Count != 1)
        {
            message.Append('s');
        }

        message.Append(':');

        foreach (GraphValidationDiagnostic diagnostic in report.Diagnostics.Take(ReportedDiagnosticLimit))
        {
            message.Append(CultureInfo.InvariantCulture, $" [{diagnostic.Rule}]");

            if (diagnostic.Subject is { } subject)
            {
                message.Append(CultureInfo.InvariantCulture, $" {subject}:");
            }

            message.Append(CultureInfo.InvariantCulture, $" {diagnostic.Message}");
        }

        if (report.Diagnostics.Count > ReportedDiagnosticLimit)
        {
            message.Append(
                CultureInfo.InvariantCulture,
                $" … and {report.Diagnostics.Count - ReportedDiagnosticLimit} more, which this message does not spell out: a refusal travels as text across a wire, so it names the first {ReportedDiagnosticLimit} and counts the rest.");
        }

        return message.ToString();
    }
}
