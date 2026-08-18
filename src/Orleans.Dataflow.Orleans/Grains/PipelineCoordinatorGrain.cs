using System.Globalization;
using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;
using Orleans.Runtime;

namespace Orleans.Dataflow.Grains;

/// <summary>
/// The coordinator grain: one activation per pipeline, ordering its runs and issuing their ownership.
/// </summary>
/// <remarks>
/// <para>
/// Everything a start has to be sure of happens here, in one order, before anything is created: the bytes
/// are a canonical document, the document is this pipeline's, every stage resolves in this silo's catalog,
/// and every provider named has a factory to build it. A caller therefore learns that a deployment cannot
/// run a graph before a run identity exists, which is what makes "refused" and "failed" two different
/// outcomes rather than two spellings of one.
/// </para>
/// <para>
/// The epoch is issued from persisted state, which is the fencing. A superseded activation that reaches
/// the write hits the ETag conflict, is killed by the runtime, and the caller retries against the fresh
/// activation that read the truth. Nothing here implements that; it is what the state write already means.
/// </para>
/// <para>
/// <b>A durable run is declared here and started somewhere else</b>, and the split is what makes resume
/// need no protocol of its own. A declaration records the document, the timing, and an epoch; the
/// activation that hosts the run claims that epoch when it comes up, and every later activation claims a
/// fresh one. So the first attempt and the attempt after a silo died take exactly the same path, and this
/// grain never has to know which of the two it is answering — which is also why nothing here ever awaits a
/// run grain while holding the register, and why the three passthroughs that do await one interleave.
/// </para>
/// <para>
/// <b>Since M5.4 the register also records that a run is over.</b> A run grain reports the terminal state it
/// reached, this grain writes it onto the declaration, and a later claim answers with it instead of handing
/// out a document to continue. The edge is one-way exactly as the claim is — a report writes state and calls
/// nobody — so the shape argument above is unchanged: the members that touch the register still await no run
/// grain, and the members that await a run grain still touch no state.
/// </para>
/// </remarks>
internal sealed class PipelineCoordinatorGrain(
    [PersistentState("pipeline", OrleansDataflowStorage.CoordinatorProviderName)]
    IPersistentState<PipelineCoordinatorState> state,
    DataflowSiloRegistry registry) : Grain, IPipelineCoordinatorGrain
{
    /// <inheritdoc/>
    public async Task<PipelineRunTicket> StartRunAsync(byte[] canonicalDocument)
    {
        ArgumentNullException.ThrowIfNull(canonicalDocument);

        GraphDocument document = Read(canonicalDocument);
        string pipeline = this.GetPrimaryKeyString();

        if (!GraphId.TryCreate(pipeline, out GraphId addressed) || document.Id != addressed)
        {
            throw new PipelineRejectedException(
                $"The document declares the pipeline '{document.Id}' and this coordinator owns the pipeline '{pipeline}'. A coordinator starts runs of its own pipeline only, because the epochs it issues order claims to that pipeline and nothing else.");
        }

        Refuse(document);

        // Written before the run grain is told anything. The epoch is the claim, and a claim that was
        // handed out before it was recorded would survive a lost activation as a number nobody can
        // reproduce; recorded first, a start that fails afterwards costs an unused epoch and nothing else.
        state.State.LastEpoch++;

        long epoch = state.State.LastEpoch;
        RunId run = RunId.Create(Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture));
        GraphFingerprint fingerprint = GraphFingerprint.OfSerialized(canonicalDocument);

        await state.WriteStateAsync();

        await GrainFactory
            .GetGrain<IPipelineRunGrain>(RunKey(document.Id, run))
            .StartAsync(canonicalDocument, epoch);

        return Ticket(document.Id, run, epoch, fingerprint);
    }

    /// <inheritdoc/>
    public async Task<PipelineRunTicket> DeclareDurableRunAsync(
        byte[] canonicalDocument,
        DurableRunDeclaration declaration)
    {
        ArgumentNullException.ThrowIfNull(canonicalDocument);
        ArgumentNullException.ThrowIfNull(declaration);

        (GraphDocument document, RunId run, GraphFingerprint fingerprint) = Admit(canonicalDocument, declaration);
        string declared = fingerprint.ToString();

        if (state.State.DurableRuns.TryGetValue(run.Value, out DurableRunRecord? existing))
        {
            // Same document, so this is the same run being addressed again rather than a second one: a
            // durable run is named by its author and a name addresses one run. The timing is taken from the
            // fresh declaration because it is a runtime cadence rather than part of the run's identity, and
            // the epoch is left exactly where it is — bumping it here would fence out the very attempt that
            // may be executing this pipeline right now.
            if (!string.Equals(existing.GraphFingerprint, declared, StringComparison.Ordinal))
            {
                throw PipelineResumeRefusedException.Mismatched(run.Value, existing.GraphFingerprint, declared);
            }

            // A finished declaration is a record and not a run, so nothing is updated and nothing is
            // written: the cadence of a run that will never take another checkpoint is not a fact worth an
            // ETag. What the caller receives is the epoch the last attempt held, which is what its handle
            // then presents to hear how the run ended.
            if (existing.Outcome is not null)
            {
                return Ticket(document.Id, run, existing.Epoch, fingerprint);
            }

            existing.Interval = declaration.Interval;
            existing.EveryElements = declaration.EveryElements;

            await state.WriteStateAsync();

            return Ticket(document.Id, run, existing.Epoch, fingerprint);
        }

        // Written before the ticket is handed back, for the reason a start's epoch is: a claim handed out
        // before it was recorded would survive a lost activation as a number nobody can reproduce.
        state.State.LastEpoch++;
        state.State.DurableRuns[run.Value] = new DurableRunRecord
        {
            CanonicalDocument = canonicalDocument,
            GraphFingerprint = declared,
            Interval = declaration.Interval,
            EveryElements = declaration.EveryElements,
            Epoch = state.State.LastEpoch,
            Claimed = false,
        };

        await state.WriteStateAsync();

        return Ticket(document.Id, run, state.State.LastEpoch, fingerprint);
    }

    /// <inheritdoc/>
    public async Task<PipelineRunTicket> ReplaceDurableRunAsync(
        byte[] canonicalDocument,
        DurableRunDeclaration declaration)
    {
        ArgumentNullException.ThrowIfNull(canonicalDocument);
        ArgumentNullException.ThrowIfNull(declaration);

        (GraphDocument document, RunId run, GraphFingerprint fingerprint) = Admit(canonicalDocument, declaration);

        // The store is emptied before the register is rewritten, and the order is the whole of this
        // operation's crash story. Cleared-then-not-recorded leaves a run that will restart from the
        // beginning under the document it already had, which its own at-least-once contract already admits;
        // recorded-then-not-cleared would leave a declaration naming one document beside a checkpoint of
        // another, which is the sticky refusal nothing but a second replace could clear. Retrying a replace
        // that failed halfway is therefore always safe: clearing a pair the store no longer holds is a
        // no-op, and rewriting the record is idempotent.
        if (registry.CheckpointStore is { } store &&
            await store.ReadAsync(document.Id, run, CancellationToken.None) is { } stored)
        {
            await store.ClearAsync(document.Id, run, stored.ETag, CancellationToken.None);
        }

        // A fresh epoch, unconditionally, because that is what supersedes whatever was executing under the
        // old declaration: its control calls are fenced from here on and its next capture is refused by the
        // store it no longer has an ETag for. Claimed goes back to false so the first activation of the
        // replacement takes this very number, exactly as a first declaration's does.
        state.State.LastEpoch++;
        state.State.DurableRuns[run.Value] = new DurableRunRecord
        {
            CanonicalDocument = canonicalDocument,
            GraphFingerprint = fingerprint.ToString(),
            Interval = declaration.Interval,
            EveryElements = declaration.EveryElements,
            Epoch = state.State.LastEpoch,
            Claimed = false,
        };

        await state.WriteStateAsync();

        return Ticket(document.Id, run, state.State.LastEpoch, fingerprint);
    }

    /// <inheritdoc/>
    public async Task ReportDurableRunEndedAsync(string runId, RunStatusSnapshot terminal)
    {
        ArgumentNullException.ThrowIfNull(runId);
        ArgumentNullException.ThrowIfNull(terminal);

        if (!RunId.TryCreate(runId, out RunId run))
        {
            throw new ArgumentException(
                $"'{runId}' is not a valid run identifier, so it names no run this coordinator could have declared.",
                nameof(runId));
        }

        if (terminal.Phase is not (RunPhase.Completed or RunPhase.Faulted))
        {
            throw new ArgumentException(
                $"The run '{run}' is reported as having ended in the phase '{terminal.Phase}', and a durable run is finished by completing or by failing and by nothing else. Cancellation in particular is not an ending here: a deactivation cancels the run it was hosting, so recording that as the run being over would retire every durable run its silo recycled.",
                nameof(terminal));
        }

        // An identity with no record is not an error, for the reason a claim of one is not: an ordinary run
        // reports nothing and a declaration a replace has already rewritten belongs to somebody else.
        if (!state.State.DurableRuns.TryGetValue(run.Value, out DurableRunRecord? record))
        {
            return;
        }

        // The same fencing every other epoch-carrying call performs, and it is what stops a superseded
        // attempt from retiring a run that has already been claimed by somebody else. A stale attempt that
        // completes late is precisely the case: its work is finished, its claim is not current, and the run
        // it would be reporting the end of is one another activation is executing.
        if (terminal.Epoch != record.Epoch)
        {
            throw new PipelineFencingException(record.Epoch, terminal.Epoch);
        }

        record.Outcome = terminal.Phase;
        record.FailureType = terminal.FailureType;
        record.FailureMessage = terminal.FailureMessage;

        await state.WriteStateAsync();
    }

    /// <inheritdoc/>
    public async Task<DurableRunClaim?> ClaimDurableRunAsync(string runId)
    {
        ArgumentNullException.ThrowIfNull(runId);

        if (!RunId.TryCreate(runId, out RunId run))
        {
            throw new ArgumentException(
                $"'{runId}' is not a valid run identifier, so it names no run this coordinator could have declared.",
                nameof(runId));
        }

        if (!state.State.DurableRuns.TryGetValue(run.Value, out DurableRunRecord? record))
        {
            return null;
        }

        // A finished run answers with how it ended and costs nothing: no epoch is minted, because an epoch
        // orders claims to ownership and nothing is going to own this, and no state is written, because
        // nothing about the record changed. The checkpoint is left where it is and is simply no longer a
        // reason to run anything.
        if (record.Outcome is { } outcome)
        {
            return new DurableRunClaim
            {
                Epoch = record.Epoch,
                CanonicalDocument = record.CanonicalDocument,
                Interval = record.Interval,
                EveryElements = record.EveryElements,
                Outcome = outcome,
                FailureType = record.FailureType,
                FailureMessage = record.FailureMessage,
            };
        }

        // The first claim takes the epoch the declaration recorded and every later one takes a fresh
        // number. That is not an optimization: the declaring client is holding a ticket carrying the
        // recorded epoch, so minting a second one for the very first attempt would make that ticket stale
        // before the run had produced an element. A later claim is a resume, which is a new claim to the
        // same run, and the attempt it replaces has to stop being current.
        if (record.Claimed)
        {
            state.State.LastEpoch++;
            record.Epoch = state.State.LastEpoch;
        }

        record.Claimed = true;

        await state.WriteStateAsync();

        return new DurableRunClaim
        {
            Epoch = record.Epoch,
            CanonicalDocument = record.CanonicalDocument,
            Interval = record.Interval,
            EveryElements = record.EveryElements,
        };
    }

    /// <inheritdoc/>
    public Task<RunStatusSnapshot> GetStatusAsync(string runId, long epoch) => Run(runId).GetStatusAsync(epoch);

    /// <inheritdoc/>
    public Task ShutdownRunAsync(string runId, long epoch) => Run(runId).ShutdownAsync(epoch);

    /// <inheritdoc/>
    public Task CancelRunAsync(string runId, long epoch) => Run(runId).CancelAsync(epoch);

    /// <summary>Checks everything a durable run has to satisfy before this coordinator records anything.</summary>
    /// <param name="canonicalDocument">The document's canonical bytes.</param>
    /// <param name="declaration">What the run is called and when it checkpoints.</param>
    /// <returns>The decoded document, the run identity, and the fingerprint of the bytes.</returns>
    /// <exception cref="PipelineRejectedException">
    /// The bytes are not a canonical document, the document is another pipeline's, the run identity is not
    /// one this runtime can address, this silo registers no checkpoint store, or the document does not
    /// resolve against this silo's catalog and factories.
    /// </exception>
    /// <remarks>
    /// Shared by declaring and by replacing, and that sharing is the contract rather than a convenience: a
    /// replacement is admitted on exactly the terms a declaration is, so nothing reaches the register through
    /// the destructive door that could not have reached it through the ordinary one.
    /// </remarks>
    private (GraphDocument Document, RunId Run, GraphFingerprint Fingerprint) Admit(
        byte[] canonicalDocument,
        DurableRunDeclaration declaration)
    {
        GraphDocument document = Read(canonicalDocument);
        string pipeline = this.GetPrimaryKeyString();

        if (!GraphId.TryCreate(pipeline, out GraphId addressed) || document.Id != addressed)
        {
            throw new PipelineRejectedException(
                $"The document declares the pipeline '{document.Id}' and this coordinator owns the pipeline '{pipeline}'. A coordinator starts runs of its own pipeline only, because the epochs it issues order claims to that pipeline and nothing else.");
        }

        if (!RunId.TryCreate(declaration.RunId, out RunId run))
        {
            throw new PipelineRejectedException(
                $"'{declaration.RunId}' is not a valid run identifier, so it names no run a checkpoint could be keyed by. A durable run is named by whoever will resume it, and the name has to be one this runtime can address.");
        }

        // Refused here rather than at the first capture, because a deployment that forgot the store would
        // otherwise learn of it from a run that had already performed side effects: the whole point of
        // declaring a run durable is that its position survives, and a silo with nowhere to put a position
        // cannot honor that however well the graph runs.
        if (registry.CheckpointStore is null)
        {
            throw new PipelineRejectedException(
                $"The run '{run}' of the pipeline '{document.Id}' was declared durable and this silo registers no checkpoint store, so there is nowhere for its position to be written. A deployment that runs durable pipelines calls UseCheckpointStore when it adds Orleans.Dataflow; which store stands behind it is the deployment's decision, exactly as the coordinator's own is.");
        }

        Refuse(document);

        return (document, run, GraphFingerprint.OfSerialized(canonicalDocument));
    }

    /// <summary>Composes the key one run grain is addressed by.</summary>
    /// <param name="graph">The pipeline the run belongs to.</param>
    /// <param name="run">The run's identity.</param>
    /// <returns>The composed key.</returns>
    /// <remarks>
    /// A run is a run <em>of</em> a pipeline, so both halves are in the key. The separator is a slash,
    /// which is not a character of the identifier grammar, so the two halves can never be confused for one
    /// and the key can never be ambiguous.
    /// </remarks>
    internal static string RunKey(GraphId graph, RunId run) => $"{graph.Value}/{run.Value}";

    /// <summary>Composes the ticket a caller addresses a run by.</summary>
    /// <param name="graph">The pipeline.</param>
    /// <param name="run">The run's identity.</param>
    /// <param name="epoch">The ownership epoch the run is claimed under.</param>
    /// <param name="fingerprint">The identity of the document this silo read.</param>
    /// <returns>The ticket.</returns>
    private PipelineRunTicket Ticket(GraphId graph, RunId run, long epoch, GraphFingerprint fingerprint) =>
        new()
        {
            GraphId = graph.Value,
            RunId = run.Value,
            Epoch = epoch,
            GraphFingerprint = fingerprint.ToString(),
            CatalogFingerprint = registry.CatalogFingerprint.ToString(),
        };

    /// <summary>Decodes the document a caller sent.</summary>
    /// <param name="canonicalDocument">The bytes.</param>
    /// <returns>The document.</returns>
    /// <exception cref="PipelineRejectedException">The bytes are not a canonical graph document.</exception>
    private static GraphDocument Read(byte[] canonicalDocument)
    {
        try
        {
            return GraphDocumentSerializer.Deserialize(canonicalDocument);
        }
        catch (GraphDocumentFormatException malformed)
        {
            // The inner exception is deliberately dropped rather than attached. Orleans serializes an
            // exception's whole chain, and this one has no codec, so attaching it would replace the
            // diagnosis with a codec error on the caller's side. The message is what carries it across.
            throw new PipelineRejectedException(
                $"The bytes are not the canonical serialization of a graph document: {malformed.Message}");
        }
    }

    /// <summary>Refuses a document this silo cannot validate or cannot build.</summary>
    /// <param name="document">The decoded document.</param>
    /// <exception cref="PipelineRejectedException">
    /// The document does not validate against this silo's catalog, or a provider it names has no
    /// registered runtime factory.
    /// </exception>
    /// <remarks>
    /// The two checks are separate because they fail for different reasons and a deployment fixes them
    /// differently: an unknown stage is a document this silo's vocabulary does not contain, and a missing
    /// factory is a vocabulary this silo published but cannot execute. Both are reported in full — every
    /// diagnostic, every missing provider — because a caller reconciling a document with a deployment
    /// needs the whole list.
    /// </remarks>
    private void Refuse(GraphDocument document)
    {
        GraphValidationReport report = GraphCompiler.Validate(document, registry.Catalog);

        if (!report.IsValid)
        {
            throw new PipelineRejectedException(PipelineMaterializer.Describe(report));
        }

        List<string> unbuildable = [];

        foreach (StageNode node in document.Nodes)
        {
            if (!registry.Factories.TryGetFactory(node.Stage.Provider, out _))
            {
                unbuildable.Add($"'{node.Id}' is an occurrence of '{node.Stage}'");
            }
        }

        if (unbuildable.Count > 0)
        {
            throw new PipelineRejectedException(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"This silo's catalog knows every stage of the document, but it registers no runtime factory for {unbuildable.Count} of its {document.Nodes.Count} nodes, so it could validate the document and not execute it: {string.Join("; ", unbuildable)}. The providers it can build are {(registry.Factories.Providers.Count == 0 ? "none" : string.Join(", ", registry.Factories.Providers))}."));
        }
    }

    /// <summary>Addresses one run of this pipeline.</summary>
    /// <param name="runId">The run identity from its ticket.</param>
    /// <returns>The run grain.</returns>
    /// <exception cref="ArgumentException"><paramref name="runId"/> is not a valid run identifier.</exception>
    private IPipelineRunGrain Run(string runId)
    {
        ArgumentNullException.ThrowIfNull(runId);

        if (!RunId.TryCreate(runId, out RunId run))
        {
            throw new ArgumentException(
                $"'{runId}' is not a valid run identifier, so it names no run this coordinator could have started.",
                nameof(runId));
        }

        return GrainFactory.GetGrain<IPipelineRunGrain>(RunKey(GraphId.Create(this.GetPrimaryKeyString()), run));
    }
}
