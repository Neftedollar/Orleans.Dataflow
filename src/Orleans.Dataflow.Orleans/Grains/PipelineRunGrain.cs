using System.Globalization;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;
using Orleans.Serialization;

namespace Orleans.Dataflow.Grains;

/// <summary>
/// The run grain: one activation hosting one execution of the local engine.
/// </summary>
/// <remarks>
/// <para>
/// Everything this grain owns lives for the length of one attempt and is held in fields rather than in
/// storage. For an ordinary run that is the whole durability contract stated as code: the run is in memory,
/// so losing the activation loses the attempt, and nothing here pretends otherwise by writing a progress
/// record it could not honor.
/// </para>
/// <para>
/// <b>A durable run lifts exactly that limit and nothing else.</b> Since M5.3 an activation whose
/// checkpoint store holds a position for this run does not report an absent run: it claims a fresh epoch
/// from the coordinator, materializes from the checkpoint, and reports itself running. The lift is
/// activation-driven and there is no second protocol — nothing is pushed at this grain, nothing polls on
/// its behalf, and the client's own status poll is what brings the activation into being. A durable run
/// that has not yet written a checkpoint is unchanged: the attempt is lost, and saying so is still the only
/// honest answer, because there is no position to continue from.
/// </para>
/// <para>
/// No method waits for the run. The engine executes on dedicated threads of its own, so the activation's
/// turn is free the moment a call has done its bookkeeping — which is what makes a status poll answer
/// during a long run and what keeps a graceful stop from parking a turn on a drain of unbounded length. The
/// one wait a call may now perform is the resume itself: reading a store and claiming an epoch, both
/// bounded, both once per activation.
/// </para>
/// </remarks>
internal sealed class PipelineRunGrain(DataflowSiloRegistry registry, Serializer serializer)
    : Grain, IPipelineRunGrain
{
    private LocalRun? _run;
    private long _epoch;
    private GraphFingerprint _fingerprint;
    private IReadOnlyList<ResultSlotId> _slots = [];
    private StoredCheckpoint? _stored;
    private PipelineResumeRefusedException? _refused;
    private GraphId _graph;
    private RunId _identity;

    /// <inheritdoc/>
    /// <remarks>
    /// The store is read here and the coordinator is not called here, which is the difference between a
    /// resume that works and a deadlock. A store is a service of this silo, so reading it is an ordinary
    /// await; a coordinator is a grain, and a grain call issued from an activation's first turn waits behind
    /// whatever message caused the activation. Reading the position at activation and claiming the epoch on
    /// the first call keeps every grain-to-grain edge on a turn that something else is already driving.
    /// </remarks>
    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        (_graph, _identity) = Address();

        if (registry.CheckpointStore is { } store && !_graph.IsDefault && !_identity.IsDefault)
        {
            _stored = await store.ReadAsync(_graph, _identity, cancellationToken);
        }

        await base.OnActivateAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public Task StartAsync(byte[] canonicalDocument, long epoch)
    {
        ArgumentNullException.ThrowIfNull(canonicalDocument);

        if (_run is not null)
        {
            throw new PipelineFencingException(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"The run '{this.GetPrimaryKeyString()}' is already active under the ownership epoch {_epoch}, and a start carrying the epoch {epoch} claims it again. A run identity is used once; a second run of a pipeline is a second identity."));
        }

        GraphDocument document = Read(canonicalDocument);

        try
        {
            _run = PipelineMaterializer.Start(
                document,
                GraphFingerprint.OfSerialized(canonicalDocument),
                registry.Catalog,
                registry.Factories,
                this.GetPrimaryKeyString(),
                CancellationToken.None);
        }
        catch (InvalidOperationException refusal)
        {
            // The inner exception is dropped for the reason the malformed-document path drops its own: a
            // refusal has to survive the hop, and an exception chain is only as serializable as its least
            // prepared link.
            throw new PipelineRejectedException(refusal.Message);
        }

        _epoch = epoch;
        _fingerprint = GraphFingerprint.OfSerialized(canonicalDocument);
        _slots = [.. document.ResultSlots.Select(static slot => slot.Id)];

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public async Task<long> EnsureStartedAsync()
    {
        Refused();

        if (_run is not null)
        {
            return _epoch;
        }

        // Named separately from "no declaration", because the two are different deployment mistakes and a
        // caller fixes them differently. A cluster whose silos do not all register the same store accepts a
        // declaration on one of them and cannot host the run on another — the same deployment-scoped honesty
        // the binding registry has carried since phase 2, reachable one grain further away.
        if (registry.CheckpointStore is null)
        {
            throw new PipelineRejectedException(
                $"The run '{this.GetPrimaryKeyString()}' was declared durable and the silo hosting it registers no checkpoint store, so it has nowhere to write a position. Every silo that may host a durable run calls UseCheckpointStore, and over the same store: a cluster whose silos disagree about that accepts a declaration on one host and cannot honor it on another.");
        }

        if (await StartOrResumeAsync() is not { } epoch)
        {
            throw new PipelineRunLostException(
                $"The grain '{this.GetPrimaryKeyString()}' was asked to start a durable run and its coordinator has no durable declaration under that identity. A durable run is declared before it is started, and a declaration that is gone is a run nothing can continue.");
        }

        Refused();

        return epoch;
    }

    /// <inheritdoc/>
    public async Task<RunStatusSnapshot> GetStatusAsync(long epoch)
    {
        await AdoptAsync();

        if (_run is not { } run)
        {
            return new RunStatusSnapshot { Phase = RunPhase.NotStarted };
        }

        Fence(epoch);

        return Describe(run, _epoch);
    }

    /// <inheritdoc/>
    public async Task<ResultEnvelope> GetResultAsync(long epoch, string slotName, string graphFingerprint)
    {
        ArgumentNullException.ThrowIfNull(slotName);
        ArgumentNullException.ThrowIfNull(graphFingerprint);

        await AdoptAsync();

        LocalRun run = Active(epoch);

        if (!GraphFingerprint.TryParse(graphFingerprint, out GraphFingerprint declaring) ||
            declaring != _fingerprint)
        {
            throw new ArgumentException(
                $"The slot '{slotName}' was declared by the document {graphFingerprint}, and this is a run of {_fingerprint}. A slot resolves only against a run of the document that declared it.",
                nameof(graphFingerprint));
        }

        if (!ResultSlotId.TryCreate(slotName, out ResultSlotId slot) || run.Result(slot) is not { } resolved)
        {
            throw new ArgumentException(
                _slots.Count == 0
                    ? $"The document of this run declares no results at all, so it declares none named '{slotName}'."
                    : $"The document of this run declares no result named '{slotName}'. The results it declares are {string.Join(", ", _slots.Select(static declared => $"'{declared}'"))}.",
                nameof(slotName));
        }

        RunStatusSnapshot status = Describe(run, _epoch);
        ResultEnvelope envelope = new()
        {
            Phase = status.Phase,
            FailureType = status.FailureType,
            FailureMessage = status.FailureMessage,
        };

        // The run's own contract is that every result is settled before the completion task transitions,
        // so a settled completion means a settled slot and this read never waits. A slot task that is
        // somehow not yet settled is reported as "no value" rather than awaited, because a grain turn is
        // not the place to discover that an invariant moved.
        if (status.Phase is RunPhase.Completed && resolved.IsCompletedSuccessfully)
        {
            // The cap is enforced here, at envelope creation, and it refuses the slot rather than the run.
            // The run has already ended successfully; reading one of its results is not an event in its
            // life, so an oversized result leaves the run completed, leaves its other results resolvable,
            // and fails this read by name. Measuring before the value is put on the envelope is what keeps
            // the bytes from crossing the wire at all, which is the whole point of having a bound.
            long bytes = ResultSizeMeter.Measure(serializer, resolved.Result);

            if (bytes > registry.MaximumResultBytes)
            {
                throw new ResultTooLargeException(slotName, bytes, registry.MaximumResultBytes);
            }

            envelope.HasValue = true;
            envelope.Value = resolved.Result;
        }

        return envelope;
    }

    /// <inheritdoc/>
    public async Task ShutdownAsync(long epoch)
    {
        await AdoptAsync();

        LocalRun run = Active(epoch);

        // Requested and not awaited. The returned task reports only that the run has stopped, never how it
        // ended, so nothing is lost by not observing it; how it ended is on the completion task, which is
        // what a caller polls. Awaiting a drain here would park this activation for as long as the
        // downstream of the graph takes.
        _ = Stopping(run.ShutdownAsync());
    }

    /// <inheritdoc/>
    public async Task CancelAsync(long epoch)
    {
        await AdoptAsync();

        LocalRun run = Active(epoch);

        _ = Stopping(run.DisposeAsync());
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The run is abandoned rather than drained. A deactivation is not a graceful stop — the activation is
    /// going away whether or not the stream finishes — and pretending otherwise would resolve results from
    /// a run nobody can read afterwards. Cancelling releases the engine's threads, its channels, and
    /// whatever an author's sink holds open, which is the one obligation a departing activation does have.
    /// </remarks>
    public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        if (_run is { } run)
        {
            _run = null;

            await run.DisposeAsync();
        }

        await base.OnDeactivateAsync(reason, cancellationToken);
    }

    /// <summary>Waits for a stop request off the activation's turn, so that nothing is left unobserved.</summary>
    /// <param name="stopping">The task a stop request returned.</param>
    /// <returns>A task that completes when the run has stopped.</returns>
    /// <remarks>
    /// <para>
    /// The point is not the waiting, which nothing reads, but the observing: a task nobody awaits and that
    /// somehow faults would surface much later as an unobserved exception on a thread with no context. The
    /// two stop requests are documented never to report an outcome, so a failure here would be a runtime
    /// invariant that moved, and swallowing it is correct exactly because the run's own completion task is
    /// where every outcome including that one is reported.
    /// </para>
    /// <para>
    /// The wait keeps the grain's context, because grain code always does: leaving it is what breaks the
    /// single-threaded execution an activation guarantees, and Orleans' own analyzer refuses the attempt.
    /// The continuation costs one turn that does nothing, which is the correct price.
    /// </para>
    /// </remarks>
    private static async Task Stopping(ValueTask stopping)
    {
        try
        {
            await stopping;
        }
        catch (Exception)
        {
            // How a run ended is reported by its completion task and never by a request to stop it.
        }
    }

    /// <summary>Reads a run's terminal state, or reports that it has none yet.</summary>
    /// <param name="run">The active run.</param>
    /// <param name="epoch">The ownership epoch it was started with.</param>
    /// <returns>The snapshot.</returns>
    /// <remarks>
    /// The completion task's status is read rather than awaited, so this is a reading of a moment and
    /// costs nothing. A faulted run's exception is unwrapped from its aggregate: what a caller wants is
    /// what the author's code threw, and the aggregate is an artifact of how a task carries it.
    /// </remarks>
    private static RunStatusSnapshot Describe(LocalRun run, long epoch)
    {
        Task completion = run.Completion;
        RunStatusSnapshot snapshot = new() { Epoch = epoch };

        if (!completion.IsCompleted)
        {
            snapshot.Phase = RunPhase.Running;

            return snapshot;
        }

        if (completion.IsCanceled)
        {
            snapshot.Phase = RunPhase.Canceled;

            return snapshot;
        }

        if (completion.Exception is { } failure)
        {
            Exception thrown = failure.InnerExceptions.Count == 1 ? failure.InnerExceptions[0] : failure;

            snapshot.Phase = RunPhase.Faulted;
            snapshot.FailureType = thrown.GetType().FullName;
            snapshot.FailureMessage = thrown.Message;

            return snapshot;
        }

        snapshot.Phase = RunPhase.Completed;

        return snapshot;
    }

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

    /// <summary>Continues a durable run this activation found a checkpoint for, if there is one.</summary>
    /// <returns>A task that completes once this activation is hosting whatever it can host.</returns>
    /// <remarks>
    /// <para>
    /// Called at the top of every call that answers for a run, and it is the whole of activation-driven
    /// resume. The gate is deliberately the <em>checkpoint</em> and not the coordinator's register: a run
    /// with a stored position is a run there is something to continue, and a run without one — durable or
    /// not — is a lost attempt exactly as it was before this existed. That keeps the cost of the lift at one
    /// store read per activation on a silo that registers a store, and at nothing at all on a silo that does
    /// not.
    /// </para>
    /// <para>
    /// It does nothing at all once the run is here, which is what makes it safe to call from five places:
    /// the second call finds <c>_run</c> and returns without touching a store or a coordinator.
    /// </para>
    /// </remarks>
    private async Task AdoptAsync()
    {
        Refused();

        if (_run is not null || _stored is null)
        {
            return;
        }

        _ = await StartOrResumeAsync();

        Refused();
    }

    /// <summary>Claims this run from its coordinator and starts it, from a checkpoint when there is one.</summary>
    /// <returns>
    /// The epoch this activation now owns the run under, or <see langword="null"/> when the coordinator has
    /// no durable declaration for it.
    /// </returns>
    /// <remarks>
    /// <para>
    /// <b>One path for a first start and for a resume</b>, which is what "no new protocol" means in code.
    /// The claim is the same call, the document comes from the same record, and the only difference is
    /// whether a checkpoint was found — in which case its values reach the plan's seams before the first
    /// element and its ETag is what the first capture presents, so a stale attempt still writing loses to
    /// this one exactly as a superseded coordinator does.
    /// </para>
    /// <para>
    /// A refusal is remembered rather than thrown from here, because the callers that reach this are
    /// answering an ordinary poll and a run whose document no longer matches its checkpoint is a fact about
    /// the run rather than about the poll. Every later call reports the same refusal, so a caller that
    /// retries reads the same sentence rather than a different one.
    /// </para>
    /// </remarks>
    private async Task<long?> StartOrResumeAsync()
    {
        if (registry.CheckpointStore is not { } store || _graph.IsDefault || _identity.IsDefault)
        {
            return null;
        }

        if (await GrainFactory
            .GetGrain<IPipelineCoordinatorGrain>(_graph.Value)
            .ClaimDurableRunAsync(_identity.Value) is not { } claim)
        {
            return null;
        }

        GraphDocument document = Read(claim.CanonicalDocument);
        GraphFingerprint fingerprint = GraphFingerprint.OfSerialized(claim.CanonicalDocument);
        LocalCheckpoint? checkpoint = null;

        if (_stored is { } stored)
        {
            if (!LocalCheckpointDocument.TryRead(
                stored.Document,
                out checkpoint,
                out IReadOnlyList<string> violations))
            {
                _refused = new PipelineResumeRefusedException(
                    $"The checkpoint stored for the run '{_identity}' of the graph '{_graph}' is not one this runtime can read, so there is nothing it can continue: {string.Join("; ", violations)}.");

                return claim.Epoch;
            }

            if (checkpoint!.Graph != fingerprint)
            {
                _refused = PipelineResumeRefusedException.Mismatched(
                    _identity.Value,
                    checkpoint.Graph.ToString(),
                    fingerprint.ToString());

                return claim.Epoch;
            }

            if (checkpoint.Revision != document.Revision)
            {
                _refused = new PipelineResumeRefusedException(
                    string.Create(
                        CultureInfo.InvariantCulture,
                        $"The checkpoint stored for the run '{_identity}' was taken at revision {checkpoint.Revision} and the document this cluster holds for it is revision {document.Revision}. A resume continues the same revision; cross-revision migration is a recorded deferral rather than a silent best effort."))
                {
                    StoredFingerprint = checkpoint.Graph.ToString(),
                    DeclaredFingerprint = fingerprint.ToString(),
                };

                return claim.Epoch;
            }
        }

        try
        {
            _run = PipelineMaterializer.StartDurable(
                document,
                fingerprint,
                registry.Catalog,
                registry.Factories,
                this.GetPrimaryKeyString(),
                new DurableRunOptions
                {
                    Store = store,
                    Run = _identity,
                    Interval = claim.Interval,
                    EveryElements = claim.EveryElements,
                },
                checkpoint,
                _stored?.ETag,
                CancellationToken.None);
        }
        catch (InvalidOperationException refusal)
        {
            // The inner exception is dropped for the reason every refusal here drops one: a refusal has to
            // survive the hop, and an exception chain is only as serializable as its least prepared link.
            throw new PipelineRejectedException(refusal.Message);
        }

        _epoch = claim.Epoch;
        _fingerprint = fingerprint;
        _slots = [.. document.ResultSlots.Select(static slot => slot.Id)];

        return claim.Epoch;
    }

    /// <summary>Reports the refusal this activation is holding, if it is holding one.</summary>
    /// <exception cref="PipelineResumeRefusedException">A resume was refused on this activation.</exception>
    private void Refused()
    {
        if (_refused is { } refusal)
        {
            throw refusal;
        }
    }

    /// <summary>Reads this grain's own key back into the two identities a checkpoint is addressed by.</summary>
    /// <returns>
    /// The graph and the run, or the default values when the key is not one this package composed.
    /// </returns>
    /// <remarks>
    /// The inverse of <c>PipelineCoordinatorGrain.RunKey</c>, and it is total rather than throwing: a key a
    /// hand-written caller composed is not a reason for an activation to fail, it is a reason for that
    /// activation to have no durable run — and the calls it then answers refuse for their own reasons. The
    /// separator is a slash, which the identifier grammar does not contain, so the split is unambiguous.
    /// </remarks>
    private (GraphId Graph, RunId Run) Address()
    {
        string key = this.GetPrimaryKeyString();
        int separator = key.IndexOf('/', StringComparison.Ordinal);

        if (separator <= 0 || separator == key.Length - 1)
        {
            return (default, default);
        }

        return GraphId.TryCreate(key[..separator], out GraphId graph) &&
            RunId.TryCreate(key[(separator + 1)..], out RunId run)
            ? (graph, run)
            : (default, default);
    }

    /// <summary>Returns the active run, refusing a call that does not own it.</summary>
    /// <param name="epoch">The epoch the call carried.</param>
    /// <returns>The run.</returns>
    /// <exception cref="PipelineRunLostException">No run is active in this grain.</exception>
    /// <exception cref="PipelineFencingException">A run is active and the epoch is not its.</exception>
    /// <remarks>
    /// The two refusals are different questions and are answered separately. "There is no run here" is
    /// about existence and sends a caller to the run's history; "your epoch is not this run's" is about
    /// ownership and sends them to their ticket. Folding both into a fencing refusal with a zero epoch
    /// would make every lost attempt look like a stale claim.
    /// </remarks>
    private LocalRun Active(long epoch)
    {
        if (_run is not { } run)
        {
            throw new PipelineRunLostException(
                $"No run is active in the grain '{this.GetPrimaryKeyString()}'. Either it was never started, or the activation hosting it was recycled while it was running; phase 1 does not resume a run across a deactivation, and a run's results live only as long as its activation.");
        }

        Fence(epoch);

        return run;
    }

    /// <summary>Refuses a call whose epoch is not this run's.</summary>
    /// <param name="epoch">The epoch the call carried.</param>
    /// <exception cref="PipelineFencingException">The epoch is not this run's.</exception>
    private void Fence(long epoch)
    {
        if (epoch != _epoch)
        {
            throw new PipelineFencingException(_epoch, epoch);
        }
    }
}
