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
/// <b>M5.4 adds the one thing a checkpoint could never say: that the run is over.</b> An attempt started
/// here reports the terminal state it reaches to its coordinator, which writes it onto the declaration — so
/// a later activation of a finished run is answered "it completed" or "it failed, with this" instead of
/// being handed a document and a position to continue. Completing and failing are endings and cancelling is
/// not, because this grain's own deactivation cancels the run it hosts.
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
    private Exception? _refused;
    private long _refusedUnder;
    private RunStatusSnapshot? _ended;
    private Task? _reporting;
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
    public async Task<long> EnsureStartedAsync(long declaredEpoch)
    {
        // The declaration this call is driving is compared against whatever this activation is already
        // holding, and a newer one supersedes it. Ordinarily nothing is newer — a second declaration of a
        // live run leaves the epoch exactly where it was, so a running attempt answers with its own number
        // and is not disturbed — and the one operation that does mint a fresher number is a replacement,
        // which is destructive by definition. So the same comparison serves both: abandon what is here and
        // take up what the register now says.
        if (_run is { } hosted)
        {
            if (declaredEpoch <= _epoch)
            {
                return _epoch;
            }

            await AbandonAsync(hosted);
        }
        else if (_refused is { } refusal && declaredEpoch <= _refusedUnder)
        {
            // The same declaration asking the same question gets the same answer without another claim,
            // which is what keeps a client that retries from minting an epoch per attempt. A newer
            // declaration is a different question and falls through to be asked afresh.
            throw refusal;
        }
        else if (_ended is { } ended && declaredEpoch <= _epoch)
        {
            return ended.Epoch;
        }

        _refused = null;
        _ended = null;
        _reporting = null;

        // Named separately from "no declaration", because the two are different deployment mistakes and a
        // caller fixes them differently. A cluster whose silos do not all register the same store accepts a
        // declaration on one of them and cannot host the run on another — the same deployment-scoped honesty
        // the binding registry has carried since phase 2, reachable one grain further away.
        if (registry.CheckpointStore is not { } store)
        {
            throw new PipelineRejectedException(
                $"The run '{this.GetPrimaryKeyString()}' was declared durable and the silo hosting it registers no checkpoint store, so it has nowhere to write a position. Every silo that may host a durable run calls UseCheckpointStore, and over the same store: a cluster whose silos disagree about that accepts a declaration on one host and cannot honor it on another.");
        }

        // Re-read rather than trusted, because a replacement clears the store after this activation read it
        // at start-up: continuing from a position the register no longer describes is precisely what the
        // destructive spelling exists to prevent.
        if (!_graph.IsDefault && !_identity.IsDefault)
        {
            _stored = await store.ReadAsync(_graph, _identity, CancellationToken.None);
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

        // A run whose declaration says it is over answers with how it ended, and answers for as long as this
        // activation lives. Nothing is running, so there is no attempt to describe; what there is, is the
        // fact its last attempt wrote down before it went away.
        if (_ended is { } ended)
        {
            Fence(epoch);

            return ended;
        }

        if (_run is not { } run)
        {
            return new RunStatusSnapshot { Phase = RunPhase.NotStarted };
        }

        Fence(epoch);

        RunStatusSnapshot status = Describe(run, _epoch);

        // Awaited before the answer leaves, so that a caller which has seen a durable run end has seen a run
        // the register already records as ended. Without that ordering a client could observe completion,
        // recycle the grain, declare the name again, and be handed a resume of the very run it had just
        // watched finish — which is the exact hole this milestone closes. It is one call, once per run, on
        // the poll that observes the ending; every other poll awaits an already-settled task.
        await ReportEndedAsync(status);

        return status;
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
        RunStatusSnapshot snapshot = new()
        {
            Epoch = epoch,
            DroppedElements = run.DroppedElements,
            SupervisedFailures = run.SupervisedFailures,
            PoisonElements = run.PoisonElements,
            Checkpoints = run.Checkpoints,
            TotalCheckpointHold = run.CheckpointHold,
        };

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

        if (_run is not null || _ended is not null || _stored is null)
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

        if (await Coordinator().ClaimDurableRunAsync(_identity.Value) is not { } claim)
        {
            return null;
        }

        // The half a checkpoint cannot carry. A stored position says where the run reached and never whether
        // it is over, so a run that completed and then lost its activation used to be continued and re-run
        // its tail; the register now says which of the two it is, and a finished run is reported rather than
        // materialized. The checkpoint is deliberately still there — a cleared one would take the forensic
        // trail with it — and it is simply no longer a reason to start anything.
        if (claim.Outcome is { } outcome)
        {
            _epoch = claim.Epoch;
            _ended = new RunStatusSnapshot
            {
                Phase = outcome,
                Epoch = claim.Epoch,
                FailureType = claim.FailureType,
                FailureMessage = claim.FailureMessage,
            };

            return claim.Epoch;
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
                return Refusing(
                    new PipelineResumeRefusedException(
                        $"The checkpoint stored for the run '{_identity}' of the graph '{_graph}' is not one this runtime can read, so there is nothing it can continue: {string.Join("; ", violations)}."),
                    claim.Epoch);
            }

            if (checkpoint!.Graph != fingerprint)
            {
                return Refusing(
                    PipelineResumeRefusedException.Mismatched(
                        _identity.Value,
                        checkpoint.Graph.ToString(),
                        fingerprint.ToString()),
                    claim.Epoch);
            }

            if (checkpoint.Revision != document.Revision)
            {
                return Refusing(
                    new PipelineResumeRefusedException(
                        string.Create(
                            CultureInfo.InvariantCulture,
                            $"The checkpoint stored for the run '{_identity}' was taken at revision {checkpoint.Revision} and the document this cluster holds for it is revision {document.Revision}. A resume continues the same revision; cross-revision migration is a recorded deferral rather than a silent best effort. Replace the run to start the new revision from the beginning, or run it under a run identity of its own."))
                    {
                        StoredFingerprint = checkpoint.Graph.ToString(),
                        DeclaredFingerprint = fingerprint.ToString(),
                    },
                    claim.Epoch);
            }
        }

        LocalRun started;

        try
        {
            started = PipelineMaterializer.StartDurable(
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
            // The M3 catalog discipline, run again at resume time and on this host's own vocabulary. A
            // rolling upgrade is what makes it bite: the silo that survives a death may publish a catalog
            // that cannot resolve the very document the dead one was running, and the honest answer is to
            // refuse by name and leave everything where it is. The declaration stays, the checkpoint stays,
            // and a later activation on a silo that can resolve the document continues the run.
            //
            // Remembered rather than thrown from here, for the reason every other refusal on this path is:
            // a poll that re-asked would claim a fresh epoch each time it was answered. The inner exception
            // is dropped because a refusal has to survive the hop, and an exception chain is only as
            // serializable as its least prepared link.
            return Refusing(new PipelineRejectedException(refusal.Message), claim.Epoch);
        }

        _run = started;
        _epoch = claim.Epoch;
        _fingerprint = fingerprint;
        _slots = [.. document.ResultSlots.Select(static slot => slot.Id)];

        // Started here, on a grain turn, so that everything after the wait is a grain turn too: an await in
        // grain code keeps the activation's scheduler, which is what lets the report be an ordinary call from
        // this grain to its coordinator rather than a message posted from an engine thread.
        _ = WatchAsync(started, claim.Epoch);

        return claim.Epoch;
    }

    /// <summary>Remembers a refusal this activation will answer with until a newer declaration arrives.</summary>
    /// <param name="refusal">What is wrong, in the words a caller acts on.</param>
    /// <param name="epoch">The epoch the claim that produced it returned.</param>
    /// <returns>That same epoch, so a caller of the resume path has one to return.</returns>
    /// <remarks>
    /// The epoch is recorded beside the refusal because it is what makes a retry cheap and a replacement
    /// effective: a caller presenting the same declaration hears the same sentence with no coordinator call
    /// at all, and a caller presenting a newer one — which only a replacement mints — gets the question
    /// asked again.
    /// </remarks>
    private long Refusing(Exception refusal, long epoch)
    {
        _refused = refusal;
        _refusedUnder = epoch;

        return epoch;
    }

    /// <summary>Watches one attempt to its end so that its ending is reported even if nobody polls.</summary>
    /// <param name="run">The attempt this activation started.</param>
    /// <param name="epoch">The epoch it owns the run under, which is what the report is fenced by.</param>
    /// <returns>A task that completes once the ending has been reported or found not worth reporting.</returns>
    /// <remarks>
    /// <para>
    /// The second of the two paths that report an ending, and the one that needs no client. A run whose
    /// declaring client has gone away still ends, and a run that ends unreported is one the next activation
    /// continues — so the poll path's ordering guarantee is not enough on its own and this covers the rest.
    /// </para>
    /// <para>
    /// <b>The wait keeps the grain's context, because grain code always does.</b> This is started from a grain
    /// turn, so everything after the await is a turn too, and the report is an ordinary call from this grain
    /// to its coordinator rather than a message posted from an engine thread.
    /// </para>
    /// </remarks>
    private async Task WatchAsync(LocalRun run, long epoch)
    {
        try
        {
            await run.Completion;
        }
        catch (Exception)
        {
            // How the run ended is read from the task below rather than from what it threw.
        }

        await ReportEndedAsync(Describe(run, epoch));
    }

    /// <summary>Tells the coordinator that this run has ended, once per activation at most.</summary>
    /// <param name="terminal">The snapshot this activation read of its own attempt.</param>
    /// <returns>A task that completes when the report has been delivered, or failed and been dropped.</returns>
    /// <remarks>
    /// <para>
    /// <b>The half a checkpoint cannot write.</b> A checkpoint says where a run reached; whether it is over is
    /// a claim, and claims live in the coordinator's register. Without this, a durable run that completed and
    /// then lost its activation was indistinguishable from one that crashed at the same position, so the next
    /// activation continued it and ran its tail a second time.
    /// </para>
    /// <para>
    /// <b>Only completing and failing are reported.</b> A cancellation is not an ending: this activation's own
    /// deactivation cancels the run it was hosting, so reporting that would retire a durable run every time
    /// its silo recycled — which is the behaviour durability exists to prevent.
    /// </para>
    /// <para>
    /// <b>One task, shared by both paths.</b> The watcher and the status poll both arrive here and both await
    /// the same call, which is what makes "a caller that saw the run end saw a run recorded as ended" true
    /// regardless of which of the two got here first.
    /// </para>
    /// <para>
    /// <b>It is awaited and its failure is dropped, and both are deliberate.</b> The call is awaited because
    /// the coordinator writes its register and calls nobody, so this edge cannot close a cycle — the shape
    /// argument the claim rests on, unchanged. The failure is dropped because nothing here can act on it: a
    /// fencing refusal means a fresher attempt owns the run and this ending is not the run's, and an
    /// unreachable coordinator leaves the declaration exactly as it was, which is where this milestone found
    /// it. A report that failed is not retried on this activation, so what such a run costs is one resume by
    /// a later one — the behaviour of the milestone before this, rather than a new failure.
    /// </para>
    /// </remarks>
    private Task ReportEndedAsync(RunStatusSnapshot terminal)
    {
        if (terminal.Phase is not (RunPhase.Completed or RunPhase.Faulted) ||
            _graph.IsDefault ||
            _identity.IsDefault)
        {
            return Task.CompletedTask;
        }

        return _reporting ??= ReportingAsync(terminal);
    }

    /// <summary>Delivers one ending to the coordinator and swallows whatever comes back.</summary>
    /// <param name="terminal">The terminal snapshot.</param>
    /// <returns>A task that never faults.</returns>
    private async Task ReportingAsync(RunStatusSnapshot terminal)
    {
        try
        {
            await Coordinator().ReportDurableRunEndedAsync(_identity.Value, terminal);
        }
        catch (Exception)
        {
            // Reported in the remarks of the caller: nothing here can act on a report that did not land.
        }
    }

    /// <summary>Stops the attempt this activation is hosting, because a newer declaration has superseded it.</summary>
    /// <param name="run">The attempt.</param>
    /// <returns>A task that completes when its engine has released everything it held.</returns>
    /// <remarks>
    /// Awaited rather than requested, which is the opposite of what a cancellation does and is right for the
    /// opposite reason: a replacement is about to start a second engine over the same run identity, and two
    /// of them writing one checkpoint key is a race the ETag would resolve by killing one of them. The wait
    /// is the deactivation path's own, and bounded by the same thing.
    /// </remarks>
    private async Task AbandonAsync(LocalRun run)
    {
        _run = null;

        await run.DisposeAsync();
    }

    /// <summary>Addresses the coordinator of the pipeline this run belongs to.</summary>
    /// <returns>The coordinator grain.</returns>
    private IPipelineCoordinatorGrain Coordinator() =>
        GrainFactory.GetGrain<IPipelineCoordinatorGrain>(_graph.Value);

    /// <summary>Reports the refusal this activation is holding, if it is holding one.</summary>
    /// <exception cref="PipelineResumeRefusedException">
    /// The stored checkpoint is not one this run's document describes.
    /// </exception>
    /// <exception cref="PipelineRejectedException">
    /// This silo's catalog cannot resolve the document the run is a run of.
    /// </exception>
    /// <remarks>
    /// Two kinds of refusal reach here and both are answers about the run rather than about the call, which
    /// is why they are remembered: a document that is not what the checkpoint describes, and a document this
    /// host cannot build. A caller fixes them differently — reconcile the document, or reach a silo that
    /// publishes the vocabulary — so they stay two exception types and are not folded into one.
    /// </remarks>
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
            // A finished durable run is told apart from a lost attempt, because they send a caller to
            // different places. "The run ended and its results went with the activation that produced them"
            // is a fact about this run's history; "no run is active here" is a fact about this grain.
            throw _ended is { } ended
                ? new PipelineRunLostException(
                    $"The durable run '{this.GetPrimaryKeyString()}' ended in the phase '{ended.Phase}' and its declaration records that, so nothing is executing here to answer for it. A run's results live only as long as the activation that produced them; what survives a finished durable run is how it ended and the checkpoint it stopped at.")
                : new PipelineRunLostException(
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
