using System.Globalization;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Grains;

/// <summary>
/// The run grain: one activation hosting one execution of the local engine.
/// </summary>
/// <remarks>
/// <para>
/// Everything this grain owns lives for the length of one run and is held in fields rather than in
/// storage. That is the phase-1 durability contract stated as code: the run is in memory, so losing the
/// activation loses the attempt, and nothing here pretends otherwise by writing a progress record it could
/// not honor.
/// </para>
/// <para>
/// No method waits for the run. The engine executes on dedicated threads of its own, so the activation's
/// turn is free the moment a call has done its bookkeeping — which is what makes a status poll answer
/// during a long run and what keeps a graceful stop from parking a turn on a drain of unbounded length.
/// </para>
/// </remarks>
internal sealed class PipelineRunGrain(DataflowSiloRegistry registry) : Grain, IPipelineRunGrain
{
    private LocalRun? _run;
    private long _epoch;
    private GraphFingerprint _fingerprint;
    private IReadOnlyList<ResultSlotId> _slots = [];

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
    public Task<RunStatusSnapshot> GetStatusAsync(long epoch)
    {
        if (_run is not { } run)
        {
            return Task.FromResult(new RunStatusSnapshot { Phase = RunPhase.NotStarted });
        }

        Fence(epoch);

        return Task.FromResult(Describe(run, _epoch));
    }

    /// <inheritdoc/>
    public Task<ResultEnvelope> GetResultAsync(long epoch, string slotName, string graphFingerprint)
    {
        ArgumentNullException.ThrowIfNull(slotName);
        ArgumentNullException.ThrowIfNull(graphFingerprint);

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
            envelope.HasValue = true;
            envelope.Value = resolved.Result;
        }

        return Task.FromResult(envelope);
    }

    /// <inheritdoc/>
    public Task ShutdownAsync(long epoch)
    {
        LocalRun run = Active(epoch);

        // Requested and not awaited. The returned task reports only that the run has stopped, never how it
        // ended, so nothing is lost by not observing it; how it ended is on the completion task, which is
        // what a caller polls. Awaiting a drain here would park this activation for as long as the
        // downstream of the graph takes.
        _ = Stopping(run.ShutdownAsync());

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task CancelAsync(long epoch)
    {
        LocalRun run = Active(epoch);

        _ = Stopping(run.DisposeAsync());

        return Task.CompletedTask;
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
