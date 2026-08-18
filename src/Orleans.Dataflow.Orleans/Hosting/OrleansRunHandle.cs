using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;
using Orleans.Runtime;

namespace Orleans.Dataflow.Hosting;

/// <summary>
/// The control surface of one run executing in a cluster: how it ends, what it produced, and how to stop
/// it.
/// </summary>
/// <remarks>
/// <para>
/// The remote counterpart of <see cref="RunHandle"/>, and the same vocabulary on purpose: a run completes,
/// a run can be shut down gracefully or cancelled, and results are read by slot. What the network changes
/// is not the meaning of any of those but how faithfully they can be reported — an exception arrives as
/// its type name and message rather than as the instance a stage threw, and completion is observed within
/// one poll interval rather than the moment it happens. Both are stated rather than papered over.
/// </para>
/// <para>
/// A handle is the run and not the pipeline. Materializing one pipeline twice yields two runs with two
/// identities and two epochs, both alive at once, and each handle answers only for its own.
/// </para>
/// <para>
/// <b>There is no pause on this handle, and that is a recorded decision rather than a hole.</b> The engine
/// a grain hosts pauses — checkpoint capture uses that very machinery — so the gap is not network-imposed;
/// what is missing is the design a remote pause owes: an epoch-fenced pause/resume protocol, a decided
/// answer to what a pause means across an activation death (is a resumed durable run born paused?), and a
/// poll-honest <c>IsPaused</c> reading. Deferring it whole was chosen over shipping a lossy version, and
/// ORLEANS-RUNTIME.md records the deferral.
/// </para>
/// <para>
/// <b>Threading.</b> Every member is safe to call from any thread at any point in the run's life.
/// <see cref="Completion"/> is one task shared by every caller, so two callers awaiting it observe one
/// outcome and the run is polled once however many are watching.
/// </para>
/// </remarks>
public sealed class OrleansRunHandle : IAsyncDisposable
{
    private readonly IPipelineRunGrain _run;
    private readonly GraphFingerprint _fingerprint;
    private readonly TimeSpan _pollInterval;
    private readonly bool _durable;
    private readonly Lazy<Task> _completion;
    private readonly Lazy<Task<RunEnding>> _watch;
    private long _epoch;

    /// <summary>Initializes a new instance of the <see cref="OrleansRunHandle"/> class.</summary>
    /// <param name="run">The grain hosting the run.</param>
    /// <param name="ticket">The ticket the coordinator issued for it.</param>
    /// <param name="fingerprint">The fingerprint of the pipeline's document, as the client computed it.</param>
    /// <param name="pollInterval">How often to poll while waiting for the run to end.</param>
    /// <param name="durable">Whether this run may be continued by a later attempt.</param>
    /// <remarks>
    /// Internal because a handle is only ever produced by materializing a pipeline. A handle over a run
    /// nothing started would be a control surface for nothing, exactly as the local one would.
    /// </remarks>
    internal OrleansRunHandle(
        IPipelineRunGrain run,
        PipelineRunTicket ticket,
        GraphFingerprint fingerprint,
        TimeSpan pollInterval,
        bool durable)
    {
        _run = run;
        _fingerprint = fingerprint;
        _pollInterval = pollInterval;
        _durable = durable;
        _epoch = ticket.Epoch;
        _completion = new Lazy<Task>(WatchAsync, LazyThreadSafetyMode.ExecutionAndPublication);
        _watch = new Lazy<Task<RunEnding>>(WatchTerminationAsync, LazyThreadSafetyMode.ExecutionAndPublication);

        Ticket = ticket;
    }

    /// <summary>Gets the ticket the coordinator issued for this run.</summary>
    /// <value>The run's identity, its ownership epoch, and the fingerprints the silo recorded.</value>
    /// <remarks>
    /// A record of what was issued when the run was started, and it does not move. For a durable run whose
    /// hosting silo has since died, the epoch on it is the one that attempt held; what the handle is
    /// actually carrying on its calls is <see cref="Epoch"/>.
    /// </remarks>
    public PipelineRunTicket Ticket { get; }

    /// <summary>Gets the identity of this run.</summary>
    public string RunId => Ticket.RunId;

    /// <summary>Gets the ownership epoch every control call for this run carries.</summary>
    /// <value>
    /// The epoch this handle is currently claiming under, which is the ticket's for an ordinary run and for
    /// a durable run that has never been continued, and the resumed attempt's afterwards.
    /// </value>
    /// <remarks>
    /// <para>
    /// <b>A durable handle follows the run rather than the attempt.</b> A resume is the same run continuing
    /// and it claims a fresh epoch, so a handle that had been holding the previous number is out of date
    /// rather than wrong: it learns the current one from the fencing refusal that names it and carries on.
    /// That is safe precisely because a durable run is <em>named</em> — the identity a handle addresses is
    /// the author's own, one run answers to it, and following its ownership forward cannot reach anybody
    /// else's work.
    /// </para>
    /// <para>
    /// An ordinary handle never does this and must not: its run has no later attempt, so a fencing refusal
    /// there means somebody else's claim and adopting it would be taking over a run this handle never
    /// started.
    /// </para>
    /// </remarks>
    public long Epoch => Interlocked.Read(ref _epoch);

    /// <summary>Gets the task that reports how this run ended.</summary>
    /// <value>
    /// A task that transitions exactly once: to <see cref="TaskStatus.RanToCompletion"/> when the stream
    /// ended or was drained by a graceful shutdown, to <see cref="TaskStatus.Faulted"/> with a
    /// <see cref="PipelineRunFailedException"/> describing what the run threw, and to
    /// <see cref="TaskStatus.Canceled"/> when the run was cancelled.
    /// </value>
    /// <remarks>
    /// <para>
    /// The polling starts when this property is first read, not when the handle is created: a caller who
    /// never asks how a run ended never generates a call, and a run nobody is watching still runs.
    /// </para>
    /// <para>
    /// It also faults with <see cref="PipelineRunLostException"/> when the activation hosting the run was
    /// recycled while it was executing and there was nothing to continue it from. An ordinary run is never
    /// continued, so the attempt is gone and saying so is the only honest answer; waiting for a terminal
    /// state that will never arrive would be the alternative. The same applies after the fact: an ordinary
    /// run's results live only as long as its activation, so a result read after the activation is recycled
    /// reports the loss rather than a value nothing is keeping.
    /// </para>
    /// <para>
    /// <b>A durable run that has written a checkpoint never reports that loss</b>, and the reason is that
    /// the poll itself is what continues it: addressing the run activates its grain, the activation finds
    /// the stored position, claims a fresh epoch, and is executing by the time this poll is answered — so
    /// what the loop sees is a running run. A durable run that died before its first capture is a different
    /// case and reports the loss like any other, because there is no position to continue from.
    /// </para>
    /// </remarks>
    public Task Completion => _completion.Value;

    /// <summary>Gets a task that resolves with how this run ended.</summary>
    /// <value>
    /// A task that resolves with <see cref="RunEnding.Completed"/> when the stream ended or was drained by
    /// a graceful shutdown, resolves with a <see cref="RunEndingKind.Failed"/> ending carrying the
    /// failure's type name and message when the run failed, and cancels when the run was cancelled.
    /// </value>
    /// <remarks>
    /// <para>
    /// The same affordance as <see cref="RunHandle.WatchTermination"/> and the same rules: a failed run's
    /// watch <em>resolves</em> with the failure as a fact to read, where <see cref="Completion"/> faults
    /// with it; cancellation is not an ending, so the watch of a cancelled run cancels rather than
    /// resolving; and reading this property is what starts the polling, exactly as reading
    /// <see cref="Completion"/> is — the two share one poll loop.
    /// </para>
    /// <para>
    /// <b>A lost run has no ending, and the watch says so by faulting.</b> When the activation hosting an
    /// ordinary run is recycled mid-run — or a durable one dies before its first capture — no terminal
    /// state was ever reached and none will be reported, so this task faults with
    /// <see cref="PipelineRunLostException"/>: the report that no ending will come. Resolving would claim
    /// an ending the run never had; staying pending would claim one is still coming. It faults with
    /// <see cref="PipelineFencingException"/> for the same reason when an ordinary run's identity turns out
    /// to be claimed by somebody else's work, which this handle has no right to report an ending for.
    /// </para>
    /// <para>
    /// The ending carries the failure the run itself reported over the wire — the type name and message a
    /// status poll carries — so it is the same pair a <see cref="PipelineRunFailedException"/> from
    /// <see cref="Completion"/> exposes, read instead of thrown.
    /// </para>
    /// </remarks>
    public Task<RunEnding> WatchTermination => _watch.Value;

    /// <summary>Takes one reading of this run's observable state.</summary>
    /// <returns>A task carrying the reading: status and the answering attempt's counters.</returns>
    /// <exception cref="PipelineRunLostException">
    /// The run is no longer active in the cluster and left nothing to continue, so there is no state to
    /// read.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The remote counterpart of <see cref="RunHandle.Snapshot"/>, and one grain call per reading: unlike
    /// <see cref="Completion"/> it neither starts nor joins the poll loop, so a monitor sampling a run on
    /// its own schedule costs exactly the calls it makes. Polling a durable run that lost its activation
    /// resumes it, exactly as any other call to it does.
    /// </para>
    /// <para>
    /// The counters describe the attempt that answered. A durable run's ending observed while its
    /// activation still lived reports that attempt's final counters; the same ending re-read after the
    /// activation is gone comes from the coordinator's register, which records outcomes and not
    /// diagnostics, so the counters there read zero. The continuous record is the metrics pipeline's.
    /// </para>
    /// </remarks>
    public async Task<RunSnapshot> SnapshotAsync()
    {
        RunStatusSnapshot status;

        try
        {
            status = await _run.GetStatusAsync(Epoch).ConfigureAwait(false);
        }
        catch (PipelineFencingException refused)
        {
            if (!Adopt(refused))
            {
                throw;
            }

            status = await _run.GetStatusAsync(Epoch).ConfigureAwait(false);
        }

        return status.Phase switch
        {
            RunPhase.NotStarted => throw new PipelineRunLostException(
                $"The run '{RunId}' is no longer active in the cluster, so there is no state to read. The activation hosting it was recycled and left nothing to continue."),
            _ => new RunSnapshot
            {
                Status = status.Phase switch
                {
                    RunPhase.Completed => RunSnapshotStatus.Completed,
                    RunPhase.Faulted => RunSnapshotStatus.Failed,
                    RunPhase.Canceled => RunSnapshotStatus.Canceled,
                    _ => RunSnapshotStatus.Running,
                },
                DroppedElements = status.DroppedElements,
                SupervisedFailures = status.SupervisedFailures,
                PoisonElements = status.PoisonElements,
                Checkpoints = status.Checkpoints,
                TotalCheckpointHold = status.TotalCheckpointHold,
            },
        };
    }

    /// <summary>Resolves one result this run's pipeline declares.</summary>
    /// <typeparam name="TResult">The type this process binds to the slot's result contract.</typeparam>
    /// <param name="slot">The slot, as <see cref="PipelineDefinition.ResultSlot{TResult}"/> produced it.</param>
    /// <param name="cancellationToken">A token that stops this wait; it does not affect the run.</param>
    /// <returns>
    /// A task that resolves with the result when the run completes, faults with a
    /// <see cref="PipelineRunFailedException"/> when the run failed, or cancels when the run cancels or
    /// <paramref name="cancellationToken"/> fires.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="slot"/> is the default value, was declared by a built
    /// <see cref="RunnableGraph"/> instance rather than by a pipeline, or was declared by a different
    /// pipeline's document.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The slot is validated here, before a call is made, and validated again by the run grain, which
    /// checks the same two facts against the document it is actually running. The local check makes a
    /// mistake a fast, well-worded exception; the remote one makes it impossible.
    /// </para>
    /// <para>
    /// What travels is the slot's name and its declaring document's fingerprint, as text. A
    /// <see cref="ResultSlot{TResult}"/> is an authoring-side value carrying a CLR type argument that
    /// means nothing on the other side, so it is deliberately not a wire type and needs no serializer.
    /// </para>
    /// <para>
    /// The value arrives through Orleans serialization, so the result type must satisfy it —
    /// <c>[GenerateSerializer]</c> with <c>[Id]</c> on every member, or a registered serializer. A type
    /// that does not fails when a result is first sent rather than when the pipeline was written, which is
    /// the documented shape of that requirement and not a surprise this call can prevent.
    /// </para>
    /// </remarks>
    public async Task<TResult> GetValueAsync<TResult>(
        ResultSlot<TResult> slot,
        CancellationToken cancellationToken = default)
    {
        if (slot.IsDefault)
        {
            throw new ArgumentException(
                $"The default {nameof(ResultSlot<TResult>)} names no result and cannot be resolved. Recover a slot from the pipeline with {nameof(PipelineDefinition)}.{nameof(PipelineDefinition.ResultSlot)}.",
                nameof(slot));
        }

        if (!slot.IsPipelineSlot)
        {
            throw new ArgumentException(
                $"The slot '{slot.Id}' belongs to a different world: it was declared by a built {nameof(RunnableGraph)} instance and binds to that instance, and this is a run of a {nameof(PipelineDefinition)}, whose slots bind by fingerprint and lineage alone. Recover the pipeline's own slot with {nameof(PipelineDefinition)}.{nameof(PipelineDefinition.ResultSlot)}.",
                nameof(slot));
        }

        if (slot.Graph != _fingerprint)
        {
            throw new ArgumentException(
                $"The slot '{slot.Id}' belongs to a different pipeline: it was declared by the document {slot.Graph}, and this is a run of {_fingerprint}. A slot resolves only against a run of the document that declared it.",
                nameof(slot));
        }

        // The run's outcome is awaited first and its failure is deliberately swallowed here: the envelope
        // is the authority on how a run ended, and reporting the failure from two places would let them
        // disagree. What this wait is for is the guarantee that the envelope will be a settled one.
        try
        {
            await Completion.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            // Observed through the envelope below.
        }

        ResultEnvelope envelope;

        try
        {
            envelope = await _run
                .GetResultAsync(Epoch, slot.Id.Value, _fingerprint.ToString())
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        catch (PipelineFencingException refused)
        {
            if (!Adopt(refused))
            {
                throw;
            }

            envelope = await _run
                .GetResultAsync(Epoch, slot.Id.Value, _fingerprint.ToString())
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        return envelope.Phase switch
        {
            RunPhase.Completed when envelope.HasValue => (TResult)envelope.Value!,
            RunPhase.Faulted => throw new PipelineRunFailedException(
                envelope.FailureType,
                envelope.FailureMessage,
                RunId),
            RunPhase.Canceled => throw new OperationCanceledException(
                $"The run '{RunId}' was cancelled, so the result '{slot.Id}' resolves to nothing. Cancellation abandons a run; a graceful shutdown is what resolves a result from the state a run had accumulated."),
            RunPhase.NotStarted => throw new PipelineRunLostException(
                $"The run '{RunId}' is no longer active in the cluster, so the result '{slot.Id}' can no longer be read. The activation hosting it was recycled, and phase 1 does not resume a run across a deactivation."),
            _ => throw new InvalidOperationException(
                $"The run '{RunId}' reported the phase '{envelope.Phase}' with no value for the result '{slot.Id}'. A completed run settles every result it declares before it reports completion, so this is a runtime invariant that has moved rather than a state a caller can reach."),
        };
    }

    /// <summary>Asks this run to stop gracefully.</summary>
    /// <returns>A task that completes when the request has been delivered.</returns>
    /// <remarks>
    /// The run stops pulling from its source and everything already admitted keeps flowing, so an
    /// aggregate resolves its slot with the state accumulated so far and <see cref="Completion"/> reports
    /// success. The returned task reports only that the request was delivered; that the drain has finished
    /// is what <see cref="Completion"/> reports, and awaiting the drain inside a grain call would park an
    /// activation for as long as the graph takes. It returns <see cref="Task"/> where the local handle's
    /// returns <see cref="ValueTask"/>, and the asymmetry is deliberate: this method is a grain call and a
    /// grain call is a <see cref="Task"/>, while the local one completes synchronously often enough for the
    /// cheaper shape to be worth having.
    /// </remarks>
    public async Task ShutdownAsync()
    {
        try
        {
            await _run.ShutdownAsync(Epoch).ConfigureAwait(false);

            return;
        }
        catch (PipelineFencingException refused)
        {
            if (!Adopt(refused))
            {
                throw;
            }
        }

        await _run.ShutdownAsync(Epoch).ConfigureAwait(false);
    }

    /// <summary>Cancels this run and stops watching it.</summary>
    /// <returns>A task that completes when the cancellation has been requested.</returns>
    /// <remarks>
    /// <para>
    /// Disposal is the abrupt stop: the run abandons what it was doing and <see cref="Completion"/> and
    /// every result end cancelled, unless the run had already reached a terminal state of its own.
    /// </para>
    /// <para>
    /// It never throws — not for the cancellation it caused itself, not for a failure the run had already
    /// suffered, and not for a run that is already gone. A teardown that replaced the caller's own
    /// exception with the run's would hide the thing worth reading, and how the run ended stays on
    /// <see cref="Completion"/>.
    /// </para>
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        try
        {
            await _run.CancelAsync(Epoch).ConfigureAwait(false);

            return;
        }
        catch (PipelineRunLostException)
        {
            // The activation hosting the run is gone, so there is nothing left to cancel.
            return;
        }
        catch (PipelineFencingException refused)
        {
            // Some other claim owns the run this handle addresses; cancelling it is not this handle's to
            // do — unless the claim is this very run's own later attempt, which a durable handle follows.
            if (!Adopt(refused))
            {
                return;
            }
        }

        try
        {
            await _run.CancelAsync(Epoch).ConfigureAwait(false);
        }
        catch (PipelineRunLostException)
        {
        }
        catch (PipelineFencingException)
        {
        }
    }

    /// <summary>Takes over the epoch a fencing refusal named, when this handle is entitled to.</summary>
    /// <param name="refused">The refusal, which carries the run's current epoch and the one the call sent.</param>
    /// <returns><see langword="true"/> when the call is worth retrying under the epoch this handle now holds.</returns>
    /// <remarks>
    /// <para>
    /// Only a durable handle adopts, and only forward. A durable run is named by its author and a resume is
    /// that same run continuing under a fresh claim, so a refusal naming a higher epoch is this run's own
    /// later attempt and following it is the whole point of a handle that outlives a silo. An ordinary run
    /// has no later attempt, so a refusal there names somebody else and adopting it would be taking over
    /// work this handle never started. The adoption is a compare-exchange loop rather than an exchange, so
    /// two of this handle's own paths adopting concurrently can only move the number forward.
    /// </para>
    /// <para>
    /// <b>A refusal can also answer a call this handle itself has already moved past</b>, and that one is
    /// retried without adopting anything. The paths of one handle race each other by design — the poll loop
    /// adopts a resumed attempt's epoch while a control call is in flight carrying the old one — and the
    /// refusal that call earns names an epoch at or behind the one this handle now holds, sent by a call
    /// that carried less. Rethrowing it would report a foreign claim on a run this handle legitimately
    /// follows; retrying with the number already held is not following anybody new, and it terminates —
    /// the retried call carries the held epoch, so it either passes the fence or earns a refusal whose
    /// caller epoch equals what is held, which is not retried again.
    /// </para>
    /// <para>
    /// A refusal whose caller epoch is the one this handle holds is a genuine answer about ownership and is
    /// not adopted: retrying it unchanged would be a loop. A refusal naming zero falls out the same way,
    /// since zero is what a grain with no run at all reports and there is nothing there to claim.
    /// </para>
    /// </remarks>
    private bool Adopt(PipelineFencingException refused)
    {
        if (!_durable)
        {
            return false;
        }

        while (true)
        {
            long held = Interlocked.Read(ref _epoch);

            if (refused.CurrentEpoch <= held)
            {
                return refused.CallerEpoch < held;
            }

            if (Interlocked.CompareExchange(ref _epoch, refused.CurrentEpoch, held) == held)
            {
                return true;
            }
        }
    }

    /// <summary>Returns a one-line diagnostic summary of this run.</summary>
    /// <returns>Text of the form <c>run 4f1c9a2b… of orders (epoch 3)</c>.</returns>
    /// <remarks>The method never throws and makes no call, so it is safe in any log line.</remarks>
    public override string ToString() => $"run {RunId} of {Ticket.GraphId} (epoch {Ticket.Epoch})";

    /// <summary>Awaits the run's completion and translates its outcome into an ending.</summary>
    /// <returns>The ending; the task cancels when the run was cancelled.</returns>
    /// <remarks>
    /// Only <see cref="PipelineRunFailedException"/> is translated, because it is the only outcome that
    /// <em>is</em> an ending. A cancellation propagates so this task cancels with it, and a
    /// <see cref="PipelineRunLostException"/> or <see cref="PipelineFencingException"/> propagates so this
    /// task faults with it: a lost attempt and a foreign claim are runs whose ending this handle cannot
    /// report, which is a different fact from either ending.
    /// </remarks>
    private async Task<RunEnding> WatchTerminationAsync()
    {
        try
        {
            await Completion.ConfigureAwait(false);

            return RunEnding.Completed;
        }
        catch (PipelineRunFailedException failed)
        {
            return RunEnding.Failed(failed.FailureType, failed.FailureMessage);
        }
    }

    /// <summary>Polls the run until it reaches a terminal state, and reports which one.</summary>
    /// <returns>A task carrying the run's outcome.</returns>
    /// <exception cref="PipelineRunFailedException">The run failed.</exception>
    /// <exception cref="PipelineRunLostException">The activation hosting the run was recycled.</exception>
    /// <exception cref="OperationCanceledException">The run was cancelled.</exception>
    /// <remarks>
    /// <para>
    /// The very first poll happens before any wait, so a run that had already finished by the time a
    /// caller looked is reported at once rather than one interval later. A run reported as not started is
    /// a lost attempt whether or not this client had seen it running: a handle exists only because a start
    /// succeeded, so "no run here" can only mean the attempt is gone.
    /// </para>
    /// <para>
    /// A poll that fails to be delivered decides nothing. A response timeout, an unavailable silo, and a
    /// rejected message are facts about one call, not about the run — a poll in flight when the run's host
    /// is killed times out against a directory entry the client has not yet learned is dead, and surfacing
    /// that as this task's outcome would report the transport's confusion instead of the run's fate. The
    /// loop retries instead, and converges on an authoritative answer: once the cluster has noticed the
    /// death, the next poll activates a fresh run grain whose "not started" is the real "this attempt is
    /// lost". Only a cluster that never answers again keeps this task pending, and no other answer would
    /// be honest there either.
    /// </para>
    /// </remarks>
    private async Task WatchAsync()
    {
        using PeriodicTimer timer = new(_pollInterval);

        while (true)
        {
            RunStatusSnapshot status;

            try
            {
                status = await _run.GetStatusAsync(Epoch).ConfigureAwait(false);
            }
            catch (Exception undelivered) when (
                undelivered is TimeoutException or SiloUnavailableException or OrleansMessageRejectionException)
            {
                _ = await timer.WaitForNextTickAsync().ConfigureAwait(false);

                continue;
            }
            catch (PipelineFencingException refused)
            {
                // A durable run this poll itself brought back has claimed a fresh epoch, so the number this
                // loop was carrying names the attempt that died. Adopting it and asking again is following
                // the run rather than the attempt; for every other handle the refusal is somebody else's
                // claim and stands.
                if (!Adopt(refused))
                {
                    throw;
                }

                continue;
            }

            switch (status.Phase)
            {
                case RunPhase.Completed:
                    return;
                case RunPhase.Faulted:
                    throw new PipelineRunFailedException(status.FailureType, status.FailureMessage, RunId);
                case RunPhase.Canceled:
                    throw new OperationCanceledException(
                        $"The run '{RunId}' was cancelled.");
                case RunPhase.NotStarted:
                    throw new PipelineRunLostException(
                        $"The run '{RunId}' is no longer active in the cluster. The activation hosting it was recycled while it was executing, and phase 1 does not resume a run across a deactivation, so this attempt is lost rather than delayed.");
                default:
                    _ = await timer.WaitForNextTickAsync().ConfigureAwait(false);
                    break;
            }
        }
    }
}
