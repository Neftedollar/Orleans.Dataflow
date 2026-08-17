using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;

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
    private readonly Lazy<Task> _completion;

    /// <summary>Initializes a new instance of the <see cref="OrleansRunHandle"/> class.</summary>
    /// <param name="run">The grain hosting the run.</param>
    /// <param name="ticket">The ticket the coordinator issued for it.</param>
    /// <param name="fingerprint">The fingerprint of the pipeline's document, as the client computed it.</param>
    /// <param name="pollInterval">How often to poll while waiting for the run to end.</param>
    /// <remarks>
    /// Internal because a handle is only ever produced by materializing a pipeline. A handle over a run
    /// nothing started would be a control surface for nothing, exactly as the local one would.
    /// </remarks>
    internal OrleansRunHandle(
        IPipelineRunGrain run,
        PipelineRunTicket ticket,
        GraphFingerprint fingerprint,
        TimeSpan pollInterval)
    {
        _run = run;
        _fingerprint = fingerprint;
        _pollInterval = pollInterval;
        _completion = new Lazy<Task>(WatchAsync, LazyThreadSafetyMode.ExecutionAndPublication);

        Ticket = ticket;
    }

    /// <summary>Gets the ticket the coordinator issued for this run.</summary>
    /// <value>The run's identity, its ownership epoch, and the fingerprints the silo recorded.</value>
    public PipelineRunTicket Ticket { get; }

    /// <summary>Gets the identity of this run.</summary>
    public string RunId => Ticket.RunId;

    /// <summary>Gets the ownership epoch every control call for this run carries.</summary>
    public long Epoch => Ticket.Epoch;

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
    /// recycled while it was executing. Phase 1 does not resume a run across a deactivation, so the
    /// attempt is gone and saying so is the only honest answer; waiting for a terminal state that will
    /// never arrive would be the alternative. The same applies after the fact: a run's results live only
    /// as long as its activation, so a result read after the activation is recycled reports the loss
    /// rather than a value nothing is keeping.
    /// </para>
    /// </remarks>
    public Task Completion => _completion.Value;

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

        ResultEnvelope envelope = await _run
            .GetResultAsync(Epoch, slot.Id.Value, _fingerprint.ToString())
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);

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
    /// activation for as long as the graph takes.
    /// </remarks>
    public Task ShutdownAsync() => _run.ShutdownAsync(Epoch);

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
        }
        catch (PipelineRunLostException)
        {
            // The activation hosting the run is gone, so there is nothing left to cancel.
        }
        catch (PipelineFencingException)
        {
            // Some other claim owns the run this handle addresses; cancelling it is not this handle's to do.
        }
    }

    /// <summary>Returns a one-line diagnostic summary of this run.</summary>
    /// <returns>Text of the form <c>run 4f1c9a2b… of orders (epoch 3)</c>.</returns>
    /// <remarks>The method never throws and makes no call, so it is safe in any log line.</remarks>
    public override string ToString() => $"run {RunId} of {Ticket.GraphId} (epoch {Ticket.Epoch})";

    /// <summary>Polls the run until it reaches a terminal state, and reports which one.</summary>
    /// <returns>A task carrying the run's outcome.</returns>
    /// <exception cref="PipelineRunFailedException">The run failed.</exception>
    /// <exception cref="PipelineRunLostException">The activation hosting the run was recycled.</exception>
    /// <exception cref="OperationCanceledException">The run was cancelled.</exception>
    /// <remarks>
    /// The very first poll happens before any wait, so a run that had already finished by the time a
    /// caller looked is reported at once rather than one interval later. A run reported as not started is
    /// a lost attempt whether or not this client had seen it running: a handle exists only because a start
    /// succeeded, so "no run here" can only mean the attempt is gone.
    /// </remarks>
    private async Task WatchAsync()
    {
        using PeriodicTimer timer = new(_pollInterval);

        while (true)
        {
            RunStatusSnapshot status = await _run.GetStatusAsync(Epoch).ConfigureAwait(false);

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
