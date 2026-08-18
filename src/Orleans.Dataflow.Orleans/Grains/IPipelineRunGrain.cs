namespace Orleans.Dataflow.Grains;

/// <summary>
/// One run of one pipeline: it hosts the engine, drives it to a terminal state, and answers for it.
/// </summary>
/// <remarks>
/// <para>
/// One activation per run, addressed by the composed key <c>graphId/runId</c>. The whole run executes
/// inside this activation — the proven local engine, hosted whole — which is what preserves every
/// semantic the engine was tested for: terminal discipline, drain versus abandon, boundaries, and the
/// order elements travel in. Distribution of a run across grains is the concern of a later phase, and
/// only where a stage's own semantics demand it.
/// </para>
/// <para>
/// The grain is non-reentrant, and nothing here needs it to be otherwise. The engine runs on its own
/// dedicated threads rather than on the activation's turn, so a status poll during a long run is answered
/// immediately, and every method here returns without waiting for the run: the two control calls request a
/// stop rather than await one, and reading a result reports "not yet" rather than parking a turn until
/// there is one.
/// </para>
/// <para>
/// <b>Durability, stated honestly, and it now has two cases.</b> This grain persists nothing about a run's
/// progress itself. For an ordinary run that is the whole story: an activation recycled while it was
/// executing takes the attempt with it, the fresh activation reports <see cref="RunPhase.NotStarted"/>, and
/// a caller that had seen the run executing learns the attempt was lost. For a run declared durable, the
/// progress is in a checkpoint store rather than here, and the fresh activation reads it: if there is a
/// position to continue from it claims a fresh epoch, materializes from that position, and reports
/// <see cref="RunPhase.Running"/>, so <see cref="PipelineRunLostException"/> is unreachable for such a run.
/// If there is not — a durable run that died before its first capture — the attempt is lost exactly as an
/// ordinary one is, because there is nothing to continue.
/// </para>
/// <para>
/// The same sentence has a consequence worth stating on its own: <b>a completed run's results live only as
/// long as its activation.</b> Nothing writes them anywhere, so a result not read before the activation is
/// recycled is gone, and the grain answers a later read with <see cref="PipelineRunLostException"/> rather
/// than with a value it no longer has. Durable results are a later milestone's checkpoint work.
/// </para>
/// </remarks>
public interface IPipelineRunGrain : IGrainWithStringKey
{
    /// <summary>Starts the run this grain is.</summary>
    /// <param name="canonicalDocument">The document's canonical bytes, as the coordinator accepted them.</param>
    /// <param name="epoch">The ownership epoch the coordinator assigned to this run.</param>
    /// <returns>A task that completes once the run has started.</returns>
    /// <exception cref="PipelineFencingException">A run is already active in this grain.</exception>
    /// <exception cref="PipelineRejectedException">
    /// The bytes are not a canonical graph document, or the document does not validate against this silo's
    /// catalog and factories.
    /// </exception>
    /// <remarks>
    /// Called by the coordinator and not by a client, which is why it takes the epoch rather than issuing
    /// one. The document is validated again here rather than trusted: this grain may be on another silo
    /// than the coordinator that accepted it, and a silo materializes only what its own catalog resolves.
    /// </remarks>
    Task StartAsync(byte[] canonicalDocument, long epoch);

    /// <summary>Starts, or continues, the durable run this grain is.</summary>
    /// <param name="declaredEpoch">
    /// The epoch the declaration this call is driving recorded, as its ticket reports it.
    /// </param>
    /// <returns>The ownership epoch this run is now executing under.</returns>
    /// <exception cref="PipelineRunLostException">
    /// The coordinator holds no durable declaration under this run's identity, so there is nothing to
    /// start.
    /// </exception>
    /// <exception cref="PipelineResumeRefusedException">
    /// A checkpoint exists for this run and it was taken of a different document or a different revision.
    /// </exception>
    /// <exception cref="PipelineRejectedException">
    /// The declared document does not validate against this silo's catalog, or the checkpoint names a node
    /// this graph has no such seam for.
    /// </exception>
    /// <remarks>
    /// <para>
    /// Called by the client that declared the run and not by the coordinator, which is the opposite of
    /// <see cref="StartAsync"/> and is deliberate: a coordinator that started run grains itself would be
    /// awaiting a grain that may be calling it back to claim its epoch. Here the client drives both hops in
    /// turn, so the only edge between the two grains runs one way at a time.
    /// </para>
    /// <para>
    /// <b>It is the same call for a first start and for a resume</b>, and idempotent for both. A run already
    /// executing in this activation answers with the epoch it holds and is not disturbed; an activation that
    /// found a checkpoint has already continued the run by the time this is answered; an activation that
    /// found none starts the run from the beginning. Which of the three happened is visible in what the run
    /// does, never in which method the caller had to choose.
    /// </para>
    /// <para>
    /// <b><paramref name="declaredEpoch"/> is what makes a replacement land.</b> Declaring an existing run
    /// again leaves its epoch where it was, so a live attempt's number is never lower than what this call
    /// carries and it answers with its own; a replacement mints a fresh number, so what this call carries is
    /// higher and the attempt it finds here has been superseded — abandoned, and the replacement started in
    /// its place. The same comparison retires a refusal this activation was remembering: the same
    /// declaration hears the same refusal without another claim, and a newer one has the question asked
    /// again.
    /// </para>
    /// <para>
    /// A run whose declaration records that it has ended answers with that epoch and starts nothing; how it
    /// ended is what a status poll then reports.
    /// </para>
    /// <para>
    /// The epoch it returns is the one this attempt owns, which is <em>not</em> necessarily the one the
    /// declaration recorded: an attempt after a crash claims a fresh number. A caller holding an older
    /// ticket is not wrong, it is out of date, and the fencing refusal it receives names the current epoch
    /// so it can catch up.
    /// </para>
    /// </remarks>
    Task<long> EnsureStartedAsync(long declaredEpoch);

    /// <summary>Reports where this run is.</summary>
    /// <param name="epoch">The ownership epoch from the run's ticket.</param>
    /// <returns>The snapshot.</returns>
    /// <exception cref="PipelineFencingException">
    /// <paramref name="epoch"/> is not this run's, and a run is active.
    /// </exception>
    /// <remarks>
    /// A grain with no active run answers <see cref="RunPhase.NotStarted"/> for any epoch rather than
    /// refusing: there is no ownership to fence when there is nothing to own, and the caller's next
    /// question is whether the run is gone rather than whether its claim is current.
    /// </remarks>
    Task<RunStatusSnapshot> GetStatusAsync(long epoch);

    /// <summary>Reads one result this run's document declares.</summary>
    /// <param name="epoch">The ownership epoch from the run's ticket.</param>
    /// <param name="slotName">The declared name of the slot.</param>
    /// <param name="graphFingerprint">The canonical text form of the declaring document's fingerprint.</param>
    /// <returns>The envelope, which reports the terminal state as well as the value.</returns>
    /// <exception cref="PipelineFencingException"><paramref name="epoch"/> is not this run's.</exception>
    /// <exception cref="PipelineRunLostException">
    /// No run is active in this grain, so there is no result to read: a run's results live only as long as
    /// its activation.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="graphFingerprint"/> is not this run's document's, or this run's document declares no
    /// slot of that name.
    /// </exception>
    /// <remarks>
    /// <para>
    /// A slot travels as its name and its declaring document's fingerprint, and never as a
    /// <see cref="Orleans.Dataflow.ResultSlot{TResult}"/>. A slot is an authoring-side value carrying a
    /// CLR type argument that means nothing here, and the two facts a run needs to check — which name and
    /// which document — are text. The client validates the slot fully before it calls; this is the
    /// second, independent check, so a hand-built call cannot read a result of a document it did not name.
    /// </para>
    /// <para>
    /// Answers immediately whether the run has ended or not. A run that has not ended has resolved
    /// nothing, and reporting that is what keeps a grain turn bounded; a caller waits by polling the
    /// status, not by parking this call.
    /// </para>
    /// </remarks>
    Task<ResultEnvelope> GetResultAsync(long epoch, string slotName, string graphFingerprint);

    /// <summary>Asks this run to stop gracefully.</summary>
    /// <param name="epoch">The ownership epoch from the run's ticket.</param>
    /// <returns>A task that completes when the request has been delivered.</returns>
    /// <exception cref="PipelineFencingException"><paramref name="epoch"/> is not this run's.</exception>
    /// <exception cref="PipelineRunLostException">No run is active in this grain.</exception>
    /// <remarks>
    /// The run stops pulling from its source and everything already admitted keeps flowing: an aggregate
    /// resolves its slot with the state it accumulated, and the run reports success. The request is
    /// delivered, not awaited — a drain has no bound and a grain turn does.
    /// </remarks>
    Task ShutdownAsync(long epoch);

    /// <summary>Cancels this run.</summary>
    /// <param name="epoch">The ownership epoch from the run's ticket.</param>
    /// <returns>A task that completes when the request has been delivered.</returns>
    /// <exception cref="PipelineFencingException"><paramref name="epoch"/> is not this run's.</exception>
    /// <exception cref="PipelineRunLostException">No run is active in this grain.</exception>
    /// <remarks>
    /// The run abandons what it was doing and nothing it declared resolves. Delivered rather than awaited,
    /// for the same reason a graceful stop is.
    /// </remarks>
    Task CancelAsync(long epoch);
}
