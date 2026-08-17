namespace Orleans.Dataflow.Grains;

/// <summary>
/// The owner of every run of one pipeline: it accepts documents, assigns run identities and ownership
/// epochs, and knows which runs it started.
/// </summary>
/// <remarks>
/// <para>
/// One activation per <see cref="Orleans.Dataflow.Identity.GraphId"/>, addressed by that identifier as the
/// grain key. A pipeline's runs are therefore serialized through one place, which is what makes an epoch
/// meaningful: a number handed out by two writers would order nothing.
/// </para>
/// <para>
/// Fencing here is Orleans-native and needs no protocol of ours. The coordinator's registry lives in
/// persistent state, so a stale activation that tries to write hits the ETag conflict, is killed by the
/// runtime, and the fresh activation reads the truth. What the coordinator hands out — the epoch — is what
/// carries that ownership onward to the run grains, which reject any other value.
/// </para>
/// <para>
/// The status and control members are passthroughs to the run they name, and they exist because a caller
/// holding only a pipeline identity and a run identity should not have to know how a run grain is
/// addressed. A client that already holds a handle addresses the run directly and saves the hop; both
/// paths carry the epoch, and both are the same check.
/// </para>
/// </remarks>
public interface IPipelineCoordinatorGrain : IGrainWithStringKey
{
    /// <summary>Accepts a pipeline document and starts a run of it.</summary>
    /// <param name="canonicalDocument">The document's canonical bytes.</param>
    /// <returns>The ticket addressing the started run.</returns>
    /// <exception cref="PipelineRejectedException">
    /// The bytes are not a canonical graph document, the document belongs to a different pipeline than
    /// this coordinator, or it does not validate against this silo's catalog and factories. The message
    /// carries every diagnostic rather than the first.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The document travels as canonical bytes and never as an Orleans-serialized object graph, so what
    /// the silo validates is byte-for-byte what the caller fingerprinted. The ticket reports the
    /// fingerprint the silo computed, which is how a caller confirms that.
    /// </para>
    /// <para>
    /// The call returns once the run has started, not once it has finished: a run executes on its own
    /// threads and reports its progress through status polls. Starting the same pipeline twice yields two
    /// runs that both live, each with its own identity and its own epoch; a second start never fences the
    /// first, because two runs of one pipeline are two runs and not two claims to one.
    /// </para>
    /// </remarks>
    Task<PipelineRunTicket> StartRunAsync(byte[] canonicalDocument);

    /// <summary>Reports where one run of this pipeline is.</summary>
    /// <param name="runId">The run identity from its ticket.</param>
    /// <param name="epoch">The ownership epoch from its ticket.</param>
    /// <returns>The snapshot the run grain answered with.</returns>
    /// <exception cref="PipelineFencingException"><paramref name="epoch"/> is not the run's.</exception>
    Task<RunStatusSnapshot> GetStatusAsync(string runId, long epoch);

    /// <summary>Asks one run of this pipeline to stop gracefully.</summary>
    /// <param name="runId">The run identity from its ticket.</param>
    /// <param name="epoch">The ownership epoch from its ticket.</param>
    /// <returns>A task that completes when the request has been delivered.</returns>
    /// <exception cref="PipelineFencingException"><paramref name="epoch"/> is not the run's.</exception>
    /// <remarks>
    /// A request and not a wait: the run stops pulling, drains what it already admitted, and resolves its
    /// results with the state accumulated from all of it. That the drain has finished is reported by a
    /// status poll, because a call that waited for it would park a grain turn on work of unbounded length.
    /// </remarks>
    Task ShutdownRunAsync(string runId, long epoch);

    /// <summary>Cancels one run of this pipeline.</summary>
    /// <param name="runId">The run identity from its ticket.</param>
    /// <param name="epoch">The ownership epoch from its ticket.</param>
    /// <returns>A task that completes when the request has been delivered.</returns>
    /// <exception cref="PipelineFencingException"><paramref name="epoch"/> is not the run's.</exception>
    /// <remarks>
    /// The abrupt stop: the run abandons what it was doing and nothing it declared resolves. A request
    /// like the graceful one, and reported the same way.
    /// </remarks>
    Task CancelRunAsync(string runId, long epoch);
}
