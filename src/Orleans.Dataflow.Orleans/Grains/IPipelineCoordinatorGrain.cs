using Orleans.Concurrency;

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
/// <para>
/// <b>Those three passthroughs interleave, and that is a correctness requirement rather than a
/// throughput one.</b> Since M5.3 a run grain calls its coordinator back — a durable run claims its epoch
/// when the activation hosting it comes up — so a passthrough that occupied this activation's turn while
/// awaiting the run grain would be waiting for a grain that is waiting for this one. They interleave
/// safely because they touch no state at all: each forwards one call and returns its answer. Everything
/// that reads or writes the register stays non-reentrant, so the epoch sequence is still produced one
/// turn at a time, and nothing that does touch state ever awaits a run grain — which is what makes the
/// absence of a cycle a property of the shape rather than a hope about timing.
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

    /// <summary>Accepts a pipeline document and declares a durable run of it under a name the caller chose.</summary>
    /// <param name="canonicalDocument">The document's canonical bytes.</param>
    /// <param name="declaration">What the run is called and when it checkpoints.</param>
    /// <returns>The ticket addressing the declared run.</returns>
    /// <exception cref="PipelineRejectedException">
    /// The bytes are not a canonical graph document, the document belongs to a different pipeline, it does
    /// not validate against this silo's catalog and factories, the run identity is not a valid one, or this
    /// silo registers no checkpoint store for a durable run to write to.
    /// </exception>
    /// <exception cref="PipelineResumeRefusedException">
    /// The run identity is already declared for a different document. V1 continues one document per durable
    /// run identity; a changed pipeline runs under a name of its own.
    /// </exception>
    /// <remarks>
    /// <para>
    /// <b>This declares and does not start.</b> The coordinator records the document, the timing, and an
    /// epoch, and returns; what starts the run is the activation of its run grain, which claims the epoch
    /// this call recorded. That separation is deliberate and it is what makes activation-driven resume
    /// possible without a second protocol: an attempt after a crash comes up exactly the way the first one
    /// did, and the coordinator never has to know which of the two it is answering.
    /// </para>
    /// <para>
    /// <b>Declaring one identity twice addresses one run.</b> A durable run is named by its author, so a
    /// second declaration with the same document updates the declared timing and hands back the run that
    /// already exists — live or waiting to be resumed — rather than starting a second one. Two independent
    /// durable runs of one pipeline are two identities.
    /// </para>
    /// </remarks>
    Task<PipelineRunTicket> DeclareDurableRunAsync(byte[] canonicalDocument, DurableRunDeclaration declaration);

    /// <summary>Claims ownership of one declared durable run for the activation about to host it.</summary>
    /// <param name="runId">The run identity from its ticket.</param>
    /// <returns>
    /// What the claiming activation needs to run it, or <see langword="null"/> when this coordinator has no
    /// durable declaration under that identity.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="runId"/> is not a valid run identifier.</exception>
    /// <remarks>
    /// <para>
    /// <b>Called by the run grain and never by a client.</b> It is the one call that flows from a run back
    /// to its coordinator, and it exists because a run grain holds nothing across an activation: the
    /// document and the timing live here, and so does the epoch sequence that orders claims to the run.
    /// </para>
    /// <para>
    /// The first claim after a declaration returns the epoch the declaration recorded, because nothing has
    /// owned the run yet. Every later claim returns a fresh one, because it is a resume — a new claim to the
    /// same run — and the previous attempt's calls must stop being current the moment somebody else owns it.
    /// </para>
    /// <para>
    /// An identity this coordinator has no record of answers <see langword="null"/> rather than refusing.
    /// That is what an ordinary, non-durable run's grain asks and hears, and it is why an activation of one
    /// still reports a lost attempt exactly as it did before durability existed.
    /// </para>
    /// </remarks>
    Task<DurableRunClaim?> ClaimDurableRunAsync(string runId);

    /// <summary>Reports where one run of this pipeline is.</summary>
    /// <param name="runId">The run identity from its ticket.</param>
    /// <param name="epoch">The ownership epoch from its ticket.</param>
    /// <returns>The snapshot the run grain answered with.</returns>
    /// <exception cref="PipelineFencingException"><paramref name="epoch"/> is not the run's.</exception>
    [AlwaysInterleave]
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
    [AlwaysInterleave]
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
    [AlwaysInterleave]
    Task CancelRunAsync(string runId, long epoch);
}
