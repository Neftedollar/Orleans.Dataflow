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
/// <para>
/// <b>Four members flow from a run back to its coordinator and none of them awaits anything.</b>
/// <see cref="ClaimDurableRunAsync"/> reads the declaration, <see cref="TakeDurableRunAsync"/> takes
/// ownership of it, <see cref="ReportDurableRunEndedAsync"/> records how it ended, and
/// <see cref="HasIssuedEpochAsync"/> confirms that an epoch offered to a run grain is one this coordinator
/// really issued. Every one of them returns without calling anybody, so the edge between the two grains is
/// still one-way and the shape argument above is unchanged.
/// </para>
/// <para>
/// <b>Reading a declaration and owning it are two calls, and separating them is what stops a reader from
/// fencing a live run.</b> A claim used to mint a fresh ownership epoch on the way past, which meant that
/// anybody who merely asked what a durable run was got a number that superseded the activation actually
/// executing it — after which that activation's own report of how the run ended was refused as stale, and
/// the run was resumed and its tail re-run. An epoch orders <em>claims to ownership</em>, so it is minted
/// where ownership is taken: by the activation that is about to host the run, once, and by nothing else.
/// </para>
/// </remarks>
public interface IPipelineCoordinatorGrain : IGrainWithStringKey
{
    /// <summary>Accepts a pipeline document and starts a run of it.</summary>
    /// <param name="canonicalDocument">The document's canonical bytes.</param>
    /// <returns>The ticket addressing the started run.</returns>
    /// <exception cref="PipelineRejectedException">
    /// The bytes are not a canonical graph document, they are longer than a coordinator will decode, the
    /// document declares more nodes than one will execute, it belongs to a different pipeline than this
    /// coordinator, or it does not validate against this silo's catalog and factories. The message carries
    /// the diagnostics rather than the first of them.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The document travels as canonical bytes and never as an Orleans-serialized object graph, so what
    /// the silo validates is byte-for-byte what the caller fingerprinted. The ticket reports the
    /// fingerprint the silo computed, which is how a caller confirms that.
    /// </para>
    /// <para>
    /// <b>The bytes are measured before they are decoded and the nodes are counted after.</b> This member
    /// is not interleaved — it issues epochs, which have to be issued one at a time — so the time it spends
    /// decoding is time the whole coordinator spends, status polls of its other runs included. A document is
    /// an input, decoding one is linear in its size, and neither bound was there to say so; both are stated
    /// in the refusal, and both are generous enough that a pipeline a person wrote never meets them.
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
    /// The bytes are not a canonical graph document or are longer than a coordinator will decode, the
    /// document declares more nodes than one will execute, it belongs to a different pipeline, it does not
    /// validate against this silo's catalog and factories, the run identity is not a valid one, this silo
    /// registers no checkpoint store for a durable run to write to, or this pipeline's register already
    /// holds as many durable run identities as it will hold.
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
    /// <b>The register a declaration grows is bounded, and the bound is generous.</b> A record holds the
    /// document it names, the whole register is rewritten on every declaration, and nothing used to remove
    /// one — so a deployment that named a durable run per request grew a state document until its storage
    /// provider refused it, at which point the coordinator could no longer write at all and every start of
    /// that pipeline stopped with it. A pipeline that reaches the cap is refused by name, told what the cap
    /// is, and told that <see cref="RetireDurableRunAsync"/> is what makes room.
    /// </para>
    /// <para>
    /// <b>Declaring one identity twice addresses one run.</b> A durable run is named by its author, so a
    /// second declaration with the same document updates the declared timing and hands back the run that
    /// already exists — live or waiting to be resumed — rather than starting a second one. Two independent
    /// durable runs of one pipeline are two identities.
    /// </para>
    /// </remarks>
    Task<PipelineRunTicket> DeclareDurableRunAsync(byte[] canonicalDocument, DurableRunDeclaration declaration);

    /// <summary>Destroys whatever one durable run identity holds and declares it afresh over a new document.</summary>
    /// <param name="canonicalDocument">The canonical bytes of the document the name is to run from now on.</param>
    /// <param name="declaration">What the run is called and when it checkpoints.</param>
    /// <returns>The ticket addressing the replacement.</returns>
    /// <exception cref="PipelineRejectedException">
    /// The same refusals a declaration meets: the bytes are not a canonical document or are longer than one
    /// will decode, the document declares too many nodes, it belongs to another pipeline, the run identity
    /// is not a valid one, this silo registers no checkpoint store, the document does not resolve against
    /// this silo's catalog and factories, or the run identity is new and the register is full.
    /// </exception>
    /// <exception cref="Orleans.Dataflow.Hosting.CheckpointConflictException">
    /// The stored checkpoint moved between this call reading it and clearing it, so somebody is still
    /// writing under the identity being replaced. Retrying the replacement is safe and is the answer.
    /// </exception>
    /// <remarks>
    /// <para>
    /// <b>This is the destructive operation and it is spelled to say so.</b> The checkpoint stored for the
    /// run is <em>cleared</em> — a position taken of the old document could not describe the new one and
    /// migrating it is a recorded deferral (ADR 0007), not something a silo will attempt — and a fresh epoch
    /// supersedes whatever was executing: its control calls are fenced from the moment this returns and its
    /// next capture is refused by a store it no longer holds an ETag for. Nothing here is a migration and
    /// nothing here is silent.
    /// </para>
    /// <para>
    /// <b>Replacing does not require the document to differ.</b> A name replaced with the very document it
    /// already held is "run this from the beginning again", which is the only way to re-run a durable run
    /// that has finished; a name replaced with a new one is a revision taking over an identity. The two are
    /// the same operation because they destroy the same thing, and a caller that meant neither should be
    /// calling <see cref="DeclareDurableRunAsync"/>, which refuses a changed document by name rather than
    /// acting on it.
    /// </para>
    /// <para>
    /// <b>It fences the previous attempt and does not stop it, because it may not.</b> Killing a run would
    /// mean awaiting a run grain from a member that writes the register, which is the one edge this grain's
    /// shape forbids. What stops the previous attempt is the <em>start</em> of the replacement: Orleans
    /// permits one activation per run grain, so the activation
    /// <see cref="IPipelineRunGrain.EnsureStartedAsync"/> reaches is the one hosting it, and it abandons what
    /// it is holding before taking up the newer declaration. A caller that replaces here and never starts the
    /// replacement leaves the old attempt running until its next capture is refused by a store it no longer
    /// holds an ETag for — or, if it declared no timing at all and therefore never captures, until something
    /// else ends it. That is stated rather than smoothed over, and it is why a replacement is an operator's
    /// decision.
    /// </para>
    /// </remarks>
    Task<PipelineRunTicket> ReplaceDurableRunAsync(byte[] canonicalDocument, DurableRunDeclaration declaration);

    /// <summary>Destroys everything one durable run identity holds and forgets that it existed.</summary>
    /// <param name="runId">The run identity to retire.</param>
    /// <returns>
    /// <see langword="true"/> when a declaration was retired; <see langword="false"/> when this coordinator
    /// held none under that identity, which is what a retirement that has already happened answers.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="runId"/> is not a valid run identifier.</exception>
    /// <exception cref="Orleans.Dataflow.Hosting.CheckpointConflictException">
    /// The stored checkpoint moved between this call reading it and clearing it, so something is still
    /// writing under the identity being retired. Retrying the retirement is safe and is the answer.
    /// </exception>
    /// <remarks>
    /// <para>
    /// <b>The runbook operation, and it is destructive in exactly the way
    /// <see cref="ReplaceDurableRunAsync"/> is.</b> The stored checkpoint is cleared, whatever was executing
    /// is superseded, and the record is removed — which is the one thing a replacement does not do, and the
    /// reason this exists: a register that only ever grows eventually cannot be written at all, and a
    /// coordinator that cannot write its state cannot start a run either. A deployment that names durable
    /// runs after something it does not control — a tenant, a day, a request — needs a way to say "this one
    /// is finished with", and until this member there was none.
    /// </para>
    /// <para>
    /// <b>The order is the crash story, and it is the replacement's.</b> The store is emptied before the
    /// register is rewritten: cleared-then-still-recorded leaves a run that starts from the beginning, which
    /// its own at-least-once contract already admits, while removed-then-not-cleared leaves a checkpoint no
    /// declaration names — an orphan nothing would ever read or free. Retrying a retirement that failed
    /// halfway is therefore always safe.
    /// </para>
    /// <para>
    /// <b>It does not stop what is running, because it may not.</b> Killing a run would mean awaiting a run
    /// grain from a member that rewrites the register, which is the one edge this grain's shape forbids. What
    /// ends a retired run is its own next capture, refused by a store it no longer holds an ETag for; a run
    /// that declared no timing and therefore never captures runs on until something else ends it. That is
    /// stated rather than smoothed over, and it is the same sentence a replacement carries.
    /// </para>
    /// <para>
    /// <b>Nothing is retired implicitly.</b> A finished run keeps its record and its checkpoint, because how
    /// a run ended and how far it got are the two questions asked afterwards; forgetting them is a decision a
    /// deployment makes here, by name, one identity at a time.
    /// </para>
    /// </remarks>
    Task<bool> RetireDurableRunAsync(string runId);

    /// <summary>Records that one durable run reached a terminal state and will not be continued.</summary>
    /// <param name="runId">The run identity from its ticket.</param>
    /// <param name="terminal">
    /// The terminal snapshot the reporting attempt read of itself, carrying the epoch it owns the run under.
    /// </param>
    /// <returns>A task that completes when the declaration has been marked finished.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="runId"/> is not a valid run identifier, or the phase reported is not one a durable
    /// run is finished by.
    /// </exception>
    /// <exception cref="PipelineFencingException">
    /// The reporting attempt no longer owns the run, so what it is reporting the end of is somebody else's
    /// work.
    /// </exception>
    /// <remarks>
    /// <para>
    /// <b>Called by the run grain and never by a client</b>, and it is the second of the two calls that flow
    /// from a run back to its coordinator. It exists because a checkpoint says <em>where</em> a run reached
    /// and never <em>whether</em> it is over: without this, a run that completed and then lost its activation
    /// was indistinguishable from one that died at the same position, so the next activation continued it and
    /// re-ran its tail.
    /// </para>
    /// <para>
    /// <b>Completing and failing are endings; cancelling is not.</b> A deactivation cancels the run it was
    /// hosting, so accepting a cancellation here would retire every durable run whose silo recycled — the
    /// exact behaviour durability exists to prevent. A cancelled durable run is therefore continued by its
    /// next activation exactly as a crashed one is, and the phase is refused by name.
    /// </para>
    /// <para>
    /// It writes the register and calls nothing, which is what keeps the two-grain edge one-way: the members
    /// here that touch state await no run grain, and the members that await a run grain touch no state.
    /// </para>
    /// </remarks>
    Task ReportDurableRunEndedAsync(string runId, RunStatusSnapshot terminal);

    /// <summary>Reads what the activation about to host one declared durable run needs to run it.</summary>
    /// <param name="runId">The run identity from its ticket.</param>
    /// <returns>
    /// The declaration as it stands, or <see langword="null"/> when this coordinator has no durable
    /// declaration under that identity.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="runId"/> is not a valid run identifier.</exception>
    /// <remarks>
    /// <para>
    /// <b>Called by the run grain and never by a client.</b> It is the first of the calls that flow from a
    /// run back to its coordinator, and it exists because a run grain holds nothing across an activation: the
    /// document and the timing live here, and so does the epoch sequence that orders claims to the run.
    /// </para>
    /// <para>
    /// <b>It reads and it changes nothing</b>, which is a correctness property and not an optimization. This
    /// used to mint a fresh ownership epoch on every call after the first, so anybody who merely asked what
    /// a durable run was superseded the activation that was executing it: that activation's own report of how
    /// the run ended was then refused as stale, no outcome was recorded, and the next activation resumed a
    /// finished run and re-ran its tail. Ownership is taken by <see cref="TakeDurableRunAsync"/>, once, by
    /// the activation that is about to host the run — so a reader fences nobody and the epoch this returns is
    /// simply the one the run is currently owned under.
    /// </para>
    /// <para>
    /// An identity this coordinator has no record of answers <see langword="null"/> rather than refusing.
    /// That is what an ordinary, non-durable run's grain asks and hears, and it is why an activation of one
    /// still reports a lost attempt exactly as it did before durability existed.
    /// </para>
    /// <para>
    /// A run whose last attempt reported an ending answers with that ending. The reading activation reports
    /// the terminal state instead of materializing anything and never goes on to take ownership, which is
    /// what stops a finished run being resumed.
    /// </para>
    /// </remarks>
    Task<DurableRunClaim?> ClaimDurableRunAsync(string runId);

    /// <summary>Takes ownership of one declared durable run for the activation that is now hosting it.</summary>
    /// <param name="runId">The run identity from its ticket.</param>
    /// <returns>
    /// The ownership epoch the run is now claimed under, or <see langword="null"/> when this coordinator no
    /// longer holds a declaration to take — because it was retired or replaced, or because its last attempt
    /// reported an ending — in which case there is nothing for the caller to host.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="runId"/> is not a valid run identifier.</exception>
    /// <remarks>
    /// <para>
    /// <b>Called by the run grain and never by a client</b>, and it is the one place an ownership epoch is
    /// minted for a durable run. The first take after a declaration answers with the epoch the declaration
    /// recorded, because nothing has owned the run yet and a second number would make the ticket the
    /// declaring client is holding stale before the run had produced an element. Every later take answers
    /// with a fresh one, because it is a resume — a new claim to the same run — and the previous attempt's
    /// calls must stop being current the moment somebody else owns it.
    /// </para>
    /// <para>
    /// <b>Taking is separate from reading because only the taker is about to execute the run.</b> Orleans
    /// permits one activation per run grain, so an activation asking for ownership is the one place in the
    /// cluster that will have it; a number handed to anybody who asked would fence the very attempt it named.
    /// </para>
    /// <para>
    /// It writes the register and calls nothing, which is what keeps the two-grain edge one-way.
    /// </para>
    /// </remarks>
    Task<long?> TakeDurableRunAsync(string runId);

    /// <summary>Reports whether one ownership epoch is one this coordinator issued.</summary>
    /// <param name="epoch">The epoch offered.</param>
    /// <returns><see langword="true"/> when this coordinator has issued that epoch.</returns>
    /// <remarks>
    /// <para>
    /// <b>The protocol's own check that an epoch is real.</b> A run grain is handed an epoch by whoever
    /// starts it and used to store whatever it was given: a start carrying <c>long.MaxValue</c> therefore
    /// wedged the grain forever, because every later declaration compared as older and was answered with the
    /// number nobody could outbid. An epoch is this coordinator's to issue, so this is where "did you issue
    /// this?" is asked, and an epoch above the highest one issued is refused by the grain that was offered
    /// it.
    /// </para>
    /// <para>
    /// <b>What it does not claim.</b> The sequence is per pipeline and does not bind a number to a run, so
    /// this confirms that an epoch was issued by this coordinator and not that it was issued <em>for the run
    /// being started</em>. Binding that would mean recording every ordinary start, which is the unbounded
    /// register this state deliberately no longer keeps.
    /// </para>
    /// <para>
    /// It interleaves, and it must: the coordinator asks a run grain to start a run while holding its own
    /// turn, so a confirmation that queued behind that call would be waiting for the very call that is
    /// waiting for it. It reads one number and writes nothing, which is what makes interleaving safe.
    /// </para>
    /// </remarks>
    [AlwaysInterleave]
    Task<bool> HasIssuedEpochAsync(long epoch);

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
