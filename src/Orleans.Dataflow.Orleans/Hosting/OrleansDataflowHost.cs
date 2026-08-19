using System.Diagnostics;
using System.Globalization;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Diagnostics;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Hosting;

/// <summary>
/// The cluster-facing host that turns a pipeline definition into a running run.
/// </summary>
/// <remarks>
/// <para>
/// The counterpart of <see cref="LocalDataflowHost"/> for the deployable plane, and the same contract:
/// building a pipeline describes it and starts nothing, a host materializes it, and materializing one
/// pipeline twice yields two independent runs whose handles answer only for themselves.
/// </para>
/// <para>
/// The host is stateless and holds no run. It can be constructed over an <see cref="IGrainFactory"/> —
/// which is what a silo has, so a grain can materialize a pipeline too — or over an
/// <see cref="IClusterClient"/> from outside the cluster; the two are the same interface as far as
/// anything here is concerned.
/// </para>
/// <para>
/// Documents travel as canonical bytes and never as Orleans-serialized object graphs. That is what makes
/// the fingerprint the client computed and the fingerprint the silo computed the same number, which the
/// returned ticket reports so that a caller can check rather than assume.
/// </para>
/// </remarks>
public sealed class OrleansDataflowHost
{
    private readonly IGrainFactory _grains;
    private readonly OrleansDataflowClientOptions _options;

    /// <summary>Initializes a new instance of the <see cref="OrleansDataflowHost"/> class.</summary>
    /// <param name="grains">The grain factory to address the cluster through.</param>
    /// <exception cref="ArgumentNullException"><paramref name="grains"/> is <see langword="null"/>.</exception>
    public OrleansDataflowHost(IGrainFactory grains)
        : this(grains, new OrleansDataflowClientOptions())
    {
    }

    /// <summary>Initializes a new instance of the <see cref="OrleansDataflowHost"/> class.</summary>
    /// <param name="grains">The grain factory to address the cluster through.</param>
    /// <param name="options">How this host watches the runs it starts.</param>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    public OrleansDataflowHost(IGrainFactory grains, OrleansDataflowClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(grains);
        ArgumentNullException.ThrowIfNull(options);

        _grains = grains;
        _options = options;
    }

    /// <summary>Materializes a pipeline into a running run in the cluster.</summary>
    /// <param name="pipeline">The pipeline to run.</param>
    /// <param name="cancellationToken">A token that stops this call; it does not stop a started run.</param>
    /// <returns>The handle of the started run.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="pipeline"/> is <see langword="null"/>.</exception>
    /// <exception cref="PipelineRejectedException">
    /// The silo refused the document: it belongs to another pipeline, it does not validate against the
    /// silo's catalog, or a provider it names has no runtime factory registered there. The message carries
    /// every diagnostic rather than the first.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The run is started before this method returns. There is no separate start step, for the reason the
    /// local host has none: a materialized run that had not started would be a state with no use and one
    /// more thing to get wrong.
    /// </para>
    /// <para>
    /// The token cancels the caller's wait for the start to be accepted. It deliberately does not cancel a
    /// run that has already started — that is what the handle's disposal is for — because a token that
    /// abandoned a started run without stopping it would leave work running that nobody holds a handle to.
    /// </para>
    /// </remarks>
    public async Task<OrleansRunHandle> MaterializeAsync(
        PipelineDefinition pipeline,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);

        using Activity? activity = DataflowDiagnostics.Materializing(
            pipeline.Fingerprint.ToString(),
            durable: false);

        try
        {
            byte[] canonical = GraphDocumentSerializer.Serialize(pipeline.Document);

            PipelineRunTicket ticket = await _grains
                .GetGrain<IPipelineCoordinatorGrain>(pipeline.Id.Value)
                .StartRunAsync(canonical)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            IPipelineRunGrain run = _grains.GetGrain<IPipelineRunGrain>($"{ticket.GraphId}/{ticket.RunId}");

            DataflowDiagnostics.Materialized(activity, ticket.RunId);

            return new OrleansRunHandle(run, ticket, pipeline.Fingerprint, _options.PollInterval, durable: false);
        }
        catch (Exception refused)
        {
            DataflowDiagnostics.MaterializeFailed(activity, refused);

            throw;
        }
    }

    /// <summary>Materializes a pipeline into a running run that can outlive the silo hosting it.</summary>
    /// <param name="pipeline">The pipeline to run.</param>
    /// <param name="durable">What the run is called and when it takes a checkpoint.</param>
    /// <param name="cancellationToken">A token that stops this call; it does not stop a started run.</param>
    /// <returns>The handle of the started run.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="pipeline"/> or <paramref name="durable"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <see cref="DurablePipelineOptions.RunId"/> is not a valid run identifier,
    /// <see cref="DurablePipelineOptions.Interval"/> is not positive, or
    /// <see cref="DurablePipelineOptions.EveryElements"/> is below one.
    /// </exception>
    /// <exception cref="PipelineRejectedException">
    /// The silo refused the document, or it registers no checkpoint store for a durable run to write to.
    /// </exception>
    /// <exception cref="PipelineResumeRefusedException">
    /// The run identity is already declared for a different document. V1 continues one document per durable
    /// run identity.
    /// </exception>
    /// <remarks>
    /// <para>
    /// <b>Two hops and both of them matter.</b> The coordinator is asked to <em>declare</em> the run — it
    /// records the document, the timing, and an epoch, and returns without starting anything — and then the
    /// run's own grain is asked to start it, which is the moment it claims that epoch and begins executing.
    /// Splitting the two is what makes resume need no protocol of its own: an activation that comes up after
    /// a silo died takes the second half of this very path on its own, so a crashed run continues by the
    /// same route it started by.
    /// </para>
    /// <para>
    /// <b>The run identity is the author's, and it is the one API-semantic change durability brings.</b>
    /// <see cref="MaterializeAsync"/> names each run afresh, so two calls are two runs; this call is named
    /// by <paramref name="durable"/>, so two calls under one name are one run — the second hands back a
    /// handle to the run that already exists, or continues it from its checkpoint when the silo hosting it
    /// has gone. A resume is the same run continuing, and a name allocated per attempt would leave nothing
    /// able to find the previous attempt's position.
    /// </para>
    /// <para>
    /// <b>The handle follows the run rather than the attempt.</b> A resumed attempt claims a fresh epoch, so
    /// a handle from before it holds a number that is out of date rather than wrong; it adopts the current
    /// epoch from the fencing refusal that names it and carries on. That is a durable handle's behavior
    /// alone — an ordinary run has no later attempt to follow.
    /// </para>
    /// </remarks>
    public async Task<OrleansRunHandle> MaterializeDurableAsync(
        PipelineDefinition pipeline,
        DurablePipelineOptions durable,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(durable);

        Guard(durable);

        using Activity? activity = DataflowDiagnostics.Materializing(
            pipeline.Fingerprint.ToString(),
            durable: true);

        try
        {
            byte[] canonical = GraphDocumentSerializer.Serialize(pipeline.Document);

            PipelineRunTicket declared = await _grains
                .GetGrain<IPipelineCoordinatorGrain>(pipeline.Id.Value)
                .DeclareDurableRunAsync(
                    canonical,
                    new DurableRunDeclaration
                    {
                        RunId = durable.RunId,
                        Interval = durable.Interval,
                        EveryElements = durable.EveryElements,
                    })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            OrleansRunHandle handle = await StartAsync(pipeline, declared, cancellationToken).ConfigureAwait(false);

            DataflowDiagnostics.Materialized(activity, handle.RunId);

            return handle;
        }
        catch (Exception refused)
        {
            DataflowDiagnostics.MaterializeFailed(activity, refused);

            throw;
        }
    }

    /// <summary>Destroys what a durable run identity holds and runs a document under it from the beginning.</summary>
    /// <param name="pipeline">The pipeline the identity is to run from now on.</param>
    /// <param name="durable">What the run is called and when it takes a checkpoint.</param>
    /// <param name="cancellationToken">A token that stops this call; it does not stop a started run.</param>
    /// <returns>The handle of the replacement run.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="pipeline"/> or <paramref name="durable"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <see cref="DurablePipelineOptions.RunId"/> is not a valid run identifier, or one of the two bounds is
    /// not usable.
    /// </exception>
    /// <exception cref="PipelineRejectedException">
    /// The silo refused the document, or it registers no checkpoint store for a durable run to write to.
    /// </exception>
    /// <exception cref="CheckpointConflictException">
    /// The stored checkpoint moved between being read and being cleared, so something is still writing under
    /// the identity. Retrying the replacement is safe and is the answer.
    /// </exception>
    /// <remarks>
    /// <para>
    /// <b>The destructive spelling, and the only one.</b> <see cref="MaterializeDurableAsync"/> refuses a run
    /// identity that already holds a different document, by name and with both fingerprints, because
    /// migrating a checkpoint across a changed graph is a recorded deferral rather than something a cluster
    /// will guess at (ADR 0007). This is what a deployment says when it means the other thing: <b>the stored
    /// checkpoint is cleared</b>, the previous attempt is <b>superseded by a fresh epoch</b>, and the
    /// document runs from the beginning under the name it took over.
    /// </para>
    /// <para>
    /// <b>The document does not have to differ.</b> Replacing an identity with the very document it already
    /// held is how a finished durable run is run again — a run that has ended stays ended, and no poll
    /// revives it — and replacing it with a new revision is how an identity moves forward. Both destroy the
    /// same thing, which is why they are one call.
    /// </para>
    /// <para>
    /// <b>The previous attempt is abandoned by the second of this call's two hops.</b> The coordinator only
    /// fences it — the member that rewrites the register may not await a run grain — but Orleans permits one
    /// activation per run grain, so the activation this call then asks to start is the very one hosting the
    /// old attempt, and it disposes that engine before starting the replacement. What is left over is the
    /// window between the two hops, in which the old attempt is executing under a claim that is already
    /// stale; a capture taken in it is refused by a store it no longer holds an ETag for.
    /// </para>
    /// </remarks>
    public async Task<OrleansRunHandle> ReplaceDurableRunAsync(
        PipelineDefinition pipeline,
        DurablePipelineOptions durable,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(durable);

        Guard(durable);

        using Activity? activity = DataflowDiagnostics.Materializing(
            pipeline.Fingerprint.ToString(),
            durable: true);

        try
        {
            byte[] canonical = GraphDocumentSerializer.Serialize(pipeline.Document);

            PipelineRunTicket declared = await _grains
                .GetGrain<IPipelineCoordinatorGrain>(pipeline.Id.Value)
                .ReplaceDurableRunAsync(
                    canonical,
                    new DurableRunDeclaration
                    {
                        RunId = durable.RunId,
                        Interval = durable.Interval,
                        EveryElements = durable.EveryElements,
                    })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            OrleansRunHandle handle = await StartAsync(pipeline, declared, cancellationToken).ConfigureAwait(false);

            DataflowDiagnostics.Materialized(activity, handle.RunId);

            return handle;
        }
        catch (Exception refused)
        {
            DataflowDiagnostics.MaterializeFailed(activity, refused);

            throw;
        }
    }

    /// <summary>Destroys everything one durable run identity holds and forgets that it existed.</summary>
    /// <param name="pipelineId">The identity of the pipeline the run belongs to.</param>
    /// <param name="runId">What the run is called.</param>
    /// <param name="cancellationToken">A token that stops this wait; it does not stop a running attempt.</param>
    /// <returns>
    /// A task carrying <see langword="true"/> when a declaration was retired, and <see langword="false"/>
    /// when the cluster held none under that identity — which is what a retirement already carried out
    /// answers, so a runbook step is safe to repeat.
    /// </returns>
    /// <exception cref="ArgumentNullException">Either identifier is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="pipelineId"/> or <paramref name="runId"/> is not a valid identifier.
    /// </exception>
    /// <exception cref="CheckpointConflictException">
    /// The stored checkpoint moved between being read and being cleared, so something is still writing under
    /// the identity. Retrying the retirement is safe and is the answer.
    /// </exception>
    /// <remarks>
    /// <para>
    /// <b>The runbook operation, and it is destructive in exactly the way
    /// <see cref="ReplaceDurableRunAsync"/> is</b> — the checkpoint is cleared and whatever was executing is
    /// superseded — with the one difference that gives it its name: the declaration is <em>removed</em>
    /// rather than rewritten. A replacement takes a name forward onto a new document; this gives the name
    /// up.
    /// </para>
    /// <para>
    /// <b>It exists because the register of durable names is a thing that grows.</b> A record holds the
    /// document it names, a coordinator rewrites the whole register on every declaration, and a deployment
    /// that names durable runs after something outside its control — a tenant, a day, a customer — otherwise
    /// grows a state document until its storage provider will not accept it, at which point that pipeline
    /// cannot start any run at all. A cap refuses the thousand-and-first name; this is what makes room for
    /// it.
    /// </para>
    /// <para>
    /// <b>It takes names rather than a pipeline.</b> Every other member here takes the
    /// <see cref="PipelineDefinition"/> because it is about to run it, and a document is what a run needs; a
    /// retirement is about to destroy one, and an operator carrying out a runbook has the two identifiers and
    /// no reason to be able to rebuild the document as well.
    /// </para>
    /// <para>
    /// <b>It does not stop what is running, because the cluster may not.</b> What ends a retired run is its
    /// own next capture, refused by a store it no longer holds an ETag for; a run that declared no timing at
    /// all and therefore never captures runs on until something else ends it. That is the sentence a
    /// replacement carries too, and it is why both are an operator's decision.
    /// </para>
    /// </remarks>
    public async Task<bool> RetireDurableRunAsync(
        string pipelineId,
        string runId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(pipelineId);
        ArgumentNullException.ThrowIfNull(runId);

        if (!GraphId.TryCreate(pipelineId, out GraphId pipeline))
        {
            throw new ArgumentException(
                $"'{pipelineId}' is not a valid pipeline identifier, so it addresses no coordinator that could hold a durable run.",
                nameof(pipelineId));
        }

        if (!RunId.TryCreate(runId, out _))
        {
            throw new ArgumentException(
                $"'{runId}' is not a valid run identifier, so it names no durable run a cluster could have declared.",
                nameof(runId));
        }

        return await _grains
            .GetGrain<IPipelineCoordinatorGrain>(pipeline.Value)
            .RetireDurableRunAsync(runId)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>Drives the second hop of a durable materialization and composes the handle.</summary>
    /// <param name="pipeline">The pipeline, whose fingerprint the handle validates slots against.</param>
    /// <param name="declared">The ticket the declaration or the replacement answered with.</param>
    /// <param name="cancellationToken">A token that stops the wait for the start to be accepted.</param>
    /// <returns>The handle.</returns>
    /// <remarks>
    /// Shared by the two durable spellings, and sharing it is the statement: declaring and replacing differ
    /// in exactly one call to the coordinator and in nothing a run does afterwards. The epoch is composed
    /// from both answers rather than taken from the declaration, because the two can differ by one attempt —
    /// declaring a run whose previous host died records nothing new, and the activation that then picks it up
    /// claims a fresh epoch. Taking the live number here is what keeps the returned handle's first call from
    /// being fenced by a run it just started.
    /// </remarks>
    private async Task<OrleansRunHandle> StartAsync(
        PipelineDefinition pipeline,
        PipelineRunTicket declared,
        CancellationToken cancellationToken)
    {
        IPipelineRunGrain run = _grains.GetGrain<IPipelineRunGrain>($"{declared.GraphId}/{declared.RunId}");

        long epoch = await run
            .EnsureStartedAsync(declared.Epoch)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);

        PipelineRunTicket ticket = new()
        {
            GraphId = declared.GraphId,
            RunId = declared.RunId,
            Epoch = epoch,
            GraphFingerprint = declared.GraphFingerprint,
            CatalogFingerprint = declared.CatalogFingerprint,
        };

        return new OrleansRunHandle(run, ticket, pipeline.Fingerprint, _options.PollInterval, durable: true);
    }

    /// <summary>Refuses declared durability this host could not honor.</summary>
    /// <param name="durable">The declaration.</param>
    /// <exception cref="ArgumentException">The identity or one of the two bounds is not usable.</exception>
    /// <remarks>
    /// Checked here as well as by the silo, and both are worth having: this one makes a mistake a fast,
    /// well-worded exception on the caller's own thread, and the silo's makes it impossible for a
    /// hand-built call to get past.
    /// </remarks>
    private static void Guard(DurablePipelineOptions durable)
    {
        if (!RunId.TryCreate(durable.RunId, out _))
        {
            throw new ArgumentException(
                $"'{durable.RunId}' is not a valid run identifier, so nothing could address the run or key its checkpoints by it. A durable run is named by whoever will resume it, and the name has to be one this runtime can address.",
                nameof(durable));
        }

        if (durable.Interval is { } interval && interval <= TimeSpan.Zero)
        {
            throw new ArgumentException(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"A checkpoint interval of {interval} describes a capture that is due forever. Declare a positive interval, or leave {nameof(DurablePipelineOptions.Interval)} unset and checkpoint on elements alone."),
                nameof(durable));
        }

        if (durable.EveryElements is { } elements && elements < 1)
        {
            throw new ArgumentException(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"A checkpoint bound of {elements} elements describes a capture that is due before an element exists. Declare a bound of at least one, or leave {nameof(DurablePipelineOptions.EveryElements)} unset and checkpoint on time alone."),
                nameof(durable));
        }
    }
}
