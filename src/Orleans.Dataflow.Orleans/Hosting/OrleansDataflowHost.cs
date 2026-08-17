using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;
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

        byte[] canonical = GraphDocumentSerializer.Serialize(pipeline.Document);

        PipelineRunTicket ticket = await _grains
            .GetGrain<IPipelineCoordinatorGrain>(pipeline.Id.Value)
            .StartRunAsync(canonical)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);

        IPipelineRunGrain run = _grains.GetGrain<IPipelineRunGrain>($"{ticket.GraphId}/{ticket.RunId}");

        return new OrleansRunHandle(run, ticket, pipeline.Fingerprint, _options.PollInterval);
    }
}
