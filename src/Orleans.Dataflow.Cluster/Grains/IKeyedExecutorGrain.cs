using Orleans.Dataflow.Adapters;

namespace Orleans.Dataflow.Grains;

/// <summary>
/// The addressable worker of one key of one keyed grain-call stage: the first thing in this design that
/// distributes below a run.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a grain per key.</b> M3's doctrine is that runs distribute before stages do, and this is the one
/// stage whose semantics ask for the exception: a keyed call is per-key work, and per-key work belongs on
/// whichever silo the cluster wants it on rather than all on the silo that happens to host the run. One
/// activation per key is what turns the declared key into a placement decision, and it is why the key is
/// part of this grain's identity rather than a parameter of its method.
/// </para>
/// <para>
/// <b>Identity, and its lifetime.</b> The key is <c>{graph}/{run}/{node}/{key}</c> — the run's own grain key,
/// the occurrence inside that run's document, and the key the stage's registered extractor produced. Every
/// part is load-bearing. The run makes an executor private to one attempt, so two runs of one pipeline never
/// share a worker and a key's work never outlives the run that asked for it; the occurrence separates two
/// keyed stages in one document; the key is the partition. Nothing is persisted here and nothing is meant to
/// survive: an executor holds one call at a time and remembers nothing between calls, so losing an
/// activation loses no work that was not already in flight, and an in-flight call is lost exactly as any
/// awaited grain call is when its host dies.
/// </para>
/// <para>
/// <b>Collection rather than teardown, stated plainly.</b> The engine's asynchronous-stage seam has no
/// per-run teardown hook — a flow stage is a callback and a concurrency bound and nothing else — so a run
/// cannot deactivate the executors it used when it ends. What it can do is make them cheap to leave behind
/// and quick to collect: an executor that is not being called holds no state at all, and the shortened
/// collection age above is what turns "dies with the run" from a promise into a bounded delay. The honest
/// statement is therefore that an executor outlives its run by at most that idle period, holding nothing.
/// </para>
/// <para>
/// <b>Concurrency.</b> Not reentrant, which is deliberate and is the second half of the stage's ordering
/// contract: one call at a time per key, whatever else the cluster is doing. The first half is the caller's
/// — it keeps exactly one call in flight per key — and together they mean the grain behind a key sees one
/// element at a time, in the order the run produced them, without depending on the transport to order
/// anything. Orleans documents no pairwise ordering between activations, and a probe in this repository's
/// suite shows it does reorder pipelined calls even within one silo, so the stage does not ask it to.
/// </para>
/// </remarks>
internal interface IKeyedExecutorGrain : IGrainWithStringKey
{
    /// <summary>Runs one element of this executor's key through a registered keyed call.</summary>
    /// <param name="call">The name of the registered keyed call to run.</param>
    /// <param name="element">The element, which must satisfy Orleans serialization.</param>
    /// <param name="cancellationToken">The run's token, carried end to end.</param>
    /// <returns>The reply, which is also the credit that lets the run send this key's next element.</returns>
    /// <exception cref="PipelineRejectedException">
    /// The silo hosting this executor registers no keyed call under that name.
    /// </exception>
    /// <exception cref="KeyedExecutionFailedException">The registered call threw.</exception>
    /// <remarks>
    /// <b>The reply is the whole credit protocol.</b> There is no grant message and no credit member on the
    /// wire: a run holds one call in flight per key, so this reply arriving is exactly what permits the next
    /// send for this key, and the stage's declared bound is what permits the next send for a different one.
    /// Grants ride on replies in the strongest sense available — the reply <em>is</em> the grant.
    /// </remarks>
    Task<object?> ExecuteAsync(string call, object? element, CancellationToken cancellationToken);
}

/// <summary>
/// The executor grain: one key's worker, holding a registry lookup and nothing else.
/// </summary>
/// <remarks>
/// The collection age is shortened from the cluster's default because these activations are made by the
/// thousand and abandoned by the runful. Two minutes is above Orleans' collection quantum, so it is a value
/// the runtime accepts, and it bounds how long a finished run's workers linger.
/// </remarks>
[CollectionAgeLimit(Minutes = 2)]
internal sealed class KeyedExecutorGrain(OrleansAdapterRegistry registry, IGrainFactory grains)
    : Grain, IKeyedExecutorGrain
{
    /// <inheritdoc/>
    public async Task<object?> ExecuteAsync(string call, object? element, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(call);

        // The silo running the executor is not necessarily the silo that materialized the run, so the name
        // is resolved here as well as there. A cluster whose silos registered different vocabularies is a
        // documented deployment error, and this is where it surfaces for a distributed keyed stage: as a
        // refusal naming the call, rather than as a null reference on a worker nobody is watching.
        if (!registry.TryGetKeyedCall(call, out IKeyedGrainCallEntry? entry))
        {
            throw new PipelineRejectedException(
                $"The executor '{this.GetPrimaryKeyString()}' was asked to run the keyed call '{call}', and the silo hosting it registers no keyed call under that name. A distributed keyed stage runs on any silo of the cluster, so every silo that may host one registers the same bindings.");
        }

        try
        {
            // No ConfigureAwait(false): grain code stays on its activation's context, which is what makes
            // an activation single-threaded, and Orleans' own analyzer refuses the attempt to leave it.
            return await entry!.InvokeAsync(grains, element, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // A cancelled run is not a failed call, and folding it into one would make every shutdown look
            // like a fault. The cancellation is what the caller asked for, so it travels as itself.
            throw;
        }
        catch (Exception thrown)
        {
            // Folded into text rather than chained: see KeyedExecutionFailedException for why an exception
            // chain cannot be trusted to cross this boundary.
            throw new KeyedExecutionFailedException(
                this.GetPrimaryKeyString(),
                call,
                thrown.GetType().FullName,
                thrown.Message);
        }
    }
}
