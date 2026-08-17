using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.BroadcastChannel;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Dataflow.Adapters;

/// <summary>
/// Builds every stage of the Orleans adapter vocabulary.
/// </summary>
/// <param name="services">The silo's container, which the named stream providers are resolved from.</param>
/// <param name="grains">The silo's grain factory, which every registered call is handed.</param>
/// <param name="registry">What this silo registered for the adapters.</param>
/// <remarks>
/// <para>
/// One factory for the whole provider, dispatching on the node's stage reference, which is the shape the
/// seam asks for and the shape a real provider has. Everything it needs beyond the node is what the silo's
/// container gave it, which is the seam's rule stated as a constructor: a factory receives no document, no
/// run identity, and no services beyond what it was constructed with.
/// </para>
/// <para>
/// <b>Where the work runs.</b> A run's engine executes on dedicated threads of its own and never on a grain
/// turn. Everything this factory builds therefore runs off any grain context, which was probed rather than
/// assumed: a silo's container resolves its stream providers, a subscription and a publication both work
/// from there, and a grain reference is callable from a long-running thread inside the silo. That is what
/// keeps the run grain's turns free while a run is in flight — a delivery that had to park a grain turn
/// would put a shutdown request behind the backpressure it was meant to relieve.
/// </para>
/// <para>
/// <b>What is checked here and what is not.</b> The payload has already been validated against this silo's
/// registry by the graph compiler, so a name resolved here is a name that resolved there. What this
/// re-checks is only what a build can still fail on and a validator could not see: whether the silo really
/// has a stream provider under the name the document gave. A missing provider is therefore a refusal of the
/// start rather than a failure of the run.
/// </para>
/// </remarks>
internal sealed class OrleansStageFactory(
    IServiceProvider services,
    IGrainFactory grains,
    OrleansAdapterRegistry registry) : IDataflowStageFactory
{
    /// <inheritdoc/>
    public DataflowStageRuntime Create(DataflowStageRequest request)
    {
        StageNode node = request.Node;

        if (node.Stage == OrleansStages.StreamSourceStage)
        {
            return StreamSource(node);
        }

        if (node.Stage == OrleansStages.StreamSinkStage)
        {
            return StreamSink(node);
        }

        if (node.Stage == OrleansStages.GrainCallStage)
        {
            return GrainCall(node);
        }

        if (node.Stage == OrleansStages.GrainCallSinkStage)
        {
            return GrainCallSink(node);
        }

        if (node.Stage == OrleansStages.GrainEnumerableStage)
        {
            return GrainEnumerable(node);
        }

        if (node.Stage == OrleansStages.ReminderTriggerStage)
        {
            return ReminderTrigger(node);
        }

        if (node.Stage == OrleansStages.ObserverBridgeStage)
        {
            return ObserverBridge(node);
        }

        if (node.Stage == OrleansStages.BroadcastSinkStage)
        {
            return BroadcastSink(node);
        }

        throw new InvalidOperationException(
            $"The node '{node.Id}' is an occurrence of '{node.Stage}', which the Orleans adapter provider does not implement.");
    }

    /// <summary>Subscribes to a stream and feeds a run's bounded ingress from it.</summary>
    /// <param name="element">The element binding that types the subscription.</param>
    /// <param name="provider">The stream provider.</param>
    /// <param name="stream">The stream identity.</param>
    /// <param name="ingress">The bound and policy of the ingress.</param>
    /// <param name="tokens">The run's tokens.</param>
    /// <returns>The sequence the run pulls.</returns>
    /// <remarks>
    /// <para>
    /// The subscription is made when the run first pulls and is cancelled in the <c>finally</c> that the
    /// engine reaches on every terminal path — completion, failure, cancellation, a graceful shutdown, and
    /// the deactivation of the run grain, which cancels the run and awaits its disposal. Nothing about the
    /// subscription outlives the enumeration that holds it, which is exactly why the run grain is not its
    /// consumer: an explicit subscription owned by a grain survives that grain's deactivation and has to be
    /// resumed, and a run that faults on deactivation would leave one behind with nobody to resume it.
    /// </para>
    /// <para>
    /// The ingress is ended before the subscription is cancelled, and the order matters: ending it releases
    /// a delivery parked for room with a refusal, so the provider's pulling agent is freed at once instead
    /// of waiting on a queue nobody will drain again.
    /// </para>
    /// </remarks>
    private static async IAsyncEnumerable<object?> Deliveries(
        IStreamElementEntry element,
        IStreamProvider provider,
        StreamId stream,
        BufferOptions ingress,
        DataflowRunTokens tokens)
    {
        LocalIngressQueue queue = new(ingress.Capacity, ingress.OverflowPolicy);
        object handle = await element.SubscribeAsync(provider, stream, new StreamIngress(queue))
            .ConfigureAwait(false);

        try
        {
            await foreach (object? delivered in queue
                .ElementsAsync(tokens.RunToken, tokens.StopToken)
                .ConfigureAwait(false))
            {
                yield return delivered;
            }
        }
        finally
        {
            queue.EndRun();

            await element.UnsubscribeAsync(handle).ConfigureAwait(false);
        }
    }

    /// <summary>Reads a node's payload or says the provider cannot.</summary>
    /// <typeparam name="TDeclaration">The declaration the payload produces.</typeparam>
    /// <param name="node">The node.</param>
    /// <param name="read">The reader.</param>
    /// <returns>The declaration.</returns>
    /// <exception cref="InvalidOperationException">The payload is not readable.</exception>
    /// <remarks>
    /// Unreachable by construction — the graph compiler ran the very same reader before a run was planned —
    /// and stated anyway, because a factory that dereferenced a null declaration would report a payload
    /// problem as a null reference somewhere else entirely.
    /// </remarks>
    private static TDeclaration Read<TDeclaration>(StageNode node, PayloadReader<TDeclaration> read)
        where TDeclaration : class
    {
        if (!read(node.Parameters, out TDeclaration? declaration, out IReadOnlyList<string> violations))
        {
            throw new InvalidOperationException(
                $"The node '{node.Id}', an occurrence of '{node.Stage}', carries parameters this provider cannot read: {string.Join("; ", violations)}.");
        }

        return declaration!;
    }

    /// <summary>Builds the stream subscription source.</summary>
    /// <param name="node">The node.</param>
    /// <returns>The runtime.</returns>
    private DataflowStageRuntime StreamSource(StageNode node)
    {
        StreamSourceDeclaration declaration = Read<StreamSourceDeclaration>(node, StreamSourcePayload.TryRead);
        IStreamElementEntry element = Element(node, declaration.Element);
        IStreamProvider provider = Provider(node, declaration.Address.Provider);
        StreamId stream = StreamId.Create(declaration.Address.Namespace, declaration.Address.Key);

        return DataflowStageRuntime.Source(tokens =>
            Deliveries(element, provider, stream, declaration.Ingress, tokens));
    }

    /// <summary>Builds the stream publication sink.</summary>
    /// <param name="node">The node.</param>
    /// <returns>The runtime.</returns>
    /// <remarks>
    /// A terminal, and therefore a synchronous fold: the publication is awaited by blocking the segment's
    /// own dedicated thread, which is what that thread is for and is the same thing a slow synchronous
    /// callback sink does. One element is published at a time, in the run's order, and the run does not
    /// advance past an element the provider has not accepted.
    /// </remarks>
    private DataflowStageRuntime StreamSink(StageNode node)
    {
        StreamSinkDeclaration declaration = Read<StreamSinkDeclaration>(node, StreamSinkPayload.TryRead);
        IStreamElementEntry element = Element(node, declaration.Element);
        IStreamProvider provider = Provider(node, declaration.Address.Provider);
        StreamId stream = StreamId.Create(declaration.Address.Namespace, declaration.Address.Key);

        return DataflowStageRuntime.Terminal(
            static () => null,
            (state, published) =>
            {
                element.PublishAsync(provider, stream, published).GetAwaiter().GetResult();

                return state;
            },
            finish: null,
            producesResult: false);
    }

    /// <summary>Builds the transforming grain call.</summary>
    /// <param name="node">The node.</param>
    /// <returns>The runtime.</returns>
    private DataflowStageRuntime GrainCall(StageNode node)
    {
        GrainCallDeclaration declaration = Read<GrainCallDeclaration>(
            node,
            (CanonicalJsonValue parameters, out GrainCallDeclaration? read, out IReadOnlyList<string> violations) =>
                GrainCallPayload.TryRead(parameters, expectsOutput: true, out read, out violations));

        if (!registry.TryGetCall(declaration.Call, out IGrainCallEntry? call))
        {
            throw Unregistered(node, "grain call", declaration.Call);
        }

        return DataflowStageRuntime.ElementAsync(
            (element, cancellationToken) => new ValueTask<object?>(GrainCallInvocation.InvokeAsync(
                token => call!.InvokeAsync(grains, element, token),
                declaration.Call,
                declaration.Timeout,
                cancellationToken)),
            declaration.MaxInFlight,
            ordered: true);
    }

    /// <summary>Builds the terminating grain call.</summary>
    /// <param name="node">The node.</param>
    /// <returns>The runtime.</returns>
    /// <remarks>
    /// The window is created per run by the seed factory, which is what the factory shape exists for: a
    /// window handed over as a value would be one set of in-flight calls that two runs of one pipeline both
    /// wrote into.
    /// </remarks>
    private DataflowStageRuntime GrainCallSink(StageNode node)
    {
        GrainCallDeclaration declaration = Read<GrainCallDeclaration>(
            node,
            (CanonicalJsonValue parameters, out GrainCallDeclaration? read, out IReadOnlyList<string> violations) =>
                GrainCallPayload.TryRead(parameters, expectsOutput: false, out read, out violations));

        if (!registry.TryGetCallSink(declaration.Call, out IGrainCallSinkEntry? call))
        {
            throw Unregistered(node, "grain call sink", declaration.Call);
        }

        return DataflowStageRuntime.Terminal(
            () => new GrainCallWindow(call!, grains, declaration.Call, declaration.MaxInFlight, declaration.Timeout),
            static (state, element) =>
            {
                ((GrainCallWindow)state!).Submit(element);

                return state;
            },
            static state =>
            {
                ((GrainCallWindow)state!).Drain();

                return null;
            },
            producesResult: false);
    }

    /// <summary>Builds the grain enumeration source.</summary>
    /// <param name="node">The node.</param>
    /// <returns>The runtime.</returns>
    private DataflowStageRuntime GrainEnumerable(StageNode node)
    {
        GrainEnumerableDeclaration declaration = Read<GrainEnumerableDeclaration>(
            node,
            GrainEnumerablePayload.TryRead);

        if (!registry.TryGetEnumerable(declaration.Source, out IGrainEnumerableEntry? source))
        {
            throw Unregistered(node, "grain enumerable", declaration.Source);
        }

        // The run token and not the stop token: a shutdown drains, and a cancelled enumeration would raise
        // where the engine expects a sequence that simply ended. The engine stops pulling between elements
        // when a shutdown is requested, which is what makes a drain work without cancelling the grain.
        return DataflowStageRuntime.Source(tokens => source!.Open(grains, tokens.RunToken));
    }

    /// <summary>Builds the reminder trigger source.</summary>
    /// <param name="node">The node.</param>
    /// <returns>The runtime.</returns>
    /// <remarks>
    /// The period is checked against this silo's configured minimum here and nowhere else. Which floor a
    /// cluster enforces is not a property of a payload, so no parameter validator could see it; and Orleans
    /// enforces it by throwing rather than by clamping — probed, not assumed — so a period below it would
    /// otherwise surface as an <see cref="ArgumentException"/> from the trigger's first registration, long
    /// after the run was accepted. Failing here makes it a refusal of the start that names the number.
    /// </remarks>
    private DataflowStageRuntime ReminderTrigger(StageNode node)
    {
        ReminderTriggerDeclaration declaration = Read<ReminderTriggerDeclaration>(
            node,
            ReminderTriggerPayload.TryRead);
        TimeSpan minimum = services.GetRequiredService<IOptions<ReminderOptions>>().Value.MinimumReminderPeriod;

        if (declaration.Period < minimum)
        {
            throw new InvalidOperationException(
                $"The node '{node.Id}', an occurrence of '{node.Stage}', declares a period of {declaration.Period} and this silo's ReminderOptions.MinimumReminderPeriod is {minimum}. Orleans refuses a shorter reminder outright rather than rounding it up, so the run is refused here rather than failing at its first tick.");
        }

        // The node's own identifier completes the key, so a graph that one day heads two chains with two
        // triggers gives each its own grain. Today a chain has one source, so the identifier is the only
        // part of the key that is not already the run's.
        string occurrence = node.Id.ToString();

        return DataflowStageRuntime.Source(tokens =>
            Ticks(grains, $"{tokens.RunIdentity}/{occurrence}", declaration, tokens));
    }

    /// <summary>Registers a reminder for one run and yields the ticks it delivers.</summary>
    /// <param name="grains">The silo's grain factory.</param>
    /// <param name="key">The trigger grain's key, composed from the run's identity and the node's.</param>
    /// <param name="declaration">The period and the ingress the ticks land in.</param>
    /// <param name="tokens">The run's tokens.</param>
    /// <returns>The sequence of tick indices.</returns>
    /// <remarks>
    /// <para>
    /// The receiver is created here rather than in the trigger grain because only here can it be created:
    /// Orleans refuses <c>CreateObjectReference</c> from inside a grain, and this runs on the run's own
    /// source thread, which is not inside one. It is deleted in the same <c>finally</c> that stops the
    /// trigger, so nothing about the bridge outlives the enumeration that holds it.
    /// </para>
    /// <para>
    /// The receiver object is kept alive by this method, and has to be: Orleans holds a hosted client's
    /// observer objects weakly, so a receiver nothing else roots is collected at the next garbage
    /// collection, the runtime silently unregisters it, and every later push finds nobody — a run that
    /// waits forever for a tick that was delivered to a dead reference. The <see cref="GC.KeepAlive"/> at
    /// the end of each path is that root, placed after the unsubscribe so the object outlives the last
    /// push the trigger could make.
    /// </para>
    /// <para>
    /// The ingress is ended before the trigger is stopped, and the order matters for the same reason it
    /// does everywhere else here: ending it makes every later offer answer at once, so a tick that arrives
    /// during the teardown is refused rather than left waiting on a run that has gone — and the refusal is
    /// what tells the trigger to unregister even if this call to stop it never lands.
    /// </para>
    /// </remarks>
    private static async IAsyncEnumerable<object?> Ticks(
        IGrainFactory grains,
        string key,
        ReminderTriggerDeclaration declaration,
        DataflowRunTokens tokens)
    {
        LocalIngressQueue queue = new(declaration.Ingress.Capacity, declaration.Ingress.OverflowPolicy);
        IReminderTriggerGrain trigger = grains.GetGrain<IReminderTriggerGrain>(key);
        PushReceiver receiver = new(queue, element: null);
        IDataflowPushReceiver reference = grains.CreateObjectReference<IDataflowPushReceiver>(receiver);

        try
        {
            await trigger.StartAsync(reference, (long)declaration.Period.TotalMilliseconds)
                .ConfigureAwait(false);
        }
        catch (Exception)
        {
            queue.EndRun();
            grains.DeleteObjectReference<IDataflowPushReceiver>(reference);
            GC.KeepAlive(receiver);

            throw;
        }

        try
        {
            await foreach (object? tick in queue
                .ElementsAsync(tokens.RunToken, tokens.StopToken)
                .ConfigureAwait(false))
            {
                yield return tick;
            }
        }
        finally
        {
            queue.EndRun();

            await Release(trigger.StopAsync, grains, reference).ConfigureAwait(false);

            // Orleans's observer table would let the receiver be collected mid-run; see the remarks.
            GC.KeepAlive(receiver);
        }
    }

    /// <summary>Builds the observer bridge source.</summary>
    /// <param name="node">The node.</param>
    /// <returns>The runtime.</returns>
    private DataflowStageRuntime ObserverBridge(StageNode node)
    {
        ObserverBridgeDeclaration declaration = Read<ObserverBridgeDeclaration>(
            node,
            ObserverBridgePayload.TryRead);

        if (!registry.TryGetBridge(declaration.Bridge, out IObserverBridgeEntry? bridge))
        {
            throw Unregistered(node, "observer bridge", declaration.Bridge);
        }

        return DataflowStageRuntime.Source(tokens => Pushes(
            grains,
            $"{tokens.RunIdentity}/{declaration.Bridge}",
            bridge!,
            declaration.Ingress,
            tokens));
    }

    /// <summary>Publishes one run's receiver on a bridge grain and yields what is pushed at it.</summary>
    /// <param name="grains">The silo's grain factory.</param>
    /// <param name="key">The bridge grain's key, composed from the run's identity and the binding's name.</param>
    /// <param name="bridge">The registered binding, which types the pushes.</param>
    /// <param name="ingress">The bound and policy of the ingress.</param>
    /// <param name="tokens">The run's tokens.</param>
    /// <returns>The sequence the run pulls.</returns>
    /// <remarks>
    /// The attachment is made when the run first pulls and dropped in the <c>finally</c> the engine reaches
    /// on every terminal path, so a bridge is listening exactly while its run is. The ingress is ended
    /// first, which releases a pusher parked for room with a refusal instead of leaving its grain call
    /// waiting on a run that has gone. The receiver object is rooted by this method for the same reason
    /// <see cref="Ticks"/> roots its own: Orleans holds observer objects weakly, and a collected receiver
    /// is unregistered silently, turning every later push into a delivery to nobody.
    /// </remarks>
    private static async IAsyncEnumerable<object?> Pushes(
        IGrainFactory grains,
        string key,
        IObserverBridgeEntry bridge,
        BufferOptions ingress,
        DataflowRunTokens tokens)
    {
        LocalIngressQueue queue = new(ingress.Capacity, ingress.OverflowPolicy);
        IObserverBridgeGrain grain = grains.GetGrain<IObserverBridgeGrain>(key);
        PushReceiver receiver = new(queue, bridge);
        IDataflowPushReceiver reference = grains.CreateObjectReference<IDataflowPushReceiver>(receiver);

        try
        {
            await grain.AttachAsync(reference).ConfigureAwait(false);
        }
        catch (Exception)
        {
            queue.EndRun();
            grains.DeleteObjectReference<IDataflowPushReceiver>(reference);
            GC.KeepAlive(receiver);

            throw;
        }

        try
        {
            await foreach (object? pushed in queue
                .ElementsAsync(tokens.RunToken, tokens.StopToken)
                .ConfigureAwait(false))
            {
                yield return pushed;
            }
        }
        finally
        {
            queue.EndRun();

            await Release(grain.DetachAsync, grains, reference).ConfigureAwait(false);

            // Orleans's observer table would let the receiver be collected mid-run; see the remarks.
            GC.KeepAlive(receiver);
        }
    }

    /// <summary>Tears one bridge down without letting the teardown replace how the run ended.</summary>
    /// <param name="release">The grain call that stops the bridge.</param>
    /// <param name="grains">The silo's grain factory.</param>
    /// <param name="receiver">The receiver reference to delete.</param>
    /// <returns>A task that completes when the teardown has been attempted.</returns>
    /// <remarks>
    /// <para>
    /// A <c>finally</c> that throws replaces the exception it was running under, so a grain call made while
    /// a silo is stopping would turn a cancelled run into a messaging failure and a completed one into a
    /// failed one. This package's own plumbing is not worth that, and the author's code is: an author's
    /// disposal still surfaces, and these two calls do not.
    /// </para>
    /// <para>
    /// Swallowing is safe here because both bridges heal themselves. The ingress has already ended, so a
    /// push or a tick that reaches a receiver whose run is gone is refused, and the refusal is exactly what
    /// makes the trigger unregister its reminder and the bridge forget its receiver. A teardown that never
    /// landed therefore costs at most one more tick or one more push.
    /// </para>
    /// </remarks>
    private static async Task Release(
        Func<Task> release,
        IGrainFactory grains,
        IDataflowPushReceiver receiver)
    {
        try
        {
            await release().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The bridge heals itself at the next tick or push; how the run ended is the run's to report.
        }

        try
        {
            grains.DeleteObjectReference<IDataflowPushReceiver>(receiver);
        }
        catch (Exception)
        {
            // Local bookkeeping in a client that may already be shutting down.
        }
    }

    /// <summary>Builds the Broadcast Channel publication sink.</summary>
    /// <param name="node">The node.</param>
    /// <returns>The runtime.</returns>
    /// <remarks>
    /// Two things are checked here that no parameter validator could see: that this silo registers a
    /// broadcast provider under the name the document gave, and that the provider's delivery mode is the
    /// one the document was written against. The second is what makes the payload's flag a contract rather
    /// than a decoration — a channel's mode is the provider's and cannot be chosen per publication, so the
    /// honest thing a document can do with it is declare what it assumed and be refused when that is wrong.
    /// </remarks>
    private DataflowStageRuntime BroadcastSink(StageNode node)
    {
        BroadcastSinkDeclaration declaration = Read<BroadcastSinkDeclaration>(node, BroadcastSinkPayload.TryRead);

        if (!registry.TryGetBroadcast(declaration.Element, out IBroadcastSinkEntry? element))
        {
            throw Unregistered(node, "broadcast element contract", declaration.Element);
        }

        if (services.GetKeyedService<IBroadcastChannelProvider>(declaration.Provider) is null)
        {
            throw new InvalidOperationException(
                $"The node '{node.Id}', an occurrence of '{node.Stage}', names the broadcast provider '{declaration.Provider}', and this silo registers no broadcast channel under that name. A broadcast sink needs the provider registered on the silo, such as by AddBroadcastChannel(\"{declaration.Provider}\").");
        }

        bool configured = services
            .GetRequiredService<IOptionsMonitor<BroadcastChannelOptions>>()
            .Get(declaration.Provider)
            .FireAndForgetDelivery;

        if (configured != declaration.FireAndForgetDelivery)
        {
            throw new InvalidOperationException(
                $"The node '{node.Id}', an occurrence of '{node.Stage}', declares FireAndForgetDelivery={declaration.FireAndForgetDelivery} and this silo configures the broadcast provider '{declaration.Provider}' with FireAndForgetDelivery={configured}. The mode belongs to the provider rather than to a publication, so the document was written against different delivery semantics from the ones this silo would give it.");
        }

        return DataflowStageRuntime.Terminal(
            static () => null,
            (state, published) =>
            {
                element!.PublishAsync(
                    services,
                    declaration.Provider,
                    declaration.Namespace,
                    declaration.Key,
                    published).GetAwaiter().GetResult();

                return state;
            },
            finish: null,
            producesResult: false);
    }

    /// <summary>Resolves a stream element binding or says the silo has none.</summary>
    /// <param name="node">The node.</param>
    /// <param name="contract">The contract text the payload carried.</param>
    /// <returns>The binding.</returns>
    /// <exception cref="InvalidOperationException">The silo binds no CLR type to that contract.</exception>
    private IStreamElementEntry Element(StageNode node, string contract) =>
        registry.TryGetElement(contract, out IStreamElementEntry? element)
            ? element!
            : throw Unregistered(node, "stream element contract", contract);

    /// <summary>Resolves a named stream provider or says the silo has none.</summary>
    /// <param name="node">The node.</param>
    /// <param name="name">The provider name the payload carried.</param>
    /// <returns>The provider.</returns>
    /// <exception cref="InvalidOperationException">The silo registers no such stream provider.</exception>
    /// <remarks>
    /// The one thing a parameter validator cannot check, because which providers a silo hosts is not a
    /// property of the payload: a document may name a provider every silo in the cluster has, and this silo
    /// may still be the one whose configuration forgot it. Failing here makes that a refusal of the start.
    /// </remarks>
    private IStreamProvider Provider(StageNode node, string name) =>
        services.GetKeyedService<IStreamProvider>(name) ??
        throw new InvalidOperationException(
            $"The node '{node.Id}', an occurrence of '{node.Stage}', names the stream provider '{name}', and this silo registers no stream provider under that name. A stream adapter needs the provider registered on the silo, such as by AddMemoryStreams(\"{name}\").");

    /// <summary>Builds the refusal of a name the silo does not register.</summary>
    /// <param name="node">The node.</param>
    /// <param name="what">What kind of thing the name addresses.</param>
    /// <param name="name">The name.</param>
    /// <returns>The exception to throw.</returns>
    private static InvalidOperationException Unregistered(StageNode node, string what, string name) =>
        new($"The node '{node.Id}', an occurrence of '{node.Stage}', addresses the {what} '{name}', which this silo does not register.");

    /// <summary>Reads one adapter's payload.</summary>
    /// <typeparam name="TDeclaration">The declaration the payload produces.</typeparam>
    /// <param name="parameters">The payload.</param>
    /// <param name="declaration">The declaration, when the payload is valid.</param>
    /// <param name="violations">The violations, when it is not.</param>
    /// <returns><see langword="true"/> when the payload is valid.</returns>
    private delegate bool PayloadReader<TDeclaration>(
        CanonicalJsonValue parameters,
        out TDeclaration? declaration,
        out IReadOnlyList<string> violations)
        where TDeclaration : class;

    /// <summary>The bridge from one stream subscription to one run's bounded ingress.</summary>
    /// <param name="queue">The run's ingress.</param>
    /// <remarks>
    /// The offer carries no token on purpose. Every way the queue stops accepting — completed, failed, or
    /// its run ended — releases a parked offer with a refusal, so a delivery is never left waiting for a run
    /// that has gone; and raising an <see cref="OperationCanceledException"/> into Orleans' delivery path
    /// instead would turn the ordinary end of a run into a stream-provider error.
    /// </remarks>
    private sealed class StreamIngress(LocalIngressQueue queue) : IStreamIngress
    {
        /// <inheritdoc/>
        public async ValueTask OfferAsync(object? element) =>
            _ = await queue.OfferAsync(element, CancellationToken.None).ConfigureAwait(false);

        /// <inheritdoc/>
        public void Complete() => queue.Complete();

        /// <inheritdoc/>
        public void Fail(Exception failure) => queue.Fail(failure);
    }

    /// <summary>The object a run publishes as a grain reference so that a bridge grain can reach it.</summary>
    /// <param name="queue">The run's ingress.</param>
    /// <param name="element">
    /// The binding that types the pushes, or <see langword="null"/> for a trigger whose elements this
    /// runtime produces itself and therefore cannot get wrong.
    /// </param>
    /// <remarks>
    /// <para>
    /// The whole of the bridge, and deliberately no more: the offer's outcome is returned rather than
    /// swallowed, so what a caller learns is exactly what the run's declared policy did with the element.
    /// The offer carries no token for the same reason a stream delivery does not — every way the queue
    /// stops accepting releases a parked offer with a refusal, and raising a cancellation into a caller's
    /// grain call would turn the ordinary end of a run into a delivery error.
    /// </para>
    /// <para>
    /// The type check happens here rather than on the caller's side because only the silo executing the run
    /// knows what its registry binds to the name. A mismatch is thrown rather than reported as an outcome:
    /// it is a programming error and not one of the four ways an element can fare.
    /// </para>
    /// </remarks>
    private sealed class PushReceiver(LocalIngressQueue queue, IObserverBridgeEntry? element)
        : IDataflowPushReceiver
    {
        /// <inheritdoc/>
        public async Task<DataflowPushOutcome> PushAsync(object? pushed)
        {
            element?.RequireElement(pushed);

            QueueOfferOutcome outcome = await queue.OfferAsync(pushed, CancellationToken.None)
                .ConfigureAwait(false);

            return outcome switch
            {
                QueueOfferOutcome.Accepted => DataflowPushOutcome.Accepted,
                QueueOfferOutcome.Dropped => DataflowPushOutcome.Dropped,
                QueueOfferOutcome.Failed => DataflowPushOutcome.Failed,
                _ => DataflowPushOutcome.Closed,
            };
        }
    }
}

/// <summary>
/// The per-run set of awaited calls a terminating grain-call stage keeps in flight.
/// </summary>
/// <param name="call">The registered call.</param>
/// <param name="grains">The silo's grain factory.</param>
/// <param name="name">The call's name, for a timeout's diagnosis.</param>
/// <param name="maxInFlight">The greatest number of calls in flight at once.</param>
/// <param name="timeout">The per-call timeout, or <see langword="null"/>.</param>
/// <remarks>
/// <para>
/// A terminal in this engine is a synchronous fold on the last segment's own thread, so the bound is kept
/// by blocking that thread rather than by a scheduler: submitting the element after the bound is reached
/// waits for the oldest call to answer. Waiting for the oldest rather than for the first to finish keeps
/// the accounting a queue and costs at most one call's latency, which is the honest trade for a shape with
/// nowhere to put a completion callback.
/// </para>
/// <para>
/// Every call is observed exactly once, and the first failure reaches the run: through the submit that
/// waited for it, or through the drain at the end of a successful stream. A run that fails or is cancelled
/// does not drain, because the engine does not project a terminal's state on those paths — the calls in
/// flight are abandoned with the run, and their outcomes are read by the continuation that keeps them from
/// resurfacing as unobserved exceptions.
/// </para>
/// </remarks>
internal sealed class GrainCallWindow(
    IGrainCallSinkEntry call,
    IGrainFactory grains,
    string name,
    int maxInFlight,
    TimeSpan? timeout)
{
    private readonly Queue<Task> _inFlight = new();

    /// <summary>Submits one element, waiting first if the bound is already reached.</summary>
    /// <param name="element">The element.</param>
    internal void Submit(object? element)
    {
        while (_inFlight.Count >= maxInFlight)
        {
            Settle(_inFlight.Dequeue());
        }

        Task pending = GrainCallInvocation.InvokeAsync(
            async token =>
            {
                await call.InvokeAsync(grains, element, token).ConfigureAwait(false);

                return (object?)null;
            },
            name,
            timeout,
            CancellationToken.None);

        // Observed the moment it is started, not only when it is waited for. A run that faults on one call
        // abandons the rest, and an abandoned task that faults later would otherwise resurface as an
        // unobserved task exception on a thread with no context. Reading the outcome twice is harmless; not
        // reading it at all is not.
        _ = pending.ContinueWith(
            static settled => _ = settled.Exception,
            CancellationToken.None,
            TaskContinuationOptions.DenyChildAttach,
            TaskScheduler.Default);

        _inFlight.Enqueue(pending);
    }

    /// <summary>Waits for every call still in flight.</summary>
    internal void Drain()
    {
        while (_inFlight.Count > 0)
        {
            Settle(_inFlight.Dequeue());
        }
    }

    /// <summary>Observes one finished call, raising what it raised.</summary>
    /// <param name="pending">The call.</param>
    private static void Settle(Task pending) => pending.GetAwaiter().GetResult();
}

/// <summary>
/// How an awaited grain call is bounded in time.
/// </summary>
/// <remarks>
/// Two things happen at once and both are needed. The token handed to the registered call is cancelled when
/// the timeout elapses, which is what asks a cooperative grain to stop; and the wait itself is bounded here,
/// which is what makes the stage fault on time whether or not the call was cooperative. Bounding only the
/// token would leave a stage waiting forever on a call that ignored it, and bounding only the wait would
/// leave the grain working on an element nobody will read.
/// </remarks>
internal static class GrainCallInvocation
{
    /// <summary>Invokes one call under the stage's declared timeout.</summary>
    /// <param name="call">The invocation, which is handed the token to carry.</param>
    /// <param name="name">The call's name, for the diagnosis.</param>
    /// <param name="timeout">The timeout, or <see langword="null"/> for none of our own.</param>
    /// <param name="cancellationToken">The run's token.</param>
    /// <returns>The reply.</returns>
    /// <exception cref="GrainCallTimeoutException">The call did not reply in time.</exception>
    internal static async Task<object?> InvokeAsync(
        Func<CancellationToken, Task<object?>> call,
        string name,
        TimeSpan? timeout,
        CancellationToken cancellationToken)
    {
        if (timeout is not { } limit)
        {
            return await call(cancellationToken).ConfigureAwait(false);
        }

        CancellationTokenSource timer = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        Task<object?> pending;

        timer.CancelAfter(limit);

        try
        {
            pending = call(timer.Token);
        }
        catch (Exception)
        {
            timer.Dispose();

            throw;
        }

        try
        {
            return await pending.WaitAsync(limit, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timer.IsCancellationRequested &&
            !cancellationToken.IsCancellationRequested)
        {
            // The cooperative half arriving first, which is the common case and not a different outcome: a
            // call that honors its token is cancelled by the timer and reports the cancellation before the
            // wait's own bound expires. What must not happen is reporting it as a cancellation, because a
            // cancelled run and an expired call mean opposite things to a caller. The token is examined and
            // not the timer alone, so a run that really was cancelled still ends cancelled.
            throw new GrainCallTimeoutException(name, limit);
        }
        catch (TimeoutException)
        {
            // The uncooperative half: the call ignored its token, so the wait is what ended.
            throw new GrainCallTimeoutException(name, limit);
        }
        finally
        {
            Release(pending, timer);
        }
    }

    /// <summary>Releases the timer once the call it bounds has settled, observing whatever it raised.</summary>
    /// <param name="pending">The call.</param>
    /// <param name="timer">The linked source whose token the call carries.</param>
    /// <remarks>
    /// The source cannot be released while the call still holds its token, so an abandoned call takes its
    /// timer with it when it finishes. Reading the outcome is what keeps a call abandoned by a timeout from
    /// resurfacing later as an unobserved task exception on a thread with no context.
    /// </remarks>
    private static void Release(Task pending, CancellationTokenSource timer)
    {
        if (pending.IsCompleted)
        {
            _ = pending.Exception;

            timer.Dispose();

            return;
        }

        timer.Cancel();

        _ = pending.ContinueWith(
            static (settled, held) =>
            {
                _ = settled.Exception;

                ((CancellationTokenSource)held!).Dispose();
            },
            timer,
            CancellationToken.None,
            TaskContinuationOptions.DenyChildAttach,
            TaskScheduler.Default);
    }
}
