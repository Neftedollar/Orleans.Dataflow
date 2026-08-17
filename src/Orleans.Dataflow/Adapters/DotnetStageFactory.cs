using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Adapters;

/// <summary>
/// Builds every stage of the .NET push-bridge vocabulary.
/// </summary>
/// <param name="registry">What this host registered for the push adapters.</param>
/// <remarks>
/// <para>
/// One factory for the whole provider, dispatching on the node's stage reference, which is the shape the
/// seam asks for and the shape a real provider has. It is constructed with the host's registry and nothing
/// else, which is the seam's rule stated as a constructor: a factory receives no document, no run
/// identity, and no services beyond what it was constructed with.
/// </para>
/// <para>
/// <b>Where the work runs.</b> Both stages open on the run's own source thread rather than on any
/// scheduler of their own: a timer is awaited there and an observable's subscription is made from there.
/// Whichever thread the observable then pushes on is the observable's business, and it is the thread that
/// pays for backpressure.
/// </para>
/// <para>
/// <b>What is checked here and what is not.</b> The payload has already been validated against this host's
/// registry by the graph compiler, so a name resolved here is a name that resolved there. The lookup is
/// repeated anyway, because a factory that dereferenced a missing registration would report a
/// configuration problem as a null reference somewhere else entirely.
/// </para>
/// </remarks>
internal sealed class DotnetStageFactory(DotnetAdapterRegistry registry) : IStageRuntimeFactory
{
    /// <inheritdoc/>
    public StageRuntime Create(StageRuntimeRequest request)
    {
        StageNode node = request.Node;

        if (node.Stage == DotnetStages.TimerStage)
        {
            TimerDeclaration declaration = Read<TimerDeclaration>(node, TimerPayload.TryRead);

            return StageRuntime.Source(tokens => Ticks(declaration.Period, declaration.TickLimit, tokens));
        }

        if (node.Stage == DotnetStages.ObservableStage)
        {
            ObservableDeclaration declaration = Read<ObservableDeclaration>(node, ObservablePayload.TryRead);

            if (!registry.TryGetObservable(declaration.Source, out IObservableEntry? source))
            {
                throw new InvalidOperationException(
                    $"The node '{node.Id}', an occurrence of '{node.Stage}', addresses the observable '{declaration.Source}', which this host does not register.");
            }

            return StageRuntime.Source(tokens => Pushes(source!, declaration.Ingress, tokens));
        }

        throw new InvalidOperationException(
            $"The node '{node.Id}' is an occurrence of '{node.Stage}', which the .NET push-adapter provider does not implement.");
    }

    /// <summary>Produces one tick index per period, on the run's own source thread.</summary>
    /// <param name="period">The period between ticks.</param>
    /// <param name="limit">The greatest number of ticks, or zero for no bound.</param>
    /// <param name="tokens">The run's tokens.</param>
    /// <returns>The sequence of tick indices.</returns>
    /// <remarks>
    /// <para>
    /// The wait observes the stop token, so a graceful shutdown ends the sequence between ticks rather than
    /// after the current period: the run drains the ticks already inside the graph and produces no more.
    /// That is why the cancellation is caught and turned into the end of a sequence — a source released by
    /// the stop token and not by the run token has run out, which is exactly the drain contract. A real
    /// cancellation is left to propagate and abandons the run.
    /// </para>
    /// <para>
    /// The timer is disposed by the iterator's own <c>using</c>, which the engine reaches on every terminal
    /// path because it disposes the enumeration on all of them. Nothing about this source outlives its run.
    /// </para>
    /// </remarks>
    private static async IAsyncEnumerable<object?> Ticks(TimeSpan period, long limit, StageRunTokens tokens)
    {
        using PeriodicTimer timer = new(period);

        for (long index = 0; limit == 0 || index < limit; index++)
        {
            bool ticked;

            try
            {
                ticked = await timer.WaitForNextTickAsync(tokens.StopToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!tokens.RunToken.IsCancellationRequested)
            {
                ticked = false;
            }

            if (!ticked)
            {
                break;
            }

            yield return index;
        }
    }

    /// <summary>Subscribes to a named observable and feeds a run's bounded ingress from it.</summary>
    /// <param name="source">The registered observable.</param>
    /// <param name="ingress">The bound and policy of the ingress.</param>
    /// <param name="tokens">The run's tokens.</param>
    /// <returns>The sequence the run pulls.</returns>
    /// <remarks>
    /// <para>
    /// The subscription is made when the run first pulls and disposed in the <c>finally</c> the engine
    /// reaches on every terminal path — completion, failure, cancellation, and a graceful shutdown. A
    /// subscription that failed to be made leaves nothing to dispose, and the queue is ended first in that
    /// case too, so a producer that had already parked for room is released with a refusal instead of
    /// waiting on a run that will never start.
    /// </para>
    /// <para>
    /// The ingress is ended before the subscription is disposed, and the order matters: ending it releases
    /// a producer parked for room with a refusal, so the pushing thread is freed at once rather than
    /// waiting for a queue nobody will drain again.
    /// </para>
    /// </remarks>
    private static async IAsyncEnumerable<object?> Pushes(
        IObservableEntry source,
        BufferOptions ingress,
        StageRunTokens tokens)
    {
        LocalIngressQueue queue = new(ingress.Capacity, ingress.OverflowPolicy);
        IDisposable subscription;

        try
        {
            subscription = source.Subscribe(new PushIngress(queue));
        }
        catch (Exception)
        {
            // A subscription that threw may still have pushed synchronously before it did, and a producer
            // parked for room in a queue nobody will read is the one thing worse than the failure itself.
            queue.EndRun();

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
            subscription.Dispose();
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

    /// <summary>The bridge from one observable subscription to one run's bounded ingress.</summary>
    /// <param name="queue">The run's ingress.</param>
    /// <remarks>
    /// The offer carries no token on purpose. Every way the queue stops accepting — completed, failed, or
    /// its run ended — releases a parked offer with a refusal, so a notification is never left waiting for
    /// a run that has gone; and raising an <see cref="OperationCanceledException"/> into an observable's
    /// notification path instead would turn the ordinary end of a run into a producer-side error.
    /// </remarks>
    private sealed class PushIngress(LocalIngressQueue queue) : IPushIngress
    {
        /// <inheritdoc/>
        /// <remarks>
        /// Blocking, and it has to be: <see cref="IObserver{T}.OnNext"/> returns <see langword="void"/>, so
        /// the only thread that can wait for room is the one that pushed. The synchronous decision answers
        /// every case but one — only a backpressuring offer into a full queue has anything to wait for —
        /// so the block is exactly the backpressure the document declared and nothing else.
        /// </remarks>
        public void Offer(object? element)
        {
            ValueTask<QueueOfferOutcome> offer = queue.OfferAsync(element, CancellationToken.None);

            _ = offer.IsCompleted ? offer.Result : offer.AsTask().GetAwaiter().GetResult();
        }

        /// <inheritdoc/>
        public void Complete() => queue.Complete();

        /// <inheritdoc/>
        public void Fail(Exception failure) => queue.Fail(failure);
    }
}
