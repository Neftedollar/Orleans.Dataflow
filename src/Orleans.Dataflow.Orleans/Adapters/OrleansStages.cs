using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Adapters;

/// <summary>
/// The Orleans-native adapter vocabulary: five registered stages, the catalog that publishes them, and the
/// typed handles and payloads an author writes them with.
/// </summary>
/// <remarks>
/// <para>
/// <b>Five stages and no more.</b> A subscription that feeds a run, a publication a run feeds, an awaited
/// grain call in its transforming and its terminating form, and a grain enumeration that heads a run. Each
/// is a real registered stage — named in a document, resolved from a silo's catalog by identity, built by a
/// runtime factory — so a pipeline written with them carries no delegate, no CLR name, and nothing a
/// document could not honestly say.
/// </para>
/// <para>
/// <b>Declare once, use twice.</b> The bindings in this namespace are written once by deployment code and
/// handed to two places: to the silo, which learns what the name means, and to the authoring helpers here,
/// which turn them into typed handles and into the parameter payloads a node stores. A silo and an author
/// therefore cannot disagree about a call's signature without the disagreement being two different
/// declarations — and the payload's contract references are what makes even that disagreement a refusal
/// rather than a runtime cast.
/// </para>
/// <para>
/// <b>The element contract these ports declare is opaque, and that is a stated limit.</b> Every one of the
/// five is one specification, and a specification declares one element contract per port; the contract a
/// given occurrence actually carries is a property of that occurrence and has nowhere in a specification to
/// live. So the ports declare <see cref="ElementContract"/> — one opaque reference, exactly as every local
/// port declares one — and what an occurrence really carries is stated in its payload and checked against
/// the silo's registry. The consequence, said plainly: an edge from an Orleans adapter to a stage that
/// declares a different element contract is reported by the graph compiler as an
/// <c>element-contract-mismatch</c>. Two adapters connect to each other freely, and a deployment's own
/// registered stage joins them by declaring <see cref="ElementContract"/> on the port that faces one.
/// Per-occurrence port contracts are what would lift the limit, and that is a definition-model change
/// rather than an adapter one.
/// </para>
/// <para>
/// <b>Semantics.</b> Every adapter's acknowledgement, delivery, ordering, replay, and backpressure answers
/// are on the member that builds it. They are not one global switch: a run of these stages is not "exactly
/// once" by configuration and nothing here pretends otherwise.
/// </para>
/// </remarks>
public static class OrleansStages
{
    private static readonly PortId InputPort = PortId.Create("in");
    private static readonly PortId OutputPort = PortId.Create("out");

    /// <summary>Gets the provider every Orleans-native adapter belongs to.</summary>
    /// <value>The provider <c>orleans</c>.</value>
    public static ProviderId Provider { get; } = ProviderId.Create("orleans");

    /// <summary>Gets the reference of the stream subscription source.</summary>
    /// <value><c>orleans/stream-source@v1</c>.</value>
    public static StageRef StreamSourceStage { get; } =
        StageRef.Create(Provider, StageId.Create("stream-source"), StageRef.FirstMajorVersion);

    /// <summary>Gets the reference of the stream publication sink.</summary>
    /// <value><c>orleans/stream-sink@v1</c>.</value>
    public static StageRef StreamSinkStage { get; } =
        StageRef.Create(Provider, StageId.Create("stream-sink"), StageRef.FirstMajorVersion);

    /// <summary>Gets the reference of the awaited grain call that transforms elements.</summary>
    /// <value><c>orleans/grain-call@v1</c>.</value>
    public static StageRef GrainCallStage { get; } =
        StageRef.Create(Provider, StageId.Create("grain-call"), StageRef.FirstMajorVersion);

    /// <summary>Gets the reference of the awaited grain call that terminates a graph.</summary>
    /// <value><c>orleans/grain-call-sink@v1</c>.</value>
    public static StageRef GrainCallSinkStage { get; } =
        StageRef.Create(Provider, StageId.Create("grain-call-sink"), StageRef.FirstMajorVersion);

    /// <summary>Gets the reference of the grain enumeration source.</summary>
    /// <value><c>orleans/grain-enumerable@v1</c>.</value>
    public static StageRef GrainEnumerableStage { get; } =
        StageRef.Create(Provider, StageId.Create("grain-enumerable"), StageRef.FirstMajorVersion);

    /// <summary>Gets the one element contract every Orleans adapter port declares.</summary>
    /// <value><c>orleans-element@v1</c>.</value>
    /// <remarks>
    /// Opaque on purpose and for the reason the local vocabulary's own opaque contract is: one
    /// specification cannot declare a contract that differs per occurrence. A deployment's own registered
    /// stage that wants to stand between two adapters declares this reference on the port that faces one,
    /// which is why the reference is public rather than hidden.
    /// </remarks>
    public static ContractReference ElementContract { get; } =
        ContractReference.Create(ContractId.Create("orleans-element"), ContractReference.FirstMajorVersion);

    /// <summary>Gets the parameter contract a stream source declares.</summary>
    /// <value><c>orleans-stream-source-parameters@v1</c>.</value>
    public static ContractReference StreamSourceParameterContract { get; } =
        ContractReference.Create(
            ContractId.Create("orleans-stream-source-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>Gets the parameter contract a stream sink declares.</summary>
    /// <value><c>orleans-stream-sink-parameters@v1</c>.</value>
    public static ContractReference StreamSinkParameterContract { get; } =
        ContractReference.Create(
            ContractId.Create("orleans-stream-sink-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>Gets the parameter contract a transforming grain call declares.</summary>
    /// <value><c>orleans-grain-call-parameters@v1</c>.</value>
    public static ContractReference GrainCallParameterContract { get; } =
        ContractReference.Create(
            ContractId.Create("orleans-grain-call-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>Gets the parameter contract a terminating grain call declares.</summary>
    /// <value><c>orleans-grain-call-sink-parameters@v1</c>.</value>
    public static ContractReference GrainCallSinkParameterContract { get; } =
        ContractReference.Create(
            ContractId.Create("orleans-grain-call-sink-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>Gets the parameter contract a grain enumeration declares.</summary>
    /// <value><c>orleans-grain-enumerable-parameters@v1</c>.</value>
    public static ContractReference GrainEnumerableParameterContract { get; } =
        ContractReference.Create(
            ContractId.Create("orleans-grain-enumerable-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>Declares the adapters' opaque element contract as one CLR type's.</summary>
    /// <typeparam name="T">The CLR type that stands on the far side of an adapter's port.</typeparam>
    /// <returns>The declaration.</returns>
    /// <remarks>
    /// The escape hatch made first class. A deployment's own registered stage that wants to sit between two
    /// adapters declares <see cref="ElementContract"/> on the port that faces one, and this is how it writes
    /// the typed half of that declaration without spelling the contract's name as a string.
    /// </remarks>
    public static ElementContract<T> Element<T>() => ElementContract<T>.Of(ElementContract);

    /// <summary>Gets the catalog an authoring process resolves these stages through.</summary>
    /// <value>
    /// A catalog whose specifications are exactly the ones a silo publishes, and whose parameter checks are
    /// the shape half of the silo's: an authoring process can say that a payload is malformed and cannot say
    /// which names a deployment registered.
    /// </value>
    /// <remarks>
    /// One shared immutable value rather than a fresh one per call, because a catalog is immutable and its
    /// identity is its contents. It is byte-identical to the silo's, so the two share a
    /// <see cref="CatalogFingerprint"/>: a parameter validator is behavior and never reaches a fingerprint.
    /// </remarks>
    public static StageCatalog Catalog { get; } = Publish(OrleansAdapterRegistry.Empty);

    /// <summary>Declares an Orleans stream subscription as the typed start of a graph.</summary>
    /// <typeparam name="T">The element type the stream carries in this process.</typeparam>
    /// <param name="element">The stream element binding this silo registered.</param>
    /// <returns>The typed handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="element"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// <b>Acknowledgement</b>: delivery into the run's bounded ingress, and not end-to-end processing. A
    /// delivery this adapter has accepted may still be lost by a run that fails afterwards; nothing here
    /// promises otherwise.
    /// </para>
    /// <para>
    /// <b>Delivery and ordering</b>: whatever the named provider gives. Orleans guarantees the order of one
    /// stream from one producer and nothing across producers, and a provider's own at-least-once or
    /// best-effort character is the provider's to state. The memory provider is non-durable by design.
    /// </para>
    /// <para>
    /// <b>Replay</b>: none is offered. The subscription is made without a sequence token, so a run reads
    /// what arrives after it subscribed and never history — even on a provider whose <c>IsRewindable</c> is
    /// true, which the memory provider's is (probed, not assumed). Exposing a cursor is a later phase's
    /// work, because a rewind API without a checkpoint owner is a foot-gun.
    /// </para>
    /// <para>
    /// <b>Backpressure</b>: the declared ingress bound. Under the backpressure policy a full ingress delays
    /// the provider's delivery, which is Orleans' own backpressure onto its pulling agent and never a parked
    /// grain turn — the subscription is made from the run's own execution context rather than from the run
    /// grain's. Under a dropping policy the delivery is answered at once and the drop is counted; under the
    /// failing policy the run faults.
    /// </para>
    /// <para>
    /// <b>What backpressure costs, said plainly.</b> A pulling agent serves a whole queue, so a run that
    /// stops taking elements delays delivery to every consumer of that queue and not only to itself. That
    /// was observed rather than deduced: a run held still with a bounded ingress under this policy stops a
    /// second, unrelated subscriber on the same stream from receiving anything. It is the correct behavior
    /// for a bounded system — the alternative is unbounded memory — but it is a shared cost, and a
    /// deployment that cannot pay it declares a dropping policy instead. A run that never drains will
    /// eventually surface as a provider-side delivery failure rather than as growth.
    /// </para>
    /// <para>
    /// <b>Subscription lifetime</b>: one run. The subscription is made when the run first pulls and is
    /// cancelled on every terminal path of the run, including a deactivation of the run grain, because it is
    /// held by the source's own enumeration and the engine disposes that on every path.
    /// </para>
    /// <para>
    /// <b>Shutdown</b>: a graceful stop stops production, so the run takes no further element from the
    /// ingress and whatever the ingress still holds is abandoned. That is not a contradiction of the
    /// acknowledgement boundary but the honest reading of it: the boundary is delivery into the ingress, and
    /// an element that reached the ingress and no further was never claimed to have been processed. Every
    /// element already inside the graph is drained, exactly as a shutdown drains any other source.
    /// </para>
    /// </remarks>
    public static RegisteredSource<T> StreamSource<T>(StreamElementBinding<T> element)
    {
        ArgumentNullException.ThrowIfNull(element);

        return RegisteredStage.Source(Catalog, StreamSourceStage, Element<T>());
    }

    /// <summary>Declares an Orleans stream publication as the typed end of a graph.</summary>
    /// <typeparam name="T">The element type the stream carries in this process.</typeparam>
    /// <param name="element">The stream element binding this silo registered.</param>
    /// <returns>The typed handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="element"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// <b>Acknowledgement</b>: one awaited <c>OnNextAsync</c> per element — publication, not end-to-end
    /// delivery. What a consumer of that stream then does with the element is between the consumer and the
    /// provider.
    /// </para>
    /// <para>
    /// <b>Delivery and ordering</b>: the provider's. Elements are published one at a time in the order the
    /// run produced them, which is the strongest order this adapter can offer: Orleans orders one stream
    /// from one producer.
    /// </para>
    /// <para>
    /// <b>Replay and checkpoint</b>: none. This adapter owns no cursor.
    /// </para>
    /// <para>
    /// <b>Backpressure</b>: the awaited publication itself. The run's last segment holds its thread until
    /// the provider has accepted the element, so a slow provider slows the run rather than filling a queue.
    /// </para>
    /// <para>
    /// <b>Cancellation</b>: observed between elements. A terminal in this engine is a synchronous fold and is
    /// handed no token, so a publication already in flight when a run is cancelled runs to its own end or to
    /// Orleans' own call timeout; what a cancellation stops is the publication of the next element. That is
    /// a limit of the phase-1 terminal seam rather than of this adapter, and it is stated rather than hidden.
    /// </para>
    /// <para>
    /// <b>Completion</b>: a run that ends completes nothing on the stream. An Orleans stream has no end a
    /// publisher can honestly signal — other producers may still be publishing, and consumers outlive this
    /// run — so calling <c>OnCompletedAsync</c> would tell every consumer a lie about a stream this run does
    /// not own. A run that fails likewise leaves the stream alone: the failure is the run's, reported on the
    /// run, and never pushed into a stream other consumers share.
    /// </para>
    /// </remarks>
    public static RegisteredSink<T> StreamSink<T>(StreamElementBinding<T> element)
    {
        ArgumentNullException.ThrowIfNull(element);

        return RegisteredStage.Sink(Catalog, StreamSinkStage, Element<T>());
    }

    /// <summary>Declares a named awaited grain call as a typed transformation.</summary>
    /// <typeparam name="TIn">The element type the call consumes.</typeparam>
    /// <typeparam name="TOut">The element type the call produces.</typeparam>
    /// <param name="call">The call binding this silo registered.</param>
    /// <returns>The typed handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="call"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// <b>Acknowledgement</b>: the awaited reply. A reply acknowledges that method invocation and nothing
    /// the grain may have started behind it.
    /// </para>
    /// <para>
    /// <b>Delivery</b>: at-most-once per element as far as this adapter is concerned — it never retries. A
    /// call that fails faults the run, and a deployment that wants a retry writes it inside the registered
    /// call, where the duplicate window it opens is the deployment's own to state.
    /// </para>
    /// <para>
    /// <b>Ordering</b>: emission is in input order. The calls themselves overlap up to the declared bound,
    /// so the grains see them concurrently; what is ordered is what leaves this stage.
    /// </para>
    /// <para>
    /// <b>Backpressure</b>: the declared bound. A call in flight is credit spent, and elements reach this
    /// stage through a bounded channel rather than a queue.
    /// </para>
    /// <para>
    /// <b>Idempotency</b>: not enforced here. An awaited grain call is request/reply and not a durable
    /// queue, and this adapter adds nothing to that.
    /// </para>
    /// </remarks>
    public static RegisteredFlow<TIn, TOut> GrainCall<TIn, TOut>(GrainCallBinding<TIn, TOut> call)
    {
        ArgumentNullException.ThrowIfNull(call);

        return RegisteredStage.Flow(Catalog, GrainCallStage, Element<TIn>(), Element<TOut>());
    }

    /// <summary>Declares a named awaited grain call as a typed termination.</summary>
    /// <typeparam name="TIn">The element type the call consumes.</typeparam>
    /// <param name="call">The call binding this silo registered.</param>
    /// <returns>The typed handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="call"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// <b>Acknowledgement</b>: the awaited reply, which is then discarded. The same boundary the
    /// transforming form has, with the value dropped rather than emitted.
    /// </para>
    /// <para>
    /// <b>Ordering</b>: none beyond the bound. With a bound of one the effects happen in the run's order;
    /// with a greater bound the calls overlap and their effects are ordered by the grains they reach, not by
    /// this stage.
    /// </para>
    /// <para>
    /// <b>Cancellation</b>: observed between elements. A terminal in this engine is a synchronous fold and
    /// is handed no token, so a call already in flight when a run is cancelled runs to its own end or to
    /// Orleans' own call timeout; what a cancellation stops is the admission of the next element. That is a
    /// limit of the phase-1 terminal seam, stated here rather than hidden.
    /// </para>
    /// </remarks>
    public static RegisteredSink<TIn> GrainCallSink<TIn>(GrainCallSinkBinding<TIn> call)
    {
        ArgumentNullException.ThrowIfNull(call);

        return RegisteredStage.Sink(Catalog, GrainCallSinkStage, Element<TIn>());
    }

    /// <summary>Declares a named grain enumeration as the typed start of a graph.</summary>
    /// <typeparam name="T">The element type the enumeration produces.</typeparam>
    /// <param name="source">The enumeration binding this silo registered.</param>
    /// <returns>The typed handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// <b>Acknowledgement</b>: the call-scoped pull. An element is taken when the run asks for it, and
    /// Orleans batches the transport underneath at its own default; the batch size is deliberately not an
    /// option this phase, and is the obvious first one to add.
    /// </para>
    /// <para>
    /// <b>Backpressure</b>: the enumeration's own. A run that stops pulling stops the grain from producing,
    /// which is what makes a grain enumeration the only Orleans source that needs no ingress buffer.
    /// </para>
    /// <para>
    /// <b>Replay</b>: none. Resuming where a previous run stopped requires an application cursor the grain
    /// owns; nothing here keeps one.
    /// </para>
    /// <para>
    /// <b>Cancellation</b>: cooperative, and the run's own token is what carries it. Orleans 10 defaults
    /// <c>MessagingOptions.CancelRequestOnTimeout</c> to false, so a response timeout does not cancel the
    /// grain-side enumeration; the token this adapter passes is the only signal that does, and a grain that
    /// ignores it delays the run's stop until it next yields. Disposal is awaited on every terminal path.
    /// </para>
    /// </remarks>
    public static RegisteredSource<T> GrainEnumerable<T>(GrainEnumerableBinding<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        return RegisteredStage.Source(Catalog, GrainEnumerableStage, Element<T>());
    }

    /// <summary>Writes the payload of one stream source occurrence.</summary>
    /// <typeparam name="T">The element type the stream carries.</typeparam>
    /// <param name="element">The stream element binding this silo registered.</param>
    /// <param name="stream">The stream to subscribe to.</param>
    /// <param name="ingress">The bounded ingress the deliveries land in.</param>
    /// <returns>The canonical payload.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="element"/> or <paramref name="ingress"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException"><paramref name="stream"/> is the default value.</exception>
    public static CanonicalJsonValue StreamSourceParameters<T>(
        StreamElementBinding<T> element,
        OrleansStreamAddress stream,
        BufferOptions ingress)
    {
        ArgumentNullException.ThrowIfNull(element);
        ArgumentNullException.ThrowIfNull(ingress);
        RequireAddress(stream);

        return StreamSourcePayload.Write(element.Element.Reference.ToString(), stream, ingress);
    }

    /// <summary>Writes the payload of one stream sink occurrence.</summary>
    /// <typeparam name="T">The element type the stream carries.</typeparam>
    /// <param name="element">The stream element binding this silo registered.</param>
    /// <param name="stream">The stream to publish to.</param>
    /// <returns>The canonical payload.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="element"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="stream"/> is the default value.</exception>
    public static CanonicalJsonValue StreamSinkParameters<T>(
        StreamElementBinding<T> element,
        OrleansStreamAddress stream)
    {
        ArgumentNullException.ThrowIfNull(element);
        RequireAddress(stream);

        return StreamSinkPayload.Write(element.Element.Reference.ToString(), stream);
    }

    /// <summary>Writes the payload of one transforming grain call occurrence.</summary>
    /// <typeparam name="TIn">The element type the call consumes.</typeparam>
    /// <typeparam name="TOut">The element type the call produces.</typeparam>
    /// <param name="call">The call binding this silo registered.</param>
    /// <param name="maxInFlight">The greatest number of calls in flight at once; at least one.</param>
    /// <param name="timeout">
    /// The per-call timeout, or <see langword="null"/> to leave the wait to Orleans' own call timeout.
    /// </param>
    /// <returns>The canonical payload.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="call"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="maxInFlight"/> is below one, or <paramref name="timeout"/> is not positive.
    /// </exception>
    public static CanonicalJsonValue GrainCallParameters<TIn, TOut>(
        GrainCallBinding<TIn, TOut> call,
        int maxInFlight,
        TimeSpan? timeout = null)
    {
        ArgumentNullException.ThrowIfNull(call);
        RequireBounds(maxInFlight, timeout);

        return GrainCallPayload.Write(
            call.Name,
            call.Input.Reference.ToString(),
            call.Output.Reference.ToString(),
            maxInFlight,
            timeout);
    }

    /// <summary>Writes the payload of one terminating grain call occurrence.</summary>
    /// <typeparam name="TIn">The element type the call consumes.</typeparam>
    /// <param name="call">The call binding this silo registered.</param>
    /// <param name="maxInFlight">The greatest number of calls in flight at once; at least one.</param>
    /// <param name="timeout">
    /// The per-call timeout, or <see langword="null"/> to leave the wait to Orleans' own call timeout.
    /// </param>
    /// <returns>The canonical payload.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="call"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="maxInFlight"/> is below one, or <paramref name="timeout"/> is not positive.
    /// </exception>
    public static CanonicalJsonValue GrainCallSinkParameters<TIn>(
        GrainCallSinkBinding<TIn> call,
        int maxInFlight,
        TimeSpan? timeout = null)
    {
        ArgumentNullException.ThrowIfNull(call);
        RequireBounds(maxInFlight, timeout);

        return GrainCallPayload.Write(
            call.Name,
            call.Input.Reference.ToString(),
            output: null,
            maxInFlight,
            timeout);
    }

    /// <summary>Writes the payload of one grain enumeration occurrence.</summary>
    /// <typeparam name="T">The element type the enumeration produces.</typeparam>
    /// <param name="source">The enumeration binding this silo registered.</param>
    /// <returns>The canonical payload.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static CanonicalJsonValue GrainEnumerableParameters<T>(GrainEnumerableBinding<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        return GrainEnumerablePayload.Write(source.Name, source.Output.Reference.ToString());
    }

    /// <summary>Builds the catalog a silo publishes, with the checks that silo's registry can make.</summary>
    /// <param name="registry">The silo's registry.</param>
    /// <returns>The catalog.</returns>
    /// <remarks>
    /// The specifications are the same in every process; only the validators differ, and a validator is
    /// behavior and never reaches a <see cref="CatalogFingerprint"/>. That is what lets an authoring
    /// process resolve these stages against <see cref="Catalog"/> and a silo accept the resulting document
    /// as a document of its own vocabulary.
    /// </remarks>
    internal static StageCatalog Publish(OrleansAdapterRegistry registry) =>
        StageCatalog.Create(
        [
            StageSpecification.Create(
                StreamSourceStage,
                [],
                [OutputPortSpecification.Create(OutputPort, ElementContract)],
                [],
                StreamSourceParameterContract,
                [],
                new OrleansStageValidator(registry, OrleansStageKind.StreamSource)),
            StageSpecification.Create(
                StreamSinkStage,
                [InputPortSpecification.Create(InputPort, ElementContract)],
                [],
                [],
                StreamSinkParameterContract,
                [],
                new OrleansStageValidator(registry, OrleansStageKind.StreamSink)),
            StageSpecification.Create(
                GrainCallStage,
                [InputPortSpecification.Create(InputPort, ElementContract)],
                [OutputPortSpecification.Create(OutputPort, ElementContract)],
                [],
                GrainCallParameterContract,
                [],
                new OrleansStageValidator(registry, OrleansStageKind.GrainCall)),
            StageSpecification.Create(
                GrainCallSinkStage,
                [InputPortSpecification.Create(InputPort, ElementContract)],
                [],
                [],
                GrainCallSinkParameterContract,
                [],
                new OrleansStageValidator(registry, OrleansStageKind.GrainCallSink)),
            StageSpecification.Create(
                GrainEnumerableStage,
                [],
                [OutputPortSpecification.Create(OutputPort, ElementContract)],
                [],
                GrainEnumerableParameterContract,
                [],
                new OrleansStageValidator(registry, OrleansStageKind.GrainEnumerable)),
        ]);

    /// <summary>Refuses a stream address that addresses nothing.</summary>
    /// <param name="stream">The address.</param>
    /// <exception cref="ArgumentException"><paramref name="stream"/> is the default value.</exception>
    private static void RequireAddress(OrleansStreamAddress stream)
    {
        if (stream.IsDefault)
        {
            throw new ArgumentException(
                $"A stream adapter's payload requires a created {nameof(OrleansStreamAddress)}; the default value addresses no stream.",
                nameof(stream));
        }
    }

    /// <summary>Refuses a bound or a timeout a run could not honor.</summary>
    /// <param name="maxInFlight">The concurrency bound.</param>
    /// <param name="timeout">The per-call timeout.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The bound is below one, or the timeout is not a positive, finite duration.
    /// </exception>
    private static void RequireBounds(int maxInFlight, TimeSpan? timeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxInFlight, 1);

        if (timeout is not { } declared)
        {
            return;
        }

        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(declared, TimeSpan.Zero, nameof(timeout));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            declared.TotalMilliseconds,
            int.MaxValue,
            nameof(timeout));
    }
}
