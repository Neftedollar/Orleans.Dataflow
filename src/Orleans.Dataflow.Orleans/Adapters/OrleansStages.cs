using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Adapters;

/// <summary>
/// The Orleans-native adapter vocabulary: nine registered stages, the catalog that publishes them, and the
/// typed handles and payloads an author writes them with.
/// </summary>
/// <remarks>
/// <para>
/// <b>Nine stages and no more.</b> A stream subscription that feeds a run and a stream publication a run
/// feeds; an awaited grain call in its transforming and its terminating form; a keyed grain call, which is
/// the one stage that may distribute below its run; a grain enumeration that heads a run; a cluster reminder
/// whose ticks head one; a named bridge external grain code pushes at; and a Broadcast Channel publication.
/// Each is a real registered stage — named in a document, resolved from a silo's catalog by identity, built
/// by a runtime factory — so a pipeline written with them carries no delegate, no CLR name, and nothing a
/// document could not honestly say.
/// </para>
/// <para>
/// <b>The Broadcast Channel <em>source</em> is deliberately absent.</b> A channel's subscription is
/// implicit — a grain type declares the namespaces it receives, and the runtime activates one grain per
/// channel key — so a run cannot subscribe to one at all. Reaching it needs a delivery registry that maps
/// live runs to the grains the runtime activates, and the keyed stage below did <em>not</em> turn out to
/// need one: an executor's address is composed from the run's own identity, so the run knows where to send
/// and never has to look anything up. A broadcast subscriber's address is the runtime's to choose, which is
/// the opposite direction and the registry is what would bridge it. The sink is complete; the source is
/// still scheduled rather than approximated.
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
/// nine is one specification, and a specification declares one element contract per port; the contract a
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

    /// <summary>Gets the reference of the keyed grain call.</summary>
    /// <value><c>orleans/grain-call-keyed@v1</c>.</value>
    public static StageRef KeyedGrainCallStage { get; } =
        StageRef.Create(Provider, StageId.Create("grain-call-keyed"), StageRef.FirstMajorVersion);

    /// <summary>Gets the reference of the awaited grain call that terminates a graph.</summary>
    /// <value><c>orleans/grain-call-sink@v1</c>.</value>
    public static StageRef GrainCallSinkStage { get; } =
        StageRef.Create(Provider, StageId.Create("grain-call-sink"), StageRef.FirstMajorVersion);

    /// <summary>Gets the reference of the grain enumeration source.</summary>
    /// <value><c>orleans/grain-enumerable@v1</c>.</value>
    public static StageRef GrainEnumerableStage { get; } =
        StageRef.Create(Provider, StageId.Create("grain-enumerable"), StageRef.FirstMajorVersion);

    /// <summary>Gets the reference of the cluster-reminder trigger source.</summary>
    /// <value><c>orleans/reminder-trigger@v1</c>.</value>
    public static StageRef ReminderTriggerStage { get; } =
        StageRef.Create(Provider, StageId.Create("reminder-trigger"), StageRef.FirstMajorVersion);

    /// <summary>Gets the reference of the observer bridge source.</summary>
    /// <value><c>orleans/observer@v1</c>.</value>
    public static StageRef ObserverBridgeStage { get; } =
        StageRef.Create(Provider, StageId.Create("observer"), StageRef.FirstMajorVersion);

    /// <summary>Gets the reference of the Broadcast Channel publication sink.</summary>
    /// <value><c>orleans/broadcast-sink@v1</c>.</value>
    public static StageRef BroadcastSinkStage { get; } =
        StageRef.Create(Provider, StageId.Create("broadcast-sink"), StageRef.FirstMajorVersion);

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

    /// <summary>Gets the parameter contract a keyed grain call declares.</summary>
    /// <value><c>orleans-grain-call-keyed-parameters@v1</c>.</value>
    public static ContractReference KeyedGrainCallParameterContract { get; } =
        ContractReference.Create(
            ContractId.Create("orleans-grain-call-keyed-parameters"),
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

    /// <summary>Gets the parameter contract a reminder trigger declares.</summary>
    /// <value><c>orleans-reminder-trigger-parameters@v1</c>.</value>
    public static ContractReference ReminderTriggerParameterContract { get; } =
        ContractReference.Create(
            ContractId.Create("orleans-reminder-trigger-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>Gets the parameter contract an observer bridge declares.</summary>
    /// <value><c>orleans-observer-parameters@v1</c>.</value>
    public static ContractReference ObserverBridgeParameterContract { get; } =
        ContractReference.Create(
            ContractId.Create("orleans-observer-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>Gets the parameter contract a broadcast sink declares.</summary>
    /// <value><c>orleans-broadcast-sink-parameters@v1</c>.</value>
    public static ContractReference BroadcastSinkParameterContract { get; } =
        ContractReference.Create(
            ContractId.Create("orleans-broadcast-sink-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>Gets the contract every reminder tick is carried under.</summary>
    /// <value>The adapters' opaque element contract, declared as <see cref="long"/>.</value>
    /// <remarks>
    /// A tick is a <see cref="long"/> index counting from zero within one run, and it is the same value in
    /// every process: a reminder trigger addresses no element registration, so unlike every other adapter
    /// here its element type is fixed by the stage rather than by a binding. The index counts the ticks
    /// this run received and is never a wall-clock reading — and because missed ticks are not replayed, it
    /// is a count of what arrived rather than of what the schedule implies.
    /// </remarks>
    public static ElementContract<long> Tick => Element<long>();

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

    /// <summary>Declares a named keyed grain call as a typed transformation.</summary>
    /// <typeparam name="TIn">The element type the call consumes.</typeparam>
    /// <typeparam name="TOut">The element type the call produces.</typeparam>
    /// <param name="call">The keyed call binding this silo registered.</param>
    /// <returns>The typed handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="call"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// <b>What makes it keyed.</b> Every element belongs to a key, read by the routing function the binding
    /// registered, and the stage promises that one key's elements are processed one at a time in the order
    /// the run produced them. Elements of different keys overlap up to the declared bound. That is the whole
    /// difference from <see cref="GrainCall{TIn, TOut}"/>, which orders nothing and overlaps everything.
    /// </para>
    /// <para>
    /// <b>Acknowledgement</b>: the awaited reply, exactly as the plain form's is. When the stage is
    /// distributed the reply is the executor's, which is the reply of the call it made — an executor adds a
    /// hop and no semantics.
    /// </para>
    /// <para>
    /// <b>Ordering, and where it comes from.</b> One call in flight per key, always, and that is not a
    /// setting. The next element of a key is not sent until the previous element's reply has arrived, so
    /// nothing between the run and the grain is ever asked to keep two messages in order. This is deliberate
    /// and was measured rather than assumed: Orleans documents no pairwise ordering between activations, and
    /// the probe in this repository's suite watched pipelined calls arrive badly out of order inside a
    /// single silo. A stage that pipelined per key would therefore promise an ordering the transport
    /// visibly does not provide. Emission is in input order across all keys, as every ordered asynchronous
    /// stage's is.
    /// </para>
    /// <para>
    /// <b>Backpressure and credit</b>: the declared bound, spent per call and returned by the reply. Two
    /// bounds hold at once — one call per key, and <c>maxInFlight</c> calls across all keys — and both are
    /// held by the run rather than by anything on the wire. There is no credit message: a reply is the grant
    /// for that key, and a freed slot is the grant for the next key. The cost is stated plainly: elements
    /// that all share one key run one at a time no matter how large the bound is, and a bound's worth of
    /// elements on one key occupies the whole stage while they wait for it.
    /// </para>
    /// <para>
    /// <b>Distribution is opt-in</b>, declared in the payload. Left off — the default — the calls are made
    /// from inside the run exactly as a plain grain call's are, and the key only orders them. Turned on,
    /// each key gets an executor grain of its own, keyed by the run's identity, this occurrence, and the
    /// key, and the cluster places those executors: work for different keys then runs on different silos
    /// rather than all on the one hosting the run. That is opt-in because M3's rule is that runs distribute
    /// before stages do, and this is the first stage allowed to distribute below its run.
    /// </para>
    /// <para>
    /// <b>Failure</b>: the first failure wins and faults the run, and no retry is ever made — an M3 keyed
    /// call is at-most-once per element from this adapter's side. A distributed call that fails arrives as
    /// the executor's refusal naming the author's exception type, its message, and the executor's own
    /// address; a run-local one arrives as the author's exception itself. That difference is the cost of the
    /// hop and is stated rather than hidden. A silo that dies while holding an executor surfaces as the
    /// failed grain call it is: the run faults, and nothing here quietly runs the element again. Supervision
    /// and retry are M5's.
    /// </para>
    /// <para>
    /// <b>Executor lifetime</b>: an executor belongs to one run and holds no state between calls, so
    /// nothing about it is durable and nothing of it is shared with another run. It is left to Orleans'
    /// activation collection when the run ends rather than deactivated by the run, because the engine's
    /// asynchronous-stage seam has no per-run teardown hook; the executors carry a short collection age so
    /// that "dies with the run" is a bounded delay rather than a promise this adapter cannot keep.
    /// </para>
    /// </remarks>
    public static RegisteredFlow<TIn, TOut> KeyedGrainCall<TIn, TOut>(KeyedGrainCallBinding<TIn, TOut> call)
    {
        ArgumentNullException.ThrowIfNull(call);

        return RegisteredStage.Flow(Catalog, KeyedGrainCallStage, Element<TIn>(), Element<TOut>());
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

    /// <summary>Declares a cluster reminder as the typed start of a graph.</summary>
    /// <returns>The typed handle, producing the tick index.</returns>
    /// <remarks>
    /// <para>
    /// <b>Acknowledgement</b>: none. A tick is generated rather than delivered, and what this adapter does
    /// with it — offer it into the run's bounded ingress — is acknowledged by the offer's outcome and by
    /// nothing further downstream.
    /// </para>
    /// <para>
    /// <b>What survives, verbatim.</b> The reminder <em>definition</em> survives restarts; this run does
    /// not. Missed ticks are never replayed: a reminder that should have fired while nothing was running
    /// fires once when a silo picks it up again, and the ticks in between are gone. So the durable half of
    /// this stage is a schedule and never a stream, and a document that needs every tick accounted for
    /// needs a different design rather than a longer period.
    /// </para>
    /// <para>
    /// <b>What happens when the run is not there.</b> This phase's runs live for one activation. If the run
    /// grain is deactivated mid-run, the attempt is faulted — that is phase 1's stated durability contract
    /// and nothing here changes it — and the reminder outlives it. The next tick finds no live attempt, and
    /// the trigger unregisters the reminder and stops. There is no silent resume: the run stays exactly as
    /// it ended, and a caller polling it sees the loss. Durable resume is M5's checkpoint work.
    /// </para>
    /// <para>
    /// <b>Period</b>: whole milliseconds, and at least the cluster's configured
    /// <c>ReminderOptions.MinimumReminderPeriod</c>. That option defaults to one minute in Orleans 10.2.2
    /// and is enforced by a throw rather than by clamping — probed, not assumed — so a period below it is
    /// refused. This adapter turns that refusal into a refusal of the start, naming the configured minimum,
    /// rather than letting it surface when the trigger first registers.
    /// </para>
    /// <para>
    /// <b>Backpressure</b>: the declared ingress bound, and a clock cannot be slowed. The overflow policy
    /// may therefore not be <c>backpressure</c>: a tick that finds no room is dropped or fails by the
    /// declared policy, and the reminder keeps its own schedule regardless of what the run is doing. That
    /// is also what keeps the trigger's grain turn free — a tick forwarded into a full queue answers at
    /// once instead of parking the activation that owns the cluster's reminder for this run, which is what
    /// lets the teardown call that stops the trigger land promptly.
    /// </para>
    /// <para>
    /// <b>The one wait a trigger can still take, measured.</b> If the run's process is gone without having
    /// stopped the trigger, the tick's forwarding call neither answers nor fails until Orleans' own
    /// response timeout expires — thirty seconds by default — and the trigger's turn is held for that long
    /// before it removes the reminder. Observed rather than deduced. The cost is paid once, because a
    /// trigger that could not reach its run removes the reminder rather than trying again.
    /// </para>
    /// <para>
    /// <b>Elements</b>: <see cref="long"/> indices from zero, counting the ticks this run received.
    /// </para>
    /// <para>
    /// <b>Cleanup</b>: the reminder is unregistered on every terminal path the run can still reach —
    /// completion, a graceful shutdown, a cancellation, and the disposal a deactivating run grain performs.
    /// A path that reaches none of them, such as a silo that stopped without running anything, is covered
    /// by the tick-side cleanup above.
    /// </para>
    /// <para>
    /// <b>The other asymmetry, stated.</b> The trigger's own activation holds the run's receiver, so an
    /// activation recycled while its run is still executing ends that run's ticks: the next tick finds
    /// nothing to forward to and removes the reminder, and the run keeps running with a source that has
    /// gone quiet rather than one that ended or failed. Nothing links back from the trigger to the run, so
    /// there is no honest way to tell it; a deployment that cannot tolerate a silently quiet trigger bounds
    /// the run some other way until failover work makes the link recoverable.
    /// </para>
    /// </remarks>
    public static RegisteredSource<long> ReminderTrigger() =>
        RegisteredStage.Source(Catalog, ReminderTriggerStage, Tick);

    /// <summary>Declares a named observer bridge as the typed start of a graph.</summary>
    /// <typeparam name="T">The element type the bridge accepts.</typeparam>
    /// <param name="bridge">The bridge binding this silo registered.</param>
    /// <returns>The typed handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="bridge"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// <b>What a bridge is.</b> The run publishes a receiver under an address composed of its own identity
    /// and this binding's name — <c>{graph}/{run}/{binding}</c>, the key of an
    /// <see cref="Grains.IObserverBridgeGrain"/> — and grain code anywhere in the cluster pushes elements
    /// at that address for as long as the run is listening. A caller holding the run's ticket can derive
    /// the address without being told, which is what makes this usable without a directory of live runs.
    /// Two runs of one graph therefore have two bridges and never share one.
    /// </para>
    /// <para>
    /// <b>Best effort, and observably so.</b> There is no history, no replay, and no delivery to a run that
    /// has not attached yet or has already ended. What this bridge adds over silence is that every push
    /// answers with what became of it —
    /// <see cref="Grains.DataflowPushOutcome.Accepted"/>,
    /// <see cref="Grains.DataflowPushOutcome.Dropped"/>,
    /// <see cref="Grains.DataflowPushOutcome.Closed"/>, or
    /// <see cref="Grains.DataflowPushOutcome.Failed"/> — so a caller learns that a run stopped listening
    /// rather than guessing.
    /// </para>
    /// <para>
    /// <b>Acknowledgement</b>: the offer into the run's bounded ingress. An element a push accepted may
    /// still be lost by a run that fails afterwards, exactly as a stream delivery may be.
    /// </para>
    /// <para>
    /// <b>Backpressure, and who pays for it</b>: the declared ingress bound, and the pusher. Under the
    /// backpressure policy a push waits for room, so the caller's grain call does not complete until the
    /// run has taken an element — and because the bridge grain is not reentrant, every other pusher waits
    /// behind it. That is backpressure applied to everyone sharing one bridge; a deployment that cannot pay
    /// it declares a dropping policy instead. A wait long enough to exceed Orleans' response timeout
    /// surfaces on the caller as a timed-out call, which is the ordinary cost of asking a grain to wait.
    /// </para>
    /// <para>
    /// <b>Ordering</b>: one pusher's elements arrive in the order it sent them, because the bridge grain
    /// serializes pushes and each caller awaits its own. Nothing is ordered across pushers.
    /// </para>
    /// <para>
    /// <b>What a receiver that vanishes costs, measured.</b> A run whose process is gone without having
    /// detached leaves a reference that neither answers nor fails: the push hangs until Orleans' own
    /// response timeout expires — thirty seconds by default — and only then is reported as
    /// <see cref="Grains.DataflowPushOutcome.Closed"/>, after which the bridge forgets the receiver and
    /// every later push is refused at once. That was observed rather than deduced, and it is the reason the
    /// bridge forgets a refusing receiver instead of asking it again: the cost is paid once per lost run
    /// rather than once per push. A pusher that cannot wait that long shortens
    /// <c>MessagingOptions.ResponseTimeout</c>, which is a cluster-wide decision and therefore not this
    /// adapter's to make.
    /// </para>
    /// <para>
    /// <b>Why not <c>IGrainObserver</c> subscriptions</b>: because the direction is the other way round.
    /// This bridge is what a run offers to publishers, and the receiver it publishes <em>is</em> an Orleans
    /// grain observer. A variant where the run subscribes to somebody else's observer list can layer on top
    /// later; the semantics claimed here — best effort, no replay, delivery only while the run lives — are
    /// the ones the capability matrix's observer row states, and they are the same either way.
    /// </para>
    /// </remarks>
    public static RegisteredSource<T> ObserverBridge<T>(ObserverBridgeBinding<T> bridge)
    {
        ArgumentNullException.ThrowIfNull(bridge);

        return RegisteredStage.Source(Catalog, ObserverBridgeStage, Element<T>());
    }

    /// <summary>Declares a Broadcast Channel publication as the typed end of a graph.</summary>
    /// <typeparam name="T">The element type the channel carries in this process.</typeparam>
    /// <param name="element">The broadcast element binding this silo registered.</param>
    /// <returns>The typed handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="element"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// <b>Acknowledgement</b>: one awaited <c>Publish</c> per element, and what that awaits depends on the
    /// provider's configuration. With <c>FireAndForgetDelivery</c> off, the publication completes when
    /// every implicit subscriber has handled the element; with it on, it completes when the deliveries have
    /// been dispatched and a subscriber that throws is never reported. Either way it is publication and
    /// never end-to-end processing.
    /// </para>
    /// <para>
    /// <b>The delivery mode is declared and checked, not chosen.</b> A channel's mode belongs to the
    /// provider a silo registered, so a document cannot select it per publication. What the payload carries
    /// is the mode the author wrote the document against, and a silo whose provider is configured the other
    /// way refuses the run at materialization rather than quietly giving it different semantics.
    /// </para>
    /// <para>
    /// <b>Subscription</b>: implicit only. A Broadcast Channel has no explicit subscription and no
    /// subscriber list a publisher can see, so this sink cannot tell whether anybody is listening, and a
    /// publication to a channel with no subscribers is a success. That is the capability matrix's
    /// best-effort row and not a limitation of this adapter.
    /// </para>
    /// <para>
    /// <b>History and replay</b>: none. A channel keeps nothing, so a subscriber that was not there is not
    /// caught up afterwards.
    /// </para>
    /// <para>
    /// <b>Ordering</b>: elements are published one at a time in the order the run produced them. What order
    /// a subscriber observes across several publishers is the channel's business and is not promised here.
    /// </para>
    /// <para>
    /// <b>Cancellation</b>: observed between elements. A terminal in this engine is a synchronous fold and
    /// is handed no token, so a publication already in flight when a run is cancelled runs to its own end;
    /// what a cancellation stops is the publication of the next element. That is a limit of the phase-1
    /// terminal seam, stated rather than hidden.
    /// </para>
    /// <para>
    /// <b>Completion</b>: a run that ends signals nothing on the channel. A channel has no end a publisher
    /// can honestly declare, so saying one would tell every subscriber something about a channel this run
    /// does not own.
    /// </para>
    /// </remarks>
    public static RegisteredSink<T> BroadcastSink<T>(BroadcastElementBinding<T> element)
    {
        ArgumentNullException.ThrowIfNull(element);

        return RegisteredStage.Sink(Catalog, BroadcastSinkStage, Element<T>());
    }

    /// <summary>Composes the address of one run's observer bridge.</summary>
    /// <param name="graphId">The identity of the pipeline the run belongs to.</param>
    /// <param name="runId">The identity of the run.</param>
    /// <param name="bridge">The registered bridge's name.</param>
    /// <returns>The key of the <see cref="Grains.IObserverBridgeGrain"/> that run publishes on.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <see langword="null"/>.</exception>
    /// <remarks>
    /// The one place the format is written down, so that the run composing its address and the caller
    /// deriving it cannot disagree. Both halves are what they already have: a run knows its own grain key,
    /// and a caller holds the ticket the coordinator issued.
    /// </remarks>
    public static string ObserverBridgeKey(string graphId, string runId, string bridge)
    {
        ArgumentNullException.ThrowIfNull(graphId);
        ArgumentNullException.ThrowIfNull(runId);
        ArgumentNullException.ThrowIfNull(bridge);

        return $"{graphId}/{runId}/{bridge}";
    }

    /// <summary>Composes the address of one run's observer bridge from the ticket that started it.</summary>
    /// <param name="ticket">The ticket the coordinator issued for the run.</param>
    /// <param name="bridge">The registered bridge's name.</param>
    /// <returns>The key of the <see cref="Grains.IObserverBridgeGrain"/> that run publishes on.</returns>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    public static string ObserverBridgeKey(PipelineRunTicket ticket, string bridge)
    {
        ArgumentNullException.ThrowIfNull(ticket);

        return ObserverBridgeKey(ticket.GraphId, ticket.RunId, bridge);
    }

    /// <summary>Writes the payload of one reminder trigger occurrence.</summary>
    /// <param name="period">The period between ticks; whole milliseconds and at least one.</param>
    /// <param name="ingress">
    /// The bounded ingress the ticks land in, whose overflow policy may not be
    /// <see cref="OverflowPolicy.Backpressure"/>.
    /// </param>
    /// <returns>The canonical payload.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="ingress"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="period"/> is below one millisecond or beyond <see cref="int.MaxValue"/>
    /// milliseconds.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="ingress"/> declares the backpressuring policy, which a clock cannot honor.
    /// </exception>
    public static CanonicalJsonValue ReminderTriggerParameters(TimeSpan period, BufferOptions ingress)
    {
        ArgumentNullException.ThrowIfNull(ingress);
        ArgumentOutOfRangeException.ThrowIfLessThan(period.TotalMilliseconds, 1, nameof(period));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(period.TotalMilliseconds, int.MaxValue, nameof(period));

        if (ingress.OverflowPolicy is OverflowPolicy.Backpressure)
        {
            throw new ArgumentException(
                "A reminder trigger cannot backpressure a cluster reminder: the schedule is the cluster's and a tick that finds no room is dropped or fails by policy. Declare one of the dropping policies or the failing one.",
                nameof(ingress));
        }

        return ReminderTriggerPayload.Write(period, ingress);
    }

    /// <summary>Writes the payload of one observer bridge occurrence.</summary>
    /// <typeparam name="T">The element type the bridge accepts.</typeparam>
    /// <param name="bridge">The bridge binding this silo registered.</param>
    /// <param name="ingress">The bounded ingress the pushes land in.</param>
    /// <returns>The canonical payload.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="bridge"/> or <paramref name="ingress"/> is <see langword="null"/>.
    /// </exception>
    public static CanonicalJsonValue ObserverBridgeParameters<T>(
        ObserverBridgeBinding<T> bridge,
        BufferOptions ingress)
    {
        ArgumentNullException.ThrowIfNull(bridge);
        ArgumentNullException.ThrowIfNull(ingress);

        return ObserverBridgePayload.Write(bridge.Name, bridge.Output.Reference.ToString(), ingress);
    }

    /// <summary>Writes the payload of one broadcast sink occurrence.</summary>
    /// <typeparam name="T">The element type the channel carries.</typeparam>
    /// <param name="element">The broadcast element binding this silo registered.</param>
    /// <param name="channel">The channel to publish to.</param>
    /// <param name="fireAndForgetDelivery">
    /// The delivery mode this document is written against, checked against the silo's provider when the run
    /// is materialized.
    /// </param>
    /// <returns>The canonical payload.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="element"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="channel"/> is the default value.</exception>
    public static CanonicalJsonValue BroadcastSinkParameters<T>(
        BroadcastElementBinding<T> element,
        OrleansStreamAddress channel,
        bool fireAndForgetDelivery)
    {
        ArgumentNullException.ThrowIfNull(element);
        RequireAddress(channel);

        return BroadcastSinkPayload.Write(
            element.Element.Reference.ToString(),
            channel.Provider,
            channel.Namespace,
            channel.Key,
            fireAndForgetDelivery);
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

    /// <summary>Writes the payload of one keyed grain call occurrence.</summary>
    /// <typeparam name="TIn">The element type the call consumes.</typeparam>
    /// <typeparam name="TOut">The element type the call produces.</typeparam>
    /// <param name="call">The keyed call binding this silo registered.</param>
    /// <param name="maxInFlight">
    /// The greatest number of calls in flight at once across all keys; at least one. One call per key is
    /// held regardless, and is not configurable: it is where this stage's per-key ordering comes from.
    /// </param>
    /// <param name="distributed">
    /// Whether each key's calls run on an executor grain of their own, which is what lets the cluster place
    /// a run's keyed work across silos. Left off, the calls are made from inside the run.
    /// </param>
    /// <param name="timeout">
    /// The per-call timeout, or <see langword="null"/> to leave the wait to Orleans' own call timeout. It
    /// bounds the whole hop — the executor's call included — rather than only the part nearest the run.
    /// </param>
    /// <returns>The canonical payload.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="call"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="maxInFlight"/> is below one, or <paramref name="timeout"/> is not positive.
    /// </exception>
    public static CanonicalJsonValue KeyedGrainCallParameters<TIn, TOut>(
        KeyedGrainCallBinding<TIn, TOut> call,
        int maxInFlight,
        bool distributed = false,
        TimeSpan? timeout = null)
    {
        ArgumentNullException.ThrowIfNull(call);
        RequireBounds(maxInFlight, timeout);

        return KeyedGrainCallPayload.Write(
            call.Name,
            call.Input.Reference.ToString(),
            call.Output.Reference.ToString(),
            maxInFlight,
            distributed,
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
                KeyedGrainCallStage,
                [InputPortSpecification.Create(InputPort, ElementContract)],
                [OutputPortSpecification.Create(OutputPort, ElementContract)],
                [],
                KeyedGrainCallParameterContract,
                [],
                new OrleansStageValidator(registry, OrleansStageKind.KeyedGrainCall)),
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
            StageSpecification.Create(
                ReminderTriggerStage,
                [],
                [OutputPortSpecification.Create(OutputPort, ElementContract)],
                [],
                ReminderTriggerParameterContract,
                [],
                new OrleansStageValidator(registry, OrleansStageKind.ReminderTrigger)),
            StageSpecification.Create(
                ObserverBridgeStage,
                [],
                [OutputPortSpecification.Create(OutputPort, ElementContract)],
                [],
                ObserverBridgeParameterContract,
                [],
                new OrleansStageValidator(registry, OrleansStageKind.ObserverBridge)),
            StageSpecification.Create(
                BroadcastSinkStage,
                [InputPortSpecification.Create(InputPort, ElementContract)],
                [],
                [],
                BroadcastSinkParameterContract,
                [],
                new OrleansStageValidator(registry, OrleansStageKind.BroadcastSink)),
        ]);

    /// <summary>Refuses an address that addresses nothing.</summary>
    /// <param name="address">The address.</param>
    /// <param name="parameter">The parameter name to report it under.</param>
    /// <exception cref="ArgumentException"><paramref name="address"/> is the default value.</exception>
    /// <remarks>
    /// The parameter name is passed rather than inferred, because one address type serves a stream and a
    /// channel and a refusal that named the wrong argument would send an author to the wrong line.
    /// </remarks>
    private static void RequireAddress(
        OrleansStreamAddress address,
        [System.Runtime.CompilerServices.CallerArgumentExpression(nameof(address))] string? parameter = null)
    {
        if (address.IsDefault)
        {
            throw new ArgumentException(
                $"An Orleans adapter's payload requires a created {nameof(OrleansStreamAddress)}; the default value addresses nothing.",
                parameter);
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
