using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Adapters;

/// <summary>
/// The .NET push-bridge vocabulary: two registered stages that head a run, the catalog that publishes
/// them, and the typed handles and payloads an author writes them with.
/// </summary>
/// <remarks>
/// <para>
/// <b>Two stages and no more.</b> A run-scoped periodic tick, and a subscription to a named
/// <see cref="IObservable{T}"/>. Both are real registered stages — named in a document, resolved from a
/// host's catalog by identity, built by a runtime factory — so a pipeline written with them carries no
/// delegate, no CLR name, and nothing a document could not honestly say.
/// </para>
/// <para>
/// <b>They need no cluster.</b> This vocabulary lives in the main package because nothing about a timer or
/// an <see cref="IObservable{T}"/> is an Orleans concept. One registration therefore serves both hosts: a
/// process registers the bindings once, and the same document runs on <see cref="LocalDataflowHost"/> and
/// on a silo. That is the runtime-factory seam's own claim, and this vocabulary is what makes it checkable
/// rather than asserted.
/// </para>
/// <para>
/// <b>A .NET event is deliberately absent.</b> An event is one adapter away from an
/// <see cref="IObservable{T}"/> — a few lines that add a handler on subscribe and remove it on dispose —
/// and a stage for it would be a second registration surface, a second payload, and a second set of
/// lifetime rules for the same delivery semantics. The row is covered by the observable half plus that
/// wrapping, and it is stated here rather than implied by omission.
/// </para>
/// <para>
/// <b>The element contract these ports declare is the main package's opaque one.</b> These stages produce
/// the author's own CLR types, which live in the C# type system and never in the document, exactly as a
/// local stage's do — so they declare the same opaque reference every local port declares, which is what
/// lets a push source head a chain of local operators with no contract friction. The consequence for the
/// other direction is stated rather than hidden: an edge from one of these to a stage that declares a
/// different element contract — an Orleans adapter's, for instance — is reported by the graph compiler as
/// an <c>element-contract-mismatch</c>, and a deployment's own registered stage joins them by declaring
/// <see cref="ElementContract"/> on the port that faces one.
/// </para>
/// </remarks>
public static class DotnetStages
{
    /// <summary>Gets the provider both .NET push adapters belong to.</summary>
    /// <value>The provider <c>dotnet</c>.</value>
    public static ProviderId Provider { get; } = ProviderId.Create("dotnet");

    /// <summary>Gets the reference of the run-scoped periodic tick source.</summary>
    /// <value><c>dotnet/timer@v1</c>.</value>
    public static StageRef TimerStage { get; } =
        StageRef.Create(Provider, StageId.Create("timer"), StageRef.FirstMajorVersion);

    /// <summary>Gets the reference of the observable subscription source.</summary>
    /// <value><c>dotnet/observable@v1</c>.</value>
    public static StageRef ObservableStage { get; } =
        StageRef.Create(Provider, StageId.Create("observable"), StageRef.FirstMajorVersion);

    /// <summary>Gets the one element contract every .NET push adapter port declares.</summary>
    /// <value><c>local-opaque@v1</c>.</value>
    /// <remarks>
    /// The same reference the local vocabulary declares, and shared rather than duplicated on purpose: one
    /// specification cannot declare a contract that differs per occurrence, and every element type in this
    /// package lives in the C# type system rather than in a document. Sharing it is what makes
    /// <c>timer -&gt; Select -&gt; Collect</c> a valid graph.
    /// </remarks>
    public static ContractReference ElementContract { get; } = LocalVocabulary.ElementContract;

    /// <summary>Gets the parameter contract a timer declares.</summary>
    /// <value><c>dotnet-timer-parameters@v1</c>.</value>
    public static ContractReference TimerParameterContract { get; } =
        ContractReference.Create(
            ContractId.Create("dotnet-timer-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>Gets the parameter contract an observable source declares.</summary>
    /// <value><c>dotnet-observable-parameters@v1</c>.</value>
    public static ContractReference ObservableParameterContract { get; } =
        ContractReference.Create(
            ContractId.Create("dotnet-observable-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>Gets the contract every timer tick is carried under.</summary>
    /// <value>The opaque element contract, declared as <see cref="long"/>.</value>
    /// <remarks>
    /// A tick is a <see cref="long"/> index counting from zero, and it is the same value in every process:
    /// the timer addresses no registration and produces nothing an author supplied, so unlike every other
    /// source in this vocabulary its element type is fixed by the stage rather than by a binding.
    /// </remarks>
    public static ElementContract<long> Tick => Element<long>();

    /// <summary>Declares the push adapters' opaque element contract as one CLR type's.</summary>
    /// <typeparam name="T">The CLR type that stands on the far side of a push adapter's port.</typeparam>
    /// <returns>The declaration.</returns>
    /// <remarks>
    /// The escape hatch made first class, and the same one the Orleans adapters expose. A deployment's own
    /// registered stage that wants to consume a push source declares <see cref="ElementContract"/> on the
    /// port that faces it, and this is how it writes the typed half of that declaration without spelling
    /// the contract's name as a string.
    /// </remarks>
    public static ElementContract<T> Element<T>() => ElementContract<T>.Of(ElementContract);

    /// <summary>Gets the catalog an authoring process resolves these stages through.</summary>
    /// <value>
    /// A catalog whose specifications are exactly the ones a host publishes, and whose parameter checks are
    /// the shape half of a host's: an authoring process can say that a payload is malformed and cannot say
    /// which names a deployment registered.
    /// </value>
    /// <remarks>
    /// One shared immutable value rather than a fresh one per call, because a catalog is immutable and its
    /// identity is its contents. It is byte-identical to a host's, so the two share a
    /// <see cref="CatalogFingerprint"/>: a parameter validator is behavior and never reaches a fingerprint.
    /// </remarks>
    public static StageCatalog Catalog { get; } = Publish(DotnetAdapterRegistry.Empty);

    /// <summary>Declares a run-scoped periodic tick as the typed start of a graph.</summary>
    /// <returns>The typed handle, producing the tick index.</returns>
    /// <remarks>
    /// <para>
    /// <b>Acknowledgement</b>: none. A tick is generated rather than delivered, so there is nothing to
    /// acknowledge and nothing that could be redelivered.
    /// </para>
    /// <para>
    /// <b>Scope and durability</b>: one run, in memory, non-durable. The timer is created when the run
    /// first pulls and disposed on every terminal path, so it dies with the run and leaves nothing behind —
    /// which is the capability matrix's "activation-scoped" timer row read honestly for a runtime whose
    /// unit is a run rather than an activation. Nothing here survives a restart; a trigger that must is a
    /// reminder.
    /// </para>
    /// <para>
    /// <b>Elements</b>: <see cref="long"/> indices from zero, one per tick, in order. The index counts the
    /// ticks this run emitted and is never a wall-clock reading, so it is stable to compare and useless to
    /// schedule against.
    /// </para>
    /// <para>
    /// <b>Backpressure</b>: the pull itself, and no queue anywhere. The timer is awaited on the run's own
    /// source thread, so a run that is slower than the period simply ticks later — ticks do not accumulate
    /// and none is dropped, because there is no buffer for them to accumulate in. That is the one honest
    /// difference between a run-scoped timer and a push source, and it is why this stage declares no
    /// ingress bound.
    /// </para>
    /// <para>
    /// <b>Completion</b>: the declared tick limit, or never. A timer with a limit ends its sequence after
    /// that many ticks and the run completes; a timer without one ends only when the run does, so an
    /// author who wants a bound writes one here or takes a bound downstream.
    /// </para>
    /// <para>
    /// <b>Shutdown and cancellation</b>: a graceful shutdown ends the sequence at once rather than after
    /// the current period, so the ticks already inside the graph drain and no further tick is produced. A
    /// cancellation abandons the wait and the run.
    /// </para>
    /// </remarks>
    public static RegisteredSource<long> Timer() => RegisteredStage.Source(Catalog, TimerStage, Tick);

    /// <summary>Declares a named <see cref="IObservable{T}"/> as the typed start of a graph.</summary>
    /// <typeparam name="T">The element type the sequence produces.</typeparam>
    /// <param name="source">The observable binding this host registered.</param>
    /// <returns>The typed handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// <b>Acknowledgement</b>: the offer into the run's bounded ingress, and not end-to-end processing. An
    /// element this adapter has accepted may still be lost by a run that fails afterwards; nothing here
    /// promises otherwise.
    /// </para>
    /// <para>
    /// <b>Subscription lifetime</b>: one run. The subscription is made when the run first pulls and
    /// disposed in the <c>finally</c> the engine reaches on every terminal path — completion, failure,
    /// cancellation, a graceful shutdown, and the disposal of the host that ran it. A binding that opens a
    /// cold observable therefore gets one producer per run; a binding that returns a hot one shares its
    /// elements between concurrent runs, which is the observable's own character and not something this
    /// stage can change.
    /// </para>
    /// <para>
    /// <b>Completion and failure</b>: <see cref="IObserver{T}.OnCompleted"/> ends the run's stream and the
    /// elements already admitted drain to the terminal; <see cref="IObserver{T}.OnError"/> faults the run
    /// with the very exception instance it was handed, and the elements still queued are abandoned, because
    /// failure wins over everything queued behind it.
    /// </para>
    /// <para>
    /// <b>Backpressure, and who pays for it</b>: the declared ingress bound, and the notification's own
    /// thread. <see cref="IObserver{T}.OnNext"/> has nothing to await, so under the backpressure policy a
    /// full ingress blocks whichever thread the observable pushes on until the run makes room. That is the
    /// same shared cost an Orleans stream source documents for a provider's pulling agent, moved to the
    /// producer that has no queue of its own: a source that cannot pay it declares a dropping policy
    /// instead, and then a full ingress answers at once and the drop is counted. Under the failing policy
    /// the run faults.
    /// </para>
    /// <para>
    /// <b>Ordering</b>: whatever the observable gives. <see cref="IObserver{T}"/> requires notifications to
    /// be serialized, and this adapter preserves that order into the ingress; an observable that violates
    /// the grammar by pushing concurrently gets an interleaving the ingress cannot repair.
    /// </para>
    /// <para>
    /// <b>Shutdown</b>: a graceful stop stops production, so the run takes no further element from the
    /// ingress and whatever the ingress still holds is abandoned. That is not a contradiction of the
    /// acknowledgement boundary but the honest reading of it: the boundary is the offer, and an element
    /// that reached the ingress and no further was never claimed to have been processed.
    /// </para>
    /// </remarks>
    public static RegisteredSource<T> Observable<T>(ObservableBinding<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        return RegisteredStage.Source(Catalog, ObservableStage, Element<T>());
    }

    /// <summary>Writes the payload of one timer occurrence.</summary>
    /// <param name="period">The period between ticks; at least one millisecond.</param>
    /// <param name="tickLimit">
    /// The greatest number of ticks this run produces, or zero for a timer that ticks until the run ends.
    /// </param>
    /// <returns>The canonical payload.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="period"/> is below one millisecond or beyond <see cref="int.MaxValue"/>
    /// milliseconds, or <paramref name="tickLimit"/> is negative.
    /// </exception>
    public static CanonicalJsonValue TimerParameters(TimeSpan period, long tickLimit = 0)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(period.TotalMilliseconds, 1, nameof(period));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(period.TotalMilliseconds, int.MaxValue, nameof(period));
        ArgumentOutOfRangeException.ThrowIfNegative(tickLimit);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(tickLimit, int.MaxValue, nameof(tickLimit));

        return TimerPayload.Write(period, tickLimit);
    }

    /// <summary>Writes the payload of one observable-source occurrence.</summary>
    /// <typeparam name="T">The element type the sequence produces.</typeparam>
    /// <param name="source">The observable binding this host registered.</param>
    /// <param name="ingress">The bounded ingress the notifications land in.</param>
    /// <returns>The canonical payload.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="source"/> or <paramref name="ingress"/> is <see langword="null"/>.
    /// </exception>
    public static CanonicalJsonValue ObservableParameters<T>(
        ObservableBinding<T> source,
        BufferOptions ingress)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(ingress);

        return ObservablePayload.Write(source.Name, source.Output.Reference.ToString(), ingress);
    }

    /// <summary>Builds the catalog a host publishes, with the checks that host's registry can make.</summary>
    /// <param name="registry">The host's registry.</param>
    /// <returns>The catalog.</returns>
    /// <remarks>
    /// The specifications are the same in every process; only the validators differ, and a validator is
    /// behavior and never reaches a <see cref="CatalogFingerprint"/>. That is what lets an authoring
    /// process resolve these stages against <see cref="Catalog"/> and a host accept the resulting document
    /// as a document of its own vocabulary.
    /// </remarks>
    internal static StageCatalog Publish(DotnetAdapterRegistry registry) =>
        StageCatalog.Create(
        [
            StageSpecification.Source(
                TimerStage,
                TimerParameterContract,
                OutputPortSpecification.Create(LocalVocabulary.OutputPort, ElementContract),
                new DotnetStageValidator(registry, DotnetStageKind.Timer)),
            StageSpecification.Source(
                ObservableStage,
                ObservableParameterContract,
                OutputPortSpecification.Create(LocalVocabulary.OutputPort, ElementContract),
                new DotnetStageValidator(registry, DotnetStageKind.Observable)),
        ]);
}
