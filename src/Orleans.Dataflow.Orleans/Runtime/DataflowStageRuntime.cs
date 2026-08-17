using Orleans.Dataflow.Runtime;

namespace Orleans.Dataflow.Hosting;

/// <summary>
/// The executable form of one registered stage, as a provider hands it to a silo.
/// </summary>
/// <remarks>
/// <para>
/// Four shapes and no more, because these are the four the run's engine executes: a source it pulls from,
/// a synchronous stage it fuses into the pull loop, an asynchronous stage that heads its own segment with
/// a bounded number of callbacks in flight, and a terminal that folds the stream into one value. A stage
/// that wants a fifth shape is asking for a new engine primitive rather than a new stage.
/// </para>
/// <para>
/// A stage runtime is built once per node per run, so whatever a provider's closures capture is fresh per
/// run: two runs of one pipeline share nothing this type carries. A terminal is given a seed
/// <em>factory</em> rather than a seed for exactly that reason — a mutable accumulator handed over as a
/// value would be one object that two runs both wrote into.
/// </para>
/// <para>
/// The values are untyped because a document never names an element type, so the engine works in
/// <see cref="object"/> and the factory is the one place that knows what the provider's own elements are.
/// Elements that cross a grain or a stream boundary are additionally the author's types and must satisfy
/// Orleans serialization; that requirement is the provider's to state and is checked at first use.
/// </para>
/// <para>
/// <b>Scope.</b> This is the phase-1 spelling of the provider seam, shipped because a silo cannot register
/// a stage without one. The provider SDK that will make it comfortable — typed payload builders, typed
/// element wrappers, per-stage registration — is M4, and nothing here is a promise about that shape beyond
/// the four executable forms themselves.
/// </para>
/// </remarks>
public sealed class DataflowStageRuntime
{
    /// <summary>Initializes a new instance of the <see cref="DataflowStageRuntime"/> class.</summary>
    /// <param name="runtime">The engine's own form of the stage.</param>
    private DataflowStageRuntime(StageRuntime runtime) => Runtime = runtime;

    /// <summary>Gets the engine's own form of this stage.</summary>
    /// <value>The internal runtime the planner consumes.</value>
    /// <remarks>
    /// Internal, which is the whole reason this wrapper exists: the engine's executor vocabulary is not
    /// public API and publishing it here would fix the M4 provider SDK's shape by accident.
    /// </remarks>
    internal StageRuntime Runtime { get; }

    /// <summary>Creates the runtime of a source stage.</summary>
    /// <param name="open">
    /// The opener of one enumeration, invoked once per run at the run's first pull, under that run's
    /// tokens.
    /// </param>
    /// <returns>The runtime.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="open"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The sequence is enumerated exactly once and disposed on every terminal path of the run, including
    /// the ones where reading it is what went wrong. A source that respects
    /// <see cref="DataflowRunTokens.StopToken"/> ends its sequence on a graceful shutdown and lets the run
    /// drain; a source that respects only <see cref="DataflowRunTokens.RunToken"/> is stopped by
    /// cancellation and delays a shutdown until it next yields, which is a documented cooperative rule
    /// rather than a defect.
    /// </remarks>
    public static DataflowStageRuntime Source(Func<DataflowRunTokens, IAsyncEnumerable<object?>> open)
    {
        ArgumentNullException.ThrowIfNull(open);

        return new DataflowStageRuntime(
            StageRuntime.Source(tokens =>
                open(new DataflowRunTokens(tokens.RunIdentity, tokens.RunToken, tokens.StopToken))));
    }

    /// <summary>Creates the runtime of a synchronous element stage.</summary>
    /// <param name="map">The mapping, applied on the thread that pulled the element.</param>
    /// <returns>The runtime.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="map"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The mapping runs inside the run's own pull loop with nothing between it and the stage before it, so
    /// it must not block: a stage that waits belongs in <see cref="ElementAsync"/>, where waiting is what
    /// the shape is for and the concurrency bound says how much of it may happen at once.
    /// </remarks>
    public static DataflowStageRuntime Element(Func<object?, object?> map)
    {
        ArgumentNullException.ThrowIfNull(map);

        return new DataflowStageRuntime(StageRuntime.Element(map));
    }

    /// <summary>Creates the runtime of an asynchronous element stage.</summary>
    /// <param name="map">The callback awaited per element.</param>
    /// <param name="maxConcurrency">The greatest number of callbacks in flight at once; at least one.</param>
    /// <param name="ordered">
    /// Whether results are emitted in the order their elements arrived, rather than as their callbacks
    /// complete.
    /// </param>
    /// <returns>The runtime.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="map"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxConcurrency"/> is below one.</exception>
    /// <remarks>
    /// The bound is the backpressure: an awaited call in flight is credit spent, and the elements reach
    /// this stage through a bounded channel rather than an unbounded queue. Nothing anywhere in a run uses
    /// a mailbox as a buffer.
    /// </remarks>
    public static DataflowStageRuntime ElementAsync(
        Func<object?, CancellationToken, ValueTask<object?>> map,
        int maxConcurrency,
        bool ordered)
    {
        ArgumentNullException.ThrowIfNull(map);

        return new DataflowStageRuntime(StageRuntime.ElementAsync(map, maxConcurrency, ordered));
    }

    /// <summary>Creates the runtime of a terminal stage.</summary>
    /// <param name="seed">The maker of this run's initial state, invoked once per run.</param>
    /// <param name="fold">The fold over the accumulated state and one element.</param>
    /// <param name="finish">
    /// The projection of the final state into the value a result slot resolves, or <see langword="null"/>
    /// when the accumulated state is already that value.
    /// </param>
    /// <param name="producesResult">
    /// Whether the final state is offered to a result slot the document declares over this stage.
    /// </param>
    /// <returns>The runtime.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="seed"/> or <paramref name="fold"/> is <see langword="null"/>.
    /// </exception>
    /// <remarks>
    /// <para>
    /// Every terminal is a fold, including the ones that look like something else: a sink that writes
    /// somewhere folds over an unchanged state and does its work in the callback, and a sink that
    /// aggregates folds over the aggregate. That is one shape rather than several, and it is why the
    /// engine has one statement of what a terminal does with an element.
    /// </para>
    /// <para>
    /// A result a run hands back over a grain boundary must satisfy Orleans serialization. That is checked
    /// at first use rather than at registration, because nothing here knows what type the provider's fold
    /// will produce.
    /// </para>
    /// </remarks>
    public static DataflowStageRuntime Terminal(
        Func<object?> seed,
        Func<object?, object?, object?> fold,
        Func<object?, object?>? finish,
        bool producesResult)
    {
        ArgumentNullException.ThrowIfNull(seed);
        ArgumentNullException.ThrowIfNull(fold);

        return new DataflowStageRuntime(StageRuntime.Terminal(seed, fold, finish, producesResult));
    }
}

/// <summary>
/// The two tokens a registered source is opened under.
/// </summary>
/// <param name="RunToken">
/// Cancelled when the run is cancelled and when anything in the run fails. This is the token a provider's
/// own asynchronous work should carry.
/// </param>
/// <param name="StopToken">
/// Cancelled for everything <paramref name="RunToken"/> is cancelled for, and additionally when a graceful
/// shutdown is asked for. A source released by this token alone ends its sequence as if it had run out,
/// which is what makes a shutdown drain the run instead of abandoning it.
/// </param>
/// <param name="RunIdentity">
/// What this run is called in the cluster — the run grain's own key, <c>{graph}/{run}</c> — and therefore
/// unique among the runs a deployment has in flight. A source that has to be addressable from outside the
/// run composes its address from this and its binding's name, so that the same address is derivable by a
/// caller holding the run's ticket. Every other source ignores it.
/// </param>
/// <remarks>
/// <para>
/// The pair of tokens states the difference between the two ways a run stops. Cancellation abandons the
/// run and resolves nothing; shutdown stops production and lets everything already admitted flow to the
/// terminal, so an aggregate resolves its slot with the state it accumulated. A source that watches only
/// the first token is correct but blunt: it turns every shutdown into a wait for its next yield.
/// </para>
/// <para>
/// The identity travels here rather than on <see cref="DataflowStageRequest"/>, and the distinction
/// matters: a stage request is answered once per materialization and says what a stage is, while these are
/// handed over once per run and say which run is opening it. A factory therefore still receives no run
/// identity, and a stage's behavior still cannot depend on which graph it is standing in.
/// </para>
/// </remarks>
public readonly record struct DataflowRunTokens(
    string RunIdentity,
    CancellationToken RunToken,
    CancellationToken StopToken);
