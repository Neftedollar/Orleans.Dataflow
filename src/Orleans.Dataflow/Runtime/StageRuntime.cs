namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The executable form of one registered stage: what a runtime factory hands back when it resolves a
/// node the document names but binds no delegate to.
/// </summary>
/// <remarks>
/// <para>
/// This is the runtime-factory seam's return type, and it is deliberately the same four shapes the local
/// engine already executes: a source it pulls from, a synchronous element stage it fuses, an asynchronous
/// element stage that heads its own segment, and a terminal that folds the stream into one value. A
/// provider that wants a fifth shape is asking for a new engine primitive rather than a new stage, and
/// this type refusing to grow is what keeps that distinction visible.
/// </para>
/// <para>
/// A stage runtime is built once per node per materialization, exactly as a local binding is wrapped once
/// per node per materialization, so whatever state a provider's closures hold is fresh per run. Two runs
/// of one pipeline share nothing this type carries.
/// </para>
/// <para>
/// The values are untyped because the engine is: a document never names an element type, so the plan
/// speaks in <see cref="object"/> and a provider's factory is the place that knows what its own elements
/// are. That is the same trade the local vocabulary makes at <see cref="LocalDelegateAdapter"/>, moved to
/// the other side of the seam.
/// </para>
/// </remarks>
internal sealed class StageRuntime
{
    /// <summary>Initializes a new instance of the <see cref="StageRuntime"/> class.</summary>
    /// <param name="shape">Which of the four executable shapes this is.</param>
    /// <param name="opener">The source opener, for a source.</param>
    /// <param name="map">The synchronous mapping, for an element stage.</param>
    /// <param name="callback">The asynchronous mapping, for an asynchronous element stage.</param>
    /// <param name="maxConcurrency">The concurrency bound of an asynchronous element stage.</param>
    /// <param name="ordered">Whether an asynchronous element stage emits in input order.</param>
    /// <param name="seed">The maker of a terminal's initial state.</param>
    /// <param name="fold">A terminal's fold over its state and one element.</param>
    /// <param name="finish">A terminal's projection of its final state into the value a slot resolves.</param>
    /// <param name="producesResult">Whether a terminal's final state is offered to a result slot.</param>
    private StageRuntime(
        StageRuntimeShape shape,
        StageSourceOpener? opener,
        Func<object?, object?>? map,
        Func<object?, CancellationToken, ValueTask<object?>>? callback,
        int maxConcurrency,
        bool ordered,
        Func<object?>? seed,
        Func<object?, object?, object?>? fold,
        Func<object?, object?>? finish,
        bool producesResult)
    {
        Shape = shape;
        Opener = opener;
        Map = map;
        Callback = callback;
        MaxConcurrency = maxConcurrency;
        Ordered = ordered;
        Seed = seed;
        Fold = fold;
        Finish = finish;
        ProducesResult = producesResult;
    }

    /// <summary>Gets which of the four executable shapes this runtime is.</summary>
    internal StageRuntimeShape Shape { get; }

    /// <summary>Gets the opener of a source's sequence.</summary>
    /// <value>The opener for <see cref="StageRuntimeShape.Source"/>; otherwise <see langword="null"/>.</value>
    internal StageSourceOpener? Opener { get; }

    /// <summary>Gets the mapping a synchronous element stage applies.</summary>
    /// <value>The mapping for <see cref="StageRuntimeShape.Element"/>; otherwise <see langword="null"/>.</value>
    internal Func<object?, object?>? Map { get; }

    /// <summary>Gets the callback an asynchronous element stage awaits per element.</summary>
    /// <value>
    /// The callback for <see cref="StageRuntimeShape.ElementAsync"/>; otherwise <see langword="null"/>.
    /// </value>
    internal Func<object?, CancellationToken, ValueTask<object?>>? Callback { get; }

    /// <summary>Gets the greatest number of callbacks an asynchronous element stage runs at once.</summary>
    /// <value>At least one for <see cref="StageRuntimeShape.ElementAsync"/>; otherwise zero.</value>
    internal int MaxConcurrency { get; }

    /// <summary>Gets a value indicating whether an asynchronous element stage emits in input order.</summary>
    internal bool Ordered { get; }

    /// <summary>Gets the maker of a terminal's initial state.</summary>
    /// <value>The factory for <see cref="StageRuntimeShape.Terminal"/>; otherwise <see langword="null"/>.</value>
    /// <remarks>
    /// A factory rather than a value, because a plan is built once and a run's state has to be its own:
    /// a terminal that accumulates into a mutable object would otherwise be one object two runs both
    /// wrote into. This is the same rule <see cref="LocalRunPlan.SeedFactory"/> states for the local
    /// collecting sink, applied to every provider terminal without asking the provider to know it.
    /// </remarks>
    internal Func<object?>? Seed { get; }

    /// <summary>Gets the fold a terminal applies to every element that reaches it.</summary>
    /// <value>The fold for <see cref="StageRuntimeShape.Terminal"/>; otherwise <see langword="null"/>.</value>
    internal Func<object?, object?, object?>? Fold { get; }

    /// <summary>Gets a terminal's projection of its final state into the value a slot resolves.</summary>
    /// <value>The projection, or <see langword="null"/> when the accumulated state is already the value.</value>
    internal Func<object?, object?>? Finish { get; }

    /// <summary>Gets a value indicating whether a terminal's final state is offered to a result slot.</summary>
    /// <value>
    /// <see langword="true"/> for a terminal a document may declare a result slot over;
    /// <see langword="false"/> for one whose work is its side effect.
    /// </value>
    internal bool ProducesResult { get; }

    /// <summary>Creates the runtime of a source stage.</summary>
    /// <param name="opener">The opener of one enumeration, invoked once per run at the first pull.</param>
    /// <returns>The runtime.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="opener"/> is <see langword="null"/>.</exception>
    internal static StageRuntime Source(StageSourceOpener opener)
    {
        ArgumentNullException.ThrowIfNull(opener);

        return new StageRuntime(
            StageRuntimeShape.Source,
            opener,
            map: null,
            callback: null,
            maxConcurrency: 0,
            ordered: false,
            seed: null,
            fold: null,
            finish: null,
            producesResult: false);
    }

    /// <summary>Creates the runtime of a synchronous element stage.</summary>
    /// <param name="map">The mapping applied on the thread that pulled the element.</param>
    /// <returns>The runtime.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="map"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// A mapping and not a filter: a stage that drops elements is a shape the engine has and this seam
    /// deliberately does not expose yet, because a provider whose stage can swallow an element needs the
    /// completion semantics of that decision stated first, and phase 1 has no provider that wants it.
    /// </remarks>
    internal static StageRuntime Element(Func<object?, object?> map)
    {
        ArgumentNullException.ThrowIfNull(map);

        return new StageRuntime(
            StageRuntimeShape.Element,
            opener: null,
            map,
            callback: null,
            maxConcurrency: 0,
            ordered: false,
            seed: null,
            fold: null,
            finish: null,
            producesResult: false);
    }

    /// <summary>Creates the runtime of an asynchronous element stage.</summary>
    /// <param name="callback">The callback awaited per element.</param>
    /// <param name="maxConcurrency">The greatest number of callbacks in flight at once; at least one.</param>
    /// <param name="ordered">Whether results are emitted in the order their elements arrived.</param>
    /// <returns>The runtime.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="callback"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxConcurrency"/> is below one.</exception>
    internal static StageRuntime ElementAsync(
        Func<object?, CancellationToken, ValueTask<object?>> callback,
        int maxConcurrency,
        bool ordered)
    {
        ArgumentNullException.ThrowIfNull(callback);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxConcurrency, 1);

        return new StageRuntime(
            StageRuntimeShape.ElementAsync,
            opener: null,
            map: null,
            callback,
            maxConcurrency,
            ordered,
            seed: null,
            fold: null,
            finish: null,
            producesResult: false);
    }

    /// <summary>Creates the runtime of a terminal stage.</summary>
    /// <param name="seed">The maker of this run's initial state.</param>
    /// <param name="fold">The fold over the state and one element.</param>
    /// <param name="finish">
    /// The projection of the final state into the value a slot resolves, or <see langword="null"/> when
    /// the state is already that value.
    /// </param>
    /// <param name="producesResult">Whether a document may declare a result slot over this terminal.</param>
    /// <returns>The runtime.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="seed"/> or <paramref name="fold"/> is <see langword="null"/>.
    /// </exception>
    internal static StageRuntime Terminal(
        Func<object?> seed,
        Func<object?, object?, object?> fold,
        Func<object?, object?>? finish,
        bool producesResult)
    {
        ArgumentNullException.ThrowIfNull(seed);
        ArgumentNullException.ThrowIfNull(fold);

        return new StageRuntime(
            StageRuntimeShape.Terminal,
            opener: null,
            map: null,
            callback: null,
            maxConcurrency: 0,
            ordered: false,
            seed,
            fold,
            finish,
            producesResult);
    }

    /// <summary>Wraps this runtime's callback into the shape the asynchronous driver executes.</summary>
    /// <returns>A callback over boxed elements that never throws synchronously.</returns>
    /// <remarks>
    /// A callback that throws before returning its task returns a faulted one instead, because the run
    /// observes a failure in exactly one place and a synchronous throw here would be a second one. The
    /// conversion to <see cref="Task{TResult}"/> is the driver's own shape and is done once per stage per
    /// run rather than once per element.
    /// </remarks>
    internal Func<object?, CancellationToken, Task<object?>> AsAsyncCallback()
    {
        Func<object?, CancellationToken, ValueTask<object?>> callback = Callback!;

        return (element, cancellationToken) =>
        {
            try
            {
                return callback(element, cancellationToken).AsTask();
            }
            catch (Exception failure)
            {
                return Task.FromException<object?>(failure);
            }
        };
    }
}

/// <summary>
/// Which of the engine's four executable shapes a resolved registered stage takes.
/// </summary>
/// <remarks>
/// The shape decides where in a plan the stage may stand: a source opens the chain, an element stage
/// stands between the source and the terminal, and a terminal closes it. A stage whose shape does not fit
/// its position is a planning failure that names both, rather than a run that misbehaves.
/// </remarks>
internal enum StageRuntimeShape
{
    /// <summary>The head of a chain, which produces elements and consumes none.</summary>
    Source,

    /// <summary>An element-to-element stage applied on the thread that pulled the element.</summary>
    Element,

    /// <summary>An element-to-element stage whose callback is awaited, and which heads its own segment.</summary>
    ElementAsync,

    /// <summary>The end of a chain, which consumes elements and produces none.</summary>
    Terminal,
}

/// <summary>
/// The two tokens a registered source is opened under.
/// </summary>
/// <param name="RunToken">
/// Cancelled when the run is cancelled and when anything in the run fails. A source that abandons its
/// work observes this one.
/// </param>
/// <param name="StopToken">
/// Cancelled for everything <paramref name="RunToken"/> is cancelled for, and additionally when a
/// graceful shutdown is asked for. A source released by this token and not by
/// <paramref name="RunToken"/> ends its sequence as if it had run out, which is what makes a shutdown
/// drain rather than abandon.
/// </param>
/// <param name="RunIdentity">
/// What this run is called in this deployment, unique among the runs a deployment has in flight. A source
/// that has to be addressable from outside the run — a bridge something else pushes into — composes its
/// own identity from this and its binding's name; every other source ignores it.
/// </param>
/// <remarks>
/// <para>
/// The pair of tokens is the same one <see cref="LocalRunContext"/> gives the local vocabulary's own
/// waiting sources, handed across the seam because a registered source is exactly the kind that waits: a
/// stream subscription, a queue, a grain enumeration. A provider that ignores both is a slow source and
/// delays a stop until its next yield, which is the documented cooperative rule and not an oversight.
/// </para>
/// <para>
/// The identity travels with the tokens rather than with the node, and the distinction is the seam's:
/// a <see cref="StageRuntimeRequest"/> is answered once per materialization and says what a stage is,
/// while these are handed over once per run and say which run is opening it. A factory therefore still
/// receives no run identity — the run announces itself when it asks the source to open.
/// </para>
/// </remarks>
internal readonly record struct StageRunTokens(
    string RunIdentity,
    CancellationToken RunToken,
    CancellationToken StopToken);

/// <summary>
/// Opens one enumeration of a registered source's elements.
/// </summary>
/// <param name="tokens">The tokens of the run being opened.</param>
/// <returns>The sequence, which the run enumerates exactly once and disposes on every terminal path.</returns>
/// <remarks>
/// Invoked once per run, at the first pull, so a run stopped before its first element never touches its
/// source. The sequence is asynchronous because every Orleans-native source is: the engine pulls it on the
/// segment's own dedicated thread, which is what makes an awaited delivery ordinary rather than special.
/// </remarks>
internal delegate IAsyncEnumerable<object?> StageSourceOpener(StageRunTokens tokens);
