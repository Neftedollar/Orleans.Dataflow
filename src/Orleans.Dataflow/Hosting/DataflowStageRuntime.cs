using Orleans.Dataflow.Runtime;

namespace Orleans.Dataflow.Hosting;

/// <summary>
/// The executable form of one registered stage, as a provider hands it to a host.
/// </summary>
/// <remarks>
/// <para>
/// Six shapes, and each one is an engine primitive that already exists rather than a new one. Four of them
/// are linear — a source the run pulls from, a synchronous stage it fuses into the pull loop, an
/// asynchronous stage that heads its own segment with a bounded number of callbacks in flight, and a
/// terminal that folds the stream into one value — and two of them are junctions, a fan-out that splits one
/// stream into the legs its specification declares and a fan-in that joins the inputs its specification
/// declares into one. A stage that wants a seventh shape is asking for a new engine primitive rather than a
/// new stage, and this type refusing to grow past what the engine runs is what keeps that distinction
/// visible.
/// </para>
/// <para>
/// The junction shapes arrived with M4.5. Until then a junction was necessarily a local stage, so a
/// branching graph declared <c>nondeployable</c> however many of its other stages were registered — the
/// limit ADR 0006 recorded and sent here. A provider that registers a junction closes that gap: the
/// junction's ports carry the provider's own contracts, its occurrence carries the author's own name, and a
/// fan-out pipeline built entirely from registered stages is a pipeline.
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
/// <b>One seam, two hosts.</b> This type lives in the core package and both hosts consume it: a silo
/// registers a factory through <c>AddOrleansDataflow</c> and an in-process host registers the very same
/// factory through <see cref="LocalDataflowHost"/>. That is what makes "a provider's stages run in either
/// runtime" a checkable claim rather than an intention, and it is why the type names no Orleans concept.
/// </para>
/// <para>
/// <b>Scope.</b> This is the executable half of the provider SDK. The comfortable half — typed payload
/// builders, typed element wrappers, per-stage registration — is still ahead, and nothing here is a promise
/// about that shape beyond the executable forms themselves.
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

    /// <summary>Creates the runtime of a source stage that knows where it is.</summary>
    /// <param name="open">
    /// The opener of one enumeration, invoked once per run at the run's first pull, under that run's
    /// tokens. It reads <paramref name="cursor"/> to learn where to open.
    /// </param>
    /// <param name="cursor">The cursor this source declares, built fresh for this node and this run.</param>
    /// <returns>The runtime.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="open"/> or <paramref name="cursor"/> is <see langword="null"/>.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The overload a durable run needs, and the only difference from the plain one: this source's position
    /// enters a checkpoint and a resume hands it back before the run's first element. Everything else — one
    /// enumeration per run, disposal on every terminal path, the two tokens — is unchanged, because a cursor
    /// is a thing a source <em>says</em> rather than a different way of running one.
    /// </para>
    /// <para>
    /// <b>The opener and the cursor are the provider's two halves of one object</b> and this seam does not
    /// join them for it: nothing here reads a position, so an adapter closes its opener over the very cursor
    /// instance it hands over and decides for itself whether a restored position is an index to skip, a
    /// token to subscribe at, or an offset to seek to.
    /// </para>
    /// <para>
    /// A source that declares one takes on a requirement the engine cannot check and the adapter must state:
    /// reopening at a stored position has to land on the elements after it. Where a provider cannot promise
    /// that, it declares no cursor and resumes from now — which is a row in its table rather than a silent
    /// approximation.
    /// </para>
    /// </remarks>
    public static DataflowStageRuntime Source(
        Func<DataflowRunTokens, IAsyncEnumerable<object?>> open,
        DataflowSourceCursor cursor)
    {
        ArgumentNullException.ThrowIfNull(open);
        ArgumentNullException.ThrowIfNull(cursor);

        return new DataflowStageRuntime(
            StageRuntime.Source(
                tokens => open(new DataflowRunTokens(tokens.RunIdentity, tokens.RunToken, tokens.StopToken)),
                cursor));
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

    /// <summary>Creates the runtime of a fan-out that delivers every element to every live leg.</summary>
    /// <returns>The runtime.</returns>
    /// <remarks>
    /// The junction asks every live leg for room before it pulls, so the slowest leg paces the stream and
    /// the junction holds one element outside the declared buffers. A leg whose downstream has completed is
    /// dropped, and the junction ends its own upstream when the last leg has left.
    /// </remarks>
    public static DataflowStageRuntime Broadcast() => new(StageRuntime.FanOut(LocalFanOut.Broadcast()));

    /// <summary>Creates the runtime of a fan-out that delivers each element to one leg with room.</summary>
    /// <returns>The runtime.</returns>
    /// <remarks>
    /// Which leg receives an element is not defined and is not meant to be: a balance rotates among the legs
    /// that have room, so a leg with none is routed around rather than blocking the others.
    /// </remarks>
    public static DataflowStageRuntime Balance() => new(StageRuntime.FanOut(LocalFanOut.Balance()));

    /// <summary>Creates the runtime of a fan-out that delivers each element to the leg a function names.</summary>
    /// <param name="route">
    /// The function answering the zero-based position of an element's leg, counting the legs the document
    /// wires in the specification's own port order.
    /// </param>
    /// <returns>The runtime.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="route"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// The positions are the order the specification declares its output ports in, which is ordinal by port
    /// name — a specification is canonical by construction, so the order is the same in every process that
    /// resolves it and a router written against it cannot drift. What is counted is the legs the document
    /// actually wires: a stage that declares an ignorable output port and a document that leaves it
    /// unwired shift every later position, exactly as they do for the local vocabulary's own partition.
    /// The authoring surface wires every declared port, so the two orders coincide for anything it built.
    /// </para>
    /// <para>
    /// An answer outside the wired legs fails the run, naming both the answer and the arity, because how
    /// many legs an occurrence has is stated by its edges and by nothing the function can see. The junction
    /// reads one element before it can ask, so it holds that element while it waits for the leg the answer
    /// named: head-of-line blocking one element deep is this junction's contract rather than a defect.
    /// </para>
    /// </remarks>
    public static DataflowStageRuntime Partition(Func<object?, int> route)
    {
        ArgumentNullException.ThrowIfNull(route);

        return new DataflowStageRuntime(StageRuntime.FanOut(LocalFanOut.Partition(route)));
    }

    /// <summary>Creates the runtime of a fan-out that delivers a row's parts to its legs.</summary>
    /// <param name="parts">
    /// One projection per output port, in the specification's own port order, each answering the part of the
    /// row that leg receives.
    /// </param>
    /// <returns>The runtime.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="parts"/>, or one of its elements, is <see langword="null"/>.
    /// </exception>
    /// <remarks>
    /// Broadcast with a projection per leg: every leg must have room before the row is pulled and every leg
    /// receives its own part of it, so the legs advance in lockstep and can be re-joined downstream without
    /// skew. The count is checked against the legs the document actually wires when the run is planned, so a
    /// stage that projects three parts into two wired legs is a planning failure naming both numbers.
    /// </remarks>
    public static DataflowStageRuntime Unzip(IReadOnlyList<Func<object?, object?>> parts)
    {
        ArgumentNullException.ThrowIfNull(parts);

        Func<object?, object?>[] copied = [.. parts];

        for (int index = 0; index < copied.Length; index++)
        {
            ArgumentNullException.ThrowIfNull(copied[index], nameof(parts));
        }

        return new DataflowStageRuntime(StageRuntime.FanOut(LocalFanOut.Unzip(copied)));
    }

    /// <summary>Creates the runtime of a fan-in that emits whichever input has an element.</summary>
    /// <returns>The runtime.</returns>
    /// <remarks>
    /// The rotation is over the inputs that have an element, so an input that is merely slower is never
    /// starved by one that is faster. Each input's own order is preserved, no order across them is claimed,
    /// and the junction completes only when every input has.
    /// </remarks>
    public static DataflowStageRuntime Merge() => new(StageRuntime.FanIn(LocalFanIn.Merge()));

    /// <summary>Creates the runtime of a fan-in that reads one input to its end before the next.</summary>
    /// <returns>The runtime.</returns>
    /// <remarks>
    /// The inputs are read in the specification's own port order. "Not consumed yet" is backpressure rather
    /// than laziness — a later input's producer is running and parks in that input's own bounded channel —
    /// which is the head-of-line cost a declared buffer relieves.
    /// </remarks>
    public static DataflowStageRuntime Concat() => new(StageRuntime.FanIn(LocalFanIn.Concat()));

    /// <summary>Creates the runtime of a fan-in that takes a fixed number of elements per input in turn.</summary>
    /// <param name="segmentSize">How many elements are taken from one input before the rotation moves on.</param>
    /// <returns>The runtime.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="segmentSize"/> is below one.</exception>
    /// <remarks>
    /// A merge with determinism bought at the price of head-of-line waiting: the input whose turn it is is
    /// waited for even when another has an element ready, so the emitted sequence is a function of the
    /// inputs and the segment size rather than of the scheduler.
    /// </remarks>
    public static DataflowStageRuntime Interleave(int segmentSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(segmentSize, 1);

        return new DataflowStageRuntime(StageRuntime.FanIn(LocalFanIn.Interleave(segmentSize)));
    }

    /// <summary>Creates the runtime of a fan-in that pairs its inputs' elements positionally.</summary>
    /// <param name="combine">
    /// The builder of one row from one element of every wired input, in the specification's own port order.
    /// </param>
    /// <returns>The runtime.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="combine"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// One row per element from each input: an input that has already given the pending row its column is
    /// not read again until that row is emitted. The junction completes as soon as any input it still needs
    /// has ended, and the partial row it was holding is discarded rather than kept for a row that cannot
    /// arrive. The array a combiner receives is fresh per row.
    /// </remarks>
    public static DataflowStageRuntime Zip(Func<object?[], object?> combine)
    {
        ArgumentNullException.ThrowIfNull(combine);

        return new DataflowStageRuntime(StageRuntime.FanIn(LocalFanIn.Zip(combine)));
    }

    /// <summary>Creates the runtime of a fan-in that emits every input's latest element on any arrival.</summary>
    /// <param name="combine">
    /// The builder of one row from the latest element of every wired input, in the specification's own port
    /// order.
    /// </param>
    /// <returns>The runtime.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="combine"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// Nothing is emitted until every input has produced at least once; every arrival after that emits one
    /// row; an input that completes freezes its last element into every later row; and the junction
    /// completes only when every input has. An input that completes without ever producing means no row can
    /// ever be built, and the run ends cleanly with no rows.
    /// </remarks>
    public static DataflowStageRuntime CombineLatest(Func<object?[], object?> combine)
    {
        ArgumentNullException.ThrowIfNull(combine);

        return new DataflowStageRuntime(StageRuntime.FanIn(LocalFanIn.CombineLatest(combine)));
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
/// What this run is called in this deployment, and therefore unique among the runs it has in flight — the
/// run grain's own key, <c>{graph}/{run}</c>, on a silo, and a fresh per-run identifier in an in-process
/// host. A source that has to be addressable from outside the run composes its address from this and its
/// binding's name, so that the same address is derivable by a caller holding the run's ticket. Every other
/// source ignores it.
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
