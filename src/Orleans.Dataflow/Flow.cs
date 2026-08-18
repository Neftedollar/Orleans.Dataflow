using System.Globalization;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow;

/// <summary>
/// A reusable typed transformation: what enters it, what leaves it, and nothing about where either comes
/// from.
/// </summary>
/// <typeparam name="TIn">The element type entering the flow.</typeparam>
/// <typeparam name="TOut">The element type leaving the flow.</typeparam>
/// <remarks>
/// <para>
/// A flow is an immutable value. Every operator returns a new flow and leaves the receiver exactly as it
/// was, so one flow composed into two graphs is the same value in both, and composing it a second time
/// cannot disturb the first graph.
/// </para>
/// <para>
/// A flow has no position. Identifiers for its lambda occurrences are allocated when a graph is closed, in
/// authoring order, so a flow of lambdas becomes different occurrences in every graph it appears in — and,
/// used twice in one graph, two disjoint sets of occurrences in that one. A flow that carries a registered
/// occurrence carries its name too, and a name is an identity rather than a position: such a flow composes
/// into any number of graphs, but twice into one graph is a collision reported at closure.
/// </para>
/// <para>
/// Operators are instance methods, per ADR 0004 section 2: an element-type mistake then reads as a
/// conversion error naming both types instead of the inference failure an extension method produces, and
/// the whole vocabulary stays in one completion list.
/// </para>
/// </remarks>
public sealed class Flow<TIn, TOut>
{
    /// <summary>Initializes a new instance of the <see cref="Flow{TIn, TOut}"/> class.</summary>
    /// <param name="stages">The occurrences this flow contributes, in authoring order.</param>
    internal Flow(IReadOnlyList<StageOccurrence> stages) => Stages = stages;

    /// <summary>Gets the occurrences this flow contributes to a graph, in authoring order.</summary>
    /// <value>
    /// An empty list for the identity flow <see cref="Flow.For{T}"/> returns, which contributes no
    /// occurrence to a graph because it does nothing to the elements.
    /// </value>
    internal IReadOnlyList<StageOccurrence> Stages { get; }

    /// <summary>Extends this flow with a mapping stage.</summary>
    /// <typeparam name="TNext">The element type the mapping produces.</typeparam>
    /// <param name="selector">The function applied to every element.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="selector"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The LINQ name is used because the LINQ semantics hold: one element in, one element out, in order.
    /// The delegate never enters the graph document, which is why a graph containing one declares
    /// <c>nondeployable</c>.
    /// </remarks>
    public Flow<TIn, TNext> Select<TNext>(Func<TOut, TNext> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        return new Flow<TIn, TNext>(LocalStageChain.Append(Stages, LocalStageDescriptor.Select(selector)));
    }

    /// <summary>Extends this flow with a filtering stage.</summary>
    /// <param name="predicate">The test every element must pass to continue.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
    /// <remarks>The LINQ name is used because the LINQ semantics hold: elements are dropped, never reordered.</remarks>
    public Flow<TIn, TOut> Where(Func<TOut, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        return new Flow<TIn, TOut>(LocalStageChain.Append(Stages, LocalStageDescriptor.Where(predicate)));
    }

    /// <summary>Extends this flow with a running fold that emits every intermediate state.</summary>
    /// <typeparam name="TState">The type of the state, which becomes the element type.</typeparam>
    /// <param name="seed">The initial state, which is not emitted.</param>
    /// <param name="folder">The function combining the running state with the next element.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="folder"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// One state out per element in, so a scan over three elements emits three states and an empty stream
    /// emits nothing at all. The seed is where the fold starts and not something that happened, which is
    /// why it is not emitted. The state is allocated per run, so a flow carrying a scan starts from the
    /// seed in every graph it is composed into and in every run of each of them.
    /// </remarks>
    public Flow<TIn, TState> Scan<TState>(TState seed, Func<TState, TOut, TState> folder)
    {
        ArgumentNullException.ThrowIfNull(folder);

        return new Flow<TIn, TState>(LocalStageChain.Append(Stages, LocalStageDescriptor.Scan(seed, folder)));
    }

    /// <summary>Extends this flow with a stage that passes a declared number of elements.</summary>
    /// <param name="count">How many elements to pass; zero or more.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
    /// <remarks>
    /// Reaching the bound completes the run the way the source running out does: everything upstream stops
    /// and is released, whatever it was holding is abandoned, and the run reports success. A flow carrying
    /// a take carries that completion into every graph it is composed into.
    /// </remarks>
    public Flow<TIn, TOut> Take(int count) =>
        new(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Take(LocalOptionGuard.Count(count, nameof(count)))));

    /// <summary>Extends this flow with a stage that drops a declared number of elements.</summary>
    /// <param name="count">How many elements to drop; zero or more.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
    /// <remarks>
    /// The dropped elements are still produced and still travel to this stage; skipping is not a way to
    /// avoid work upstream of it.
    /// </remarks>
    public Flow<TIn, TOut> Skip(int count) =>
        new(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Skip(LocalOptionGuard.Count(count, nameof(count)))));

    /// <summary>Extends this flow with a stage that passes elements while a predicate holds.</summary>
    /// <param name="predicate">The test each element must pass for the stream to continue.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The boundary is exclusive, per the naming rules of ADR 0004 section 7: the first element the
    /// predicate rejects is not emitted, and the run completes as if the source had ended there.
    /// <see cref="TakeThrough"/> is the inclusive spelling and is a different word rather than a flag.
    /// </remarks>
    public Flow<TIn, TOut> TakeWhile(Func<TOut, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        return new Flow<TIn, TOut>(LocalStageChain.Append(Stages, LocalStageDescriptor.TakeWhile(predicate)));
    }

    /// <summary>Extends this flow with a stage that passes elements up to and including one the predicate accepts.</summary>
    /// <param name="predicate">The test that decides which element is the last one.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The inclusive counterpart of <see cref="TakeWhile"/>: the element the predicate accepts is emitted
    /// and the run completes after it, which is how a stream ends at a terminator it has to deliver.
    /// </remarks>
    public Flow<TIn, TOut> TakeThrough(Func<TOut, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        return new Flow<TIn, TOut>(LocalStageChain.Append(Stages, LocalStageDescriptor.TakeThrough(predicate)));
    }

    /// <summary>Extends this flow with a stage that drops elements while a predicate holds.</summary>
    /// <param name="predicate">The test that decides which elements to drop.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// Exclusive in the same sense <see cref="TakeWhile"/> is: the first element the predicate rejects is
    /// emitted, and so is everything after it, whether or not the predicate would accept it again.
    /// </remarks>
    public Flow<TIn, TOut> SkipWhile(Func<TOut, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        return new Flow<TIn, TOut>(LocalStageChain.Append(Stages, LocalStageDescriptor.SkipWhile(predicate)));
    }

    /// <summary>Extends this flow with a stage that passes the first occurrence of every element.</summary>
    /// <param name="options">The greatest number of distinct elements the stage may remember.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="DistinctOptions.MaxTrackedKeys"/> is below one.
    /// </exception>
    /// <remarks>
    /// Elements are compared with <see cref="EqualityComparer{T}.Default"/>. The bound is required and is
    /// not a hint: an element that would be the one key past it faults the run with a
    /// <see cref="TrackedKeyOverflowException"/> rather than evicting an older key. The remembered keys are
    /// per run, so a flow carrying a distinct deduplicates within a run and never across two.
    /// </remarks>
    public Flow<TIn, TOut> Distinct(DistinctOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new Flow<TIn, TOut>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Distinct(
                LocalOptionGuard.Distinct(options, nameof(options)),
                EqualityComparer<TOut>.Default)));
    }

    /// <summary>Extends this flow with a bounded buffer.</summary>
    /// <param name="options">The capacity and the overflow policy.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="BufferOptions.Capacity"/> is below one, or
    /// <see cref="BufferOptions.OverflowPolicy"/> is not a declared member of its enumeration.
    /// </exception>
    /// <remarks>
    /// A buffer is where a graph stops being one loop: everything upstream of it runs as one fused segment
    /// and everything downstream as another, with this one bounded queue between them. A flow carrying a
    /// buffer carries it into every graph it is composed into, and into each of them separately.
    /// </remarks>
    public Flow<TIn, TOut> Buffer(BufferOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new Flow<TIn, TOut>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Buffer(LocalOptionGuard.Buffer(options, nameof(options)))));
    }

    /// <summary>Extends this flow with a stage that holds every element for a declared duration.</summary>
    /// <param name="delay">How long each element is held before it is emitted.</param>
    /// <param name="holdback">How many elements may be held at once, and what happens to the next one.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="holdback"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="delay"/> is not positive, <see cref="BufferOptions.Capacity"/> is below one, or
    /// <see cref="BufferOptions.OverflowPolicy"/> is not a declared member of its enumeration.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The stream is shifted rather than paced: every element starts its own wait when the stage takes it,
    /// the results are emitted in the order the elements arrived, and a burst that fits the declared
    /// holdback comes out with its gaps intact, later by the delay. The holdback is required and is the
    /// bound on that: <see cref="BufferOptions.Capacity"/> elements may be waiting out their delay at once,
    /// with one more in the handoff in front of them as there is in front of every asynchronous stage, and
    /// an element arriving when both are occupied is answered by
    /// <see cref="BufferOptions.OverflowPolicy"/> — the upstream waits, an element is dropped, or the run
    /// fails, exactly as it would at a buffer. A <c>Buffer</c> written immediately before the delay is that
    /// handoff rather than a second queue, so an author who wants a deeper one says so there.
    /// </para>
    /// <para>
    /// The clock is the host's, resolved when the graph is materialized. A cancellation abandons the
    /// elements being held; a graceful shutdown drains them, waiting out the delays already started as it
    /// waits out an asynchronous callback in flight, and a pause waits for them for the same reason —
    /// which is bounded by the delay itself.
    /// </para>
    /// </remarks>
    public Flow<TIn, TOut> Delay(TimeSpan delay, BufferOptions holdback)
    {
        ArgumentNullException.ThrowIfNull(holdback);

        return new Flow<TIn, TOut>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Delay(
            LocalOptionGuard.Duration(delay, nameof(delay)),
            LocalOptionGuard.Buffer(holdback, nameof(holdback)))));
    }

    /// <summary>Extends this flow with a stage that holds the first element until a duration has passed.</summary>
    /// <param name="delay">How long after the run starts the first element may be emitted.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="delay"/> is not positive.</exception>
    /// <remarks>
    /// The delay is on the stream and not on its elements: the first element is held until
    /// <paramref name="delay"/> has passed since the run started, and everything after it passes untouched.
    /// A stream whose first element arrives later than that is not delayed at all, because the wait is for
    /// the moment rather than for the duration. A cancellation abandons the element being held and a
    /// graceful shutdown releases it, which is where this differs from <see cref="Delay"/>: an element in
    /// the segment's own hand is delivered by a stop, and one in an asynchronous window is waited out.
    /// </remarks>
    public Flow<TIn, TOut> InitialDelay(TimeSpan delay) =>
        new Flow<TIn, TOut>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Timed(
            LocalStageKind.InitialDelay,
            LocalOptionGuard.Duration(delay, nameof(delay)))));

    /// <summary>Extends this flow with a stage that fails the run when the stream goes quiet.</summary>
    /// <param name="gap">The greatest silence allowed between two elements, and before the first.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="gap"/> is not positive.</exception>
    /// <remarks>
    /// <para>
    /// The run faults with a <see cref="StreamTimeoutException"/> when <paramref name="gap"/> passes with
    /// no element reaching this stage — counted from the previous element, and for the first element from
    /// the moment the run started, so a stream that never produces anything at all fails rather than
    /// hanging. Nothing is dropped and nothing is retried: a timeout is a statement that the stream broke
    /// its own promise.
    /// </para>
    /// <para>
    /// What is measured is arrivals at this stage and never the time an element spends below it. The clock
    /// is the host's and keeps running while the run is paused, so a run held for longer than the gap
    /// fails: a pause holds the elements, not the clock.
    /// </para>
    /// </remarks>
    public Flow<TIn, TOut> Timeout(TimeSpan gap) =>
        new Flow<TIn, TOut>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Timed(
            LocalStageKind.Timeout,
            LocalOptionGuard.Duration(gap, nameof(gap)))));

    /// <summary>Extends this flow with a stage that ends the stream when a duration has passed.</summary>
    /// <param name="window">How long after the run starts the stream ends.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="window"/> is not positive.</exception>
    /// <remarks>
    /// The window is wall-clock and not a count: everything emitted before it closes is kept, the element
    /// that arrives at or after it is not emitted, and the stream ends there the way reaching a
    /// <c>Take</c> bound ends it — upstream stops and is released, everything already downstream drains,
    /// and the run reports success. A stream that has gone quiet still ends at the deadline rather than
    /// waiting for an element to notice it with, which is the case this operator exists for.
    /// </remarks>
    public Flow<TIn, TOut> TakeWithin(TimeSpan window) =>
        new Flow<TIn, TOut>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Timed(
            LocalStageKind.TakeWithin,
            LocalOptionGuard.Duration(window, nameof(window)))));

    /// <summary>Extends this flow with a stage that drops every element until a duration has passed.</summary>
    /// <param name="window">How long after the run starts elements begin to pass.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="window"/> is not positive.</exception>
    /// <remarks>
    /// The mirror of <see cref="TakeWithin"/> and the wall-clock <c>Skip</c>: an element arriving inside
    /// the window is dropped rather than held, and everything from the first element after it passes. The
    /// stage never waits — it has an answer for every element the moment it arrives — so a stream that
    /// produces nothing during the window costs nothing at all.
    /// </remarks>
    public Flow<TIn, TOut> SkipWithin(TimeSpan window) =>
        new Flow<TIn, TOut>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Timed(
            LocalStageKind.SkipWithin,
            LocalOptionGuard.Duration(window, nameof(window)))));

    /// <summary>Extends this flow with a stage that holds the stream to a declared rate.</summary>
    /// <param name="options">The rate, the burst, and what to do with an element there is no budget for.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="ThrottleOptions.Elements"/> is below one, <see cref="ThrottleOptions.Per"/> is not
    /// positive, <see cref="ThrottleOptions.MaximumBurst"/> is below
    /// <see cref="ThrottleOptions.Elements"/>, or <see cref="ThrottleOptions.Mode"/> is not a declared
    /// member of its enumeration.
    /// </exception>
    /// <remarks>
    /// Every element costs one unit. The budget is a token bucket that starts full, holds
    /// <see cref="ThrottleOptions.MaximumBurst"/> units, and refills continuously at
    /// <see cref="ThrottleOptions.Elements"/> per <see cref="ThrottleOptions.Per"/>, so a stream at or
    /// below the declared rate passes untouched and a faster one is either paced or refused by
    /// <see cref="ThrottleOptions.Mode"/>. Nothing is ever dropped here: a shaping throttle waits on the
    /// segment's own thread, which backpressures upstream, and an enforcing one fails the run with a
    /// <see cref="RateLimitExceededException"/>.
    /// </remarks>
    public Flow<TIn, TOut> Throttle(ThrottleOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new Flow<TIn, TOut>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Throttle(
            LocalOptionGuard.Throttle(options, nameof(options)),
            cost: null)));
    }


    /// <summary>Extends this flow with a gate an author opens and closes while the run is running.</summary>
    /// <param name="controlName">The author-stable name to expose the valve under.</param>
    /// <param name="initialMode">The state the valve starts each run in.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="controlName"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="controlName"/> is not a valid result slot identifier.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="initialMode"/> is not a declared member of its enumeration.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The control is an <see cref="IValve"/> resolved by name from the run handle, and it exists as soon as
    /// the run does — a control is a thing an author uses while the run is running, which is what separates
    /// it from a result. Closing it holds the element the stage has in its hand and backpressures everything
    /// above it, exactly as a full buffer does; nothing is dropped and nothing is buffered here, because a
    /// valve has no capacity of its own. Elements accumulate in whatever boundaries the author declared
    /// above it, under the policies declared there.
    /// </para>
    /// <para>
    /// The state the valve starts in is written into the document, because a graph whose valve starts closed
    /// produces nothing until something opens it; what an author does to it afterwards is a run's own
    /// business and is never durable topology. A closed valve is one of this runtime's own waits: a paused
    /// run comes to rest inside it, a shutdown releases it and the element is delivered, and a cancellation
    /// abandons the run.
    /// </para>
    /// </remarks>
    public Flow<TIn, TOut> Valve(string controlName, ValveMode initialMode = ValveMode.Open) =>
        new Flow<TIn, TOut>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Valve(
            LocalOptionGuard.Valve(initialMode, nameof(initialMode)),
            LocalOptionGuard.SlotName(controlName, nameof(controlName)))));

    /// <summary>Extends this flow with a stage that holds the stream to a declared rate by cost.</summary>
    /// <param name="options">The rate, the burst, and what to do with an element there is no budget for.</param>
    /// <param name="cost">What one element costs the rate; zero or more.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> or <paramref name="cost"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="ThrottleOptions.Elements"/> is below one, <see cref="ThrottleOptions.Per"/> is not
    /// positive, <see cref="ThrottleOptions.MaximumBurst"/> is below
    /// <see cref="ThrottleOptions.Elements"/>, or <see cref="ThrottleOptions.Mode"/> is not a declared
    /// member of its enumeration.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The same bucket, charged by what the element is worth rather than by one per element: a rate of a
    /// thousand per second with a cost function answering a batch's size admits a thousand rows per second
    /// however many batches carry them. The function runs once per element, on the segment's own thread,
    /// before the budget is examined.
    /// </para>
    /// <para>
    /// An element whose cost exceeds <see cref="ThrottleOptions.MaximumBurst"/> fails the run in both
    /// modes, because no amount of waiting could ever admit it; a negative cost fails the run too, because
    /// an element cannot give a stream budget back.
    /// </para>
    /// </remarks>
    public Flow<TIn, TOut> Throttle(ThrottleOptions options, Func<TOut, int> cost)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(cost);

        return new Flow<TIn, TOut>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Throttle(
            LocalOptionGuard.Throttle(options, nameof(options)),
            cost)));
    }

    /// <summary>Extends this flow with an asynchronous mapping stage that preserves input order.</summary>
    /// <typeparam name="TNext">The element type the mapping produces.</typeparam>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="selector">The callback applied to every element.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> or <paramref name="selector"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="ParallelismOptions.MaxConcurrency"/> is below one.
    /// </exception>
    /// <remarks>
    /// Up to <see cref="ParallelismOptions.MaxConcurrency"/> callbacks run at once and their results are
    /// emitted in the order their elements arrived, so a slow callback holds up emission but not admission.
    /// The callback receives the run's own cancellation token, which is cancelled both when the run is
    /// cancelled and when anything in the run fails.
    /// </remarks>
    public Flow<TIn, TNext> SelectAsync<TNext>(
        ParallelismOptions options,
        Func<TOut, CancellationToken, Task<TNext>> selector)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(selector);

        return new Flow<TIn, TNext>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.SelectAsync(LocalOptionGuard.Parallelism(options, nameof(options)), selector)));
    }

    /// <summary>Extends this flow with an asynchronous mapping stage that emits in completion order.</summary>
    /// <typeparam name="TNext">The element type the mapping produces.</typeparam>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="selector">The callback applied to every element.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> or <paramref name="selector"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="ParallelismOptions.MaxConcurrency"/> is below one.
    /// </exception>
    /// <remarks>
    /// The same bounds as <see cref="SelectAsync"/> with one difference stated in the name: a result is
    /// emitted as soon as its callback finishes, so the output order is the order the callbacks completed
    /// in and not the order the elements arrived in.
    /// </remarks>
    public Flow<TIn, TNext> SelectAsyncUnordered<TNext>(
        ParallelismOptions options,
        Func<TOut, CancellationToken, Task<TNext>> selector)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(selector);

        return new Flow<TIn, TNext>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.SelectAsyncUnordered(
                LocalOptionGuard.Parallelism(options, nameof(options)),
                selector)));
    }

    /// <summary>
    /// Extends this flow with an asynchronous mapping stage over value tasks that preserves input order.
    /// </summary>
    /// <typeparam name="TNext">The element type the mapping produces.</typeparam>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="selector">The callback applied to every element.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> or <paramref name="selector"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="ParallelismOptions.MaxConcurrency"/> is below one.
    /// </exception>
    /// <remarks>
    /// The value-task family of <see cref="Source{T}.SelectValueTaskAsync{TOut}"/>, whose contract this
    /// shares exactly: the same bounds and ordering as <see cref="SelectAsync{TNext}"/>, and the rule that
    /// the runtime awaits each returned value task exactly once and never after reading its result.
    /// </remarks>
    public Flow<TIn, TNext> SelectValueTaskAsync<TNext>(
        ParallelismOptions options,
        Func<TOut, CancellationToken, ValueTask<TNext>> selector)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(selector);

        return new Flow<TIn, TNext>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.SelectValueTaskAsync(
                LocalOptionGuard.Parallelism(options, nameof(options)),
                selector)));
    }

    /// <summary>
    /// Extends this flow with an asynchronous mapping stage over value tasks that emits in completion
    /// order.
    /// </summary>
    /// <typeparam name="TNext">The element type the mapping produces.</typeparam>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="selector">The callback applied to every element.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> or <paramref name="selector"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="ParallelismOptions.MaxConcurrency"/> is below one.
    /// </exception>
    /// <remarks>
    /// The unordered spelling of <see cref="SelectValueTaskAsync{TNext}"/>: a result is emitted as soon as
    /// its callback finishes, and the single-consumption rule applies unchanged.
    /// </remarks>
    public Flow<TIn, TNext> SelectValueTaskAsyncUnordered<TNext>(
        ParallelismOptions options,
        Func<TOut, CancellationToken, ValueTask<TNext>> selector)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(selector);

        return new Flow<TIn, TNext>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.SelectValueTaskAsyncUnordered(
                LocalOptionGuard.Parallelism(options, nameof(options)),
                selector)));
    }

    /// <summary>Extends this flow with another flow.</summary>
    /// <typeparam name="TNext">The element type the downstream flow produces.</typeparam>
    /// <param name="flow">The downstream flow, which is not modified.</param>
    /// <returns>A new flow; neither argument is changed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="flow"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// Composition of two reusable values into a third. The occurrences of <paramref name="flow"/> are
    /// copied into the result, so the result and the argument share no state at all.
    /// </remarks>
    public Flow<TIn, TNext> Via<TNext>(Flow<TOut, TNext> flow)
    {
        ArgumentNullException.ThrowIfNull(flow);

        return new Flow<TIn, TNext>(LocalStageChain.Concat(Stages, flow.Stages));
    }

    /// <summary>Extends this flow with one named occurrence of a registered stage.</summary>
    /// <typeparam name="TNext">The element type the registered stage produces.</typeparam>
    /// <param name="flow">The typed handle of the registered stage.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="flow"/> or <paramref name="occurrenceName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// A flow holding a named occurrence is reusable in the same sense every flow is, with one consequence
    /// worth stating: composing it twice into one graph contributes the name twice, and two occurrences of
    /// one graph may not share a name. An explicit name is an identity rather than a position, so the
    /// second use is a collision reported at closure rather than a second numbering.
    /// </remarks>
    public Flow<TIn, TNext> Via<TNext>(
        RegisteredFlow<TOut, TNext> flow,
        string occurrenceName,
        CanonicalJsonValue parameters)
    {
        ArgumentNullException.ThrowIfNull(flow);

        return new Flow<TIn, TNext>(LocalStageChain.Append(
            Stages,
            RegisteredAttachment.Occurrence(flow.Specification, occurrenceName, parameters)));
    }

    /// <summary>Ends this flow in a sink that declares no result, making a branch of it.</summary>
    /// <param name="sink">The sink consuming what the flow produces.</param>
    /// <returns>The branch, ready to be handed to a junction call.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sink"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The <c>To</c> family here mirrors <see cref="Source{T}"/>'s and differs in exactly one respect: it
    /// produces a <see cref="Branch{TIn}"/> instead of a closed graph, because a leg of a junction is not a
    /// graph until the junction call takes it. Everything else — the mandatory slot name, the sink-factory
    /// lambdas that make inference total, the registered overloads, and the guards against a dropped result
    /// — is the same surface for the same reasons (ADR 0004 sections 2 and 3).
    /// </remarks>
    public Branch<TIn> To(Sink<TOut> sink)
    {
        ArgumentNullException.ThrowIfNull(sink);

        return new Branch<TIn>(LocalStageChain.Concat(Stages, sink.Stages));
    }

    /// <summary>Ends this flow in a resultless sink built from the element type's own vocabulary.</summary>
    /// <param name="sink">A function choosing a sink from the factory for the element type this flow produces.</param>
    /// <returns>The branch, ready to be handed to a junction call.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sink"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="sink"/> returned <see langword="null"/>.</exception>
    /// <remarks>
    /// The factory form of the one-argument close, so that <c>Flow.For&lt;Order&gt;().To(s =&gt; s.Ignore())</c>
    /// reads the same way as the result-bearing factory overloads do.
    /// </remarks>
    public Branch<TIn> To(Func<SinkFactory<TOut>, Sink<TOut>> sink)
    {
        ArgumentNullException.ThrowIfNull(sink);

        Sink<TOut> resolved = sink(SinkFactory<TOut>.Instance) ??
            throw new ArgumentException(
                $"The sink factory returned null, and a branch ends in a sink. Return a sink from the {nameof(SinkFactory<TOut>)} the lambda receives, such as 's => s.Ignore()'.",
                nameof(sink));

        return To(resolved);
    }

    /// <summary>Ends this flow in a result-bearing sink, handing back the slot as an output.</summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The sink consuming what the flow produces.</param>
    /// <param name="slotName">The author-stable name to expose the result under.</param>
    /// <param name="slot">When this method returns, the slot that resolves the result.</param>
    /// <returns>The branch, ready to be handed to a junction call.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="sink"/> or <paramref name="slotName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="slotName"/> is not a valid <see cref="ResultSlotId"/>.
    /// </exception>
    /// <remarks>
    /// <para>
    /// There is one form rather than the two <see cref="Source{T}"/> offers, and the missing one is the
    /// tuple. A branch is written as an argument of the junction call that consumes it — that is the whole
    /// reason an <see langword="out"/> parameter is legal here, per ADR 0006 — and a tuple in that position
    /// would have to be unpacked into a statement first, which is the shape the fluent form exists to avoid.
    /// </para>
    /// <para>
    /// The slot names its graph from the junction call onwards, because that is the first moment a graph
    /// exists. A branch that declares a result therefore closes exactly one graph; handing it to a second
    /// junction call is refused rather than quietly repointing the first graph's slot.
    /// </para>
    /// </remarks>
    public Branch<TIn> To<TResult>(
        SinkWithResult<TOut, TResult> sink,
        string slotName,
        out ResultSlot<TResult> slot)
    {
        ArgumentNullException.ThrowIfNull(sink);

        return Terminate(sink.Stages, LocalOptionGuard.SlotName(slotName, nameof(slotName)), out slot);
    }

    /// <summary>
    /// Ends this flow in a result-bearing sink built from the element type's own vocabulary, handing back
    /// the slot as an output.
    /// </summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">A function choosing a sink from the factory for the element type this flow produces.</param>
    /// <param name="slotName">The author-stable name to expose the result under.</param>
    /// <param name="slot">When this method returns, the slot that resolves the result.</param>
    /// <returns>The branch, ready to be handed to a junction call.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="sink"/> or <paramref name="slotName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="sink"/> returned <see langword="null"/>, or <paramref name="slotName"/> is not a
    /// valid <see cref="ResultSlotId"/>.
    /// </exception>
    /// <remarks>
    /// This is what makes a branch infer completely: the element type is pinned by
    /// <see cref="Flow.For{T}"/> at the head of the branch and the result type flows out of the lambda, so
    /// <c>Flow.For&lt;Order&gt;().To(s =&gt; s.Count(), "counted", out ResultSlot&lt;long&gt; counted)</c> needs no
    /// type argument and no lambda annotation. <paramref name="slotName"/> is validated before the lambda is
    /// invoked, so a rejected name never costs the author a side effect.
    /// </remarks>
    public Branch<TIn> To<TResult>(
        Func<SinkFactory<TOut>, SinkWithResult<TOut, TResult>> sink,
        string slotName,
        out ResultSlot<TResult> slot)
    {
        ArgumentNullException.ThrowIfNull(sink);

        ResultSlotId slotId = LocalOptionGuard.SlotName(slotName, nameof(slotName));

        SinkWithResult<TOut, TResult> resolved = sink(SinkFactory<TOut>.Instance) ??
            throw new ArgumentException(
                $"The sink factory returned null, and a branch ends in a sink. Return a sink from the {nameof(SinkFactory<TOut>)} the lambda receives, such as 's => s.Aggregate(seed, folder)'.",
                nameof(sink));

        return Terminate(resolved.Stages, slotId, out slot);
    }

    /// <summary>Ends this flow in one named occurrence of a registered stage that declares no result.</summary>
    /// <param name="sink">The typed handle of the registered stage terminating the branch.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <returns>The branch, ready to be handed to a junction call.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="sink"/> or <paramref name="occurrenceName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// A registered stage that does declare a result port is a
    /// <see cref="RegisteredSinkWithResult{TIn, TResult}"/> and does not convert to a
    /// <see cref="RegisteredSink{TIn}"/> at all, so this overload cannot drop a result: the mistake is a
    /// conversion error naming both types rather than a branch that silently produces nothing readable.
    /// </remarks>
    public Branch<TIn> To(RegisteredSink<TOut> sink, string occurrenceName, CanonicalJsonValue parameters)
    {
        ArgumentNullException.ThrowIfNull(sink);

        return new Branch<TIn>(LocalStageChain.Append(
            Stages,
            RegisteredAttachment.Occurrence(sink.Specification, occurrenceName, parameters)));
    }

    /// <summary>
    /// Ends this flow in one named occurrence of a registered result-bearing stage, handing back the slot as
    /// an output.
    /// </summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The typed handle of the registered stage terminating the branch.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <param name="slotName">The author-stable name to expose the result under.</param>
    /// <param name="slot">When this method returns, the slot that resolves the result.</param>
    /// <returns>The branch, ready to be handed to a junction call.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="sink"/>, <paramref name="occurrenceName"/>, or <paramref name="slotName"/> is
    /// <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier,
    /// <paramref name="slotName"/> is not a valid <see cref="ResultSlotId"/>, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// The two names mean different things and neither is derivable from the other: the occurrence name is
    /// the node's durable identity in the graph, and the slot name is what a run handle resolves the result
    /// under. A branch built entirely from registered stages still ends in a graph that declares
    /// <c>nondeployable</c> whenever a junction joins it, because the junction itself is a local stage.
    /// </remarks>
    public Branch<TIn> To<TResult>(
        RegisteredSinkWithResult<TOut, TResult> sink,
        string occurrenceName,
        CanonicalJsonValue parameters,
        string slotName,
        out ResultSlot<TResult> slot)
    {
        ArgumentNullException.ThrowIfNull(sink);

        ResultSlotId slotId = LocalOptionGuard.SlotName(slotName, nameof(slotName));

        return Terminate(
            LocalStageChain.Of(RegisteredAttachment.Occurrence(sink.Specification, occurrenceName, parameters)),
            slotId,
            out slot);
    }

    /// <summary>Ends this flow in a result-bearing sink and no name for the result. Never valid.</summary>
    /// <typeparam name="TResult">The type of the result the sink declares.</typeparam>
    /// <param name="sink">The sink that would terminate the branch.</param>
    /// <returns>Nothing; the call cannot compile, and cannot be reached if it somehow does.</returns>
    /// <exception cref="NotSupportedException">Always.</exception>
    /// <remarks>
    /// The branch half of the guard ADR 0004 section 3 introduced for chains. Without it,
    /// <c>To(countingSink)</c> is a wrong-type call whose one compiler-suggested repair is a cast to
    /// <see cref="Sink{TOut}"/> — which compiles, and silently drops the result the author asked for. A
    /// branch that carries a result therefore has a real overload to bind to, and binding to it says what to
    /// write instead.
    /// </remarks>
    [Obsolete(
        "A result-bearing sink needs a name for its result: write To(sink, \"name\", out var slot). To run the sink on this branch and deliberately discard its result, write To(sink.ToSink()).",
        error: true)]
    public Branch<TIn> To<TResult>(SinkWithResult<TOut, TResult> sink) =>
        throw new NotSupportedException(GuardOverload());

    /// <summary>
    /// Ends this flow in a sink-factory lambda that chooses a result-bearing sink, and no name for the
    /// result. Never valid.
    /// </summary>
    /// <typeparam name="TResult">The type of the result the chosen sink declares.</typeparam>
    /// <param name="sink">The function that would choose the sink.</param>
    /// <returns>Nothing; the call cannot compile, and cannot be reached if it somehow does.</returns>
    /// <exception cref="NotSupportedException">Always.</exception>
    /// <remarks>
    /// The factory-lambda half of the same guard, and the one an author is likelier to hit, because the
    /// factory form is the one a branch is normally written in.
    /// </remarks>
    [Obsolete(
        "A result-bearing sink needs a name for its result: write To(s => s.Count(), \"name\", out var slot). To run the sink on this branch and deliberately discard its result, write To(s => s.Count().ToSink()).",
        error: true)]
    public Branch<TIn> To<TResult>(Func<SinkFactory<TOut>, SinkWithResult<TOut, TResult>> sink) =>
        throw new NotSupportedException(GuardOverload());

    /// <summary>Returns a one-line diagnostic summary of this flow.</summary>
    /// <returns>Text of the form <c>flow (2 stages)</c>, singular for one (<c>flow (1 stage)</c>).</returns>
    /// <remarks>The count is formatted with the invariant culture, and the method never throws.</remarks>
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"flow ({Stages.Count} {(Stages.Count == 1 ? "stage" : "stages")})");

    /// <summary>Builds the message a guard overload throws if it is ever reached.</summary>
    /// <returns>The message.</returns>
    /// <remarks>
    /// The branch counterpart of <see cref="Source{T}"/>'s, and there is exactly one thing to say: this
    /// member exists to fail at compile time and has no runtime behavior to fall back on.
    /// </remarks>
    private static string GuardOverload() =>
        $"This {nameof(To)} overload exists only as a compile-time guard against ending a branch with a result-bearing sink and no name for its result. It is marked as an error and is never a legal call; nothing in this library invokes it.";

    /// <summary>Ends this flow in a result-bearing terminal under a validated name.</summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="terminal">The terminal's occurrences.</param>
    /// <param name="slotId">The validated slot name.</param>
    /// <param name="slot">When this method returns, the slot that resolves the result.</param>
    /// <returns>The branch.</returns>
    /// <remarks>
    /// Every result-bearing branch funnels through here, which is what makes the lambda-implemented and the
    /// registered forms produce the same kind of slot: one that names its graph from the junction call
    /// onwards and refuses to name a second.
    /// </remarks>
    private Branch<TIn> Terminate<TResult>(
        IReadOnlyList<StageOccurrence> terminal,
        ResultSlotId slotId,
        out ResultSlot<TResult> slot)
    {
        BranchSlotBinding binding = new();

        slot = ResultSlot<TResult>.OnBranch(slotId, binding);

        return new Branch<TIn>(LocalStageChain.Concat(Stages, terminal), slotId, binding);
    }
}

/// <summary>
/// The factory that starts a flow.
/// </summary>
/// <remarks>
/// <see cref="For{T}"/> and a hypothetical <c>Create&lt;T&gt;</c> are inference-identical, because the type
/// argument appears only in return position and has to be written either way. ADR 0004 section 1 chose the
/// name that reads next to <c>Source.From</c>.
/// </remarks>
public static class Flow
{
    /// <summary>Starts a flow that passes its elements through unchanged.</summary>
    /// <typeparam name="T">The element type entering the flow.</typeparam>
    /// <returns>The identity flow, ready to be extended with operators.</returns>
    /// <remarks>
    /// The identity flow contributes no stage occurrence to a graph, so composing it into a graph is
    /// invisible in the resulting document. That is the honest encoding: doing nothing to every element is
    /// not work a graph should describe.
    /// </remarks>
    public static Flow<T, T> For<T>() => new(LocalStageChain.Empty);
}
