using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading.Channels;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow;

/// <summary>
/// A reusable description of where elements enter a graph, together with everything done to them so far.
/// </summary>
/// <typeparam name="T">The element type currently flowing.</typeparam>
/// <remarks>
/// <para>
/// A source is an immutable value and starts nothing. Every operator returns a new source and leaves the
/// receiver unchanged, so the same source can be the head of any number of graphs, and closing one of them
/// cannot disturb another.
/// </para>
/// <para>
/// <c>To</c> is where a graph stops being a description and becomes a document: node identifiers are
/// allocated in authoring order, the fragment algebra composes and closes the shape, and the closed
/// document is fingerprinted. Nothing before that point has a position or an identity.
/// </para>
/// <para>
/// Every lambda occurrence is automatically named, so a graph built only from lambdas declares
/// <c>ephemeral-identity</c> as well as <c>nondeployable</c>, and is therefore rejected for durable
/// pipelines by design (ADR 0004 section 6). A lambda occurrence has no spelling for a name at all: a name
/// on a delegate would promise an edit-stable identity the delegate behind it cannot honor. The registered
/// overloads take one and require it, so what a closed document declares is a fact about what the chain
/// actually holds.
/// </para>
/// </remarks>
public sealed class Source<T>
{
    /// <summary>Initializes a new instance of the <see cref="Source{T}"/> class.</summary>
    /// <param name="stages">The occurrences this source contributes, in authoring order.</param>
    /// <remarks>
    /// The chain-shaped spelling, for a source that is a straight line of occurrences and nothing else,
    /// which is every source a factory returns.
    /// </remarks>
    internal Source(IReadOnlyList<StageOccurrence> stages)
        : this(LocalGraphShape.OfChain(stages))
    {
    }

    /// <summary>Initializes a new instance of the <see cref="Source{T}"/> class.</summary>
    /// <param name="shape">The partial graph this source carries, with exactly one open output.</param>
    internal Source(LocalGraphShape shape) => Shape = shape;

    /// <summary>Gets the partial graph this source carries.</summary>
    /// <value>
    /// The occurrences, the wiring between them, and the one output port everything downstream attaches to.
    /// A source built from factories and operators is a chain; a source built by a fan-in combinator, or by
    /// a tap, is a shape with a junction in it, and both are one open output away from being closed.
    /// </value>
    internal LocalGraphShape Shape { get; }

    /// <summary>Gets the occurrences this source contributes to a graph, in authoring order.</summary>
    internal IReadOnlyList<StageOccurrence> Stages => Shape.Stages;

    /// <summary>Extends this source with a mapping stage.</summary>
    /// <typeparam name="TOut">The element type the mapping produces.</typeparam>
    /// <param name="selector">The function applied to every element.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="selector"/> is <see langword="null"/>.</exception>
    public Source<TOut> Select<TOut>(Func<T, TOut> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        return new Source<TOut>(Shape.Append(LocalStageDescriptor.Select(selector)));
    }

    /// <summary>Extends this source with a filtering stage.</summary>
    /// <param name="predicate">The test every element must pass to continue.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
    public Source<T> Where(Func<T, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        return new Source<T>(Shape.Append(LocalStageDescriptor.Where(predicate)));
    }

    /// <summary>Extends this source with a running fold that emits every intermediate state.</summary>
    /// <typeparam name="TState">The type of the state, which becomes the element type.</typeparam>
    /// <param name="seed">The initial state, which is not emitted.</param>
    /// <param name="folder">The function combining the running state with the next element.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="folder"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// One state out per element in, so a scan over three elements emits three states and an empty stream
    /// emits nothing at all. The seed is where the fold starts and not something that happened, which is
    /// why it is not emitted; an author who wants it emitted writes it into the stream. The state is
    /// allocated per run, like an aggregate's, so two runs of one graph never continue each other.
    /// </remarks>
    public Source<TState> Scan<TState>(TState seed, Func<TState, T, TState> folder)
    {
        ArgumentNullException.ThrowIfNull(folder);

        return new Source<TState>(Shape.Append(LocalStageDescriptor.Scan(seed, folder)));
    }

    /// <summary>Extends this source with a stage that passes a declared number of elements.</summary>
    /// <param name="count">How many elements to pass; zero or more.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
    /// <remarks>
    /// Reaching the bound completes the run the way the source running out does: everything upstream stops
    /// and is released, whatever it was holding is abandoned, and the run reports success with the results
    /// it has. <c>Take(0)</c> therefore completes a run that never touches its source at all, and a
    /// <c>Take</c> of more elements than arrive is simply never reached.
    /// </remarks>
    public Source<T> Take(int count) =>
        new(Shape.Append(LocalStageDescriptor.Take(LocalOptionGuard.Count(count, nameof(count)))));

    /// <summary>Extends this source with a stage that drops a declared number of elements.</summary>
    /// <param name="count">How many elements to drop; zero or more.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
    /// <remarks>
    /// The dropped elements are still produced and still travel to this stage; skipping is not a way to
    /// avoid work upstream of it. A skip of more elements than arrive passes nothing and completes with the
    /// source.
    /// </remarks>
    public Source<T> Skip(int count) =>
        new(Shape.Append(LocalStageDescriptor.Skip(LocalOptionGuard.Count(count, nameof(count)))));

    /// <summary>Extends this source with a stage that passes elements while a predicate holds.</summary>
    /// <param name="predicate">The test each element must pass for the stream to continue.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The boundary is exclusive, per the naming rules of ADR 0004 section 7: the first element the
    /// predicate rejects is not emitted, and the run completes as if the source had ended there.
    /// <see cref="TakeThrough"/> is the inclusive spelling and is a different word rather than a flag.
    /// </remarks>
    public Source<T> TakeWhile(Func<T, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        return new Source<T>(Shape.Append(LocalStageDescriptor.TakeWhile(predicate)));
    }

    /// <summary>Extends this source with a stage that passes elements up to and including one the predicate accepts.</summary>
    /// <param name="predicate">The test that decides which element is the last one.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The inclusive counterpart of <see cref="TakeWhile"/>, and the streaming name rather than a LINQ one
    /// because LINQ has no such operator to borrow from: the element the predicate accepts is emitted and
    /// the run completes after it. This is how a stream ends at a terminator it has to deliver — the last
    /// page, the closing record, the sentinel.
    /// </remarks>
    public Source<T> TakeThrough(Func<T, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        return new Source<T>(Shape.Append(LocalStageDescriptor.TakeThrough(predicate)));
    }

    /// <summary>Extends this source with a stage that drops elements while a predicate holds.</summary>
    /// <param name="predicate">The test that decides which elements to drop.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// Exclusive in the same sense <see cref="TakeWhile"/> is: the first element the predicate rejects is
    /// emitted, and so is everything after it, whether or not the predicate would accept it again. The
    /// predicate is not consulted after that element at all.
    /// </remarks>
    public Source<T> SkipWhile(Func<T, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        return new Source<T>(Shape.Append(LocalStageDescriptor.SkipWhile(predicate)));
    }

    /// <summary>Extends this source with a stage that passes the first occurrence of every element.</summary>
    /// <param name="options">The greatest number of distinct elements the stage may remember.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="DistinctOptions.MaxTrackedKeys"/> is below one.
    /// </exception>
    /// <remarks>
    /// Elements are compared with <see cref="EqualityComparer{T}.Default"/>, so a type that defines its own
    /// equality is deduplicated by it. The bound is required and is not a hint: an element that would be
    /// the one key past it faults the run with a <see cref="TrackedKeyOverflowException"/> rather than
    /// evicting an older key, because an evicted key would be emitted twice and the stream would silently
    /// stop being distinct. A repeated element is recognized and dropped without occupying anything new.
    /// </remarks>
    public Source<T> Distinct(DistinctOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new Source<T>(Shape.Append(LocalStageDescriptor.Distinct(
                LocalOptionGuard.Distinct(options, nameof(options)),
                EqualityComparer<T>.Default)));
    }

    /// <summary>Extends this source with a bounded buffer.</summary>
    /// <param name="options">The capacity and the overflow policy.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="BufferOptions.Capacity"/> is below one, or
    /// <see cref="BufferOptions.OverflowPolicy"/> is not a declared member of its enumeration.
    /// </exception>
    /// <remarks>
    /// A buffer is where a graph stops being one loop. Everything upstream of it runs as one fused segment
    /// and everything downstream as another, and the buffer is the one bounded queue between them; without
    /// one, adjacent stages fuse and there is no queue anywhere. The elements the buffer holds are counted
    /// against nothing else: total memory is the sum of the capacities the author declared.
    /// </remarks>
    public Source<T> Buffer(BufferOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new Source<T>(
            Shape.Append(LocalStageDescriptor.Buffer(LocalOptionGuard.Buffer(options, nameof(options)))));
    }

    /// <summary>Extends this source with a stage that holds every element for a declared duration.</summary>
    /// <param name="delay">How long each element is held before it is emitted.</param>
    /// <param name="holdback">How many elements may be held at once, and what happens to the next one.</param>
    /// <returns>A new source; this one is unchanged.</returns>
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
    public Source<T> Delay(TimeSpan delay, BufferOptions holdback)
    {
        ArgumentNullException.ThrowIfNull(holdback);

        return new Source<T>(Shape.Append(LocalStageDescriptor.Delay(
            LocalOptionGuard.Duration(delay, nameof(delay)),
            LocalOptionGuard.Buffer(holdback, nameof(holdback)))));
    }

    /// <summary>Extends this source with a stage that holds the first element until a duration has passed.</summary>
    /// <param name="delay">How long after the run starts the first element may be emitted.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="delay"/> is not positive.</exception>
    /// <remarks>
    /// The delay is on the stream and not on its elements: the first element is held until
    /// <paramref name="delay"/> has passed since the run started, and everything after it passes untouched.
    /// A stream whose first element arrives later than that is not delayed at all, because the wait is for
    /// the moment rather than for the duration. A cancellation abandons the element being held and a
    /// graceful shutdown releases it, which is where this differs from <see cref="Delay"/>: an element in
    /// the segment's own hand is delivered by a stop, and one in an asynchronous window is waited out.
    /// </remarks>
    public Source<T> InitialDelay(TimeSpan delay) =>
        new Source<T>(Shape.Append(LocalStageDescriptor.Timed(
            LocalStageKind.InitialDelay,
            LocalOptionGuard.Duration(delay, nameof(delay)))));

    /// <summary>Extends this source with a stage that fails the run when the stream goes quiet.</summary>
    /// <param name="gap">The greatest silence allowed between two elements, and before the first.</param>
    /// <returns>A new source; this one is unchanged.</returns>
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
    public Source<T> Timeout(TimeSpan gap) =>
        new Source<T>(Shape.Append(LocalStageDescriptor.Timed(
            LocalStageKind.Timeout,
            LocalOptionGuard.Duration(gap, nameof(gap)))));

    /// <summary>Extends this source with a stage that ends the stream when a duration has passed.</summary>
    /// <param name="window">How long after the run starts the stream ends.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="window"/> is not positive.</exception>
    /// <remarks>
    /// The window is wall-clock and not a count: everything emitted before it closes is kept, the element
    /// that arrives at or after it is not emitted, and the stream ends there the way reaching a
    /// <c>Take</c> bound ends it — upstream stops and is released, everything already downstream drains,
    /// and the run reports success. A stream that has gone quiet still ends at the deadline rather than
    /// waiting for an element to notice it with, which is the case this operator exists for.
    /// </remarks>
    public Source<T> TakeWithin(TimeSpan window) =>
        new Source<T>(Shape.Append(LocalStageDescriptor.Timed(
            LocalStageKind.TakeWithin,
            LocalOptionGuard.Duration(window, nameof(window)))));

    /// <summary>Extends this source with a stage that drops every element until a duration has passed.</summary>
    /// <param name="window">How long after the run starts elements begin to pass.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="window"/> is not positive.</exception>
    /// <remarks>
    /// The mirror of <see cref="TakeWithin"/> and the wall-clock <c>Skip</c>: an element arriving inside
    /// the window is dropped rather than held, and everything from the first element after it passes. The
    /// stage never waits — it has an answer for every element the moment it arrives — so a stream that
    /// produces nothing during the window costs nothing at all.
    /// </remarks>
    public Source<T> SkipWithin(TimeSpan window) =>
        new Source<T>(Shape.Append(LocalStageDescriptor.Timed(
            LocalStageKind.SkipWithin,
            LocalOptionGuard.Duration(window, nameof(window)))));

    /// <summary>Extends this source with a stage that holds the stream to a declared rate.</summary>
    /// <param name="options">The rate, the burst, and what to do with an element there is no budget for.</param>
    /// <returns>A new source; this one is unchanged.</returns>
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
    public Source<T> Throttle(ThrottleOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new Source<T>(Shape.Append(LocalStageDescriptor.Throttle(
            LocalOptionGuard.Throttle(options, nameof(options)),
            cost: null)));
    }


    /// <summary>Extends this source with a gate an author opens and closes while the run is running.</summary>
    /// <param name="controlName">The author-stable name to expose the valve under.</param>
    /// <param name="initialMode">The state the valve starts each run in.</param>
    /// <returns>A new source; this one is unchanged.</returns>
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
    public Source<T> Valve(string controlName, ValveMode initialMode = ValveMode.Open) =>
        new Source<T>(Shape.Append(LocalStageDescriptor.Valve(
            LocalOptionGuard.Valve(initialMode, nameof(initialMode)),
            LocalOptionGuard.SlotName(controlName, nameof(controlName)))));

    /// <summary>Extends this source with a stage that holds the stream to a declared rate by cost.</summary>
    /// <param name="options">The rate, the burst, and what to do with an element there is no budget for.</param>
    /// <param name="cost">What one element costs the rate; zero or more.</param>
    /// <returns>A new source; this one is unchanged.</returns>
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
    public Source<T> Throttle(ThrottleOptions options, Func<T, int> cost)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(cost);

        return new Source<T>(Shape.Append(LocalStageDescriptor.Throttle(
            LocalOptionGuard.Throttle(options, nameof(options)),
            cost)));
    }

    /// <summary>Extends this source with an asynchronous mapping stage that preserves input order.</summary>
    /// <typeparam name="TOut">The element type the mapping produces.</typeparam>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="selector">The callback applied to every element.</param>
    /// <returns>A new source; this one is unchanged.</returns>
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
    public Source<TOut> SelectAsync<TOut>(
        ParallelismOptions options,
        Func<T, CancellationToken, Task<TOut>> selector)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(selector);

        return new Source<TOut>(Shape.Append(
            LocalStageDescriptor.SelectAsync(LocalOptionGuard.Parallelism(options, nameof(options)), selector)));
    }

    /// <summary>Extends this source with an asynchronous mapping stage that emits in completion order.</summary>
    /// <typeparam name="TOut">The element type the mapping produces.</typeparam>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="selector">The callback applied to every element.</param>
    /// <returns>A new source; this one is unchanged.</returns>
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
    public Source<TOut> SelectAsyncUnordered<TOut>(
        ParallelismOptions options,
        Func<T, CancellationToken, Task<TOut>> selector)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(selector);

        return new Source<TOut>(Shape.Append(
            LocalStageDescriptor.SelectAsyncUnordered(
                LocalOptionGuard.Parallelism(options, nameof(options)),
                selector)));
    }

    /// <summary>
    /// Extends this source with an asynchronous mapping stage over value tasks that preserves input order.
    /// </summary>
    /// <typeparam name="TOut">The element type the mapping produces.</typeparam>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="selector">The callback applied to every element.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> or <paramref name="selector"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="ParallelismOptions.MaxConcurrency"/> is below one.
    /// </exception>
    /// <remarks>
    /// <para>
    /// <see cref="SelectAsync"/> in every respect but the type of the thing awaited (ADR 0004 section 7
    /// names the three families): the same bound on callbacks in flight, the same emission in input order,
    /// the same run token, and the same rule that a failing callback faults the run, cancels the callbacks
    /// beside it, and starts no later element.
    /// </para>
    /// <para>
    /// The family exists because a <see cref="ValueTask{TResult}"/> is what a callback that usually
    /// finishes synchronously should return — a cache hit, a value already in memory — and converting one
    /// to a <see cref="Task{TResult}"/> at the call site to reach <see cref="SelectAsync"/> would allocate
    /// exactly what the value task was chosen to avoid.
    /// </para>
    /// <para>
    /// <b>The runtime awaits each returned value task exactly once, and never after reading its result.</b>
    /// That is the rule a value task imposes on whoever consumes it, and it is stated here because the
    /// consumer is this runtime rather than the author: a callback may return a value task backed by a
    /// pooled source, and this operator is a correct consumer of one. What the author must not do is hand
    /// back a value task that something else is also going to await.
    /// </para>
    /// </remarks>
    public Source<TOut> SelectValueTaskAsync<TOut>(
        ParallelismOptions options,
        Func<T, CancellationToken, ValueTask<TOut>> selector)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(selector);

        return new Source<TOut>(Shape.Append(
            LocalStageDescriptor.SelectValueTaskAsync(
                LocalOptionGuard.Parallelism(options, nameof(options)),
                selector)));
    }

    /// <summary>
    /// Extends this source with an asynchronous mapping stage over value tasks that emits in completion
    /// order.
    /// </summary>
    /// <typeparam name="TOut">The element type the mapping produces.</typeparam>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="selector">The callback applied to every element.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> or <paramref name="selector"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="ParallelismOptions.MaxConcurrency"/> is below one.
    /// </exception>
    /// <remarks>
    /// The unordered spelling of <see cref="SelectValueTaskAsync{TOut}"/>, with the one difference its name
    /// states: a result is emitted as soon as its callback finishes. The single-consumption rule of
    /// <see cref="SelectValueTaskAsync{TOut}"/> applies here unchanged.
    /// </remarks>
    public Source<TOut> SelectValueTaskAsyncUnordered<TOut>(
        ParallelismOptions options,
        Func<T, CancellationToken, ValueTask<TOut>> selector)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(selector);

        return new Source<TOut>(Shape.Append(
            LocalStageDescriptor.SelectValueTaskAsyncUnordered(
                LocalOptionGuard.Parallelism(options, nameof(options)),
                selector)));
    }

    /// <summary>Extends this source with a reusable flow.</summary>
    /// <typeparam name="TOut">The element type the flow produces.</typeparam>
    /// <param name="flow">The flow to compose, which is not modified.</param>
    /// <returns>A new source; neither argument is changed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="flow"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The flow's occurrences are copied into the result, so the same flow can be composed here and
    /// elsewhere with no shared state. Composing one flow twice into one source is not a special case
    /// either: it contributes its occurrences twice, and closure numbers them as the distinct occurrences
    /// they are.
    /// </remarks>
    public Source<TOut> Via<TOut>(Flow<T, TOut> flow)
    {
        ArgumentNullException.ThrowIfNull(flow);

        return new Source<TOut>(Shape.Concat(flow.Stages));
    }

    /// <summary>Extends this source with one named occurrence of a registered stage.</summary>
    /// <typeparam name="TOut">The element type the registered stage produces.</typeparam>
    /// <param name="flow">The typed handle of the registered stage.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="flow"/> or <paramref name="occurrenceName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The name is required, because a registered occurrence exists to be addressed across an edit, a
    /// checkpoint, and an upgrade, and a positional identifier anchors none of those (ADR 0004 section 6).
    /// Two occurrences of one graph may not share a name; that is reported when the chain is closed, which
    /// is where the whole chain is first visible.
    /// </para>
    /// <para>
    /// The payload is the raw canonical value the stage's parameter contract describes, and it is checked
    /// against that contract by the graph compiler rather than here. Typed parameter builders are
    /// provider-SDK sugar and are deliberately not part of this surface.
    /// </para>
    /// </remarks>
    public Source<TOut> Via<TOut>(
        RegisteredFlow<T, TOut> flow,
        string occurrenceName,
        CanonicalJsonValue parameters)
    {
        ArgumentNullException.ThrowIfNull(flow);

        return new Source<TOut>(Shape.Append(
            RegisteredAttachment.Occurrence(flow.Specification, occurrenceName, parameters)));
    }

    /// <summary>Joins this source with another, emitting from whichever of the two has an element.</summary>
    /// <param name="other">The source to merge with, which is not modified.</param>
    /// <returns>A source of both streams' elements; neither argument is changed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="other"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// The elements of one input keep their order relative to each other and nothing is promised about how
    /// the two interleave: a merge emits what has arrived, which is what makes it the junction to reach for
    /// when the streams are independent and the order between them carries no meaning. The merged stream
    /// ends when both inputs have.
    /// </para>
    /// <para>
    /// Merging three streams is <c>a.Merge(b, c)</c> and merging four is <c>a.Merge(b, c).Merge(d)</c>. The
    /// second is honestly two junctions rather than one: merge semantics are associative, but the two
    /// documents are distinct and fingerprint differently, and ADR 0006 states that rather than papering over
    /// it with a rewrite that flattens the chain.
    /// </para>
    /// </remarks>
    public Source<T> Merge(Source<T> other)
    {
        ArgumentNullException.ThrowIfNull(other);

        return Joined<T>(LocalStageDescriptor.Merge(), other.Shape);
    }

    /// <summary>Joins this source with two others, emitting from whichever of the three has an element.</summary>
    /// <param name="second">The second source, which is not modified.</param>
    /// <param name="third">The third source, which is not modified.</param>
    /// <returns>A source of all three streams' elements; no argument is changed.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="second"/> or <paramref name="third"/> is <see langword="null"/>.
    /// </exception>
    /// <remarks>
    /// One junction with three inputs rather than two junctions, which is a different document from
    /// <c>a.Merge(b).Merge(c)</c> and is the one to write when the three streams are peers. Three is where
    /// the overloads stop: wider merges chain, and a chain says what it is.
    /// </remarks>
    public Source<T> Merge(Source<T> second, Source<T> third)
    {
        ArgumentNullException.ThrowIfNull(second);
        ArgumentNullException.ThrowIfNull(third);

        return Joined<T>(LocalStageDescriptor.Merge(), second.Shape, third.Shape);
    }

    /// <summary>Follows this source with another, emitting the second only after the first has ended.</summary>
    /// <param name="next">The source to emit after this one, which is not modified.</param>
    /// <returns>A source of this stream followed by that one; neither argument is changed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="next"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The ordered fan-in: every element of this source is emitted, in order, before the first element of
    /// <paramref name="next"/> is asked for. That is the difference from <see cref="Merge(Source{T})"/> and
    /// it is a difference in when the second source is pulled at all, not only in the order elements come
    /// out: a concat holds its later inputs untouched until their turn.
    /// </remarks>
    public Source<T> Concat(Source<T> next)
    {
        ArgumentNullException.ThrowIfNull(next);

        return Joined<T>(LocalStageDescriptor.Concat(), next.Shape);
    }

    /// <summary>Joins this source with another by taking a declared number of elements from each in turn.</summary>
    /// <param name="other">The source to interleave with, which is not modified.</param>
    /// <param name="segmentSize">How many elements to take from one input before moving to the next.</param>
    /// <returns>A source of both streams' elements in a fixed rotation; neither argument is changed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="other"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="segmentSize"/> is below one.</exception>
    /// <remarks>
    /// The deterministic fan-in: unlike a merge, the output order is decided by the rotation and not by
    /// which input happened to have an element. An input that ends is dropped from the rotation and the
    /// remaining ones carry on, so a shorter stream does not end the join.
    /// </remarks>
    public Source<T> Interleave(Source<T> other, int segmentSize)
    {
        ArgumentNullException.ThrowIfNull(other);

        return Joined<T>(
            LocalStageDescriptor.Interleave(LocalOptionGuard.SegmentSize(segmentSize, nameof(segmentSize))),
            other.Shape);
    }

    /// <summary>Joins this source with another into a stream of pairs.</summary>
    /// <typeparam name="T2">The element type of the other source.</typeparam>
    /// <param name="other">The source to pair with, which is not modified.</param>
    /// <returns>A source of one pair per element from each input; neither argument is changed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="other"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// Positional and lockstep: the first element of each input makes the first pair, the second of each
    /// makes the second, and the joined stream ends as soon as either input does — whatever the other still
    /// had. The pair's members are named for the order the inputs were written in.
    /// </remarks>
    public Source<(T First, T2 Second)> Zip<T2>(Source<T2> other) =>
        Zip(other, static (first, second) => (first, second));

    /// <summary>Joins this source with another through a function of one element from each.</summary>
    /// <typeparam name="T2">The element type of the other source.</typeparam>
    /// <typeparam name="TOut">The element type the function produces.</typeparam>
    /// <param name="other">The source to join with, which is not modified.</param>
    /// <param name="combine">The function building one element from one element of each input.</param>
    /// <returns>A source of one element per element from each input; neither argument is changed.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="other"/> or <paramref name="combine"/> is <see langword="null"/>.
    /// </exception>
    /// <remarks>
    /// The same lockstep join as <see cref="Zip{T2}(Source{T2})"/> with the row built by the author rather
    /// than by the tuple, which is what keeps a join that immediately projects from allocating a pair only
    /// to take it apart again. The function never enters the document, so a graph holding one is
    /// <c>nondeployable</c> exactly as a graph holding any lambda is.
    /// </remarks>
    public Source<TOut> Zip<T2, TOut>(Source<T2> other, Func<T, T2, TOut> combine)
    {
        ArgumentNullException.ThrowIfNull(other);
        ArgumentNullException.ThrowIfNull(combine);

        return Joined<TOut>(LocalStageDescriptor.Zip(LocalRowCombiner.Of(combine)), other.Shape);
    }

    /// <summary>Joins this source with another by combining each arrival with the other's latest element.</summary>
    /// <typeparam name="T2">The element type of the other source.</typeparam>
    /// <typeparam name="TOut">The element type the function produces.</typeparam>
    /// <param name="other">The source to join with, which is not modified.</param>
    /// <param name="combine">The function building one element from the latest element of each input.</param>
    /// <returns>A source of one element per arrival once both inputs have produced; neither argument is changed.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="other"/> or <paramref name="combine"/> is <see langword="null"/>.
    /// </exception>
    /// <remarks>
    /// Not a lockstep join and deliberately a different word for it: nothing is emitted until both inputs
    /// have produced at least once, and after that every arrival on either side emits a row built from it
    /// and from whatever the other side last produced. A fast input therefore produces many rows against one
    /// slow element, which is the point — this is the join for a stream against a setting, not for two
    /// streams of matching rows.
    /// </remarks>
    public Source<TOut> CombineLatest<T2, TOut>(Source<T2> other, Func<T, T2, TOut> combine)
    {
        ArgumentNullException.ThrowIfNull(other);
        ArgumentNullException.ThrowIfNull(combine);

        return Joined<TOut>(LocalStageDescriptor.CombineLatest(LocalRowCombiner.Of(combine)), other.Shape);
    }

    /// <summary>Sends every element down two flows at once, to be rejoined.</summary>
    /// <typeparam name="T1">The element type the left flow produces.</typeparam>
    /// <typeparam name="T2">The element type the right flow produces.</typeparam>
    /// <param name="left">The first derivation, which is not modified.</param>
    /// <param name="right">The second derivation, which is not modified.</param>
    /// <returns>The fork, which is rejoined by one of its own calls.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="left"/> or <paramref name="right"/> is <see langword="null"/>.
    /// </exception>
    /// <remarks>
    /// The one shape a tree cannot express: the same element travels two paths and the paths meet again.
    /// Every element is broadcast to both flows, so the two derived streams advance together — which is what
    /// makes <see cref="Fork{T1, T2}.Zip()"/> a join that needs no buffer between the halves. The fork is a
    /// value with two open ends and no way to close a graph, so a program that builds one has to rejoin it.
    /// </remarks>
    public Fork<T1, T2> Fork<T1, T2>(Flow<T, T1> left, Flow<T, T2> right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);

        return new Fork<T1, T2>(Split(LocalStageDescriptor.Broadcast(), left.Stages, right.Stages));
    }

    /// <summary>Sends every element down two flows at once and takes whichever result arrives first.</summary>
    /// <typeparam name="TOut">The element type both flows produce.</typeparam>
    /// <param name="left">The first derivation, which is not modified.</param>
    /// <param name="right">The second derivation, which is not modified.</param>
    /// <returns>A source of both derivations' elements; neither argument is changed.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="left"/> or <paramref name="right"/> is <see langword="null"/>.
    /// </exception>
    /// <remarks>
    /// The unordered rejoin, and the shape a race is written in: one element in produces two elements out —
    /// one per path — in whatever order the paths finish. That is a merge and not a zip, so the two
    /// derivations of one element are not paired and nothing waits for the slower path before emitting the
    /// faster one. <see cref="Fork{T1, T2}"/> is the rejoin for when the two derivations belong together.
    /// </remarks>
    public Source<TOut> ForkMerge<TOut>(Flow<T, TOut> left, Flow<T, TOut> right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);

        return new Source<TOut>(Split(LocalStageDescriptor.Broadcast(), left.Stages, right.Stages)
            .Combine(LocalStageDescriptor.Merge(), LocalJunctionGuard.FanInPorts(LocalVocabulary.MinFanIn)));
    }

    /// <summary>Sends every element to a branch as well, and continues.</summary>
    /// <param name="side">The branch to tap into, which is not modified.</param>
    /// <returns>A source of the same elements; the argument is not changed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="side"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// The tap, and broadcast sugar underneath: a junction with the main line on its first leg and the
    /// branch on its second, so every element reaches both. What that costs is the broadcast's own rule —
    /// an element is delivered to every leg, so a branch that stops consuming holds the main line up. A tap
    /// is not a fire-and-forget side effect, and this is the honest place to say so.
    /// </para>
    /// <para>
    /// A branch that declares a result is welcome here: it named its slot where its sink was written, so the
    /// result is carried until the graph is closed and declared then, beside whatever the main line
    /// declares.
    /// </para>
    /// </remarks>
    public Source<T> AlsoTo(Branch<T> side)
    {
        ArgumentNullException.ThrowIfNull(side);

        LocalGraphShape shape = Split(LocalStageDescriptor.Broadcast(), [], side.Stages);

        return new Source<T>(
            side.SlotName is { } name
                ? shape.Declaring(new LocalSlotRequest(name, shape.Stages.Count - 1, side.Binding))
                : shape);
    }

    /// <summary>Closes this source with a sink that declares no result.</summary>
    /// <param name="sink">The sink terminating the graph.</param>
    /// <returns>The closed graph.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sink"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// A result-bearing sink does not fit here without an explicit conversion, so a graph never drops a
    /// result by overload accident: dropping one is the one-argument call spelled deliberately.
    /// </remarks>
    public RunnableGraph To(Sink<T> sink)
    {
        ArgumentNullException.ThrowIfNull(sink);

        return CloseShape(Shape.Concat(sink.Stages));
    }

    /// <summary>Closes this source with a resultless sink built from the element type's own vocabulary.</summary>
    /// <param name="sink">A function choosing a sink from the factory for this element type.</param>
    /// <returns>The closed graph.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sink"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="sink"/> returned <see langword="null"/>.</exception>
    /// <remarks>
    /// The factory form of the one-argument close, so <c>source.To(s =&gt; s.Ignore())</c> reads the same
    /// way as the result-bearing factory overloads. Overload resolution between this and the
    /// result-bearing factory forms is by the lambda's return type and was probed unambiguous (ADR 0004).
    /// </remarks>
    public RunnableGraph To(Func<SinkFactory<T>, Sink<T>> sink)
    {
        ArgumentNullException.ThrowIfNull(sink);

        Sink<T> resolved = sink(SinkFactory<T>.Instance) ??
            throw new ArgumentException(
                $"The sink factory returned null, and a graph is closed by a sink. Return a sink from the {nameof(SinkFactory<T>)} the lambda receives, such as 's => s.Ignore()'.",
                nameof(sink));

        return To(resolved);
    }

    /// <summary>Closes this source with one named occurrence of a registered stage that declares no result.</summary>
    /// <param name="sink">The typed handle of the registered stage terminating the graph.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <returns>The closed graph.</returns>
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
    /// conversion error naming both types rather than a graph that silently produces nothing readable.
    /// </remarks>
    public RunnableGraph To(RegisteredSink<T> sink, string occurrenceName, CanonicalJsonValue parameters)
    {
        ArgumentNullException.ThrowIfNull(sink);

        return CloseShape(Shape.Append(RegisteredAttachment.Occurrence(sink.Specification, occurrenceName, parameters)));
    }

    /// <summary>
    /// Closes this source with one named occurrence of a registered result-bearing stage, returning the
    /// graph and its slot together.
    /// </summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The typed handle of the registered stage terminating the graph.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <param name="slotName">The author-stable name to expose the result under.</param>
    /// <returns>The closed graph and the slot that resolves its result.</returns>
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
    /// the node's durable identity in the graph, and the slot name is what a run handle resolves the
    /// result under. This is the composable form, for the reason ADR 0004 section 3 gives — a tuple
    /// survives <c>async</c> signatures, collections, and interface members.
    /// </remarks>
    public (RunnableGraph Graph, ResultSlot<TResult> Slot) To<TResult>(
        RegisteredSinkWithResult<T, TResult> sink,
        string occurrenceName,
        CanonicalJsonValue parameters,
        string slotName)
    {
        RunnableGraph graph = CloseWithRegisteredResult(
            sink,
            occurrenceName,
            parameters,
            slotName,
            out ResultSlot<TResult> slot);

        return (graph, slot);
    }

    /// <summary>
    /// Closes this source with one named occurrence of a registered result-bearing stage, handing back the
    /// slot as an output.
    /// </summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The typed handle of the registered stage terminating the graph.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <param name="slotName">The author-stable name to expose the result under.</param>
    /// <param name="slot">When this method returns, the slot that resolves the result.</param>
    /// <returns>The closed graph.</returns>
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
    /// The fluent form, which produces the same document as the tuple overload because both funnel through
    /// one closure.
    /// </remarks>
    public RunnableGraph To<TResult>(
        RegisteredSinkWithResult<T, TResult> sink,
        string occurrenceName,
        CanonicalJsonValue parameters,
        string slotName,
        out ResultSlot<TResult> slot) =>
        CloseWithRegisteredResult(sink, occurrenceName, parameters, slotName, out slot);

    /// <summary>Closes this source with a result-bearing sink, returning the graph and its slot together.</summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The sink terminating the graph.</param>
    /// <param name="slotName">The author-stable name to expose the result under.</param>
    /// <returns>The closed graph and the slot that resolves its result.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="sink"/> or <paramref name="slotName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="slotName"/> is not a valid <see cref="ResultSlotId"/>.
    /// </exception>
    /// <remarks>
    /// This is the composable form: a tuple survives <c>async</c> signatures, collections, and interface
    /// members, where an <see langword="out"/> parameter is not allowed at all (ADR 0004 section 3).
    /// </remarks>
    public (RunnableGraph Graph, ResultSlot<TResult> Slot) To<TResult>(
        SinkWithResult<T, TResult> sink,
        string slotName)
    {
        RunnableGraph graph = CloseWithResult(sink, slotName, out ResultSlot<TResult> slot);

        return (graph, slot);
    }

    /// <summary>Closes this source with a result-bearing sink, handing back the slot as an output.</summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The sink terminating the graph.</param>
    /// <param name="slotName">The author-stable name to expose the result under.</param>
    /// <param name="slot">When this method returns, the slot that resolves the result.</param>
    /// <returns>The closed graph.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="sink"/> or <paramref name="slotName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="slotName"/> is not a valid <see cref="ResultSlotId"/>.
    /// </exception>
    /// <remarks>
    /// This is the fluent form: the call is an expression of type <see cref="RunnableGraph"/>, so it reads
    /// inside an argument list, a switch arm, a ternary, or an initializer. It produces the same document
    /// as the tuple overload.
    /// </remarks>
    public RunnableGraph To<TResult>(
        SinkWithResult<T, TResult> sink,
        string slotName,
        out ResultSlot<TResult> slot) =>
        CloseWithResult(sink, slotName, out slot);

    /// <summary>
    /// Closes this source with a sink built from the element type's own vocabulary, returning the graph and
    /// its slot together.
    /// </summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">A function choosing a sink from the factory for this element type.</param>
    /// <param name="slotName">The author-stable name to expose the result under.</param>
    /// <returns>The closed graph and the slot that resolves its result.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="sink"/> or <paramref name="slotName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="sink"/> returned <see langword="null"/>, or <paramref name="slotName"/> is not a
    /// valid <see cref="ResultSlotId"/>.
    /// </exception>
    /// <remarks>
    /// The factory form is what makes inference total: the element type is pinned by this source and the
    /// result type flows out of the lambda, so <c>s =&gt; s.Aggregate(0L, (count, _) =&gt; count + 1)</c> needs
    /// no type argument and no lambda annotation. <paramref name="slotName"/> is validated before the
    /// lambda is invoked, so a rejected name never costs the author a side effect.
    /// </remarks>
    public (RunnableGraph Graph, ResultSlot<TResult> Slot) To<TResult>(
        Func<SinkFactory<T>, SinkWithResult<T, TResult>> sink,
        string slotName)
    {
        RunnableGraph graph = CloseWithFactory(sink, slotName, out ResultSlot<TResult> slot);

        return (graph, slot);
    }

    /// <summary>
    /// Closes this source with a sink built from the element type's own vocabulary, handing back the slot
    /// as an output.
    /// </summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">A function choosing a sink from the factory for this element type.</param>
    /// <param name="slotName">The author-stable name to expose the result under.</param>
    /// <param name="slot">When this method returns, the slot that resolves the result.</param>
    /// <returns>The closed graph.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="sink"/> or <paramref name="slotName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="sink"/> returned <see langword="null"/>, or <paramref name="slotName"/> is not a
    /// valid <see cref="ResultSlotId"/>.
    /// </exception>
    /// <remarks>
    /// This is the spelling the flagship example uses; it produces the same document as the tuple
    /// overload. <paramref name="slotName"/> is validated before the lambda is invoked, so a rejected name
    /// never costs the author a side effect.
    /// </remarks>
    public RunnableGraph To<TResult>(
        Func<SinkFactory<T>, SinkWithResult<T, TResult>> sink,
        string slotName,
        out ResultSlot<TResult> slot) =>
        CloseWithFactory(sink, slotName, out slot);

    /// <summary>Closes this source with a result-bearing sink and no name for the result. Never valid.</summary>
    /// <typeparam name="TResult">The type of the result the sink declares.</typeparam>
    /// <param name="sink">The sink that would terminate the graph.</param>
    /// <returns>Nothing; the call cannot compile, and cannot be reached if it somehow does.</returns>
    /// <exception cref="NotSupportedException">Always.</exception>
    /// <remarks>
    /// <para>
    /// This overload exists only to make the mistake it names a compile error with a useful message.
    /// Without it, <c>To(countingSink)</c> is a wrong-type call whose one compiler-suggested repair is a
    /// cast to <see cref="Sink{T}"/> — which compiles, and silently drops the result the author asked for.
    /// A result-bearing close therefore has a real overload to bind to, and binding to it says what to
    /// write instead (ADR 0004 section 3).
    /// </para>
    /// <para>
    /// A guard is compile-time surface: it is never called, and nothing in a passing test suite can
    /// invoke it. The body throws rather than returning, because reaching it at all — through reflection,
    /// or through a compiler that stopped honoring the attribute — means the guarantee is already gone.
    /// </para>
    /// </remarks>
    [Obsolete(
        "A result-bearing sink needs a name for its result: write To(sink, \"name\") for the tuple form or To(sink, \"name\", out var slot) for the fluent form. To run the sink and deliberately discard its result, write To(sink.ToSink()).",
        error: true)]
    public RunnableGraph To<TResult>(SinkWithResult<T, TResult> sink) =>
        throw new NotSupportedException(GuardOverload());

    /// <summary>
    /// Closes this source with a sink-factory lambda that chooses a result-bearing sink, and no name for
    /// the result. Never valid.
    /// </summary>
    /// <typeparam name="TResult">The type of the result the chosen sink declares.</typeparam>
    /// <param name="sink">The function that would choose the sink.</param>
    /// <returns>Nothing; the call cannot compile, and cannot be reached if it somehow does.</returns>
    /// <exception cref="NotSupportedException">Always.</exception>
    /// <remarks>
    /// The factory-lambda half of the same guard. <c>To(s =&gt; s.Aggregate(0L, (count, _) =&gt; count + 1))</c>
    /// would otherwise be a wrong-type call the compiler repairs with a cast that drops the result, so the
    /// shape binds here instead and names the two correct spellings.
    /// </remarks>
    [Obsolete(
        "A result-bearing sink needs a name for its result: write To(s => s.Aggregate(seed, folder), \"name\") for the tuple form or To(s => s.Aggregate(seed, folder), \"name\", out var slot) for the fluent form. To run the sink and deliberately discard its result, write To(s => s.Aggregate(seed, folder).ToSink()).",
        error: true)]
    public RunnableGraph To<TResult>(Func<SinkFactory<T>, SinkWithResult<T, TResult>> sink) =>
        throw new NotSupportedException(GuardOverload());

    /// <summary>Closes this source by delivering every element to every branch.</summary>
    /// <param name="branches">The branches, in the order they are wired to the junction's legs.</param>
    /// <returns>The closed graph.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="branches"/>, or one of its elements, is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// There are fewer than two branches or more than the eight a local junction declares legs for, or two
    /// branches declare a result under one name.
    /// </exception>
    /// <remarks>
    /// <para>
    /// A terminal call, exactly as <c>To</c> is: the branches end in sinks, so nothing is left open and the
    /// graph is closed here. Every element reaches every branch, which means a branch that stops consuming
    /// holds up all of them — a broadcast asks each leg for room before it pulls, and that is the bounded
    /// memory this junction buys.
    /// </para>
    /// <para>
    /// Branch order is argument order and is identity-bearing: the first branch's occurrences are numbered
    /// before the second's, so swapping two arguments builds a different document with a different
    /// fingerprint. That is the same rule reordering a chain follows.
    /// </para>
    /// </remarks>
    public RunnableGraph BroadcastTo(params Branch<T>[] branches) =>
        FanOut(LocalStageDescriptor.Broadcast(), branches, nameof(branches));

    /// <summary>Closes this source by delivering each element to one branch that has room.</summary>
    /// <param name="branches">The branches, in the order they are wired to the junction's legs.</param>
    /// <returns>The closed graph.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="branches"/>, or one of its elements, is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// There are fewer than two branches or more than the eight a local junction declares legs for, or two
    /// branches declare a result under one name.
    /// </exception>
    /// <remarks>
    /// Every element goes to exactly one branch and which one is not defined: a balance hands an element to
    /// whichever leg is ready for it, which is what makes it the junction for spreading work rather than for
    /// classifying it. The branches are usually the same pipeline written twice, and nothing requires them
    /// to be.
    /// </remarks>
    public RunnableGraph BalanceTo(params Branch<T>[] branches) =>
        FanOut(LocalStageDescriptor.Balance(), branches, nameof(branches));

    /// <summary>Closes this source by sending each element to the branch a function names.</summary>
    /// <param name="router">The function answering the zero-based position of the branch for an element.</param>
    /// <param name="branches">The branches, in the order the router's answers index them.</param>
    /// <returns>The closed graph.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="router"/>, <paramref name="branches"/>, or one of its elements, is
    /// <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// There are fewer than two branches or more than the eight a local junction declares legs for, or two
    /// branches declare a result under one name.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The classifying fan-out: the router sees the element and answers which branch it belongs on, so the
    /// branches are the classes and their order is the numbering the router answers in. Every element goes
    /// to exactly one branch, so unlike a broadcast this junction never duplicates and unlike a balance it
    /// is completely determined by the element.
    /// </para>
    /// <para>
    /// An answer outside the wired branches faults the run when it happens, not when the graph is built: how
    /// many branches this occurrence has is stated by its edges, and a function is not something a document
    /// can check. The router never enters the document either, which is why a partitioned graph is
    /// <c>nondeployable</c>.
    /// </para>
    /// </remarks>
    public RunnableGraph PartitionTo(Func<T, int> router, params Branch<T>[] branches)
    {
        ArgumentNullException.ThrowIfNull(router);

        return FanOut(LocalStageDescriptor.Partition(router), branches, nameof(branches));
    }

    /// <summary>Returns a one-line diagnostic summary of this source.</summary>
    /// <returns>Text of the form <c>source (3 stages)</c>, singular for one (<c>source (1 stage)</c>).</returns>
    /// <remarks>The count is formatted with the invariant culture, and the method never throws.</remarks>
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"source ({Stages.Count} {(Stages.Count == 1 ? "stage" : "stages")})");

    /// <summary>Builds the message a guard overload throws if it is ever reached.</summary>
    /// <returns>The message.</returns>
    /// <remarks>
    /// Shared by both guards, because there is exactly one thing to say: this member exists to fail at
    /// compile time and has no runtime behavior to fall back on.
    /// </remarks>
    private static string GuardOverload() =>
        $"This {nameof(To)} overload exists only as a compile-time guard against closing a graph with a result-bearing sink and no name for its result. It is marked as an error and is never a legal call; nothing in this library invokes it.";
    /// <summary>Joins this source and others into one through a fan-in junction.</summary>
    /// <typeparam name="TOut">The element type the junction emits.</typeparam>
    /// <param name="junction">The junction occurrence.</param>
    /// <param name="others">The shapes of the sources to join with, in argument order.</param>
    /// <returns>The joined source.</returns>
    /// <remarks>
    /// The receiver's occurrences come first and the arguments' follow in order, so the numbering of a join
    /// is the order it was written in and the junction's input ports follow the same order: this source
    /// reaches <c>in-0</c>, the first argument <c>in-1</c>, and so on.
    /// </remarks>
    private Source<TOut> Joined<TOut>(LocalStageDescriptor junction, params LocalGraphShape[] others)
    {
        LocalGraphShape joined = Shape;

        for (int index = 0; index < others.Length; index++)
        {
            joined = joined.Union(others[index]);
        }

        return new Source<TOut>(joined.Combine(junction, LocalJunctionGuard.FanInPorts(others.Length + 1)));
    }

    /// <summary>Splits this source into two legs through a fan-out junction.</summary>
    /// <param name="junction">The junction occurrence.</param>
    /// <param name="left">The occurrences of the first leg, which may be none.</param>
    /// <param name="right">The occurrences of the second leg, which may be none.</param>
    /// <returns>The split shape, with one open end per leg that still produces.</returns>
    /// <remarks>
    /// The two-legged split every non-terminal fan-out is: a fork, its merging sibling, and a tap. A leg with
    /// no occurrences of its own leaves the junction's own leg port open, which is how a tap keeps the main
    /// line flowing and how a fork through the identity flow costs no stage.
    /// </remarks>
    private LocalGraphShape Split(
        LocalStageDescriptor junction,
        IReadOnlyList<StageOccurrence> left,
        IReadOnlyList<StageOccurrence> right) =>
        Shape.Split(junction, LocalJunctionGuard.FanOutPorts(LocalVocabulary.MinFanOut), [left, right]);

    /// <summary>Closes this source into a graph through a fan-out junction and its branches.</summary>
    /// <param name="junction">The junction occurrence.</param>
    /// <param name="branches">The branches, unchecked.</param>
    /// <param name="parameterName">The name of the calling parameter, for the diagnostics.</param>
    /// <returns>The closed graph.</returns>
    /// <remarks>
    /// Every fan-out terminal funnels through here, which is what makes them one operation with three
    /// junction stages rather than three implementations: what differs between a broadcast, a balance, and a
    /// partition is the occurrence handed in, and everything else — the arity check, the leg order, the slot
    /// each result-bearing branch asks for — is the same statement in all three.
    /// </remarks>
    private RunnableGraph FanOut(LocalStageDescriptor junction, Branch<T>[] branches, string parameterName)
    {
        LocalJunctionGuard.Branches(branches, parameterName);

        int position = Shape.Stages.Count;
        LocalGraphShape shape = Shape.Split(
            junction,
            LocalJunctionGuard.FanOutPorts(branches.Length),
            LocalJunctionGuard.Chains(branches));

        return LocalGraphBuilder.Close(shape, LocalJunctionGuard.Slots(position, branches));
    }

    /// <summary>Closes a shape that declares no result.</summary>
    /// <param name="shape">The complete shape.</param>
    /// <returns>The closed graph.</returns>
    private static RunnableGraph CloseShape(LocalGraphShape shape) =>
        LocalGraphBuilder.Close(shape, LocalGraphBuilder.NoSlots);

    /// <summary>Closes a shape whose last occurrence declares the graph's result.</summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="shape">The complete shape.</param>
    /// <param name="slotId">The validated slot name.</param>
    /// <param name="slot">When this method returns, the slot that resolves the result.</param>
    /// <returns>The closed graph.</returns>
    /// <remarks>
    /// The producing occurrence is the shape's last, because a chain's terminal is where a chain ends. A
    /// branch's terminal is not, which is why a branch's slot is asked for by position instead.
    /// </remarks>
    private static RunnableGraph CloseShape<TResult>(
        LocalGraphShape shape,
        ResultSlotId slotId,
        out ResultSlot<TResult> slot)
    {
        RunnableGraph graph = LocalGraphBuilder.Close(
            shape,
            [new LocalSlotRequest(slotId, shape.Stages.Count - 1, null)]);

        slot = ResultSlot<TResult>.Create(slotId, graph.Fingerprint, graph.AuthoringNonce);

        return graph;
    }


    /// <summary>Invokes a sink-factory lambda and rejects a <see langword="null"/> result.</summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The lambda to invoke, already known to be non-null.</param>
    /// <returns>The sink the lambda chose.</returns>
    /// <exception cref="ArgumentException"><paramref name="sink"/> returned <see langword="null"/>.</exception>
    private static SinkWithResult<T, TResult> ResolveSink<TResult>(
        Func<SinkFactory<T>, SinkWithResult<T, TResult>> sink) =>
        sink(SinkFactory<T>.Instance) ??
        throw new ArgumentException(
            $"The sink factory returned null, and a graph is closed by a sink. Return a sink from the {nameof(SinkFactory<T>)} the lambda receives, such as 's => s.Aggregate(seed, folder)'.",
            nameof(sink));

    /// <summary>Validates a slot name as a <see cref="ResultSlotId"/>.</summary>
    /// <param name="slotName">The candidate name.</param>
    /// <returns>The validated identifier.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="slotName"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="slotName"/> is not a valid identifier segment.</exception>
    /// <remarks>
    /// <see cref="ResultSlotId"/> owns the segment grammar and the diagnostic for breaking it, so the
    /// message is reused verbatim rather than restated; restating it would let the two drift apart. Only
    /// the parameter name is corrected, because the author wrote a slot name and not a
    /// <see cref="ResultSlotId"/> value.
    /// </remarks>
    private static ResultSlotId ParseSlotName(string slotName) =>
        LocalOptionGuard.SlotName(slotName, nameof(slotName));

    /// <summary>Closes this source with a result-bearing sink under a candidate name.</summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The sink terminating the graph.</param>
    /// <param name="slotName">The candidate slot name.</param>
    /// <param name="slot">When this method returns, the slot that resolves the result.</param>
    /// <returns>The closed graph.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="sink"/> or <paramref name="slotName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException"><paramref name="slotName"/> is not a valid identifier segment.</exception>
    private RunnableGraph CloseWithResult<TResult>(
        SinkWithResult<T, TResult> sink,
        string slotName,
        out ResultSlot<TResult> slot)
    {
        ArgumentNullException.ThrowIfNull(sink);

        return Close(sink, ParseSlotName(slotName), out slot);
    }

    /// <summary>Closes this source with a sink a factory lambda chooses, under a candidate name.</summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The lambda choosing the sink.</param>
    /// <param name="slotName">The candidate slot name.</param>
    /// <param name="slot">When this method returns, the slot that resolves the result.</param>
    /// <returns>The closed graph.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="sink"/> or <paramref name="slotName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="slotName"/> is not a valid identifier segment, or <paramref name="sink"/> returned
    /// <see langword="null"/>.
    /// </exception>
    /// <remarks>
    /// The name is validated before the lambda runs. The lambda is the author's own code and may do
    /// anything, so an argument this method was always going to reject must not cost them a side effect
    /// first; a rejected call leaves the program exactly as it found it.
    /// </remarks>
    private RunnableGraph CloseWithFactory<TResult>(
        Func<SinkFactory<T>, SinkWithResult<T, TResult>> sink,
        string slotName,
        out ResultSlot<TResult> slot)
    {
        ArgumentNullException.ThrowIfNull(sink);

        ResultSlotId slotId = ParseSlotName(slotName);

        return Close(ResolveSink(sink), slotId, out slot);
    }

    /// <summary>Closes this source with a result-bearing sink under a validated name.</summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The sink terminating the graph.</param>
    /// <param name="slotId">The validated slot name.</param>
    /// <param name="slot">When this method returns, the slot that resolves the result.</param>
    /// <returns>The closed graph.</returns>
    /// <remarks>
    /// Every result-bearing overload funnels through here, which is what makes the tuple form and the
    /// <see langword="out"/> form produce byte-identical documents rather than merely similar ones.
    /// </remarks>
    private RunnableGraph Close<TResult>(
        SinkWithResult<T, TResult> sink,
        ResultSlotId slotId,
        out ResultSlot<TResult> slot)
    {
        return CloseShape(Shape.Concat(sink.Stages), slotId, out slot);
    }

    /// <summary>Closes this source with a named occurrence of a registered result-bearing stage.</summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The typed handle terminating the graph.</param>
    /// <param name="occurrenceName">The candidate occurrence name.</param>
    /// <param name="parameters">The occurrence's payload.</param>
    /// <param name="slotName">The candidate slot name.</param>
    /// <param name="slot">When this method returns, the slot that resolves the result.</param>
    /// <returns>The closed graph.</returns>
    /// <remarks>
    /// <para>
    /// Both result-bearing registered overloads funnel through here, which is what makes the tuple form
    /// and the <see langword="out"/> form produce byte-identical documents rather than merely similar ones.
    /// </para>
    /// <para>
    /// The slot binds to the graph's authoring nonce exactly as a lambda graph's does, because this is
    /// still a <see cref="RunnableGraph"/>: it is a pipeline that binds slots by fingerprint and lineage
    /// without a nonce, and turning this graph into one is <see cref="RunnableGraph.AsPipeline"/>'s
    /// business. Carrying the nonce here costs a fully registered graph nothing and keeps one rule for
    /// every runnable graph.
    /// </para>
    /// </remarks>
    private RunnableGraph CloseWithRegisteredResult<TResult>(
        RegisteredSinkWithResult<T, TResult> sink,
        string occurrenceName,
        CanonicalJsonValue parameters,
        string slotName,
        out ResultSlot<TResult> slot)
    {
        ArgumentNullException.ThrowIfNull(sink);

        ResultSlotId slotId = ParseSlotName(slotName);

        return CloseShape(
            Shape.Append(RegisteredAttachment.Occurrence(sink.Specification, occurrenceName, parameters)),
            slotId,
            out slot);
    }
}

/// <summary>
/// The factories that start a source.
/// </summary>
/// <remarks>
/// <para>
/// The factories live on a non-generic companion class so that the element type is inferred from the
/// argument wherever it can be, per ADR 0004 section 1.
/// </para>
/// <para>
/// <see cref="UnzipTo{TLeft, TRight}"/> lives here too, and is the one junction call on a source that is an
/// extension method rather than an instance method. Every other one applies to a source of any element type
/// and is therefore an instance method, per ADR 0004 section 2; an unzip applies only to a source of pairs,
/// and a receiver constrained to one shape of element type is exactly what an instance method cannot say.
/// It reads identically at the call site.
/// </para>
/// </remarks>
public static class Source
{
    /// <summary>Starts a source that emits the elements of an in-memory sequence.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="elements">The sequence to emit.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="elements"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The sequence is captured by reference and is not enumerated here: building a graph starts no work,
    /// and a source is a description of where elements come from, not a snapshot of them. When the sequence
    /// is enumerated, and how often, is the local runtime's semantics to define.
    /// </remarks>
    public static Source<T> From<T>(IEnumerable<T> elements)
    {
        ArgumentNullException.ThrowIfNull(elements);

        return new Source<T>(LocalStageChain.Of(LocalStageDescriptor.FromEnumerable(elements)));
    }

    /// <summary>Starts a source that emits nothing and completes at once.</summary>
    /// <typeparam name="T">The element type the graph downstream of it is typed by.</typeparam>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <remarks>
    /// A real source rather than a degenerate one: it is what a graph is tested against when the question
    /// is what happens with no elements at all, and it is what a conditional composition yields when there
    /// is nothing to read. A run of it completes successfully, and an aggregate resolves its seed.
    /// </remarks>
    public static Source<T> Empty<T>() => new(LocalStageChain.Of(LocalStageDescriptor.Empty()));

    /// <summary>Starts a source that emits one element and completes.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="value">The element to emit, which may be <see langword="null"/>.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <remarks>
    /// The element is captured as it is given and emitted once per run, so two runs of one graph deliver
    /// the same instance twice. What that instance is, and whether handing it to two runs is safe, is the
    /// author's to decide, exactly as for a sequence.
    /// </remarks>
    [SuppressMessage(
        "Naming",
        "CA1720:Identifier contains type name",
        Justification = "The type name it collides with is the CLR name of the 32-bit floating-point type, which nothing on a source factory could be mistaken for. 'Single' is what a stream of one element is called in every vocabulary this API borrows from, LINQ's own included, and renaming it to avoid an alias for 'float' would cost every reader the word they came looking for.")]
    public static Source<T> Single<T>(T value) => new(LocalStageChain.Of(LocalStageDescriptor.Single(value)));

    /// <summary>Starts a source that emits one element a declared number of times.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="value">The element to emit, which may be <see langword="null"/>.</param>
    /// <param name="count">How many times to emit it; zero or more.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
    /// <remarks>
    /// The count is required and there is no endless spelling. A source that never ends is
    /// <see cref="Unfold{TState, T}"/>, whose author writes the logic that ends it, and either shape is
    /// bounded downstream by <c>Take</c>; a repeat with no count would be an endless stream nobody had to
    /// ask for.
    /// </remarks>
    public static Source<T> Repeat<T>(T value, int count) =>
        new(LocalStageChain.Of(LocalStageDescriptor.Repeat(value, LocalOptionGuard.Count(count, nameof(count)))));

    /// <summary>Starts a source that emits a run of consecutive integers.</summary>
    /// <param name="start">The first integer to emit.</param>
    /// <param name="count">How many integers to emit; zero or more.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="count"/> is negative, or the last integer would be past
    /// <see cref="int.MaxValue"/>.
    /// </exception>
    /// <remarks>
    /// The one source with no behavior at all: a document states both numbers, so a range is the same
    /// stream wherever it is run. The elements are <paramref name="start"/> through
    /// <c>start + count - 1</c>, ascending.
    /// </remarks>
    public static Source<int> Range(int start, int count) =>
        new(LocalStageChain.Of(LocalStageDescriptor.Range(start, LocalOptionGuard.Range(start, count))));

    /// <summary>Starts a source that emits the value of one task.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="task">The task whose value is the single element.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="task"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// The task is awaited once per run and its value emitted as one element. A task that has already
    /// finished replays its value into every run, because a completed task is a value and not an event; a
    /// task that fails faults the run with the exception it failed with, unwrapped from the
    /// <see cref="AggregateException"/> a task carries it in. A task that was cancelled faults the run too,
    /// with the <see cref="OperationCanceledException"/> it carries: the run itself was not asked to stop,
    /// and a source that cannot produce its element is a source that failed, whatever the reason.
    /// </para>
    /// <para>
    /// A run whose task has not finished waits inside its first pull, exactly as it would for any source
    /// that takes a long time to produce its first element; cancellation is observed between elements, so
    /// such a run stops once the task settles.
    /// </para>
    /// </remarks>
    public static Source<T> FromTask<T>(Task<T> task)
    {
        ArgumentNullException.ThrowIfNull(task);

        return new Source<T>(LocalStageChain.Of(LocalStageDescriptor.FromTask(task)));
    }

    /// <summary>Starts a source that fails without emitting anything.</summary>
    /// <typeparam name="T">The element type the graph downstream of it is typed by.</typeparam>
    /// <param name="exception">The failure every run of the graph reports.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="exception"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The run faults with this very instance, so a caller that compares exceptions by identity sees the
    /// one it supplied. The instance is shared by every run of the graph, which is what makes that identity
    /// meaningful and also means its stack trace is the one of the most recent throw.
    /// </remarks>
    public static Source<T> Failed<T>(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        return new Source<T>(LocalStageChain.Of(LocalStageDescriptor.Failed(exception)));
    }

    /// <summary>Starts a source that produces its elements from a state it carries.</summary>
    /// <typeparam name="TState">The type of the state carried between elements.</typeparam>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="seed">The state the first call receives.</param>
    /// <param name="generator">The function producing the next element and the next state.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="generator"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// Every run starts from <paramref name="seed"/> again, so two runs of one graph produce the same
    /// elements; state the generator keeps outside its parameters is the author's to keep fresh, exactly as
    /// for every other lambda.
    /// </para>
    /// <para>
    /// The generator decides when the source ends by returning <see langword="false"/>, and nothing else
    /// bounds it: an unfold that never says so is an endless source, which is a legitimate thing to write
    /// and is bounded downstream by <c>Take</c>. An exception the generator throws faults the run, as any
    /// stage's does.
    /// </para>
    /// </remarks>
    public static Source<T> Unfold<TState, T>(TState seed, UnfoldGenerator<TState, T> generator)
    {
        ArgumentNullException.ThrowIfNull(generator);

        return new Source<T>(LocalStageChain.Of(LocalStageDescriptor.Unfold(seed, generator)));
    }

    /// <summary>Starts a source that emits the elements of an asynchronous sequence.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="elements">The sequence to emit.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="elements"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// One enumeration per run, opened with that run's own cancellation token — the token
    /// <c>WithCancellation</c> would have supplied, handed over directly because the run is what has one.
    /// The enumeration is disposed on every terminal path, and disposing it means awaiting its
    /// <c>DisposeAsync</c> rather than starting it: a sequence that closes a file or a subscription has not
    /// closed it until that task finishes, and the run does not end before it has.
    /// </para>
    /// <para>
    /// The run waits for each element inside its pull, on the segment's own dedicated thread, exactly as it
    /// waits for a slow synchronous sequence. That is the blocking-source model this runtime is built on
    /// and it is stated rather than hidden: an asynchronous source buys ordinary <c>await</c> inside the
    /// author's sequence, not a run that occupies no thread.
    /// </para>
    /// <para>
    /// Cancellation is cooperative. A sequence that ignores the token it was opened with delays the run's
    /// stop until it next yields or finishes, which is the same slow-source rule a blocking synchronous
    /// sequence follows; an element the sequence was already producing when the run was cancelled is
    /// awaited to its outcome rather than abandoned.
    /// </para>
    /// </remarks>
    public static Source<T> FromAsyncEnumerable<T>(IAsyncEnumerable<T> elements)
    {
        ArgumentNullException.ThrowIfNull(elements);

        LocalAsyncCursorFactory open = token => new LocalAsyncCursor<T>(elements.GetAsyncEnumerator(token));

        return new Source<T>(LocalStageChain.Of(LocalStageDescriptor.FromAsyncEnumerable(open)));
    }

    /// <summary>Starts a source that emits one element a factory produces.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="factory">The function producing the element.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="factory"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// The factory is invoked once per run and never while the graph is being built, which is the whole
    /// difference from <see cref="Single{T}"/>: a single source captures a value two runs then share, and
    /// this one produces a fresh value for every run. Building the graph starts nothing at all.
    /// </para>
    /// <para>
    /// An exception the factory throws faults the run with that very instance, unwrapped, exactly as a
    /// stage's does. It runs inside the run's first pull, on the segment's own thread, so a factory that
    /// takes a long time is an ordinary slow source.
    /// </para>
    /// </remarks>
    public static Source<T> FromFactory<T>(Func<T> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);

        return new Source<T>(LocalStageChain.Of(LocalStageDescriptor.FromFactory(factory)));
    }

    /// <summary>Starts a source that emits one element an asynchronous factory produces.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="factory">The function producing the element.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="factory"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// The asynchronous sibling of <see cref="FromFactory{T}"/> and the deferred sibling of
    /// <see cref="FromTask{T}"/>: a task is started once and replayed into every run, and this is started
    /// once per run. That is the difference an author reaches for when the work must not begin until the
    /// graph does, or must not be shared between two runs.
    /// </para>
    /// <para>
    /// The factory receives the run's own token and the run waits for its task inside the first pull. A
    /// task that fails faults the run with its exception unwrapped; a task cancelled by anything other than
    /// the run's own token faults it too, because a source that cannot produce its element is a source that
    /// failed, whatever the reason.
    /// </para>
    /// </remarks>
    public static Source<T> FromAsyncFactory<T>(Func<CancellationToken, Task<T>> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);

        return new Source<T>(LocalStageChain.Of(LocalStageDescriptor.FromAsyncFactory(factory)));
    }

    /// <summary>Starts a source that emits nothing and never ends of its own accord.</summary>
    /// <typeparam name="T">The element type the graph downstream of it is typed by.</typeparam>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <remarks>
    /// <para>
    /// The opposite of <see cref="Empty{T}"/> along the one axis that matters: an empty source completes at
    /// once with no elements, and this one has no elements and never completes. It is what a graph is
    /// tested against when the question is what stopping does to a run that would otherwise wait forever,
    /// and it is what a conditional composition yields when a stream is meant to stay open.
    /// </para>
    /// <para>
    /// A run of it waits rather than spins: the thread is parked until the run is stopped, and it costs no
    /// processor time at all. Shutting the run down completes it successfully with whatever downstream had
    /// accumulated, which for a graph whose only source is this one is an aggregate's seed; cancelling it
    /// cancels the run and resolves nothing.
    /// </para>
    /// </remarks>
    public static Source<T> Never<T>() => new(LocalStageChain.Of(LocalStageDescriptor.Never()));

    /// <summary>Starts a source that emits the number of every tick of an interval.</summary>
    /// <param name="initialDelay">How long after the run starts tick zero is due.</param>
    /// <param name="interval">How long after each tick the next one is due.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="initialDelay"/> or <paramref name="interval"/> is not positive.
    /// </exception>
    /// <remarks>
    /// <para>
    /// <b>A tick is a clock, not a queue.</b> The source is pulled like every other, so a tick that comes
    /// due while the run is still busy with the previous one is <i>skipped</i> rather than queued: nothing
    /// accumulates, and the next element is the number of the tick that is due now. That is the same
    /// honesty the reminder trigger has about missed ticks, and it is the only answer a bounded runtime can
    /// give — a queue of moments that have already passed grows without bound whenever the consumer is
    /// slower than the interval.
    /// </para>
    /// <para>
    /// <b>The element is the tick's number, and the number counts ticks that were due rather than ticks
    /// that were emitted.</b> Tick <c>n</c> is due at <c>initialDelay + n * interval</c> after the run
    /// started, so a consumer that missed three ticks receives a number three higher than it otherwise
    /// would and can see that it fell behind. Akka's tick source emits a fixed element the author supplies,
    /// which is honest for a source that never skips; here the skipping is the contract, and a counter
    /// jumping silently would hide the one thing worth reading. A stream of a constant is
    /// <c>Tick(d, i).Select(_ => value)</c> and a stream of timestamps is one more <c>Select</c>.
    /// </para>
    /// <para>
    /// The source is endless, is not durable, and belongs to its run exactly as the .NET timer adapter's
    /// belongs to its activation: two runs of one graph tick independently, a run that ends stops ticking,
    /// and nothing is replayed. It is bounded downstream by <c>Take</c>, <c>TakeWithin</c>, or a stop.
    /// The clock is the host's <see cref="TimeProvider"/>, resolved when the graph is materialized, which
    /// is what makes a deterministic test of a ticking graph possible; a pause holds the elements and not
    /// the clock, so the ticks a pause covers are missed ticks like any others and are skipped like any
    /// others.
    /// </para>
    /// </remarks>
    public static Source<long> Tick(TimeSpan initialDelay, TimeSpan interval) =>
        new(LocalStageChain.Of(LocalStageDescriptor.Tick(
            LocalOptionGuard.Duration(initialDelay, nameof(initialDelay)),
            LocalOptionGuard.Duration(interval, nameof(interval)))));

    /// <summary>Starts a source that repeats an in-memory sequence for as long as it is pulled.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="elements">The sequence to repeat.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="elements"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// Endless by construction and bounded by the author: a cycle ends where a <c>Take</c>, a
    /// <c>TakeWhile</c>, or a first-element sink downstream of it ends, and nowhere else. That is the same
    /// bargain <see cref="Unfold{TState, T}"/> makes, and it is why <see cref="Repeat{T}"/> — which counts —
    /// is a different factory rather than an overload.
    /// </para>
    /// <para>
    /// Each lap enumerates the sequence again from the start, with an enumerator of its own that is
    /// released at the end of the lap and again if the run stops in the middle of one. A sequence that
    /// cannot be enumerated twice is therefore the author's to know about, exactly as it is for a sequence
    /// handed to two runs.
    /// </para>
    /// <para>
    /// A lap that produces nothing faults the run with an <see cref="InvalidOperationException"/>. A cycle
    /// over an empty sequence is not an empty stream: it is a loop that emits nothing and never ends, and a
    /// run that hung on one would be indistinguishable from a run waiting on a slow source.
    /// </para>
    /// </remarks>
    public static Source<T> Cycle<T>(IEnumerable<T> elements)
    {
        ArgumentNullException.ThrowIfNull(elements);

        return new Source<T>(LocalStageChain.Of(LocalStageDescriptor.Cycle(elements)));
    }

    /// <summary>Starts a source that produces its elements asynchronously from a state it carries.</summary>
    /// <typeparam name="TState">The type of the state carried between elements.</typeparam>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="seed">The state the first call receives.</param>
    /// <param name="generator">The function producing the next step, or nothing to end the source.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="generator"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// The asynchronous sibling of <see cref="Unfold{TState, T}"/>, with the same contract: every run
    /// starts from <paramref name="seed"/> again, the generator decides when the source ends, and an
    /// endless one is bounded downstream by <c>Take</c>. The generator receives the run's own token, and
    /// the run waits for each step inside its pull.
    /// </para>
    /// <para>
    /// The shape differs because it has to. An <see langword="async"/> method has no <see langword="out"/>
    /// parameters, so the try-shape that makes <see cref="Unfold{TState, T}"/> infer both of its type
    /// arguments is unavailable here; a step is returned instead, and <see langword="null"/> ends the
    /// source. The cost is that both type arguments have to be written at the call site, because a
    /// conditional expression over a step and <see langword="null"/> has no natural type for inference to
    /// start from. Written that way, everything inside the lambda is target-typed and the spelling stays
    /// short:
    /// </para>
    /// <code>
    /// Source.UnfoldAsync&lt;int, string&gt;(1, async (state, token) =&gt;
    ///     state &lt;= 1024 ? new(await RenderAsync(state, token), state * 2) : null);
    /// </code>
    /// </remarks>
    public static Source<T> UnfoldAsync<TState, T>(TState seed, AsyncUnfoldGenerator<TState, T> generator)
    {
        ArgumentNullException.ThrowIfNull(generator);

        return new Source<T>(LocalStageChain.Of(LocalStageDescriptor.UnfoldAsync(seed, generator)));
    }

    /// <summary>Starts a source that emits what producers offer to a bounded queue of its own.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="options">The capacity of the queue and what it does when it is full.</param>
    /// <param name="controlName">The author-stable name to expose the queue under.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> or <paramref name="controlName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="BufferOptions.Capacity"/> is below one, or
    /// <see cref="BufferOptions.OverflowPolicy"/> is not a declared member of its enumeration.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="controlName"/> is not a valid <see cref="ResultSlotId"/>.
    /// </exception>
    /// <remarks>
    /// <para>
    /// Every other source is pulled: the run asks for the next element and the source produces it. A
    /// producer that pushes cannot be asked, so this source owns a bounded queue and the author's producers
    /// offer into it. The options are a buffer's options for the same reason: a full ingress queue and a
    /// full buffer are one situation seen from the two ends of a graph.
    /// </para>
    /// <para>
    /// The queue is a per-run control rather than part of the graph, and it is reached by name. Closing the
    /// graph declares a result slot under <paramref name="controlName"/>, <see cref="RunnableGraph.Control{TControl}"/>
    /// turns that name back into a typed <see cref="ResultSlot{TResult}"/> of
    /// <see cref="IIngressQueue{T}"/>, and <see cref="RunHandle.GetValueAsync{TResult}"/> resolves it
    /// against one run. The name is written here rather than at <c>To</c> because the queue belongs to a
    /// stage at the head of the chain, and a chain has one closing call at its other end.
    /// </para>
    /// <para>
    /// The control resolves at the start of a run, not at its end: producers push into a run that is
    /// already running. Two runs of one graph therefore have two queues, and an element offered to one is
    /// never seen by the other.
    /// </para>
    /// </remarks>
    public static Source<T> Queue<T>(BufferOptions options, string controlName)
    {
        ArgumentNullException.ThrowIfNull(options);

        BufferOptions bounded = LocalOptionGuard.Buffer(options, nameof(options));
        ResultSlotId control = LocalOptionGuard.SlotName(controlName, nameof(controlName));
        Func<LocalIngressQueue, object> facade = static queue => new IngressQueue<T>(queue);

        return new Source<T>(LocalStageChain.Of(
            LocalStageDescriptor.Queue(bounded, control, typeof(IIngressQueue<T>), facade)));
    }

    /// <summary>Starts a source that emits the elements of a channel the author owns.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="reader">The reader to drain.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// The run reads until the channel is completed and empty, and then completes; a channel completed with
    /// an exception faults the run with that exception, unwrapped. The channel's own bound is the
    /// backpressure, and the run neither completes the reader nor resets it, because a run does not own
    /// what it was handed.
    /// </para>
    /// <para>
    /// This is the one source that is not fresh per run, and the honest consequence is stated rather than
    /// hidden: a reader is not re-enumerable, so two runs of one graph <em>compete</em> for its elements.
    /// Each element goes to exactly one of them, no element is lost or duplicated, and which run gets which
    /// element is not defined. An author who wants two independent streams creates two channels;
    /// <see cref="Queue{T}"/> is the source that gives every run an ingress of its own.
    /// </para>
    /// </remarks>
    public static Source<T> FromChannel<T>(ChannelReader<T> reader)
    {
        ArgumentNullException.ThrowIfNull(reader);

        return new Source<T>(LocalStageChain.Of(LocalStageDescriptor.FromChannel(reader)));
    }

    /// <summary>Starts a source at one named occurrence of a registered stage.</summary>
    /// <typeparam name="T">The element type the registered stage produces.</typeparam>
    /// <param name="source">The typed handle of the registered stage.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="source"/> or <paramref name="occurrenceName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// The deployable counterpart of <see cref="From{T}(IEnumerable{T})"/>: where that one captures a
    /// sequence this process happens to hold, this one names a stage a catalog resolves, so the document
    /// says everything about where the elements come from. Building a graph still starts no work.
    /// </remarks>
    public static Source<T> FromRegistered<T>(
        RegisteredSource<T> source,
        string occurrenceName,
        CanonicalJsonValue parameters)
    {
        ArgumentNullException.ThrowIfNull(source);

        return new Source<T>(LocalStageChain.Of(
            RegisteredAttachment.Occurrence(source.Specification, occurrenceName, parameters)));
    }

    /// <summary>Closes a source of pairs by sending each half of every pair to a branch of its own.</summary>
    /// <typeparam name="TLeft">The element type of the left half.</typeparam>
    /// <typeparam name="TRight">The element type of the right half.</typeparam>
    /// <param name="source">The source of pairs to split.</param>
    /// <param name="left">The branch the left halves take.</param>
    /// <param name="right">The branch the right halves take.</param>
    /// <returns>The closed graph.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="source"/>, <paramref name="left"/>, or <paramref name="right"/> is
    /// <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">Both branches declare a result under one name.</exception>
    /// <remarks>
    /// <para>
    /// The one fan-out whose legs are differently typed, and the reason its arity is fixed at two rather
    /// than open like a broadcast's: the halves of a pair are two, and each one's type is a type argument.
    /// Both halves of every pair are delivered, so this junction is a broadcast in its flow control — a
    /// branch that stops consuming holds the other one up — and a split in its elements.
    /// </para>
    /// <para>
    /// The two projections are ordinary functions of a pair and never enter the document, which is what
    /// makes the halves' element types the C# compiler's business rather than the graph compiler's, and what
    /// makes an unzipped graph <c>nondeployable</c> like every other local one.
    /// </para>
    /// </remarks>
    public static RunnableGraph UnzipTo<TLeft, TRight>(
        this Source<(TLeft Left, TRight Right)> source,
        Branch<TLeft> left,
        Branch<TRight> right)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);

        int position = source.Shape.Stages.Count;
        LocalGraphShape shape = source.Shape.Split(
            LocalStageDescriptor.Unzip(
                (Func<(TLeft Left, TRight Right), TLeft>)(row => row.Left),
                (Func<(TLeft Left, TRight Right), TRight>)(row => row.Right)),
            [LocalVocabulary.LeftPort, LocalVocabulary.RightPort],
            [left.Stages, right.Stages]);

        List<LocalSlotRequest> slots = [];
        int leftTerminal = position + left.Stages.Count;
        int rightTerminal = leftTerminal + right.Stages.Count;

        if (left.SlotName is { } leftName)
        {
            slots.Add(new LocalSlotRequest(leftName, leftTerminal, left.Binding));
        }

        if (right.SlotName is { } rightName)
        {
            slots.Add(new LocalSlotRequest(rightName, rightTerminal, right.Binding));
        }

        return LocalGraphBuilder.Close(shape, slots);
    }
}
