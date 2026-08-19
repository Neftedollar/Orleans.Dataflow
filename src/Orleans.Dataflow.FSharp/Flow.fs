namespace Orleans.Dataflow.FSharp

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Orleans.Dataflow.Authoring

// Orleans.Dataflow itself is deliberately not opened: see the note in Source.fs. The option records and
// the enumerations an operator is configured by are therefore written qualified.

/// <summary>Constructs and composes reusable element transformations.</summary>
/// <remarks>
/// <para>
/// Every function answers a new immutable value and touches nothing it was given. The delegates an author
/// writes are stored typed — the runtime's own delegate adapter is the single owner of how a typed lambda
/// meets a boxed element — so this module converts an F# function to its <see cref="T:System.Func`2"/>
/// shape and nothing more, exactly as the C# facade stores what it receives. One named function per
/// operation, never an overload family: overloads are what degrade F# diagnostics to a candidate dump.
/// </para>
/// <para>
/// Where the C# facade overloads one name, this module spells the difference: the effect is in the name
/// (<c>map</c>, <c>mapTask</c>, <c>mapValueTask</c>, <c>mapAsync</c>), the ordering is in the name
/// (<c>mapTask</c> against <c>mapTaskUnordered</c>), and a form that needs a fallback element is a
/// different word rather than a third argument on the same one. Options come first and the author's
/// function last, so a call reads as configuration followed by behavior.
/// </para>
/// <para>
/// Every option record and every enumeration an operator is configured by is the C# package's own. This
/// package mirrors no configuration type, because a second record of the same fields would be a second
/// thing to keep in step with the runtime for no gain in either safety or reading.
/// </para>
/// </remarks>
[<RequireQualifiedAccess>]
module Flow =

    /// <summary>The flow that changes nothing: it contributes no occurrence to any graph.</summary>
    /// <remarks>
    /// The unit of <see cref="M:Orleans.Dataflow.FSharp.Flow.andThen``3"/>, and the F# spelling of the C#
    /// <c>Flow.For&lt;'T&gt;()</c> anchor: a source composed through it is the source it was, byte for byte.
    /// </remarks>
    [<GeneralizableValue>]
    let identity<'T> : Flow<'T, 'T> = Flow<'T, 'T>(LocalStageChain.Empty)

    /// <summary>Transforms every element through a function.</summary>
    /// <param name="mapping">The function applied to each element.</param>
    /// <returns>The flow.</returns>
    let map (mapping: 'In -> 'Out) : Flow<'In, 'Out> =
        Flow<'In, 'Out>(LocalStageChain.Of(LocalStageDescriptor.Select(Func<'In, 'Out> mapping)))

    /// <summary>Keeps the elements a predicate answers true for.</summary>
    /// <param name="predicate">The predicate deciding each element.</param>
    /// <returns>The flow.</returns>
    let filter (predicate: 'T -> bool) : Flow<'T, 'T> =
        Flow<'T, 'T>(LocalStageChain.Of(LocalStageDescriptor.Where(Func<'T, bool> predicate)))

    /// <summary>Transforms and filters in one step, keeping the elements a function answers a value for.</summary>
    /// <param name="chooser">The function answering the transformed element, or nothing to drop it.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// <para>
    /// One occurrence and one call of <paramref name="chooser"/> per element, because it is the flattening
    /// stage read at its degenerate size: the C# facade states that "a function answering an empty sequence
    /// drops its element, which is what makes filtering a special case of flattening rather than a second
    /// operator", and an optional value is exactly a sequence of nought or one. The document says
    /// <c>select-many</c>, which is what actually happens, rather than a fused stage this vocabulary does
    /// not have.
    /// </para>
    /// <para>
    /// Nothing is allocated for a dropped element — the empty sequence is the shared empty array — and one
    /// single-element array is allocated for a kept one. A dedicated stage would save that array; it does
    /// not exist in the vocabulary yet, and inventing one here would be a spelling only F# graphs could
    /// have.
    /// </para>
    /// </remarks>
    let choose (chooser: 'In -> 'Out voption) : Flow<'In, 'Out> =
        Flow<'In, 'Out>(
            LocalStageChain.Of(
                LocalStageDescriptor.SelectMany(
                    Func<'In, IEnumerable<'Out>>(fun element ->
                        match chooser element with
                        | ValueSome chosen -> [| chosen |] :> IEnumerable<'Out>
                        | ValueNone -> Array.empty<'Out> :> IEnumerable<'Out>))))

    /// <summary>Transforms and filters in one step, over the reference-typed option.</summary>
    /// <param name="chooser">The function answering the transformed element, or nothing to drop it.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// <see cref="M:Orleans.Dataflow.FSharp.Flow.choose``2"/> in every respect but the option type, and a
    /// separate function for the reason the two option types are two types: which of them an author's
    /// function answers is a decision about allocation, and inferring it away is how a hot path acquires an
    /// allocation nobody chose. This one costs one option per element; the value-typed one costs none.
    /// </remarks>
    let chooseOption (chooser: 'In -> 'Out option) : Flow<'In, 'Out> =
        Flow<'In, 'Out>(
            LocalStageChain.Of(
                LocalStageDescriptor.SelectMany(
                    Func<'In, IEnumerable<'Out>>(fun element ->
                        match chooser element with
                        | Some chosen -> [| chosen |] :> IEnumerable<'Out>
                        | None -> Array.empty<'Out> :> IEnumerable<'Out>))))

    /// <summary>Replaces every element with the sequence a function answers, in order.</summary>
    /// <param name="mapping">The function answering one sequence per element.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// Concat-map, and named for what <c>List.collect</c> is named for: one inner sequence is read to its
    /// end before the next element is asked for, so the result is a function of the input alone. The inner
    /// sequence is read one element at a time and never collected, and a function answering
    /// <see langword="null"/> fails the run — an element that produces nothing is an empty sequence, and
    /// reading one meaning into the other would hide a mistake that costs elements.
    /// </remarks>
    let collect (mapping: 'In -> seq<'Out>) : Flow<'In, 'Out> =
        Flow<'In, 'Out>(
            LocalStageChain.Of(LocalStageDescriptor.SelectMany(Func<'In, IEnumerable<'Out>> mapping)))

    /// <summary>Merges the sequences of several elements at once, unordered across them.</summary>
    /// <param name="options">The greatest number of inner sequences open at one time.</param>
    /// <param name="mapping">The function answering one sequence per element.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The other half of flattening: emission is unordered across inner sequences, and the order of each
    /// inner sequence is preserved. A slot is freed when a sequence ends rather than when it produces one
    /// more element, so an empty inner sequence frees its slot at once and an endless one holds it for the
    /// life of the run. An ordinary sequence is advanced on the segment's own thread, so an inner sequence
    /// that blocks holds up every other sequence open beside it —
    /// <see cref="M:Orleans.Dataflow.FSharp.Flow.mergeMapAsyncEnumerable``2"/> is what that is for.
    /// </remarks>
    let mergeMap
        (options: Orleans.Dataflow.ParallelismOptions)
        (mapping: 'In -> seq<'Out>)
        : Flow<'In, 'Out> =
        Flow<'In, 'Out>(
            LocalStageChain.Of(
                LocalStageDescriptor.MergeMap(
                    LocalOptionGuard.Parallelism(options, nameof options),
                    Func<'In, IEnumerable<'Out>> mapping)))

    /// <summary>Merges the asynchronous sequences of several elements at once, unordered across them.</summary>
    /// <param name="options">The greatest number of inner sequences open at one time.</param>
    /// <param name="mapping">The function answering one asynchronous sequence per element.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The same node and the same machinery as <see cref="M:Orleans.Dataflow.FSharp.Flow.mergeMap``2"/> —
    /// how an author's sequence produces its elements is behavior in the way the body of a mapping function
    /// is — with the one difference that matters at run time: an inner sequence that waits does not hold up
    /// the ones open beside it. The sequences are opened with the run's own token and every one of them is
    /// released, awaited, on every terminal path.
    /// </remarks>
    let mergeMapAsyncEnumerable
        (options: Orleans.Dataflow.ParallelismOptions)
        (mapping: 'In -> IAsyncEnumerable<'Out>)
        : Flow<'In, 'Out> =
        Flow<'In, 'Out>(
            LocalStageChain.Of(
                LocalStageDescriptor.MergeMap(
                    LocalOptionGuard.Parallelism(options, nameof options),
                    Func<'In, IAsyncEnumerable<'Out>> mapping)))

    /// <summary>Transforms every element through a task-returning function.</summary>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="mapping">The callback applied to each element, which receives the run's own token.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// Results are emitted in the order their elements arrived, so a slow callback holds up emission but
    /// not admission. The token is the run's: it is cancelled when the run is cancelled and when anything
    /// in the run fails, and a callback that ignores it is a callback the run has to wait for.
    /// </remarks>
    let mapTask
        (options: Orleans.Dataflow.ParallelismOptions)
        (mapping: 'In -> CancellationToken -> Task<'Out>)
        : Flow<'In, 'Out> =
        Flow<'In, 'Out>(
            LocalStageChain.Of(
                LocalStageDescriptor.SelectAsync(
                    LocalOptionGuard.Parallelism(options, nameof options),
                    Func<'In, CancellationToken, Task<'Out>>(fun element token -> mapping element token))))

    /// <summary>Transforms every element through a task-returning function, emitting in completion order.</summary>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="mapping">The callback applied to each element, which receives the run's own token.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The same bounds as <see cref="M:Orleans.Dataflow.FSharp.Flow.mapTask``2"/> with one difference stated
    /// in the name: a result is emitted as soon as its callback finishes, so the output order is the order
    /// the callbacks completed in and not the order the elements arrived in.
    /// </remarks>
    let mapTaskUnordered
        (options: Orleans.Dataflow.ParallelismOptions)
        (mapping: 'In -> CancellationToken -> Task<'Out>)
        : Flow<'In, 'Out> =
        Flow<'In, 'Out>(
            LocalStageChain.Of(
                LocalStageDescriptor.SelectAsyncUnordered(
                    LocalOptionGuard.Parallelism(options, nameof options),
                    Func<'In, CancellationToken, Task<'Out>>(fun element token -> mapping element token))))

    /// <summary>Transforms every element through a value-task-returning function.</summary>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="mapping">The callback applied to each element, which receives the run's own token.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// A distinct stage rather than a flavour of <see cref="M:Orleans.Dataflow.FSharp.Flow.mapTask``2"/>,
    /// because what an author wrote is what the document states. The runtime awaits each returned value task
    /// exactly once and never after reading its result.
    /// <see cref="T:System.Threading.Tasks.ValueTask`1"/> is not the default F# effect; this exists for
    /// explicit allocation-sensitive interop and nothing else.
    /// </remarks>
    let mapValueTask
        (options: Orleans.Dataflow.ParallelismOptions)
        (mapping: 'In -> CancellationToken -> ValueTask<'Out>)
        : Flow<'In, 'Out> =
        Flow<'In, 'Out>(
            LocalStageChain.Of(
                LocalStageDescriptor.SelectValueTaskAsync(
                    LocalOptionGuard.Parallelism(options, nameof options),
                    Func<'In, CancellationToken, ValueTask<'Out>>(fun element token -> mapping element token))))

    /// <summary>Transforms every element through a value-task-returning function, emitting in completion order.</summary>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="mapping">The callback applied to each element, which receives the run's own token.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The unordered spelling of <see cref="M:Orleans.Dataflow.FSharp.Flow.mapValueTask``2"/>, and the
    /// single-consumption rule applies unchanged.
    /// </remarks>
    let mapValueTaskUnordered
        (options: Orleans.Dataflow.ParallelismOptions)
        (mapping: 'In -> CancellationToken -> ValueTask<'Out>)
        : Flow<'In, 'Out> =
        Flow<'In, 'Out>(
            LocalStageChain.Of(
                LocalStageDescriptor.SelectValueTaskAsyncUnordered(
                    LocalOptionGuard.Parallelism(options, nameof options),
                    Func<'In, CancellationToken, ValueTask<'Out>>(fun element token -> mapping element token))))

    /// <summary>Transforms every element through an asynchronous computation.</summary>
    /// <param name="options">The greatest number of computations in flight at one time.</param>
    /// <param name="mapping">The computation built for each element.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The F# effect over the very stage <see cref="M:Orleans.Dataflow.FSharp.Flow.mapTask``2"/> writes —
    /// the document cannot tell them apart, because how a callback is spelled is behavior — with the run's
    /// own token starting the computation, so <c>Async.CancellationToken</c> inside it is the run's token.
    /// Results are emitted in the order their elements arrived.
    /// </remarks>
    let mapAsync
        (options: Orleans.Dataflow.ParallelismOptions)
        (mapping: 'In -> Async<'Out>)
        : Flow<'In, 'Out> =
        Flow<'In, 'Out>(
            LocalStageChain.Of(
                LocalStageDescriptor.SelectAsync(
                    LocalOptionGuard.Parallelism(options, nameof options),
                    Func<'In, CancellationToken, Task<'Out>>(fun element token ->
                        Bindings.asTask (mapping element) token))))

    /// <summary>Transforms every element through an asynchronous computation, emitting in completion order.</summary>
    /// <param name="options">The greatest number of computations in flight at one time.</param>
    /// <param name="mapping">The computation built for each element.</param>
    /// <returns>The flow.</returns>
    /// <remarks>The unordered spelling of <see cref="M:Orleans.Dataflow.FSharp.Flow.mapAsync``2"/>.</remarks>
    let mapAsyncUnordered
        (options: Orleans.Dataflow.ParallelismOptions)
        (mapping: 'In -> Async<'Out>)
        : Flow<'In, 'Out> =
        Flow<'In, 'Out>(
            LocalStageChain.Of(
                LocalStageDescriptor.SelectAsyncUnordered(
                    LocalOptionGuard.Parallelism(options, nameof options),
                    Func<'In, CancellationToken, Task<'Out>>(fun element token ->
                        Bindings.asTask (mapping element) token))))

    /// <summary>Emits the running state of a fold, one state per element.</summary>
    /// <param name="seed">The initial state, which is not emitted.</param>
    /// <param name="folder">The function combining the running state with the next element.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// One state out per element in, so a scan over three elements emits three states and an empty stream
    /// emits nothing at all. The seed is where the fold starts rather than something that happened, which is
    /// why it is not emitted; the state is allocated per run, so a flow carrying a scan starts from the seed
    /// in every graph it is composed into and in every run of each of them.
    /// </remarks>
    let scan (seed: 'State) (folder: 'State -> 'In -> 'State) : Flow<'In, 'State> =
        Flow<'In, 'State>(
            LocalStageChain.Of(LocalStageDescriptor.Scan(seed, Func<'State, 'In, 'State> folder)))

    /// <summary>Emits the running state of a fold whose state a durable scope can checkpoint.</summary>
    /// <param name="seed">The initial state, which is not emitted.</param>
    /// <param name="folder">The function combining the running state with the next element.</param>
    /// <param name="export">The projection of the running state into a canonical value.</param>
    /// <param name="restore">The projection of such a value back into a state.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The same stage and the same document as <see cref="M:Orleans.Dataflow.FSharp.Flow.scan``2"/> — a
    /// codec is behavior, so two graphs whose scans differ only in carrying one share a fingerprint — with a
    /// binding of three values instead of one. The codec is the author's because a state is a value of a
    /// type no document names, and it is what <see cref="M:Orleans.Dataflow.FSharp.Flow.durable``3"/>
    /// requires of a scan inside it.
    /// </remarks>
    let scanDurable
        (seed: 'State)
        (folder: 'State -> 'In -> 'State)
        (export: 'State -> Orleans.Dataflow.Serialization.CanonicalJsonValue)
        (restore: Orleans.Dataflow.Serialization.CanonicalJsonValue -> 'State)
        : Flow<'In, 'State> =
        Flow<'In, 'State>(
            LocalStageChain.Of(
                LocalStageDescriptor.Scan(
                    seed,
                    Func<'State, 'In, 'State> folder,
                    Func<objnull, Orleans.Dataflow.Serialization.CanonicalJsonValue>(fun state ->
                        export (unbox<'State> state)),
                    Func<Orleans.Dataflow.Serialization.CanonicalJsonValue, objnull>(fun value ->
                        box (restore value)))))

    /// <summary>Emits the running state of a fold whose function returns a task.</summary>
    /// <param name="seed">The initial state, which is not emitted.</param>
    /// <param name="folder">The callback combining the running state with the next element.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// Everything <see cref="M:Orleans.Dataflow.FSharp.Flow.scan``2"/> promises holds unchanged, and there
    /// is no concurrency to declare: the state the next element folds into is this fold's answer, so one
    /// fold runs at a time by construction. The wait happens on the segment's own thread, exactly where a
    /// synchronous fold's work would happen.
    /// </remarks>
    let scanTask
        (seed: 'State)
        (folder: 'State -> 'In -> CancellationToken -> Task<'State>)
        : Flow<'In, 'State> =
        Flow<'In, 'State>(
            LocalStageChain.Of(
                LocalStageDescriptor.ScanAsync(
                    seed,
                    Func<'State, 'In, CancellationToken, Task<'State>>(fun state element token ->
                        folder state element token))))

    /// <summary>Emits the running state of a fold whose function is an asynchronous computation.</summary>
    /// <param name="seed">The initial state, which is not emitted.</param>
    /// <param name="folder">The computation built from the running state and the next element.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The F# effect over the stage <see cref="M:Orleans.Dataflow.FSharp.Flow.scanTask``2"/> writes, with
    /// the run's own token starting the computation.
    /// </remarks>
    let scanAsync (seed: 'State) (folder: 'State -> 'In -> Async<'State>) : Flow<'In, 'State> =
        Flow<'In, 'State>(
            LocalStageChain.Of(
                LocalStageDescriptor.ScanAsync(
                    seed,
                    Func<'State, 'In, CancellationToken, Task<'State>>(fun state element token ->
                        Bindings.asTask (folder state element) token))))

    /// <summary>Passes a declared number of elements and ends the stream.</summary>
    /// <param name="count">How many elements to pass; zero or more.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// Reaching the bound completes the run the way the source running out does: everything upstream stops
    /// and is released, whatever it was holding is abandoned, and the run reports success.
    /// </remarks>
    let take (count: int) : Flow<'T, 'T> =
        Flow<'T, 'T>(LocalStageChain.Of(LocalStageDescriptor.Take(LocalOptionGuard.Count(count, nameof count))))

    /// <summary>Drops a declared number of elements.</summary>
    /// <param name="count">How many elements to drop; zero or more.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The dropped elements are still produced and still travel to this stage; skipping is not a way to
    /// avoid work upstream of it.
    /// </remarks>
    let skip (count: int) : Flow<'T, 'T> =
        Flow<'T, 'T>(LocalStageChain.Of(LocalStageDescriptor.Skip(LocalOptionGuard.Count(count, nameof count))))

    /// <summary>Passes elements while a predicate holds, exclusive of the one that ends it.</summary>
    /// <param name="predicate">The test each element must pass for the stream to continue.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The first element the predicate rejects is not emitted, and the run completes as if the source had
    /// ended there. <see cref="M:Orleans.Dataflow.FSharp.Flow.takeThrough``1"/> is the inclusive spelling
    /// and is a different word rather than a flag.
    /// </remarks>
    let takeWhile (predicate: 'T -> bool) : Flow<'T, 'T> =
        Flow<'T, 'T>(LocalStageChain.Of(LocalStageDescriptor.TakeWhile(Func<'T, bool> predicate)))

    /// <summary>Passes elements while a predicate holds, and the first element it rejects.</summary>
    /// <param name="predicate">The test each element must pass for the stream to continue past it.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The inclusive counterpart of <see cref="M:Orleans.Dataflow.FSharp.Flow.takeWhile``1"/>, and inclusive
    /// of the element that <em>ends</em> the stream rather than of the last one that passed: the predicate
    /// answers "keep going", and the first element it says no to is emitted before the run completes. That
    /// is how a stream ends at a terminator it has to deliver — the closing record, the sentinel — and the
    /// predicate for such a stream is the one written for
    /// <see cref="M:Orleans.Dataflow.FSharp.Flow.takeWhile``1"/>, unchanged.
    /// </remarks>
    let takeThrough (predicate: 'T -> bool) : Flow<'T, 'T> =
        Flow<'T, 'T>(LocalStageChain.Of(LocalStageDescriptor.TakeThrough(Func<'T, bool> predicate)))

    /// <summary>Drops elements while a predicate holds.</summary>
    /// <param name="predicate">The test that decides which elements to drop.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// Exclusive in the same sense <see cref="M:Orleans.Dataflow.FSharp.Flow.takeWhile``1"/> is: the first
    /// element the predicate rejects is emitted, and so is everything after it, whether or not the predicate
    /// would accept it again.
    /// </remarks>
    let skipWhile (predicate: 'T -> bool) : Flow<'T, 'T> =
        Flow<'T, 'T>(LocalStageChain.Of(LocalStageDescriptor.SkipWhile(Func<'T, bool> predicate)))

    /// <summary>Passes the first occurrence of every element.</summary>
    /// <param name="options">The greatest number of distinct elements the stage may remember.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// Elements are compared with the element type's default equality. The bound is required and is not a
    /// hint: an element that would be the one key past it faults the run rather than evicting an older key.
    /// The remembered keys are per run, so this deduplicates within a run and never across two.
    /// </remarks>
    let distinct (options: Orleans.Dataflow.DistinctOptions) : Flow<'T, 'T> =
        Flow<'T, 'T>(
            LocalStageChain.Of(
                LocalStageDescriptor.Distinct(
                    LocalOptionGuard.Distinct(options, nameof options),
                    EqualityComparer<'T>.Default)))

    /// <summary>Drops an element equal to the one immediately before it.</summary>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The bounded deduplicator, bounded by what it is rather than by a number an author declared: it
    /// remembers exactly one element, so there is nothing to declare and nothing that can overflow. It
    /// collapses runs and never compares across them — <c>a a b b a</c> becomes <c>a b a</c> — which makes
    /// it the operator for repeats that are adjacent by construction and the wrong one for repeats that are
    /// not. <see cref="M:Orleans.Dataflow.FSharp.Flow.distinct``1"/> is the other one, and it costs a
    /// declared bound because it has to.
    /// </remarks>
    [<GeneralizableValue>]
    let deduplicateConsecutive<'T> : Flow<'T, 'T> =
        Flow<'T, 'T>(
            LocalStageChain.Of(LocalStageDescriptor.DeduplicateConsecutive(EqualityComparer<'T>.Default)))

    /// <summary>Collects elements into lists of a declared size.</summary>
    /// <param name="size">How many elements one group holds; at least one.</param>
    /// <returns>The flow of groups.</returns>
    /// <remarks>
    /// A group is emitted the moment it fills, so the stage holds at most <paramref name="size"/> elements
    /// and that is the whole of its memory bound. The last group is emitted when the stream ends and is the
    /// only one that may be partial; an empty group is never emitted.
    /// </remarks>
    let grouped (size: int) : Flow<'T, IReadOnlyList<'T>> =
        Flow<'T, IReadOnlyList<'T>>(
            LocalStageChain.Of(
                LocalStageDescriptor.Grouped(
                    LocalOptionGuard.Size(size, nameof size),
                    Bindings.groupOf<'T> ())))

    /// <summary>Emits a window of a declared size, advancing by a declared step.</summary>
    /// <param name="size">How many elements one window holds; at least one.</param>
    /// <param name="step">How far the window advances after each emission; at least one.</param>
    /// <returns>The flow of windows.</returns>
    /// <remarks>
    /// The relation between the two numbers is the operator: a step below the size overlaps windows, a step
    /// equal to it partitions the stream, and a step above it samples it. The end of the stream emits the
    /// buffer as one final window only if it holds an element no window has carried.
    /// </remarks>
    let sliding (size: int) (step: int) : Flow<'T, IReadOnlyList<'T>> =
        Flow<'T, IReadOnlyList<'T>>(
            LocalStageChain.Of(
                LocalStageDescriptor.Sliding(
                    LocalOptionGuard.Size(size, nameof size),
                    LocalOptionGuard.Step(step, nameof step),
                    Bindings.groupOf<'T> ())))

    /// <summary>Closes a group by a count or by a window, whichever comes first.</summary>
    /// <param name="maxElements">How many elements close a group; at least one.</param>
    /// <param name="window">How long a group stays open once its first element has arrived.</param>
    /// <returns>The flow of groups.</returns>
    /// <remarks>
    /// The window belongs to the group rather than to the stage, so a stream that goes quiet emits nothing
    /// during the quiet and the group that follows is timed from its own first element. The clock is the
    /// host's, resolved when the graph is materialized, and there is nothing to thread here: a clock is a
    /// property of the run and never of the document. This stage is a boundary, which is the price of being
    /// able to emit while nothing is arriving.
    /// </remarks>
    let groupedWithin (maxElements: int) (window: TimeSpan) : Flow<'T, IReadOnlyList<'T>> =
        Flow<'T, IReadOnlyList<'T>>(
            LocalStageChain.Of(
                LocalStageDescriptor.GroupedWithin(
                    LocalOptionGuard.Size(maxElements, nameof maxElements),
                    LocalOptionGuard.Duration(window, nameof window),
                    Bindings.groupOf<'T> ())))

    /// <summary>Closes a group by a count, a weight, or a window, whichever comes first.</summary>
    /// <param name="maxElements">How many elements close a group; at least one.</param>
    /// <param name="maxWeight">How much one group may weigh; at least one.</param>
    /// <param name="window">How long a group stays open once its first element has arrived.</param>
    /// <param name="cost">What one element weighs; zero or more.</param>
    /// <returns>The flow of groups.</returns>
    /// <remarks>
    /// Everything <see cref="M:Orleans.Dataflow.FSharp.Flow.groupedWithin``1"/> promises, with a third bound
    /// that is what the elements are worth rather than how many there are. The weight bound is never
    /// exceeded, because the group closes before the element that would break it. A negative weight and a
    /// weight above <paramref name="maxWeight"/> both fail the run rather than being absorbed.
    /// </remarks>
    let groupedWeightedWithin
        (maxElements: int)
        (maxWeight: int)
        (window: TimeSpan)
        (cost: 'T -> int)
        : Flow<'T, IReadOnlyList<'T>> =
        Flow<'T, IReadOnlyList<'T>>(
            LocalStageChain.Of(
                LocalStageDescriptor.GroupedWeightedWithin(
                    LocalOptionGuard.Size(maxElements, nameof maxElements),
                    LocalOptionGuard.Weight(maxWeight, nameof maxWeight),
                    LocalOptionGuard.Duration(window, nameof window),
                    Func<'T, int> cost,
                    Bindings.groupOf<'T> ())))

    /// <summary>Runs one instance of a flow per key.</summary>
    /// <param name="options">The bound on active keys and what the key past it costs.</param>
    /// <param name="keySelector">The function answering which key an element belongs to.</param>
    /// <param name="group">The flow one key's substream is, instantiated once per key.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The group flow is declared once and instantiated per key, so every key keeps its own state; emission
    /// is merged, so the keys interleave downstream in the order their elements arrived while each key's own
    /// order is preserved; the bound on active keys is required, with the key past it either faulting the
    /// run or evicting the idlest key; and the end of the stream flushes every key still open, in the order
    /// its key first arrived. The group flow holds element stages only, because it is fused per key, and a
    /// flow holding anything else is refused here by name.
    /// </remarks>
    let groupBy
        (options: Orleans.Dataflow.GroupByOptions)
        (keySelector: 'In -> 'Key)
        (group: Flow<'In, 'Out>)
        : Flow<'In, 'Out> =
        Flow<'In, 'Out>(
            LocalStageChain.Of(
                LocalStageDescriptor.GroupBy(
                    LocalOptionGuard.GroupBy(options, nameof options),
                    Func<'In, 'Key> keySelector,
                    EqualityComparer<'Key>.Default,
                    LocalOptionGuard.Group(group.Stages, nameof group))))

    /// <summary>Puts a bounded buffer between what is above this point and what is below it.</summary>
    /// <param name="options">The capacity and the overflow policy.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// A buffer is where a graph stops being one loop: everything upstream of it runs as one fused segment
    /// and everything downstream as another, with this one bounded queue between them.
    /// </remarks>
    let buffer (options: Orleans.Dataflow.BufferOptions) : Flow<'T, 'T> =
        Flow<'T, 'T>(
            LocalStageChain.Of(
                LocalStageDescriptor.Buffer(LocalOptionGuard.Buffer(options, nameof options))))

    /// <summary>Holds every element for a declared duration.</summary>
    /// <param name="delay">How long each element is held before it is emitted.</param>
    /// <param name="holdback">How many elements may be held at once, and what happens to the next one.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The stream is shifted rather than paced: every element starts its own wait when the stage takes it,
    /// the results are emitted in the order the elements arrived, and a burst that fits the declared
    /// holdback comes out with its gaps intact, later by the delay. The holdback is required and is the
    /// bound on that.
    /// </remarks>
    let delay (delay: TimeSpan) (holdback: Orleans.Dataflow.BufferOptions) : Flow<'T, 'T> =
        Flow<'T, 'T>(
            LocalStageChain.Of(
                LocalStageDescriptor.Delay(
                    LocalOptionGuard.Duration(delay, nameof delay),
                    LocalOptionGuard.Buffer(holdback, nameof holdback))))

    /// <summary>Holds the first element until a duration has passed since the run started.</summary>
    /// <param name="delay">How long after the run starts the first element may be emitted.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The delay is on the stream and not on its elements: everything after the first passes untouched, and
    /// a stream whose first element arrives later than that is not delayed at all, because the wait is for
    /// the moment rather than for the duration.
    /// </remarks>
    let initialDelay (delay: TimeSpan) : Flow<'T, 'T> =
        Flow<'T, 'T>(
            LocalStageChain.Of(
                LocalStageDescriptor.Timed(
                    LocalStageKind.InitialDelay,
                    LocalOptionGuard.Duration(delay, nameof delay))))

    /// <summary>Fails the run when the stream goes quiet for longer than a declared gap.</summary>
    /// <param name="gap">The greatest silence allowed between two elements, and before the first.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// Counted from the previous element, and for the first element from the moment the run started, so a
    /// stream that never produces anything at all fails rather than hanging. Nothing is dropped and nothing
    /// is retried: a timeout is a statement that the stream broke its own promise. The clock keeps running
    /// while the run is paused — a pause holds the elements, not the clock.
    /// </remarks>
    let timeout (gap: TimeSpan) : Flow<'T, 'T> =
        Flow<'T, 'T>(
            LocalStageChain.Of(
                LocalStageDescriptor.Timed(
                    LocalStageKind.Timeout,
                    LocalOptionGuard.Duration(gap, nameof gap))))

    /// <summary>Ends the stream when a duration has passed since the run started.</summary>
    /// <param name="window">How long after the run starts the stream ends.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// <para>
    /// The wall-clock <c>take</c>: everything emitted before the window closes is kept, the element that
    /// arrives at or after it is not, and the stream ends there the way reaching a count bound ends it.
    /// </para>
    /// <para>
    /// A stream that has gone quiet ends at the deadline rather than waiting for an element to notice it
    /// with — <em>provided this stage stands in a segment of its own</em>. Fused into the segment above it,
    /// it can only act while that segment is running, so a stage fused directly onto a source that is parked
    /// waiting for its next element is a stage that is parked too. Writing a
    /// <see cref="M:Orleans.Dataflow.FSharp.Flow.buffer``1"/> immediately before it is what puts it in a
    /// segment of its own, and is what an author wants whenever the deadline has to fire during silence.
    /// </para>
    /// <para>
    /// The deadline ends the <em>stream</em>; the <em>run</em> ends once everything above has learned. A
    /// source asleep in a wait of this runtime's own is not released by a completion below it — it learns at
    /// its next attempt to hand an element over, when the boundary refuses it — so a run whose source is
    /// parked for an hour outlives its own deadline by that hour. That is the same rule that leaves a source
    /// parked on an empty channel where it is, and it is why cancelling such a run is what a caller who
    /// cannot wait reaches for.
    /// </para>
    /// </remarks>
    let takeWithin (window: TimeSpan) : Flow<'T, 'T> =
        Flow<'T, 'T>(
            LocalStageChain.Of(
                LocalStageDescriptor.Timed(
                    LocalStageKind.TakeWithin,
                    LocalOptionGuard.Duration(window, nameof window))))

    /// <summary>Drops every element until a duration has passed since the run started.</summary>
    /// <param name="window">How long after the run starts elements begin to pass.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The mirror of <see cref="M:Orleans.Dataflow.FSharp.Flow.takeWithin``1"/> and the wall-clock
    /// <c>skip</c>: an element arriving inside the window is dropped rather than held, and the stage never
    /// waits, so a stream that produces nothing during the window costs nothing at all.
    /// </remarks>
    let skipWithin (window: TimeSpan) : Flow<'T, 'T> =
        Flow<'T, 'T>(
            LocalStageChain.Of(
                LocalStageDescriptor.Timed(
                    LocalStageKind.SkipWithin,
                    LocalOptionGuard.Duration(window, nameof window))))

    /// <summary>Holds the stream to a declared rate, one unit per element.</summary>
    /// <param name="options">The rate, the burst, and what to do with an element there is no budget for.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The budget is a token bucket that starts full and refills continuously, so a stream at or below the
    /// declared rate passes untouched and a faster one is either paced or refused by the declared mode.
    /// Nothing is ever dropped here: a shaping throttle waits on the segment's own thread, which
    /// backpressures upstream, and an enforcing one fails the run.
    /// </remarks>
    let throttle (options: Orleans.Dataflow.ThrottleOptions) : Flow<'T, 'T> =
        Flow<'T, 'T>(
            LocalStageChain.Of(
                LocalStageDescriptor.Throttle(LocalOptionGuard.Throttle(options, nameof options), null)))

    /// <summary>Holds the stream to a declared rate, charged by what each element is worth.</summary>
    /// <param name="options">The rate, the burst, and what to do with an element there is no budget for.</param>
    /// <param name="cost">What one element costs the rate; zero or more.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The same bucket as <see cref="M:Orleans.Dataflow.FSharp.Flow.throttle``1"/>, charged by the answer of
    /// <paramref name="cost"/> instead of by one per element: a rate of a thousand per second with a cost
    /// function answering a batch's size admits a thousand rows per second however many batches carry them.
    /// An element whose cost exceeds the burst fails the run in both modes, because no amount of waiting
    /// could ever admit it; a negative cost fails it too.
    /// </remarks>
    let throttleBy (options: Orleans.Dataflow.ThrottleOptions) (cost: 'T -> int) : Flow<'T, 'T> =
        Flow<'T, 'T>(
            LocalStageChain.Of(
                LocalStageDescriptor.Throttle(
                    LocalOptionGuard.Throttle(options, nameof options),
                    Func<'T, int> cost)))

    /// <summary>Puts a gate in the stream that an author opens and closes while the run is running.</summary>
    /// <param name="controlName">The author-stable name to expose the valve under.</param>
    /// <param name="initialMode">The state the valve starts each run in.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The control is resolved by name from the run handle and exists as soon as the run does — a control is
    /// something an author uses while the run is running, which is what separates it from a result. Closing
    /// it holds the element the stage has in its hand and backpressures everything above it; nothing is
    /// dropped and nothing is buffered here, because a valve has no capacity of its own. The state it starts
    /// in is written into the document, because a graph whose valve starts closed produces nothing until
    /// something opens it; there is no default, because a default is a decision this surface would be making
    /// silently.
    /// </remarks>
    let valve (controlName: string) (initialMode: Orleans.Dataflow.ValveMode) : Flow<'T, 'T> =
        Flow<'T, 'T>(
            LocalStageChain.Of(
                LocalStageDescriptor.Valve(
                    LocalOptionGuard.Valve(initialMode, nameof initialMode),
                    LocalOptionGuard.SlotName(controlName, nameof controlName))))

    /// <summary>Answers the failures raised inside a flow instead of letting them fail the run.</summary>
    /// <param name="options">The form, and the retrying form's attempts, ladder, and exhaustion answer.</param>
    /// <param name="scope">The flow the scope owns the per-element execution of.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The scope is declared once and owns one instance of <paramref name="scope"/>; a failure raised inside
    /// it is answered by the declared form, while everything outside it keeps the engine's own rule. A
    /// cancellation is never caught, and a failure of the machinery is refused at materialization rather
    /// than supervised. The recovering form needs an element to emit and is
    /// <see cref="M:Orleans.Dataflow.FSharp.Flow.supervisedRecovering``3"/>, which is a different word
    /// rather than an argument that is meaningless for the other forms.
    /// </remarks>
    let supervised
        (options: Orleans.Dataflow.SupervisionOptions)
        (scope: Flow<'In, 'Out>)
        : Flow<'In, 'Out> =
        Flow<'In, 'Out>(
            LocalStageChain.Of(
                LocalStageDescriptor.Supervised(
                    LocalOptionGuard.Supervision(options, nameof options, recovering = false),
                    null,
                    LocalOptionGuard.Scope(scope.Stages, nameof scope))))

    /// <summary>Ends a failing flow's stream with a declared element instead of failing the run.</summary>
    /// <param name="options">The form, which must be the recovering one.</param>
    /// <param name="fallback">The element the scope emits when a failure ends its stream.</param>
    /// <param name="scope">The flow the scope owns the per-element execution of.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// The first failure inside the scope emits <paramref name="fallback"/> and ends the scope's stream
    /// successfully, so everything above it stops and everything below it drains. The fallback comes before
    /// the scope because the scope is the value the operator is about, and an author reads configuration
    /// first.
    /// </remarks>
    let supervisedRecovering
        (options: Orleans.Dataflow.SupervisionOptions)
        (fallback: 'Out)
        (scope: Flow<'In, 'Out>)
        : Flow<'In, 'Out> =
        Flow<'In, 'Out>(
            LocalStageChain.Of(
                LocalStageDescriptor.Supervised(
                    LocalOptionGuard.Supervision(options, nameof options, recovering = true),
                    fallback,
                    LocalOptionGuard.Scope(scope.Stages, nameof scope))))

    /// <summary>Declares the stages whose state survives a resume.</summary>
    /// <param name="scope">The flow whose stages' state a checkpoint carries.</param>
    /// <returns>The flow.</returns>
    /// <remarks>
    /// Everything outside the scope resets on resume. It is not a supervision form and answers no failure;
    /// it holds stages whose state is a canonical value, which is why a scan inside one is
    /// <see cref="M:Orleans.Dataflow.FSharp.Flow.scanDurable``2"/> and not
    /// <see cref="M:Orleans.Dataflow.FSharp.Flow.scan``2"/>. A document holding one declares
    /// <c>durable-state</c>.
    /// </remarks>
    let durable (scope: Flow<'In, 'Out>) : Flow<'In, 'Out> =
        Flow<'In, 'Out>(
            LocalStageChain.Of(
                LocalStageDescriptor.Durable(LocalOptionGuard.DurableScope(scope.Stages, nameof scope))))

    /// <summary>Composes two flows into one that applies them in order.</summary>
    /// <param name="next">The flow applied second.</param>
    /// <param name="current">The flow applied first.</param>
    /// <returns>The composed flow.</returns>
    /// <remarks>
    /// The value being extended is the final argument, so composition reads forward under
    /// <c>|&gt;</c>: <c>Flow.filter isValid |&gt; Flow.andThen (Flow.map normalize)</c> filters and then
    /// maps. This is the primary readable composition function; no symbolic operator stands in for it.
    /// </remarks>
    let andThen (next: Flow<'Middle, 'Out>) (current: Flow<'In, 'Middle>) : Flow<'In, 'Out> =
        Flow<'In, 'Out>(LocalStageChain.Concat(current.Stages, next.Stages))

    /// <summary>Composes a flow with one named occurrence of a registered stage.</summary>
    /// <param name="stage">The typed handle of the registered stage, resolved from a catalog.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <param name="current">The flow applied first, which is unchanged.</param>
    /// <returns>The composed flow.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The deployable sibling of <see cref="M:Orleans.Dataflow.FSharp.Flow.andThen``3"/>, and the only way to
    /// put a registered stage inside a leg: a branch is a flow ending in a terminal, so a leg whose middle is
    /// registered is written here and closed by <see cref="T:Orleans.Dataflow.FSharp.Branch"/>.
    /// </para>
    /// <para>
    /// The name is required, because a registered occurrence exists to be addressed across an edit, a
    /// checkpoint, and an upgrade, and a positional identifier anchors none of those. The payload is the raw
    /// canonical value the stage's parameter contract describes, and it is checked against that contract by
    /// the graph compiler rather than here, exactly as it is for the C# spelling.
    /// </para>
    /// </remarks>
    let andThenRegistered
        (stage: Orleans.Dataflow.RegisteredFlow<'Middle, 'Out>)
        (occurrenceName: string)
        (parameters: Orleans.Dataflow.Serialization.CanonicalJsonValue)
        (current: Flow<'In, 'Middle>)
        : Flow<'In, 'Out> =
        Flow<'In, 'Out>(
            LocalStageChain.Append(
                current.Stages,
                RegisteredAttachment.Occurrence(stage.Specification, occurrenceName, parameters)))
