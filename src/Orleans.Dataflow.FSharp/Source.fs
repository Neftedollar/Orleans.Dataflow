namespace Orleans.Dataflow.FSharp

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open Orleans.Dataflow.Authoring
open Orleans.Dataflow.Identity

// Orleans.Dataflow itself is deliberately not opened: its Source/Flow/Sink are the C# facade's spellings
// of these very concepts, and an open would shadow this package's own types with them. The two shared
// values this module answers with are qualified instead, as are the option records and enumerations the
// operators are configured by.

/// <summary>Constructs sources and composes them toward a closed graph.</summary>
/// <remarks>
/// <para>
/// The value being transformed is the final argument throughout, so a graph reads top to bottom under
/// <c>|&gt;</c>. Closing a source produces the very <see cref="T:Orleans.Dataflow.RunnableGraph"/> the C#
/// facade produces — one shared closed-graph value, one document, one fingerprint — because both frontends
/// funnel through the one graph builder. The result-bearing close answers a tuple, which is the F# shape
/// of what C# spells with an <c>out</c> parameter.
/// </para>
/// <para>
/// Every operator this module carries beyond construction and closure is a shorthand for
/// <see cref="M:Orleans.Dataflow.FSharp.Source.via``2"/> over the flow of the same name, and each is one
/// line for exactly that reason: two spellings of one operator that could drift are two spellings too
/// many. The documents are identical, which is asserted rather than asserted about.
/// </para>
/// </remarks>
[<RequireQualifiedAccess>]
module Source =

    /// <summary>Creates a source that emits the elements of a sequence, in order.</summary>
    /// <param name="elements">The sequence, enumerated once per run at the run's own pace.</param>
    /// <returns>The source.</returns>
    /// <remarks>
    /// <para>
    /// The sequence is the author's: a run enumerates it lazily and disposes the enumerator on every
    /// terminal path, and materializing the graph twice enumerates it twice.
    /// </para>
    /// <para>
    /// This is the only sequence constructor. An F# list, an array, and a set are all sequences, and they
    /// build the same document through this one function, so <c>ofList</c> and <c>ofArray</c> would be two
    /// more names in a completion list for no second behavior.
    /// </para>
    /// </remarks>
    let ofSeq (elements: seq<'T>) : Source<'T> =
        Source<'T>(LocalGraphShape.OfChain(LocalStageChain.Of(LocalStageDescriptor.FromEnumerable elements)))

    /// <summary>The source that emits nothing and completes at once.</summary>
    /// <remarks>
    /// A real source rather than a degenerate one: it is what a graph is tested against when the question is
    /// what happens with no elements at all. A run of it completes successfully, and an aggregate resolves
    /// its seed.
    /// </remarks>
    [<GeneralizableValue>]
    let empty<'T> : Source<'T> =
        Source<'T>(LocalGraphShape.OfChain(LocalStageChain.Of(LocalStageDescriptor.Empty())))

    /// <summary>Creates a source that emits one element and completes.</summary>
    /// <param name="value">The element to emit.</param>
    /// <returns>The source.</returns>
    /// <remarks>
    /// The element is captured as it is given and emitted once per run, so two runs of one graph deliver the
    /// same instance twice. What that instance is, and whether handing it to two runs is safe, is the
    /// author's to decide, exactly as for a sequence.
    /// </remarks>
    let single (value: 'T) : Source<'T> =
        Source<'T>(LocalGraphShape.OfChain(LocalStageChain.Of(LocalStageDescriptor.Single value)))

    /// <summary>Creates a source that emits one element a declared number of times.</summary>
    /// <param name="count">How many times to emit it; zero or more.</param>
    /// <param name="value">The element to emit.</param>
    /// <returns>The source.</returns>
    /// <remarks>
    /// The count is required and there is no endless spelling: a repeat with no count would be an endless
    /// stream nobody had to ask for. <see cref="M:Orleans.Dataflow.FSharp.Source.unfold``2"/> is the source
    /// whose author writes the logic that ends it.
    /// </remarks>
    let repeat (count: int) (value: 'T) : Source<'T> =
        Source<'T>(
            LocalGraphShape.OfChain(
                LocalStageChain.Of(
                    LocalStageDescriptor.Repeat(value, LocalOptionGuard.Count(count, nameof count)))))

    /// <summary>Creates a source over a run of consecutive integers.</summary>
    /// <param name="start">The first integer to emit.</param>
    /// <param name="count">How many integers to emit; zero or more.</param>
    /// <returns>The source.</returns>
    /// <remarks>
    /// The one source with no behavior at all: a document states both numbers, so a range is the same stream
    /// wherever it is run. The elements are <paramref name="start"/> through <c>start + count - 1</c>,
    /// ascending.
    /// </remarks>
    let range (start: int) (count: int) : Source<int> =
        Source<int>(
            LocalGraphShape.OfChain(
                LocalStageChain.Of(LocalStageDescriptor.Range(start, LocalOptionGuard.Range(start, count)))))

    /// <summary>Creates a source that emits the value of one task.</summary>
    /// <param name="task">The task whose value is the single element.</param>
    /// <returns>The source.</returns>
    /// <remarks>
    /// The task is awaited once per run and its value emitted as one element. A task that has already
    /// finished replays its value into every run, because a completed task is a value and not an event; a
    /// task that fails, or was cancelled, faults the run with the exception it carries, unwrapped.
    /// </remarks>
    let ofTask (task: Task<'T>) : Source<'T> =
        Source<'T>(LocalGraphShape.OfChain(LocalStageChain.Of(LocalStageDescriptor.FromTask task)))

    /// <summary>Creates a source that computes its one element when the run asks for it.</summary>
    /// <param name="factory">The function answering the element.</param>
    /// <returns>The source.</returns>
    /// <remarks>
    /// The difference from <see cref="M:Orleans.Dataflow.FSharp.Source.single``1"/> is when the element
    /// exists: this one calls the function once per run, so two runs of one graph get two elements and a
    /// graph built long before it is run holds no element at all.
    /// </remarks>
    let ofFactory (factory: unit -> 'T) : Source<'T> =
        Source<'T>(
            LocalGraphShape.OfChain(
                LocalStageChain.Of(LocalStageDescriptor.FromFactory(Func<'T> factory))))

    /// <summary>Creates a source that awaits its one element when the run asks for it.</summary>
    /// <param name="factory">The callback answering the element, which receives the run's own token.</param>
    /// <returns>The source.</returns>
    /// <remarks>
    /// <see cref="M:Orleans.Dataflow.FSharp.Source.ofFactory``1"/> with a callback that awaits, and
    /// per-run in the same sense. The token is the run's, so a run cancelled before its first element
    /// reaches the callback.
    /// </remarks>
    let ofTaskFactory (factory: CancellationToken -> Task<'T>) : Source<'T> =
        Source<'T>(
            LocalGraphShape.OfChain(
                LocalStageChain.Of(
                    LocalStageDescriptor.FromAsyncFactory(
                        Func<CancellationToken, Task<'T>>(fun token -> factory token)))))

    /// <summary>Creates a source over an asynchronous computation of one element.</summary>
    /// <param name="computation">The computation, started once per run.</param>
    /// <returns>The source.</returns>
    /// <remarks>
    /// The F# effect over the very stage <see cref="M:Orleans.Dataflow.FSharp.Source.ofTaskFactory``1"/>
    /// writes, and the natural F# spelling of a deferred element: an <c>Async</c> is cold, so it is a
    /// factory already and is started per run rather than shared between runs, which is exactly what
    /// separates it from <see cref="M:Orleans.Dataflow.FSharp.Source.ofTask``1"/>. The run's own token
    /// starts the computation.
    /// </remarks>
    let ofAsync (computation: Async<'T>) : Source<'T> =
        Source<'T>(
            LocalGraphShape.OfChain(
                LocalStageChain.Of(
                    LocalStageDescriptor.FromAsyncFactory(
                        Func<CancellationToken, Task<'T>>(fun token -> Bindings.asTask computation token)))))

    /// <summary>Creates a source that fails without emitting anything.</summary>
    /// <param name="exception">The failure every run of the graph reports.</param>
    /// <returns>The source.</returns>
    /// <remarks>
    /// The run faults with this very instance, so a caller that compares exceptions by identity sees the one
    /// it supplied. The instance is shared by every run of the graph.
    /// </remarks>
    /// <example>
    /// The element type appears only in the answer, so where nothing downstream fixes it an annotation
    /// does: <c>let refused : Source&lt;Order&gt; = Source.failed reason</c>.
    /// </example>
    let failed (``exception``: exn) : Source<'T> =
        Source<'T>(LocalGraphShape.OfChain(LocalStageChain.Of(LocalStageDescriptor.Failed ``exception``)))

    /// <summary>The source that emits nothing and never ends.</summary>
    /// <remarks>
    /// What a graph is tested against when the question is what happens while nothing arrives: a timing
    /// operator's silence, a valve's hold, a shutdown with no element in flight. A run of it ends only when
    /// something else ends it.
    /// </remarks>
    [<GeneralizableValue>]
    let never<'T> : Source<'T> =
        Source<'T>(LocalGraphShape.OfChain(LocalStageChain.Of(LocalStageDescriptor.Never())))

    /// <summary>Creates a source that repeats a sequence endlessly.</summary>
    /// <param name="elements">The sequence, re-enumerated every time it runs out.</param>
    /// <returns>The source.</returns>
    /// <remarks>
    /// The sequence is enumerated from its start again whenever it ends, so it has to be re-enumerable; a
    /// sequence that is not is a stream this source would silently stop producing. Nothing downstream will
    /// ever see the end of it, so something has to bound it.
    /// </remarks>
    let cycle (elements: seq<'T>) : Source<'T> =
        Source<'T>(LocalGraphShape.OfChain(LocalStageChain.Of(LocalStageDescriptor.Cycle elements)))

    /// <summary>Creates a source that produces its elements from a state it carries.</summary>
    /// <param name="generator">
    /// The function answering the next element and the state after it, or nothing to end the stream.
    /// </param>
    /// <param name="seed">The state the first call receives.</param>
    /// <returns>The source.</returns>
    /// <remarks>
    /// The F# shape of the generator is an option rather than the C# vocabulary's boolean and two output
    /// parameters, because the two carry the same information and only one of them can be pattern matched.
    /// The state is per run, so two runs of one graph both start from <paramref name="seed"/>.
    /// </remarks>
    let unfold (generator: 'State -> ('T * 'State) voption) (seed: 'State) : Source<'T> =
        Source<'T>(
            LocalGraphShape.OfChain(
                LocalStageChain.Of(
                    LocalStageDescriptor.Unfold(
                        seed,
                        Orleans.Dataflow.UnfoldGenerator<'State, 'T>(fun state value next ->
                            match generator state with
                            | ValueSome (produced, following) ->
                                value <- produced
                                next <- following
                                true
                            | ValueNone ->
                                // The output parameters must be written before the delegate returns, and
                                // neither is read once it has answered false; this is what the C# facade's
                                // own callers write.
                                value <- Unchecked.defaultof<'T>
                                next <- state
                                false)))))

    /// <summary>Creates a source that awaits each of its elements from a state it carries.</summary>
    /// <param name="generator">
    /// The callback answering the next element and the state after it, or nothing to end the stream, which
    /// receives the run's own token.
    /// </param>
    /// <param name="seed">The state the first call receives.</param>
    /// <returns>The source.</returns>
    /// <remarks>
    /// <see cref="M:Orleans.Dataflow.FSharp.Source.unfold``2"/> with a generator that awaits. One call runs
    /// at a time, because the state the next call receives is this call's answer.
    /// </remarks>
    let unfoldTask
        (generator: 'State -> CancellationToken -> Task<('T * 'State) option>)
        (seed: 'State)
        : Source<'T> =
        Source<'T>(
            LocalGraphShape.OfChain(
                LocalStageChain.Of(
                    LocalStageDescriptor.UnfoldAsync(
                        seed,
                        Orleans.Dataflow.AsyncUnfoldGenerator<'State, 'T>(fun state token ->
                            task {
                                match! generator state token with
                                | Some (produced, following) ->
                                    return Nullable(Orleans.Dataflow.UnfoldStep<'State, 'T>(produced, following))
                                | None -> return Nullable()
                            })))))

    /// <summary>Creates a source that computes each of its elements asynchronously from a state it carries.</summary>
    /// <param name="generator">
    /// The computation answering the next element and the state after it, or nothing to end the stream.
    /// </param>
    /// <param name="seed">The state the first call receives.</param>
    /// <returns>The source.</returns>
    /// <remarks>
    /// The F# effect over the very stage <see cref="M:Orleans.Dataflow.FSharp.Source.unfoldTask``2"/>
    /// writes, with the run's own token starting the computation.
    /// </remarks>
    let unfoldAsync (generator: 'State -> Async<('T * 'State) option>) (seed: 'State) : Source<'T> =
        Source<'T>(
            LocalGraphShape.OfChain(
                LocalStageChain.Of(
                    LocalStageDescriptor.UnfoldAsync(
                        seed,
                        Orleans.Dataflow.AsyncUnfoldGenerator<'State, 'T>(fun state token ->
                            task {
                                match! Bindings.asTask (generator state) token with
                                | Some (produced, following) ->
                                    return Nullable(Orleans.Dataflow.UnfoldStep<'State, 'T>(produced, following))
                                | None -> return Nullable()
                            })))))

    /// <summary>Creates a source over an asynchronous sequence.</summary>
    /// <param name="elements">The sequence, opened once per run with the run's own token.</param>
    /// <returns>The source.</returns>
    /// <remarks>
    /// The type argument is what says which element type an implementation of the interface is being read
    /// at, and it is written into the binding rather than inferred at run time: one class may implement the
    /// interface for two element types, and nothing in a document names which of them the graph means.
    /// </remarks>
    let ofAsyncEnumerable (elements: IAsyncEnumerable<'T>) : Source<'T> =
        Source<'T>(
            LocalGraphShape.OfChain(
                LocalStageChain.Of(
                    LocalStageDescriptor.FromAsyncEnumerable(
                        Orleans.Dataflow.Runtime.LocalAsyncCursorFactory(fun token ->
                            Orleans.Dataflow.Runtime.LocalAsyncCursor<'T>(elements.GetAsyncEnumerator token)
                            :> Orleans.Dataflow.Runtime.LocalAsyncCursor)))))

    /// <summary>Creates a source that drains a channel the author owns.</summary>
    /// <param name="reader">The reader to drain.</param>
    /// <returns>The source.</returns>
    /// <remarks>
    /// The run reads until the channel is completed and empty, and then completes; a channel completed with
    /// an exception faults the run with it, unwrapped. This is the one source that is not fresh per run: a
    /// reader is not re-enumerable, so two runs of one graph compete for its elements.
    /// <see cref="M:Orleans.Dataflow.FSharp.Source.queue``1"/> is the source that gives every run an
    /// ingress of its own.
    /// </remarks>
    let ofChannel (reader: ChannelReader<'T>) : Source<'T> =
        Source<'T>(LocalGraphShape.OfChain(LocalStageChain.Of(LocalStageDescriptor.FromChannel reader)))

    /// <summary>Creates a source that emits the number of every tick of an interval.</summary>
    /// <param name="initialDelay">How long after the run starts the first tick is emitted.</param>
    /// <param name="interval">How long between ticks.</param>
    /// <returns>The source of tick numbers.</returns>
    /// <remarks>
    /// Two durations say exactly which elements and when; what they do not say is which clock measures them,
    /// because a clock is a property of the run and never of the document — the host's time provider is
    /// resolved at materialization, so there is nothing to thread through an authoring call.
    /// </remarks>
    let tick (initialDelay: TimeSpan) (interval: TimeSpan) : Source<int64> =
        Source<int64>(
            LocalGraphShape.OfChain(
                LocalStageChain.Of(
                    LocalStageDescriptor.Tick(
                        LocalOptionGuard.Duration(initialDelay, nameof initialDelay),
                        LocalOptionGuard.Duration(interval, nameof interval)))))

    /// <summary>Creates a source that a producer pushes into, through a bounded queue of its own.</summary>
    /// <param name="options">The capacity of the queue and what it does when it is full.</param>
    /// <param name="controlName">The author-stable name to expose the queue under.</param>
    /// <returns>The source.</returns>
    /// <remarks>
    /// <para>
    /// Every other source is pulled: the run asks for the next element and the source produces it. A
    /// producer that pushes cannot be asked, so this source owns a bounded queue and the author's producers
    /// offer into it. The options are a buffer's options for the same reason: a full ingress queue and a
    /// full buffer are one situation seen from the two ends of a graph.
    /// </para>
    /// <para>
    /// The queue is a per-run control rather than part of the graph and is reached by name: closing the
    /// graph declares a control under <paramref name="controlName"/>, and the closed graph's
    /// <c>Control&lt;IIngressQueue&lt;'T&gt;&gt;</c> turns that name back into a slot a run handle
    /// resolves. It resolves at the start of a run and not at its end, because producers push into a run
    /// that is already running.
    /// </para>
    /// </remarks>
    /// <example>
    /// The element type appears only in the answer, so it is written as an annotation rather than as a type
    /// argument: <c>let orders : Source&lt;Order&gt; = Source.queue options "orders"</c>.
    /// </example>
    let queue (options: Orleans.Dataflow.BufferOptions) (controlName: string) : Source<'T> =
        Source<'T>(
            LocalGraphShape.OfChain(
                LocalStageChain.Of(
                    LocalStageDescriptor.Queue(
                        LocalOptionGuard.Buffer(options, nameof options),
                        LocalOptionGuard.SlotName(controlName, nameof controlName),
                        typeof<Orleans.Dataflow.IIngressQueue<'T>>,
                        Func<Orleans.Dataflow.Runtime.LocalIngressQueue, obj>(fun queue ->
                            Orleans.Dataflow.Runtime.IngressQueue<'T>(queue) :> obj)))))

    /// <summary>Extends a source with a flow.</summary>
    /// <param name="flow">The transformation to apply.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The extended source.</returns>
    let via (flow: Flow<'In, 'Out>) (source: Source<'In>) : Source<'Out> =
        Source<'Out>(source.State.Concat flow.Stages)

    /// <summary>Transforms every element of a source through a function.</summary>
    /// <param name="mapping">The function applied to each element.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The transformed source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.map mapping)</c>, producing the identical document.</remarks>
    let map (mapping: 'In -> 'Out) (source: Source<'In>) : Source<'Out> =
        via (Flow.map mapping) source

    /// <summary>Keeps the elements of a source a predicate answers true for.</summary>
    /// <param name="predicate">The predicate deciding each element.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The filtered source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.filter predicate)</c>, producing the identical document.</remarks>
    let filter (predicate: 'T -> bool) (source: Source<'T>) : Source<'T> =
        via (Flow.filter predicate) source

    /// <summary>Transforms and filters a source in one step.</summary>
    /// <param name="chooser">The function answering the transformed element, or nothing to drop it.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The transformed source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.choose chooser)</c>, producing the identical document.</remarks>
    let choose (chooser: 'In -> 'Out voption) (source: Source<'In>) : Source<'Out> =
        via (Flow.choose chooser) source

    /// <summary>Transforms and filters a source in one step, over the reference-typed option.</summary>
    /// <param name="chooser">The function answering the transformed element, or nothing to drop it.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The transformed source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.chooseOption chooser)</c>, producing the identical document.</remarks>
    let chooseOption (chooser: 'In -> 'Out option) (source: Source<'In>) : Source<'Out> =
        via (Flow.chooseOption chooser) source

    /// <summary>Replaces every element of a source with the sequence a function answers, in order.</summary>
    /// <param name="mapping">The function answering one sequence per element.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The flattened source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.collect mapping)</c>, producing the identical document.</remarks>
    let collect (mapping: 'In -> seq<'Out>) (source: Source<'In>) : Source<'Out> =
        via (Flow.collect mapping) source

    /// <summary>Merges the sequences of several of a source's elements at once.</summary>
    /// <param name="options">The greatest number of inner sequences open at one time.</param>
    /// <param name="mapping">The function answering one sequence per element.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The flattened source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.mergeMap options mapping)</c>, producing the identical document.</remarks>
    let mergeMap
        (options: Orleans.Dataflow.ParallelismOptions)
        (mapping: 'In -> seq<'Out>)
        (source: Source<'In>)
        : Source<'Out> =
        via (Flow.mergeMap options mapping) source

    /// <summary>Merges the asynchronous sequences of several of a source's elements at once.</summary>
    /// <param name="options">The greatest number of inner sequences open at one time.</param>
    /// <param name="mapping">The function answering one asynchronous sequence per element.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The flattened source.</returns>
    /// <remarks>
    /// Shorthand for <c>Source.via (Flow.mergeMapAsyncEnumerable options mapping)</c>, producing the
    /// identical document.
    /// </remarks>
    let mergeMapAsyncEnumerable
        (options: Orleans.Dataflow.ParallelismOptions)
        (mapping: 'In -> IAsyncEnumerable<'Out>)
        (source: Source<'In>)
        : Source<'Out> =
        via (Flow.mergeMapAsyncEnumerable options mapping) source

    /// <summary>Transforms every element of a source through a task-returning function.</summary>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="mapping">The callback applied to each element, which receives the run's own token.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The transformed source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.mapTask options mapping)</c>, producing the identical document.</remarks>
    let mapTask
        (options: Orleans.Dataflow.ParallelismOptions)
        (mapping: 'In -> CancellationToken -> Task<'Out>)
        (source: Source<'In>)
        : Source<'Out> =
        via (Flow.mapTask options mapping) source

    /// <summary>Transforms every element of a source through a task-returning function, in completion order.</summary>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="mapping">The callback applied to each element, which receives the run's own token.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The transformed source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.mapTaskUnordered options mapping)</c>, producing the identical document.</remarks>
    let mapTaskUnordered
        (options: Orleans.Dataflow.ParallelismOptions)
        (mapping: 'In -> CancellationToken -> Task<'Out>)
        (source: Source<'In>)
        : Source<'Out> =
        via (Flow.mapTaskUnordered options mapping) source

    /// <summary>Transforms every element of a source through a value-task-returning function.</summary>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="mapping">The callback applied to each element, which receives the run's own token.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The transformed source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.mapValueTask options mapping)</c>, producing the identical document.</remarks>
    let mapValueTask
        (options: Orleans.Dataflow.ParallelismOptions)
        (mapping: 'In -> CancellationToken -> ValueTask<'Out>)
        (source: Source<'In>)
        : Source<'Out> =
        via (Flow.mapValueTask options mapping) source

    /// <summary>Transforms every element of a source through a value-task-returning function, in completion order.</summary>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="mapping">The callback applied to each element, which receives the run's own token.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The transformed source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.mapValueTaskUnordered options mapping)</c>, producing the identical document.</remarks>
    let mapValueTaskUnordered
        (options: Orleans.Dataflow.ParallelismOptions)
        (mapping: 'In -> CancellationToken -> ValueTask<'Out>)
        (source: Source<'In>)
        : Source<'Out> =
        via (Flow.mapValueTaskUnordered options mapping) source

    /// <summary>Transforms every element of a source through an asynchronous computation.</summary>
    /// <param name="options">The greatest number of computations in flight at one time.</param>
    /// <param name="mapping">The computation built for each element.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The transformed source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.mapAsync options mapping)</c>, producing the identical document.</remarks>
    let mapAsync
        (options: Orleans.Dataflow.ParallelismOptions)
        (mapping: 'In -> Async<'Out>)
        (source: Source<'In>)
        : Source<'Out> =
        via (Flow.mapAsync options mapping) source

    /// <summary>Transforms every element of a source through an asynchronous computation, in completion order.</summary>
    /// <param name="options">The greatest number of computations in flight at one time.</param>
    /// <param name="mapping">The computation built for each element.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The transformed source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.mapAsyncUnordered options mapping)</c>, producing the identical document.</remarks>
    let mapAsyncUnordered
        (options: Orleans.Dataflow.ParallelismOptions)
        (mapping: 'In -> Async<'Out>)
        (source: Source<'In>)
        : Source<'Out> =
        via (Flow.mapAsyncUnordered options mapping) source

    /// <summary>Emits the running state of a fold over a source, one state per element.</summary>
    /// <param name="seed">The initial state, which is not emitted.</param>
    /// <param name="folder">The function combining the running state with the next element.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source of states.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.scan seed folder)</c>, producing the identical document.</remarks>
    let scan (seed: 'State) (folder: 'State -> 'In -> 'State) (source: Source<'In>) : Source<'State> =
        via (Flow.scan seed folder) source

    /// <summary>Emits the running state of a fold whose state a durable scope can checkpoint.</summary>
    /// <param name="seed">The initial state, which is not emitted.</param>
    /// <param name="folder">The function combining the running state with the next element.</param>
    /// <param name="export">The projection of the running state into a canonical value.</param>
    /// <param name="restore">The projection of such a value back into a state.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source of states.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.scanDurable seed folder export restore)</c>, producing the identical document.</remarks>
    let scanDurable
        (seed: 'State)
        (folder: 'State -> 'In -> 'State)
        (export: 'State -> Orleans.Dataflow.Serialization.CanonicalJsonValue)
        (restore: Orleans.Dataflow.Serialization.CanonicalJsonValue -> 'State)
        (source: Source<'In>)
        : Source<'State> =
        via (Flow.scanDurable seed folder export restore) source

    /// <summary>Emits the running state of a fold over a source whose function returns a task.</summary>
    /// <param name="seed">The initial state, which is not emitted.</param>
    /// <param name="folder">The callback combining the running state with the next element.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source of states.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.scanTask seed folder)</c>, producing the identical document.</remarks>
    let scanTask
        (seed: 'State)
        (folder: 'State -> 'In -> CancellationToken -> Task<'State>)
        (source: Source<'In>)
        : Source<'State> =
        via (Flow.scanTask seed folder) source

    /// <summary>Emits the running state of a fold over a source whose function is an asynchronous computation.</summary>
    /// <param name="seed">The initial state, which is not emitted.</param>
    /// <param name="folder">The computation built from the running state and the next element.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source of states.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.scanAsync seed folder)</c>, producing the identical document.</remarks>
    let scanAsync
        (seed: 'State)
        (folder: 'State -> 'In -> Async<'State>)
        (source: Source<'In>)
        : Source<'State> =
        via (Flow.scanAsync seed folder) source

    /// <summary>Passes a declared number of a source's elements and ends the stream.</summary>
    /// <param name="count">How many elements to pass; zero or more.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The bounded source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.take count)</c>, producing the identical document.</remarks>
    let take (count: int) (source: Source<'T>) : Source<'T> = via (Flow.take count) source

    /// <summary>Drops a declared number of a source's elements.</summary>
    /// <param name="count">How many elements to drop; zero or more.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.skip count)</c>, producing the identical document.</remarks>
    let skip (count: int) (source: Source<'T>) : Source<'T> = via (Flow.skip count) source

    /// <summary>Passes a source's elements while a predicate holds, exclusive of the one that ends it.</summary>
    /// <param name="predicate">The test each element must pass for the stream to continue.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The bounded source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.takeWhile predicate)</c>, producing the identical document.</remarks>
    let takeWhile (predicate: 'T -> bool) (source: Source<'T>) : Source<'T> =
        via (Flow.takeWhile predicate) source

    /// <summary>Passes a source's elements while a predicate holds, and the first element it rejects.</summary>
    /// <param name="predicate">The test each element must pass for the stream to continue past it.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The bounded source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.takeThrough predicate)</c>, producing the identical document.</remarks>
    let takeThrough (predicate: 'T -> bool) (source: Source<'T>) : Source<'T> =
        via (Flow.takeThrough predicate) source

    /// <summary>Drops a source's elements while a predicate holds.</summary>
    /// <param name="predicate">The test that decides which elements to drop.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.skipWhile predicate)</c>, producing the identical document.</remarks>
    let skipWhile (predicate: 'T -> bool) (source: Source<'T>) : Source<'T> =
        via (Flow.skipWhile predicate) source

    /// <summary>Passes the first occurrence of every element of a source.</summary>
    /// <param name="options">The greatest number of distinct elements the stage may remember.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The deduplicated source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.distinct options)</c>, producing the identical document.</remarks>
    let distinct (options: Orleans.Dataflow.DistinctOptions) (source: Source<'T>) : Source<'T> =
        via (Flow.distinct options) source

    /// <summary>Drops an element of a source equal to the one immediately before it.</summary>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The deduplicated source.</returns>
    /// <remarks>Shorthand for <c>Source.via Flow.deduplicateConsecutive</c>, producing the identical document.</remarks>
    let deduplicateConsecutive (source: Source<'T>) : Source<'T> =
        via Flow.deduplicateConsecutive source

    /// <summary>Collects a source's elements into lists of a declared size.</summary>
    /// <param name="size">How many elements one group holds; at least one.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source of groups.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.grouped size)</c>, producing the identical document.</remarks>
    let grouped (size: int) (source: Source<'T>) : Source<IReadOnlyList<'T>> =
        via (Flow.grouped size) source

    /// <summary>Emits a window of a declared size over a source, advancing by a declared step.</summary>
    /// <param name="size">How many elements one window holds; at least one.</param>
    /// <param name="step">How far the window advances after each emission; at least one.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source of windows.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.sliding size step)</c>, producing the identical document.</remarks>
    let sliding (size: int) (step: int) (source: Source<'T>) : Source<IReadOnlyList<'T>> =
        via (Flow.sliding size step) source

    /// <summary>Closes a group of a source's elements by a count or by a window.</summary>
    /// <param name="maxElements">How many elements close a group; at least one.</param>
    /// <param name="window">How long a group stays open once its first element has arrived.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source of groups.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.groupedWithin maxElements window)</c>, producing the identical document.</remarks>
    let groupedWithin (maxElements: int) (window: TimeSpan) (source: Source<'T>) : Source<IReadOnlyList<'T>> =
        via (Flow.groupedWithin maxElements window) source

    /// <summary>Closes a group of a source's elements by a count, a weight, or a window.</summary>
    /// <param name="maxElements">How many elements close a group; at least one.</param>
    /// <param name="maxWeight">How much one group may weigh; at least one.</param>
    /// <param name="window">How long a group stays open once its first element has arrived.</param>
    /// <param name="cost">What one element weighs; zero or more.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source of groups.</returns>
    /// <remarks>
    /// Shorthand for <c>Source.via (Flow.groupedWeightedWithin maxElements maxWeight window cost)</c>,
    /// producing the identical document.
    /// </remarks>
    let groupedWeightedWithin
        (maxElements: int)
        (maxWeight: int)
        (window: TimeSpan)
        (cost: 'T -> int)
        (source: Source<'T>)
        : Source<IReadOnlyList<'T>> =
        via (Flow.groupedWeightedWithin maxElements maxWeight window cost) source

    /// <summary>Runs one instance of a flow per key over a source.</summary>
    /// <param name="options">The bound on active keys and what the key past it costs.</param>
    /// <param name="keySelector">The function answering which key an element belongs to.</param>
    /// <param name="group">The flow one key's substream is, instantiated once per key.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.groupBy options keySelector group)</c>, producing the identical document.</remarks>
    let groupBy
        (options: Orleans.Dataflow.GroupByOptions)
        (keySelector: 'In -> 'Key)
        (group: Flow<'In, 'Out>)
        (source: Source<'In>)
        : Source<'Out> =
        via (Flow.groupBy options keySelector group) source

    /// <summary>Puts a bounded buffer between what is above this point in a source and what is below it.</summary>
    /// <param name="options">The capacity and the overflow policy.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.buffer options)</c>, producing the identical document.</remarks>
    let buffer (options: Orleans.Dataflow.BufferOptions) (source: Source<'T>) : Source<'T> =
        via (Flow.buffer options) source

    /// <summary>Holds every element of a source for a declared duration.</summary>
    /// <param name="delay">How long each element is held before it is emitted.</param>
    /// <param name="holdback">How many elements may be held at once, and what happens to the next one.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.delay delay holdback)</c>, producing the identical document.</remarks>
    let delay
        (delay: TimeSpan)
        (holdback: Orleans.Dataflow.BufferOptions)
        (source: Source<'T>)
        : Source<'T> =
        via (Flow.delay delay holdback) source

    /// <summary>Holds a source's first element until a duration has passed since the run started.</summary>
    /// <param name="delay">How long after the run starts the first element may be emitted.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.initialDelay delay)</c>, producing the identical document.</remarks>
    let initialDelay (delay: TimeSpan) (source: Source<'T>) : Source<'T> =
        via (Flow.initialDelay delay) source

    /// <summary>Fails the run when a source goes quiet for longer than a declared gap.</summary>
    /// <param name="gap">The greatest silence allowed between two elements, and before the first.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.timeout gap)</c>, producing the identical document.</remarks>
    let timeout (gap: TimeSpan) (source: Source<'T>) : Source<'T> = via (Flow.timeout gap) source

    /// <summary>Ends a source's stream when a duration has passed since the run started.</summary>
    /// <param name="window">How long after the run starts the stream ends.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The bounded source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.takeWithin window)</c>, producing the identical document.</remarks>
    let takeWithin (window: TimeSpan) (source: Source<'T>) : Source<'T> =
        via (Flow.takeWithin window) source

    /// <summary>Drops every element of a source until a duration has passed since the run started.</summary>
    /// <param name="window">How long after the run starts elements begin to pass.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.skipWithin window)</c>, producing the identical document.</remarks>
    let skipWithin (window: TimeSpan) (source: Source<'T>) : Source<'T> =
        via (Flow.skipWithin window) source

    /// <summary>Holds a source to a declared rate, one unit per element.</summary>
    /// <param name="options">The rate, the burst, and what to do with an element there is no budget for.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The paced source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.throttle options)</c>, producing the identical document.</remarks>
    let throttle (options: Orleans.Dataflow.ThrottleOptions) (source: Source<'T>) : Source<'T> =
        via (Flow.throttle options) source

    /// <summary>Holds a source to a declared rate, charged by what each element is worth.</summary>
    /// <param name="options">The rate, the burst, and what to do with an element there is no budget for.</param>
    /// <param name="cost">What one element costs the rate; zero or more.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The paced source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.throttleBy options cost)</c>, producing the identical document.</remarks>
    let throttleBy
        (options: Orleans.Dataflow.ThrottleOptions)
        (cost: 'T -> int)
        (source: Source<'T>)
        : Source<'T> =
        via (Flow.throttleBy options cost) source

    /// <summary>Puts a gate in a source that an author opens and closes while the run is running.</summary>
    /// <param name="controlName">The author-stable name to expose the valve under.</param>
    /// <param name="initialMode">The state the valve starts each run in.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.valve controlName initialMode)</c>, producing the identical document.</remarks>
    let valve
        (controlName: string)
        (initialMode: Orleans.Dataflow.ValveMode)
        (source: Source<'T>)
        : Source<'T> =
        via (Flow.valve controlName initialMode) source

    /// <summary>Answers the failures raised inside a flow applied to a source.</summary>
    /// <param name="options">The form, and the retrying form's attempts, ladder, and exhaustion answer.</param>
    /// <param name="scope">The flow the scope owns the per-element execution of.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.supervised options scope)</c>, producing the identical document.</remarks>
    let supervised
        (options: Orleans.Dataflow.SupervisionOptions)
        (scope: Flow<'In, 'Out>)
        (source: Source<'In>)
        : Source<'Out> =
        via (Flow.supervised options scope) source

    /// <summary>Ends a failing flow's stream with a declared element instead of failing the run.</summary>
    /// <param name="options">The form, which must be the recovering one.</param>
    /// <param name="fallback">The element the scope emits when a failure ends its stream.</param>
    /// <param name="scope">The flow the scope owns the per-element execution of.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.supervisedRecovering options fallback scope)</c>, producing the identical document.</remarks>
    let supervisedRecovering
        (options: Orleans.Dataflow.SupervisionOptions)
        (fallback: 'Out)
        (scope: Flow<'In, 'Out>)
        (source: Source<'In>)
        : Source<'Out> =
        via (Flow.supervisedRecovering options fallback scope) source

    /// <summary>Declares the stages of a source whose state survives a resume.</summary>
    /// <param name="scope">The flow whose stages' state a checkpoint carries.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.durable scope)</c>, producing the identical document.</remarks>
    let durable (scope: Flow<'In, 'Out>) (source: Source<'In>) : Source<'Out> =
        via (Flow.durable scope) source

    /// <summary>Closes a source with a sink that declares no result.</summary>
    /// <param name="sink">The terminal consuming the stream.</param>
    /// <param name="source">The source being closed, which is unchanged.</param>
    /// <returns>The closed graph, ready to materialize.</returns>
    let toSink (sink: Sink<'T>) (source: Source<'T>) : Orleans.Dataflow.RunnableGraph =
        LocalGraphBuilder.Close(source.State.Concat sink.Stages, LocalGraphBuilder.NoSlots)

    /// <summary>Closes a source with a result-bearing sink, naming the slot the result resolves under.</summary>
    /// <param name="slotName">The author-stable name the run handle resolves the result by.</param>
    /// <param name="sink">The terminal folding the stream into the result.</param>
    /// <param name="source">The source being closed, which is unchanged.</param>
    /// <returns>The closed graph and the slot that resolves its result.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="slotName"/> is not a valid single-segment identifier.
    /// </exception>
    /// <remarks>
    /// The slot binds to the document's fingerprint and to this built instance — two graphs of one shape
    /// share a fingerprint, so a slot also remembers which instance declared it, exactly as a C#-declared
    /// slot does (ADR 0004 section 4). The tuple is the composable form; there is no out-parameter
    /// spelling to mirror, because F# already has one.
    /// </remarks>
    let toResult
        (slotName: string)
        (sink: SinkWithResult<'T, 'Result>)
        (source: Source<'T>)
        : Orleans.Dataflow.RunnableGraph * Orleans.Dataflow.ResultSlot<'Result> =
        let slotId =
            match ResultSlotId.TryCreate(slotName) with
            | true, id -> id
            | false, _ ->
                invalidArg
                    (nameof slotName)
                    $"The slot name '{slotName}' is not a valid identifier segment. A result slot is named by a single lowercase segment, such as 'total'."

        let closed = source.State.Concat sink.Stages

        let graph =
            LocalGraphBuilder.Close(
                closed,
                [| LocalSlotRequest(slotId, closed.Stages.Count - 1, null) |])

        graph, Orleans.Dataflow.ResultSlot<'Result>.Create(slotId, graph.Fingerprint, graph.AuthoringNonce)
