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

    /// <summary>Starts a source at one named occurrence of a registered stage.</summary>
    /// <param name="stage">The typed handle of the registered stage, resolved from a catalog.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The deployable counterpart of <see cref="M:Orleans.Dataflow.FSharp.Source.ofSeq``1"/>: where that one
    /// captures a sequence this process happens to hold, this one names a stage a catalog resolves, so the
    /// document says everything about where the elements come from and no CLR value is bound behind it.
    /// Building a graph still starts no work.
    /// </para>
    /// <para>
    /// The handle is the shared plane's own value — this package mirrors none of the registered types,
    /// because they are language-neutral already and a second spelling of one would be a second thing to
    /// keep in step with a catalog. What differs between the frontends is only the shape of the call.
    /// </para>
    /// </remarks>
    let ofRegistered
        (stage: Orleans.Dataflow.RegisteredSource<'T>)
        (occurrenceName: string)
        (parameters: Orleans.Dataflow.Serialization.CanonicalJsonValue)
        : Source<'T> =
        Source<'T>(
            LocalGraphShape.OfChain(
                LocalStageChain.Of(
                    RegisteredAttachment.Occurrence(stage.Specification, occurrenceName, parameters))))

    /// <summary>Extends a source with a flow.</summary>
    /// <param name="flow">The transformation to apply.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The extended source.</returns>
    let via (flow: Flow<'In, 'Out>) (source: Source<'In>) : Source<'Out> =
        Source<'Out>(source.State.Concat flow.Stages)

    /// <summary>Gives the occurrence a source ends at an author-stable name.</summary>
    /// <param name="occurrenceName">The name, which is one identifier segment.</param>
    /// <param name="source">The source being named, which is unchanged.</param>
    /// <returns>The named source.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier.
    /// </exception>
    /// <exception cref="T:System.InvalidOperationException">
    /// The occurrence the source ends at is already named, whether by an earlier
    /// <see cref="M:Orleans.Dataflow.FSharp.Source.named``1"/> or by the registered attachment that created
    /// it. Renaming is refused rather than performed.
    /// </exception>
    /// <remarks>
    /// <para>
    /// It names <em>the occurrence this source ends at</em> — the stage the next function in the pipeline
    /// would attach to. After an operator that is the stage the operator added; after a fan-in it is the
    /// junction; after <see cref="M:Orleans.Dataflow.FSharp.Source.alsoTo``1"/> or
    /// <see cref="M:Orleans.Dataflow.FSharp.Source.divertTo``1"/> it is the tapping junction, which is the
    /// one occurrence such a call contributes that has no other spelling, because the branch named its own
    /// stages where they were written.
    /// </para>
    /// <para>
    /// A closing fan-out takes its junction's name as an argument instead, for the reason a registered one
    /// always has: the call answers with a document, so there is no value left to name the junction on.
    /// </para>
    /// <para>
    /// Everything <see cref="M:Orleans.Dataflow.FSharp.Flow.named``2"/> states about what a name is holds
    /// here: it is the node identifier the occurrence carries into the document, a named graph therefore has
    /// a different fingerprint from the unnamed one, and a graph whose occurrences are all named declares
    /// <c>ephemeral-identity</c> no longer. Two occurrences of one graph sharing a name is refused when the
    /// graph is closed, by the algebra that reports every collision.
    /// </para>
    /// </remarks>
    let named (occurrenceName: string) (source: Source<'T>) : Source<'T> =
        Source<'T>(source.State.Naming(LocalOccurrenceName.Parse(occurrenceName, nameof occurrenceName)))

    /// <summary>Extends a source with one named occurrence of a registered stage.</summary>
    /// <param name="stage">The typed handle of the registered stage, resolved from a catalog.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The extended source.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// Two occurrences of one graph may not share a name; that is reported when the chain is closed, which is
    /// where the whole chain is first visible. Typed parameter builders are provider-SDK sugar and are
    /// deliberately not part of this surface either, for the reason the C# spelling gives: the payload is the
    /// raw canonical value the stage's parameter contract describes, and the graph compiler is what checks it
    /// against that contract.
    /// </remarks>
    let viaRegistered
        (stage: Orleans.Dataflow.RegisteredFlow<'In, 'Out>)
        (occurrenceName: string)
        (parameters: Orleans.Dataflow.Serialization.CanonicalJsonValue)
        (source: Source<'In>)
        : Source<'Out> =
        Source<'Out>(
            source.State.Append(RegisteredAttachment.Occurrence(stage.Specification, occurrenceName, parameters)))

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

    /// <summary>Reads one branch as the leg a composition works over.</summary>
    /// <remarks>
    /// The bridge from the typed branch value to the untyped leg. It exists because the legs of one junction
    /// can carry unlike element types — an unzip's do — so the list a composition walks cannot be typed by
    /// any one of them. A leg carries exactly the three things composition needs: the occurrences, the name
    /// of the result the leg declares, and the binding waiting for the graph that will fill it.
    /// </remarks>
    let private legOf (branch: Branch<'T>) : BranchLeg =
        match branch.Result with
        | ValueSome(name, binding) -> BranchLeg(branch.Stages, Nullable name, binding)
        | ValueNone -> BranchLeg(branch.Stages, Nullable<ResultSlotId>(), null)

    /// <summary>Reads the branches of a fan-out call as legs, having checked how many there are.</summary>
    /// <remarks>
    /// The bound is the shared vocabulary's, so the numbers cannot drift; the sentence is restated rather
    /// than called because the guard that owns it is typed to the C# facade's own branch value, and reaching
    /// it would mean building one of those from an F# branch. That is the detour through the fluent types
    /// this package exists not to take. Nothing downstream restates the bound, so a divergence here would
    /// show as a call this frontend accepts and the other refuses — which is what the arity parity case
    /// asserts.
    /// </remarks>
    let private legsOf (parameterName: string) (branches: Branch<'T> list) : IReadOnlyList<BranchLeg> =
        let legs = branches |> List.map legOf |> List.toArray

        if legs.Length < LocalVocabulary.MinFanOut || legs.Length > LocalVocabulary.MaxFanOut then
            invalidArg
                parameterName
                $"A fan-out junction has between {LocalVocabulary.MinFanOut} and {LocalVocabulary.MaxFanOut} branches, and this call has {legs.Length}. One branch is a chain written the long way, none is a discarding sink, and more than {LocalVocabulary.MaxFanOut} is past the legs a local junction declares."

        legs :> IReadOnlyList<BranchLeg>

    /// <summary>Reads the branches of a registered fan-out call as legs, having checked how many there are.</summary>
    /// <remarks>
    /// A local junction's bound is a range, because the local specifications declare eight ports of which the
    /// first two are required and the rest ignorable. A registered junction's arity is not a range at all: the
    /// stage declares exactly the ports it has and every one of them is wired, so this is an equality and the
    /// diagnostic names the stage that fixed it. The sentence is restated rather than called for the reason
    /// <c>legsOf</c> gives — the guard that owns it is typed to the C# facade's own branch value — and the
    /// registered arity parity case asserts the two frontends refuse the same call with the same words.
    /// </remarks>
    let private registeredLegsOf
        (parameterName: string)
        (legs: int)
        (stage: StageRef)
        (branches: Branch<'T> list)
        : IReadOnlyList<BranchLeg> =
        let wired = branches |> List.map legOf |> List.toArray

        if wired.Length <> legs then
            invalidArg
                parameterName
                $"The registered fan-out '{stage}' declares {legs} output ports, and this call has {wired.Length} branches. A junction's legs are the ports its stage declares, so a branch is written for each one; the order is the specification's own port order."

        wired :> IReadOnlyList<BranchLeg>

    /// <summary>Checks how many streams a registered fan-in call joins, counting the source it was written on.</summary>
    /// <remarks>
    /// The receiver counts, which is why the arithmetic is here rather than at the call site: joining one
    /// source with one other is two streams, and a junction declaring two inputs is the one that fits.
    /// </remarks>
    let private registeredJoined
        (parameterName: string)
        (inputs: int)
        (stage: StageRef)
        (others: 'Other list)
        : 'Other list =
        let joined = List.length others + 1

        if joined <> inputs then
            invalidArg
                parameterName
                $"The registered fan-in '{stage}' declares {inputs} input ports, and this call joins {joined} streams counting the source it was written on. A junction's inputs are the ports its stage declares, so a source is written for each one; the order is the specification's own port order, with the receiver first."

        others

    /// <summary>Closes a source into a graph through a registered fan-out and its legs.</summary>
    /// <remarks>
    /// Both registered fan-out spellings funnel through here, which is what makes the like-legged and the
    /// unlike-legged forms produce byte-identical documents from the same arguments. The ports are read from
    /// the specification rather than from the local vocabulary's numbered names, which is the whole of what a
    /// registered junction changes about composition: a provider names its own ports and the document names
    /// those.
    /// </remarks>
    let private splitToRegistered
        (specification: Orleans.Dataflow.Definition.StageSpecification)
        (occurrenceName: string)
        (parameters: Orleans.Dataflow.Serialization.CanonicalJsonValue)
        (legs: IReadOnlyList<BranchLeg>)
        (source: Source<'T>)
        : Orleans.Dataflow.RunnableGraph =
        let occurrence = RegisteredAttachment.Occurrence(specification, occurrenceName, parameters)
        let position = source.State.Stages.Count

        let shape =
            source.State.Split(
                occurrence,
                LocalJunctionGuard.PortsOf specification.OutputPorts,
                LocalJunctionGuard.Chains legs)

        LocalGraphBuilder.Close(shape, LocalJunctionGuard.Slots(position, legs))

    /// <summary>Joins a source and others into one through a registered fan-in.</summary>
    /// <remarks>
    /// The registered sibling of <c>joinedWith</c>, and the same composition: the source the call was written
    /// on reaches the junction's first declared input port, the first argument the second, and so on.
    /// </remarks>
    let private combineIntoRegistered
        (specification: Orleans.Dataflow.Definition.StageSpecification)
        (occurrenceName: string)
        (parameters: Orleans.Dataflow.Serialization.CanonicalJsonValue)
        (others: LocalGraphShape list)
        (source: Source<'T>)
        : Source<'Out> =
        let occurrence = RegisteredAttachment.Occurrence(specification, occurrenceName, parameters)

        let placed =
            others |> List.fold (fun (shape: LocalGraphShape) other -> shape.Union other) source.State

        Source<'Out>(placed.Combine(occurrence, LocalJunctionGuard.PortsOf specification.InputPorts))

    /// <summary>Builds the unzip junction occurrence for one pair type.</summary>
    /// <remarks>
    /// Both unzip spellings read the projections from here, so the named one writes the document the unnamed
    /// one writes with a single identifier replaced and nothing else moved.
    /// </remarks>
    let private unzipping<'Left, 'Right> () : LocalStageDescriptor =
        LocalStageDescriptor.Unzip(
            Func<struct ('Left * 'Right), 'Left>(fun struct (first, _) -> first),
            Func<struct ('Left * 'Right), 'Right>(fun struct (_, second) -> second))

    /// <summary>Names the junction occurrence a closing fan-out is about to add.</summary>
    /// <remarks>
    /// The one place a name reaches a junction a closing call adds, so all four local fan-out closes check it
    /// with the rule <see cref="M:Orleans.Dataflow.FSharp.Source.named``1"/> uses and report the caller's own
    /// parameter. The caller's parameter name is passed rather than inferred, exactly as it is for a slot
    /// name: inferring it would name this function's parameter and the author wrote the closing call's.
    /// </remarks>
    let private junctionNamed
        (parameterName: string)
        (occurrenceName: string)
        (junction: LocalStageDescriptor)
        : LocalStageDescriptor =
        junction.Named(LocalOccurrenceName.Parse(occurrenceName, parameterName))

    /// <summary>Closes a source into a graph through a fan-out junction and its legs.</summary>
    /// <remarks>
    /// Every terminal fan-out funnels through here, which is what makes a broadcast, a balance, a partition,
    /// and an unzip one operation with four junction stages rather than four implementations: what differs is
    /// the occurrence — named or not — and the leg ports, and the slot each result-bearing leg asks for is the
    /// same arithmetic in all of them. That arithmetic is the shared guard's, not this package's.
    /// </remarks>
    let private fanOutTo
        (junction: LocalStageDescriptor)
        (legs: IReadOnlyList<PortId>)
        (branches: IReadOnlyList<BranchLeg>)
        (source: Source<'T>)
        : Orleans.Dataflow.RunnableGraph =
        let position = source.State.Stages.Count
        let shape = source.State.Split(junction, legs, LocalJunctionGuard.Chains branches)

        LocalGraphBuilder.Close(shape, LocalJunctionGuard.Slots(position, branches))

    /// <summary>Splits a source into the two legs every non-terminal fan-out has.</summary>
    /// <remarks>
    /// A fork, its merging sibling, and a tap are all this shape. A leg with no occurrences of its own leaves
    /// the junction's own leg port open, which is how a tap keeps the main line flowing and how a fork
    /// through the identity flow costs no stage at all.
    /// </remarks>
    let private splitInto
        (junction: LocalStageDescriptor)
        (left: IReadOnlyList<StageOccurrence>)
        (right: IReadOnlyList<StageOccurrence>)
        (source: Source<'T>)
        : LocalGraphShape =
        source.State.Split(junction, LocalJunctionGuard.FanOutPorts LocalVocabulary.MinFanOut, [| left; right |])

    /// <summary>Sends every element the junction accepts to one branch, and continues with what is left.</summary>
    /// <remarks>
    /// The tap and its predicate-routed sibling are one composition over two junctions. A result the branch
    /// declares is carried on the shape rather than answered here: the call does not close a graph, so there
    /// is no graph yet for the slot to belong to, and the request travels until something closes it.
    /// </remarks>
    let private tapping (junction: LocalStageDescriptor) (side: Branch<'T>) (source: Source<'T>) : Source<'T> =
        let shape = splitInto junction LocalStageChain.Empty side.Stages source

        Source<'T>(
            match side.Result with
            | ValueSome(name, binding) -> shape.Declaring(LocalSlotRequest(name, shape.Stages.Count - 1, binding))
            | ValueNone -> shape)

    /// <summary>Joins a source and others into one through a fan-in junction.</summary>
    /// <remarks>
    /// The receiver's occurrences come first and the arguments' follow in order, so the numbering of a join
    /// is the order it was written in and the junction's input ports follow that same order: the source the
    /// call was written on reaches <c>in-0</c>, the first argument <c>in-1</c>, and so on.
    /// </remarks>
    let private joinedWith
        (junction: LocalStageDescriptor)
        (others: LocalGraphShape list)
        (source: Source<'T>)
        : Source<'Out> =
        let placed =
            others |> List.fold (fun (shape: LocalGraphShape) other -> shape.Union other) source.State

        Source<'Out>(placed.Combine(junction, LocalJunctionGuard.FanInPorts(List.length others + 1)))

    /// <summary>Joins a source with another, emitting from whichever of the two has an element.</summary>
    /// <param name="other">The source to merge with, which is unchanged.</param>
    /// <param name="source">The source being joined, which is unchanged.</param>
    /// <returns>A source of both streams' elements.</returns>
    /// <remarks>
    /// The elements of one input keep their order relative to each other and nothing is promised about how
    /// the two interleave: a merge emits what has arrived. The merged stream ends when both inputs have.
    /// Merging four streams is <c>merge d (merge3 b c a)</c>, and that is honestly two junctions rather than
    /// one — merge semantics are associative, but the two documents are distinct and fingerprint differently.
    /// </remarks>
    let merge (other: Source<'T>) (source: Source<'T>) : Source<'T> =
        joinedWith (LocalStageDescriptor.Merge()) [ other.State ] source

    /// <summary>Joins a source with two others, emitting from whichever of the three has an element.</summary>
    /// <param name="second">The second source, which is unchanged.</param>
    /// <param name="third">The third source, which is unchanged.</param>
    /// <param name="source">The source being joined, which is unchanged.</param>
    /// <returns>A source of all three streams' elements.</returns>
    /// <remarks>
    /// One junction with three inputs rather than two junctions, which is a different document from
    /// <c>merge c (merge b a)</c> and is the one to write when the three streams are peers. Three is where
    /// the arities stop, exactly as they do in the other frontend: wider merges chain, and a chain says what
    /// it is. The digit is in the name because this package spells one operation per name and never overloads
    /// one.
    /// </remarks>
    let merge3 (second: Source<'T>) (third: Source<'T>) (source: Source<'T>) : Source<'T> =
        joinedWith (LocalStageDescriptor.Merge()) [ second.State; third.State ] source

    /// <summary>Follows a source with another, emitting the second only after the first has ended.</summary>
    /// <param name="next">The source to emit after this one, which is unchanged.</param>
    /// <param name="source">The source emitted first, which is unchanged.</param>
    /// <returns>A source of this stream followed by that one.</returns>
    /// <remarks>
    /// The ordered fan-in: every element of <paramref name="source"/> is emitted, in order, before the first
    /// element of <paramref name="next"/> is asked for. That is a difference in when the later input is
    /// pulled at all and not only in the order elements come out — a concat holds its later inputs untouched
    /// until their turn.
    /// </remarks>
    let concat (next: Source<'T>) (source: Source<'T>) : Source<'T> =
        joinedWith (LocalStageDescriptor.Concat()) [ next.State ] source

    /// <summary>Joins a source with another by taking a declared number of elements from each in turn.</summary>
    /// <param name="other">The source to interleave with, which is unchanged.</param>
    /// <param name="segmentSize">How many elements to take from one input before moving to the next.</param>
    /// <param name="source">The source being joined, which is unchanged.</param>
    /// <returns>A source of both streams' elements in a fixed rotation.</returns>
    /// <exception cref="T:System.ArgumentOutOfRangeException"><paramref name="segmentSize"/> is below one.</exception>
    /// <remarks>
    /// The deterministic fan-in: unlike a merge, the output order is decided by the rotation and not by which
    /// input happened to have an element. An input that ends is dropped from the rotation and the remaining
    /// ones carry on, so a shorter stream does not end the join. The segment size is the one number a
    /// junction writes into its document, so it changes the fingerprint.
    /// </remarks>
    let interleave (other: Source<'T>) (segmentSize: int) (source: Source<'T>) : Source<'T> =
        joinedWith
            (LocalStageDescriptor.Interleave(LocalOptionGuard.SegmentSize(segmentSize, nameof segmentSize)))
            [ other.State ]
            source

    /// <summary>Joins a source with another through a function of one element from each.</summary>
    /// <param name="other">The source to join with, which is unchanged.</param>
    /// <param name="combine">The function building one element from one element of each input.</param>
    /// <param name="source">The source being joined, which is unchanged.</param>
    /// <returns>A source of one element per element from each input.</returns>
    /// <remarks>
    /// Positional and lockstep: the first element of each input builds the first row, the second of each the
    /// second, and the joined stream ends as soon as either input does — whatever the other still had.
    /// Building the row here rather than pairing first is what keeps a join that immediately projects from
    /// allocating a pair only to take it apart again.
    /// </remarks>
    let zipWith (other: Source<'T2>) (combine: 'T -> 'T2 -> 'Out) (source: Source<'T>) : Source<'Out> =
        joinedWith
            (LocalStageDescriptor.Zip(LocalRowCombiner.Of(Func<'T, 'T2, 'Out> combine)))
            [ other.State ]
            source

    /// <summary>Joins a source with another into a stream of pairs.</summary>
    /// <param name="other">The source to pair with, which is unchanged.</param>
    /// <param name="source">The source being joined, which is unchanged.</param>
    /// <returns>A source of one pair per element from each input.</returns>
    /// <remarks>
    /// The same lockstep join, with the pair built by the tuple. Its members are named by the order the
    /// inputs were written in, and it is a struct tuple because that is the very
    /// <see cref="T:System.ValueTuple`2"/> the other frontend's zip produces — so one graph authored in
    /// either language carries one element type as well as one document, and a zipped stream can be unzipped
    /// again without a conversion in between.
    /// </remarks>
    let zip (other: Source<'T2>) (source: Source<'T>) : Source<struct ('T * 'T2)> =
        zipWith other (fun first second -> struct (first, second)) source

    /// <summary>Joins a source with another by combining each arrival with the other's latest element.</summary>
    /// <param name="other">The source to join with, which is unchanged.</param>
    /// <param name="combine">The function building one element from the latest element of each input.</param>
    /// <param name="source">The source being joined, which is unchanged.</param>
    /// <returns>A source of one element per arrival once both inputs have produced.</returns>
    /// <remarks>
    /// Not a lockstep join and deliberately a different word for it: nothing is emitted until both inputs
    /// have produced at least once, and after that every arrival on either side emits a row built from it and
    /// from whatever the other side last produced. A fast input therefore produces many rows against one slow
    /// element, which is the point — this is the join for a stream against a setting, not for two streams of
    /// matching rows. There is one form because the other frontend has one: a row is always the author's to
    /// build.
    /// </remarks>
    let combineLatest (other: Source<'T2>) (combine: 'T -> 'T2 -> 'Out) (source: Source<'T>) : Source<'Out> =
        joinedWith
            (LocalStageDescriptor.CombineLatest(LocalRowCombiner.Of(Func<'T, 'T2, 'Out> combine)))
            [ other.State ]
            source

    /// <summary>Emits another source's elements before this one's.</summary>
    /// <param name="head">The source to emit first, which is unchanged.</param>
    /// <param name="source">The source emitted second, which is unchanged.</param>
    /// <returns>A source of that stream followed by this one.</returns>
    /// <remarks>
    /// <c>prepend b a</c> is <c>concat a b</c> and is exactly that document, junction and all: this is the
    /// spelling for when the stream being extended is the one already in hand, which is what a pipeline of
    /// operators leaves an author holding. Everything <see cref="M:Orleans.Dataflow.FSharp.Source.concat``1"/>
    /// promises therefore holds here, including the one that costs something — the later input's source is
    /// running and parked in its own bounded channel while the earlier one plays out.
    /// </remarks>
    let prepend (head: Source<'T>) (source: Source<'T>) : Source<'T> =
        joinedWith (LocalStageDescriptor.Concat()) [ source.State ] head

    /// <summary>Emits another source's elements after this one's.</summary>
    /// <param name="tail">The source to emit last, which is unchanged.</param>
    /// <param name="source">The source emitted first, which is unchanged.</param>
    /// <returns>A source of this stream followed by that one.</returns>
    /// <remarks>
    /// The same junction <see cref="M:Orleans.Dataflow.FSharp.Source.concat``1"/> builds, under the name the
    /// sequence-edit vocabulary uses, and deliberately the same document rather than a second one. Which word
    /// to write is a question of what the author is saying — joining two streams, or extending one. A fixed
    /// run of elements is <c>append (Source.ofSeq [ … ]) source</c>, which is the document the other
    /// frontend's element overload builds and is why there is no second name for it here.
    /// </remarks>
    let append (tail: Source<'T>) (source: Source<'T>) : Source<'T> =
        concat tail source

    /// <summary>Joins a source and others through one named occurrence of a registered junction.</summary>
    /// <param name="junction">The typed handle of the registered junction, whose input count is its stage's own.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <param name="others">
    /// The sources joined with this one, in the specification's own port order after it; each is unchanged.
    /// </param>
    /// <param name="source">The source being joined, which reaches the first input port and is unchanged.</param>
    /// <returns>The joined source.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// The source plus <paramref name="others"/> are not exactly the junction's declared inputs,
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// A combinator on sources, exactly as <see cref="M:Orleans.Dataflow.FSharp.Source.merge``1"/> is: the
    /// chain continues from the junction's one output. What the junction does with the streams it joins —
    /// merge, concatenate, interleave — is the provider's and is stated by the runtime its factory builds,
    /// which is why nothing here takes a combiner. That is the difference between a registered junction and a
    /// local one, and it is the same difference every registered stage has.
    /// </remarks>
    let fanInRegistered
        (junction: Orleans.Dataflow.RegisteredFanIn<'T, 'Out>)
        (occurrenceName: string)
        (parameters: Orleans.Dataflow.Serialization.CanonicalJsonValue)
        (others: Source<'T> list)
        (source: Source<'T>)
        : Source<'Out> =
        let joined = registeredJoined (nameof others) junction.Inputs junction.Stage others

        combineIntoRegistered
            junction.Specification
            occurrenceName
            parameters
            (joined |> List.map (fun other -> other.State))
            source

    /// <summary>Joins a source and one unlike other through a named occurrence of a registered junction.</summary>
    /// <param name="junction">The typed handle of the registered junction, whose two inputs carry unlike types.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <param name="other">The source wired to the junction's second input port, which is unchanged.</param>
    /// <param name="source">The source wired to its first input port, which is unchanged.</param>
    /// <returns>The joined source.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// The zip-shaped join: two unlike streams in, one row out. It is a second name rather than an overload
    /// because the two are different operations — one joins any number of like streams and reads its arity
    /// from the stage, the other joins exactly two whose element types differ and could not be a list at all.
    /// First and second are the specification's own port order.
    /// </remarks>
    let fanInRegisteredPair
        (junction: Orleans.Dataflow.RegisteredFanIn<'T, 'Second, 'Out>)
        (occurrenceName: string)
        (parameters: Orleans.Dataflow.Serialization.CanonicalJsonValue)
        (other: Source<'Second>)
        (source: Source<'T>)
        : Source<'Out> =
        combineIntoRegistered junction.Specification occurrenceName parameters [ other.State ] source

    /// <summary>Sends every element to a branch as well, and continues.</summary>
    /// <param name="side">The branch to tap into, which is unchanged.</param>
    /// <param name="source">The source being tapped, which is unchanged.</param>
    /// <returns>A source of the same elements.</returns>
    /// <remarks>
    /// The tap, and broadcast sugar underneath: a junction with the main line on its first leg and the branch
    /// on its second, so every element reaches both. What that costs is the broadcast's own rule — an element
    /// is delivered to every leg, so a branch that stops consuming holds the main line up. A tap is not a
    /// fire-and-forget side effect. A branch that declares a result is welcome here: the result is carried
    /// until the graph is closed and declared then, beside whatever the main line declares.
    /// </remarks>
    let alsoTo (side: Branch<'T>) (source: Source<'T>) : Source<'T> =
        tapping (LocalStageDescriptor.Broadcast()) side source

    /// <summary>Sends the elements a predicate accepts to a branch, and continues with the rest.</summary>
    /// <param name="predicate">The test deciding which elements leave the main line.</param>
    /// <param name="side">The branch the accepted elements go to, which is unchanged.</param>
    /// <param name="source">The source being diverted, which is unchanged.</param>
    /// <returns>A source of the elements the predicate rejected.</returns>
    /// <remarks>
    /// Partition sugar, and the two-legged partition exactly: the accepted elements go to the branch and
    /// nothing else does, so unlike a tap this junction never duplicates an element. It is the shape a
    /// validation stage wants — the rejects to a dead-letter sink, everything else onward. What it costs is
    /// the partition's own rule: the junction holds one element and waits for the leg that element belongs
    /// on, so a diverted element the branch is slow to take holds the main line up for exactly as long.
    /// </remarks>
    let divertTo (predicate: 'T -> bool) (side: Branch<'T>) (source: Source<'T>) : Source<'T> =
        tapping
            (LocalStageDescriptor.Partition(Func<'T, int>(fun element -> if predicate element then 1 else 0)))
            side
            source

    /// <summary>Sends every element down two flows at once, to be rejoined.</summary>
    /// <param name="left">The first derivation, which is unchanged.</param>
    /// <param name="right">The second derivation, which is unchanged.</param>
    /// <param name="source">The source being forked, which is unchanged.</param>
    /// <returns>The fork, which is closed by one of the <c>Fork</c> module's own functions.</returns>
    /// <remarks>
    /// The one shape a pipeline cannot express: the same element travels two paths and the paths meet again.
    /// Every element is broadcast to both flows, so the two derived streams advance together — which is what
    /// makes <see cref="M:Orleans.Dataflow.FSharp.Fork.zip``2"/> a join that needs no buffer between the
    /// halves. The fork has two open ends and no way to close a graph, so a program that builds one has to
    /// rejoin it.
    /// </remarks>
    let fork (left: Flow<'T, 'T1>) (right: Flow<'T, 'T2>) (source: Source<'T>) : Fork<'T1, 'T2> =
        Fork<'T1, 'T2>(splitInto (LocalStageDescriptor.Broadcast()) left.Stages right.Stages source)

    /// <summary>Sends every element down two flows at once through a named junction, to be rejoined.</summary>
    /// <param name="occurrenceName">The author-stable name of the broadcasting junction occurrence.</param>
    /// <param name="left">The first derivation, which is unchanged.</param>
    /// <param name="right">The second derivation, which is unchanged.</param>
    /// <param name="source">The source being forked, which is unchanged.</param>
    /// <returns>The fork, which is closed by one of the <c>Fork</c> module's own functions.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier.
    /// </exception>
    /// <remarks>
    /// The name is an argument here for the reason it is one on a closing fan-out:
    /// <see cref="M:Orleans.Dataflow.FSharp.Source.named``1"/> names the occurrence a source ends at, and a
    /// fork ends at two — so the junction it was split by has no other spelling. The rejoin is a source again
    /// and is named the usual way, which is why one name is written here and not two.
    /// </remarks>
    let forkNamed
        (occurrenceName: string)
        (left: Flow<'T, 'T1>)
        (right: Flow<'T, 'T2>)
        (source: Source<'T>)
        : Fork<'T1, 'T2> =
        Fork<'T1, 'T2>(
            splitInto
                (junctionNamed (nameof occurrenceName) occurrenceName (LocalStageDescriptor.Broadcast()))
                left.Stages
                right.Stages
                source)

    /// <summary>Sends every element down two flows at once and takes whichever result arrives first.</summary>
    /// <param name="left">The first derivation, which is unchanged.</param>
    /// <param name="right">The second derivation, which is unchanged.</param>
    /// <param name="source">The source being forked, which is unchanged.</param>
    /// <returns>A source of both derivations' elements.</returns>
    /// <remarks>
    /// The unordered rejoin, and the shape a race is written in: one element in produces two elements out —
    /// one per path — in whatever order the paths finish. That is a merge and not a zip, so the two
    /// derivations of one element are not paired and nothing waits for the slower path before emitting the
    /// faster one. <see cref="M:Orleans.Dataflow.FSharp.Source.fork``3"/> is the rejoin for when the two
    /// derivations belong together.
    /// </remarks>
    let forkMerge (left: Flow<'T, 'Out>) (right: Flow<'T, 'Out>) (source: Source<'T>) : Source<'Out> =
        let diamond = splitInto (LocalStageDescriptor.Broadcast()) left.Stages right.Stages source

        Source<'Out>(
            diamond.Combine(LocalStageDescriptor.Merge(), LocalJunctionGuard.FanInPorts LocalVocabulary.MinFanIn))

    /// <summary>Sends every element down two flows at once through a named junction, taking whichever arrives first.</summary>
    /// <param name="occurrenceName">The author-stable name of the broadcasting junction occurrence.</param>
    /// <param name="left">The first derivation, which is unchanged.</param>
    /// <param name="right">The second derivation, which is unchanged.</param>
    /// <param name="source">The source being forked, which is unchanged.</param>
    /// <returns>A source of both derivations' elements.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier.
    /// </exception>
    /// <remarks>
    /// This function adds two junctions — the broadcast that splits and the merge that rejoins — and names
    /// one of them. The merge is the occurrence the answering source ends at, so it is named with
    /// <see cref="M:Orleans.Dataflow.FSharp.Source.named``1"/>; the broadcast is the one with no other
    /// spelling, which is why it is the one this argument is for.
    /// </remarks>
    let forkMergeNamed
        (occurrenceName: string)
        (left: Flow<'T, 'Out>)
        (right: Flow<'T, 'Out>)
        (source: Source<'T>)
        : Source<'Out> =
        let diamond =
            splitInto
                (junctionNamed (nameof occurrenceName) occurrenceName (LocalStageDescriptor.Broadcast()))
                left.Stages
                right.Stages
                source

        Source<'Out>(
            diamond.Combine(LocalStageDescriptor.Merge(), LocalJunctionGuard.FanInPorts LocalVocabulary.MinFanIn))

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
    /// slot does. The tuple is the composable form; there is no out-parameter
    /// spelling to mirror, because F# already has one.
    /// </remarks>
    let toResult
        (slotName: string)
        (sink: SinkWithResult<'T, 'Result>)
        (source: Source<'T>)
        : Orleans.Dataflow.RunnableGraph * Orleans.Dataflow.ResultSlot<'Result> =
        let slotId = Bindings.slotId (nameof slotName) slotName
        let closed = source.State.Concat sink.Stages

        let graph =
            LocalGraphBuilder.Close(
                closed,
                [| LocalSlotRequest(slotId, closed.Stages.Count - 1, null) |])

        graph, Orleans.Dataflow.ResultSlot<'Result>.Create(slotId, graph.Fingerprint, graph.AuthoringNonce)

    /// <summary>Closes a source with one named occurrence of a registered terminal that declares no result.</summary>
    /// <param name="stage">The typed handle of the registered stage terminating the graph.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <param name="source">The source being closed, which is unchanged.</param>
    /// <returns>The closed graph, ready to materialize.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// A registered stage that does declare a result port is a
    /// <see cref="T:Orleans.Dataflow.RegisteredSinkWithResult`2"/> and does not convert to a
    /// <see cref="T:Orleans.Dataflow.RegisteredSink`1"/> at all, so this function cannot drop a result: the
    /// mistake is a type error naming both handles rather than a graph that silently produces nothing
    /// readable. A chain of registered stages closed here declares neither <c>nondeployable</c> nor
    /// <c>ephemeral-identity</c>, which is what <see cref="M:Orleans.Dataflow.FSharp.Pipeline.define"/>
    /// requires of it.
    /// </remarks>
    let toRegistered
        (stage: Orleans.Dataflow.RegisteredSink<'T>)
        (occurrenceName: string)
        (parameters: Orleans.Dataflow.Serialization.CanonicalJsonValue)
        (source: Source<'T>)
        : Orleans.Dataflow.RunnableGraph =
        LocalGraphBuilder.Close(
            source.State.Append(RegisteredAttachment.Occurrence(stage.Specification, occurrenceName, parameters)),
            LocalGraphBuilder.NoSlots)

    /// <summary>Closes a source with one named occurrence of a registered result-bearing terminal.</summary>
    /// <param name="slotName">The author-stable name the run handle resolves the result by.</param>
    /// <param name="stage">The typed handle of the registered stage terminating the graph.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <param name="source">The source being closed, which is unchanged.</param>
    /// <returns>The closed graph and the slot that resolves its result.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="slotName"/> is not a valid single-segment identifier,
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The two names mean different things and neither is derivable from the other: the occurrence name is
    /// the node's durable identity in the graph, and the slot name is what a run handle resolves the result
    /// under. The slot name comes first because every result-declaring call in this package puts it there.
    /// </para>
    /// <para>
    /// The slot binds to the authoring nonce exactly as a lambda graph's does, because this is still a
    /// <see cref="T:Orleans.Dataflow.RunnableGraph"/>. A pipeline binds slots by fingerprint and lineage
    /// without a nonce, and turning this graph into one is
    /// <see cref="M:Orleans.Dataflow.FSharp.Pipeline.define"/>'s business; the slot a pipeline's run resolves
    /// is recovered from the pipeline rather than kept from here.
    /// </para>
    /// </remarks>
    let toRegisteredResult
        (slotName: string)
        (stage: Orleans.Dataflow.RegisteredSinkWithResult<'T, 'Result>)
        (occurrenceName: string)
        (parameters: Orleans.Dataflow.Serialization.CanonicalJsonValue)
        (source: Source<'T>)
        : Orleans.Dataflow.RunnableGraph * Orleans.Dataflow.ResultSlot<'Result> =
        let slotId = Bindings.slotId (nameof slotName) slotName

        let closed =
            source.State.Append(RegisteredAttachment.Occurrence(stage.Specification, occurrenceName, parameters))

        let graph =
            LocalGraphBuilder.Close(
                closed,
                [| LocalSlotRequest(slotId, closed.Stages.Count - 1, null) |])

        graph, Orleans.Dataflow.ResultSlot<'Result>.Create(slotId, graph.Fingerprint, graph.AuthoringNonce)

    /// <summary>Closes a source by delivering every element to every branch.</summary>
    /// <param name="branches">The branches, in the order they are wired to the junction's legs.</param>
    /// <param name="source">The source being closed, which is unchanged.</param>
    /// <returns>The closed graph, ready to materialize.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// There are fewer than two branches or more than the eight a local junction declares legs for, or two
    /// branches declare a result under one name.
    /// </exception>
    /// <remarks>
    /// <para>
    /// A closing call, exactly as <see cref="M:Orleans.Dataflow.FSharp.Source.toSink``1"/> is: the branches
    /// end in terminals, so nothing is left open. Every element reaches every branch, which means a branch
    /// that stops consuming holds up all of them — a broadcast asks each leg for room before it pulls, and
    /// that is the bounded memory this junction buys.
    /// </para>
    /// <para>
    /// Branch order is argument order and is identity-bearing: the first branch's occurrences are numbered
    /// before the second's, so swapping two elements of the list builds a different document with a different
    /// fingerprint. That is the same rule reordering a pipeline follows. The slots of result-bearing branches
    /// are already in the author's hand, because a branch names its result where its terminal is written, so
    /// nothing is answered here but the graph.
    /// </para>
    /// </remarks>
    let broadcastTo (branches: Branch<'T> list) (source: Source<'T>) : Orleans.Dataflow.RunnableGraph =
        let legs = legsOf (nameof branches) branches

        fanOutTo (LocalStageDescriptor.Broadcast()) (LocalJunctionGuard.FanOutPorts legs.Count) legs source

    /// <summary>Closes a source by delivering every element to every branch, through a named junction.</summary>
    /// <param name="occurrenceName">The author-stable name of the junction occurrence.</param>
    /// <param name="branches">The branches, in the order they are wired to the junction's legs.</param>
    /// <param name="source">The source being closed, which is unchanged.</param>
    /// <returns>The closed graph, ready to materialize.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, there are fewer than
    /// two branches or more than the eight a local junction declares legs for, or two branches declare a
    /// result under one name.
    /// </exception>
    /// <remarks>
    /// The name is an argument here rather than an <see cref="M:Orleans.Dataflow.FSharp.Source.named``1"/>
    /// call, and the reason is the shape of the call rather than a second rule about junctions: this function
    /// adds a junction occurrence <em>and</em> closes the graph, so it answers with a document and there is no
    /// value left to name it on. That is how
    /// <see cref="M:Orleans.Dataflow.FSharp.Source.fanOutToRegistered``2"/> has always spelled it, and it is
    /// the one spelling that lets a branching graph of local stages be named to its last occurrence.
    /// </remarks>
    let broadcastToNamed
        (occurrenceName: string)
        (branches: Branch<'T> list)
        (source: Source<'T>)
        : Orleans.Dataflow.RunnableGraph =
        let legs = legsOf (nameof branches) branches

        fanOutTo
            (junctionNamed (nameof occurrenceName) occurrenceName (LocalStageDescriptor.Broadcast()))
            (LocalJunctionGuard.FanOutPorts legs.Count)
            legs
            source

    /// <summary>Closes a source by delivering each element to one branch that has room.</summary>
    /// <param name="branches">The branches, in the order they are wired to the junction's legs.</param>
    /// <param name="source">The source being closed, which is unchanged.</param>
    /// <returns>The closed graph, ready to materialize.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// There are fewer than two branches or more than the eight a local junction declares legs for, or two
    /// branches declare a result under one name.
    /// </exception>
    /// <remarks>
    /// Every element goes to exactly one branch and which one is not defined: a balance hands an element to
    /// whichever leg is ready for it, which is what makes it the junction for spreading work rather than for
    /// classifying it. The branches are usually the same pipeline written twice, and nothing requires them to
    /// be.
    /// </remarks>
    let balanceTo (branches: Branch<'T> list) (source: Source<'T>) : Orleans.Dataflow.RunnableGraph =
        let legs = legsOf (nameof branches) branches

        fanOutTo (LocalStageDescriptor.Balance()) (LocalJunctionGuard.FanOutPorts legs.Count) legs source

    /// <summary>Closes a source by delivering each element to one branch that has room, through a named junction.</summary>
    /// <param name="occurrenceName">The author-stable name of the junction occurrence.</param>
    /// <param name="branches">The branches, in the order they are wired to the junction's legs.</param>
    /// <param name="source">The source being closed, which is unchanged.</param>
    /// <returns>The closed graph, ready to materialize.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, there are fewer than
    /// two branches or more than the eight a local junction declares legs for, or two branches declare a
    /// result under one name.
    /// </exception>
    /// <remarks>
    /// The name is an argument for the reason it is one on
    /// <see cref="M:Orleans.Dataflow.FSharp.Source.broadcastToNamed``1"/>: the call adds a junction and closes
    /// the graph in one step, so it answers with a document and there is no value left to name the junction
    /// on.
    /// </remarks>
    let balanceToNamed
        (occurrenceName: string)
        (branches: Branch<'T> list)
        (source: Source<'T>)
        : Orleans.Dataflow.RunnableGraph =
        let legs = legsOf (nameof branches) branches

        fanOutTo
            (junctionNamed (nameof occurrenceName) occurrenceName (LocalStageDescriptor.Balance()))
            (LocalJunctionGuard.FanOutPorts legs.Count)
            legs
            source

    /// <summary>Closes a source by sending each element to the branch a function names.</summary>
    /// <param name="router">The function answering the zero-based position of the branch for an element.</param>
    /// <param name="branches">The branches, in the order the router's answers index them.</param>
    /// <param name="source">The source being closed, which is unchanged.</param>
    /// <returns>The closed graph, ready to materialize.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// There are fewer than two branches or more than the eight a local junction declares legs for, or two
    /// branches declare a result under one name.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The classifying fan-out: the router sees the element and answers which branch it belongs on, so the
    /// branches are the classes and their order is the numbering the router answers in. Every element goes to
    /// exactly one branch, so unlike a broadcast this junction never duplicates and unlike a balance it is
    /// completely determined by the element.
    /// </para>
    /// <para>
    /// An answer outside the wired branches faults the run when it happens, not when the graph is built: how
    /// many branches an occurrence has is stated by its edges, and a function is not something a document can
    /// check. The router never enters the document either, which is why a partitioned graph is
    /// <c>nondeployable</c>.
    /// </para>
    /// </remarks>
    let partitionTo
        (router: 'T -> int)
        (branches: Branch<'T> list)
        (source: Source<'T>)
        : Orleans.Dataflow.RunnableGraph =
        let legs = legsOf (nameof branches) branches

        fanOutTo
            (LocalStageDescriptor.Partition(Func<'T, int> router))
            (LocalJunctionGuard.FanOutPorts legs.Count)
            legs
            source

    /// <summary>Closes a source by sending each element to the branch a function names, through a named junction.</summary>
    /// <param name="router">The function answering the zero-based position of the branch for an element.</param>
    /// <param name="occurrenceName">The author-stable name of the junction occurrence.</param>
    /// <param name="branches">The branches, in the order the router's answers index them.</param>
    /// <param name="source">The source being closed, which is unchanged.</param>
    /// <returns>The closed graph, ready to materialize.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, there are fewer than
    /// two branches or more than the eight a local junction declares legs for, or two branches declare a
    /// result under one name.
    /// </exception>
    /// <remarks>
    /// The name comes after the router for the reason it comes after the junction handle on
    /// <see cref="M:Orleans.Dataflow.FSharp.Source.fanOutToRegistered``2"/>: what the stage is reads first,
    /// and what this occurrence of it is called reads second. Naming the occurrence does not make the router
    /// a document's business — a function never enters one, so a partitioned graph is <c>nondeployable</c>
    /// named or not.
    /// </remarks>
    let partitionToNamed
        (router: 'T -> int)
        (occurrenceName: string)
        (branches: Branch<'T> list)
        (source: Source<'T>)
        : Orleans.Dataflow.RunnableGraph =
        let legs = legsOf (nameof branches) branches

        fanOutTo
            (junctionNamed
                (nameof occurrenceName)
                occurrenceName
                (LocalStageDescriptor.Partition(Func<'T, int> router)))
            (LocalJunctionGuard.FanOutPorts legs.Count)
            legs
            source

    /// <summary>Closes a source of pairs by sending each half of every pair to a branch of its own.</summary>
    /// <param name="left">The branch the left halves take, which is unchanged.</param>
    /// <param name="right">The branch the right halves take, which is unchanged.</param>
    /// <param name="source">The source of pairs being closed, which is unchanged.</param>
    /// <returns>The closed graph, ready to materialize.</returns>
    /// <exception cref="T:System.ArgumentException">Both branches declare a result under one name.</exception>
    /// <remarks>
    /// <para>
    /// The one fan-out whose legs are differently typed, and the reason its arity is fixed at two rather than
    /// open like a broadcast's: the halves of a pair are two, and each one's type is a type argument. Both
    /// halves of every pair are delivered, so this junction is a broadcast in its flow control — a branch that
    /// stops consuming holds the other one up — and a split in its elements.
    /// </para>
    /// <para>
    /// The pair is a struct tuple, which is the row
    /// <see cref="M:Orleans.Dataflow.FSharp.Source.zip``2"/> and the other frontend's own zip both produce, so
    /// a zipped stream unzips again with nothing in between. It costs something and the cost is worth
    /// stating: a source of ordinary F# tuples is a source of a different CLR type and does not fit here, so
    /// a stream of pairs meant to be unzipped is written <c>struct (left, right)</c> at whatever built it.
    /// That is the price of one element type across the two frontends, and of not allocating a reference
    /// tuple per row only to take it apart again. The two projections are ordinary functions of a pair and
    /// never enter the document, which is what makes the halves' element types the compiler's business rather
    /// than the graph compiler's.
    /// </para>
    /// </remarks>
    let unzipTo
        (left: Branch<'Left>)
        (right: Branch<'Right>)
        (source: Source<struct ('Left * 'Right)>)
        : Orleans.Dataflow.RunnableGraph =
        let legs = [| legOf left; legOf right |] :> IReadOnlyList<BranchLeg>

        fanOutTo
            (unzipping<'Left, 'Right> ())
            [| LocalVocabulary.LeftPort; LocalVocabulary.RightPort |]
            legs
            source

    /// <summary>Closes a source of pairs through a named unzip junction, one branch per half.</summary>
    /// <param name="occurrenceName">The author-stable name of the junction occurrence.</param>
    /// <param name="left">The branch the left halves take, which is unchanged.</param>
    /// <param name="right">The branch the right halves take, which is unchanged.</param>
    /// <param name="source">The source of pairs being closed, which is unchanged.</param>
    /// <returns>The closed graph, ready to materialize.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, or both branches
    /// declare a result under one name.
    /// </exception>
    /// <remarks>
    /// The name is an argument for the reason it is one on
    /// <see cref="M:Orleans.Dataflow.FSharp.Source.broadcastToNamed``1"/>: the call adds a junction and closes
    /// the graph in one step, so it answers with a document and there is no value left to name the junction
    /// on. The two projections still never enter the document, so an unzipped graph stays
    /// <c>nondeployable</c> whether or not its junction is named.
    /// </remarks>
    let unzipToNamed
        (occurrenceName: string)
        (left: Branch<'Left>)
        (right: Branch<'Right>)
        (source: Source<struct ('Left * 'Right)>)
        : Orleans.Dataflow.RunnableGraph =
        let legs = [| legOf left; legOf right |] :> IReadOnlyList<BranchLeg>

        fanOutTo
            (junctionNamed (nameof occurrenceName) occurrenceName (unzipping<'Left, 'Right> ()))
            [| LocalVocabulary.LeftPort; LocalVocabulary.RightPort |]
            legs
            source

    /// <summary>Closes a source through one named occurrence of a registered junction and its branches.</summary>
    /// <param name="junction">The typed handle of the registered junction, whose leg count is its stage's own.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <param name="branches">One branch per declared leg, in the specification's own port order.</param>
    /// <param name="source">The source being closed, which is unchanged.</param>
    /// <returns>The closed graph, ready to materialize.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// There is not exactly one branch per declared leg, <paramref name="occurrenceName"/> is not a valid
    /// single-segment node identifier, <paramref name="parameters"/> is the default value or the JSON null
    /// value, or two branches declare a result under one name.
    /// </exception>
    /// <remarks>
    /// <para>
    /// A closing call, exactly as <see cref="M:Orleans.Dataflow.FSharp.Source.broadcastTo``1"/> is: the
    /// branches end in terminals, so nothing is left open. What differs is that every part of it can be
    /// registered — the junction is named, its ports carry real contracts, and its behavior is resolved from
    /// a catalog — so this is the call that makes a branching pipeline deployable, provided its branches are
    /// registered too.
    /// </para>
    /// <para>
    /// Which leg is which is the specification's canonical port order, ordinal by port name, and not anything
    /// this call decides. Branch order is argument order and is identity-bearing exactly as it is for a local
    /// fan-out: the first branch's occurrences are numbered before the second's. What the junction does with
    /// an element — every leg, one leg with room, the leg a function names — is the provider's; a document
    /// says which stage stands here, and behavior is resolved by identity.
    /// </para>
    /// </remarks>
    let fanOutToRegistered
        (junction: Orleans.Dataflow.RegisteredFanOut<'T, 'Out>)
        (occurrenceName: string)
        (parameters: Orleans.Dataflow.Serialization.CanonicalJsonValue)
        (branches: Branch<'Out> list)
        (source: Source<'T>)
        : Orleans.Dataflow.RunnableGraph =
        let legs = registeredLegsOf (nameof branches) junction.Legs junction.Stage branches

        splitToRegistered junction.Specification occurrenceName parameters legs source

    /// <summary>Closes a source through a named occurrence of a registered junction with two unlike legs.</summary>
    /// <param name="junction">The typed handle of the registered junction, whose legs carry unlike types.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <param name="left">The branch wired to the junction's first output port, which is unchanged.</param>
    /// <param name="right">The branch wired to its second output port, which is unchanged.</param>
    /// <param name="source">The source being closed, which is unchanged.</param>
    /// <returns>The closed graph, ready to materialize.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier,
    /// <paramref name="parameters"/> is the default value or the JSON null value, or both branches declare a
    /// result under one name.
    /// </exception>
    /// <remarks>
    /// The unzip-shaped close: one element in, two unlike things out. It is a second name rather than an
    /// overload for the reason <see cref="M:Orleans.Dataflow.FSharp.Source.fanInRegisteredPair``3"/> is — the
    /// two branches are two arguments because their element types differ and a list of them has no element
    /// type. First and second are the specification's own port order.
    /// </remarks>
    let fanOutToRegisteredPair
        (junction: Orleans.Dataflow.RegisteredFanOut<'T, 'Left, 'Right>)
        (occurrenceName: string)
        (parameters: Orleans.Dataflow.Serialization.CanonicalJsonValue)
        (left: Branch<'Left>)
        (right: Branch<'Right>)
        (source: Source<'T>)
        : Orleans.Dataflow.RunnableGraph =
        let legs = [| legOf left; legOf right |] :> IReadOnlyList<BranchLeg>

        splitToRegistered junction.Specification occurrenceName parameters legs source
