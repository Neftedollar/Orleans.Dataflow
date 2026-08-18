namespace Orleans.Dataflow.Runtime;

/// <summary>
/// What the last segment of a plan does with an element that reaches it, and what the run owes the author
/// when the stream ends.
/// </summary>
/// <remarks>
/// <para>
/// Every terminal the local vocabulary has is one fold over the run's state and a few extra facts: whether
/// the first element is enough, whether there had to be one, what the accumulated state becomes when the
/// run succeeds, and what has to be closed when the run ends however it ends. A discarding sink is the
/// absence of a terminal rather than a terminal that does nothing, and so is an asynchronous callback sink,
/// whose whole work is the callback its segment already drives.
/// </para>
/// <para>
/// The fold receives the run's own context because a terminal may wait: a sink that writes into a bounded
/// channel holds the segment's thread until there is room, and a probe sink holds it until a receiver asks
/// for the element, which is the same backpressure a slow synchronous callback applies. A terminal that
/// waits needs what a source that waits needs — the run's token to abandon by, the stop token to tell a
/// graceful stop from an abandonment, and the pause gate to report a wait to. Every other terminal ignores
/// all three, and passing them to all of them keeps the element path one shape rather than one shape plus a
/// special case.
/// </para>
/// <para>
/// The state itself is not here. It belongs to the run, because a run is what a state is fresh per, and a
/// plan is shared by none: <see cref="LocalEnding.Seed"/> and <see cref="LocalEnding.SeedFactory"/> are
/// where a run starts and this is what moves it. A graph with several sinks has several endings and
/// therefore several states, and one terminal instance never sees another's.
/// </para>
/// </remarks>
internal sealed class LocalTerminal
{
    /// <summary>Initializes a new instance of the <see cref="LocalTerminal"/> class.</summary>
    /// <param name="folder">The fold over boxed state, boxed elements, and the run's own context.</param>
    /// <param name="completesOnFirstElement">Whether one element is the whole of what this terminal wants.</param>
    /// <param name="requiresElement">Whether a stream that ended with no element is a failure.</param>
    /// <param name="element">Which element this terminal is about, when it requires one.</param>
    /// <param name="finisher">The projection of the final state into the result, when there is one.</param>
    /// <param name="closing">What to release when the run ends, when there is anything.</param>
    private LocalTerminal(
        Func<object?, object?, LocalRunContext, object?> folder,
        bool completesOnFirstElement,
        bool requiresElement,
        string element = "first",
        Func<object?, object?>? finisher = null,
        Action<Exception?>? closing = null)
    {
        Folder = folder;
        CompletesOnFirstElement = completesOnFirstElement;
        RequiresElement = requiresElement;
        Element = element;
        Finisher = finisher;
        Closing = closing;
    }

    /// <summary>Gets the fold this terminal applies to every element that reaches it.</summary>
    /// <value>
    /// The author's own folder for an aggregate, the increment of a count, the callback of a synchronous
    /// per-element sink written as a fold that keeps its state, the replacement of the state by the element
    /// for a first-or-last-element sink, the append of a collecting one, and the write of a channel sink.
    /// </value>
    internal Func<object?, object?, LocalRunContext, object?> Folder { get; }

    /// <summary>Gets a value indicating whether the first element completes the run.</summary>
    /// <value>
    /// <see langword="true"/> for the two first-element sinks, which end the run exactly as a
    /// <c>Take(1)</c> in their place would.
    /// </value>
    internal bool CompletesOnFirstElement { get; }

    /// <summary>Gets a value indicating whether a stream that ended with no element is a failure.</summary>
    /// <value>
    /// <see langword="true"/> for the strict first-element and last-element sinks. The honest variants
    /// resolve the default value they were given as their seed, and every other terminal has a state that
    /// already means something when nothing arrived.
    /// </value>
    internal bool RequiresElement { get; }

    /// <summary>Gets which element of the stream this terminal is about.</summary>
    /// <value>The word <c>first</c> or the word <c>last</c>, read only when <see cref="RequiresElement"/>.</value>
    /// <remarks>
    /// The failure of an empty stream names the element the author asked for and the sink that answers
    /// honestly instead, and those two sentences differ by this word alone.
    /// </remarks>
    internal string Element { get; }

    /// <summary>Gets the projection of the accumulated state into the value the result slot resolves.</summary>
    /// <value>
    /// The projection, or <see langword="null"/> when the accumulated state is already the result.
    /// </value>
    /// <remarks>
    /// Only a collecting sink has one. The run accumulates boxed elements in a list it can build without a
    /// type argument, and the author asked for a list of their element type; the projection is where the
    /// two meet, and it is closed over that type by the authoring surface because the runtime has no type
    /// argument to close it over.
    /// </remarks>
    internal Func<object?, object?>? Finisher { get; }

    /// <summary>Gets what this terminal releases when the run ends, however it ends.</summary>
    /// <value>
    /// The release, which receives the run's failure or <see langword="null"/> for a run that succeeded; or
    /// <see langword="null"/> when this terminal holds nothing that outlives the run.
    /// </value>
    /// <remarks>
    /// Only a channel sink has one, and what it releases is the author's writer. It is called exactly once,
    /// after every segment has stopped, on every terminal path including a run cancelled before its first
    /// element — because a consumer waiting on the other side of that channel has to be told the stream is
    /// over whichever way it ended.
    /// </remarks>
    internal Action<Exception?>? Closing { get; }

    /// <summary>Creates the terminal of a folding sink.</summary>
    /// <param name="folder">The author's fold over boxed state and boxed elements.</param>
    /// <returns>The terminal.</returns>
    internal static LocalTerminal Folding(Func<object?, object?, object?> folder) =>
        new((state, element, _) => folder(state, element), completesOnFirstElement: false, requiresElement: false);

    /// <summary>Creates the terminal of a sink that folds every element through an asynchronous function.</summary>
    /// <param name="folder">The author's fold over boxed state, boxed elements, and the run's token.</param>
    /// <returns>The terminal.</returns>
    /// <remarks>
    /// <para>
    /// The result-bearing asynchronous terminal, and it is the ordinary fold with the wait a fold could not
    /// take before. Everything else about it is the folding sink's: the state belongs to the run, the slot
    /// resolves it when the run ends, a failure anywhere faults every slot of the run, and a shutdown
    /// resolves what was folded so far.
    /// </para>
    /// <para>
    /// One fold at a time and nothing to declare, for the reason the asynchronous scan has none: the state
    /// the next element folds into is the answer of the previous fold, so a bound on concurrency would be a
    /// number with only one legal value. That is what makes this a terminal rather than a second
    /// asynchronous segment — and it is the difference from <c>ForEachAsync</c>, which declares a bound
    /// because its callbacks are independent and declares no result because it accumulates nothing.
    /// </para>
    /// </remarks>
    internal static LocalTerminal FoldingAsync(Func<object?, object?, CancellationToken, Task<object?>> folder) =>
        new(
            (state, element, context) => context.Await(folder(state, element, context.RunToken)),
            completesOnFirstElement: false,
            requiresElement: false);

    /// <summary>Creates the terminal of a counting sink.</summary>
    /// <returns>The terminal.</returns>
    /// <remarks>
    /// A count is a fold and is executed as one, over the zero the authoring surface supplied as the seed.
    /// Writing it as a fold rather than as a case of its own is what keeps the run's state one thing.
    /// </remarks>
    internal static LocalTerminal Counting() => Folding(static (state, _) => (long)state! + 1L);

    /// <summary>Creates the terminal of a sink that hands every element to a callback.</summary>
    /// <param name="callback">The author's callback over boxed elements.</param>
    /// <returns>The terminal.</returns>
    internal static LocalTerminal Calling(Action<object?> callback) =>
        Folding((state, element) =>
        {
            callback(element);

            return state;
        });

    /// <summary>Creates the terminal of a first-element sink.</summary>
    /// <param name="requiresElement">Whether a stream that ended with no element is a failure.</param>
    /// <returns>The terminal.</returns>
    /// <remarks>
    /// The state becomes the element and the run completes, which is the same thing a <c>Take(1)</c> in
    /// front of a folding sink would do and is implemented by the same mechanism: the terminal reports that
    /// the stream is over, and everything upstream is stopped and released.
    /// </remarks>
    internal static LocalTerminal FirstElement(bool requiresElement) =>
        new(static (_, element, _) => element, completesOnFirstElement: true, requiresElement);

    /// <summary>Creates the terminal of a last-element sink.</summary>
    /// <param name="requiresElement">Whether a stream that ended with no element is a failure.</param>
    /// <returns>The terminal.</returns>
    /// <remarks>
    /// The same fold as a first-element sink and the opposite lifetime: every element replaces the state,
    /// and the stream has to end for the answer to exist. That is why a last-element sink completes no run
    /// early and why it holds exactly one element rather than accumulating.
    /// </remarks>
    internal static LocalTerminal LastElement(bool requiresElement) =>
        new(static (_, element, _) => element, completesOnFirstElement: false, requiresElement, element: "last");

    /// <summary>Creates the terminal of a collecting sink.</summary>
    /// <param name="maxElements">The greatest number of elements to collect; at least one.</param>
    /// <param name="finisher">The projection of the collected elements into the author's result type.</param>
    /// <returns>The terminal.</returns>
    /// <remarks>
    /// The bound is checked before the element is added, so a run that delivers exactly the bound succeeds
    /// with all of it and the element after it is what fails. The failure travels to the run loop like any
    /// other terminal's; truncating instead would produce a shorter list nothing downstream could tell from
    /// a complete one.
    /// </remarks>
    internal static LocalTerminal Collecting(int maxElements, Func<object?, object?> finisher) =>
        new(
            (state, element, _) =>
            {
                List<object?> collected = (List<object?>)state!;

                if (collected.Count >= maxElements)
                {
                    throw CollectOverflowException.Exceeded(maxElements);
                }

                collected.Add(element);

                return collected;
            },
            completesOnFirstElement: false,
            requiresElement: false,
            finisher: finisher);

    /// <summary>Creates the terminal a runtime factory built for a registered sink.</summary>
    /// <param name="folder">The provider's fold over boxed state and boxed elements.</param>
    /// <param name="finisher">
    /// The provider's projection of the final state into the value a slot resolves, or
    /// <see langword="null"/> when the state is already that value.
    /// </param>
    /// <returns>The terminal.</returns>
    /// <remarks>
    /// The most general of the factories here and the only one open to code outside this assembly, which
    /// is why it fixes the two answers a provider must not be allowed to give: a registered sink never
    /// completes the run on its first element and never fails a stream that carried none. Both of those
    /// change what the stream itself means rather than what the sink does with it, so they belong to the
    /// engine's own vocabulary and not to a provider's payload.
    /// </remarks>
    internal static LocalTerminal Provided(
        Func<object?, object?, object?> folder,
        Func<object?, object?>? finisher) =>
        new(
            (state, element, _) => folder(state, element),
            completesOnFirstElement: false,
            requiresElement: false,
            finisher: finisher);

    /// <summary>Creates the terminal of a sink that writes into a channel the author owns.</summary>
    /// <param name="channel">The bridge over the author's writer.</param>
    /// <returns>The terminal.</returns>
    /// <remarks>
    /// The write is the backpressure and the completion is the contract: the writer is completed when the
    /// run ends, with the run's failure when it had one, so a consumer reading the other side learns both
    /// that the stream is over and why.
    /// </remarks>
    internal static LocalTerminal Channel(LocalChannelSink channel) =>
        new(
            (state, element, context) =>
            {
                channel.Write(element, context);

                return state;
            },
            completesOnFirstElement: false,
            requiresElement: false,
            closing: channel.Close);

    /// <summary>Creates the terminal of a probe sink.</summary>
    /// <param name="probe">The rendezvous this run's probe hands its elements through.</param>
    /// <returns>The terminal.</returns>
    /// <remarks>
    /// The one terminal that consumes on demand rather than on arrival: an element waits here until a
    /// receiver asks for it, which holds the segment's thread exactly as a slow synchronous callback would,
    /// and is why a run in front of a probe sink advances only as far as its declared bounds allow. The
    /// release is the probe's own, because how the run ended is what a receiver still waiting has to be
    /// told.
    /// </remarks>
    internal static LocalTerminal Probing(LocalSinkProbe probe) =>
        new(
            (state, element, context) =>
            {
                probe.Deliver(element, context);

                return state;
            },
            completesOnFirstElement: false,
            requiresElement: false,
            closing: probe.Close);
}
