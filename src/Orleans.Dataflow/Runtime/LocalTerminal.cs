namespace Orleans.Dataflow.Runtime;

/// <summary>
/// What the last segment of a plan does with an element that reaches it, and what the run owes the author
/// when the stream ends.
/// </summary>
/// <remarks>
/// <para>
/// Every terminal the local vocabulary has is one fold over the run's state and at most two extra facts:
/// whether the first element is enough, and whether there had to be one. A discarding sink is the absence
/// of a terminal rather than a terminal that does nothing, and so is an asynchronous callback sink, whose
/// whole work is the callback its segment already drives.
/// </para>
/// <para>
/// The state itself is not here. It belongs to the run, because a run is what a state is fresh per, and a
/// plan is shared by none: <see cref="LocalRunPlan.Seed"/> is where a run starts and this is what moves it.
/// </para>
/// </remarks>
internal sealed class LocalTerminal
{
    /// <summary>Initializes a new instance of the <see cref="LocalTerminal"/> class.</summary>
    /// <param name="folder">The fold over boxed state and boxed elements.</param>
    /// <param name="completesOnFirstElement">Whether one element is the whole of what this terminal wants.</param>
    /// <param name="requiresElement">Whether a stream that ended with no element is a failure.</param>
    private LocalTerminal(
        Func<object?, object?, object?> folder,
        bool completesOnFirstElement,
        bool requiresElement)
    {
        Folder = folder;
        CompletesOnFirstElement = completesOnFirstElement;
        RequiresElement = requiresElement;
    }

    /// <summary>Gets the fold this terminal applies to every element that reaches it.</summary>
    /// <value>
    /// The author's own folder for an aggregate, the increment of a count, the callback of a synchronous
    /// per-element sink written as a fold that keeps its state, or the replacement of the state by the
    /// element for a first-element sink.
    /// </value>
    internal Func<object?, object?, object?> Folder { get; }

    /// <summary>Gets a value indicating whether the first element completes the run.</summary>
    /// <value>
    /// <see langword="true"/> for the two first-element sinks, which end the run exactly as a
    /// <c>Take(1)</c> in their place would.
    /// </value>
    internal bool CompletesOnFirstElement { get; }

    /// <summary>Gets a value indicating whether a stream that ended with no element is a failure.</summary>
    /// <value>
    /// <see langword="true"/> only for the strict first-element sink. The honest variant resolves the
    /// default value it was given as its seed, and every other terminal has a state that already means
    /// something when nothing arrived.
    /// </value>
    internal bool RequiresElement { get; }

    /// <summary>Creates the terminal of a folding sink.</summary>
    /// <param name="folder">The author's fold over boxed state and boxed elements.</param>
    /// <returns>The terminal.</returns>
    internal static LocalTerminal Folding(Func<object?, object?, object?> folder) =>
        new(folder, completesOnFirstElement: false, requiresElement: false);

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
        new(static (_, element) => element, completesOnFirstElement: true, requiresElement);
}
