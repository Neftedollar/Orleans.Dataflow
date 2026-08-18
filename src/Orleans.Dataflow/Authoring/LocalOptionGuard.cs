using System.Globalization;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The checks an operator applies to the options it is handed, before anything is built from them.
/// </summary>
/// <remarks>
/// <para>
/// The options records deliberately validate nothing themselves, so that <c>with</c> expressions and object
/// initializers compose freely; the check lives at the operator, which is where the author wrote something
/// and where a diagnostic can name the argument they wrote. Failing here also means a rejected call leaves
/// the program exactly as it found it: no descriptor is created, no chain is copied, and nothing is closed.
/// </para>
/// <para>
/// The parameter name is passed in rather than inferred, so that the exception names the operator's own
/// parameter and not this type's. Every operator that takes options spells it <c>options</c>, and the one
/// place that could drift is this argument.
/// </para>
/// </remarks>
internal static class LocalOptionGuard
{
    /// <summary>Checks the options of a buffer.</summary>
    /// <param name="options">The options the author supplied, already known to be non-null.</param>
    /// <param name="parameterName">The name of the operator's parameter the options arrived in.</param>
    /// <returns>The same options.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="BufferOptions.Capacity"/> is below one, or <see cref="BufferOptions.OverflowPolicy"/> is
    /// not a declared member of its enumeration.
    /// </exception>
    /// <remarks>
    /// The policy is checked because an enumeration is not a closed set at run time: a cast from an
    /// arbitrary integer produces a value no member declares, and such a value has no spelling in a
    /// document and no behavior in a run. Rejecting it here is what keeps both statements true.
    /// </remarks>
    internal static BufferOptions Buffer(BufferOptions options, string parameterName)
    {
        if (options.Capacity < 1)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                options.Capacity,
                $"A buffer holds at least one element, so {nameof(BufferOptions.Capacity)} must be 1 or more. There is no spelling for an unbounded buffer: the size elements may accumulate to is the author's decision, and a default would be a memory leak nobody wrote down.");
        }

        if (LocalBufferParameters.Spell(options.OverflowPolicy) is null)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                options.OverflowPolicy,
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"The value {(int)options.OverflowPolicy} is not a declared {nameof(OverflowPolicy)}, so there is no policy to apply when the buffer is full. The declared policies are {nameof(OverflowPolicy.Backpressure)}, {nameof(OverflowPolicy.DropOldest)}, {nameof(OverflowPolicy.DropNewest)}, {nameof(OverflowPolicy.DropBuffer)}, and {nameof(OverflowPolicy.Fail)}."));
        }

        return options;
    }

    /// <summary>Checks a name a slot or a control is to be declared under.</summary>
    /// <param name="name">The name the author supplied.</param>
    /// <param name="parameterName">The name of the operator's parameter the name arrived in.</param>
    /// <returns>The validated identifier.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="name"/> is not a valid identifier segment.</exception>
    /// <remarks>
    /// <see cref="ResultSlotId"/> owns the segment grammar and the diagnostic for breaking it, so the
    /// message is reused verbatim rather than restated; restating it would let the two drift apart. Only
    /// the parameter name is corrected, because the author wrote a name and not a
    /// <see cref="ResultSlotId"/> value.
    /// </remarks>
    internal static ResultSlotId SlotName(string name, string parameterName)
    {
        // The caller's parameter name is passed rather than inferred, here as well as below: inferring it
        // would name this method's own parameter, and the author wrote the operator's.
        ArgumentNullException.ThrowIfNull(name, parameterName);

        try
        {
            return ResultSlotId.Create(name);
        }
        catch (ArgumentException failure)
        {
            throw new ArgumentException(failure.Message, parameterName, failure);
        }
    }

    /// <summary>Checks the count of a stage counted in elements.</summary>
    /// <param name="count">The count the author supplied.</param>
    /// <param name="parameterName">The name of the operator's parameter the count arrived in.</param>
    /// <returns>The same count.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
    /// <remarks>
    /// Zero is admitted, and deliberately: taking no elements, skipping none, and repeating a value no
    /// times are all things an author can mean, and all three arise from arithmetic on a configured number
    /// rather than from a typing mistake. A negative count means nothing at all.
    /// </remarks>
    internal static int Count(int count, string parameterName) =>
        count >= 0
            ? count
            : throw new ArgumentOutOfRangeException(
                parameterName,
                count,
                "A count of elements is zero or more. Zero is a legal count with a defined meaning, and a negative one has none.");

    /// <summary>Checks the segment size of an interleaving junction.</summary>
    /// <param name="segmentSize">The segment size the author supplied.</param>
    /// <param name="parameterName">The name of the combinator's parameter it arrived in.</param>
    /// <returns>The same segment size.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="segmentSize"/> is below one.</exception>
    /// <remarks>
    /// One is the smallest rotation there is — one element from each input in turn — and zero would be a
    /// junction that takes nothing from anybody and never advances. The payload contract requires a positive
    /// integer for the same reason, so this rejects at the call site what the catalog would otherwise reject
    /// at the document.
    /// </remarks>
    internal static int SegmentSize(int segmentSize, string parameterName) =>
        segmentSize >= 1
            ? segmentSize
            : throw new ArgumentOutOfRangeException(
                parameterName,
                segmentSize,
                "An interleave takes at least one element from an input before moving to the next, so the segment size must be 1 or more. A segment of none is a rotation that never advances.");

    /// <summary>Checks the bounds of a range source.</summary>
    /// <param name="start">The first element the author supplied.</param>
    /// <param name="count">The number of elements the author supplied.</param>
    /// <returns>The same count.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="count"/> is negative, or the last element would not fit in an <see cref="int"/>.
    /// </exception>
    /// <remarks>
    /// The overflow check is the one <see cref="Enumerable.Range"/> applies and is reported against the
    /// count, because the start is a number the author chose freely and the count is the one that has to
    /// fit beside it.
    /// </remarks>
    internal static int Range(int start, int count)
    {
        _ = Count(count, nameof(count));

        return LocalRangeParameters.Fits(start, count)
            ? count
            : throw new ArgumentOutOfRangeException(
                nameof(count),
                count,
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"A range of {count} elements from {start} ends at {(long)start + count - 1L}, which is past {int.MaxValue}. A range's last element is start plus count minus one and has to be an integer this runtime can hold."));
    }

    /// <summary>Checks the size of a batch or of a sliding window.</summary>
    /// <param name="size">The size the author supplied.</param>
    /// <param name="parameterName">The name of the operator's parameter it arrived in.</param>
    /// <returns>The same size.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="size"/> is below one.</exception>
    /// <remarks>
    /// One is a legal size and a real one — a batch of one is every element in a list of its own, which is
    /// what a downstream stage typed in lists wants from a stream that is not batched — where zero would be
    /// a group that is full before it holds anything and a window that never moves. This is stricter than
    /// <see cref="Count"/>, which admits zero, and it is stricter on purpose: a take of nothing means
    /// something and a group of nothing does not.
    /// </remarks>
    internal static int Size(int size, string parameterName) =>
        size >= 1
            ? size
            : throw new ArgumentOutOfRangeException(
                parameterName,
                size,
                "A group holds at least one element, so the size must be 1 or more. A group of no elements is one that is full before anything arrives.");

    /// <summary>Checks the step of a sliding window.</summary>
    /// <param name="step">The step the author supplied.</param>
    /// <param name="parameterName">The name of the operator's parameter it arrived in.</param>
    /// <returns>The same step.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="step"/> is below one.</exception>
    /// <remarks>
    /// A step above the size is legal and is the sampling window: the elements between two windows are
    /// passed over and never carried. A step of zero is the one that has no meaning, because a window that
    /// does not move would emit the same elements forever.
    /// </remarks>
    internal static int Step(int step, string parameterName) =>
        step >= 1
            ? step
            : throw new ArgumentOutOfRangeException(
                parameterName,
                step,
                "A sliding window advances by at least one element, so the step must be 1 or more. A window that does not advance would emit the same elements forever.");

    /// <summary>Checks the weight bound of a weighted batch.</summary>
    /// <param name="maxWeight">The bound the author supplied.</param>
    /// <param name="parameterName">The name of the operator's parameter it arrived in.</param>
    /// <returns>The same bound.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxWeight"/> is below one.</exception>
    /// <remarks>
    /// Zero is refused rather than read as "do not bound by weight": a group that may weigh nothing could
    /// never accept an element that weighs something, and the spelling for an unweighted batch is the
    /// overload that takes no cost function.
    /// </remarks>
    internal static int Weight(int maxWeight, string parameterName) =>
        maxWeight >= 1
            ? maxWeight
            : throw new ArgumentOutOfRangeException(
                parameterName,
                maxWeight,
                "A weighted group carries at least one unit of weight, so the bound must be 1 or more. The spelling for a batch that is not bounded by weight is the overload with no cost function.");

    /// <summary>Checks the options of a deduplicating stage.</summary>
    /// <param name="options">The options the author supplied, already known to be non-null.</param>
    /// <param name="parameterName">The name of the operator's parameter the options arrived in.</param>
    /// <returns>The same options.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="DistinctOptions.MaxTrackedKeys"/> is below one.
    /// </exception>
    internal static DistinctOptions Distinct(DistinctOptions options, string parameterName)
    {
        if (options.MaxTrackedKeys < 1)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                options.MaxTrackedKeys,
                $"A deduplicating stage remembers at least one key, so {nameof(DistinctOptions.MaxTrackedKeys)} must be 1 or more. There is no spelling for unbounded key tracking: what a stream of unrepeated elements would accumulate is unbounded memory, and a stage that could remember nothing could not pass its first element.");
        }

        if (LocalDistinctParameters.Spell(options.OverflowPolicy) is null)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                options.OverflowPolicy,
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"The value {(int)options.OverflowPolicy} is not a declared {nameof(KeyOverflowPolicy)}, so there is no answer for the key past the bound. The declared policies are {nameof(KeyOverflowPolicy.Fail)}, which fails the run, and {nameof(KeyOverflowPolicy.EvictOldest)}, which forgets the key remembered longest."));
        }

        return options;
    }

    /// <summary>Checks the options of a keyed stage.</summary>
    /// <param name="options">The options the author supplied, already known to be non-null.</param>
    /// <param name="parameterName">The name of the operator's parameter the options arrived in.</param>
    /// <returns>The same options.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="GroupByOptions.MaxActiveKeys"/> is below one, or
    /// <see cref="GroupByOptions.OverflowPolicy"/> is not a declared member of its enumeration.
    /// </exception>
    internal static GroupByOptions GroupBy(GroupByOptions options, string parameterName)
    {
        if (options.MaxActiveKeys < 1)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                options.MaxActiveKeys,
                $"A keyed stage holds a substream for at least one key, so {nameof(GroupByOptions.MaxActiveKeys)} must be 1 or more. There is no spelling for an unbounded number of active keys: one substream per key a stream ever carried is unbounded memory, and a stage that could hold none could not accept its first element.");
        }

        if (LocalGroupByParameters.Spell(options.OverflowPolicy) is null)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                options.OverflowPolicy,
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"The value {(int)options.OverflowPolicy} is not a declared {nameof(ActiveKeyOverflowPolicy)}, so there is no answer for the key past the bound. The declared policies are {nameof(ActiveKeyOverflowPolicy.Fail)}, which fails the run, and {nameof(ActiveKeyOverflowPolicy.EvictIdle)}, which flushes and forgets the key that has waited longest for an element."));
        }

        return options;
    }

    /// <summary>Checks that every stage of a group flow is one a keyed stage can run per key.</summary>
    /// <param name="stages">The occurrences the author's group flow contributes, in flow order.</param>
    /// <param name="parameterName">The name of the operator's parameter the flow arrived in.</param>
    /// <returns>The same stages, as the descriptors a keyed stage instantiates per key.</returns>
    /// <exception cref="ArgumentException">
    /// At least one stage of the flow is not one that fuses per key, or is a registered occurrence.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The refusal names <em>every</em> offending stage and its position rather than the first one, because
    /// a group flow is written as one expression and an author fixing them one per compile is an author
    /// re-running the same call four times. The wording is the same claim the payload reader makes, so a
    /// hand-written document and an authored one are refused for the same reason in the same words.
    /// </para>
    /// <para>
    /// A registered occurrence is refused with the rest and named by its stage reference. A registered
    /// element stage really is a function of an element, so it could one day be instantiated per key — but
    /// its behavior is resolved through a catalog and a binder rather than carried by a descriptor, and
    /// resolving one per key is machinery this version does not have. Refusing it by name is honest; letting
    /// it through and failing at plan time would not be.
    /// </para>
    /// </remarks>
    internal static IReadOnlyList<LocalStageDescriptor> Group(
        IReadOnlyList<StageOccurrence> stages,
        string parameterName)
    {
        LocalStageDescriptor[] group = new LocalStageDescriptor[stages.Count];
        List<string> refused = [];

        for (int stage = 0; stage < stages.Count; stage++)
        {
            if (stages[stage] is LocalStageDescriptor descriptor &&
                LocalVocabulary.RunsInsideAGroup(descriptor.Kind))
            {
                group[stage] = descriptor;
            }
            else
            {
                refused.Add(string.Create(
                    CultureInfo.InvariantCulture,
                    $"'{stages[stage].Stage}' at position {stage + 1}"));
            }
        }

        return refused.Count == 0
            ? group
            : throw new ArgumentException(
                $"A group flow runs fused per key, so it holds element stages only: {string.Join(", ", refused)}. An asynchronous stage, a buffer, a junction, and a stage that reads the clock each want a segment, a channel, or a run of their own, and one per key is not something a fused stage can hold. A flattening stage and a nested group-by are refused for this operator's own reasons, which are stated in the documentation.",
                parameterName);
    }

    /// <summary>Checks the options of a collecting sink.</summary>
    /// <param name="options">The options the author supplied, already known to be non-null.</param>
    /// <param name="parameterName">The name of the factory's parameter the options arrived in.</param>
    /// <returns>The same options.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="CollectOptions.MaxElements"/> is below one.
    /// </exception>
    internal static CollectOptions Collect(CollectOptions options, string parameterName) =>
        options.MaxElements >= 1
            ? options
            : throw new ArgumentOutOfRangeException(
                parameterName,
                options.MaxElements,
                $"A collecting sink holds at least one element, so {nameof(CollectOptions.MaxElements)} must be 1 or more. There is no spelling for an unbounded collection: what a long stream would accumulate is unbounded memory, and a sink bounded at zero could not accept its first element.");

    /// <summary>Checks the options of an asynchronous stage.</summary>
    /// <param name="options">The options the author supplied, already known to be non-null.</param>
    /// <param name="parameterName">The name of the operator's parameter the options arrived in.</param>
    /// <returns>The same options.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="ParallelismOptions.MaxConcurrency"/> is below one.
    /// </exception>
    internal static ParallelismOptions Parallelism(ParallelismOptions options, string parameterName) =>
        options.MaxConcurrency >= 1
            ? options
            : throw new ArgumentOutOfRangeException(
                parameterName,
                options.MaxConcurrency,
                $"An asynchronous stage runs at least one callback at a time, so {nameof(ParallelismOptions.MaxConcurrency)} must be 1 or more. There is no spelling for unbounded concurrency, and 1 is the sequential asynchronous map rather than a disabled stage.");

    /// <summary>Checks the state a valve is to start in.</summary>
    /// <param name="mode">The state the author supplied.</param>
    /// <param name="parameterName">The name of the operator's parameter the state arrived in.</param>
    /// <returns>The same state.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="mode"/> is not a declared member of its enumeration.
    /// </exception>
    /// <remarks>
    /// Checked because an enumeration is not a closed set at run time: a cast from an arbitrary integer
    /// produces a value no member declares, and such a value has no spelling in a document and no behavior
    /// in a run.
    /// </remarks>
    internal static ValveMode Valve(ValveMode mode, string parameterName) =>
        LocalValveParameters.Spell(mode) is not null
            ? mode
            : throw new ArgumentOutOfRangeException(
                parameterName,
                mode,
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"The value {(int)mode} is not a declared {nameof(ValveMode)}, so there is no state for the valve to start in. The declared states are {nameof(ValveMode.Open)} and {nameof(ValveMode.Closed)}."));

    /// <summary>Checks a duration an operator is configured by.</summary>
    /// <param name="duration">The duration the author supplied.</param>
    /// <param name="parameterName">The name of the operator's parameter the duration arrived in.</param>
    /// <returns>The same duration.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="duration"/> is zero, negative, or <see cref="Timeout.InfiniteTimeSpan"/>.
    /// </exception>
    /// <remarks>
    /// <para>
    /// Every duration this vocabulary carries is positive, and zero is refused rather than admitted as a
    /// no-op: a delay of no time, a window of no duration, and a timeout that has already elapsed are
    /// operators an author meant something else by. Leaving the operator out is the spelling for "no delay",
    /// and it is a spelling that costs nothing at run time.
    /// </para>
    /// <para>
    /// <see cref="Timeout.InfiniteTimeSpan"/> is minus one tick and is refused by the same test, which is
    /// the answer for it too: a timing operator with no deadline is the operator not being there.
    /// </para>
    /// </remarks>
    internal static TimeSpan Duration(TimeSpan duration, string parameterName) =>
        duration > TimeSpan.Zero
            ? duration
            : throw new ArgumentOutOfRangeException(
                parameterName,
                duration,
                "A timing operator is configured by a positive duration. Zero, a negative duration, and an infinite one all describe an operator that should be left out rather than written with nothing in it.");

    /// <summary>Checks the options of a throttle.</summary>
    /// <param name="options">The options the author supplied, already known to be non-null.</param>
    /// <param name="parameterName">The name of the operator's parameter the options arrived in.</param>
    /// <returns>
    /// The same options with <see cref="ThrottleOptions.MaximumBurst"/> stated rather than defaulted, so
    /// that what is written into the document is what the run executes.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="ThrottleOptions.Elements"/> is below one, <see cref="ThrottleOptions.Per"/> is not a
    /// positive duration, <see cref="ThrottleOptions.MaximumBurst"/> is below
    /// <see cref="ThrottleOptions.Elements"/>, or <see cref="ThrottleOptions.Mode"/> is not a declared
    /// member of its enumeration.
    /// </exception>
    /// <remarks>
    /// The burst is defaulted here rather than in the runtime, because the default is an authoring decision
    /// and the document has to state the rate the run will actually hold: a payload that left it out would
    /// make two versions of this package read one graph differently. The mode is checked because an
    /// enumeration is not a closed set at run time — a cast from an arbitrary integer has no spelling in a
    /// document and no behavior in a run.
    /// </remarks>
    internal static ThrottleOptions Throttle(ThrottleOptions options, string parameterName)
    {
        if (options.Elements < 1)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                options.Elements,
                $"A throttle admits at least one cost unit per period, so {nameof(ThrottleOptions.Elements)} must be 1 or more. There is no spelling for an unlimited rate: a stream that is not to be paced is one written without a throttle.");
        }

        if (options.Per <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                options.Per,
                $"A rate is a number of cost units over a period, so {nameof(ThrottleOptions.Per)} must be a positive duration. A period of no length is a rate with no meaning.");
        }

        if (options.MaximumBurst is { } burst && burst < options.Elements)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                burst,
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"A throttle's {nameof(ThrottleOptions.MaximumBurst)} is the most budget it ever holds, so it must be at least {nameof(ThrottleOptions.Elements)}, which is {options.Elements}. A bucket smaller than one period's worth would make the declared rate one the stream never reaches."));
        }

        if (LocalThrottleParameters.Spell(options.Mode) is null)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                options.Mode,
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"The value {(int)options.Mode} is not a declared {nameof(ThrottleMode)}, so there is no answer for an element the rate has no budget for. The declared modes are {nameof(ThrottleMode.Shaping)}, which waits, and {nameof(ThrottleMode.Enforcing)}, which fails the run."));
        }

        return options.MaximumBurst is null ? options with { MaximumBurst = options.Elements } : options;
    }
}
