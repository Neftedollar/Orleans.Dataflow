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

        if (refused.Count > 0)
        {
            throw new ArgumentException(
                $"A group flow runs fused per key, so it holds element stages only: {string.Join(", ", refused)}. An asynchronous stage, a buffer, a junction, and a stage that reads the clock each want a segment, a channel, or a run of their own, and one per key is not something a fused stage can hold. A flattening stage, a nested group-by, a supervision scope, and a fault point are refused for this operator's own reasons, which are stated in the documentation.",
                parameterName);
        }

        Anonymous(stages, "A group flow", "the keyed occurrence that runs the flow", parameterName);

        return group;
    }

    /// <summary>Checks the durable options a run was started under.</summary>
    /// <param name="options">The options the author supplied, already known to be non-null.</param>
    /// <param name="parameterName">The name of the host method's parameter the options arrived in.</param>
    /// <exception cref="ArgumentException">
    /// The store is <see langword="null"/>, the run identity is the default value, the interval is not
    /// positive, or the element bound is below one.
    /// </exception>
    /// <remarks>
    /// The one option here that is <em>not</em> checked is "at least one of the two timings is set", because
    /// declaring neither is a legal and documented state: such a run never touches the store. What is
    /// refused is a timing that is present and meaningless — an interval of no time would make a capture due
    /// forever, and a bound of no elements would make one due before an element existed.
    /// </remarks>
    internal static void Durable(DurableRunOptions options, string parameterName)
    {
        if (options.Store is null)
        {
            throw new ArgumentException(
                $"A durable run needs a checkpoint store to write to, and {nameof(DurableRunOptions.Store)} is null.",
                parameterName);
        }

        if (options.RunId.IsDefault)
        {
            throw new ArgumentException(
                $"A durable run is named by whoever will resume it, and {nameof(DurableRunOptions.RunId)} is the default value. Give the run an identity a resume can present back.",
                parameterName);
        }

        if (options.Interval is { } interval && interval <= TimeSpan.Zero)
        {
            throw new ArgumentException(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"A checkpoint interval of {interval} describes a capture that is due forever. Declare a positive interval, or leave {nameof(DurableRunOptions.Interval)} unset and checkpoint on elements alone."),
                parameterName);
        }

        if (options.EveryElements is { } elements && elements < 1)
        {
            throw new ArgumentException(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"A checkpoint bound of {elements} elements describes a capture that is due before an element exists. Declare a bound of at least one, or leave {nameof(DurableRunOptions.EveryElements)} unset and checkpoint on time alone."),
                parameterName);
        }
    }

    /// <summary>Checks that every stage of a durable scope is one whose state a checkpoint can carry.</summary>
    /// <param name="stages">The occurrences the author's scope flow contributes, in flow order.</param>
    /// <param name="parameterName">The name of the operator's parameter the flow arrived in.</param>
    /// <returns>The same stages, as the descriptors the scope owns.</returns>
    /// <exception cref="ArgumentException">
    /// At least one stage of the flow holds state no checkpoint could carry, is a registered occurrence, or
    /// declares a runtime control.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The supervision scope's check read over the shortest of the three admitted lists, and the refusal
    /// says what this scope needs rather than what the other one does: a stage inside a durable scope has to
    /// be able to hand its state over as a canonical value, and a stage that cannot is refused <b>by
    /// name</b> so that an author reads which stage moved out of the scope rather than discovering later
    /// that a resume reset it.
    /// </para>
    /// <para>
    /// What this check cannot see is a scan with no state codec: a codec is a delegate, so the document
    /// plane cannot state whether one was bound. That refusal happens when the plan is built, which is the
    /// same line every other disagreement between a scope's two planes falls on.
    /// </para>
    /// </remarks>
    internal static IReadOnlyList<LocalStageDescriptor> DurableScope(
        IReadOnlyList<StageOccurrence> stages,
        string parameterName)
    {
        LocalStageDescriptor[] scope = new LocalStageDescriptor[stages.Count];
        List<string> refused = [];
        List<string> named = [];

        for (int stage = 0; stage < stages.Count; stage++)
        {
            if (stages[stage] is not LocalStageDescriptor descriptor ||
                !LocalVocabulary.RunsInsideADurableScope(descriptor.Kind))
            {
                refused.Add(string.Create(
                    CultureInfo.InvariantCulture,
                    $"'{stages[stage].Stage}' at position {stage + 1}"));
            }
            else if (descriptor.ControlSlot is { } control)
            {
                named.Add(string.Create(
                    CultureInfo.InvariantCulture,
                    $"'{stages[stage].Stage}' at position {stage + 1} declaring the control '{control}'"));
            }
            else
            {
                scope[stage] = descriptor;
            }
        }

        if (refused.Count > 0)
        {
            throw new ArgumentException(
                $"A durable scope writes its stages' state into a checkpoint, so it holds stages whose state is a canonical value: {string.Join(", ", refused)}. A mapping and a filter hold nothing, a take and a skip hold a count, and a scan holds whatever its state codec can write down; a distinct, a batch, a sliding window, and the two prefix operators hold values of element types no document names, so a checkpoint could not carry them and a resume would silently reset them. Move such a stage outside the scope, where the reset is the documented contract.",
                parameterName);
        }

        if (named.Count > 0)
        {
            throw new ArgumentException(
                $"A durable scope's stages are not nodes of the document, so nothing could resolve a runtime control declared on one: {string.Join(", ", named)}. Place the control-bearing spelling before or after the scope, or use the spelling that declares no control inside it.",
                parameterName);
        }

        Anonymous(stages, "A durable scope", "the occurrence that carries the scope", parameterName);

        return scope;
    }

    /// <summary>Checks that every stage of a supervision scope is one the scope can own the execution of.</summary>
    /// <param name="stages">The occurrences the author's scope flow contributes, in flow order.</param>
    /// <param name="parameterName">The name of the operator's parameter the flow arrived in.</param>
    /// <returns>The same stages, as the descriptors the scope instantiates.</returns>
    /// <exception cref="ArgumentException">
    /// At least one stage of the flow is not one a scope can execute element by element, is a registered
    /// occurrence, or declares a runtime control.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The keyed stage's own check read over a different admitted list, with the same rule about naming
    /// <em>every</em> offending stage and the same wording as the payload reader's, so a hand-written
    /// document and an authored one are refused for the same reason in the same words.
    /// </para>
    /// <para>
    /// The one refusal that is this check's alone is a stage declaring a runtime <b>control</b>. A control is
    /// resolved by the node that produces it, and the stages of a scope's chain are not nodes; a fault point
    /// belongs inside a scope and is admitted, but the spelling that names a control belongs at a node of its
    /// own. Refusing it here is what keeps a declared slot from being one nothing could ever resolve.
    /// </para>
    /// </remarks>
    internal static IReadOnlyList<LocalStageDescriptor> Scope(
        IReadOnlyList<StageOccurrence> stages,
        string parameterName)
    {
        LocalStageDescriptor[] scope = new LocalStageDescriptor[stages.Count];
        List<string> refused = [];
        List<string> named = [];

        for (int stage = 0; stage < stages.Count; stage++)
        {
            if (stages[stage] is not LocalStageDescriptor descriptor ||
                !LocalVocabulary.RunsInsideAScope(descriptor.Kind))
            {
                refused.Add(string.Create(
                    CultureInfo.InvariantCulture,
                    $"'{stages[stage].Stage}' at position {stage + 1}"));
            }
            else if (descriptor.ControlSlot is { } control)
            {
                named.Add(string.Create(
                    CultureInfo.InvariantCulture,
                    $"'{stages[stage].Stage}' at position {stage + 1} declaring the control '{control}'"));
            }
            else
            {
                scope[stage] = descriptor;
            }
        }

        if (refused.Count > 0)
        {
            throw new ArgumentException(
                $"A supervision scope owns the execution of its chain element by element, so it holds element stages only: {string.Join(", ", refused)}. An asynchronous stage, a buffer, a junction, and a stage that reads the clock each want a segment, a channel, or a run of their own. A flattening stage is refused because its sequence is read after the scope has returned, so a failure inside it would fall outside the scope it appears to be in; a nested scope and a group-by are refused as this version's honesty, and both are stated in the documentation.",
                parameterName);
        }

        if (named.Count > 0)
        {
            throw new ArgumentException(
                $"A supervision scope's stages are not nodes of the document, so nothing could resolve a runtime control declared on one: {string.Join(", ", named)}. Place the control-bearing spelling before or after the scope, or use the spelling that declares no control inside it.",
                parameterName);
        }

        Anonymous(stages, "A supervision scope", "the occurrence that carries the scope", parameterName);

        return scope;
    }

    /// <summary>Checks that no stage of an inner chain carries a name the author wrote.</summary>
    /// <param name="stages">The occurrences the author's flow contributes, in flow order.</param>
    /// <param name="owner">What the owner is called, read as the subject of a sentence.</param>
    /// <param name="remedy">What to name instead, read after "Name".</param>
    /// <param name="parameterName">The name of the operator's parameter the flow arrived in.</param>
    /// <exception cref="ArgumentException">At least one stage of the chain is named.</exception>
    /// <remarks>
    /// <para>
    /// The runtime-control refusal read over identity instead of over a slot, and it is the same sentence
    /// because it is the same fact: the stages of an inner chain are fused into their owner's payload and
    /// are not nodes, so a name written on one names nothing a checkpoint, a diagnostic, or a document reader
    /// could resolve. Silently dropping it would be worse than refusing it — an author would have written a
    /// durable identity, watched the graph accept it, and got a document that still declares
    /// <c>ephemeral-identity</c> with no statement of why.
    /// </para>
    /// <para>
    /// Every offending stage is named rather than the first, for the reason the other two checks name every
    /// one: an inner chain is written as one expression, and fixing them one per compile is running the same
    /// call several times to learn what one message could have said.
    /// </para>
    /// </remarks>
    private static void Anonymous(
        IReadOnlyList<StageOccurrence> stages,
        string owner,
        string remedy,
        string parameterName)
    {
        List<string> named = [];

        for (int stage = 0; stage < stages.Count; stage++)
        {
            if (stages[stage].Name is { } name)
            {
                named.Add(string.Create(
                    CultureInfo.InvariantCulture,
                    $"'{stages[stage].Stage}' at position {stage + 1} named '{name}'"));
            }
        }

        if (named.Count > 0)
        {
            throw new ArgumentException(
                $"{owner}'s stages are not nodes of the document, so a name written on one names nothing: {string.Join(", ", named)}. Name {remedy} instead, which is the node this chain stands at.",
                parameterName);
        }
    }

    /// <summary>Checks the options of a supervision scope.</summary>
    /// <param name="options">The options the author supplied, already known to be non-null.</param>
    /// <param name="parameterName">The name of the operator's parameter the options arrived in.</param>
    /// <param name="recovering">Whether the operator's spelling is the one that carries a fallback.</param>
    /// <returns>The same options.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="SupervisionOptions.Form"/> is not a declared member of its enumeration,
    /// <see cref="SupervisionOptions.MaxAttempts"/> is below one,
    /// <see cref="SupervisionOptions.OnExhaustion"/> is not a declared member of its enumeration, or a rung
    /// of <see cref="SupervisionOptions.Backoff"/> is negative.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// A retry-only member is set on a form that does not retry, the backoff ladder is <see langword="null"/>,
    /// or the form and the spelling disagree about the fallback.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The retry-only members are refused on the other three forms rather than ignored, because a number
    /// nothing reads is a number a reader of the document would have to guess about. The check is on the
    /// values and not on whether the author wrote them, so the defaults compose: a resuming scope declared
    /// with nothing but its form passes, and one declared with three attempts does not.
    /// </para>
    /// <para>
    /// The fallback is checked against the form in both directions, because the two spellings are what
    /// separate a scope that emits an element from one that drops it, and a scope whose declared form did not
    /// match the arguments it was given would be a graph doing something the call site does not read as.
    /// </para>
    /// </remarks>
    internal static SupervisionOptions Supervision(
        SupervisionOptions options,
        string parameterName,
        bool recovering)
    {
        if (LocalSupervisionParameters.Spell(options.Form) is null)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                options.Form,
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"The value {(int)options.Form} is not a declared {nameof(SupervisionForm)}, so there is no answer for a failure inside the scope. The declared forms are {nameof(SupervisionForm.Resume)}, {nameof(SupervisionForm.RestartStage)}, {nameof(SupervisionForm.Retry)}, and {nameof(SupervisionForm.Recover)}."));
        }

        if (recovering != (options.Form is SupervisionForm.Recover))
        {
            throw new ArgumentException(
                recovering
                    ? $"This spelling carries the fallback element a recovering scope emits, so its {nameof(SupervisionOptions.Form)} must be {nameof(SupervisionForm.Recover)} and is {options.Form}. The other three forms drop the failing element and have nothing to emit in its place."
                    : $"A scope whose {nameof(SupervisionOptions.Form)} is {nameof(SupervisionForm.Recover)} emits a declared fallback element, and this spelling carries none. Use the overload that takes the fallback beside the scope.",
                parameterName);
        }

        ArgumentNullException.ThrowIfNull(options.Backoff, parameterName);

        if (options.Form is not SupervisionForm.Retry)
        {
            return options.MaxAttempts is 1 &&
                options.Backoff.Count is 0 &&
                options.OnExhaustion is RetryExhaustion.Fail
                ? options
                : throw new ArgumentException(
                    $"A scope whose {nameof(SupervisionOptions.Form)} is {options.Form} never re-offers an element, so {nameof(SupervisionOptions.MaxAttempts)}, {nameof(SupervisionOptions.Backoff)}, and {nameof(SupervisionOptions.OnExhaustion)} say nothing about what it does and are refused rather than written into a document nothing would read them from.",
                    parameterName);
        }

        if (options.MaxAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                options.MaxAttempts,
                $"A retrying scope offers an element at least once, so {nameof(SupervisionOptions.MaxAttempts)} must be 1 or more. The count is attempts and not re-offers, so 1 means the exhaustion answer is applied to the first failure.");
        }

        if (LocalSupervisionParameters.Spell(options.OnExhaustion) is null)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                options.OnExhaustion,
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"The value {(int)options.OnExhaustion} is not a declared {nameof(RetryExhaustion)}, so there is no answer for an element that used every attempt. The declared answers are {nameof(RetryExhaustion.Fail)}, {nameof(RetryExhaustion.Resume)}, and {nameof(RetryExhaustion.RestartStage)}."));
        }

        for (int rung = 0; rung < options.Backoff.Count; rung++)
        {
            if (options.Backoff[rung] < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(
                    parameterName,
                    options.Backoff[rung],
                    string.Create(
                        CultureInfo.InvariantCulture,
                        $"Rung {rung + 1} of {nameof(SupervisionOptions.Backoff)} is negative, and a wait before a re-offer is zero or more. A rung of zero means the re-offer happens at once, which is the ordinary shape of a first rung."));
            }
        }

        return options;
    }

    /// <summary>Checks the arming of a fault point.</summary>
    /// <param name="firstFailure">The one-based position of the first failing arrival.</param>
    /// <param name="parameterName">The name of the factory's parameter the position arrived in.</param>
    /// <returns>The same position.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="firstFailure"/> is below one.</exception>
    internal static int FaultPosition(int firstFailure, string parameterName) =>
        firstFailure >= 1
            ? firstFailure
            : throw new ArgumentOutOfRangeException(
                parameterName,
                firstFailure,
                "A fault point counts the arrivals it has seen from one, so the position of the first failing arrival is 1 or more.");

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
