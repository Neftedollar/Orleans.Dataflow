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

    /// <summary>Checks the options of a deduplicating stage.</summary>
    /// <param name="options">The options the author supplied, already known to be non-null.</param>
    /// <param name="parameterName">The name of the operator's parameter the options arrived in.</param>
    /// <returns>The same options.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="DistinctOptions.MaxTrackedKeys"/> is below one.
    /// </exception>
    internal static DistinctOptions Distinct(DistinctOptions options, string parameterName) =>
        options.MaxTrackedKeys >= 1
            ? options
            : throw new ArgumentOutOfRangeException(
                parameterName,
                options.MaxTrackedKeys,
                $"A deduplicating stage remembers at least one key, so {nameof(DistinctOptions.MaxTrackedKeys)} must be 1 or more. There is no spelling for unbounded key tracking: what a stream of unrepeated elements would accumulate is unbounded memory, and a stage that could remember nothing could not pass its first element.");

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
}
