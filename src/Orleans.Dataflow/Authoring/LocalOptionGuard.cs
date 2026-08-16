using System.Globalization;

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
