using System.Globalization;

namespace Orleans.Dataflow;

/// <summary>
/// The failure a buffer declared with <see cref="OverflowPolicy.Fail"/> raises when an element is offered
/// to it and it is full.
/// </summary>
/// <remarks>
/// <para>
/// A type of its own rather than a general-purpose exception with a recognizable message, because a
/// caller that wants to tell overflow apart from every other way a run can fail has to be able to write
/// the <c>catch</c>. The run faults with this very instance, so it is what
/// <see cref="RunHandle.Completion"/> and every result slot rethrow.
/// </para>
/// <para>
/// Overflow is the only condition this type reports. The other four overflow policies never raise it:
/// they wait or they drop, and dropping is counted rather than thrown.
/// </para>
/// </remarks>
public sealed class BufferOverflowException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="BufferOverflowException"/> class.</summary>
    public BufferOverflowException()
        : base("A buffer declared with the fail overflow policy was full when an element was offered to it.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="BufferOverflowException"/> class.</summary>
    /// <param name="message">The message that describes the overflow.</param>
    public BufferOverflowException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="BufferOverflowException"/> class.</summary>
    /// <param name="message">The message that describes the overflow.</param>
    /// <param name="innerException">The exception that caused this one.</param>
    public BufferOverflowException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Builds the exception a full buffer under <see cref="OverflowPolicy.Fail"/> raises.</summary>
    /// <param name="capacity">The declared capacity the buffer had already filled.</param>
    /// <returns>The exception to fault the run with.</returns>
    /// <remarks>
    /// The capacity is in the message because it is the number the author chose and the number the report
    /// is about; it is formatted with the invariant culture so that the text does not change with the
    /// ambient culture.
    /// </remarks>
    internal static BufferOverflowException Full(int capacity) =>
        new(string.Create(
            CultureInfo.InvariantCulture,
            $"A buffer of capacity {capacity} was full when an element was offered to it, and its overflow policy is '{nameof(OverflowPolicy.Fail)}'. Raise the capacity, slow the source, or choose a policy that drops."));
}
