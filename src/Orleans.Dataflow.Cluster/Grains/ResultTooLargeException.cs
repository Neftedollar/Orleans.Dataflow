using System.Globalization;

namespace Orleans.Dataflow.Grains;

/// <summary>
/// A run's result is larger than the silo's declared bound, so it was not sent.
/// </summary>
/// <remarks>
/// <para>
/// The named error the result-size cap exists to produce. Without it an oversized result is a foot-gun with
/// three bad endings and no good one: a codec or transport failure whose message is about buffers rather
/// than about the graph, a poll that never answers, or a silo that quietly moves hundreds of megabytes
/// because a <c>Collect</c> was written over a cluster. Naming the failure turns "why did this hang" into
/// "this result is 4 MiB and the cap is 1 MiB".
/// </para>
/// <para>
/// <b>The cap is enforced at envelope creation, on the grain side, and only the slot fails.</b> The run
/// itself has already ended — successfully — and nothing about reading a result changes how it ended, so a
/// run whose result is too large stays <see cref="RunPhase.Completed"/>, its other results resolve
/// normally, and reading the oversized one is what refuses. Faulting the run instead would rewrite history
/// on a read, and refusing at the client would mean the bytes had already crossed the wire, which is the
/// one thing the cap exists to prevent.
/// </para>
/// <para>
/// The size is exact rather than estimated: it is the number of bytes Orleans' own serializer produces for
/// the value, measured through a writer that counts and discards. The measurement therefore costs one
/// serialization of a result that is about to be serialized again — a stated cost, paid once per read of
/// one slot and never per element.
/// </para>
/// <para>
/// No inner exception is attached, per the wire discipline every grain-thrown refusal in this package
/// follows: Orleans serializes an exception's whole chain, and a chain is only as serializable as its least
/// prepared link.
/// </para>
/// </remarks>
[GenerateSerializer]
public sealed class ResultTooLargeException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="ResultTooLargeException"/> class.</summary>
    public ResultTooLargeException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ResultTooLargeException"/> class.</summary>
    /// <param name="message">The message describing the refusal.</param>
    public ResultTooLargeException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ResultTooLargeException"/> class.</summary>
    /// <param name="message">The message describing the refusal.</param>
    /// <param name="innerException">The exception that caused this one.</param>
    public ResultTooLargeException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ResultTooLargeException"/> class.</summary>
    /// <param name="slotName">The name of the result slot that was read.</param>
    /// <param name="bytes">The serialized size of the value, in bytes.</param>
    /// <param name="maximumBytes">The bound this silo declared, in bytes.</param>
    public ResultTooLargeException(string? slotName, long bytes, int maximumBytes)
        : base(string.Create(
            CultureInfo.InvariantCulture,
            $"The result '{slotName}' serializes to {bytes} bytes, and this silo caps a result at {maximumBytes}. The run itself completed and its other results resolve normally; what is refused is sending this one. Either narrow what the terminal accumulates — a Collect over a cluster is the shape this cap exists for — or raise the bound with LimitResultSize when the silo is built."))
    {
        SlotName = slotName;
        Bytes = bytes;
        MaximumBytes = maximumBytes;
    }

    /// <summary>Gets the name of the result slot that was read.</summary>
    [Id(0)]
    public string? SlotName { get; init; }

    /// <summary>Gets the serialized size of the value, in bytes.</summary>
    /// <value>The exact number of bytes Orleans' serializer produced for it.</value>
    [Id(1)]
    public long Bytes { get; init; }

    /// <summary>Gets the bound the silo declared, in bytes.</summary>
    [Id(2)]
    public int MaximumBytes { get; init; }
}
