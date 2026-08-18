using Orleans.Dataflow.Runtime;

namespace Orleans.Dataflow.Testing;

/// <summary>
/// The facade over one run's marking sink.
/// </summary>
/// <remarks>
/// One line of code, and it exists for the reason every facade in this package does: the mark is kept by the
/// runtime, on the segment's own thread, beside the callback it follows, and the type argument a test holds
/// never reaches there. A control is where the two meet.
/// </remarks>
/// <param name="sink">The run's own marking sink.</param>
internal sealed class MarkingSink(LocalMarkingSink sink) : IMarkingSink
{
    /// <inheritdoc/>
    public long Mark => sink.Committed;
}
