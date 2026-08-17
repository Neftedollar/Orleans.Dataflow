namespace Orleans.Dataflow.Runtime;

/// <summary>
/// What one fan-out junction does with the element it pulled: which of its outputs must have room before
/// it pulls at all, and which of them receive what.
/// </summary>
/// <remarks>
/// <para>
/// The pump shape is one and the strategies are small, which is the whole design of ADR 0005's fan-out
/// table. Every junction is one reader and several writers on one thread; what a broadcast, a balance, and
/// an unzip disagree about is two synchronous questions — which writers have to have room, and which of
/// them the element goes to — and nothing else. The waiting, the pause discipline, the per-leg completion,
/// and the failure rule are the pump's and are therefore stated once.
/// </para>
/// <para>
/// The element bound the table states holds by construction rather than by counting: the room is checked
/// first and the pull happens second, so the one element a junction ever holds is the one it is placing.
/// A junction that pulled first and then waited for room would hold that element for the whole of the
/// wait, which is the same number in this case and the wrong shape for every case where a leg leaves in
/// the meantime.
/// </para>
/// <para>
/// <see cref="Halves"/> is the only place a junction carries behavior, and it carries it for the reason
/// every projection is behavior: which member of a row is its left half is a statement about an element
/// type, and an element type never appears in a local document.
/// </para>
/// </remarks>
internal sealed class LocalFanOut
{
    /// <summary>Initializes a new instance of the <see cref="LocalFanOut"/> class.</summary>
    /// <param name="kind">Which junction this is.</param>
    /// <param name="halves">The projections of a row onto its outputs, for an unzip alone.</param>
    private LocalFanOut(LocalFanOutKind kind, IReadOnlyList<Func<object?, object?>>? halves)
    {
        Kind = kind;
        Halves = halves;
    }

    /// <summary>Gets which junction this is.</summary>
    internal LocalFanOutKind Kind { get; }

    /// <summary>Gets the projection each output applies to the row the junction pulled.</summary>
    /// <value>
    /// One projection per output port for an unzip, in port order; <see langword="null"/> for a broadcast
    /// and a balance, which deliver the element they pulled and never look inside it.
    /// </value>
    internal IReadOnlyList<Func<object?, object?>>? Halves { get; }

    /// <summary>Gets a value indicating whether every live output must have room before the pull.</summary>
    /// <value>
    /// <see langword="true"/> for a broadcast and an unzip, which is slowest-consumer backpressure;
    /// <see langword="false"/> for a balance, which needs one willing output and no more.
    /// </value>
    internal bool NeedsEveryOutput => Kind is not LocalFanOutKind.Balance;

    /// <summary>Creates the strategy of a junction that delivers every element to every live output.</summary>
    /// <returns>The strategy.</returns>
    internal static LocalFanOut Broadcast() => new(LocalFanOutKind.Broadcast, halves: null);

    /// <summary>Creates the strategy of a junction that delivers each element to one output with room.</summary>
    /// <returns>The strategy.</returns>
    internal static LocalFanOut Balance() => new(LocalFanOutKind.Balance, halves: null);

    /// <summary>Creates the strategy of a junction that delivers a row's halves to its outputs.</summary>
    /// <param name="halves">One projection per output port, in port order.</param>
    /// <returns>The strategy.</returns>
    /// <remarks>
    /// Broadcast with a projection per leg, which is exactly what the table says an unzip is: both outputs
    /// must have room, both receive their half of the same row, and the two legs therefore advance in
    /// lockstep and can be re-joined downstream without skew.
    /// </remarks>
    internal static LocalFanOut Unzip(IReadOnlyList<Func<object?, object?>> halves) =>
        new(LocalFanOutKind.Unzip, halves);
}
