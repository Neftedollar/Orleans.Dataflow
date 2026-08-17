namespace Orleans.Dataflow.Runtime;

/// <summary>
/// What one fan-out junction does with the element it pulled: which of its outputs must have room before
/// it pulls at all, and which of them receive what.
/// </summary>
/// <remarks>
/// <para>
/// The pump shape is one for three of the four, which is most of the design of ADR 0005's fan-out table.
/// Every junction is one reader and several writers on one thread; what a broadcast, a balance, and an
/// unzip disagree about is two synchronous questions — which writers have to have room, and which of them
/// the element goes to — and nothing else. The waiting, the pause discipline, the per-leg completion, and
/// the failure rule are the pump's and are therefore stated once.
/// </para>
/// <para>
/// The element bound the table states holds by construction rather than by counting: the room is checked
/// first and the pull happens second, so the one element such a junction ever holds is the one it is
/// placing. A junction that pulled first and then waited for room would hold that element for the whole of
/// the wait, which is the same number in this case and the wrong shape for every case where a leg leaves
/// in the meantime.
/// </para>
/// <para>
/// <b>A partition is the exception and cannot be anything else.</b> Which leg its element belongs on is
/// what the author's function answers, and the function needs the element, so the room its contract talks
/// about is knowable only after the read. It therefore reads first, routes once, and then waits for that
/// one leg while holding the element — head-of-line blocking one element deep, which is what the table
/// says it is. The bound is still one element; what differs is when the junction is holding it, which is
/// why it has a loop of its own rather than a third answer inside the shared one.
/// </para>
/// <para>
/// <see cref="Halves"/> and <see cref="Router"/> are the two places a junction carries behavior, and they
/// carry it for the reason every projection is behavior: which member of a row is its left half, and which
/// leg an element belongs on, are statements about an element type, and an element type never appears in a
/// local document.
/// </para>
/// </remarks>
internal sealed class LocalFanOut
{
    /// <summary>Initializes a new instance of the <see cref="LocalFanOut"/> class.</summary>
    /// <param name="kind">Which junction this is.</param>
    /// <param name="halves">The projections of a row onto its outputs, for an unzip alone.</param>
    /// <param name="router">The function naming an element's output, for a partition alone.</param>
    private LocalFanOut(
        LocalFanOutKind kind,
        IReadOnlyList<Func<object?, object?>>? halves,
        Func<object?, int>? router)
    {
        Kind = kind;
        Halves = halves;
        Router = router;
    }

    /// <summary>Gets which junction this is.</summary>
    internal LocalFanOutKind Kind { get; }

    /// <summary>Gets the projection each output applies to the row the junction pulled.</summary>
    /// <value>
    /// One projection per output port for an unzip, in port order; <see langword="null"/> for a broadcast
    /// and a balance, which deliver the element they pulled and never look inside it.
    /// </value>
    internal IReadOnlyList<Func<object?, object?>>? Halves { get; }

    /// <summary>Gets the function naming the output one element belongs on.</summary>
    /// <value>
    /// The author's routing function for a partition, answering the zero-based position of a leg in port
    /// order; <see langword="null"/> for every junction that routes by its own rule instead.
    /// </value>
    /// <remarks>
    /// Also the discriminator the run loop switches on, because it is the one thing that decides which of
    /// the two fan-out loops a segment runs: a junction with a router reads before it waits, and a junction
    /// without one waits before it reads.
    /// </remarks>
    internal Func<object?, int>? Router { get; }

    /// <summary>Gets a value indicating whether every live output must have room before the pull.</summary>
    /// <value>
    /// <see langword="true"/> for a broadcast and an unzip, which is slowest-consumer backpressure;
    /// <see langword="false"/> for a balance, which needs one willing output and no more.
    /// </value>
    /// <remarks>
    /// Asked only by the loop that waits before it reads, so a partition never reaches it: what a partition
    /// needs is room on the one leg its function named, which is neither of the two answers here.
    /// </remarks>
    internal bool NeedsEveryOutput => Kind is not LocalFanOutKind.Balance;

    /// <summary>Creates the strategy of a junction that delivers every element to every live output.</summary>
    /// <returns>The strategy.</returns>
    internal static LocalFanOut Broadcast() => new(LocalFanOutKind.Broadcast, halves: null, router: null);

    /// <summary>Creates the strategy of a junction that delivers each element to one output with room.</summary>
    /// <returns>The strategy.</returns>
    internal static LocalFanOut Balance() => new(LocalFanOutKind.Balance, halves: null, router: null);

    /// <summary>Creates the strategy of a junction that delivers each element to the output it names.</summary>
    /// <param name="router">The author's routing function, over boxed elements.</param>
    /// <returns>The strategy.</returns>
    /// <remarks>
    /// The one junction whose behavior decides where an element goes rather than how the legs are paced.
    /// Nothing here checks what the function may answer, because the range is the count of wired legs and a
    /// strategy does not know the edges; the pump checks it against the legs it was given, once per
    /// element, and fails the run on an answer that names no leg.
    /// </remarks>
    internal static LocalFanOut Partition(Func<object?, int> router) =>
        new(LocalFanOutKind.Partition, halves: null, router);

    /// <summary>Creates the strategy of a junction that delivers a row's halves to its outputs.</summary>
    /// <param name="halves">One projection per output port, in port order.</param>
    /// <returns>The strategy.</returns>
    /// <remarks>
    /// Broadcast with a projection per leg, which is exactly what the table says an unzip is: both outputs
    /// must have room, both receive their half of the same row, and the two legs therefore advance in
    /// lockstep and can be re-joined downstream without skew.
    /// </remarks>
    internal static LocalFanOut Unzip(IReadOnlyList<Func<object?, object?>> halves) =>
        new(LocalFanOutKind.Unzip, halves, router: null);
}
