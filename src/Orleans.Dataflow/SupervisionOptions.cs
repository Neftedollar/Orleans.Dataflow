using System.Globalization;

namespace Orleans.Dataflow;

/// <summary>
/// What a supervision scope does with a failure raised inside it, and — for the retrying form — how many
/// times it tries, how long it waits between attempts, and what it does when the attempts run out.
/// </summary>
/// <remarks>
/// <para>
/// One record per concern, never one options bag. Everything here is written into the
/// document, because all of it changes what the graph observably does: a scope that resumes and one that
/// restarts produce different streams from the same elements, and two ladders of different lengths are two
/// different graphs.
/// </para>
/// <para>
/// <see cref="Form"/> is <see langword="required"/> and has no default, because there is no supervision an
/// author could have meant without saying it. The three members after it are read <b>only</b> for
/// <see cref="SupervisionForm.Retry"/> and are refused on every other form: an attempt count on a scope
/// that does not retry is a statement the graph cannot honor, and admitting it would put a number in the
/// document that nothing reads.
/// </para>
/// <para>
/// The values are checked where the scope is placed rather than here, so <c>with</c> expressions and object
/// initializers compose freely and the diagnostic names the operator's own parameter.
/// </para>
/// </remarks>
public sealed record class SupervisionOptions
{
    /// <summary>Gets what the scope does with a failure raised inside it.</summary>
    /// <value>One of the four declared forms; there is no default.</value>
    public required SupervisionForm Form { get; init; }

    /// <summary>Gets how many times a retrying scope offers one element before giving up.</summary>
    /// <value>
    /// A positive count including the first attempt, defaulting to one; read only when <see cref="Form"/>
    /// is <see cref="SupervisionForm.Retry"/>.
    /// </value>
    /// <remarks>
    /// The count is attempts and not retries, so three means one offer and two re-offers. One is legal and
    /// means "no re-offer": the exhaustion answer is applied to the first failure, which is the long way of
    /// writing one of the other forms and is admitted because a graph generated from configuration may
    /// legitimately turn the retries down to none.
    /// </remarks>
    public int MaxAttempts { get; init; } = 1;

    /// <summary>Gets how long a retrying scope waits before each re-offer.</summary>
    /// <value>
    /// The ladder in attempt order, defaulting to empty; read only when <see cref="Form"/> is
    /// <see cref="SupervisionForm.Retry"/>.
    /// </value>
    /// <remarks>
    /// <para>
    /// A ladder rather than a base and a factor, because a ladder is what a document can state exactly: an
    /// author reading the payload sees the waits the run will take, and no reader has to reproduce an
    /// arithmetic nobody wrote down. <b>The last rung repeats</b>, so a ladder shorter than the attempt
    /// count is legal and means "and then this long every time"; an empty ladder means every re-offer
    /// happens at once.
    /// </para>
    /// <para>
    /// A rung of zero is admitted, unlike every other duration this vocabulary carries: "try again now" is
    /// the ordinary shape of a first rung, where a delay of no time and a window of no duration describe
    /// operators that should have been left out.
    /// </para>
    /// <para>
    /// The waits are taken on the run's own clock and are not jittered in this version. Jitter answers a
    /// question a per-element retry inside one run does not ask — it spreads a fleet's restarts, and there
    /// is no fleet here — and adding a random source would turn the one guarantee worth having, that the
    /// waits are exactly what the document says, into a statistical claim instead of an exact one.
    /// </para>
    /// </remarks>
    public IReadOnlyList<TimeSpan> Backoff { get; init; } = [];

    /// <summary>Gets what a retrying scope does with an element that has used every attempt.</summary>
    /// <value>
    /// <see cref="RetryExhaustion.Fail"/> by default; read only when <see cref="Form"/> is
    /// <see cref="SupervisionForm.Retry"/>.
    /// </value>
    /// <remarks>
    /// Defaulted to failing because that is the value that keeps the engine's own rule visible: a scope
    /// weakens "a failure fails the run" for as long as its attempts last, and an author who wants it
    /// weakened past that says so.
    /// </remarks>
    public RetryExhaustion OnExhaustion { get; init; }

    /// <summary>Returns a one-line diagnostic summary of these options.</summary>
    /// <returns>
    /// Text of the form <c>supervised (Resume)</c>, or <c>supervised (Retry, 3 attempts, 2 rungs, Fail)</c>
    /// for the retrying form.
    /// </returns>
    /// <remarks>
    /// The counts are formatted with the invariant culture and the method never throws, including for
    /// values that placing a scope would reject.
    /// </remarks>
    public override string ToString() =>
        Form is SupervisionForm.Retry
            ? string.Create(
                CultureInfo.InvariantCulture,
                $"supervised ({Form}, {MaxAttempts} attempts, {Backoff.Count} rungs, {OnExhaustion})")
            : string.Create(CultureInfo.InvariantCulture, $"supervised ({Form})");
}
