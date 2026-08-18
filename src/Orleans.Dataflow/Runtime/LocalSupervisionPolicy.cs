namespace Orleans.Dataflow.Runtime;

/// <summary>
/// One supervision scope's policy as a compiled plan holds it: the form, and the retrying form's attempts,
/// ladder, and answer for exhaustion.
/// </summary>
/// <param name="Form">What the scope does with a failure raised inside it.</param>
/// <param name="MaxAttempts">
/// How many times a retrying scope offers one element, including the first; one for every other form.
/// </param>
/// <param name="Backoff">
/// How long a retrying scope waits before each re-offer, in attempt order, with the last rung repeating;
/// empty for every other form and for a retry that waits for nothing.
/// </param>
/// <param name="OnExhaustion">What an element that used every attempt costs.</param>
/// <remarks>
/// The runtime's own reading of <see cref="Orleans.Dataflow.SupervisionOptions"/>, taken once when the plan
/// is built. It exists rather than the options themselves being carried for the reason every reading in this
/// planner exists: what a document states is checked once, and what a run executes is a value that cannot be
/// wrong any more.
/// </remarks>
internal readonly record struct LocalSupervisionPolicy(
    SupervisionForm Form,
    int MaxAttempts,
    IReadOnlyList<TimeSpan> Backoff,
    RetryExhaustion OnExhaustion);
