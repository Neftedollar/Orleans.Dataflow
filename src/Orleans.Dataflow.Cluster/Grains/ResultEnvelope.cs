namespace Orleans.Dataflow.Grains;

/// <summary>
/// One result of one run, as it crosses a grain boundary: the value the slot resolved to, and the terminal
/// state that made it available.
/// </summary>
/// <remarks>
/// <para>
/// The value travels as <see cref="object"/> and not as bytes, deliberately. A result is the author's own
/// type — the state a fold accumulated, a list a sink collected — and the definition plane never names CLR
/// types, so there is nothing this library could serialize it as. Orleans serializes it polymorphically,
/// which means a result type must satisfy Orleans serialization: <c>[GenerateSerializer]</c> with
/// <c>[Id]</c> on every member, or a registered serializer. That requirement is checked at first use, when
/// a result is actually sent, and not when a pipeline is written. Graph documents, by contrast, always
/// travel as canonical bytes and never as Orleans-serialized object graphs.
/// </para>
/// <para>
/// <see cref="HasValue"/> is not <c><see cref="Value"/> is not null</c>: a slot may legitimately resolve
/// to <see langword="null"/>, and a run that has not ended resolves to nothing at all. Two different
/// absences deserve two different answers, so the flag carries one and <see cref="Phase"/> carries the
/// other.
/// </para>
/// <para>
/// The envelope reports a failure as well as a value, which makes it the single authority on how a run
/// ended: a caller that reads the envelope learns the outcome without a second call, and the completion
/// task and this type can never disagree because both are read from the same settled run.
/// </para>
/// </remarks>
[GenerateSerializer]
public sealed class ResultEnvelope
{
    /// <summary>Gets or sets the terminal state of the run that produced this envelope.</summary>
    /// <value>
    /// One of the three terminal phases once the run has ended, or <see cref="RunPhase.Running"/> when it
    /// had not ended when the grain answered.
    /// </value>
    [Id(0)]
    public RunPhase Phase { get; set; }

    /// <summary>Gets or sets a value indicating whether the slot resolved to a value.</summary>
    /// <value>
    /// <see langword="true"/> when <see cref="Value"/> is the resolved value, including when that value is
    /// <see langword="null"/>; <see langword="false"/> when the run had not ended or did not resolve the
    /// slot.
    /// </value>
    [Id(1)]
    public bool HasValue { get; set; }

    /// <summary>Gets or sets the value the slot resolved to.</summary>
    /// <value>The author's own value, or <see langword="null"/> when there is none to report.</value>
    /// <remarks>
    /// <b>The declared type is <see cref="object"/> and the receiving cast happens after deserialization, so
    /// what bounds the types that can arrive here is Orleans' own allow-list rather than anything this
    /// library declares</b>: Orleans 7 and later deserialize only <c>[GenerateSerializer]</c> types and
    /// registered serializers, and a deployment that widens that allow-list widens this member with it.
    /// </remarks>
    [Id(2)]
    public object? Value { get; set; }

    /// <summary>Gets or sets the CLR type name of the exception that ended the run.</summary>
    /// <value>The full type name for <see cref="RunPhase.Faulted"/>; otherwise <see langword="null"/>.</value>
    [Id(3)]
    public string? FailureType { get; set; }

    /// <summary>Gets or sets the message of the exception that ended the run.</summary>
    /// <value>The message for <see cref="RunPhase.Faulted"/>; otherwise <see langword="null"/>.</value>
    [Id(4)]
    public string? FailureMessage { get; set; }
}
