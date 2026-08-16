using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Definition;

/// <summary>
/// A declared fact about a graph document that validation and hosts must honor.
/// </summary>
/// <remarks>
/// <para>
/// Capability tokens are the extension point by which a document states something a host has to know
/// before it acts on the document, without the core format growing a boolean flag per feature. A host
/// that does not understand a declared token must refuse the document rather than ignore the token.
/// </para>
/// <para>
/// The initial vocabulary is a single token, <see cref="Nondeployable"/>. Further tokens arrive with the
/// features that need them, which is why the token is an open identifier rather than an enumeration.
/// </para>
/// <para>
/// The value is a single identifier segment: <c>[a-z0-9]+(-[a-z0-9]+)*</c>, 1 to 64 characters of
/// lowercase ASCII letters, ASCII digits, and single interior hyphens, compared ordinally. Tokens share
/// the identifier grammar so that they sort and serialize by exactly the same rules as every other
/// identifier in a document, and <see cref="CompareTo"/> is that sort order made callable: it is what a
/// document's canonical capability order is built from rather than a comparator restated beside it.
/// </para>
/// <para>
/// The default value carries no token: <see cref="IsDefault"/> reports it, <see cref="Value"/> throws
/// for it, and <see cref="ToString"/> renders a diagnostic literal for it rather than throwing.
/// </para>
/// </remarks>
public readonly record struct CapabilityToken : IComparable<CapabilityToken>, IComparable
{
    /// <summary>The text of the <see cref="Nondeployable"/> token.</summary>
    private const string NondeployableText = "nondeployable";

    /// <summary>The text of the <see cref="EphemeralIdentity"/> token.</summary>
    private const string EphemeralIdentityText = "ephemeral-identity";

    private readonly string? _value;

    private CapabilityToken(string value) => _value = value;

    /// <summary>
    /// Gets the token that marks a graph carrying locally registered behavior.
    /// </summary>
    /// <value>The token <c>nondeployable</c>.</value>
    /// <remarks>
    /// A graph that declares this token depends on behavior registered in one process rather than on
    /// catalog-resolvable stages alone. Such a graph must never be persisted, resumed, or placed on a
    /// remote silo, because nothing outside the process that registered the behavior can materialize it.
    /// The token exists so that this restriction travels with the document instead of living in the head
    /// of whoever wrote it.
    /// </remarks>
    public static CapabilityToken Nondeployable { get; } = new(NondeployableText);

    /// <summary>Gets the token marking a graph whose node identities are machine-generated.</summary>
    /// <value>The token <c>ephemeral-identity</c>.</value>
    /// <remarks>
    /// Auto-allocated sequential node ids are deterministic for one authoring pass but not edit-stable:
    /// inserting a stage renumbers everything after it. A graph that declares this token therefore has no
    /// identities a durable pipeline could anchor checkpoints, upgrades, or resumes to, and deployability
    /// validation rejects it for durable use (ADR 0004 section 6). The token is orthogonal to
    /// <see cref="Nondeployable"/>: a fully named graph of lambdas is nondeployable but not ephemeral, and
    /// a pipeline of registered stages with unnamed occurrences is the reverse.
    /// </remarks>
    public static CapabilityToken EphemeralIdentity { get; } = new(EphemeralIdentityText);

    /// <summary>
    /// Gets the validated token text.
    /// </summary>
    /// <value>The canonical identifier segment.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which carries no token.
    /// </exception>
    public string Value =>
        _value ?? throw new InvalidOperationException(IdentifierGrammar.DescribeDefaultAccess(nameof(CapabilityToken)));

    /// <summary>
    /// Gets a value indicating whether this instance is the uninitialized default value.
    /// </summary>
    /// <value><see langword="true"/> for the default value; otherwise <see langword="false"/>.</value>
    public bool IsDefault => _value is null;

    /// <summary>
    /// Creates a <see cref="CapabilityToken"/> from its text form.
    /// </summary>
    /// <param name="value">The identifier segment.</param>
    /// <returns>The validated token.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="value"/> does not satisfy the identifier segment grammar. The message names the
    /// offending value and the rule it breaks.
    /// </exception>
    /// <remarks>
    /// Any grammatical token is accepted, including one this version does not know: recognizing tokens is
    /// the job of the validator and the host, not of the lexical type.
    /// </remarks>
    public static CapabilityToken Create(string value)
    {
        IdentifierGrammar.EnsureSegment(value, nameof(CapabilityToken), nameof(value));
        return new CapabilityToken(value);
    }

    /// <summary>
    /// Attempts to create a <see cref="CapabilityToken"/> from its text form.
    /// </summary>
    /// <param name="value">The candidate identifier segment, which may be <see langword="null"/>.</param>
    /// <param name="token">
    /// When this method returns <see langword="true"/>, the validated token; otherwise the default value.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="value"/> satisfies the identifier segment grammar;
    /// otherwise <see langword="false"/>.
    /// </returns>
    /// <remarks>This method never throws, including for a <see langword="null"/> input.</remarks>
    public static bool TryCreate(string? value, out CapabilityToken token)
    {
        if (value is not null && IdentifierGrammar.IsSegment(value))
        {
            token = new CapabilityToken(value);
            return true;
        }

        token = default;
        return false;
    }

    /// <summary>
    /// Determines whether one token sorts before another in canonical order.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> sorts before <paramref name="right"/>;
    /// otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator <(CapabilityToken left, CapabilityToken right) => left.CompareTo(right) < 0;

    /// <summary>
    /// Determines whether one token sorts before another in canonical order, or is equal to it.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> does not sort after
    /// <paramref name="right"/>; otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator <=(CapabilityToken left, CapabilityToken right) => left.CompareTo(right) <= 0;

    /// <summary>
    /// Determines whether one token sorts after another in canonical order.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> sorts after <paramref name="right"/>;
    /// otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator >(CapabilityToken left, CapabilityToken right) => left.CompareTo(right) > 0;

    /// <summary>
    /// Determines whether one token sorts after another in canonical order, or is equal to it.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> does not sort before
    /// <paramref name="right"/>; otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator >=(CapabilityToken left, CapabilityToken right) => left.CompareTo(right) >= 0;

    /// <summary>
    /// Compares this token with another in canonical order.
    /// </summary>
    /// <param name="other">The token to compare with.</param>
    /// <returns>
    /// A negative number when this instance sorts first, zero when the two are equal, and a positive
    /// number when <paramref name="other"/> sorts first.
    /// </returns>
    /// <remarks>
    /// The order is ordinal over the token text, which is the canonical order a document is written in:
    /// it depends on no ambient culture, and it is the same order the serializer emits and the strict
    /// reader enforces. The default value carries no text and sorts before every created one, so the
    /// order is total over every instance instead of leaving a hole at the default; ordering is
    /// consistent with equality, because two values compare equal exactly when they are equal.
    /// </remarks>
    public int CompareTo(CapabilityToken other) => string.CompareOrdinal(_value, other._value);

    /// <summary>
    /// Compares this instance with another object in canonical order.
    /// </summary>
    /// <param name="obj">The object to compare with, which may be <see langword="null"/>.</param>
    /// <returns>
    /// A negative number when this instance sorts first, zero when the two are equal, and a positive
    /// number when <paramref name="obj"/> sorts first. A <see langword="null"/> always sorts first, which
    /// is the convention every <see cref="IComparable"/> implementation in .NET follows.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="obj"/> is not a <see cref="CapabilityToken"/>.</exception>
    /// <remarks>
    /// The non-generic interface is implemented explicitly and exists for one reason: F#'s
    /// <c>comparison</c> constraint is satisfied by <see cref="IComparable"/> and not by
    /// <see cref="IComparable{T}"/>, so without it this type cannot key an F# <c>Set</c> or <c>Map</c> —
    /// which is what the F# frontend needs of it. C# callers bind to
    /// <see cref="CompareTo(CapabilityToken)"/> instead and box nothing.
    /// </remarks>
    int IComparable.CompareTo(object? obj) => obj switch
    {
        null => 1,
        CapabilityToken other => CompareTo(other),
        _ => throw new ArgumentException($"The argument must be a {nameof(CapabilityToken)}.", nameof(obj)),
    };

    /// <summary>
    /// Returns the token text, or a diagnostic literal when this instance is the default value.
    /// </summary>
    /// <returns>
    /// The token text, or <c>"(default CapabilityToken)"</c> when <see cref="IsDefault"/> is
    /// <see langword="true"/>.
    /// </returns>
    /// <remarks>
    /// This method never throws, so logging and debugger display stay safe for every instance, including
    /// the default one.
    /// </remarks>
    public override string ToString() => _value ?? "(default CapabilityToken)";
}
