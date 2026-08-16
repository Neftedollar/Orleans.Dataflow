namespace Orleans.Dataflow.Identity;

/// <summary>
/// Identity of one retry or failover incarnation within a run.
/// </summary>
/// <remarks>
/// <para>
/// A run may produce several attempts; the attempt identifier distinguishes them so that retries and
/// failovers remain separable in state, logs, and metrics.
/// </para>
/// <para>
/// The value is a single identifier segment: <c>[a-z0-9]+(-[a-z0-9]+)*</c>, 1 to 64 characters of
/// lowercase ASCII letters, ASCII digits, and single interior hyphens. Lowercase is the only accepted
/// casing so that two identifiers cannot collide in a case-insensitive store, and validation uses
/// explicit ordinal character ranges so that it never depends on the ambient culture. The grammar can
/// be relaxed compatibly in a later version but is never tightened, so it starts strict.
/// </para>
/// <para>
/// The default value of this type carries no identifier: <see cref="IsDefault"/> reports it,
/// <see cref="Value"/> throws for it, and <see cref="ToString"/> renders a diagnostic literal for it
/// rather than throwing. Equality is ordinal over the identifier text, and so is the order
/// <see cref="CompareTo"/> and the comparison operators define; the default value sorts first.
/// </para>
/// </remarks>
public readonly record struct AttemptId : IComparable<AttemptId>, IComparable
{
    private readonly string? _value;

    private AttemptId(string value) => _value = value;

    /// <summary>
    /// Gets the validated identifier text.
    /// </summary>
    /// <value>The canonical identifier segment.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which carries no identifier.
    /// </exception>
    public string Value =>
        _value ?? throw new InvalidOperationException(IdentifierGrammar.DescribeDefaultAccess(nameof(AttemptId)));

    /// <summary>
    /// Gets a value indicating whether this instance is the uninitialized default value.
    /// </summary>
    /// <value><see langword="true"/> for the default value; otherwise <see langword="false"/>.</value>
    public bool IsDefault => _value is null;

    /// <summary>
    /// Creates a <see cref="AttemptId"/> from its text form.
    /// </summary>
    /// <param name="value">The identifier segment.</param>
    /// <returns>The validated identifier.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="value"/> does not satisfy the identifier segment grammar. The message names the
    /// offending value and the rule it breaks.
    /// </exception>
    public static AttemptId Create(string value)
    {
        IdentifierGrammar.EnsureSegment(value, nameof(AttemptId), nameof(value));
        return new AttemptId(value);
    }

    /// <summary>
    /// Attempts to create a <see cref="AttemptId"/> from its text form.
    /// </summary>
    /// <param name="value">The candidate identifier segment, which may be <see langword="null"/>.</param>
    /// <param name="identifier">
    /// When this method returns <see langword="true"/>, the validated identifier; otherwise the default
    /// value.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="value"/> satisfies the identifier segment grammar;
    /// otherwise <see langword="false"/>.
    /// </returns>
    /// <remarks>This method never throws, including for a <see langword="null"/> input.</remarks>
    public static bool TryCreate(string? value, out AttemptId identifier)
    {
        if (value is not null && IdentifierGrammar.IsSegment(value))
        {
            identifier = new AttemptId(value);
            return true;
        }

        identifier = default;
        return false;
    }

    /// <summary>
    /// Determines whether one identifier sorts before another in canonical order.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> sorts before <paramref name="right"/>;
    /// otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator <(AttemptId left, AttemptId right) => left.CompareTo(right) < 0;

    /// <summary>
    /// Determines whether one identifier sorts before another in canonical order, or is equal to it.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> does not sort after
    /// <paramref name="right"/>; otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator <=(AttemptId left, AttemptId right) => left.CompareTo(right) <= 0;

    /// <summary>
    /// Determines whether one identifier sorts after another in canonical order.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> sorts after <paramref name="right"/>;
    /// otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator >(AttemptId left, AttemptId right) => left.CompareTo(right) > 0;

    /// <summary>
    /// Determines whether one identifier sorts after another in canonical order, or is equal to it.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> does not sort before
    /// <paramref name="right"/>; otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator >=(AttemptId left, AttemptId right) => left.CompareTo(right) >= 0;

    /// <summary>
    /// Compares this identifier with another in canonical order.
    /// </summary>
    /// <param name="other">The identifier to compare with.</param>
    /// <returns>
    /// A negative number when this instance sorts first, zero when the two are equal, and a positive
    /// number when <paramref name="other"/> sorts first.
    /// </returns>
    /// <remarks>
    /// The order is ordinal over the identifier text, which is the canonical order a document is written in:
    /// it depends on no ambient culture, and it is the same order the serializer emits and the strict
    /// reader enforces. The default value carries no text and sorts before every created one, so the
    /// order is total over every instance instead of leaving a hole at the default; ordering is
    /// consistent with equality, because two values compare equal exactly when they are equal.
    /// </remarks>
    public int CompareTo(AttemptId other) => string.CompareOrdinal(_value, other._value);

    /// <summary>
    /// Compares this instance with another object in canonical order.
    /// </summary>
    /// <param name="obj">The object to compare with, which may be <see langword="null"/>.</param>
    /// <returns>
    /// A negative number when this instance sorts first, zero when the two are equal, and a positive
    /// number when <paramref name="obj"/> sorts first. A <see langword="null"/> always sorts first, which
    /// is the convention every <see cref="IComparable"/> implementation in .NET follows.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="obj"/> is not a <see cref="AttemptId"/>.</exception>
    /// <remarks>
    /// The non-generic interface is implemented explicitly and exists for one reason: F#'s
    /// <c>comparison</c> constraint is satisfied by <see cref="IComparable"/> and not by
    /// <see cref="IComparable{T}"/>, so without it this type cannot key an F# <c>Set</c> or <c>Map</c> —
    /// which is what the F# frontend needs of it. C# callers bind to
    /// <see cref="CompareTo(AttemptId)"/> instead and box nothing.
    /// </remarks>
    int IComparable.CompareTo(object? obj) => obj switch
    {
        null => 1,
        AttemptId other => CompareTo(other),
        _ => throw new ArgumentException($"The argument must be a {nameof(AttemptId)}.", nameof(obj)),
    };

    /// <summary>
    /// Returns the identifier text, or a diagnostic literal when this instance is the default value.
    /// </summary>
    /// <returns>
    /// The identifier text, or <c>"(default AttemptId)"</c> when <see cref="IsDefault"/> is
    /// <see langword="true"/>.
    /// </returns>
    /// <remarks>
    /// This method never throws, so logging and debugger display stay safe for every instance,
    /// including the default one.
    /// </remarks>
    public override string ToString() => _value ?? "(default AttemptId)";
}
