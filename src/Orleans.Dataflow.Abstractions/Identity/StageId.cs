namespace Orleans.Dataflow.Identity;

/// <summary>
/// Stable identity of a stage implementation family within one provider.
/// </summary>
/// <remarks>
/// <para>
/// A stage identifier is scoped by its provider; together with a compatibility major version it forms
/// a <see cref="StageRef"/>, which is what a node in a graph document actually references.
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
/// rather than throwing. Equality is ordinal over the identifier text.
/// </para>
/// </remarks>
public readonly record struct StageId
{
    private readonly string? _value;

    private StageId(string value) => _value = value;

    /// <summary>
    /// Gets the validated identifier text.
    /// </summary>
    /// <value>The canonical identifier segment.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which carries no identifier.
    /// </exception>
    public string Value =>
        _value ?? throw new InvalidOperationException(IdentifierGrammar.DescribeDefaultAccess(nameof(StageId)));

    /// <summary>
    /// Gets a value indicating whether this instance is the uninitialized default value.
    /// </summary>
    /// <value><see langword="true"/> for the default value; otherwise <see langword="false"/>.</value>
    public bool IsDefault => _value is null;

    /// <summary>
    /// Creates a <see cref="StageId"/> from its text form.
    /// </summary>
    /// <param name="value">The identifier segment.</param>
    /// <returns>The validated identifier.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="value"/> does not satisfy the identifier segment grammar. The message names the
    /// offending value and the rule it breaks.
    /// </exception>
    public static StageId Create(string value)
    {
        IdentifierGrammar.EnsureSegment(value, nameof(StageId), nameof(value));
        return new StageId(value);
    }

    /// <summary>
    /// Attempts to create a <see cref="StageId"/> from its text form.
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
    public static bool TryCreate(string? value, out StageId identifier)
    {
        if (value is not null && IdentifierGrammar.IsSegment(value))
        {
            identifier = new StageId(value);
            return true;
        }

        identifier = default;
        return false;
    }

    /// <summary>
    /// Returns the identifier text, or a diagnostic literal when this instance is the default value.
    /// </summary>
    /// <returns>
    /// The identifier text, or <c>"(default StageId)"</c> when <see cref="IsDefault"/> is
    /// <see langword="true"/>.
    /// </returns>
    /// <remarks>
    /// This method never throws, so logging and debugger display stay safe for every instance,
    /// including the default one.
    /// </remarks>
    public override string ToString() => _value ?? "(default StageId)";
}
