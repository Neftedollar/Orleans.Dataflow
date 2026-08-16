using System.Globalization;

namespace Orleans.Dataflow.Identity;

/// <summary>
/// A monotonically increasing revision number of a graph definition.
/// </summary>
/// <remarks>
/// <para>
/// A <see cref="GraphId"/> names a graph lineage; a <see cref="GraphRevision"/> selects one immutable
/// document within it. Revisions start at <see cref="FirstRevisionNumber"/>: revision <c>0</c> does not
/// exist, so the uninitialized default struct can never be mistaken for a real revision.
/// </para>
/// <para>
/// The default value carries no revision: <see cref="IsDefault"/> reports it, <see cref="Value"/>
/// throws for it, and <see cref="ToString"/> renders a diagnostic literal for it rather than throwing.
/// </para>
/// </remarks>
public readonly record struct GraphRevision
{
    /// <summary>
    /// The number of the first revision of any graph.
    /// </summary>
    /// <remarks>
    /// Numbering starts at <c>1</c> so that the default <see cref="GraphRevision"/> is distinguishable
    /// from a valid revision without a separate flag.
    /// </remarks>
    public const int FirstRevisionNumber = 1;

    private readonly int _value;

    private GraphRevision(int value) => _value = value;

    /// <summary>
    /// Gets the revision number.
    /// </summary>
    /// <value>A positive revision number.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which carries no revision.
    /// </exception>
    public int Value =>
        _value != 0 ? _value : throw new InvalidOperationException(IdentifierGrammar.DescribeDefaultAccess(nameof(GraphRevision)));

    /// <summary>
    /// Gets a value indicating whether this instance is the uninitialized default value.
    /// </summary>
    /// <value><see langword="true"/> for the default value; otherwise <see langword="false"/>.</value>
    public bool IsDefault => _value == 0;

    /// <summary>
    /// Creates a <see cref="GraphRevision"/> from a revision number.
    /// </summary>
    /// <param name="value">The revision number, which must be at least <see cref="FirstRevisionNumber"/>.</param>
    /// <returns>The validated revision.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="value"/> is less than <see cref="FirstRevisionNumber"/>. The message names the
    /// offending value.
    /// </exception>
    public static GraphRevision Create(int value)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(value, FirstRevisionNumber);
        return new GraphRevision(value);
    }

    /// <summary>
    /// Attempts to create a <see cref="GraphRevision"/> from a revision number.
    /// </summary>
    /// <param name="value">The candidate revision number.</param>
    /// <param name="revision">
    /// When this method returns <see langword="true"/>, the validated revision; otherwise the default
    /// value.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="value"/> is at least
    /// <see cref="FirstRevisionNumber"/>; otherwise <see langword="false"/>.
    /// </returns>
    /// <remarks>This method never throws.</remarks>
    public static bool TryCreate(int value, out GraphRevision revision)
    {
        if (value >= FirstRevisionNumber)
        {
            revision = new GraphRevision(value);
            return true;
        }

        revision = default;
        return false;
    }

    /// <summary>
    /// Returns the revision number, or a diagnostic literal when this instance is the default value.
    /// </summary>
    /// <returns>
    /// The revision number formatted with the invariant culture, or <c>"(default GraphRevision)"</c>
    /// when <see cref="IsDefault"/> is <see langword="true"/>.
    /// </returns>
    /// <remarks>
    /// Formatting is invariant so that a revision renders identically under every ambient culture,
    /// which matters because this text appears in identifiers, logs, and durable documents. The method
    /// never throws.
    /// </remarks>
    public override string ToString() =>
        _value != 0 ? _value.ToString(CultureInfo.InvariantCulture) : "(default GraphRevision)";
}
