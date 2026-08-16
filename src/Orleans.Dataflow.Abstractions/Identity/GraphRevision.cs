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
/// <para>
/// <see cref="CompareTo"/> orders revisions numerically, which is what makes "later revision" a
/// comparison rather than a convention. Because the default value is revision zero and a created
/// revision starts at <see cref="FirstRevisionNumber"/>, the default sorts before every created one and
/// the order is total.
/// </para>
/// </remarks>
public readonly record struct GraphRevision : IComparable<GraphRevision>, IComparable
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
    /// Determines whether one revision precedes another.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> is the earlier revision; otherwise
    /// <see langword="false"/>.
    /// </returns>
    public static bool operator <(GraphRevision left, GraphRevision right) => left.CompareTo(right) < 0;

    /// <summary>
    /// Determines whether one revision precedes another or is the same revision.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> is not the later revision; otherwise
    /// <see langword="false"/>.
    /// </returns>
    public static bool operator <=(GraphRevision left, GraphRevision right) => left.CompareTo(right) <= 0;

    /// <summary>
    /// Determines whether one revision follows another.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> is the later revision; otherwise
    /// <see langword="false"/>.
    /// </returns>
    public static bool operator >(GraphRevision left, GraphRevision right) => left.CompareTo(right) > 0;

    /// <summary>
    /// Determines whether one revision follows another or is the same revision.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> is not the earlier revision; otherwise
    /// <see langword="false"/>.
    /// </returns>
    public static bool operator >=(GraphRevision left, GraphRevision right) => left.CompareTo(right) >= 0;

    /// <summary>
    /// Compares this revision with another by revision number.
    /// </summary>
    /// <param name="other">The revision to compare with.</param>
    /// <returns>
    /// A negative number when this revision is the earlier one, zero when the two are the same
    /// revision, and a positive number when <paramref name="other"/> is the earlier one.
    /// </returns>
    /// <remarks>
    /// The order is numeric rather than textual, so <c>r2</c> precedes <c>r10</c> as a reader of a
    /// lineage expects. The default value is revision zero, which no created revision can be, so it
    /// sorts before every created one and the order is total over every instance. Ordering is consistent
    /// with equality, because two revisions compare equal exactly when their numbers are equal.
    /// </remarks>
    public int CompareTo(GraphRevision other) => _value.CompareTo(other._value);

    /// <summary>
    /// Compares this instance with another object in canonical order.
    /// </summary>
    /// <param name="obj">The object to compare with, which may be <see langword="null"/>.</param>
    /// <returns>
    /// A negative number when this instance sorts first, zero when the two are equal, and a positive
    /// number when <paramref name="obj"/> sorts first. A <see langword="null"/> always sorts first, which
    /// is the convention every <see cref="IComparable"/> implementation in .NET follows.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="obj"/> is not a <see cref="GraphRevision"/>.</exception>
    /// <remarks>
    /// The non-generic interface is implemented explicitly and exists for one reason: F#'s
    /// <c>comparison</c> constraint is satisfied by <see cref="IComparable"/> and not by
    /// <see cref="IComparable{T}"/>, so without it this type cannot key an F# <c>Set</c> or <c>Map</c> —
    /// which is what the F# frontend needs of it. C# callers bind to
    /// <see cref="CompareTo(GraphRevision)"/> instead and box nothing.
    /// </remarks>
    int IComparable.CompareTo(object? obj) => obj switch
    {
        null => 1,
        GraphRevision other => CompareTo(other),
        _ => throw new ArgumentException($"The argument must be a {nameof(GraphRevision)}.", nameof(obj)),
    };

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
