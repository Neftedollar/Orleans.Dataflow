using System.Globalization;

namespace Orleans.Dataflow.Identity;

/// <summary>
/// The identity of a logical stage occurrence inside one graph lineage.
/// </summary>
/// <remarks>
/// <para>
/// A node identifier is a path of one or more identifier segments joined by <see cref="Separator"/>,
/// such as <c>normalize</c> or <c>orders/import-a/normalize</c>. Every segment obeys the shared
/// segment grammar <c>[a-z0-9]+(-[a-z0-9]+)*</c>; a path holds at most <see cref="MaxDepth"/> segments
/// and at most <see cref="MaxPathLength"/> characters in total.
/// </para>
/// <para>
/// The hierarchy exists for import scoping. A reusable graph fragment carries local node identifiers,
/// and importing it requires a stable scope segment; <see cref="InScope(string)"/> rebases every
/// internal identifier below that scope. Because rebasing is pure prefixing, importing the same
/// fragment under two different scopes deterministically yields two disjoint sets of node identifiers,
/// and nested imports compose by nesting prefixes (ADR 0001, identity).
/// </para>
/// <para>
/// The default value carries no path: <see cref="IsDefault"/> reports it, <see cref="Value"/>,
/// <see cref="Depth"/>, and <see cref="GetSegments"/> throw for it, and <see cref="ToString"/> renders
/// a diagnostic literal for it rather than throwing. Equality is ordinal equality of the canonical
/// path text, and <see cref="CompareTo"/> orders identifiers over that same full path text; the
/// default value sorts before every created one.
/// </para>
/// </remarks>
public readonly record struct NodeId : IComparable<NodeId>, IComparable
{
    /// <summary>
    /// The character that separates path segments in the canonical form.
    /// </summary>
    public const char Separator = '/';

    /// <summary>
    /// The maximum number of segments in a node identifier path.
    /// </summary>
    /// <remarks>
    /// The bound exists so that nested fragment imports cannot grow identifiers without limit, and so
    /// that validation cost stays bounded for untrusted graph data.
    /// </remarks>
    public const int MaxDepth = 16;

    /// <summary>
    /// The maximum length, in characters, of a canonical node identifier path including separators.
    /// </summary>
    public const int MaxPathLength = 256;

    private const string SeparatorText = "/";

    private readonly string? _value;

    private NodeId(string value) => _value = value;

    /// <summary>
    /// Gets the canonical path text.
    /// </summary>
    /// <value>One or more identifier segments joined by <see cref="Separator"/>.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which carries no path.
    /// </exception>
    public string Value =>
        _value ?? throw new InvalidOperationException(IdentifierGrammar.DescribeDefaultAccess(nameof(NodeId)));

    /// <summary>
    /// Gets a value indicating whether this instance is the uninitialized default value.
    /// </summary>
    /// <value><see langword="true"/> for the default value; otherwise <see langword="false"/>.</value>
    public bool IsDefault => _value is null;

    /// <summary>
    /// Gets the number of segments in the path.
    /// </summary>
    /// <value>A value between <c>1</c> and <see cref="MaxDepth"/>.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which carries no path.
    /// </exception>
    /// <remarks>
    /// The depth is counted on each call, which is linear in the path length and bounded by
    /// <see cref="MaxPathLength"/>. The property allocates nothing.
    /// </remarks>
    public int Depth => Value.AsSpan().Count(Separator) + 1;

    /// <summary>
    /// Creates a single-segment <see cref="NodeId"/>.
    /// </summary>
    /// <param name="segment">The identifier segment, which must not contain <see cref="Separator"/>.</param>
    /// <returns>The validated node identifier.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="segment"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="segment"/> is not a valid identifier segment. A multi-segment path is rejected
    /// here because <see cref="Separator"/> is not a segment character; use <see cref="Parse(string)"/>
    /// for paths.
    /// </exception>
    public static NodeId Create(string segment)
    {
        IdentifierGrammar.EnsureSegment(segment, $"{nameof(NodeId)} segment", nameof(segment));
        return new NodeId(segment);
    }

    /// <summary>
    /// Parses a canonical node identifier path.
    /// </summary>
    /// <param name="path">One or more identifier segments joined by <see cref="Separator"/>.</param>
    /// <returns>The validated node identifier.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="path"/> is empty, exceeds <see cref="MaxPathLength"/> or <see cref="MaxDepth"/>,
    /// or contains a segment that is not a valid identifier segment. The message names the offending
    /// value and the rule it breaks.
    /// </exception>
    public static NodeId Parse(string path)
    {
        ArgumentNullException.ThrowIfNull(path);

        string? violation = DescribePathViolation(path);

        if (violation is not null)
        {
            throw new ArgumentException(FormatPathError(path, violation), nameof(path));
        }

        return new NodeId(path);
    }

    /// <summary>
    /// Attempts to parse a canonical node identifier path.
    /// </summary>
    /// <param name="path">The candidate path, which may be <see langword="null"/>.</param>
    /// <param name="nodeId">
    /// When this method returns <see langword="true"/>, the validated node identifier; otherwise the
    /// default value.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="path"/> is a valid canonical path; otherwise
    /// <see langword="false"/>.
    /// </returns>
    /// <remarks>This method never throws, including for a <see langword="null"/> input.</remarks>
    public static bool TryParse(string? path, out NodeId nodeId)
    {
        if (path is not null && DescribePathViolation(path) is null)
        {
            nodeId = new NodeId(path);
            return true;
        }

        nodeId = default;
        return false;
    }

    /// <summary>
    /// Rebases this node identifier below an import scope.
    /// </summary>
    /// <param name="scopeSegment">The scope segment to prefix, which must be a valid identifier segment.</param>
    /// <returns>
    /// A new node identifier whose path is <paramref name="scopeSegment"/>, a separator, and this
    /// identifier's path.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="scopeSegment"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="scopeSegment"/> is not a valid identifier segment, or prefixing it would push
    /// the result past <see cref="MaxDepth"/> or <see cref="MaxPathLength"/>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which carries no path to rebase.
    /// </exception>
    /// <remarks>
    /// <para>
    /// This is the identity-rebasing primitive for fragment import. Rebasing is deterministic pure
    /// prefixing: equal inputs always produce equal results, distinct scopes always produce disjoint
    /// result sets, and nesting scopes composes by nesting prefixes.
    /// </para>
    /// <para>
    /// The instance is not modified; a new value is returned.
    /// </para>
    /// </remarks>
    public NodeId InScope(string scopeSegment)
    {
        IdentifierGrammar.EnsureSegment(scopeSegment, $"{nameof(NodeId)} scope segment", nameof(scopeSegment));

        string path = Value;
        int rebasedLength = scopeSegment.Length + SeparatorText.Length + path.Length;

        if (rebasedLength > MaxPathLength)
        {
            throw new ArgumentException(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"Rebasing {nameof(NodeId)} '{path}' below scope '{scopeSegment}' produces a path of {rebasedLength} characters, which exceeds the maximum of {MaxPathLength}."),
                nameof(scopeSegment));
        }

        int rebasedDepth = Depth + 1;

        if (rebasedDepth > MaxDepth)
        {
            throw new ArgumentException(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"Rebasing {nameof(NodeId)} '{path}' below scope '{scopeSegment}' produces a path of {rebasedDepth} segments, which exceeds the maximum depth of {MaxDepth}."),
                nameof(scopeSegment));
        }

        return new NodeId(string.Concat(scopeSegment, SeparatorText, path));
    }

    /// <summary>
    /// Returns the path segments in order, from the outermost scope to the local segment.
    /// </summary>
    /// <returns>A list of <see cref="Depth"/> segments.</returns>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which carries no path.
    /// </exception>
    /// <remarks>
    /// This is a method rather than a property because each call splits the path and allocates a fresh
    /// list; callers that need the segments repeatedly should hold the result. The returned list is
    /// never shared with another call or with the identifier's own state.
    /// </remarks>
    public IReadOnlyList<string> GetSegments() => Value.Split(Separator);

    /// <summary>
    /// Determines whether one node identifier sorts before another in canonical order.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> sorts before <paramref name="right"/>;
    /// otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator <(NodeId left, NodeId right) => left.CompareTo(right) < 0;

    /// <summary>
    /// Determines whether one node identifier sorts before another in canonical order, or is equal to it.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> does not sort after
    /// <paramref name="right"/>; otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator <=(NodeId left, NodeId right) => left.CompareTo(right) <= 0;

    /// <summary>
    /// Determines whether one node identifier sorts after another in canonical order.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> sorts after <paramref name="right"/>;
    /// otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator >(NodeId left, NodeId right) => left.CompareTo(right) > 0;

    /// <summary>
    /// Determines whether one node identifier sorts after another in canonical order, or is equal to it.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> does not sort before
    /// <paramref name="right"/>; otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator >=(NodeId left, NodeId right) => left.CompareTo(right) >= 0;

    /// <summary>
    /// Compares this node identifier with another in canonical order.
    /// </summary>
    /// <param name="other">The node identifier to compare with.</param>
    /// <returns>
    /// A negative number when this instance sorts first, zero when the two are equal, and a positive
    /// number when <paramref name="other"/> sorts first.
    /// </returns>
    /// <remarks>
    /// <para>
    /// The comparison is over the full <see cref="Separator"/>-joined path text, not segment by segment,
    /// which is the order ADR 0003 fixes for a document's nodes: <c>a</c> sorts before <c>a-b</c>, and
    /// <c>a-b</c> before <c>a/b</c>, because <c>-</c> precedes <c>/</c> in code-point order. A canonical
    /// order only has to be total, deterministic, and documented, and comparing whole paths is the
    /// cheapest rule that is all three.
    /// </para>
    /// <para>
    /// The default value carries no path and sorts before every created one, so the order is total over
    /// every instance instead of leaving a hole at the default. Ordering is consistent with equality,
    /// because two identifiers compare equal exactly when their path texts are equal.
    /// </para>
    /// </remarks>
    public int CompareTo(NodeId other) => string.CompareOrdinal(_value, other._value);

    /// <summary>
    /// Compares this instance with another object in canonical order.
    /// </summary>
    /// <param name="obj">The object to compare with, which may be <see langword="null"/>.</param>
    /// <returns>
    /// A negative number when this instance sorts first, zero when the two are equal, and a positive
    /// number when <paramref name="obj"/> sorts first. A <see langword="null"/> always sorts first, which
    /// is the convention every <see cref="IComparable"/> implementation in .NET follows.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="obj"/> is not a <see cref="NodeId"/>.</exception>
    /// <remarks>
    /// The non-generic interface is implemented explicitly and exists for one reason: F#'s
    /// <c>comparison</c> constraint is satisfied by <see cref="IComparable"/> and not by
    /// <see cref="IComparable{T}"/>, so without it this type cannot key an F# <c>Set</c> or <c>Map</c> —
    /// which is what the F# frontend needs of it. C# callers bind to
    /// <see cref="CompareTo(NodeId)"/> instead and box nothing.
    /// </remarks>
    int IComparable.CompareTo(object? obj) => obj switch
    {
        null => 1,
        NodeId other => CompareTo(other),
        _ => throw new ArgumentException($"The argument must be a {nameof(NodeId)}.", nameof(obj)),
    };

    /// <summary>
    /// Returns the canonical path text, or a diagnostic literal when this instance is the default value.
    /// </summary>
    /// <returns>
    /// The canonical path, or <c>"(default NodeId)"</c> when <see cref="IsDefault"/> is
    /// <see langword="true"/>.
    /// </returns>
    /// <remarks>
    /// This method never throws, so logging and debugger display stay safe for every instance,
    /// including the default one.
    /// </remarks>
    public override string ToString() => _value ?? "(default NodeId)";

    private static string? DescribePathViolation(ReadOnlySpan<char> path)
    {
        if (path.Length == 0)
        {
            return "the path is empty";
        }

        if (path.Length > MaxPathLength)
        {
            return string.Create(
                CultureInfo.InvariantCulture,
                $"the path is {path.Length} characters long, which exceeds the maximum of {MaxPathLength}");
        }

        ReadOnlySpan<char> remaining = path;
        int depth = 0;

        while (true)
        {
            int separatorIndex = remaining.IndexOf(Separator);
            ReadOnlySpan<char> segment = separatorIndex < 0 ? remaining : remaining[..separatorIndex];
            depth++;

            if (depth > MaxDepth)
            {
                return string.Create(
                    CultureInfo.InvariantCulture,
                    $"the path has more than {MaxDepth} segments");
            }

            string? violation = IdentifierGrammar.DescribeSegmentViolation(segment);

            if (violation is not null)
            {
                return string.Create(
                    CultureInfo.InvariantCulture,
                    $"segment {depth} ('{segment}') is invalid because {violation}");
            }

            if (separatorIndex < 0)
            {
                return null;
            }

            remaining = remaining[(separatorIndex + 1)..];
        }
    }

    private static string FormatPathError(ReadOnlySpan<char> path, string violation) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"'{path}' is not a valid {nameof(NodeId)}: {violation}. A node identifier is 1 to {MaxDepth} segments matching {IdentifierGrammar.SegmentGrammar}, joined by '{Separator}', with a total length of at most {MaxPathLength} characters.");
}
