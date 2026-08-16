using System.Globalization;

namespace Orleans.Dataflow.Identity;

/// <summary>
/// A reference to a versioned data, parameter, policy, or result contract.
/// </summary>
/// <remarks>
/// <para>
/// A contract reference is how graph data names a payload shape: a <see cref="ContractId"/> plus the
/// compatibility major version of that contract. Contract identity is never a CLR type name, because a
/// type name is neither language-neutral nor stable across refactoring (ADR 0001).
/// </para>
/// <para>
/// Two references are compatible only when both the <see cref="Contract"/> and the
/// <see cref="MajorVersion"/> are equal. Finer-grained compatibility, such as accepting an additive
/// minor version, is deliberately absent from the M0 model: an exact match is the only rule, so
/// compatibility can be relaxed later without invalidating documents already written.
/// </para>
/// <para>
/// The default value carries no reference: <see cref="IsDefault"/> reports it, the component properties
/// throw for it, and <see cref="ToString"/> renders a diagnostic literal for it rather than throwing.
/// <see cref="CompareTo"/> orders references by contract, then major version, and sorts the default
/// value before every created one.
/// </para>
/// </remarks>
public readonly record struct ContractReference : IComparable<ContractReference>, IComparable
{
    /// <summary>
    /// The lowest valid contract compatibility major version.
    /// </summary>
    /// <remarks>
    /// Numbering starts at <c>1</c> so that the default <see cref="ContractReference"/> is
    /// distinguishable from a valid reference without a separate flag.
    /// </remarks>
    public const int FirstMajorVersion = 1;

    private readonly ContractId _contract;
    private readonly int _majorVersion;

    private ContractReference(ContractId contract, int majorVersion)
    {
        _contract = contract;
        _majorVersion = majorVersion;
    }

    /// <summary>
    /// Gets the identity of the referenced contract.
    /// </summary>
    /// <value>A created <see cref="ContractId"/>.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which carries no reference.
    /// </exception>
    public ContractId Contract => IsDefault ? throw DefaultAccess() : _contract;

    /// <summary>
    /// Gets the compatibility major version of the referenced contract.
    /// </summary>
    /// <value>A version of at least <see cref="FirstMajorVersion"/>.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which carries no reference.
    /// </exception>
    public int MajorVersion => IsDefault ? throw DefaultAccess() : _majorVersion;

    /// <summary>
    /// Gets a value indicating whether this instance is the uninitialized default value.
    /// </summary>
    /// <value><see langword="true"/> for the default value; otherwise <see langword="false"/>.</value>
    /// <remarks>
    /// A created reference always has a major version of at least <see cref="FirstMajorVersion"/>, so a
    /// zero major version identifies the default instance exactly.
    /// </remarks>
    public bool IsDefault => _majorVersion == 0;

    /// <summary>
    /// Creates a <see cref="ContractReference"/> from its components.
    /// </summary>
    /// <param name="contract">The referenced contract; must not be the default value.</param>
    /// <param name="majorVersion">
    /// The compatibility major version, which must be at least <see cref="FirstMajorVersion"/>.
    /// </param>
    /// <returns>The validated contract reference.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="contract"/> is the default value.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="majorVersion"/> is less than <see cref="FirstMajorVersion"/>. The message names
    /// the offending value.
    /// </exception>
    public static ContractReference Create(ContractId contract, int majorVersion)
    {
        if (contract.IsDefault)
        {
            throw new ArgumentException(
                $"A {nameof(ContractReference)} requires a created {nameof(ContractId)}; the default {nameof(ContractId)} names no contract.",
                nameof(contract));
        }

        ArgumentOutOfRangeException.ThrowIfLessThan(majorVersion, FirstMajorVersion);

        return new ContractReference(contract, majorVersion);
    }

    /// <summary>
    /// Attempts to create a <see cref="ContractReference"/> from its components.
    /// </summary>
    /// <param name="contract">The candidate contract.</param>
    /// <param name="majorVersion">The candidate compatibility major version.</param>
    /// <param name="reference">
    /// When this method returns <see langword="true"/>, the validated reference; otherwise the default
    /// value.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when both components are valid; otherwise <see langword="false"/>.
    /// </returns>
    /// <remarks>This method never throws.</remarks>
    public static bool TryCreate(ContractId contract, int majorVersion, out ContractReference reference)
    {
        if (!contract.IsDefault && majorVersion >= FirstMajorVersion)
        {
            reference = new ContractReference(contract, majorVersion);
            return true;
        }

        reference = default;
        return false;
    }

    /// <summary>
    /// Determines whether one contract reference sorts before another in canonical order.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> sorts before <paramref name="right"/>;
    /// otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator <(ContractReference left, ContractReference right) =>
        left.CompareTo(right) < 0;

    /// <summary>
    /// Determines whether one contract reference sorts before another in canonical order, or is equal to
    /// it.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> does not sort after
    /// <paramref name="right"/>; otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator <=(ContractReference left, ContractReference right) =>
        left.CompareTo(right) <= 0;

    /// <summary>
    /// Determines whether one contract reference sorts after another in canonical order.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> sorts after <paramref name="right"/>;
    /// otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator >(ContractReference left, ContractReference right) =>
        left.CompareTo(right) > 0;

    /// <summary>
    /// Determines whether one contract reference sorts after another in canonical order, or is equal to
    /// it.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> does not sort before
    /// <paramref name="right"/>; otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator >=(ContractReference left, ContractReference right) =>
        left.CompareTo(right) >= 0;

    /// <summary>
    /// Compares this reference with another by contract, then major version.
    /// </summary>
    /// <param name="other">The reference to compare with.</param>
    /// <returns>
    /// A negative number when this instance sorts first, zero when the two are equal, and a positive
    /// number when <paramref name="other"/> sorts first.
    /// </returns>
    /// <remarks>
    /// The component order is the reading order of the text form <c>contract@v1</c>: the contract
    /// identifier compares ordinally over its text and the version compares numerically, so <c>@v2</c>
    /// precedes <c>@v10</c> rather than following it. The default value carries no components and sorts
    /// before every created one, so the order is total over every instance. Ordering is consistent with
    /// equality, because two references compare equal exactly when both components are equal.
    /// </remarks>
    public int CompareTo(ContractReference other)
    {
        int comparison = _contract.CompareTo(other._contract);

        return comparison != 0 ? comparison : _majorVersion.CompareTo(other._majorVersion);
    }

    /// <summary>
    /// Compares this instance with another object in canonical order.
    /// </summary>
    /// <param name="obj">The object to compare with, which may be <see langword="null"/>.</param>
    /// <returns>
    /// A negative number when this instance sorts first, zero when the two are equal, and a positive
    /// number when <paramref name="obj"/> sorts first. A <see langword="null"/> always sorts first, which
    /// is the convention every <see cref="IComparable"/> implementation in .NET follows.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="obj"/> is not a <see cref="ContractReference"/>.</exception>
    /// <remarks>
    /// The non-generic interface is implemented explicitly and exists for one reason: F#'s
    /// <c>comparison</c> constraint is satisfied by <see cref="IComparable"/> and not by
    /// <see cref="IComparable{T}"/>, so without it this type cannot key an F# <c>Set</c> or <c>Map</c> —
    /// which is what the F# frontend needs of it. C# callers bind to
    /// <see cref="CompareTo(ContractReference)"/> instead and box nothing.
    /// </remarks>
    int IComparable.CompareTo(object? obj) => obj switch
    {
        null => 1,
        ContractReference other => CompareTo(other),
        _ => throw new ArgumentException($"The argument must be a {nameof(ContractReference)}.", nameof(obj)),
    };

    /// <summary>
    /// Returns the canonical text form of this reference, or a diagnostic literal when this instance is
    /// the default value.
    /// </summary>
    /// <returns>
    /// Text of the form <c>contract@v1</c>, or <c>"(default ContractReference)"</c> when
    /// <see cref="IsDefault"/> is <see langword="true"/>.
    /// </returns>
    /// <remarks>
    /// The version is formatted with the invariant culture so that the text is identical under every
    /// ambient culture. The method never throws.
    /// </remarks>
    public override string ToString() =>
        IsDefault
            ? "(default ContractReference)"
            : string.Create(CultureInfo.InvariantCulture, $"{_contract}@v{_majorVersion}");

    private static InvalidOperationException DefaultAccess() =>
        new(IdentifierGrammar.DescribeDefaultAccess(nameof(ContractReference)));
}
