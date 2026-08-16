using System.Globalization;

namespace Orleans.Dataflow.Identity;

/// <summary>
/// A reference from a graph node to a registered stage implementation family and its compatibility
/// major version.
/// </summary>
/// <remarks>
/// <para>
/// A stage reference is the only way graph data names behavior. It resolves through a trusted stage
/// catalog registered by deployment code, never by loading a CLR type named in the document
/// (ADR 0001, provider boundary).
/// </para>
/// <para>
/// <see cref="MajorVersion"/> is the compatibility version of the stage contract, not the package
/// version of the assembly that implements it. Two stages with the same provider and stage identifier
/// but different major versions are different references, because their parameter and port contracts
/// are allowed to differ.
/// </para>
/// <para>
/// The default value carries no reference: <see cref="IsDefault"/> reports it, the component
/// properties throw for it, and <see cref="ToString"/> renders a diagnostic literal for it rather than
/// throwing.
/// </para>
/// </remarks>
public readonly record struct StageRef
{
    /// <summary>
    /// The lowest valid stage compatibility major version.
    /// </summary>
    /// <remarks>
    /// Numbering starts at <c>1</c> so that the default <see cref="StageRef"/> is distinguishable from
    /// a valid reference without a separate flag.
    /// </remarks>
    public const int FirstMajorVersion = 1;

    private readonly ProviderId _provider;
    private readonly StageId _stage;
    private readonly int _majorVersion;

    private StageRef(ProviderId provider, StageId stage, int majorVersion)
    {
        _provider = provider;
        _stage = stage;
        _majorVersion = majorVersion;
    }

    /// <summary>
    /// Gets the provider that owns the referenced stage.
    /// </summary>
    /// <value>A created <see cref="ProviderId"/>.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which carries no reference.
    /// </exception>
    public ProviderId Provider => IsDefault ? throw DefaultAccess() : _provider;

    /// <summary>
    /// Gets the referenced stage implementation family within <see cref="Provider"/>.
    /// </summary>
    /// <value>A created <see cref="StageId"/>.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which carries no reference.
    /// </exception>
    public StageId Stage => IsDefault ? throw DefaultAccess() : _stage;

    /// <summary>
    /// Gets the compatibility major version of the referenced stage contract.
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
    /// Creates a <see cref="StageRef"/> from its components.
    /// </summary>
    /// <param name="provider">The provider that owns the stage; must not be the default value.</param>
    /// <param name="stage">The stage implementation family; must not be the default value.</param>
    /// <param name="majorVersion">
    /// The compatibility major version, which must be at least <see cref="FirstMajorVersion"/>.
    /// </param>
    /// <returns>The validated stage reference.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="provider"/> or <paramref name="stage"/> is the default value.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="majorVersion"/> is less than <see cref="FirstMajorVersion"/>. The message names
    /// the offending value.
    /// </exception>
    public static StageRef Create(ProviderId provider, StageId stage, int majorVersion)
    {
        if (provider.IsDefault)
        {
            throw new ArgumentException(
                $"A {nameof(StageRef)} requires a created {nameof(ProviderId)}; the default {nameof(ProviderId)} names no provider.",
                nameof(provider));
        }

        if (stage.IsDefault)
        {
            throw new ArgumentException(
                $"A {nameof(StageRef)} requires a created {nameof(StageId)}; the default {nameof(StageId)} names no stage.",
                nameof(stage));
        }

        ArgumentOutOfRangeException.ThrowIfLessThan(majorVersion, FirstMajorVersion);

        return new StageRef(provider, stage, majorVersion);
    }

    /// <summary>
    /// Attempts to create a <see cref="StageRef"/> from its components.
    /// </summary>
    /// <param name="provider">The candidate provider.</param>
    /// <param name="stage">The candidate stage.</param>
    /// <param name="majorVersion">The candidate compatibility major version.</param>
    /// <param name="stageRef">
    /// When this method returns <see langword="true"/>, the validated reference; otherwise the default
    /// value.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when all three components are valid; otherwise <see langword="false"/>.
    /// </returns>
    /// <remarks>This method never throws.</remarks>
    public static bool TryCreate(ProviderId provider, StageId stage, int majorVersion, out StageRef stageRef)
    {
        if (!provider.IsDefault && !stage.IsDefault && majorVersion >= FirstMajorVersion)
        {
            stageRef = new StageRef(provider, stage, majorVersion);
            return true;
        }

        stageRef = default;
        return false;
    }

    /// <summary>
    /// Returns the canonical text form of this reference, or a diagnostic literal when this instance is
    /// the default value.
    /// </summary>
    /// <returns>
    /// Text of the form <c>provider/stage@v1</c>, or <c>"(default StageRef)"</c> when
    /// <see cref="IsDefault"/> is <see langword="true"/>.
    /// </returns>
    /// <remarks>
    /// The version is formatted with the invariant culture so that the text is identical under every
    /// ambient culture. The method never throws.
    /// </remarks>
    public override string ToString() =>
        IsDefault
            ? "(default StageRef)"
            : string.Create(CultureInfo.InvariantCulture, $"{_provider}/{_stage}@v{_majorVersion}");

    private static InvalidOperationException DefaultAccess() =>
        new(IdentifierGrammar.DescribeDefaultAccess(nameof(StageRef)));
}
