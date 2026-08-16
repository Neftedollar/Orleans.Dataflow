using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Definition;

/// <summary>
/// An immutable, in-memory <see cref="IStageCatalog"/> built once from a set of stage specifications.
/// </summary>
/// <remarks>
/// <para>
/// The catalog is closed the moment it is created: there is no add, no remove, and no mutable backing
/// collection, so a graph being validated cannot change what the catalog resolves while it is being
/// validated (ADR 0001).
/// </para>
/// <para>
/// A catalog is canonical by construction. <see cref="Create"/> sorts the specifications ordinally by
/// provider identifier, then ordinally by stage identifier, then by ascending major version, so two
/// catalogs built from the same specifications in different orders enumerate and serialize identically.
/// Major versions are compared as numbers rather than as text, so <c>v2</c> precedes <c>v10</c>.
/// </para>
/// <para>
/// Lookup is a dictionary probe rather than a scan of <see cref="Specifications"/>: a compiler resolves
/// one reference per node, and the catalog of a large deployment is not small.
/// </para>
/// </remarks>
public sealed class StageCatalog : IStageCatalog
{
    private readonly Dictionary<StageRef, StageSpecification> _byStage;

    /// <summary>
    /// Initializes a new instance of the <see cref="StageCatalog"/> class.
    /// </summary>
    /// <param name="specifications">The validated, canonically ordered, read-only specifications.</param>
    /// <param name="byStage">The lookup index over the same specifications.</param>
    /// <remarks>
    /// The constructor is private, so a catalog cannot be built around <see cref="Create"/> and the two
    /// views of the same specifications cannot be made to disagree.
    /// </remarks>
    private StageCatalog(
        IReadOnlyList<StageSpecification> specifications,
        Dictionary<StageRef, StageSpecification> byStage)
    {
        Specifications = specifications;
        _byStage = byStage;
    }

    /// <inheritdoc/>
    public IReadOnlyList<StageSpecification> Specifications { get; }

    /// <summary>
    /// Creates a canonical, valid <see cref="StageCatalog"/>.
    /// </summary>
    /// <param name="specifications">The specifications to register, in any order.</param>
    /// <returns>The validated catalog, with its specifications in canonical order.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="specifications"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// An element is <see langword="null"/>, or two elements declare the same <see cref="StageRef"/>. The
    /// message is a numbered list of every violation found, so one call reports every problem rather than
    /// one problem per call.
    /// </exception>
    /// <remarks>
    /// <para>
    /// A duplicate reference is rejected rather than resolved by a last-one-wins rule. Two registrations
    /// of one reference are a deployment mistake, and silently picking one of them would make which stage
    /// a document runs depend on registration order.
    /// </para>
    /// <para>
    /// The sequence is enumerated exactly once and copied, so a caller may pass a lazy sequence and may
    /// keep mutating its own collection afterwards without affecting the catalog.
    /// </para>
    /// </remarks>
    public static StageCatalog Create(IEnumerable<StageSpecification> specifications)
    {
        ArgumentNullException.ThrowIfNull(specifications);

        StageSpecification[] specificationArray = [.. specifications];
        Dictionary<StageRef, StageSpecification> byStage = [];
        List<string> violations = [];

        for (int index = 0; index < specificationArray.Length; index++)
        {
            StageSpecification specification = specificationArray[index];

            if (specification is null)
            {
                violations.Add(string.Create(CultureInfo.InvariantCulture, $"specifications[{index}] is null"));
                continue;
            }

            if (!byStage.TryAdd(specification.Stage, specification))
            {
                violations.Add(string.Create(
                    CultureInfo.InvariantCulture,
                    $"specifications[{index}] repeats the stage reference '{specification.Stage}', and a catalog registers each reference at most once"));
            }
        }

        if (violations.Count > 0)
        {
            throw new ArgumentException(FormatViolations(violations), nameof(specifications));
        }

        // The sort key is unique on validated input, because duplicate stage references are rejected
        // above. The order is therefore total, and an unstable sort still yields one deterministic result
        // for every permutation of the same specifications.
        Array.Sort(specificationArray, CompareSpecifications);

        return new StageCatalog(Array.AsReadOnly(specificationArray), byStage);
    }

    /// <inheritdoc/>
    public bool TryGetSpecification(
        StageRef stageRef,
        [MaybeNullWhen(false)] out StageSpecification specification) =>
        _byStage.TryGetValue(stageRef, out specification);

    /// <summary>
    /// Returns a one-line diagnostic summary of this catalog.
    /// </summary>
    /// <returns>Text of the form <c>stage catalog (2 specifications)</c>.</returns>
    /// <remarks>
    /// The count is formatted with the invariant culture so that the text is identical under every
    /// ambient culture. Naming the registered stages instead would put an unbounded list into a log line;
    /// the durable summary of a catalog's contents is its <see cref="CatalogFingerprint"/>.
    /// </remarks>
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"stage catalog ({Specifications.Count} specification{(Specifications.Count == 1 ? string.Empty : "s")})");

    /// <summary>
    /// Compares two specifications by provider, stage, and major version.
    /// </summary>
    /// <param name="left">The left specification.</param>
    /// <param name="right">The right specification.</param>
    /// <returns>The canonical comparison result.</returns>
    /// <remarks>
    /// The two identifier components are compared ordinally, as every identifier in this model is, and
    /// the major version numerically, because it is a number and not an identifier segment.
    /// </remarks>
    private static int CompareSpecifications(StageSpecification left, StageSpecification right)
    {
        int comparison = string.CompareOrdinal(left.Stage.Provider.Value, right.Stage.Provider.Value);

        if (comparison != 0)
        {
            return comparison;
        }

        comparison = string.CompareOrdinal(left.Stage.Stage.Value, right.Stage.Stage.Value);

        return comparison != 0 ? comparison : left.Stage.MajorVersion.CompareTo(right.Stage.MajorVersion);
    }

    /// <summary>
    /// Renders the collected violations as one numbered list.
    /// </summary>
    /// <param name="violations">The violations, in the order they were found.</param>
    /// <returns>A message whose first line states the count and whose remaining lines are numbered.</returns>
    private static string FormatViolations(List<string> violations)
    {
        StringBuilder message = new();

        message.Append(CultureInfo.InvariantCulture, $"The stage catalog breaks {violations.Count} ");
        message.Append(violations.Count == 1 ? "invariant:" : "invariants:");

        for (int index = 0; index < violations.Count; index++)
        {
            message.Append(Environment.NewLine)
                .Append(CultureInfo.InvariantCulture, $"{index + 1}. {violations[index]}.");
        }

        return message.ToString();
    }
}
