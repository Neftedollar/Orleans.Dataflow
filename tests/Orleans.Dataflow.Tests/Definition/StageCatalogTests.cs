using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Xunit;

namespace Orleans.Dataflow.Tests.Definition;

/// <summary>
/// Tests for <see cref="StageCatalog"/>.
/// </summary>
public sealed class StageCatalogTests
{
    private static readonly ContractReference SampleParameterContract =
        ContractReference.Create(ContractId.Create("map-parameters"), 1);

    [Fact]
    public void CreateAcceptsAnEmptyCatalog()
    {
        StageCatalog catalog = StageCatalog.Create([]);

        Assert.Empty(catalog.Specifications);
        Assert.Equal("stage catalog (0 specifications)", catalog.ToString());
    }

    [Fact]
    public void CreateRoundTripsARegisteredSpecification()
    {
        StageSpecification specification = Specification("orleans-core", "map-async", 1);
        StageCatalog catalog = StageCatalog.Create([specification]);

        Assert.Equal([specification], catalog.Specifications);
        Assert.Equal("stage catalog (1 specification)", catalog.ToString());
    }

    [Fact]
    public void SpecificationsAreOrderedByProviderThenStageThenMajorVersion()
    {
        StageCatalog catalog = StageCatalog.Create(
            [
                Specification("orleans-core", "map-async", 10),
                Specification("contoso-sinks", "to-table", 1),
                Specification("orleans-core", "map-async", 2),
                Specification("orleans-core", "from-source", 1),
            ]);

        // The major version is compared as a number, so 2 precedes 10; comparing the rendered text would
        // put 10 first and make the canonical order depend on how a version happens to be spelled.
        Assert.Equal(
            [
                "contoso-sinks/to-table@v1",
                "orleans-core/from-source@v1",
                "orleans-core/map-async@v2",
                "orleans-core/map-async@v10",
            ],
            catalog.Specifications.Select(specification => specification.Stage.ToString()));
    }

    [Fact]
    public void PermutedInputsProduceIdenticallyOrderedCatalogs()
    {
        StageSpecification first = Specification("orleans-core", "map-async", 2);
        StageSpecification second = Specification("contoso-sinks", "to-table", 1);
        StageSpecification third = Specification("orleans-core", "from-source", 1);

        StageCatalog forward = StageCatalog.Create([first, second, third]);
        StageCatalog reversed = StageCatalog.Create([third, second, first]);

        Assert.Equal(forward.Specifications, reversed.Specifications);
    }

    [Fact]
    public void TryGetSpecificationFindsARegisteredReference()
    {
        StageSpecification specification = Specification("orleans-core", "map-async", 2);
        StageCatalog catalog = StageCatalog.Create([specification, Specification("contoso-sinks", "to-table", 1)]);

        Assert.True(catalog.TryGetSpecification(Stage("orleans-core", "map-async", 2), out StageSpecification? found));
        Assert.Same(specification, found);
    }

    [Fact]
    public void TryGetSpecificationMissesAnUnregisteredReference()
    {
        StageCatalog catalog = StageCatalog.Create([Specification("orleans-core", "map-async", 2)]);

        Assert.False(catalog.TryGetSpecification(Stage("orleans-core", "map-async", 3), out StageSpecification? byVersion));
        Assert.Null(byVersion);
        Assert.False(catalog.TryGetSpecification(Stage("orleans-core", "map-sync", 2), out _));
        Assert.False(catalog.TryGetSpecification(Stage("contoso-sinks", "map-async", 2), out _));
    }

    [Fact]
    public void TwoMajorVersionsOfOneStageResolveIndependently()
    {
        // Two major versions are two references, not two spellings of one, because they are allowed to
        // declare different ports and different parameter contracts.
        StageSpecification second = Specification("orleans-core", "map-async", 2);
        StageSpecification third = Specification("orleans-core", "map-async", 3);
        StageCatalog catalog = StageCatalog.Create([second, third]);

        Assert.True(catalog.TryGetSpecification(Stage("orleans-core", "map-async", 2), out StageSpecification? found));
        Assert.Same(second, found);
        Assert.True(catalog.TryGetSpecification(Stage("orleans-core", "map-async", 3), out found));
        Assert.Same(third, found);
    }

    [Fact]
    public void TryGetSpecificationAnswersFalseForTheDefaultReferenceRatherThanThrowing()
    {
        StageCatalog catalog = StageCatalog.Create([Specification("orleans-core", "map-async", 2)]);

        // Lookup is total: every reference gets an answer, so a caller validating an untrusted document
        // never has to guard the call. The default reference names no stage, so the answer is 'no'.
        Assert.False(catalog.TryGetSpecification(default, out StageSpecification? found));
        Assert.Null(found);
    }

    [Fact]
    public void TryGetSpecificationOnAnEmptyCatalogAnswersFalse() =>
        Assert.False(StageCatalog.Create([]).TryGetSpecification(Stage("orleans-core", "map-async", 1), out _));

    [Fact]
    public void CatalogIsUsableThroughTheInterface() =>
        AssertResolvesThroughTheInterface(StageCatalog.Create([Specification("orleans-core", "map-async", 2)]));

    [Fact]
    public void CreateRejectsANullSequence() =>
        Assert.Throws<ArgumentNullException>("specifications", () => { _ = StageCatalog.Create(null!); });

    [Fact]
    public void CreateRejectsANullSpecification()
    {
        string message = Rejection([Specification("orleans-core", "map-async", 1), null!]);

        Assert.Contains("specifications[1] is null", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADuplicateStageReference()
    {
        string message = Rejection(
            [Specification("orleans-core", "map-async", 2), Specification("orleans-core", "map-async", 2)]);

        Assert.Contains(
            "specifications[1] repeats the stage reference 'orleans-core/map-async@v2'",
            message,
            StringComparison.Ordinal);
        Assert.Contains("The stage catalog breaks 1 invariant:", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsTwoSpecificationsThatDifferOnlyInTheirPorts()
    {
        // Same reference, different declared shape: the catalog cannot choose between them, and picking
        // one by registration order would make what a document runs depend on startup order.
        StageSpecification first = StageSpecification.Create(
            Stage("orleans-core", "map-async", 2),
            [],
            [],
            [],
            SampleParameterContract,
            []);

        StageSpecification second = StageSpecification.Create(
            Stage("orleans-core", "map-async", 2),
            [InputPortSpecification.Create(PortId.Create("in"), SampleParameterContract)],
            [],
            [],
            SampleParameterContract,
            []);

        Assert.Contains(
            "specifications[1] repeats the stage reference 'orleans-core/map-async@v2'",
            Rejection([first, second]),
            StringComparison.Ordinal);
    }

    [Fact]
    public void CreateReportsEveryViolationAtOnce()
    {
        string message = Rejection(
            [
                Specification("orleans-core", "map-async", 2),
                null!,
                Specification("orleans-core", "map-async", 2),
            ]);

        Assert.Contains("The stage catalog breaks 2 invariants:", message, StringComparison.Ordinal);
        Assert.Contains("1. specifications[1] is null", message, StringComparison.Ordinal);
        Assert.Contains(
            "2. specifications[2] repeats the stage reference 'orleans-core/map-async@v2'",
            message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void SpecificationsAreReadOnlyAndNotTheUnderlyingArray()
    {
        StageCatalog catalog = StageCatalog.Create([Specification("orleans-core", "map-async", 1)]);

        Assert.IsNotType<StageSpecification[]>(catalog.Specifications);

        IList<StageSpecification> specifications =
            Assert.IsAssignableFrom<IList<StageSpecification>>(catalog.Specifications);

        Assert.True(specifications.IsReadOnly);
        Assert.Throws<NotSupportedException>(
            () => specifications.Add(Specification("contoso-sinks", "to-table", 1)));
    }

    [Fact]
    public void CreateCopiesTheSequenceOnce()
    {
        List<StageSpecification> specifications = [Specification("orleans-core", "map-async", 1)];
        StageCatalog catalog = StageCatalog.Create(specifications);

        specifications.Add(Specification("contoso-sinks", "to-table", 1));

        Assert.Single(catalog.Specifications);
        Assert.False(catalog.TryGetSpecification(Stage("contoso-sinks", "to-table", 1), out _));
    }

    /// <summary>
    /// Asserts that a catalog answers correctly when it is reached through the interface the graph
    /// compiler depends on.
    /// </summary>
    /// <typeparam name="TCatalog">The catalog implementation, reached only through the interface.</typeparam>
    /// <param name="catalog">The catalog under test.</param>
    private static void AssertResolvesThroughTheInterface<TCatalog>(TCatalog catalog)
        where TCatalog : IStageCatalog
    {
        Assert.True(catalog.TryGetSpecification(Stage("orleans-core", "map-async", 2), out StageSpecification? found));
        Assert.NotNull(found);
        Assert.Single(catalog.Specifications);
        Assert.False(catalog.TryGetSpecification(Stage("orleans-core", "map-async", 3), out _));
    }

    /// <summary>Builds a stage reference from its three components.</summary>
    /// <param name="provider">The provider identifier text.</param>
    /// <param name="stage">The stage identifier text.</param>
    /// <param name="majorVersion">The compatibility major version.</param>
    /// <returns>The stage reference.</returns>
    private static StageRef Stage(string provider, string stage, int majorVersion) =>
        StageRef.Create(ProviderId.Create(provider), StageId.Create(stage), majorVersion);

    /// <summary>Builds a portless specification for one stage reference.</summary>
    /// <param name="provider">The provider identifier text.</param>
    /// <param name="stage">The stage identifier text.</param>
    /// <param name="majorVersion">The compatibility major version.</param>
    /// <returns>The specification.</returns>
    private static StageSpecification Specification(string provider, string stage, int majorVersion) =>
        StageSpecification.Create(
            Stage(provider, stage, majorVersion),
            [],
            [],
            [],
            SampleParameterContract,
            []);

    /// <summary>
    /// Asserts that a candidate catalog is rejected and returns the rejection message.
    /// </summary>
    /// <param name="specifications">The candidate specifications.</param>
    /// <returns>The message of the thrown <see cref="ArgumentException"/>.</returns>
    private static string Rejection(IEnumerable<StageSpecification> specifications)
    {
        ArgumentException exception = Assert.ThrowsAny<ArgumentException>(
            () => { _ = StageCatalog.Create(specifications); });

        Assert.IsType<ArgumentException>(exception);
        Assert.Equal("specifications", exception.ParamName);

        return exception.Message;
    }
}
