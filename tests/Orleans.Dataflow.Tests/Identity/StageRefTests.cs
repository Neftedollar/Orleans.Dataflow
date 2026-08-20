using Orleans.Dataflow.Identity;
using Xunit;

namespace Orleans.Dataflow.Tests.Identity;

/// <summary>
/// Tests for <see cref="StageRef"/>.
/// </summary>
public sealed class StageRefTests
{
    private static readonly ProviderId SampleProvider = ProviderId.Create("orleans-core");
    private static readonly StageId SampleStage = StageId.Create("map-async");

    [Fact]
    public void CreateRoundTripsComponents()
    {
        StageRef stageRef = StageRef.Create(SampleProvider, SampleStage, 3);

        Assert.Equal(SampleProvider, stageRef.Provider);
        Assert.Equal(SampleStage, stageRef.Stage);
        Assert.Equal(3, stageRef.MajorVersion);
        Assert.False(stageRef.IsDefault);
    }

    [Fact]
    public void ForBuildsTheSameReferenceAsCreateFromTheTextOfItsIdentifiers()
    {
        // The short spelling is the long one with the identifier types written for the author, so what it
        // produces has to be indistinguishable from what they would have produced by hand.
        Assert.Equal(StageRef.Create(SampleProvider, SampleStage, 3), StageRef.For("orleans-core", "map-async", 3));
        Assert.Equal(
            StageRef.Create(SampleProvider, SampleStage, StageRef.FirstMajorVersion),
            StageRef.For("orleans-core", "map-async"));
    }

    [Fact]
    public void ForDefaultsToTheFirstMajorVersion() =>
        Assert.Equal(StageRef.FirstMajorVersion, StageRef.For("orleans-core", "map-async").MajorVersion);

    [Fact]
    public void ForKeepsEveryCheckTheIdentifierTypesMake()
    {
        // What the short spelling drops is the obligation to name the types, never the validation they
        // carry: the segment grammar, the version bound, and the null checks all still refuse, and each
        // refusal names the argument the author actually wrote.
        Assert.Contains(
            "segment",
            Assert.Throws<ArgumentException>("provider", () => StageRef.For("Orleans Core", "map-async")).Message,
            StringComparison.Ordinal);
        Assert.Contains(
            "segment",
            Assert.Throws<ArgumentException>("stage", () => StageRef.For("orleans-core", "Map Async")).Message,
            StringComparison.Ordinal);
        Assert.Throws<ArgumentOutOfRangeException>(
            "majorVersion",
            () => StageRef.For("orleans-core", "map-async", 0));
        Assert.Throws<ArgumentNullException>("provider", () => StageRef.For(null!, "map-async"));
        Assert.Throws<ArgumentNullException>("stage", () => StageRef.For("orleans-core", null!));
    }

    [Fact]
    public void ToStringUsesCanonicalFormat()
    {
        Assert.Equal("orleans-core/map-async@v3", StageRef.Create(SampleProvider, SampleStage, 3).ToString());
        Assert.Equal("orleans-core/map-async@v1", StageRef.Create(SampleProvider, SampleStage, 1).ToString());
    }

    [Fact]
    public void FirstMajorVersionIsOne()
    {
        Assert.Equal(1, StageRef.FirstMajorVersion);
        Assert.Equal(1, StageRef.Create(SampleProvider, SampleStage, StageRef.FirstMajorVersion).MajorVersion);
    }

    [Fact]
    public void CreateAcceptsMaximumMajorVersion()
    {
        Assert.Equal(int.MaxValue, StageRef.Create(SampleProvider, SampleStage, int.MaxValue).MajorVersion);
    }

    [Fact]
    public void CreateRejectsDefaultProvider()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "provider",
            () => { _ = StageRef.Create(default, SampleStage, 1); });

        Assert.Contains(nameof(ProviderId), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsDefaultStage()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "stage",
            () => { _ = StageRef.Create(SampleProvider, default, 1); });

        Assert.Contains(nameof(StageId), exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(int.MinValue)]
    public void CreateRejectsNonPositiveMajorVersion(int version)
    {
        ArgumentException exception =
            Assert.ThrowsAny<ArgumentException>(() => { _ = StageRef.Create(SampleProvider, SampleStage, version); });

        Assert.IsType<ArgumentOutOfRangeException>(exception);
        Assert.Equal("majorVersion", exception.ParamName);
    }

    [Fact]
    public void TryCreateAcceptsValidComponents()
    {
        Assert.True(StageRef.TryCreate(SampleProvider, SampleStage, 2, out StageRef stageRef));
        Assert.Equal("orleans-core/map-async@v2", stageRef.ToString());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void TryCreateRejectsNonPositiveMajorVersionWithoutThrowing(int version)
    {
        Assert.False(StageRef.TryCreate(SampleProvider, SampleStage, version, out StageRef stageRef));
        Assert.True(stageRef.IsDefault);
    }

    [Fact]
    public void TryCreateRejectsDefaultComponentsWithoutThrowing()
    {
        Assert.False(StageRef.TryCreate(default, SampleStage, 1, out StageRef withoutProvider));
        Assert.True(withoutProvider.IsDefault);

        Assert.False(StageRef.TryCreate(SampleProvider, default, 1, out StageRef withoutStage));
        Assert.True(withoutStage.IsDefault);
    }

    [Fact]
    public void EqualReferencesAreEqualAndShareHashCode()
    {
        StageRef left = StageRef.Create(ProviderId.Create("orleans-core"), StageId.Create("map-async"), 3);
        StageRef right = StageRef.Create(ProviderId.Create("orleans-core"), StageId.Create("map-async"), 3);

        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.True(left == right);
        Assert.False(left != right);
    }

    [Fact]
    public void ReferencesDifferingInAnyComponentAreNotEqual()
    {
        StageRef reference = StageRef.Create(SampleProvider, SampleStage, 3);

        Assert.NotEqual(reference, StageRef.Create(ProviderId.Create("other-provider"), SampleStage, 3));
        Assert.NotEqual(reference, StageRef.Create(SampleProvider, StageId.Create("other-stage"), 3));
        Assert.NotEqual(reference, StageRef.Create(SampleProvider, SampleStage, 4));
        Assert.True(reference != StageRef.Create(SampleProvider, SampleStage, 4));
    }

    [Fact]
    public void DefaultInstanceIsDefault()
    {
        Assert.True(default(StageRef).IsDefault);
        Assert.Equal(default, default(StageRef));
        Assert.NotEqual(default, StageRef.Create(SampleProvider, SampleStage, 1));
    }

    [Fact]
    public void DefaultInstanceComponentAccessThrowsInvalidOperationException()
    {
        StageRef stageRef = default;

        Assert.Throws<InvalidOperationException>(() => { _ = stageRef.Provider; });
        Assert.Throws<InvalidOperationException>(() => { _ = stageRef.Stage; });

        InvalidOperationException exception =
            Assert.Throws<InvalidOperationException>(() => { _ = stageRef.MajorVersion; });

        Assert.Contains(nameof(StageRef), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DefaultInstanceToStringIsDiagnosticLiteralAndDoesNotThrow()
    {
        Assert.Equal("(default StageRef)", default(StageRef).ToString());
    }

    [Fact]
    public void DefaultInstanceIsUsableInAHashSet()
    {
        HashSet<StageRef> references = [default, StageRef.Create(SampleProvider, SampleStage, 1), default];

        Assert.Equal(2, references.Count);
    }

    [Fact]
    public void ComparisonRunsProviderThenStageThenMajorVersion()
    {
        // The ordering table, in the reading order of 'provider/stage@v1': the provider decides first, the
        // stage next, and only then the version — numerically, so v2 precedes v10.
        StageRef[] ordered =
        [
            StageRef.Create(ProviderId.Create("alpha"), StageId.Create("zulu"), 9),
            StageRef.Create(ProviderId.Create("beta"), StageId.Create("alpha"), 1),
            StageRef.Create(ProviderId.Create("beta"), StageId.Create("map"), 1),
            StageRef.Create(ProviderId.Create("beta"), StageId.Create("map"), 2),
            StageRef.Create(ProviderId.Create("beta"), StageId.Create("map"), 10),
            StageRef.Create(ProviderId.Create("beta"), StageId.Create("map-async"), 1),
        ];

        for (int index = 1; index < ordered.Length; index++)
        {
            Assert.True(
                ordered[index - 1].CompareTo(ordered[index]) < 0,
                $"'{ordered[index - 1]}' should sort before '{ordered[index]}'");

            Assert.True(ordered[index].CompareTo(ordered[index - 1]) > 0);
            Assert.True(ordered[index - 1] < ordered[index]);
            Assert.True(ordered[index - 1] <= ordered[index]);
            Assert.True(ordered[index] > ordered[index - 1]);
            Assert.True(ordered[index] >= ordered[index - 1]);
        }
    }

    [Fact]
    public void SortingUsesTheSameOrderWhicheverWayTheInputArrived()
    {
        StageRef[] shuffled =
        [
            StageRef.Create(SampleProvider, SampleStage, 10),
            StageRef.Create(ProviderId.Create("contoso-sinks"), SampleStage, 1),
            StageRef.Create(SampleProvider, SampleStage, 2),
            StageRef.Create(SampleProvider, StageId.Create("from-source"), 1),
        ];

        Array.Sort(shuffled);

        Assert.Equal(
            [
                "contoso-sinks/map-async@v1",
                "orleans-core/from-source@v1",
                "orleans-core/map-async@v2",
                "orleans-core/map-async@v10",
            ],
            shuffled.Select(reference => reference.ToString()));
    }

    [Fact]
    public void TheDefaultInstanceSortsBeforeEveryCreatedOne()
    {
        StageRef created = StageRef.Create(SampleProvider, SampleStage, 1);

        Assert.True(default(StageRef).CompareTo(created) < 0);
        Assert.True(created.CompareTo(default) > 0);
        Assert.Equal(0, default(StageRef).CompareTo(default));
        Assert.True(default(StageRef) < created);
        Assert.True(created >= default(StageRef));
    }

    [Fact]
    public void ComparisonIsConsistentWithEquality()
    {
        StageRef left = StageRef.Create(SampleProvider, SampleStage, 3);
        StageRef right = StageRef.Create(SampleProvider, SampleStage, 3);

        Assert.Equal(0, left.CompareTo(right));
        Assert.Equal(left, right);
        Assert.True(left <= right);
        Assert.True(left >= right);
        Assert.False(left < right);
        Assert.False(left > right);
    }

    [Fact]
    public void TheNonGenericComparisonAgreesWithTheTypedOne()
    {
        // F#'s 'comparison' constraint is satisfied by System.IComparable and not by IComparable<'T>, so
        // this implementation is what lets the type key an F# Set or Map.
        IComparable left = StageRef.Create(SampleProvider, SampleStage, 2);
        StageRef right = StageRef.Create(SampleProvider, SampleStage, 10);

        Assert.True(typeof(IComparable).IsAssignableFrom(typeof(StageRef)));
        Assert.Equal(((StageRef)left).CompareTo(right), left.CompareTo(right));
        Assert.True(left.CompareTo(null) > 0);
        Assert.Throws<ArgumentException>("obj", () => left.CompareTo("not a StageRef"));
    }
}
