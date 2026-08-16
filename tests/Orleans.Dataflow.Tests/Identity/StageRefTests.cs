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
}
