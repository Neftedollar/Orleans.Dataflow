using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Tests.Identity;
using Xunit;

namespace Orleans.Dataflow.Tests.Definition;

/// <summary>
/// Runs the shared single-segment identifier contract against <see cref="CapabilityToken"/> and adds the
/// rules specific to the declared capability vocabulary.
/// </summary>
public sealed class CapabilityTokenTests : SimpleIdentifierContractTests<CapabilityToken>
{
    /// <inheritdoc/>
    protected override string IdentifierName => nameof(CapabilityToken);

    [Fact]
    public void NondeployableIsTheExpectedToken()
    {
        Assert.Equal("nondeployable", CapabilityToken.Nondeployable.Value);
        Assert.Equal("nondeployable", CapabilityToken.Nondeployable.ToString());
        Assert.False(CapabilityToken.Nondeployable.IsDefault);
    }

    [Fact]
    public void NondeployableEqualsTheCreatedToken()
    {
        CapabilityToken created = CapabilityToken.Create("nondeployable");

        Assert.Equal(created, CapabilityToken.Nondeployable);
        Assert.Equal(created.GetHashCode(), CapabilityToken.Nondeployable.GetHashCode());
        Assert.True(created == CapabilityToken.Nondeployable);
    }

    [Fact]
    public void NondeployableIsStableAcrossReads()
    {
        Assert.Equal(CapabilityToken.Nondeployable, CapabilityToken.Nondeployable);
    }

    [Fact]
    public void CreateAcceptsATokenThisVersionDoesNotKnow()
    {
        Assert.Equal("some-future-capability", CapabilityToken.Create("some-future-capability").Value);
    }

    /// <inheritdoc/>
    protected override CapabilityToken Create(string value) => CapabilityToken.Create(value);

    /// <inheritdoc/>
    protected override bool TryCreate(string? value, out CapabilityToken identifier) =>
        CapabilityToken.TryCreate(value, out identifier);

    /// <inheritdoc/>
    protected override string ValueOf(CapabilityToken identifier) => identifier.Value;

    /// <inheritdoc/>
    protected override bool IsDefaultOf(CapabilityToken identifier) => identifier.IsDefault;

    /// <inheritdoc/>
    protected override bool OperatorEquals(CapabilityToken left, CapabilityToken right) => left == right;

    /// <inheritdoc/>
    protected override bool OperatorNotEquals(CapabilityToken left, CapabilityToken right) => left != right;
}
