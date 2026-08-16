using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Xunit;

namespace Orleans.Dataflow.Tests.Definition;

/// <summary>
/// Tests for <see cref="PortAddress"/>.
/// </summary>
public sealed class PortAddressTests
{
    private static readonly NodeId SampleNode = NodeId.Create("reader");
    private static readonly PortId SamplePort = PortId.Create("out");

    [Fact]
    public void CreateRoundTripsComponents()
    {
        PortAddress address = PortAddress.Create(SampleNode, SamplePort);

        Assert.Equal(SampleNode, address.Node);
        Assert.Equal(SamplePort, address.Port);
        Assert.False(address.IsDefault);
    }

    [Theory]
    [InlineData("reader", "out", "reader#out")]
    [InlineData("orders/reader", "in", "orders/reader#in")]
    [InlineData("a-b", "result-1", "a-b#result-1")]
    public void ToStringUsesCanonicalFormat(string node, string port, string expected)
    {
        Assert.Equal(expected, PortAddress.Create(NodeId.Parse(node), PortId.Create(port)).ToString());
    }

    [Fact]
    public void CreateRejectsDefaultNode()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "node",
            () => { _ = PortAddress.Create(default, SamplePort); });

        Assert.Contains(nameof(NodeId), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsDefaultPort()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "port",
            () => { _ = PortAddress.Create(SampleNode, default); });

        Assert.Contains(nameof(PortId), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void EqualAddressesAreEqualAndShareHashCode()
    {
        PortAddress left = PortAddress.Create(NodeId.Parse("orders/reader"), PortId.Create("out"));
        PortAddress right = PortAddress.Create(NodeId.Parse("orders/reader"), PortId.Create("out"));

        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.True(left == right);
        Assert.False(left != right);
    }

    [Fact]
    public void AddressesDifferingInAnyComponentAreNotEqual()
    {
        PortAddress address = PortAddress.Create(SampleNode, SamplePort);

        Assert.NotEqual(address, PortAddress.Create(NodeId.Create("writer"), SamplePort));
        Assert.NotEqual(address, PortAddress.Create(SampleNode, PortId.Create("in")));
        Assert.True(address != PortAddress.Create(SampleNode, PortId.Create("in")));
    }

    [Fact]
    public void AddressesAreUsableAsDictionaryKeys()
    {
        Dictionary<PortAddress, int> counts = new()
        {
            [PortAddress.Create(SampleNode, SamplePort)] = 1,
        };

        Assert.True(counts.ContainsKey(PortAddress.Create(NodeId.Create("reader"), PortId.Create("out"))));
        Assert.False(counts.ContainsKey(PortAddress.Create(NodeId.Create("reader"), PortId.Create("in"))));
    }

    [Fact]
    public void DefaultInstanceIsDefault()
    {
        Assert.True(default(PortAddress).IsDefault);
        Assert.Equal(default, default(PortAddress));
        Assert.NotEqual(default, PortAddress.Create(SampleNode, SamplePort));
    }

    [Fact]
    public void DefaultInstanceComponentAccessThrowsInvalidOperationException()
    {
        PortAddress address = default;

        Assert.Throws<InvalidOperationException>(() => { _ = address.Node; });

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(() => { _ = address.Port; });

        Assert.Contains(nameof(PortAddress), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DefaultInstanceToStringIsDiagnosticLiteralAndDoesNotThrow()
    {
        Assert.Equal("(default PortAddress)", default(PortAddress).ToString());
    }
}
