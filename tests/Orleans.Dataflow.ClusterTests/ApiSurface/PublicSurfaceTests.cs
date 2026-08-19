using Orleans.Dataflow.ApiSurface;
using Orleans.Dataflow.Hosting;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.ApiSurface;

/// <summary>
/// Pins the whole public shape of the Orleans hosting package, as text under source control.
/// </summary>
/// <remarks>
/// <para>
/// The same guard the core suite runs over the other four packages, and it lives here because this is the
/// only project that references this one. What it adds over the <c>PublicAPI</c> analyzer matters most in
/// this package: the wire types carry <c>[Id(n)]</c> on every member, and renumbering one is a
/// compatibility break that no round-trip test can catch — a test that serializes and deserializes with the
/// same numbering agrees with itself no matter which numbers it uses. The baseline records the numbers.
/// </para>
/// <para>
/// <b>The generated <c>OrleansCodeGen</c> namespace is deliberately left out.</b> That surface is the
/// Orleans SDK's own contract with itself, emitted by the code generator and version-coupled to it: it
/// changes when the SDK is upgraded, for reasons that are the SDK's rather than this library's, and
/// freezing it here would make a package bump fail a test that has nothing to say about the bump. What this
/// library owns is the types it declares, and those are what the baseline covers.
/// </para>
/// </remarks>
public sealed class PublicSurfaceTests
{
    [Fact]
    public void TheOrleansHostingSurfaceMatchesItsBaseline() =>
        PublicSurfaceDump.AssertMatchesBaseline(
            "tests/Orleans.Dataflow.ClusterTests/ApiSurface/Orleans.Dataflow.Cluster.surface.txt",
            typeof(OrleansRunHandle).Assembly,
            excludedNamespace: "OrleansCodeGen");
}
