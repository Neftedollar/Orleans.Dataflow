using Orleans.Dataflow.ApiSurface;
using Xunit;

namespace Orleans.Dataflow.Tests.ApiSurface;

/// <summary>
/// Pins the whole public shape of the four packages this suite can see, as text under source control.
/// </summary>
/// <remarks>
/// <para>
/// A companion to the <c>PublicAPI</c> analyzer rather than a duplicate of it. The analyzer records a
/// member's signature and nothing around it, so a generic parameter gaining or losing variance, a base type
/// or an implemented interface changing, and an attribute appearing, vanishing, or changing its arguments
/// all pass it silently. Each of those is a change a consumer can be broken by, and the baselines beside
/// this file are what make them show up in a diff somebody reads.
/// </para>
/// <para>
/// <b>The F# frontend has no analyzer at all</b>, so its baseline is the only record of its surface that
/// exists. It is referenced by this project for exactly that: nothing here calls into it, and the reference
/// is what puts the assembly and <c>FSharp.Core</c> in this project's output where the reader can find them.
/// </para>
/// <para>
/// A failing test here is not automatically a bug — new surface is a normal thing to add — but it is always
/// a decision. The failure message classifies the diff and names the deliberate way to regenerate a
/// baseline; nothing here ever rewrites one on its own.
/// </para>
/// </remarks>
public sealed class PublicSurfaceTests
{
    [Fact]
    public void TheAbstractionsSurfaceMatchesItsBaseline() =>
        PublicSurfaceDump.AssertMatchesBaseline(
            "tests/Orleans.Dataflow.Tests/ApiSurface/Orleans.Dataflow.Abstractions.surface.txt",
            typeof(global::Orleans.Dataflow.Definition.GraphDocument).Assembly);

    [Fact]
    public void TheCoreSurfaceMatchesItsBaseline() =>
        PublicSurfaceDump.AssertMatchesBaseline(
            "tests/Orleans.Dataflow.Tests/ApiSurface/Orleans.Dataflow.surface.txt",
            typeof(global::Orleans.Dataflow.RunHandle).Assembly);

    [Fact]
    public void TheTestingSurfaceMatchesItsBaseline() =>
        PublicSurfaceDump.AssertMatchesBaseline(
            "tests/Orleans.Dataflow.Tests/ApiSurface/Orleans.Dataflow.Testing.surface.txt",
            typeof(global::Orleans.Dataflow.Testing.TestFlow).Assembly);

    [Fact]
    public void TheFSharpSurfaceMatchesItsBaseline() =>
        PublicSurfaceDump.AssertMatchesBaseline(
            "tests/Orleans.Dataflow.Tests/ApiSurface/Orleans.Dataflow.FSharp.surface.txt",
            typeof(global::Orleans.Dataflow.FSharp.Pipeline).Assembly);
}
