using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Orleans.Dataflow.Testing;
using Orleans.Dataflow.Tests.Api;
using Xunit;

namespace Orleans.Dataflow.Tests.Conformance;

/// <summary>
/// The .NET push-bridge vocabulary, run through the conformance kit the provider SDK publishes.
/// </summary>
/// <remarks>
/// <para>
/// The first of the kit's two shipped consumers, and the reason the kit exists in a package rather than in
/// a test file: what a provider outside this repository writes to check its own vocabulary is exactly what
/// is written here, and the only thing this suite has that an outside provider does not is friend access to
/// construct the factory without building a host around it.
/// </para>
/// <para>
/// The kit is pointed at the catalog and the factory a host really registers — <c>DotnetStages.Publish</c>
/// and <see cref="DotnetStageFactory"/> over one registry — rather than at a fixture standing in for them,
/// because a conformance suite checking a copy of the thing would pass while the thing drifted.
/// </para>
/// </remarks>
public sealed class DotnetVocabularyConformanceTests
{
    /// <summary>Gets the kit's checks as this theory's data.</summary>
    /// <value>One row per check, so a failure names the rule that stopped being true.</value>
    /// <remarks>
    /// The whole of what a provider author writes to run the kit, and it grows on its own: a check added to
    /// <see cref="ProviderConformance.Checks"/> becomes a test here without this file changing.
    /// </remarks>
    public static TheoryData<string> Checks => [.. ProviderConformance.Checks];

    [Theory]
    [MemberData(nameof(Checks))]
    public void TheDotnetVocabularyKeepsTheProviderContract(string check) => Kit().Check(check);

    [Fact]
    public void TheKitRefusesToMeasureAProviderTheCatalogDoesNotDeclare()
    {
        // The one way a conformance suite can lie, refused where it starts: a kit pointed at a catalog with
        // none of the provider's stages in it would pass every check while measuring nothing, and a green
        // suite that measured nothing reads exactly like a green suite that measured everything.
        ArgumentException refused = Assert.Throws<ArgumentException>(() => ProviderConformance.Create(
            ProviderId.Create("nobody"),
            DotnetStages.Catalog,
            new DotnetStageFactory(DotnetAdapterRegistry.Empty),
            []));

        Assert.Contains("declares no stage of the provider 'nobody'", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheKitRefusesAStageWithNoSampleAndASampleWithNoStage()
    {
        // The other two ways it could measure less than it claims. A declared stage with no sample would be
        // skipped in silence; a sample naming a stage the catalog does not declare is a registration the
        // author believes they wrote and did not.
        ArgumentException refused = Assert.Throws<ArgumentException>(() => ProviderConformance.Create(
            DotnetStages.Provider,
            DotnetStages.Catalog,
            new DotnetStageFactory(DotnetAdapterRegistry.Empty),
            [ProviderStageSample.Create(RegisteredFixtures.Stage("order-source"), Blank)]));

        Assert.Contains("dotnet/observable@v1", refused.Message, StringComparison.Ordinal);
        Assert.Contains("dotnet/timer@v1", refused.Message, StringComparison.Ordinal);
        Assert.Contains("which this catalog does not declare", refused.Message, StringComparison.Ordinal);
    }

    /// <summary>Gets a payload with nothing in it, for the samples that are refused before they are read.</summary>
    private static CanonicalJsonValue Blank => CanonicalJsonValue.Parse("{}");

    /// <summary>Points the kit at the .NET vocabulary as a host registers it.</summary>
    /// <returns>The kit.</returns>
    /// <remarks>
    /// The observable binding is registered in the registry the factory is constructed with and named by the
    /// sample payload, because half the payload checks are about a name the deployment published: a sample
    /// the provider's own reader rejects is a broken sample, and the kit says so rather than passing.
    /// </remarks>
    private static ProviderConformance Kit()
    {
        ObservableBinding<string> notes = ObservableBinding.Create(
            "notes",
            ElementContract.For<string>("dotnet-note", 1),
            static () => new NoObservable());

        DotnetAdapterRegistry.Builder registrations = new();

        registrations.Add((IObservableEntry)notes);

        DotnetAdapterRegistry registry = registrations.Build();

        return ProviderConformance.Create(
            DotnetStages.Provider,
            DotnetStages.Publish(registry),
            new DotnetStageFactory(registry),
            [
                ProviderStageSample.Create(
                    DotnetStages.TimerStage,
                    DotnetStages.TimerParameters(TimeSpan.FromMilliseconds(25), tickLimit: 3)),
                ProviderStageSample.Create(
                    DotnetStages.ObservableStage,
                    DotnetStages.ObservableParameters(notes, new BufferOptions { Capacity = 4 })),
            ]);
    }

    /// <summary>An observable nothing ever pushes at, because nothing here runs a graph.</summary>
    /// <remarks>
    /// The kit builds a stage runtime and never opens it, so what the binding opens is irrelevant and
    /// saying so with the emptiest possible sequence is more honest than reusing a fixture that suggests
    /// otherwise.
    /// </remarks>
    private sealed class NoObservable : IObservable<string>
    {
        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<string> observer) => new Subscription();

        /// <summary>A subscription with nothing to release.</summary>
        private sealed class Subscription : IDisposable
        {
            /// <inheritdoc/>
            public void Dispose()
            {
            }
        }
    }
}
