using System.Globalization;
using Orleans.Dataflow.Identity;
using Xunit;

namespace Orleans.Dataflow.Tests.Identity;

/// <summary>
/// Cross-cutting tests for the shared identifier grammar.
/// </summary>
public sealed class IdentifierGrammarTests
{
    [Fact]
    public void SegmentValidationIsIndependentOfAmbientCulture()
    {
        RunInCulture("tr-TR", () =>
        {
            // Turkish casing maps 'I' and 'i' differently from the invariant culture, so a validator
            // written with culture-sensitive casing would accept or reject these inconsistently.
            Assert.Equal("i", ProviderId.Create("i").Value);
            Assert.Equal("iso-8601", ContractId.Create("iso-8601").Value);
            Assert.False(ProviderId.TryCreate("I", out _));
            Assert.False(ProviderId.TryCreate("İ", out _));
            Assert.False(ProviderId.TryCreate("ı", out _));
            Assert.True(NodeId.TryParse("orders/import-i", out _));
            Assert.False(NodeId.TryParse("orders/İmport", out _));
        });
    }

    [Fact]
    public void IdentifierTextIsFormattedIndependentlyOfAmbientCulture()
    {
        StageRef stageRef = StageRef.Create(ProviderId.Create("orleans-core"), StageId.Create("map-async"), 1234567);

        RunInCulture("de-DE", () =>
        {
            Assert.Equal("orleans-core/map-async@v1234567", stageRef.ToString());
            Assert.Equal("1234567", GraphRevision.Create(1234567).ToString());
        });

        RunInCulture("ar-SA", () =>
        {
            Assert.Equal("orleans-core/map-async@v1234567", stageRef.ToString());
            Assert.Equal("1234567", GraphRevision.Create(1234567).ToString());
        });
    }

    [Fact]
    public void EveryIdentifierTypeAcceptsTheSameSegmentGrammar()
    {
        const string Segment = "shared-segment-1";

        Assert.Equal(Segment, ProviderId.Create(Segment).Value);
        Assert.Equal(Segment, StageId.Create(Segment).Value);
        Assert.Equal(Segment, GraphId.Create(Segment).Value);
        Assert.Equal(Segment, PortId.Create(Segment).Value);
        Assert.Equal(Segment, ResultSlotId.Create(Segment).Value);
        Assert.Equal(Segment, ContractId.Create(Segment).Value);
        Assert.Equal(Segment, RunId.Create(Segment).Value);
        Assert.Equal(Segment, NodeId.Create(Segment).Value);
    }

    [Fact]
    public void EveryIdentifierTypeRejectsTheSameInvalidSegment()
    {
        const string Segment = "Shared_Segment";

        Assert.False(ProviderId.TryCreate(Segment, out _));
        Assert.False(StageId.TryCreate(Segment, out _));
        Assert.False(GraphId.TryCreate(Segment, out _));
        Assert.False(PortId.TryCreate(Segment, out _));
        Assert.False(ResultSlotId.TryCreate(Segment, out _));
        Assert.False(ContractId.TryCreate(Segment, out _));
        Assert.False(RunId.TryCreate(Segment, out _));
        Assert.False(NodeId.TryParse(Segment, out _));
    }

    [Fact]
    public void IdentifiersOfDifferentTypesDoNotShareEquality()
    {
        // The identifier types are distinct value types, so the same text in two different roles can
        // never compare equal by accident; this is the compile-time half of the identity table.
        ProviderId provider = ProviderId.Create("shared-text");
        StageId stage = StageId.Create("shared-text");

        Assert.Equal(provider.Value, stage.Value);
        Assert.False(provider.Equals((object)stage));
    }

    private static void RunInCulture(string cultureName, Action action)
    {
        CultureInfo originalCulture = CultureInfo.CurrentCulture;
        CultureInfo originalUICulture = CultureInfo.CurrentUICulture;

        try
        {
            CultureInfo culture = CultureInfo.GetCultureInfo(cultureName);
            CultureInfo.CurrentCulture = culture;
            CultureInfo.CurrentUICulture = culture;
            action();
        }
        finally
        {
            CultureInfo.CurrentCulture = originalCulture;
            CultureInfo.CurrentUICulture = originalUICulture;
        }
    }
}
