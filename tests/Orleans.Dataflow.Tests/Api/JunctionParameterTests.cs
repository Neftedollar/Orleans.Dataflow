using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;
using static Orleans.Dataflow.Tests.Api.RegisteredJunctionFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What a typed parameter builder is and what it is not, on the fixture provider that has one.
/// </summary>
/// <remarks>
/// The M1 design left a promissory note — "typed parameter builders are provider-SDK sugar (M4); the M1
/// surface is honest raw payloads" — and this is the note being kept rather than replaced. The builder
/// writes the very bytes the raw payload had, so nothing about the document, the fingerprint, or the reader
/// changes; what changes is that the member name is spelled once and the set of values is the C# compiler's
/// to enforce.
/// </remarks>
public sealed class JunctionParameterTests
{
    [Fact]
    public void TheTypedBuilderWritesTheBytesTheRawPayloadHad()
    {
        // The claim that makes the whole pattern safe to adopt: sugar over the payload, not a second
        // format. A provider that switches to builders changes no document and no fingerprint.
        Assert.Equal("""{"mode":"broadcast"}""", SplitParameters(SplitMode.Broadcast).ToString());
        Assert.Equal("""{"mode":"balance"}""", SplitParameters(SplitMode.Balance).ToString());
        Assert.Equal("""{"mode":"concat"}""", JoinParameters(JoinMode.Concat).ToString());
    }

    [Fact]
    public void TheBuildersPayloadIsWhatTheDocumentStores()
    {
        RunnableGraph graph = RegisteredFanOut(out ResultSlot<long> _, out ResultSlot<long> _, BalanceParameters);

        Assert.Equal(
            SplitParameters(SplitMode.Balance),
            graph.Document.Nodes.Single(node => node.Id.Value is "split").Parameters);
    }

    [Fact]
    public void AModeThisVocabularyDoesNotHaveIsRefusedByTheGraphCompiler()
    {
        // What the builder cannot do and the validator must: a document not written through the builder —
        // hand-authored, from another version, from another provider — reaches the compiler, and the reader
        // the builder shares with it is what refuses the mode by name.
        RunnableGraph graph = RegisteredFanOut(
            out ResultSlot<long> _,
            out ResultSlot<long> _,
            CanonicalJsonValue.Parse("""{"mode":"round-robin"}"""));

        GraphValidationReport report = GraphCompiler.Validate(graph.Document, Catalog);

        GraphValidationDiagnostic refused = Assert.Single(report.Diagnostics);

        Assert.Equal("invalid-parameters", refused.Rule);
        Assert.Contains("'round-robin'", refused.Message, StringComparison.Ordinal);
        Assert.Contains("'broadcast', 'balance', 'halves'", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AMemberThisVocabularyDoesNotDeclareIsRefusedByTheGraphCompiler()
    {
        // The rule the conformance kit checks of every provider, kept here by the reader the builder is the
        // other half of: a payload carrying a member this stage never heard of is either written for a
        // different stage or written against a version this one is not.
        RunnableGraph graph = RegisteredFanIn(
            out ResultSlot<long> _,
            CanonicalJsonValue.Parse("""{"mode":"merge","preferred":"primary"}"""));

        GraphValidationReport report = GraphCompiler.Validate(graph.Document, Catalog);

        GraphValidationDiagnostic refused = Assert.Single(report.Diagnostics);

        Assert.Equal("invalid-parameters", refused.Rule);
        Assert.Contains("'preferred' is not one this stage declares", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AJunctionPayloadWithNoModeAtAllIsRefusedByTheGraphCompiler()
    {
        RunnableGraph graph = RegisteredFanOut(
            out ResultSlot<long> _,
            out ResultSlot<long> _,
            CanonicalJsonValue.Parse("{}"));

        GraphValidationReport report = GraphCompiler.Validate(graph.Document, Catalog);

        GraphValidationDiagnostic refused = Assert.Single(report.Diagnostics);

        Assert.Equal("invalid-parameters", refused.Rule);
        Assert.Contains("the member 'mode' is missing", refused.Message, StringComparison.Ordinal);
    }
}
