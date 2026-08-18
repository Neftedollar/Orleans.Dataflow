using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the checkpoint document promises: ADR 0007's five parts, written by one writer, read by one reader
/// that refuses what it does not declare, and the same bytes every time.
/// </summary>
/// <remarks>
/// <para>
/// The payload discipline every stage of the local vocabulary follows, applied to the one document that is
/// not a stage's. The golden assertion is the load-bearing one: a checkpoint is bytes another process reads,
/// so a writer whose output depended on dictionary order would make two captures of one run state two
/// different documents, and nothing downstream could tell that apart from a run that had moved.
/// </para>
/// <para>
/// Nothing here runs a graph. What a cursor, a state, or a mark <em>means</em> is the seam's business and is
/// asserted where those seams live; what is asserted here is that the document carries them faithfully and
/// refuses everything else.
/// </para>
/// </remarks>
public sealed class CheckpointDocumentTests
{
    private static readonly GraphFingerprint Fingerprint =
        GraphFingerprint.OfSerialized("checkpoint-document-tests"u8);

    private static readonly GraphRevision Revision = GraphRevision.Create(1);

    /// <summary>An empty table, for the parts a test is not making a claim about.</summary>
    private static Dictionary<NodeId, CanonicalJsonValue> None => [];

    [Fact]
    public void ACheckpointCarriesTheFiveParts()
    {
        CanonicalJsonValue document = LocalCheckpointDocument.Write(
            Fingerprint,
            Revision,
            Table("source", """{"index":7}"""),
            Table("scope", """{"stages":[{}]}"""),
            Table("sink", """{"committed":5}"""));

        Assert.True(LocalCheckpointDocument.TryRead(
            document,
            out LocalCheckpoint? read,
            out IReadOnlyList<string> violations));
        Assert.Empty(violations);
        Assert.Equal(Fingerprint, read!.Graph);
        Assert.Equal(Revision, read.Revision);
        Assert.Equal(CanonicalJsonValue.Parse("""{"index":7}"""), read.Cursors[NodeId.Create("source")]);
        Assert.Equal(CanonicalJsonValue.Parse("""{"stages":[{}]}"""), read.States[NodeId.Create("scope")]);
        Assert.Equal(CanonicalJsonValue.Parse("""{"committed":5}"""), read.Marks[NodeId.Create("sink")]);
    }

    [Fact]
    public void ACheckpointOfNothingIsThreeEmptyTablesRatherThanAbsentMembers()
    {
        CanonicalJsonValue document = LocalCheckpointDocument.Write(Fingerprint, Revision, None, None, None);

        // A shape that varied with what a run happened to have would make an absent 'marks' ambiguous
        // between "no sink marks" and "written by a version that had none", and guessing is what a strict
        // reader exists to avoid.
        Assert.Contains("""."cursors":{}""".AsSpan()[1..], document.ToString(), StringComparison.Ordinal);
        Assert.Contains("""."marks":{}""".AsSpan()[1..], document.ToString(), StringComparison.Ordinal);
        Assert.Contains("""."states":{}""".AsSpan()[1..], document.ToString(), StringComparison.Ordinal);
        Assert.True(LocalCheckpointDocument.TryRead(document, out LocalCheckpoint? read, out _));
        Assert.Empty(read!.Cursors);
        Assert.Empty(read.States);
        Assert.Empty(read.Marks);
    }

    [Fact]
    public void TwoWritesOfOneSnapshotProduceTheSameBytesWhateverOrderTheSeamsWereEnumeratedIn()
    {
        Dictionary<NodeId, CanonicalJsonValue> forwards = new()
        {
            [NodeId.Create("alpha")] = CanonicalJsonValue.Parse("""{"index":1}"""),
            [NodeId.Create("beta")] = CanonicalJsonValue.Parse("""{"index":2}"""),
            [NodeId.Create("gamma")] = CanonicalJsonValue.Parse("""{"index":3}"""),
        };
        Dictionary<NodeId, CanonicalJsonValue> backwards = new()
        {
            [NodeId.Create("gamma")] = CanonicalJsonValue.Parse("""{"index":3}"""),
            [NodeId.Create("beta")] = CanonicalJsonValue.Parse("""{"index":2}"""),
            [NodeId.Create("alpha")] = CanonicalJsonValue.Parse("""{"index":1}"""),
        };

        CanonicalJsonValue first = LocalCheckpointDocument.Write(Fingerprint, Revision, forwards, None, None);
        CanonicalJsonValue second = LocalCheckpointDocument.Write(Fingerprint, Revision, backwards, None, None);

        Assert.Equal(first.ToString(), second.ToString());
        Assert.Equal(first, second);
    }

    [Fact]
    public void TheGoldenDocumentIsExactlyTheseBytes()
    {
        CanonicalJsonValue document = LocalCheckpointDocument.Write(
            GraphFingerprint.Parse("sha256:" + new string('0', 64)),
            GraphRevision.Create(3),
            Table("stage-0001", """{"index":4}"""),
            Table("stage-0002", """{"stages":[{},{"remaining":2}]}"""),
            Table("stage-0003", """{"committed":4}"""));

        // Written out rather than derived, because a golden test that computed its own expectation would
        // pass for any writer at all. The members are in canonical key order and so is every table.
        Assert.Equal(
            """{"cursors":{"stage-0001":{"index":4}},"fingerprint":"sha256:0000000000000000000000000000000000000000000000000000000000000000","marks":{"stage-0003":{"committed":4}},"revision":3,"states":{"stage-0002":{"stages":[{},{"remaining":2}]}}}""",
            document.ToString());
    }

    [Theory]
    [InlineData("""{"cursors":{},"marks":{},"revision":1,"states":{}}""", "fingerprint")]
    [InlineData("""{"cursors":{},"fingerprint":"sha256:00","marks":{},"revision":1,"states":{}}""", "fingerprint")]
    [InlineData("""{"cursors":{},"fingerprint":"x","marks":{},"revision":1,"states":{}}""", "fingerprint")]
    [InlineData("""{"cursors":{},"fingerprint":"sha256:0000000000000000000000000000000000000000000000000000000000000000","marks":{},"states":{}}""", "revision")]
    [InlineData("""{"cursors":{},"fingerprint":"sha256:0000000000000000000000000000000000000000000000000000000000000000","marks":{},"revision":0,"states":{}}""", "revision")]
    [InlineData("""{"fingerprint":"sha256:0000000000000000000000000000000000000000000000000000000000000000","marks":{},"revision":1,"states":{}}""", "cursors")]
    [InlineData("""{"cursors":[],"fingerprint":"sha256:0000000000000000000000000000000000000000000000000000000000000000","marks":{},"revision":1,"states":{}}""", "cursors")]
    [InlineData("""{"cursors":{},"fingerprint":"sha256:0000000000000000000000000000000000000000000000000000000000000000","revision":1,"states":{}}""", "marks")]
    [InlineData("""{"cursors":{},"fingerprint":"sha256:0000000000000000000000000000000000000000000000000000000000000000","marks":{},"revision":1}""", "states")]
    public void AMemberThatIsMissingOrWrongIsNamedInTheReport(string json, string member)
    {
        Assert.False(LocalCheckpointDocument.TryRead(
            CanonicalJsonValue.Parse(json),
            out LocalCheckpoint? read,
            out IReadOnlyList<string> violations));
        Assert.Null(read);
        Assert.Contains(violations, violation => violation.Contains(member, StringComparison.Ordinal));
    }

    [Fact]
    public void AMemberTheDocumentDoesNotDeclareIsRefusedRatherThanIgnored()
    {
        CanonicalJsonValue document = LocalCheckpointDocument.Write(Fingerprint, Revision, None, None, None);
        string extended = document.ToString().Replace("{\"cursors\"", "{\"epoch\":4,\"cursors\"", StringComparison.Ordinal);

        Assert.False(LocalCheckpointDocument.TryRead(
            CanonicalJsonValue.Parse(extended),
            out LocalCheckpoint? _,
            out IReadOnlyList<string> violations));
        Assert.Contains(violations, violation => violation.Contains("epoch", StringComparison.Ordinal));
    }

    [Fact]
    public void AKeyThatIsNotANodeIdentifierIsRefusedByName()
    {
        Assert.False(LocalCheckpointDocument.TryRead(
            CanonicalJsonValue.Parse(
                """{"cursors":{"NOT A NODE":{"index":1}},"fingerprint":"sha256:0000000000000000000000000000000000000000000000000000000000000000","marks":{},"revision":1,"states":{}}"""),
            out LocalCheckpoint? _,
            out IReadOnlyList<string> violations));
        Assert.Contains(violations, violation => violation.Contains("NOT A NODE", StringComparison.Ordinal));
    }

    [Fact]
    public void EveryViolationIsReportedRatherThanTheFirst()
    {
        Assert.False(LocalCheckpointDocument.TryRead(
            CanonicalJsonValue.Parse("""{"cursors":[],"marks":3}"""),
            out LocalCheckpoint? _,
            out IReadOnlyList<string> violations));

        // A caller reconciling a foreign document needs the whole report, which is the graph compiler's own
        // rule read over a document of a different shape.
        Assert.True(violations.Count >= 4, string.Join("; ", violations));
    }

    [Fact]
    public void ADocumentThatIsNotAnObjectIsRefusedAsSuch()
    {
        Assert.False(LocalCheckpointDocument.TryRead(
            CanonicalJsonValue.Parse("[]"),
            out LocalCheckpoint? _,
            out IReadOnlyList<string> violations));
        Assert.Single(violations);

        Assert.False(LocalCheckpointDocument.TryRead(
            default,
            out LocalCheckpoint? _,
            out IReadOnlyList<string> absent));
        Assert.Single(absent);
    }

    /// <summary>Builds a one-entry table for a test that only needs one seam of a kind.</summary>
    /// <param name="node">The node identifier.</param>
    /// <param name="json">The value, as its seam would have written it.</param>
    /// <returns>The table.</returns>
    private static Dictionary<NodeId, CanonicalJsonValue> Table(string node, string json) =>
        new() { [NodeId.Create(node)] = CanonicalJsonValue.Parse(json) };
}
