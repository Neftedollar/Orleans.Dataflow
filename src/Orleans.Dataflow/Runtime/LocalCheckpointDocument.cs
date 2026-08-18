using System.Globalization;
using System.Text;
using System.Text.Json;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The one place that knows how a checkpoint is written into a store and read back out of one.
/// </summary>
/// <remarks>
/// <para>
/// ADR 0007's five parts, and all five are members of one canonical value: the graph fingerprint and the
/// revision the snapshot was taken of, the per-source cursors, the per-scope declared-durable state, and the
/// per-sink commit marks. A payload with a writer, a reader that refuses what it does not declare, and a
/// golden test over the bytes — the same discipline every stage payload of this vocabulary follows, applied
/// to the one document that is not a stage's.
/// </para>
/// <para>
/// <b>Every value inside it is a canonical value, and that is the seam's requirement rather than this
/// type's convenience.</b> A cursor, a durable state, and a commit mark are produced by an adapter, a scope,
/// and a sink respectively, and each of them hands over a <see cref="CanonicalJsonValue"/> — never an
/// object, never a CLR type name, never a serializer's opinion. That is what makes a checkpoint a value one
/// process writes and another reads, which is the whole reason the durable half of this engine can exist at
/// all. A seam that cannot serialize into the canonical plane declares no cursor, no state, and no mark, and
/// contributes nothing here.
/// </para>
/// <para>
/// <b>The three tables are keyed by node identifier</b>, because a node identifier is the one name a
/// document and a resumed run of it agree on. A key that names no node of the resumed graph is refused by
/// the resume rather than ignored: it is either a checkpoint of a different graph, which the fingerprint
/// would already have caught, or a bug worth a diagnostic.
/// </para>
/// <para>
/// <b>All five members are always present</b>, with an empty object standing for "nothing of this kind". A
/// shape that varied with what a run happened to have would make a reader guess whether an absent
/// <c>marks</c> meant "no sink marks" or "written by a version that had no marks", and guessing is what a
/// strict reader exists to avoid.
/// </para>
/// </remarks>
internal static class LocalCheckpointDocument
{
    /// <summary>The member holding the fingerprint of the graph the snapshot was taken of.</summary>
    internal const string FingerprintMember = "fingerprint";

    /// <summary>The member holding the revision of that graph.</summary>
    internal const string RevisionMember = "revision";

    /// <summary>The member holding one cursor per source that declares one.</summary>
    internal const string CursorsMember = "cursors";

    /// <summary>The member holding one exported state per durable scope.</summary>
    internal const string StatesMember = "states";

    /// <summary>The member holding one commit mark per sink that declares one.</summary>
    internal const string MarksMember = "marks";

    /// <summary>The members a checkpoint document declares, and no others.</summary>
    private static readonly string[] Declared =
        [FingerprintMember, RevisionMember, CursorsMember, StatesMember, MarksMember];

    /// <summary>Writes one snapshot as the canonical document a store holds.</summary>
    /// <param name="graph">The fingerprint of the graph the snapshot was taken of.</param>
    /// <param name="revision">The revision of that graph.</param>
    /// <param name="cursors">One position per source that declares a cursor, keyed by node.</param>
    /// <param name="states">One exported state per durable scope, keyed by node.</param>
    /// <param name="marks">One commit mark per sink that declares one, keyed by node.</param>
    /// <returns>The canonical document.</returns>
    /// <remarks>
    /// The three tables are written in node order, which costs a sort per capture and buys the property that
    /// makes a golden test meaningful: two captures of one run state produce byte-identical documents
    /// whatever order the plan happened to enumerate its seams in. Canonicalization would sort the keys
    /// anyway; sorting here is what makes the text this method builds already canonical, so the parse below
    /// is a validation rather than a rewrite.
    /// </remarks>
    internal static CanonicalJsonValue Write(
        GraphFingerprint graph,
        GraphRevision revision,
        IReadOnlyDictionary<NodeId, CanonicalJsonValue> cursors,
        IReadOnlyDictionary<NodeId, CanonicalJsonValue> states,
        IReadOnlyDictionary<NodeId, CanonicalJsonValue> marks)
    {
        StringBuilder text = new();

        _ = text.Append(CultureInfo.InvariantCulture, $"{{\"{CursorsMember}\":");

        Table(text, cursors);

        _ = text.Append(CultureInfo.InvariantCulture, $",\"{FingerprintMember}\":\"{graph}\"")
            .Append(CultureInfo.InvariantCulture, $",\"{MarksMember}\":");

        Table(text, marks);

        _ = text.Append(CultureInfo.InvariantCulture, $",\"{RevisionMember}\":{revision.Value}")
            .Append(CultureInfo.InvariantCulture, $",\"{StatesMember}\":");

        Table(text, states);

        return CanonicalJsonValue.Parse(text.Append('}').ToString());
    }

    /// <summary>Reads a stored document back into the snapshot it was written from.</summary>
    /// <param name="document">The document a store handed back, in canonical form.</param>
    /// <param name="checkpoint">
    /// When this method returns <see langword="true"/>, the snapshot; otherwise <see langword="null"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation;
    /// otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the document is a checkpoint this runtime can read.</returns>
    /// <remarks>
    /// Every violation is collected rather than the first one reported, because a caller reconciling a
    /// hand-written or foreign document needs the whole report; that is the graph compiler's rule applied to
    /// a document of a different shape.
    /// </remarks>
    internal static bool TryRead(
        CanonicalJsonValue document,
        out LocalCheckpoint? checkpoint,
        out IReadOnlyList<string> violations)
    {
        checkpoint = null;

        if (document.IsDefault || document.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = [
                document.IsDefault
                    ? "the document is absent, and a checkpoint is a JSON object"
                    : "the document is not a JSON object, and a checkpoint is one"];

            return false;
        }

        JsonElement payload = document.ToElement();
        List<string> found = [];

        bool read = TryReadFingerprint(payload, found, out GraphFingerprint fingerprint);

        read &= TryReadRevision(payload, found, out GraphRevision revision);
        read &= TryReadTable(payload, CursorsMember, found, out IReadOnlyDictionary<NodeId, CanonicalJsonValue> cursors);
        read &= TryReadTable(payload, StatesMember, found, out IReadOnlyDictionary<NodeId, CanonicalJsonValue> states);
        read &= TryReadTable(payload, MarksMember, found, out IReadOnlyDictionary<NodeId, CanonicalJsonValue> marks);

        LocalParameterPayload.ReportUnknownMembers(payload, Declared, found);

        if (!read || found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        checkpoint = new LocalCheckpoint(fingerprint, revision, cursors, states, marks);

        return true;
    }

    /// <summary>Writes one table of node-keyed canonical values, in node order.</summary>
    /// <param name="text">The document under construction, positioned where the object begins.</param>
    /// <param name="values">The values, keyed by node.</param>
    private static void Table(StringBuilder text, IReadOnlyDictionary<NodeId, CanonicalJsonValue> values)
    {
        _ = text.Append('{');

        List<NodeId> order = [.. values.Keys];

        order.Sort();

        for (int index = 0; index < order.Count; index++)
        {
            _ = text.Append(index == 0 ? string.Empty : ",")
                .Append(CultureInfo.InvariantCulture, $"\"{order[index]}\":{values[order[index]]}");
        }

        _ = text.Append('}');
    }

    /// <summary>Reads the member that has to be the fingerprint the snapshot was taken of.</summary>
    /// <param name="payload">The document object.</param>
    /// <param name="violations">The list one lower-case sentence fragment is added to per violation.</param>
    /// <param name="fingerprint">
    /// When this method returns <see langword="true"/>, the fingerprint; otherwise the default value.
    /// </param>
    /// <returns><see langword="true"/> when the member is a well-formed fingerprint.</returns>
    private static bool TryReadFingerprint(
        JsonElement payload,
        List<string> violations,
        out GraphFingerprint fingerprint)
    {
        fingerprint = default;

        if (!payload.TryGetProperty(FingerprintMember, out JsonElement declared))
        {
            violations.Add(LocalParameterPayload.DescribeMissing(FingerprintMember));

            return false;
        }

        if (declared.ValueKind is not JsonValueKind.String)
        {
            violations.Add(LocalParameterPayload.DescribeWrongKind(
                FingerprintMember,
                declared,
                "the text of a graph fingerprint"));

            return false;
        }

        if (!GraphFingerprint.TryParse(declared.GetString(), out fingerprint))
        {
            violations.Add(
                $"the member '{FingerprintMember}' is '{declared.GetString()}', and it is the text of a graph fingerprint");

            return false;
        }

        return true;
    }

    /// <summary>Reads the member that has to be the revision the snapshot was taken of.</summary>
    /// <param name="payload">The document object.</param>
    /// <param name="violations">The list one lower-case sentence fragment is added to per violation.</param>
    /// <param name="revision">
    /// When this method returns <see langword="true"/>, the revision; otherwise the default value.
    /// </param>
    /// <returns><see langword="true"/> when the member is a declared revision number.</returns>
    private static bool TryReadRevision(JsonElement payload, List<string> violations, out GraphRevision revision)
    {
        revision = default;

        if (!LocalParameterPayload.TryReadPositiveInteger(payload, RevisionMember, violations, out int number))
        {
            return false;
        }

        if (!GraphRevision.TryCreate(number, out revision))
        {
            violations.Add(string.Create(
                CultureInfo.InvariantCulture,
                $"the member '{RevisionMember}' is {number}, and it is a graph revision number"));

            return false;
        }

        return true;
    }

    /// <summary>Reads one member that has to be a table of node-keyed canonical values.</summary>
    /// <param name="payload">The document object.</param>
    /// <param name="member">The member name.</param>
    /// <param name="violations">The list one lower-case sentence fragment is added to per violation.</param>
    /// <param name="values">
    /// When this method returns <see langword="true"/>, the values keyed by node; otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the member is an object whose keys are node identifiers.</returns>
    /// <remarks>
    /// The values are not looked into at all, and that is the seam's contract rather than laziness: what a
    /// cursor, a state, or a mark means is the adapter's, the scope's, and the sink's own business, and this
    /// document carries the value without a second opinion about it. What it does check is the key, because
    /// the key is this document's own grammar.
    /// </remarks>
    private static bool TryReadTable(
        JsonElement payload,
        string member,
        List<string> violations,
        out IReadOnlyDictionary<NodeId, CanonicalJsonValue> values)
    {
        values = new Dictionary<NodeId, CanonicalJsonValue>();

        if (!payload.TryGetProperty(member, out JsonElement declared))
        {
            violations.Add(LocalParameterPayload.DescribeMissing(member));

            return false;
        }

        if (declared.ValueKind is not JsonValueKind.Object)
        {
            violations.Add(LocalParameterPayload.DescribeWrongKind(
                member,
                declared,
                "an object keyed by node identifier"));

            return false;
        }

        Dictionary<NodeId, CanonicalJsonValue> read = [];
        bool valid = true;

        foreach (JsonProperty entry in declared.EnumerateObject())
        {
            if (!NodeId.TryParse(entry.Name, out NodeId node))
            {
                violations.Add($"the key '{entry.Name}' of the member '{member}' is not a node identifier");
                valid = false;

                continue;
            }

            read[node] = CanonicalJsonValue.FromElement(entry.Value);
        }

        if (valid)
        {
            values = read;
        }

        return valid;
    }
}
