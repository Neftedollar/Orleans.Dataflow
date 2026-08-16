using System.Buffers;
using System.Globalization;
using System.Text;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Serialization;

/// <summary>
/// Writes a <see cref="GraphDocument"/> as the canonical envelope bytes of its format version.
/// </summary>
/// <remarks>
/// <para>
/// The envelope is canonical JSON with fixed schema property order (ADR 0003): every property of every
/// object type is written, always, in the order the format version fixes, and an absent optional value is
/// an explicit <c>null</c> rather than an omitted property. That is deliberately not the discipline of
/// <see cref="CanonicalJsonValue"/>, which sorts object keys, so the envelope is never routed through a
/// canonical value and the two disciplines never mix.
/// </para>
/// <para>
/// The writer is hand written for the same reason <see cref="CanonicalJsonWriter"/> is: no
/// <c>JavaScriptEncoder</c> produces the canonical escape table, which forbids the short escapes
/// <c>\n</c> and friends, requires lowercase hexadecimal in <c>\u00xx</c>, and forbids escaping non-ASCII
/// characters at all. The table is restated here rather than shared because the two writers serve
/// different disciplines and are versioned separately; a future envelope format version may change its
/// string rules without touching payload rules.
/// </para>
/// <para>
/// Collections are written in the order the document stores them. <see cref="GraphDocument.Create"/>
/// already put them in canonical order, and re-sorting here would mean two places could disagree about
/// what canonical order is; the document is the single authority.
/// </para>
/// <para>
/// Payloads are spliced byte for byte from <see cref="CanonicalJsonValue.CanonicalUtf8Bytes"/>. They are
/// canonical already, and re-encoding them would risk a second, subtly different, canonical form.
/// </para>
/// </remarks>
internal sealed class GraphEnvelopeWriter
{
    /// <summary>The widest decimal form of an <see cref="int"/>, <c>-2147483648</c>.</summary>
    private const int MaxInt32DigitCount = 11;

    /// <summary>The name of the public parameter a refused document arrived through.</summary>
    private const string DocumentParameterName = "document";

    private readonly ArrayBufferWriter<byte> _output = new();

    /// <summary>
    /// Prevents a default instance of the <see cref="GraphEnvelopeWriter"/> class from being created
    /// outside <see cref="Write"/>.
    /// </summary>
    private GraphEnvelopeWriter()
    {
    }

    /// <summary>
    /// Writes <paramref name="document"/> as canonical envelope bytes.
    /// </summary>
    /// <param name="document">The document to serialize.</param>
    /// <returns>
    /// A fresh array holding minified UTF-8 without a byte order mark, owned by the caller.
    /// </returns>
    internal static byte[] Write(GraphDocument document)
    {
        EnsureEveryPayloadIsEncodable(document);

        GraphEnvelopeWriter writer = new();
        writer.WriteDocument(document);
        return writer._output.WrittenSpan.ToArray();
    }

    /// <summary>
    /// Rejects a document that format version 1 has no byte form for.
    /// </summary>
    /// <param name="document">The document about to be written.</param>
    /// <exception cref="ArgumentException">
    /// A stage node carries the JSON null value as a payload. The message names the node and the member.
    /// </exception>
    /// <remarks>
    /// <para>
    /// Format version 1 encodes an absent execution policy as the literal <c>null</c> at a payload
    /// position, so a payload that is itself the JSON null value would share its byte form with an absent
    /// one. Rather than emit bytes that this library's own reader rejects, the document is refused here,
    /// before a byte is written, with a message that names the restriction.
    /// </para>
    /// <para>
    /// The check runs over the whole document first so that a refusal never leaves a half-written buffer
    /// behind, and so that the restriction is stated in exactly one place rather than at each of the two
    /// payload positions.
    /// </para>
    /// </remarks>
    private static void EnsureEveryPayloadIsEncodable(GraphDocument document)
    {
        for (int index = 0; index < document.Nodes.Count; index++)
        {
            StageNode node = document.Nodes[index];

            if (IsJsonNull(node.Parameters))
            {
                throw new ArgumentException(DescribeNullPayload(node, "parameter payload"), DocumentParameterName);
            }

            if (node.ExecutionPolicy is { } policy && IsJsonNull(policy))
            {
                throw new ArgumentException(
                    DescribeNullPayload(node, "execution policy payload"),
                    DocumentParameterName);
            }
        }
    }

    /// <summary>
    /// Determines whether a payload is the JSON null value.
    /// </summary>
    /// <param name="payload">The payload to classify.</param>
    /// <returns><see langword="true"/> when its canonical form is the four bytes <c>null</c>.</returns>
    private static bool IsJsonNull(CanonicalJsonValue payload) =>
        payload.CanonicalUtf8Bytes.Span.SequenceEqual("null"u8);

    /// <summary>
    /// Builds the message for a payload format version 1 cannot encode.
    /// </summary>
    /// <param name="node">The offending node.</param>
    /// <param name="role">The payload's role in the node, in prose.</param>
    /// <returns>A message naming the node, the member, and the restriction.</returns>
    private static string DescribeNullPayload(StageNode node, string role) =>
        $"The stage node '{node.Id}' carries the JSON null value as its {role}, and format version {GraphDocument.CurrentFormatVersion} has no byte form for it: the literal null at a payload position is how the format encodes an absent execution policy, so a null payload and an absent one would share one byte form. Model the empty case inside the payload schema, as an empty object or an explicit member, rather than as the JSON null value.";

    /// <summary>
    /// Writes the document object.
    /// </summary>
    /// <param name="document">The document to write.</param>
    private void WriteDocument(GraphDocument document)
    {
        Append("{"u8);

        WriteName(GraphEnvelopeSchema.FormatVersion);
        WriteInteger(document.FormatVersion);

        WriteSeparatedName(GraphEnvelopeSchema.GraphIdName);
        WriteString(document.Id.Value);

        WriteSeparatedName(GraphEnvelopeSchema.Revision);
        WriteInteger(document.Revision.Value);

        WriteSeparatedName(GraphEnvelopeSchema.Capabilities);
        Append("["u8);

        for (int index = 0; index < document.Capabilities.Count; index++)
        {
            WriteElementSeparator(index);
            WriteString(document.Capabilities[index].Value);
        }

        Append("]"u8);

        WriteSeparatedName(GraphEnvelopeSchema.Nodes);
        Append("["u8);

        for (int index = 0; index < document.Nodes.Count; index++)
        {
            WriteElementSeparator(index);
            WriteNode(document.Nodes[index]);
        }

        Append("]"u8);

        WriteSeparatedName(GraphEnvelopeSchema.Edges);
        Append("["u8);

        for (int index = 0; index < document.Edges.Count; index++)
        {
            WriteElementSeparator(index);
            WriteEdge(document.Edges[index]);
        }

        Append("]"u8);

        WriteSeparatedName(GraphEnvelopeSchema.ResultSlots);
        Append("["u8);

        for (int index = 0; index < document.ResultSlots.Count; index++)
        {
            WriteElementSeparator(index);
            WriteResultSlot(document.ResultSlots[index]);
        }

        Append("]"u8);

        Append("}"u8);
    }

    /// <summary>
    /// Writes one stage node object.
    /// </summary>
    /// <param name="node">The node to write.</param>
    /// <remarks>
    /// The execution policy contract and payload are written as literal <c>null</c> when the node takes
    /// the provider default, never omitted: a reader of a fixed-schema envelope decides what a property
    /// means by its position, so a missing property has no meaning to decide.
    /// </remarks>
    private void WriteNode(StageNode node)
    {
        Append("{"u8);

        WriteName(GraphEnvelopeSchema.NodeIdName);
        WriteString(node.Id.Value);

        WriteSeparatedName(GraphEnvelopeSchema.StageRefName);
        WriteStageRef(node.Stage);

        WriteSeparatedName(GraphEnvelopeSchema.ParameterContract);
        WriteContractReference(node.ParameterContract);

        WriteSeparatedName(GraphEnvelopeSchema.Parameters);
        WritePayload(node.Parameters);

        WriteSeparatedName(GraphEnvelopeSchema.ExecutionPolicyContract);

        if (node.ExecutionPolicyContract is { } policyContract)
        {
            WriteContractReference(policyContract);
        }
        else
        {
            Append("null"u8);
        }

        WriteSeparatedName(GraphEnvelopeSchema.ExecutionPolicy);

        if (node.ExecutionPolicy is { } policy)
        {
            WritePayload(policy);
        }
        else
        {
            Append("null"u8);
        }

        Append("}"u8);
    }

    /// <summary>
    /// Writes one stage reference object.
    /// </summary>
    /// <param name="stage">The stage reference to write.</param>
    private void WriteStageRef(StageRef stage)
    {
        Append("{"u8);

        WriteName(GraphEnvelopeSchema.ProviderIdName);
        WriteString(stage.Provider.Value);

        WriteSeparatedName(GraphEnvelopeSchema.StageIdName);
        WriteString(stage.Stage.Value);

        WriteSeparatedName(GraphEnvelopeSchema.MajorVersion);
        WriteInteger(stage.MajorVersion);

        Append("}"u8);
    }

    /// <summary>
    /// Writes one contract reference object.
    /// </summary>
    /// <param name="reference">The contract reference to write.</param>
    private void WriteContractReference(ContractReference reference)
    {
        Append("{"u8);

        WriteName(GraphEnvelopeSchema.ContractIdName);
        WriteString(reference.Contract.Value);

        WriteSeparatedName(GraphEnvelopeSchema.MajorVersion);
        WriteInteger(reference.MajorVersion);

        Append("}"u8);
    }

    /// <summary>
    /// Writes one edge object.
    /// </summary>
    /// <param name="edge">The edge to write.</param>
    private void WriteEdge(GraphEdge edge)
    {
        Append("{"u8);

        WriteName(GraphEnvelopeSchema.From);
        WritePortAddress(edge.From);

        WriteSeparatedName(GraphEnvelopeSchema.To);
        WritePortAddress(edge.To);

        Append("}"u8);
    }

    /// <summary>
    /// Writes one result slot definition object.
    /// </summary>
    /// <param name="slot">The result slot to write.</param>
    private void WriteResultSlot(ResultSlotDefinition slot)
    {
        Append("{"u8);

        WriteName(GraphEnvelopeSchema.ResultSlotIdName);
        WriteString(slot.Id.Value);

        WriteSeparatedName(GraphEnvelopeSchema.ResultContract);
        WriteContractReference(slot.ResultContract);

        WriteSeparatedName(GraphEnvelopeSchema.Producer);
        WritePortAddress(slot.Producer);

        Append("}"u8);
    }

    /// <summary>
    /// Writes one port address object.
    /// </summary>
    /// <param name="address">The port address to write.</param>
    private void WritePortAddress(PortAddress address)
    {
        Append("{"u8);

        WriteName(GraphEnvelopeSchema.NodeIdName);
        WriteString(address.Node.Value);

        WriteSeparatedName(GraphEnvelopeSchema.PortIdName);
        WriteString(address.Port.Value);

        Append("}"u8);
    }

    /// <summary>
    /// Splices an embedded canonical payload.
    /// </summary>
    /// <param name="payload">The payload, already in canonical form.</param>
    private void WritePayload(CanonicalJsonValue payload) => Append(payload.CanonicalUtf8Bytes.Span);

    /// <summary>
    /// Writes the separator that precedes every array element except the first.
    /// </summary>
    /// <param name="index">The zero-based index of the element about to be written.</param>
    private void WriteElementSeparator(int index)
    {
        if (index > 0)
        {
            Append(","u8);
        }
    }

    /// <summary>
    /// Writes a property name and the colon that follows it.
    /// </summary>
    /// <param name="name">The property name, taken from <see cref="GraphEnvelopeSchema"/>.</param>
    private void WriteName(string name)
    {
        WriteString(name);
        Append(":"u8);
    }

    /// <summary>
    /// Writes the separator that precedes every property except the first, then the property name.
    /// </summary>
    /// <param name="name">The property name, taken from <see cref="GraphEnvelopeSchema"/>.</param>
    private void WriteSeparatedName(string name)
    {
        Append(","u8);
        WriteName(name);
    }

    /// <summary>
    /// Writes an integer in minimal decimal form.
    /// </summary>
    /// <param name="value">The value to write.</param>
    /// <remarks>
    /// Formatting passes <see cref="CultureInfo.InvariantCulture"/> explicitly, because the ambient
    /// culture supplies the negative sign and some cultures do not spell it <c>-</c>. Every integer the
    /// envelope carries is positive by construction, and the invariant culture keeps that a property of
    /// the format rather than of the machine.
    /// </remarks>
    private void WriteInteger(int value)
    {
        Span<byte> digits = stackalloc byte[MaxInt32DigitCount];

        if (!value.TryFormat(digits, out int written, format: default, provider: CultureInfo.InvariantCulture))
        {
            // Unreachable: MaxInt32DigitCount is the widest decimal form an Int32 can take.
            throw new InvalidOperationException(
                "Formatting a 32-bit integer overflowed the canonical number buffer.");
        }

        Append(digits[..written]);
    }

    /// <summary>
    /// Writes a string with the canonical escape table.
    /// </summary>
    /// <param name="value">The unescaped string content.</param>
    /// <remarks>
    /// <para>
    /// Only <c>"</c>, <c>\</c>, and <c>U+0000</c> through <c>U+001F</c> are escaped; everything else,
    /// including every non-ASCII character and every surrogate pair, is written as raw UTF-8. Because no
    /// escaped character is a surrogate, a run of unescaped characters never splits a surrogate pair.
    /// </para>
    /// <para>
    /// Format version 1 puts nothing but property names and identifiers in envelope strings, and the
    /// identifier grammar admits only lowercase ASCII letters, ASCII digits, hyphens, and the node path
    /// separator, so no input reaches the escaping branch today. The table is implemented anyway rather
    /// than assumed away, so that a later format version relaxing a grammar finds a correct writer instead
    /// of a writer that happened to be right about a narrower alphabet.
    /// </para>
    /// </remarks>
    private void WriteString(string value)
    {
        Append("\""u8);

        int runStart = 0;

        for (int index = 0; index < value.Length; index++)
        {
            char character = value[index];

            if (!RequiresEscape(character))
            {
                continue;
            }

            AppendText(value.AsSpan(runStart, index - runStart));
            AppendEscape(character);
            runStart = index + 1;
        }

        AppendText(value.AsSpan(runStart));
        Append("\""u8);
    }

    /// <summary>
    /// Determines whether <paramref name="character"/> must be escaped in a canonical JSON string.
    /// </summary>
    /// <param name="character">The character to classify.</param>
    /// <returns>
    /// <see langword="true"/> for <c>"</c>, <c>\</c>, and <c>U+0000</c> through <c>U+001F</c>; otherwise
    /// <see langword="false"/>.
    /// </returns>
    private static bool RequiresEscape(char character) =>
        character is '"' or '\\' or <= '\u001f';

    /// <summary>
    /// Appends the canonical escape for one character.
    /// </summary>
    /// <param name="character">A character for which <see cref="RequiresEscape"/> is <see langword="true"/>.</param>
    /// <remarks>
    /// Control characters always take the six-character <c>\u00xx</c> form with lowercase hexadecimal
    /// digits; the canonical form has no short escapes, so a newline is written as <c>\u000a</c> and
    /// never as <c>\n</c>.
    /// </remarks>
    private void AppendEscape(char character)
    {
        switch (character)
        {
            case '"':
                Append("\\\""u8);
                return;

            case '\\':
                Append("\\\\"u8);
                return;

            default:
                Span<byte> escape = stackalloc byte[6];
                escape[0] = (byte)'\\';
                escape[1] = (byte)'u';
                escape[2] = (byte)'0';
                escape[3] = (byte)'0';
                escape[4] = LowercaseHexDigit(character >> 4);
                escape[5] = LowercaseHexDigit(character & 0xF);
                Append(escape);
                return;
        }
    }

    /// <summary>
    /// Renders one nibble as a lowercase ASCII hexadecimal digit.
    /// </summary>
    /// <param name="nibble">A value from <c>0</c> to <c>15</c>.</param>
    /// <returns>The ASCII byte for <c>0</c>-<c>9</c> or <c>a</c>-<c>f</c>.</returns>
    private static byte LowercaseHexDigit(int nibble) =>
        (byte)(nibble < 10 ? '0' + nibble : 'a' + (nibble - 10));

    /// <summary>
    /// Appends raw canonical bytes.
    /// </summary>
    /// <param name="bytes">The bytes to append.</param>
    private void Append(ReadOnlySpan<byte> bytes) => _output.Write(bytes);

    /// <summary>
    /// Appends a run of unescaped string characters as UTF-8.
    /// </summary>
    /// <param name="text">The characters to encode; may be empty.</param>
    private void AppendText(ReadOnlySpan<char> text)
    {
        if (text.IsEmpty)
        {
            return;
        }

        Span<byte> destination = _output.GetSpan(Encoding.UTF8.GetMaxByteCount(text.Length));
        int written = Encoding.UTF8.GetBytes(text, destination);
        _output.Advance(written);
    }
}
