using System.Buffers;
using System.Globalization;
using System.Text;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Serialization;

/// <summary>
/// Writes a <see cref="StageCatalog"/> as the canonical envelope bytes of its format version.
/// </summary>
/// <remarks>
/// <para>
/// The envelope is canonical JSON with fixed schema property order (ADR 0003): every property of every
/// object type is written, always, in the order the format version fixes. It is the same discipline the
/// document envelope uses, over a different vocabulary, and it is deliberately not the discipline of
/// <see cref="CanonicalJsonValue"/>, which sorts object keys.
/// </para>
/// <para>
/// A catalog envelope carries no embedded payload and therefore no <c>null</c> anywhere: every property
/// of every catalog object is required. That is why this writer has no optional-value branch, while the
/// document writer needs one for the execution policy.
/// </para>
/// <para>
/// This writer is separate from <see cref="GraphEnvelopeWriter"/> rather than sharing its primitives.
/// The two envelopes are versioned separately, and a later catalog format version has to be able to
/// change its own rules without touching the document format; a shared writer would make one format's
/// change the other format's problem. What the two share is the vocabulary of names, taken from
/// <see cref="GraphEnvelopeSchema"/> wherever the two formats spell the same thing the same way.
/// </para>
/// <para>
/// Specifications and their port lists are written in the order the catalog and its specifications store
/// them. <see cref="StageCatalog.Create"/> and the <see cref="StageSpecification"/> factory already put
/// them in canonical order, and re-sorting here would mean two places could disagree about what
/// canonical order is; the model is the single authority.
/// </para>
/// </remarks>
internal sealed class CatalogEnvelopeWriter
{
    /// <summary>The name of the specification array property.</summary>
    internal const string Specifications = "specifications";

    /// <summary>The name of a specification's input port array property.</summary>
    internal const string InputPorts = "inputPorts";

    /// <summary>The name of a specification's output port array property.</summary>
    internal const string OutputPorts = "outputPorts";

    /// <summary>The name of a specification's result port array property.</summary>
    internal const string ResultPorts = "resultPorts";

    /// <summary>The name of a specification's required capability array property.</summary>
    internal const string RequiredCapabilities = "requiredCapabilities";

    /// <summary>The name of a port specification's element contract property.</summary>
    internal const string ElementContract = "elementContract";

    /// <summary>The name of an input port specification's optionality property.</summary>
    internal const string IsOptional = "isOptional";

    /// <summary>The name of an output port specification's ignorability property.</summary>
    internal const string IsIgnorable = "isIgnorable";

    /// <summary>The widest decimal form of an <see cref="int"/>, <c>-2147483648</c>.</summary>
    private const int MaxInt32DigitCount = 11;

    private readonly ArrayBufferWriter<byte> _output = new();

    /// <summary>
    /// Prevents a default instance of the <see cref="CatalogEnvelopeWriter"/> class from being created
    /// outside <see cref="Write"/>.
    /// </summary>
    private CatalogEnvelopeWriter()
    {
    }

    /// <summary>
    /// Writes <paramref name="catalog"/> as canonical envelope bytes.
    /// </summary>
    /// <param name="catalog">The catalog to serialize.</param>
    /// <param name="formatVersion">The format version to declare.</param>
    /// <returns>
    /// A fresh array holding minified UTF-8 without a byte order mark, owned by the caller.
    /// </returns>
    internal static byte[] Write(StageCatalog catalog, int formatVersion)
    {
        CatalogEnvelopeWriter writer = new();
        writer.WriteCatalog(catalog, formatVersion);
        return writer._output.WrittenSpan.ToArray();
    }

    /// <summary>
    /// Writes the catalog object.
    /// </summary>
    /// <param name="catalog">The catalog to write.</param>
    /// <param name="formatVersion">The format version to declare.</param>
    private void WriteCatalog(StageCatalog catalog, int formatVersion)
    {
        Append("{"u8);

        WriteName(GraphEnvelopeSchema.FormatVersion);
        WriteInteger(formatVersion);

        WriteSeparatedName(Specifications);
        Append("["u8);

        for (int index = 0; index < catalog.Specifications.Count; index++)
        {
            WriteElementSeparator(index);
            WriteSpecification(catalog.Specifications[index]);
        }

        Append("]"u8);

        Append("}"u8);
    }

    /// <summary>
    /// Writes one stage specification object.
    /// </summary>
    /// <param name="specification">The specification to write.</param>
    /// <remarks>
    /// The parameter validator is behavior and has no property here. The envelope carries the declared
    /// shape only, which is what makes a <see cref="CatalogFingerprint"/> a statement about contracts
    /// rather than about the code registered behind them.
    /// </remarks>
    private void WriteSpecification(StageSpecification specification)
    {
        Append("{"u8);

        WriteName(GraphEnvelopeSchema.StageRefName);
        WriteStageRef(specification.Stage);

        WriteSeparatedName(InputPorts);
        Append("["u8);

        for (int index = 0; index < specification.InputPorts.Count; index++)
        {
            WriteElementSeparator(index);
            WriteInputPort(specification.InputPorts[index]);
        }

        Append("]"u8);

        WriteSeparatedName(OutputPorts);
        Append("["u8);

        for (int index = 0; index < specification.OutputPorts.Count; index++)
        {
            WriteElementSeparator(index);
            WriteOutputPort(specification.OutputPorts[index]);
        }

        Append("]"u8);

        WriteSeparatedName(ResultPorts);
        Append("["u8);

        for (int index = 0; index < specification.ResultPorts.Count; index++)
        {
            WriteElementSeparator(index);
            WriteResultPort(specification.ResultPorts[index]);
        }

        Append("]"u8);

        WriteSeparatedName(GraphEnvelopeSchema.ParameterContract);
        WriteContractReference(specification.ParameterContract);

        WriteSeparatedName(RequiredCapabilities);
        Append("["u8);

        for (int index = 0; index < specification.RequiredCapabilities.Count; index++)
        {
            WriteElementSeparator(index);
            WriteString(specification.RequiredCapabilities[index].Value);
        }

        Append("]"u8);

        Append("}"u8);
    }

    /// <summary>
    /// Writes one input port specification object.
    /// </summary>
    /// <param name="port">The port to write.</param>
    private void WriteInputPort(InputPortSpecification port)
    {
        Append("{"u8);

        WriteName(GraphEnvelopeSchema.PortIdName);
        WriteString(port.Id.Value);

        WriteSeparatedName(ElementContract);
        WriteContractReference(port.ElementContract);

        WriteSeparatedName(IsOptional);
        WriteBoolean(port.IsOptional);

        Append("}"u8);
    }

    /// <summary>
    /// Writes one output port specification object.
    /// </summary>
    /// <param name="port">The port to write.</param>
    private void WriteOutputPort(OutputPortSpecification port)
    {
        Append("{"u8);

        WriteName(GraphEnvelopeSchema.PortIdName);
        WriteString(port.Id.Value);

        WriteSeparatedName(ElementContract);
        WriteContractReference(port.ElementContract);

        WriteSeparatedName(IsIgnorable);
        WriteBoolean(port.IsIgnorable);

        Append("}"u8);
    }

    /// <summary>
    /// Writes one result port specification object.
    /// </summary>
    /// <param name="port">The port to write.</param>
    private void WriteResultPort(ResultPortSpecification port)
    {
        Append("{"u8);

        WriteName(GraphEnvelopeSchema.PortIdName);
        WriteString(port.Id.Value);

        WriteSeparatedName(GraphEnvelopeSchema.ResultContract);
        WriteContractReference(port.ResultContract);

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
    /// <param name="name">The property name.</param>
    private void WriteName(string name)
    {
        WriteString(name);
        Append(":"u8);
    }

    /// <summary>
    /// Writes the separator that precedes every property except the first, then the property name.
    /// </summary>
    /// <param name="name">The property name.</param>
    private void WriteSeparatedName(string name)
    {
        Append(","u8);
        WriteName(name);
    }

    /// <summary>
    /// Writes a boolean as a JSON literal.
    /// </summary>
    /// <param name="value">The value to write.</param>
    /// <remarks>
    /// The flags are JSON booleans rather than <c>0</c> and <c>1</c> or the strings <c>"true"</c> and
    /// <c>"false"</c>, so a reader in any language decodes them as booleans without a convention of ours
    /// to know about.
    /// </remarks>
    private void WriteBoolean(bool value) => Append(value ? "true"u8 : "false"u8);

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
    /// identifier grammar admits only lowercase ASCII letters, ASCII digits, and hyphens, so no input
    /// reaches the escaping branch today. The table is implemented anyway rather than assumed away, so
    /// that a later format version relaxing a grammar finds a correct writer instead of a writer that
    /// happened to be right about a narrower alphabet.
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
