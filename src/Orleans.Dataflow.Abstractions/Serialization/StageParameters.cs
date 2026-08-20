using System.Buffers;
using System.Text;
using System.Text.Json;

namespace Orleans.Dataflow.Serialization;

/// <summary>
/// Writes a stage's parameter payload a member at a time, so that ordinary authoring never composes JSON
/// text.
/// </summary>
/// <remarks>
/// <para>
/// A parameter payload is a JSON object whose members are the numbers, words, and flags that configure one
/// occurrence of a stage. Before this type the only way to write one was to build the JSON yourself — with a
/// format string, an invariant culture, and hand-escaped quotation marks — which is a lot of ceremony for
/// <c>{"n":10}</c> and one typo away from a payload that parses and means something else.
/// </para>
/// <para>
/// <b>The document does not change.</b> <see cref="Build"/> ends in
/// <see cref="CanonicalJsonValue.Parse(ReadOnlySpan{byte})"/>, the same entry point a hand-composed string
/// goes through, so the canonical bytes of a payload built here are byte-identical to the canonical bytes of
/// the equivalent text. Every rule of the canonical form is applied by that call and not restated here:
/// members are sorted, a duplicate member is refused, depth and size are bounded, and a number outside
/// <see cref="long"/> never reaches the writer because the API cannot express one.
/// </para>
/// <para>
/// <b>Nothing here is reflection.</b> The intermediate text is written with <see cref="Utf8JsonWriter"/>,
/// which is a writer over values this builder already holds and not a serializer over a CLR type graph. No
/// type name is read, none is written, and there is nothing for trimming or Native AOT to fail to see. That
/// is the whole reason the builder is explicit rather than a <c>Serialize(myOptions)</c> convenience: a
/// reflection serializer would put CLR names one attribute away from a document that must never contain
/// one, and would undo the AOT work this package already paid for.
/// </para>
/// <para>
/// The intermediate <see cref="Utf8JsonWriter"/> output is not itself canonical — it may order members as
/// they were added and may emit the short escapes <c>\n</c> and <c>\t</c> that the canonical form forbids —
/// and that is harmless precisely because it is an intermediate. Canonicalization unescapes what it is given
/// and re-escapes with its own fixed table, so what is stored is the canonical form of the value rather than
/// of the spelling.
/// </para>
/// <para>
/// One thing about that writer is <em>not</em> harmless and is refused here rather than inherited: it
/// substitutes the replacement character for an unpaired surrogate, where
/// <see cref="CanonicalJsonValue.Parse(string)"/> refuses one by name. Text with no UTF-8 encoding is
/// therefore rejected as it is added, so a member name or a word this builder accepts is exactly a member
/// name or a word the text spelling accepts, and neither spelling ever stores text its author did not write.
/// </para>
/// <para>
/// A builder is mutable and every method returns the same instance, so a chain reads as one expression.
/// It is not thread-safe and is not meant to be shared: build a payload, call <see cref="Build"/>, and let
/// it go. <see cref="Build"/> may be called more than once, and answers what the builder says at the moment
/// it is called — a nested builder or a sequence added earlier is read then rather than copied when it was
/// added, so a builder that is still being filled produces a value that reflects the filling.
/// </para>
/// <para>
/// The empty payload has a name of its own rather than a spelling here: a stage that takes no parameters
/// carries <see cref="CanonicalJsonValue.Empty"/>.
/// </para>
/// </remarks>
public sealed class StageParameters
{
    /// <summary>The members added so far, in the order they were added.</summary>
    /// <remarks>
    /// Order is not canonical order and does not have to be: the canonicalizing parse in
    /// <see cref="Build"/> sorts the members and refuses a duplicate, so the builder keeps the author's
    /// order and lets one rule about ordering live in one place.
    /// </remarks>
    private readonly List<Action<Utf8JsonWriter>> _members = [];

    /// <summary>Initializes a new instance of the <see cref="StageParameters"/> class.</summary>
    /// <remarks>Private because <see cref="Create"/> is the one spelling that starts a payload.</remarks>
    private StageParameters()
    {
    }

    /// <summary>Starts a parameter payload with no members yet.</summary>
    /// <returns>The builder.</returns>
    /// <remarks>
    /// A stage that has no parameters at all does not start a builder and discard it: it carries
    /// <see cref="CanonicalJsonValue.Empty"/>, which says the same thing in one word.
    /// </remarks>
    public static StageParameters Create() => new();

    /// <summary>Adds a whole number.</summary>
    /// <param name="name">The member name.</param>
    /// <param name="value">The value.</param>
    /// <returns>This builder, so members chain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="name"/> has no UTF-8 encoding.</exception>
    /// <remarks>
    /// <see cref="long"/> and no floating-point sibling, because the canonical form admits integers that fit
    /// in a <see cref="long"/> and nothing else. A fraction has no canonical spelling, so there is no
    /// overload that would let one be written and refused a call later; a value that is genuinely fractional
    /// is written by an author as the units it is counted in.
    /// </remarks>
    public StageParameters Add(string name, long value)
    {
        string member = Named(name);

        _members.Add(writer => writer.WriteNumber(member, value));

        return this;
    }

    /// <summary>Adds a word.</summary>
    /// <param name="name">The member name.</param>
    /// <param name="value">The value.</param>
    /// <returns>This builder, so members chain.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> or <paramref name="value"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="name"/> or <paramref name="value"/> has no UTF-8 encoding.
    /// </exception>
    /// <remarks>
    /// A <see langword="null"/> value is refused rather than written as JSON <c>null</c>, because the two
    /// are different statements and <see cref="AddNull"/> is how the second one is made.
    /// </remarks>
    public StageParameters Add(string name, string value)
    {
        string member = Named(name);

        ArgumentNullException.ThrowIfNull(value);

        string text = Transcodable(value, "member value", nameof(value));

        _members.Add(writer => writer.WriteString(member, text));

        return this;
    }

    /// <summary>Adds a flag.</summary>
    /// <param name="name">The member name.</param>
    /// <param name="value">The value.</param>
    /// <returns>This builder, so members chain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="name"/> has no UTF-8 encoding.</exception>
    public StageParameters Add(string name, bool value)
    {
        string member = Named(name);

        _members.Add(writer => writer.WriteBoolean(member, value));

        return this;
    }

    /// <summary>Adds a nested object.</summary>
    /// <param name="name">The member name.</param>
    /// <param name="value">The builder holding the nested object's members.</param>
    /// <returns>This builder, so members chain.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> or <paramref name="value"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException"><paramref name="name"/> has no UTF-8 encoding.</exception>
    /// <remarks>
    /// The nested builder is read when <see cref="Build"/> runs rather than copied now, so a payload
    /// assembled out of order still writes what its parts say at the end.
    /// </remarks>
    public StageParameters Add(string name, StageParameters value)
    {
        string member = Named(name);

        ArgumentNullException.ThrowIfNull(value);

        _members.Add(writer =>
        {
            writer.WritePropertyName(member);
            value.WriteObject(writer);
        });

        return this;
    }

    /// <summary>Adds a value that is already canonical.</summary>
    /// <param name="name">The member name.</param>
    /// <param name="value">The value; must not be the default value.</param>
    /// <returns>This builder, so members chain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="value"/> is the default value, or <paramref name="name"/> has no UTF-8 encoding.
    /// </exception>
    /// <remarks>
    /// The escape hatch, and the one member overload that names <see cref="CanonicalJsonValue"/>: a stage
    /// whose payload embeds another stage's payload — a scope holding the chain inside it, a policy read from
    /// somewhere else — has a whole canonical value in hand and should put it in rather than take it apart.
    /// </remarks>
    public StageParameters Add(string name, CanonicalJsonValue value)
    {
        string member = Named(name);

        if (value.IsDefault)
        {
            throw new ArgumentException(
                $"A parameter member requires a created {nameof(CanonicalJsonValue)}; the default {nameof(CanonicalJsonValue)} carries no JSON. Write {nameof(CanonicalJsonValue)}.{nameof(CanonicalJsonValue.Empty)} for the empty object, or {nameof(AddNull)} for JSON null.",
                nameof(value));
        }

        _members.Add(writer =>
        {
            writer.WritePropertyName(member);
            writer.WriteRawValue(value.CanonicalUtf8Bytes.Span, skipInputValidation: true);
        });

        return this;
    }

    /// <summary>Adds a member whose value is JSON <c>null</c>.</summary>
    /// <param name="name">The member name.</param>
    /// <returns>This builder, so members chain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="name"/> has no UTF-8 encoding.</exception>
    /// <remarks>
    /// A method of its own rather than a <see langword="null"/> passed to <see cref="Add(string, string)"/>,
    /// so that "this member is absent from my configuration" and "this member is present and its value is
    /// nothing" cannot be written the same way by accident.
    /// </remarks>
    public StageParameters AddNull(string name)
    {
        string member = Named(name);

        _members.Add(writer => writer.WriteNull(member));

        return this;
    }

    /// <summary>Adds an ordered list of whole numbers.</summary>
    /// <param name="name">The member name.</param>
    /// <param name="values">The values, in the order the payload states them.</param>
    /// <returns>This builder, so members chain.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> or <paramref name="values"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException"><paramref name="name"/> has no UTF-8 encoding.</exception>
    /// <remarks>
    /// The sequence is enumerated when <see cref="Build"/> runs, so a list still being filled writes what it
    /// holds at the end. Array order is preserved by the canonical form, unlike member order.
    /// </remarks>
    public StageParameters Add(string name, IEnumerable<long> values)
    {
        string member = Named(name);

        ArgumentNullException.ThrowIfNull(values);

        _members.Add(writer =>
        {
            writer.WriteStartArray(member);

            foreach (long value in values)
            {
                writer.WriteNumberValue(value);
            }

            writer.WriteEndArray();
        });

        return this;
    }

    /// <summary>Adds an ordered list of words.</summary>
    /// <param name="name">The member name.</param>
    /// <param name="values">The values, in the order the payload states them.</param>
    /// <returns>This builder, so members chain.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> or <paramref name="values"/> is <see langword="null"/>, or one of
    /// <paramref name="values"/> is.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="name"/> has no UTF-8 encoding, or one of <paramref name="values"/> has none — the
    /// second reported when <see cref="Build"/> enumerates the sequence, because that is when it is read.
    /// </exception>
    public StageParameters Add(string name, IEnumerable<string> values)
    {
        string member = Named(name);

        ArgumentNullException.ThrowIfNull(values);

        _members.Add(writer =>
        {
            writer.WriteStartArray(member);

            foreach (string value in values)
            {
                ArgumentNullException.ThrowIfNull(value, nameof(values));

                writer.WriteStringValue(Transcodable(value, "member value", nameof(values)));
            }

            writer.WriteEndArray();
        });

        return this;
    }

    /// <summary>Adds an ordered list of nested objects.</summary>
    /// <param name="name">The member name.</param>
    /// <param name="values">The builders holding each object's members, in the order the payload states them.</param>
    /// <returns>This builder, so members chain.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> or <paramref name="values"/> is <see langword="null"/>, or one of
    /// <paramref name="values"/> is.
    /// </exception>
    /// <exception cref="ArgumentException"><paramref name="name"/> has no UTF-8 encoding.</exception>
    /// <remarks>
    /// The shape a stage that owns a chain of other stages writes: one object per step, in the order the
    /// steps run, which is why the list is ordered and the members of each step are not.
    /// </remarks>
    public StageParameters Add(string name, IEnumerable<StageParameters> values)
    {
        string member = Named(name);

        ArgumentNullException.ThrowIfNull(values);

        _members.Add(writer =>
        {
            writer.WriteStartArray(member);

            foreach (StageParameters value in values)
            {
                ArgumentNullException.ThrowIfNull(value, nameof(values));

                value.WriteObject(writer);
            }

            writer.WriteEndArray();
        });

        return this;
    }

    /// <summary>Canonicalizes everything added so far into the payload a node carries.</summary>
    /// <returns>The canonical payload, which is the empty object when nothing was added.</returns>
    /// <exception cref="ArgumentException">
    /// The payload breaks a canonical rule: two members share a name, the value nests deeper than
    /// <see cref="CanonicalJsonValue.MaxDepth"/>, or its canonical form is longer than
    /// <see cref="CanonicalJsonValue.MaxCanonicalBytes"/>. The message names the rule that was broken.
    /// </exception>
    /// <remarks>
    /// Every check belongs to the canonicalizing parse rather than to this builder, which is what makes a
    /// payload written here indistinguishable from the same payload written as text: one rule set, applied
    /// once, at the one place a canonical value can come into being.
    /// </remarks>
    public CanonicalJsonValue Build()
    {
        ArrayBufferWriter<byte> buffer = new();

        using (Utf8JsonWriter writer = new(buffer))
        {
            WriteObject(writer);
        }

        return CanonicalJsonValue.Parse(buffer.WrittenSpan);
    }

    /// <summary>Returns the canonical text of the payload built so far.</summary>
    /// <returns>The canonical JSON of <see cref="Build"/>.</returns>
    /// <remarks>
    /// Rendering the built value rather than the pending members, so that what a debugger shows is what a
    /// document would store. A builder whose members break a canonical rule renders the refusal instead,
    /// because a <see cref="object.ToString"/> that throws is worse than one that explains.
    /// </remarks>
    public override string ToString()
    {
        try
        {
            return Build().ToString();
        }
        catch (ArgumentException refusal)
        {
            return $"(invalid {nameof(StageParameters)}: {refusal.Message})";
        }
    }

    /// <summary>Checks a member name and hands it back.</summary>
    /// <param name="name">The candidate name.</param>
    /// <returns>The name.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="name"/> has no UTF-8 encoding.</exception>
    /// <remarks>
    /// A member name is free text rather than an identifier segment — the canonical form says nothing about
    /// what a JSON key may spell — so the only rule here is the one the canonical form does state: it has to
    /// be text UTF-8 can carry.
    /// </remarks>
    private static string Named(string name)
    {
        ArgumentNullException.ThrowIfNull(name);

        return Transcodable(name, "member name", nameof(name));
    }

    /// <summary>Refuses text that has no UTF-8 encoding, and hands back text that has one.</summary>
    /// <param name="text">The candidate text.</param>
    /// <param name="part">What the text is, for the message.</param>
    /// <param name="parameterName">The name of the argument the author wrote.</param>
    /// <returns><paramref name="text"/>.</returns>
    /// <exception cref="ArgumentException"><paramref name="text"/> contains an unpaired surrogate.</exception>
    /// <remarks>
    /// <para>
    /// This is the one rule the intermediate writer would otherwise break silently. <see cref="Utf8JsonWriter"/>
    /// substitutes the replacement character for an unpaired surrogate, so <c>"\ud800"</c> would be
    /// <em>stored</em> as <c>"�"</c> — a different payload, a different fingerprint, and no diagnostic —
    /// while the same value composed as JSON text is refused by
    /// <see cref="CanonicalJsonValue.Parse(string)"/> by name. Two spellings of one payload have to meet at
    /// the same bytes or refuse together, so this refuses.
    /// </para>
    /// <para>
    /// The check is made where the author wrote the argument rather than at <see cref="Build"/>, so the
    /// refusal names the line that produced it. <see cref="Encoding.GetByteCount(string)"/> applies the
    /// encoder fallback without allocating the encoded bytes, so a well-formed string pays a scan and
    /// nothing else.
    /// </para>
    /// </remarks>
    private static string Transcodable(string text, string part, string parameterName)
    {
        try
        {
            _ = StrictUtf8.GetByteCount(text);
        }
        catch (EncoderFallbackException exception)
        {
            throw new ArgumentException(
                CanonicalJsonGrammar.FormatUntranscodableText(part),
                parameterName,
                exception);
        }

        return text;
    }

    /// <summary>Writes this builder's members as one JSON object.</summary>
    /// <param name="writer">The writer to write into.</param>
    private void WriteObject(Utf8JsonWriter writer)
    {
        writer.WriteStartObject();

        foreach (Action<Utf8JsonWriter> member in _members)
        {
            member(writer);
        }

        writer.WriteEndObject();
    }

    /// <summary>A UTF-8 encoding that throws instead of substituting the replacement character.</summary>
    /// <remarks>
    /// The same encoding <see cref="CanonicalJsonValue.Parse(string)"/> transcodes with, for the same
    /// reason: the two spellings of a payload have to agree about which text is writable at all.
    /// </remarks>
    private static UTF8Encoding StrictUtf8 { get; } =
        new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
}
