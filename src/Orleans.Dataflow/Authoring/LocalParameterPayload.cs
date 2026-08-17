using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The payload rules the parameterized local stages share: what an object looks like, what a positive
/// integer member looks like, and how a violation of either is worded.
/// </summary>
/// <remarks>
/// <para>
/// Two stage families carry parameters — the buffer and the two asynchronous mappings — and both say the
/// same things about the same kinds of mistake. Saying them once means the two reports read alike and
/// cannot drift into two dialects of the same complaint.
/// </para>
/// <para>
/// Every fragment produced here follows the <see cref="Definition.IStageParameterValidator"/> convention: a
/// lower-case sentence fragment, no leading capital, no trailing period, and no CLR type name, because the
/// graph compiler embeds it in a diagnostic it composes itself.
/// </para>
/// </remarks>
internal static class LocalParameterPayload
{
    /// <summary>Describes a payload that is not a JSON object at all.</summary>
    /// <param name="parameters">The payload, which may be the default value.</param>
    /// <returns>The violation fragment.</returns>
    internal static string DescribeNotAnObject(CanonicalJsonValue parameters) =>
        parameters.IsDefault
            ? "the payload is absent, and this stage's parameters are a JSON object"
            : $"the payload is {Describe(parameters.ToElement())}, and this stage's parameters are a JSON object";

    /// <summary>Describes a member the payload has to carry and does not.</summary>
    /// <param name="member">The member name.</param>
    /// <returns>The violation fragment.</returns>
    internal static string DescribeMissing(string member) => $"the member '{member}' is missing";

    /// <summary>Describes a member that is present with the wrong kind of value.</summary>
    /// <param name="member">The member name.</param>
    /// <param name="value">The value found.</param>
    /// <param name="expected">What the member has to be, read after "and it is".</param>
    /// <returns>The violation fragment.</returns>
    internal static string DescribeWrongKind(string member, JsonElement value, string expected) =>
        $"the member '{member}' is {Describe(value)}, and it is {expected}";

    /// <summary>Reads a member that has to be an integer of at least one.</summary>
    /// <param name="payload">The payload object.</param>
    /// <param name="member">The member name.</param>
    /// <param name="violations">The report under construction, appended to when the member is wrong.</param>
    /// <param name="value">
    /// When this method returns <see langword="true"/>, the value; otherwise zero.
    /// </param>
    /// <returns><see langword="true"/> when the member is present and is an integer of at least one.</returns>
    /// <remarks>
    /// Canonical JSON admits only integers that fit in a 64-bit signed integer, so the two ways this fails
    /// beyond "not a number" are a value outside the 32-bit range and a value below one. Both are reported
    /// with the offending number in the message, because the number is what the reader has to change.
    /// </remarks>
    internal static bool TryReadPositiveInteger(
        JsonElement payload,
        string member,
        List<string> violations,
        out int value)
    {
        value = 0;

        if (!payload.TryGetProperty(member, out JsonElement declared))
        {
            violations.Add(DescribeMissing(member));

            return false;
        }

        if (declared.ValueKind is not JsonValueKind.Number)
        {
            violations.Add(DescribeWrongKind(member, declared, "a positive integer"));

            return false;
        }

        if (!declared.TryGetInt32(out int number))
        {
            violations.Add(
                $"the member '{member}' is {declared.GetRawText()}, and it is a positive integer no greater than {int.MaxValue.ToString(CultureInfo.InvariantCulture)}");

            return false;
        }

        if (number < 1)
        {
            violations.Add(string.Create(
                CultureInfo.InvariantCulture,
                $"the member '{member}' is {number}, and it is a positive integer"));

            return false;
        }

        value = number;

        return true;
    }

    /// <summary>Reads a member that has to be an integer of at least zero.</summary>
    /// <param name="payload">The payload object.</param>
    /// <param name="member">The member name.</param>
    /// <param name="violations">The report under construction, appended to when the member is wrong.</param>
    /// <param name="value">
    /// When this method returns <see langword="true"/>, the value; otherwise zero.
    /// </param>
    /// <returns><see langword="true"/> when the member is present and is an integer of at least zero.</returns>
    /// <remarks>
    /// The counted stages differ from the bounded ones in exactly this: a buffer of zero elements and an
    /// asynchronous stage running zero callbacks describe nothing that could run, while taking zero
    /// elements, skipping zero, and repeating a value zero times all describe something perfectly
    /// ordinary. Zero is therefore admitted here and refused there.
    /// </remarks>
    internal static bool TryReadNonNegativeInteger(
        JsonElement payload,
        string member,
        List<string> violations,
        out int value)
    {
        value = 0;

        if (!TryReadInteger(payload, member, "an integer of zero or more", violations, out int number))
        {
            return false;
        }

        if (number < 0)
        {
            violations.Add(string.Create(
                CultureInfo.InvariantCulture,
                $"the member '{member}' is {number}, and it is an integer of zero or more"));

            return false;
        }

        value = number;

        return true;
    }

    /// <summary>Reads a member that has to be an integer, of any sign.</summary>
    /// <param name="payload">The payload object.</param>
    /// <param name="member">The member name.</param>
    /// <param name="expected">What the member has to be, read after "and it is".</param>
    /// <param name="violations">The report under construction, appended to when the member is wrong.</param>
    /// <param name="value">
    /// When this method returns <see langword="true"/>, the value; otherwise zero.
    /// </param>
    /// <returns><see langword="true"/> when the member is present and fits in a 32-bit signed integer.</returns>
    /// <remarks>
    /// The one member with no sign restriction is a range's start, which counts down from a negative
    /// number as legitimately as it counts up from a positive one. The 32-bit bound is not a restriction of
    /// this reader's own: canonical JSON admits integers up to 64 bits, and an element index this runtime
    /// cannot hold in an <see cref="int"/> is one it could not enumerate either.
    /// </remarks>
    internal static bool TryReadInteger(
        JsonElement payload,
        string member,
        string expected,
        List<string> violations,
        out int value)
    {
        value = 0;

        if (!payload.TryGetProperty(member, out JsonElement declared))
        {
            violations.Add(DescribeMissing(member));

            return false;
        }

        if (declared.ValueKind is not JsonValueKind.Number)
        {
            violations.Add(DescribeWrongKind(member, declared, expected));

            return false;
        }

        if (!declared.TryGetInt32(out int number))
        {
            violations.Add(
                $"the member '{member}' is {declared.GetRawText()}, and it is {expected} between {int.MinValue.ToString(CultureInfo.InvariantCulture)} and {int.MaxValue.ToString(CultureInfo.InvariantCulture)}");

            return false;
        }

        value = number;

        return true;
    }

    /// <summary>Reports every member of a payload that the stage does not declare.</summary>
    /// <param name="payload">The payload object.</param>
    /// <param name="declared">The member names the stage declares, in payload order.</param>
    /// <param name="violations">The report under construction, appended to for each unknown member.</param>
    /// <remarks>
    /// Unknown members are rejected rather than ignored. A payload carrying a member this stage does not
    /// read is either written for a different stage or written against a version this one is not, and both
    /// are worth a diagnostic rather than a silent execution of the half that happened to be understood.
    /// </remarks>
    internal static void ReportUnknownMembers(
        JsonElement payload,
        IReadOnlyList<string> declared,
        List<string> violations)
    {
        foreach (JsonProperty member in payload.EnumerateObject())
        {
            bool known = false;

            for (int index = 0; index < declared.Count; index++)
            {
                if (string.Equals(member.Name, declared[index], StringComparison.Ordinal))
                {
                    known = true;

                    break;
                }
            }

            if (!known)
            {
                violations.Add($"the member '{member.Name}' is not one this stage declares");
            }
        }
    }

    /// <summary>Renders the kind of a JSON value for a diagnostic.</summary>
    /// <param name="value">The value.</param>
    /// <returns>An article and a noun, such as <c>a string</c>.</returns>
    private static string Describe(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.Object => "an object",
        JsonValueKind.Array => "an array",
        JsonValueKind.String => "a string",
        JsonValueKind.Number => "a number",
        JsonValueKind.True or JsonValueKind.False => "a boolean",
        _ => "null",
    };
}
