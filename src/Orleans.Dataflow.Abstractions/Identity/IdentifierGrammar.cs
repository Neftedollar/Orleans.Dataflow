using System.Globalization;

namespace Orleans.Dataflow.Identity;

/// <summary>
/// The lexical grammar shared by every stable Orleans.Dataflow identifier.
/// </summary>
/// <remarks>
/// <para>
/// A <em>segment</em> is the atom of every identifier: <c>[a-z0-9]+(-[a-z0-9]+)*</c> with a length of
/// 1 to <see cref="MaxSegmentLength"/> characters. Lowercase ASCII letters and ASCII digits are the
/// only allowed characters, and a hyphen is allowed only as an interior word separator: no leading or
/// trailing hyphen, no consecutive hyphens, no uppercase, no dots, slashes, underscores, whitespace,
/// or non-ASCII characters.
/// </para>
/// <para>
/// Two properties of this grammar are deliberate. Lowercase is the only accepted casing, so two
/// distinct identifiers can never collide in a case-insensitive store or URL path. Validation compares
/// explicit ordinal character ranges rather than calling culture-sensitive helpers such as
/// <see cref="char.IsLetter(char)"/>, so the same text is accepted or rejected identically under every
/// ambient culture, including cultures with non-invariant casing rules such as <c>tr-TR</c>.
/// </para>
/// <para>
/// The grammar starts strict on purpose: relaxing it later accepts identifiers that were previously
/// rejected and is therefore a compatible change, while tightening it would invalidate identifiers
/// already written into durable graph documents and is never allowed.
/// </para>
/// </remarks>
internal static class IdentifierGrammar
{
    /// <summary>
    /// The maximum length, in characters, of a single identifier segment.
    /// </summary>
    internal const int MaxSegmentLength = 64;

    /// <summary>
    /// The identifier segment grammar in regular-expression notation, for diagnostics and documentation.
    /// </summary>
    /// <remarks>
    /// Validation is implemented as explicit character-range checks rather than by running this pattern;
    /// the constant exists so that error messages can quote one authoritative spelling of the rule.
    /// </remarks>
    internal const string SegmentGrammar = "[a-z0-9]+(-[a-z0-9]+)*";

    /// <summary>
    /// Determines whether <paramref name="value"/> is a valid identifier segment.
    /// </summary>
    /// <param name="value">The candidate segment.</param>
    /// <returns><see langword="true"/> when the segment is valid; otherwise <see langword="false"/>.</returns>
    internal static bool IsSegment(ReadOnlySpan<char> value) => DescribeSegmentViolation(value) is null;

    /// <summary>
    /// Describes the first grammar rule that <paramref name="value"/> violates.
    /// </summary>
    /// <param name="value">The candidate segment.</param>
    /// <returns>
    /// A lower-case sentence fragment naming the violated rule, or <see langword="null"/> when the
    /// segment is valid.
    /// </returns>
    internal static string? DescribeSegmentViolation(ReadOnlySpan<char> value)
    {
        if (value.Length == 0)
        {
            return "the value is empty";
        }

        if (value.Length > MaxSegmentLength)
        {
            return string.Create(
                CultureInfo.InvariantCulture,
                $"the value is {value.Length} characters long, which exceeds the maximum of {MaxSegmentLength}");
        }

        if (value[0] == '-')
        {
            return "the value starts with a hyphen";
        }

        if (value[^1] == '-')
        {
            return "the value ends with a hyphen";
        }

        for (int index = 0; index < value.Length; index++)
        {
            char character = value[index];

            if (character == '-')
            {
                if (value[index - 1] == '-')
                {
                    return string.Create(
                        CultureInfo.InvariantCulture,
                        $"the value contains consecutive hyphens at index {index - 1}");
                }

                continue;
            }

            if (!IsSegmentCharacter(character))
            {
                return string.Create(
                    CultureInfo.InvariantCulture,
                    $"the character '{character}' at index {index} is not a lowercase ASCII letter, an ASCII digit, or an interior hyphen");
            }
        }

        return null;
    }

    /// <summary>
    /// Validates <paramref name="value"/> as an identifier segment and throws when it is not one.
    /// </summary>
    /// <param name="value">The candidate segment.</param>
    /// <param name="identifierName">The identifier name to quote in the error message, such as <c>ProviderId</c>.</param>
    /// <param name="parameterName">The parameter name to report on the thrown exception.</param>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="value"/> is not a valid identifier segment.</exception>
    internal static void EnsureSegment(string value, string identifierName, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(value, parameterName);

        string? violation = DescribeSegmentViolation(value);

        if (violation is not null)
        {
            throw new ArgumentException(FormatSegmentError(value, identifierName, violation), parameterName);
        }
    }

    /// <summary>
    /// Builds the message for a rejected identifier segment.
    /// </summary>
    /// <param name="value">The rejected value, quoted into the message.</param>
    /// <param name="identifierName">The identifier name, such as <c>ProviderId</c>.</param>
    /// <param name="violation">The violated rule, as returned by <see cref="DescribeSegmentViolation"/>.</param>
    /// <returns>A message naming the offending value and the rule it breaks.</returns>
    internal static string FormatSegmentError(ReadOnlySpan<char> value, string identifierName, string violation) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"'{value}' is not a valid {identifierName}: {violation}. An identifier segment must match {SegmentGrammar} and be 1 to {MaxSegmentLength} characters long.");

    /// <summary>
    /// Builds the message for reading a value out of a default identifier instance.
    /// </summary>
    /// <param name="identifierName">The identifier name, such as <c>ProviderId</c>.</param>
    /// <returns>A message explaining that the instance was never created through a factory method.</returns>
    internal static string DescribeDefaultAccess(string identifierName) =>
        $"The default {identifierName} carries no value. Obtain an instance from a {identifierName} factory method instead of using the uninitialized struct.";

    /// <summary>
    /// Determines whether <paramref name="character"/> is a lowercase ASCII letter or an ASCII digit.
    /// </summary>
    /// <param name="character">The character to classify.</param>
    /// <returns><see langword="true"/> for <c>a</c>-<c>z</c> and <c>0</c>-<c>9</c>; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// The ranges are compared explicitly so that classification is ordinal and cannot vary with the
    /// ambient culture.
    /// </remarks>
    private static bool IsSegmentCharacter(char character) =>
        character is >= 'a' and <= 'z' or >= '0' and <= '9';
}
