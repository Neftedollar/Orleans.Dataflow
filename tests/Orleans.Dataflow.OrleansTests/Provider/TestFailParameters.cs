using System.Globalization;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.OrleansTests.Provider;

/// <summary>
/// How the test failing flow states which element makes it throw.
/// </summary>
/// <remarks>
/// One member and no validator, which is deliberate: a stage that declares no parameter validator is a
/// legitimate shape, and having one such stage in the vocabulary keeps the compiler's "validated only when
/// a validator exists" path exercised alongside the range source's validated one.
/// </remarks>
internal static class TestFailParameters
{
    /// <summary>The payload member holding the element to fail at.</summary>
    internal const string AtMember = "at";

    /// <summary>Writes the payload of a flow that fails at one element.</summary>
    /// <param name="at">The element to fail at.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(long at) =>
        CanonicalJsonValue.Parse(string.Create(CultureInfo.InvariantCulture, $"{{\"{AtMember}\":{at}}}"));
}
