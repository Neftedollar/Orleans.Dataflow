using System.Diagnostics;
using System.Text;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.Tests.Serialization;

/// <summary>
/// What a canonical value costs to refuse, which is a different question from what it is allowed to be.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="CanonicalJsonValueTests"/> and <see cref="CanonicalJsonStructureTests"/> say what the
/// canonical form admits. This says what the parser is willing to spend finding out. The size rule is
/// enforced on the canonical form, so applying it means writing the canonical form, which means parsing the
/// whole input first: before M8.4 a 76-million-character document was materialized as a
/// <c>JsonDocument</c> — over a gigabyte on one thread — and only then refused for being over 256 KiB. The
/// cap was real and arrived after the memory was spent.
/// </para>
/// <para>
/// So the assertions here are about <em>cost</em> and are written as such: what is measured is allocation
/// and not wall-clock, because a machine under load can be slow for reasons that have nothing to do with a
/// parser, while a parse that allocates a bounded number of bytes has provably not built a document over an
/// unbounded input. <see cref="GC.GetAllocatedBytesForCurrentThread"/> is exact for the thread that made
/// the allocations, and every parse below is made on the thread that measures it.
/// </para>
/// <para>
/// <b>The ceiling is deliberately far above the canonical limit</b>, and one test here is entirely about
/// that gap: an input several times 256 KiB that shrinks under it — whitespace, escapes — is still a value
/// this library accepts, and a check placed at the canonical bound would have rejected documents that used
/// to work. The two tests are a pair and neither alone says the bound is right.
/// </para>
/// </remarks>
public sealed class CanonicalLimitsTests
{
    /// <summary>The size limit of the canonical form, which the input ceiling is a multiple of.</summary>
    private const int CanonicalLimit = 262144;

    /// <summary>The parameter name the text overload reports its refusals under.</summary>
    private const string JsonParameterName = "json";

    /// <summary>The parameter name the UTF-8 overload reports its refusals under.</summary>
    private const string Utf8ParameterName = "utf8Json";

    [Fact]
    public void AnInputFarPastTheCanonicalLimitIsRefusedWithoutBuildingADocumentForIt()
    {
        // Sixteen mebibytes of short strings in an array: well-formed JSON, and a canonical form that could
        // never fit, so the only question is whether the refusal is reached before or after the document.
        string huge = "[" + string.Join(',', Enumerable.Repeat("\"aaaaaaaaaaaaaaaa\"", 1_000_000)) + "]";

        Assert.True(huge.Length > 16 * 1024 * 1024);

        long before = GC.GetAllocatedBytesForCurrentThread();
        Stopwatch clock = Stopwatch.StartNew();

        ArgumentException refused =
            Assert.Throws<ArgumentException>(JsonParameterName, () => { _ = CanonicalJsonValue.Parse(huge); });

        clock.Stop();

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        // The whole refusal is a length comparison and a message, so what it may allocate is the message.
        // A single kibibyte is generous for that and nowhere near a transcoding of the input, which alone
        // would be sixteen mebibytes, let alone the document over it.
        Assert.True(
            allocated < 1024 * 1024,
            $"refusing a {huge.Length:N0}-character input allocated {allocated:N0} bytes on this thread in {clock.ElapsedMilliseconds} ms");

        Assert.Contains("4194304", refused.Message, StringComparison.Ordinal);
        Assert.Contains("4 MiB", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheUtf8OverloadRefusesOnLengthBeforeItCopiesTheSpan()
    {
        byte[] huge = Encoding.UTF8.GetBytes(
            "[" + string.Join(',', Enumerable.Repeat("\"aaaaaaaaaaaaaaaa\"", 1_000_000)) + "]");

        long before = GC.GetAllocatedBytesForCurrentThread();

        ArgumentException refused = Assert.Throws<ArgumentException>(
            Utf8ParameterName,
            () => { _ = CanonicalJsonValue.Parse(huge.AsSpan()); });

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        // The span overload copies its input, because canonicalization holds the bytes past the call. The
        // copy is what this asserts is not made: sixteen mebibytes in, under a mebibyte allocated.
        Assert.True(
            allocated < 1024 * 1024,
            $"refusing a {huge.Length:N0}-byte span allocated {allocated:N0} bytes on this thread");

        Assert.Contains("4194304", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRefusalReportsTheLengthItSawAndBothBounds()
    {
        string huge = new('a', (16 * CanonicalLimit) + 1);

        ArgumentException refused = Assert.Throws<ArgumentException>(
            JsonParameterName,
            () => { _ = CanonicalJsonValue.Parse("\"" + huge + "\""); });

        // Three numbers and each earns its place: what arrived, the ceiling it passed, and the canonical
        // limit the ceiling stands in front of — so a reader can tell a payload that grew from a payload
        // that was never going to fit.
        Assert.Contains("4194307", refused.Message, StringComparison.Ordinal);
        Assert.Contains("4194304", refused.Message, StringComparison.Ordinal);
        Assert.Contains("262144", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnInputWellPastTheCanonicalLimitThatShrinksUnderItIsStillAccepted()
    {
        // The point of the gap between the two bounds. Every character is written as a six-character
        // escape, so the input is six times the canonical form it produces: 1,200,000 characters in,
        // 200,002 canonical bytes out, which is under 256 KiB and therefore a value this library has always
        // accepted. A ceiling placed at the canonical limit would have refused it.
        const int Characters = 200_000;

        string escaped = "\"" + string.Concat(Enumerable.Repeat("\\u0061", Characters)) + "\"";

        Assert.True(escaped.Length > 4 * CanonicalLimit);

        CanonicalJsonValue value = CanonicalJsonValue.Parse(escaped);

        Assert.Equal(Characters + 2, value.ByteLength);
        Assert.True(value.ByteLength < CanonicalLimit);
    }

    [Fact]
    public void AnInputAtTheCeilingIsStillParsedAndJudgedByTheCanonicalRules()
    {
        // Exactly at the ceiling, so the length check passes and the canonical size rule is what answers.
        // This is the boundary that says the new check is a floor under the old one rather than a
        // replacement for it: the message names the canonical limit, not the ceiling.
        string atCeiling = "\"" + new string('a', (16 * CanonicalLimit) - 2) + "\"";

        Assert.Equal(16 * CanonicalLimit, atCeiling.Length);

        ArgumentException refused = Assert.Throws<ArgumentException>(
            JsonParameterName,
            () => { _ = CanonicalJsonValue.Parse(atCeiling); });

        Assert.Contains("canonical form of the value exceeds", refused.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("4 MiB", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheNonThrowingEntryPointsReportTheCeilingAsAnOrdinaryRefusal()
    {
        string huge = new('a', (16 * CanonicalLimit) + 1);
        string json = "\"" + huge + "\"";

        // TryParse promises never to throw, for any input at all, and a bound that escaped through it would
        // be the one refusal a caller could not opt out of.
        Assert.False(CanonicalJsonValue.TryParse(json, out CanonicalJsonValue fromText));
        Assert.True(fromText.IsDefault);

        Assert.False(CanonicalJsonValue.TryParse(Encoding.UTF8.GetBytes(json), out CanonicalJsonValue fromBytes));
        Assert.True(fromBytes.IsDefault);
    }

    [Fact]
    public void AValueOfOrdinarySizeIsUntouchedByTheCeiling()
    {
        // The regression guard the other tests cannot be: nothing about an ordinary payload changed, and
        // the canonical form of one is still byte-for-byte what it was.
        CanonicalJsonValue value = CanonicalJsonValue.Parse("""  { "b" : 2 , "a" : [ 1 , 2 ] }  """);

        Assert.Equal("""{"a":[1,2],"b":2}""", value.ToString());
    }
}
