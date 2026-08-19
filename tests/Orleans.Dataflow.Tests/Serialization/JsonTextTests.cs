using System.Globalization;
using System.Text;
using System.Text.Json;
using Orleans.Dataflow.Adapters;
using Xunit;

namespace Orleans.Dataflow.Tests.Serialization;

/// <summary>
/// Pins the one claim <see cref="JsonText.Quote"/> makes: for every string it writes what
/// <see cref="JsonSerializer"/> would have written, byte for byte.
/// </summary>
/// <remarks>
/// <para>
/// The claim is what let twenty-nine payload-writing sites drop a reflection-based serializer they were
/// using only as a string escaper, and every one of those sites feeds a
/// <see cref="Orleans.Dataflow.Serialization.CanonicalJsonValue"/> that a graph fingerprint is taken over.
/// So this is a compatibility test in the same sense the golden fixtures are: if it fails, documents that
/// used to be one fingerprint are now two, and the fix is never to adjust the expectation.
/// </para>
/// <para>
/// <b>The hostile set was built from a real divergence rather than from imagination.</b> Encoding refuses
/// the input the serializer silently substitutes into — an unpaired surrogate — so the two disagreed on
/// four of the curated inputs and on fifteen percent of a random sweep before the helper learned to perform
/// the substitution itself. Those four inputs are the reason the rest of the file exists, and they are kept
/// first among equals.
/// </para>
/// <para>
/// <b>The sweep is seeded on purpose.</b> An unseeded one would be a different test on every run: a failure
/// nobody can reproduce is a failure nobody fixes, and a suite that is green because it happened to draw
/// easy inputs has proved nothing. The seed is fixed, the alphabets are stated, and the second sweep draws
/// mostly surrogates so that the substitution path is exercised thousands of times rather than incidentally.
/// </para>
/// </remarks>
public sealed class JsonTextTests
{
    /// <summary>The seed every sweep here draws from, fixed so that a failure reproduces exactly.</summary>
    private const int Seed = 20260819;

    /// <summary>How many strings each sweep draws.</summary>
    private const int SweepSize = 4000;

    /// <summary>The inputs chosen to break the equivalence, one class of hostility each.</summary>
    /// <returns>The hostile set.</returns>
    /// <remarks>
    /// <para>
    /// Escapes and controls because those are what an escape table is; non-ASCII and astral text because
    /// transcoding is where a wrong answer hides; the HTML-sensitive characters because the default encoder
    /// escapes them and a different encoder would not; unpaired surrogates because that is the case that
    /// actually diverged; and strings past two hundred and fifty-six characters with escapes and surrogate
    /// pairs straddling the boundary, because an internal buffer boundary is where a fast path stops
    /// agreeing with a slow one.
    /// </para>
    /// <para>
    /// A plain array walked by a <c>[Fact]</c> rather than theory data. Two reasons, and the first one was
    /// measured rather than assumed: an unpaired surrogate has no UTF-8 form, so a theory argument carrying
    /// one is at the mercy of how a runner transports it to the test — on the pinned xUnit, running
    /// in process, the code units arrive intact, which is a fact about today's runner and not a property of
    /// theory data. The second reason is the one that decides it: the set has to be counted as a whole to
    /// assert that it still contains the case that once diverged, and that assertion has nowhere to live in
    /// a per-row theory. The failure message names the code points, so nothing is lost but a row in the
    /// runner.
    /// </para>
    /// </remarks>
    private static string[] HostileStrings() =>
    [
        // The four that diverged before the helper substituted for them.
        "a\ud83db",
        "a\udc00b",
        "ab\ud83d",
        "\udc00ab",

        string.Empty,
        "hello world",
        "a\"b",
        "a\\b",
        "a/b",
        "a\nb",
        "a\rb",
        "a\tb",
        "a\0b",
        "a\bb",
        "a\fb",
        new string([.. Enumerable.Range(0, 32).Select(code => (char)code)]),
        new string([.. Enumerable.Range(0x7f, 0x21).Select(code => (char)code)]),
        "café",
        "你好",
        "\U0001F600",
        "<a href=\"x\">&amp;</a> + ' & >",
        "a+b'c",
        "a`b",
        "a\u2028b",
        "a\u2029b",
        "a\ufffdb",
        "a\u00a0b",
        "a\u200fb",
        Sprinkled(600),
        new string('x', 700),
        Repeated("\"\\\n\t", 200),
        Repeated("你好\U0001F600", 150),
        Repeated("<&>'+", 120),
        new string('a', 255),
        new string('a', 256),
        new string('a', 257),
        new string('a', 255) + "\"",
        new string('a', 256) + "\"",
        new string('a', 256) + "\n" + new string('b', 10),
        new string('a', 255) + "\U0001F600" + new string('b', 10),
        new string('a', 256) + "\U0001F600" + new string('b', 10),
    ];

    [Fact]
    public void QuoteWritesWhatTheSerializerWritesForEveryHostileInput()
    {
        string[] hostile = HostileStrings();

        foreach (string value in hostile)
        {
            AssertAgrees(value);
        }

        // The set is only hostile if it still contains the case that once diverged. A future tidy-up that
        // dropped or repaired the unpaired-surrogate rows would leave every assertion above passing over
        // inputs that were never the problem, and this is what refuses to let that happen quietly.
        Assert.Equal(4, hostile.Count(value => !JsonText.IsWellFormed(value)));
    }

    [Fact]
    public void QuoteWritesWhatTheSerializerWritesAcrossASeededSweepOfTheWholePlane()
    {
        Random random = new(Seed);
        int checkedCount = 0;

        for (int trial = 0; trial < SweepSize; trial++)
        {
            string value = new([.. Enumerable
                .Range(0, random.Next(0, 12))
                .Select(_ => (char)random.Next(0, 0x10000))]);

            AssertAgrees(value);
            checkedCount++;
        }

        Assert.Equal(SweepSize, checkedCount);
    }

    [Fact]
    public void QuoteWritesWhatTheSerializerWritesAcrossASeededSweepOfMostlySurrogates()
    {
        // Eight high surrogates, eight low ones, and four ordinary characters, so that runs of consecutive
        // unpaired surrogates and pairs split by an ordinary character both occur constantly. Without an
        // alphabet like this a uniform sweep leaves the substitution path barely touched.
        char[] alphabet =
        [
            .. Enumerable.Range(0xd800, 8).Select(code => (char)code),
            .. Enumerable.Range(0xdc00, 8).Select(code => (char)code),
            'a', '"', '\n', 'é',
        ];

        Random random = new(Seed);
        int illFormed = 0;

        for (int trial = 0; trial < SweepSize; trial++)
        {
            string value = new([.. Enumerable
                .Range(0, random.Next(0, 9))
                .Select(_ => alphabet[random.Next(alphabet.Length)])]);

            AssertAgrees(value);

            if (!JsonText.IsWellFormed(value))
            {
                illFormed++;
            }
        }

        // The sweep is only evidence if it actually reached the substitution path, so it says how often it
        // did. A change that made every drawn string well-formed would leave the assertions above passing
        // over inputs that prove nothing, and this is what would catch that.
        Assert.True(
            illFormed > SweepSize / 2,
            string.Create(
                CultureInfo.InvariantCulture,
                $"Only {illFormed} of {SweepSize} drawn strings were ill-formed, so the substitution path was barely exercised."));
    }

    [Fact]
    public void IsWellFormedAnswersForEachShapeOfSurrogate()
    {
        // Inline rather than theory data for the reason the hostile set is: an unpaired surrogate has no
        // UTF-8 form, and half of these cases are unpaired surrogates.
        (string Value, bool Expected)[] cases =
        [
            ("", true),
            ("plain", true),
            ("\u4f60\u597d", true),
            ("\U0001F600", true),  // a well-formed pair
            ("a\ufffdb", true),  // the replacement character itself is ordinary text
            ("\U0001F600\U0001F600", true),  // two pairs running together
            ("a\U0001F600b", true),  // a pair between ordinary characters
            ("\ud83d", false),  // a high surrogate with nothing after it
            ("\udc00", false),  // a low surrogate with nothing before it
            ("\udc00\ud83d", false),  // a pair in the wrong order
            ("\ud83da", false),  // a high surrogate followed by ordinary text
            ("\ud83d\U0001F600", false),  // a high surrogate followed by a well-formed pair
            ("a\U0001F600\udc00", false),  // an unpaired low surrogate after a well-formed pair
        ];

        foreach ((string value, bool expected) in cases)
        {
            Assert.Equal(expected, JsonText.IsWellFormed(value));
        }
    }

    /// <summary>Asserts the two spellings agree, in both the UTF-16 and the UTF-8 form.</summary>
    /// <param name="value">The string to write both ways.</param>
    private static void AssertAgrees(string value)
    {
        string expected = JsonSerializer.Serialize(value);
        string actual = JsonText.Quote(value);

        if (!string.Equals(expected, actual, StringComparison.Ordinal))
        {
            Assert.Fail(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"The two spellings disagree for the code points [{string.Join(",", value.Select(character => ((int)character).ToString("X4", CultureInfo.InvariantCulture)))}]. The serializer wrote {expected} and the helper wrote {actual}."));
        }

        Assert.Equal(JsonSerializer.SerializeToUtf8Bytes(value), Encoding.UTF8.GetBytes(actual));
    }

    /// <summary>Builds a string of at least one length from an alphabet of everything that escapes.</summary>
    /// <param name="length">The least length to reach.</param>
    /// <returns>The string.</returns>
    private static string Sprinkled(int length)
    {
        const string Alphabet = "abc\"\\\n\t<&>é你";
        StringBuilder built = new();

        for (int index = 0; built.Length < length; index++)
        {
            _ = built.Append(Alphabet[index % Alphabet.Length]);
        }

        return built.ToString();
    }

    /// <summary>Repeats one fragment a number of times.</summary>
    /// <param name="fragment">The fragment.</param>
    /// <param name="count">How many times to repeat it.</param>
    /// <returns>The string.</returns>
    private static string Repeated(string fragment, int count) =>
        string.Concat(Enumerable.Repeat(fragment, count));
}
