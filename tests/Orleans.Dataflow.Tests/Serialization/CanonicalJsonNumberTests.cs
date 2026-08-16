using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.Tests.Serialization;

/// <summary>
/// Tests for the integer-only number rule of <see cref="CanonicalJsonValue"/>.
/// </summary>
/// <remarks>
/// Canonical JSON admits only integers that fit in <see cref="long"/>, because floating-point formatting
/// is the classic determinism trap across runtimes and cultures (ADR 0003).
/// </remarks>
public sealed class CanonicalJsonNumberTests
{
    /// <summary>The parameter name the API reports for the <c>json</c> argument.</summary>
    private const string JsonParameterName = "json";

    [Theory]
    [InlineData("0", "0")]
    [InlineData("-0", "0")]
    [InlineData("1234", "1234")]
    [InlineData("-1234", "-1234")]
    [InlineData("9223372036854775807", "9223372036854775807")]
    [InlineData("-9223372036854775808", "-9223372036854775808")]
    public void ParseCanonicalizesInteger(string json, string expected) =>
        Assert.Equal(expected, CanonicalJsonValue.Parse(json).ToString());

    [Theory]
    [InlineData("""{"a":-0}""", """{"a":0}""")]
    [InlineData("[-0,0]", "[0,0]")]
    public void NegativeZeroCanonicalizesToZeroInsideContainers(string json, string expected) =>
        Assert.Equal(expected, CanonicalJsonValue.Parse(json).ToString());

    [Fact]
    public void NegativeZeroEqualsZero() =>
        Assert.Equal(CanonicalJsonValue.Parse("0"), CanonicalJsonValue.Parse("-0"));

    [Theory]
    [InlineData("1.5")]
    [InlineData("1.0")]
    [InlineData("0.0")]
    [InlineData("-0.0")]
    [InlineData("1e3")]
    [InlineData("1E-2")]
    [InlineData("1e+3")]
    [InlineData("-2.5e10")]
    public void ParseRejectsNumberWithFractionOrExponent(string json)
    {
        ArgumentException exception =
            Assert.Throws<ArgumentException>(JsonParameterName, () => { _ = CanonicalJsonValue.Parse(json); });

        Assert.Contains(json, exception.Message, StringComparison.Ordinal);
        Assert.Contains("integer", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("9223372036854775808")]
    [InlineData("-9223372036854775809")]
    [InlineData("99999999999999999999999999999999")]
    [InlineData("-99999999999999999999999999999999")]
    public void ParseRejectsIntegerOutsideTheSignedSixtyFourBitRange(string json)
    {
        ArgumentException exception =
            Assert.Throws<ArgumentException>(JsonParameterName, () => { _ = CanonicalJsonValue.Parse(json); });

        Assert.Contains(json, exception.Message, StringComparison.Ordinal);
        Assert.Contains("64-bit", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("""{"a":1.5}""")]
    [InlineData("[1,2,3.5]")]
    [InlineData("""{"a":{"b":[9223372036854775808]}}""")]
    public void ParseRejectsANonCanonicalNumberAnywhereInTheValue(string json) =>
        Assert.Throws<ArgumentException>(JsonParameterName, () => { _ = CanonicalJsonValue.Parse(json); });

    [Theory]
    [InlineData("007")]
    [InlineData("01")]
    [InlineData("-01")]
    [InlineData("+1")]
    [InlineData(".5")]
    [InlineData("1.")]
    [InlineData("1e")]
    [InlineData("-")]
    [InlineData("NaN")]
    [InlineData("Infinity")]
    [InlineData("-Infinity")]
    [InlineData("0x1")]
    public void ParseThrowsJsonExceptionForMalformedNumber(string json) =>
        Assert.ThrowsAny<JsonException>(() => { _ = CanonicalJsonValue.Parse(json); });

    [Fact]
    public void NumbersFormatInvariantlyUnderAHostileCulture()
    {
        // A culture may spell the negative sign with anything at all, and the default number formatting
        // provider is the ambient culture. The canonical writer must not consult it.
        string? actual = null;
        string? hostileControl = null;

        Thread worker = new(() =>
        {
            CultureInfo culture = (CultureInfo)CultureInfo.InvariantCulture.Clone();
            culture.NumberFormat.NegativeSign = "MINUS";
            CultureInfo.CurrentCulture = culture;

            hostileControl = (-42L).ToString(CultureInfo.CurrentCulture);
            actual = CanonicalJsonValue.Parse("""{"a":-42}""").ToString();
        });

        worker.Start();
        worker.Join();

        // The control proves the hostile culture really was in force on that thread.
        Assert.Equal("MINUS42", hostileControl);
        Assert.Equal("""{"a":-42}""", actual);
    }

    [Fact]
    public void LargeMagnitudeBoundariesRoundTripThroughEveryEntryPoint()
    {
        foreach (long boundary in new[] { long.MinValue, long.MaxValue, 0L, -1L })
        {
            string expected = boundary.ToString(CultureInfo.InvariantCulture);
            CanonicalJsonValue value = CanonicalJsonValue.Parse(expected);

            Assert.Equal(expected, value.ToString());
            Assert.Equal(value, CanonicalJsonValue.Parse(value.CanonicalUtf8Bytes.Span));
            Assert.Equal(value, CanonicalJsonValue.FromElement(value.ToElement()));
            Assert.Equal(boundary, value.ToElement().GetInt64());
        }
    }
}
