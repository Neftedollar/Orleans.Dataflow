using Xunit;

namespace Orleans.Dataflow.Tests.Identity;

/// <summary>
/// The behavioural contract every single-segment identifier must satisfy.
/// </summary>
/// <typeparam name="TIdentifier">The identifier type under test.</typeparam>
/// <remarks>
/// The rules live here once and run against every identifier type through a derived class, so a new
/// identifier cannot be added with a weaker grammar, a nullable <c>Value</c>, or a throwing
/// <c>ToString</c> without a test failing.
/// </remarks>
public abstract class SimpleIdentifierContractTests<TIdentifier>
    where TIdentifier : struct, IEquatable<TIdentifier>
{
    /// <summary>The parameter name every identifier <c>Create</c> factory must report on failure.</summary>
    private const string FactoryParameterName = "value";

    /// <summary>Gets the identifier type name, as it must appear in diagnostics.</summary>
    protected abstract string IdentifierName { get; }

    [Theory]
    [InlineData("a")]
    [InlineData("z")]
    [InlineData("0")]
    [InlineData("abc")]
    [InlineData("a1")]
    [InlineData("1a")]
    [InlineData("a-b")]
    [InlineData("orders-import")]
    [InlineData("a1-b2-c3")]
    [InlineData("provider-1")]
    public void CreateRoundTripsValidSegment(string value)
    {
        TIdentifier identifier = Create(value);

        Assert.Equal(value, ValueOf(identifier));
        Assert.Equal(value, identifier.ToString());
        Assert.False(IsDefaultOf(identifier));
    }

    [Fact]
    public void CreateAcceptsMaximumLengthSegment()
    {
        string value = new('a', 64);

        Assert.Equal(value, ValueOf(Create(value)));
    }

    [Fact]
    public void CreateRejectsNull()
    {
        Assert.Throws<ArgumentNullException>(FactoryParameterName, () => { _ = Create(null!); });
    }

    [Theory]
    [InlineData("")]
    [InlineData("-")]
    [InlineData("-a")]
    [InlineData("a-")]
    [InlineData("a--b")]
    [InlineData("A")]
    [InlineData("aB")]
    [InlineData("Abc")]
    [InlineData("a b")]
    [InlineData(" a")]
    [InlineData("a\tb")]
    [InlineData("a.b")]
    [InlineData("a/b")]
    [InlineData("a_b")]
    [InlineData("a:b")]
    [InlineData("é")]
    [InlineData("café")]
    [InlineData("İ")]
    [InlineData("ı")]
    [InlineData("заказ")]
    [InlineData("a\U0001F600b")]
    public void CreateRejectsInvalidSegment(string value)
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(FactoryParameterName, () => { _ = Create(value); });

        Assert.Contains(value, exception.Message, StringComparison.Ordinal);
        Assert.Contains(IdentifierName, exception.Message, StringComparison.Ordinal);
        Assert.Contains("[a-z0-9]+(-[a-z0-9]+)*", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsOverLengthSegment()
    {
        string value = new('a', 65);

        ArgumentException exception = Assert.Throws<ArgumentException>(FactoryParameterName, () => { _ = Create(value); });

        Assert.Contains(value, exception.Message, StringComparison.Ordinal);
        Assert.Contains("65", exception.Message, StringComparison.Ordinal);
        Assert.Contains("64", exception.Message, StringComparison.Ordinal);
        Assert.False(TryCreate(value, out TIdentifier identifier));
        Assert.True(IsDefaultOf(identifier));
    }

    [Theory]
    [InlineData("a")]
    [InlineData("0")]
    [InlineData("a-b")]
    [InlineData("orders-import")]
    public void TryCreateAcceptsValidSegment(string value)
    {
        Assert.True(TryCreate(value, out TIdentifier identifier));
        Assert.Equal(value, ValueOf(identifier));
        Assert.False(IsDefaultOf(identifier));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("-")]
    [InlineData("-a")]
    [InlineData("a-")]
    [InlineData("a--b")]
    [InlineData("A")]
    [InlineData("aB")]
    [InlineData("a b")]
    [InlineData("a.b")]
    [InlineData("a/b")]
    [InlineData("a_b")]
    [InlineData("é")]
    [InlineData("İ")]
    [InlineData("заказ")]
    public void TryCreateRejectsInvalidSegmentWithoutThrowing(string? value)
    {
        Assert.False(TryCreate(value, out TIdentifier identifier));
        Assert.True(IsDefaultOf(identifier));
    }

    [Fact]
    public void EqualValuesAreEqualAndShareHashCode()
    {
        TIdentifier left = Create("shared-value");
        TIdentifier right = Create("shared-value");

        Assert.Equal(left, right);
        Assert.True(left.Equals(right));
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.True(OperatorEquals(left, right));
        Assert.False(OperatorNotEquals(left, right));
    }

    [Fact]
    public void BoxedEqualityMatchesValueEquality()
    {
        object left = Create("shared-value");
        object right = Create("shared-value");

        Assert.True(left.Equals(right));
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.False(left.Equals(Create("other-value")));
    }

    [Fact]
    public void DifferentValuesAreNotEqual()
    {
        TIdentifier left = Create("alpha");
        TIdentifier right = Create("beta");

        Assert.NotEqual(left, right);
        Assert.False(left.Equals(right));
        Assert.False(OperatorEquals(left, right));
        Assert.True(OperatorNotEquals(left, right));
    }

    [Fact]
    public void DefaultInstanceIsDefault()
    {
        Assert.True(IsDefaultOf(default));
        Assert.True(IsDefaultOf(new TIdentifier()));
        Assert.Equal(default, new TIdentifier());
    }

    [Fact]
    public void DefaultInstanceValueThrowsInvalidOperationException()
    {
        TIdentifier identifier = default;

        InvalidOperationException exception =
            Assert.Throws<InvalidOperationException>(() => { _ = ValueOf(identifier); });

        Assert.Contains(IdentifierName, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DefaultInstanceToStringIsDiagnosticLiteralAndDoesNotThrow()
    {
        TIdentifier identifier = default;

        Assert.Equal($"(default {IdentifierName})", identifier.ToString());
    }

    [Fact]
    public void DefaultInstancesAreEqual()
    {
        TIdentifier left = default;
        TIdentifier right = default;

        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.True(OperatorEquals(left, right));
    }

    [Fact]
    public void DefaultInstanceIsUsableInAHashSet()
    {
        HashSet<TIdentifier> identifiers = [default, Create("alpha"), default, Create("alpha")];

        Assert.Equal(2, identifiers.Count);
    }

    [Fact]
    public void CreatedInstanceIsNotEqualToDefault()
    {
        TIdentifier created = Create("alpha");

        Assert.NotEqual(default, created);
        Assert.False(OperatorEquals(default, created));
        Assert.True(OperatorNotEquals(default, created));
    }

    /// <summary>Calls the identifier's <c>Create</c> factory.</summary>
    /// <param name="value">The candidate segment.</param>
    /// <returns>The created identifier.</returns>
    protected abstract TIdentifier Create(string value);

    /// <summary>Calls the identifier's <c>TryCreate</c> factory.</summary>
    /// <param name="value">The candidate segment.</param>
    /// <param name="identifier">The created identifier, or the default value.</param>
    /// <returns><see langword="true"/> when the segment is valid.</returns>
    protected abstract bool TryCreate(string? value, out TIdentifier identifier);

    /// <summary>Reads the identifier's <c>Value</c> property.</summary>
    /// <param name="identifier">The identifier to read.</param>
    /// <returns>The identifier text.</returns>
    protected abstract string ValueOf(TIdentifier identifier);

    /// <summary>Reads the identifier's <c>IsDefault</c> property.</summary>
    /// <param name="identifier">The identifier to inspect.</param>
    /// <returns><see langword="true"/> for the default value.</returns>
    protected abstract bool IsDefaultOf(TIdentifier identifier);

    /// <summary>Applies the identifier's <c>==</c> operator.</summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>The operator result.</returns>
    protected abstract bool OperatorEquals(TIdentifier left, TIdentifier right);

    /// <summary>Applies the identifier's <c>!=</c> operator.</summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>The operator result.</returns>
    protected abstract bool OperatorNotEquals(TIdentifier left, TIdentifier right);
}
