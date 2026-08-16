using System.Globalization;
using Orleans.Dataflow.Identity;
using Xunit;

namespace Orleans.Dataflow.Tests.Identity;

/// <summary>
/// Tests for <see cref="GraphRevision"/>.
/// </summary>
public sealed class GraphRevisionTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(42)]
    [InlineData(int.MaxValue)]
    public void CreateRoundTripsPositiveRevision(int revisionNumber)
    {
        GraphRevision revision = GraphRevision.Create(revisionNumber);

        Assert.Equal(revisionNumber, revision.Value);
        Assert.False(revision.IsDefault);
        Assert.Equal(revisionNumber.ToString(CultureInfo.InvariantCulture), revision.ToString());
    }

    [Fact]
    public void FirstRevisionNumberIsOne()
    {
        Assert.Equal(1, GraphRevision.FirstRevisionNumber);
        Assert.Equal(1, GraphRevision.Create(GraphRevision.FirstRevisionNumber).Value);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-42)]
    [InlineData(int.MinValue)]
    public void CreateRejectsNonPositiveRevision(int revisionNumber)
    {
        ArgumentException exception =
            Assert.ThrowsAny<ArgumentException>(() => { _ = GraphRevision.Create(revisionNumber); });

        Assert.IsType<ArgumentOutOfRangeException>(exception);
        Assert.Equal("value", exception.ParamName);
        Assert.Contains(
            revisionNumber.ToString(CultureInfo.InvariantCulture),
            exception.Message,
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(int.MaxValue)]
    public void TryCreateAcceptsPositiveRevision(int revisionNumber)
    {
        Assert.True(GraphRevision.TryCreate(revisionNumber, out GraphRevision revision));
        Assert.Equal(revisionNumber, revision.Value);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(int.MinValue)]
    public void TryCreateRejectsNonPositiveRevisionWithoutThrowing(int revisionNumber)
    {
        Assert.False(GraphRevision.TryCreate(revisionNumber, out GraphRevision revision));
        Assert.True(revision.IsDefault);
    }

    [Fact]
    public void EqualRevisionsAreEqualAndShareHashCode()
    {
        GraphRevision left = GraphRevision.Create(7);
        GraphRevision right = GraphRevision.Create(7);

        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.True(left == right);
        Assert.False(left != right);
    }

    [Fact]
    public void DifferentRevisionsAreNotEqual()
    {
        GraphRevision left = GraphRevision.Create(7);
        GraphRevision right = GraphRevision.Create(8);

        Assert.NotEqual(left, right);
        Assert.False(left == right);
        Assert.True(left != right);
    }

    [Fact]
    public void DefaultInstanceIsDefault()
    {
        Assert.True(default(GraphRevision).IsDefault);
        Assert.False(GraphRevision.Create(1).IsDefault);
        Assert.NotEqual(default, GraphRevision.Create(1));
    }

    [Fact]
    public void DefaultInstanceValueThrowsInvalidOperationException()
    {
        GraphRevision revision = default;

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(() => { _ = revision.Value; });

        Assert.Contains(nameof(GraphRevision), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DefaultInstanceToStringIsDiagnosticLiteralAndDoesNotThrow()
    {
        Assert.Equal("(default GraphRevision)", default(GraphRevision).ToString());
    }

    [Fact]
    public void ToStringDoesNotGroupDigits()
    {
        Assert.Equal("1234567", GraphRevision.Create(1234567).ToString());
    }

    [Fact]
    public void ComparisonIsNumericAndNotTextual()
    {
        // The ordering table. Revision 2 precedes revision 10, which is the whole reason the order is
        // numeric: as text it would not, and "later revision" would stop being a comparison.
        int[] ordered = [1, 2, 9, 10, 11, 1234567];

        for (int index = 1; index < ordered.Length; index++)
        {
            GraphRevision left = GraphRevision.Create(ordered[index - 1]);
            GraphRevision right = GraphRevision.Create(ordered[index]);

            Assert.True(left.CompareTo(right) < 0);
            Assert.True(right.CompareTo(left) > 0);
            Assert.True(left < right);
            Assert.True(left <= right);
            Assert.True(right > left);
            Assert.True(right >= left);
        }
    }

    [Fact]
    public void SortingUsesTheSameOrderWhicheverWayTheInputArrived()
    {
        GraphRevision[] shuffled =
        [
            GraphRevision.Create(10),
            GraphRevision.Create(2),
            GraphRevision.Create(1),
            GraphRevision.Create(11),
        ];

        Array.Sort(shuffled);

        Assert.Equal([1, 2, 10, 11], shuffled.Select(revision => revision.Value));
    }

    [Fact]
    public void TheDefaultInstanceSortsBeforeEveryCreatedOne()
    {
        GraphRevision first = GraphRevision.Create(GraphRevision.FirstRevisionNumber);

        Assert.True(default(GraphRevision).CompareTo(first) < 0);
        Assert.True(first.CompareTo(default) > 0);
        Assert.Equal(0, default(GraphRevision).CompareTo(default));
        Assert.True(default(GraphRevision) < first);
        Assert.True(first >= default(GraphRevision));
    }

    [Fact]
    public void ComparisonIsConsistentWithEquality()
    {
        GraphRevision left = GraphRevision.Create(7);
        GraphRevision right = GraphRevision.Create(7);

        Assert.Equal(0, left.CompareTo(right));
        Assert.Equal(left, right);
        Assert.True(left <= right);
        Assert.True(left >= right);
        Assert.False(left < right);
        Assert.False(left > right);
    }

    [Fact]
    public void TheNonGenericComparisonAgreesWithTheTypedOne()
    {
        // F#'s 'comparison' constraint is satisfied by System.IComparable and not by IComparable<'T>, so
        // this implementation is what lets the type key an F# Set or Map.
        IComparable left = GraphRevision.Create(2);
        GraphRevision right = GraphRevision.Create(10);

        Assert.True(typeof(IComparable).IsAssignableFrom(typeof(GraphRevision)));
        Assert.Equal(((GraphRevision)left).CompareTo(right), left.CompareTo(right));
        Assert.True(left.CompareTo(null) > 0);
        Assert.Throws<ArgumentException>("obj", () => left.CompareTo("not a GraphRevision"));
    }
}
