using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>A bound is a limit an author declared and never an allocation a run makes up front.</summary>
public sealed class DeclaredBoundCostTests
{
    [Fact]
    public async Task ALargeDeclaredGroupSizeCostsNothingUntilElementsArrive()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Range(1, 3)
            .Grouped(int.MaxValue)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Materializing this would throw before the first element if the stage sized its buffer by the
        // bound, which is why it does not: the whole stream is one partial group at the end.
        Assert.Equal([[1, 2, 3]], observed.Select(group => group.ToArray()));
    }

    [Fact]
    public async Task ALargeDeclaredWindowCostsNothingUntilElementsArrive()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Range(1, 2)
            .Sliding(int.MaxValue, int.MaxValue)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([[1, 2]], observed.Select(window => window.ToArray()));
    }
}
