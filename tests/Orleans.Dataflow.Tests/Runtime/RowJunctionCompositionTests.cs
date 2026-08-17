using Orleans.Dataflow.Authoring;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.JunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// The shapes a row-building junction makes when it stands beside another junction, and the one shape
/// checkpoint 2 could not build at all.
/// </summary>
/// <remarks>
/// <para>
/// The diamond is the interesting one, and it is the cousin of the head-of-line hazard checkpoint 2
/// documented. A broadcast pulls only when every live leg has room and then gives the same element to all of
/// them; a concat or an interleave with a segment above one wants several elements from one leg before it
/// touches another, and the split cannot supply them, which is a deadlock the author has to break with a
/// declared buffer. A zip wants exactly one element from every leg per row, which is exactly one per leg per
/// element — the two contracts are the same shape read from opposite ends, so the demands match and the
/// diamond needs nothing between the two junctions at all. A combine-latest is easier still: it takes
/// whichever leg has something, so no leg ever waits behind another.
/// </para>
/// <para>
/// That is a claim about liveness, and the tests below are how it is known rather than argued: each of them
/// is written with no buffer anywhere, and every boundary in them holds one element. If the reasoning were
/// wrong the run would stop, and the deadline in <see cref="JunctionFixtures.Reaches"/> would report it.
/// </para>
/// </remarks>
public sealed class RowJunctionCompositionTests
{
    [Fact]
    public async Task AZipRejoinsWhatABroadcastSplitWithNothingBetweenThem()
    {
        // The diamond checkpoint 2 could not build. Every element reaches both legs, each leg maps it its
        // own way, and the zip pairs them back positionally — so the result is an exact sequence rather than
        // a multiset, which is the difference a zip makes to a diamond a merge could only report unordered.
        // There is no buffer anywhere: the legs are handoffs of one element, which is what makes this a
        // statement about the two contracts fitting rather than about capacity papering over them.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Node("stage-3", "select"),
                    Node("stage-4", "select"),
                    Node("stage-5", "zip"),
                    Collect("stage-6", 16),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Leg("stage-2", 1, "stage-4"),
                    Into("stage-3", "stage-5", 0),
                    Into("stage-4", "stage-5", 1),
                    Edge("stage-5", "stage-6"),
                ],
                [Slot("rows", "stage-6")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3, 4, 5))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", LocalStageDescriptor.Select((Func<int, int>)(value => value * 10))),
                ("stage-4", LocalStageDescriptor.Select((Func<int, int>)(value => value + 100))),
                ("stage-5", LocalStageDescriptor.Zip(Rows())),
                ("stage-6", CollectingRows(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the split rejoins itself through a zip with no buffer between them");

        string[] rows = await run.GetValueAsync(Result<string[]>(graph, "rows"), TestToken);

        Assert.Equal(["10-101", "20-102", "30-103", "40-104", "50-105"], rows);
    }

    [Fact]
    public async Task ACombineLatestBelowABroadcastKeepsRunning()
    {
        // The same diamond with the other row-building junction, where the emitted sequence is genuinely a
        // scheduling question and is not asserted as if it were not. What is asserted is what the contract
        // decides: every row is built from elements of the one stream that was split, and the last row is
        // the last element paired with itself, because by the end both legs have delivered everything and
        // the latest of each is the same element.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Node("stage-3", "combine-latest"),
                    Collect("stage-4", 32),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Rejoins("stage-2", 0, "stage-3", 0),
                    Rejoins("stage-2", 1, "stage-3", 1),
                    Edge("stage-3", "stage-4"),
                ],
                [Slot("rows", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", LocalStageDescriptor.CombineLatest(Rows())),
                ("stage-4", CollectingRows(32))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the split rejoins itself through a combine-latest");

        string[] rows = await run.GetValueAsync(Result<string[]>(graph, "rows"), TestToken);

        Assert.NotEmpty(rows);
        Assert.Equal("3-3", rows[^1]);
        Assert.All(
            rows,
            row => Assert.All(
                row.Split('-'),
                part => Assert.Contains(part, (string[])["1", "2", "3"])));
    }

    [Fact]
    public async Task AMergeCanFeedAZip()
    {
        // Nothing about a junction says what may feed it. The merge decides an order nobody promised and the
        // zip pairs whatever arrives with the counting input positionally, so the second half of every row
        // is an exact sequence and the first half is the merge's own multiset — which is precisely the
        // division of promises the two contracts make.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "merge"),
                    Node("stage-4", "from-enumerable"),
                    Node("stage-5", "zip"),
                    Collect("stage-6", 16),
                ],
                [
                    Into("stage-1", "stage-3", 0),
                    Into("stage-2", "stage-3", 1),
                    Into("stage-3", "stage-5", 0),
                    Into("stage-4", "stage-5", 1),
                    Edge("stage-5", "stage-6"),
                ],
                [Slot("rows", "stage-6")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2))),
                ("stage-2", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(3, 4))),
                ("stage-3", LocalStageDescriptor.Merge()),
                ("stage-4", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(10, 20, 30, 40))),
                ("stage-5", LocalStageDescriptor.Zip(Rows())),
                ("stage-6", CollectingRows(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the merged stream is paired with the counting one to its end");

        string[] rows = await run.GetValueAsync(Result<string[]>(graph, "rows"), TestToken);

        string[] joined = [.. rows.Select(row => row.Split('-')[0])];
        string[] counted = [.. rows.Select(row => row.Split('-')[1])];

        Assert.Equal(["10", "20", "30", "40"], counted);
        Assert.Equal(["1", "2", "3", "4"], joined.Order(StringComparer.Ordinal));
        Assert.Equal(["1", "2"], joined.Where(value => value is "1" or "2"));
        Assert.Equal(["3", "4"], joined.Where(value => value is "3" or "4"));
    }

    [Fact]
    public async Task AZipCanFeedABroadcast()
    {
        // The two shapes composed the other way round: the rows a zip builds are one stream like any other,
        // and a broadcast below it delivers every row to both of its legs. Both sinks therefore agree row
        // for row, which is the fan-out's lockstep promise applied to elements the graph itself built.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "zip"),
                    Node("stage-4", "broadcast"),
                    Collect("stage-5", 16),
                    Collect("stage-6", 16),
                ],
                [
                    Into("stage-1", "stage-3", 0),
                    Into("stage-2", "stage-3", 1),
                    Edge("stage-3", "stage-4"),
                    Leg("stage-4", 0, "stage-5"),
                    Leg("stage-4", 1, "stage-6"),
                ],
                [Slot("left", "stage-5"), Slot("right", "stage-6")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3))),
                ("stage-2", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(10, 20, 30))),
                ("stage-3", LocalStageDescriptor.Zip(Rows())),
                ("stage-4", LocalStageDescriptor.Broadcast()),
                ("stage-5", CollectingRows(16)),
                ("stage-6", CollectingRows(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both sinks below the split have");

        string[] left = await run.GetValueAsync(Result<string[]>(graph, "left"), TestToken);
        string[] right = await run.GetValueAsync(Result<string[]>(graph, "right"), TestToken);

        Assert.Equal(["1-10", "2-20", "3-30"], left);
        Assert.Equal(left, right);
    }

    [Fact]
    public async Task AnUnzippedRowIsZippedBackTogetherWithoutSkew()
    {
        // The claim the unzip row of the capability matrix has been making since checkpoint 1, now proved
        // end to end rather than by pairing what two sinks collected. Both halves of a row leave the unzip
        // together because both legs had to have room before it pulled; each half is then transformed on its
        // own; and the zip pairs them positionally, so the i-th row out is built from the i-th row in. A
        // skew of one anywhere in the middle would show as every row being wrong from that point on.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "unzip"),
                    Node("stage-3", "select"),
                    Node("stage-4", "select"),
                    Node("stage-5", "zip"),
                    Collect("stage-6", 16),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Half("stage-2", "left", "stage-3"),
                    Half("stage-2", "right", "stage-4"),
                    Into("stage-3", "stage-5", 0),
                    Into("stage-4", "stage-5", 1),
                    Edge("stage-5", "stage-6"),
                ],
                [Slot("rows", "stage-6")]),
            Bindings(
                (
                    "stage-1",
                    LocalStageDescriptor.FromEnumerable(
                        new RecordingEnumerable<(int Left, int Right)>((1, 10), (2, 20), (3, 30), (4, 40)))),
                (
                    "stage-2",
                    LocalStageDescriptor.Unzip(
                        (Func<(int Left, int Right), int>)(row => row.Left),
                        (Func<(int Left, int Right), int>)(row => row.Right))),
                ("stage-3", LocalStageDescriptor.Select((Func<int, int>)(value => value * 2))),
                ("stage-4", LocalStageDescriptor.Select((Func<int, int>)(value => value + 1))),
                ("stage-5", LocalStageDescriptor.Zip(Rows())),
                ("stage-6", CollectingRows(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the halves are zipped back together to the end of the stream");

        string[] rows = await run.GetValueAsync(Result<string[]>(graph, "rows"), TestToken);

        Assert.Equal(["2-11", "4-21", "6-31", "8-41"], rows);
    }
}
