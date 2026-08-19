using System.Globalization;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What a run holds is what its author declared, and never what its source happens to be long.
/// </summary>
/// <remarks>
/// <para>
/// <b>The claim, stated as an experiment.</b> Every shape below is run twice through the same graph — once
/// over a stream of <see cref="Elements"/> elements and once over a stream ten times longer — and the two
/// runs are compared. Two different things are compared, because two different things are claimed.
/// Allocation is a per-element cost and is <em>supposed</em> to grow with the stream: a graph that
/// allocated for ten times the elements without allocating roughly ten times as much would have stopped
/// doing per-element work. Peak live heap is a bound and is supposed <em>not</em> to grow: whatever the
/// graph is holding at its fullest moment is its declared capacities, its declared concurrency and its
/// declared key count, none of which the stream length touches.
/// </para>
/// <para>
/// <b>Why the peak assertion is a fraction of the elements' own weight rather than a ratio of the peaks.</b>
/// The bounded shapes hold kilobytes — a fused chain holds a hundred and forty-four bytes — so a ratio
/// between two such readings is a ratio between two small numbers and says more about the moment than about
/// the runtime. What is stated instead is the thing worth stating: the extra nine-tenths of the stream did not
/// leave a mark. A <see cref="long"/> weighs eight bytes, so nine times <see cref="Elements"/> of them
/// weigh <see cref="Weight"/>, and a graph that retained even a quarter of that would be caught. A graph
/// that retained all of them — which is what "unbounded" means — would be caught four times over.
/// </para>
/// <para>
/// <b>Why the instrument collects rather than asking the collector to.</b> <c>GC.GetTotalMemory(true)</c>
/// stops when two successive readings agree to within about five percent, and five percent of a test
/// host's live set is hundreds of kilobytes — larger than everything these graphs retain put together.
/// Measured, not assumed: with that call the collecting control below reported a peak of <em>zero</em>.
/// So the sampler performs the collection itself, blocking and compacting, twice with a finalizer wait
/// between, and then reads a number that has no tolerance in it.
/// </para>
/// <para>
/// <b>What a peak is here: the range of one run's own samples</b> — the fullest reading minus the
/// quietest, with the first element always a sample point so that the quietest is normally the graph
/// before anything has flowed through it. Whatever else the process is holding is in every sample and
/// subtracts out, which is the property that makes kilobytes measurable inside a host holding megabytes.
/// </para>
/// <para>
/// <b>Why not a delta against a baseline taken before the run.</b> That was tried first and it broke
/// twice. A run that has completed and whose handle has been disposed is still holding its last
/// accumulator — the async machinery that carried it keeps the terminal's state reachable until the pool
/// threads pick up other work — so a baseline taken then belongs to the <em>previous</em> run: the deltas
/// came out at minus ten megabytes, drifting upward through the next run as the old state was let go.
/// Churning the thread pool before the baseline fixed that. The second break is why the baseline is gone
/// altogether: a residue that is reachable throughout a run but was <em>not</em> reachable when the
/// baseline was taken is indistinguishable from retention, and it was caught adding six megabytes to a
/// shape that holds twelve kilobytes. A residue stable across a run is in the lowest sample and the
/// highest alike, so a range subtracts it and a baseline cannot.
/// </para>
/// <para>
/// The churn survives that change for a different reason than it was introduced for: it releases the
/// previous run's state <em>before</em> this one starts, so it cannot be released halfway through and drag
/// the quietest sample below the fullest for reasons that have nothing to do with the graph.
/// </para>
/// <para>
/// <b>Why this class has a collection of its own, and why that collection runs alone.</b> Both instruments
/// read the whole process: <see cref="GC.GetTotalAllocatedBytes(bool)"/> counts every thread's allocation
/// and a live set includes every other test's objects. Sharing the process with the rest of a parallel
/// suite is therefore not a matter of noise around a signal — it was measured, and the noise <em>was</em>
/// the signal. The buffered shape allocates about a hundred bytes an element in a quiet process; read
/// alongside the running suite it read four hundred, and its ten-to-one ratio collapsed to between four
/// and five, because a run of four milliseconds had picked up six megabytes of somebody else's work. A
/// wider band would have made that pass without making it mean anything. So the collection declares
/// <c>DisableParallelization</c> and the suite stands still while these tests run. It is the most
/// expensive thing in this file and the only thing that makes the rest of it true.
/// </para>
/// <para>
/// Standing still is not silence: the runtime's own background threads, the test host, and this class's
/// own previous runs are all still there. So every measurement is additionally the <em>minimum</em> of
/// <see cref="Repetitions"/> runs — interference only ever adds, so the smallest reading is the closest to
/// the truth — and every claim is a comparison between two runs of the same graph rather than a number
/// with a threshold under it.
/// </para>
/// <para>
/// <b>Where the two thresholds came from.</b> Measured on the harness, which runs these same shapes in a
/// process with nothing else in it. Going from twenty thousand elements to two hundred thousand, the six
/// bounded shapes moved between −8 KB and +4.8 KB against an allowance of 360 KB — a factor of
/// seventy-four in hand on the worst of them. The collecting control moved by 10.5 MB against a required
/// 720 KB, a factor of fourteen. The allocation ratios came out between 9.0 and 10.2 against a band of 6
/// to 14. The margins are deliberately wide rather than snug: these run on other people's machines, and a
/// threshold set at the edge of one machine's measurement fails for reasons no reader can act on. If one
/// of them ever does turn flaky, widen <em>the number</em> and record why — never soften the sentence the
/// test is making.
/// </para>
/// <para>
/// <b>What has actually been shown to fail.</b> An assertion nobody has watched fail is a hope. The
/// bounded claim was run once against the collecting shape and rejected it — 10.47 MB of growth against
/// the 360 KB allowance — so the six shapes below pass because they are bounded and not because the
/// assertion cannot say no. The control, meanwhile, is what shows the instrument can say yes.
/// </para>
/// <para>
/// <b>What is not measured here at all.</b> Nothing in a cluster. A run hosted on a silo holds what these
/// shapes hold plus whatever Orleans holds for it, and separating the two needs a cluster harness rather
/// than an assertion; cluster memory is deliberately out of scope for this file and for 1.0's evidence.
/// The grain-call sink is present as a <em>shape</em> — a terminal keeping a declared number of calls in
/// flight — with the call faked locally, because what this file can honestly claim about it is the part
/// this runtime owns. The published measurements, including throughput and the recovery latency this file
/// says nothing about, live in docs/BENCHMARKS.md and come from benchmarks/Orleans.Dataflow.Benchmarks.
/// </para>
/// </remarks>
[Collection(BoundedMemoryCollectionDefinition.Name)]
public sealed class BoundedMemoryTests
{
    /// <summary>The shorter of the two streams every shape is run over.</summary>
    /// <remarks>
    /// Twenty thousand, and the same for every shape rather than tuned per shape. It is large enough that
    /// the allowance derived from it — see <see cref="Weight"/> — is comfortably above what a busy test
    /// host's own variation contributes, and small enough that the slowest shape here, a broadcast
    /// junction at ten times this, stays a few seconds rather than a minute.
    /// </remarks>
    private const long Elements = 20_000;

    /// <summary>How much longer the second stream is.</summary>
    private const long Multiplier = 10;

    /// <summary>How many times each measurement is taken.</summary>
    /// <remarks>
    /// Two, and the minimum of the two is used. A third would buy a little more protection from a badly
    /// timed neighbour and would cost a third more of the suite's slowest tests; two already turns a
    /// single unlucky run from a failure into a discarded reading.
    /// </remarks>
    private const int Repetitions = 2;

    /// <summary>What the elements the longer run adds would weigh if the graph kept every one of them.</summary>
    /// <remarks>
    /// Eight bytes each, which is what a <see cref="long"/> occupies and therefore the least a retained one
    /// can cost — the runtime hands elements around as <see cref="object"/> and a boxed long is three times
    /// this, so the allowance below is conservative in the direction that matters.
    /// </remarks>
    private const long Weight = (Multiplier - 1) * Elements * sizeof(long);

    /// <summary>The name of the shape that declares no boundary at all.</summary>
    private const string FusedChain = "fused-chain";

    /// <summary>The name of the shape with a declared buffer in the middle.</summary>
    private const string BufferedBoundary = "buffered-boundary";

    /// <summary>The name of the shape with a declared concurrency in the middle.</summary>
    private const string AsyncMap = "async-map";

    /// <summary>The name of the shape that hands every element to two consumers.</summary>
    private const string Broadcast = "broadcast";

    /// <summary>The name of the shape with a declared number of live keys.</summary>
    private const string BoundedGroupBy = "bounded-group-by";

    /// <summary>The name of the shape whose terminal keeps a declared number of calls in flight.</summary>
    private const string GrainCallShape = "grain-call-shape";

    /// <summary>The capacity the buffered shape declares.</summary>
    private const int BufferCapacity = 1024;

    /// <summary>The concurrency the asynchronous shapes declare.</summary>
    private const int Concurrency = 4;

    /// <summary>How many keys the grouping shape keeps active.</summary>
    private const int ActiveKeys = 16;

    [Theory]
    [InlineData(FusedChain)]
    [InlineData(BufferedBoundary)]
    [InlineData(AsyncMap)]
    [InlineData(Broadcast)]
    [InlineData(BoundedGroupBy)]
    [InlineData(GrainCallShape)]
    public async Task ADeclaredBoundIsWhatAGraphHoldsWhateverTheStreamLength(string shape)
    {
        long shorter = await PeakAsync(shape, Elements);
        long longer = await PeakAsync(shape, Elements * Multiplier);
        long growth = longer - shorter;

        // A quarter of what the added elements weigh, valued at the eight bytes a long occupies. A graph
        // that held them all is four times over this. A graph that held a tenth of them is over it too,
        // because the runtime hands elements around as objects and a boxed long is twenty-four bytes: a
        // tenth of a hundred and eighty thousand of those is 432 KB against an allowance of 360 KB. What
        // the slack buys is the difference between this machine and somebody else's — the six shapes moved
        // by at most 4.8 KB when measured with nothing else in the process.
        //
        // Growth and not magnitude, and only in one direction: a shape holding *less* over the longer
        // stream is not a defect, it is a graph whose fullest moment happened to fall between two samples.
        Assert.True(
            growth < Weight / 4,
            string.Create(
                CultureInfo.InvariantCulture,
                $"the '{shape}' graph held {shorter} bytes over {Elements} elements and {longer} over {Elements * Multiplier}, which is {growth} bytes more for {(Multiplier - 1) * Elements} more elements weighing {Weight}"));
    }

    [Theory]
    [InlineData(FusedChain)]
    [InlineData(BufferedBoundary)]
    [InlineData(AsyncMap)]
    [InlineData(Broadcast)]
    [InlineData(BoundedGroupBy)]
    [InlineData(GrainCallShape)]
    public async Task AllocationIsWhatEachElementCostsAndThereforeGrowsWithTheStream(string shape)
    {
        long shorter = await AllocatedAsync(shape, Elements);
        long longer = await AllocatedAsync(shape, Elements * Multiplier);
        double ratio = (double)longer / shorter;

        // The band is wide on purpose and it is bounded on both sides on purpose. Below it lies a graph
        // whose cost is dominated by something the stream does not touch — which would mean this test had
        // stopped measuring per-element work and become vacuous. Above it lies a graph whose per-element
        // cost is itself growing with the stream, which is the superlinear failure worth catching: at ten
        // times the elements, a cost proportional to the square would land at a hundred.
        Assert.True(
            ratio is > 6.0 and < 14.0,
            string.Create(
                CultureInfo.InvariantCulture,
                $"the '{shape}' graph allocated {shorter} bytes over {Elements} elements and {longer} over {Elements * Multiplier}, a ratio of {ratio:0.##} where the elements are {Multiplier} times as many"));
    }

    [Fact]
    public async Task ACollectingSinkGrowsWithTheMaximumItDeclaredAndSoTheInstrumentIsNotBlind()
    {
        // The control, and the reason to believe the six shapes above. Every assertion in this file is of
        // the form "the peak did not grow", and an instrument that could not see growth at all would make
        // every one of them pass while measuring nothing. This graph is built to grow — a collecting sink
        // under a declared maximum, and the maximum is the run length — so the same instrument, on the
        // same host, under the same interference, has to report it.
        //
        // It is also the other half of the claim proper. Memory follows what an author declared: declare a
        // bound of a thousand and a thousand is what is held, declare a bound of a million and the runtime
        // will hold a million. Nothing here is a promise that a graph cannot be written to use memory.
        long shorter = await PeakAsync(Collecting(Elements), Elements);
        long longer = await PeakAsync(Collecting(Elements * Multiplier), Elements * Multiplier);
        long growth = longer - shorter;

        Assert.True(
            growth > Weight / 2,
            string.Create(
                CultureInfo.InvariantCulture,
                $"the collecting sink held {shorter} bytes under a declared maximum of {Elements} and {longer} under a declared maximum of {Elements * Multiplier}, which is {growth} bytes more where the added elements weigh {Weight}"));
    }

    /// <summary>Measures the peak live heap of one named shape, over the given stream length.</summary>
    /// <param name="shape">Which shape.</param>
    /// <param name="elements">How long the stream is.</param>
    /// <returns>The smallest peak of <see cref="Repetitions"/> runs, in bytes above the run's baseline.</returns>
    private static Task<long> PeakAsync(string shape, long elements) => PeakAsync(Build(shape), elements);

    /// <summary>Measures the peak live heap of one shape, over the given stream length.</summary>
    /// <param name="build">Builds the graph over a sampler's numbers.</param>
    /// <param name="elements">How long the stream is.</param>
    /// <returns>The smallest peak of <see cref="Repetitions"/> runs, in bytes above the run's baseline.</returns>
    /// <remarks>
    /// One warmup run before the measured ones, because the first run through a graph leaves behind
    /// everything the runtime compiled and cached for it, and counting that as retention would credit the
    /// shorter run with a cost the longer one has already paid.
    /// </remarks>
    private static async Task<long> PeakAsync(Shape build, long elements)
    {
        HeapSampler sampler = new();
        RunnableGraph graph = build(elements, sampler);

        await sampler.ArmAsync(elements, sampling: false);

        await RunAsync(graph);

        long peak = long.MaxValue;

        for (int repetition = 0; repetition < Repetitions; repetition++)
        {
            await sampler.ArmAsync(elements, sampling: true);

            await RunAsync(graph);

            peak = Math.Min(peak, sampler.PeakBytes);
        }

        return peak;
    }

    /// <summary>Measures what one named shape allocates over the given stream length.</summary>
    /// <param name="shape">Which shape.</param>
    /// <param name="elements">How long the stream is.</param>
    /// <returns>The smallest total of <see cref="Repetitions"/> runs, in bytes.</returns>
    /// <remarks>
    /// Measured in its own pass with the sampler disarmed. The peak pass stops the world eight times per
    /// run, and a total read across those pauses would be a total read over a much longer stretch of the
    /// host's life than the run itself occupies.
    /// </remarks>
    private static async Task<long> AllocatedAsync(string shape, long elements)
    {
        HeapSampler sampler = new();
        RunnableGraph graph = Build(shape)(elements, sampler);

        await sampler.ArmAsync(elements, sampling: false);

        await RunAsync(graph);

        long allocated = long.MaxValue;

        for (int repetition = 0; repetition < Repetitions; repetition++)
        {
            await sampler.ArmAsync(elements, sampling: false);

            long before = GC.GetTotalAllocatedBytes(precise: true);

            await RunAsync(graph);

            allocated = Math.Min(allocated, GC.GetTotalAllocatedBytes(precise: true) - before);
        }

        return allocated;
    }

    /// <summary>Materializes a graph and waits for it to finish.</summary>
    /// <param name="graph">The graph.</param>
    /// <returns>A task that completes when the run has ended and its resources are released.</returns>
    private static async Task RunAsync(RunnableGraph graph)
    {
        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await run.Completion;
    }

    /// <summary>Builds one measured graph over a sampler's numbers.</summary>
    /// <param name="elements">How many elements the graph carries.</param>
    /// <param name="sampler">The source of the numbers, which reads the heap as it emits them.</param>
    /// <returns>The closed graph.</returns>
    private delegate RunnableGraph Shape(long elements, HeapSampler sampler);

    /// <summary>Finds one shape by name.</summary>
    /// <param name="shape">The name.</param>
    /// <returns>What builds it.</returns>
    /// <remarks>
    /// The theories name their shapes with strings rather than carrying delegates as data, so that what a
    /// failing case prints is the name of the shape that failed. Every one of these ends in something that
    /// discards — a fold to a number, a count, a callback that returns — so that what the peak measures is
    /// what the runtime holds and never what the author asked to keep.
    /// </remarks>
    private static Shape Build(string shape) => shape switch
    {
        FusedChain => static (elements, sampler) => Source.From(sampler.Numbers(elements))
            .Select(static value => value * 2)
            .Where(static value => value > 0)
            .Select(static value => value + 1)
            .To(static s => s.Aggregate(0L, static (sum, value) => sum + value).ToSink()),

        BufferedBoundary => static (elements, sampler) => Source.From(sampler.Numbers(elements))
            .Select(static value => value * 2)
            .Buffer(new BufferOptions { Capacity = BufferCapacity, OverflowPolicy = OverflowPolicy.Backpressure })
            .Select(static value => value + 1)
            .To(static s => s.Aggregate(0L, static (sum, value) => sum + value).ToSink()),

        AsyncMap => static (elements, sampler) => Source.From(sampler.Numbers(elements))
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = Concurrency },
                static (value, _) => Task.FromResult(value * 2))
            .To(static s => s.Aggregate(0L, static (sum, value) => sum + value).ToSink()),

        Broadcast => static (elements, sampler) => Source.From(sampler.Numbers(elements))
            .BroadcastTo(
                Flow.For<long>().To(static s => s.Aggregate(0L, static (sum, value) => sum + value).ToSink()),
                Flow.For<long>().To(static s => s.Count().ToSink())),

        BoundedGroupBy => static (elements, sampler) => Source.From(sampler.Numbers(elements))
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = ActiveKeys, OverflowPolicy = ActiveKeyOverflowPolicy.Fail },
                static value => value % ActiveKeys,
                Flow.For<long>().Select(static value => value + 1))
            .To(static s => s.Aggregate(0L, static (sum, value) => sum + value).ToSink()),

        GrainCallShape => static (elements, sampler) => Source.From(sampler.Numbers(elements))
            .To(static s => s.ForEachAsync(
                new ParallelismOptions { MaxConcurrency = Concurrency },
                static (_, _) => Task.CompletedTask)),

        _ => throw new ArgumentOutOfRangeException(nameof(shape), shape, "There is no shape by that name."),
    };

    /// <summary>Builds the collecting control, which keeps every element it is handed.</summary>
    /// <param name="maximum">The maximum the sink declares, which is what it is allowed to hold.</param>
    /// <returns>What builds it.</returns>
    private static Shape Collecting(long maximum) =>
        (elements, sampler) => Source.From(sampler.Numbers(elements))
            .To(_ => Sink.Collect<long>(new CollectOptions { MaxElements = checked((int)maximum) }).ToSink());

    /// <summary>
    /// The instrument: a source that reads the live heap from inside the stream it is feeding.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Sampling from inside rather than from a thread outside is what makes two runs of different lengths
    /// comparable. A thread polling from outside reads whatever the collector happens to be holding at a
    /// moment nobody chose; a sample taken between two elements reads a graph at a known position in its
    /// stream, and the positions are the same fraction of the way through each run.
    /// </para>
    /// <para>
    /// A twin of the sampler in benchmarks/Orleans.Dataflow.Benchmarks, and deliberately a twin rather than
    /// a shared type: a test project that referenced the harness would make the harness a dependency of the
    /// suite, and what the two share is thirty lines whose whole content is stated in the comments above.
    /// </para>
    /// </remarks>
    private sealed class HeapSampler
    {
        /// <summary>How many samples a probing run takes.</summary>
        private const int SampleCount = 8;

        /// <summary>How many rounds of churn and collection precede a weighed run.</summary>
        private const int SettleRounds = 3;

        private long _stride;
        private long _lowest;
        private long _highest;
        private int _samples;

        /// <summary>Gets how much the live set grew between the quietest and the fullest sample of a run.</summary>
        internal long PeakBytes => _samples == 0 ? 0 : _highest - _lowest;

        /// <summary>Arms the sampler for one run.</summary>
        /// <param name="elements">How many elements the run will carry.</param>
        /// <param name="sampling">
        /// <see langword="true"/> to read the heap during the run; <see langword="false"/> for a run whose
        /// allocation is being counted, which must not stop to collect.
        /// </param>
        /// <returns>A task that completes when the process has settled and the run may start.</returns>
        internal async Task ArmAsync(long elements, bool sampling)
        {
            _lowest = long.MaxValue;
            _highest = long.MinValue;
            _samples = 0;
            _stride = sampling ? Math.Max(1, elements / SampleCount) : 0;

            if (sampling)
            {
                await SettleAsync();
            }
        }

        /// <summary>Waits until the previous run has finished letting go of what it held.</summary>
        /// <returns>A task that completes when the process is as quiet as this can make it.</returns>
        private static async Task SettleAsync()
        {
            for (int round = 0; round < SettleRounds; round++)
            {
                await ChurnAsync();

                _ = Live();
            }
        }

        /// <summary>Hands the thread pool enough trivial work to recycle the threads the last run used.</summary>
        /// <returns>A task that completes when the work has been done.</returns>
        private static Task ChurnAsync() =>
            Task.WhenAll([.. Enumerable
                .Range(0, Environment.ProcessorCount * 4)
                .Select(static _ => Task.Run(static () => { }))]);

        /// <summary>Emits the numbers the measured graphs run on, reading the heap as it goes.</summary>
        /// <param name="count">How many numbers to emit, starting at one.</param>
        /// <returns>The sequence, which may be enumerated once per run.</returns>
        /// <remarks>
        /// The first element is always a sample point: that is the graph before anything has flowed through
        /// it, and without it a boundary that fills faster than one stride would never be seen filling.
        /// </remarks>
        internal IEnumerable<long> Numbers(long count)
        {
            for (long index = 1; index <= count; index++)
            {
                if (_stride > 0 && (index == 1 || index % _stride == 0))
                {
                    long live = Live();

                    _samples++;
                    _lowest = Math.Min(_lowest, live);
                    _highest = Math.Max(_highest, live);
                }

                yield return index;
            }
        }

        /// <summary>Reads the live set, exactly.</summary>
        /// <returns>How many bytes are reachable.</returns>
        /// <remarks>
        /// Two collections with a finalizer wait between them, because an object whose finalizer has not
        /// run is still reachable from the finalization queue and would count as live on the first pass.
        /// Compacting, so that two readings from different points in a run are not separated by
        /// fragmentation. The read itself forces nothing: the collection has already happened.
        /// </remarks>
        private static long Live()
        {
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);

            return GC.GetTotalMemory(forceFullCollection: false);
        }
    }
}

/// <summary>
/// The collection the bounded-memory tests belong to, which the suite does not run anything beside.
/// </summary>
/// <remarks>
/// The only collection in this project that declares <c>DisableParallelization</c>, and the reason is in
/// <see cref="BoundedMemoryTests"/>: its two instruments read the whole process, so a neighbour running at
/// the same time is not noise around the measurement but a larger contribution than the measurement
/// itself. Nothing else in the suite needs this and nothing else should take it — it costs the suite the
/// wall clock of these tests, spent alone.
/// </remarks>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class BoundedMemoryCollectionDefinition
{
    /// <summary>The collection's name.</summary>
    public const string Name = "bounded-memory";
}
