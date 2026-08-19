namespace Orleans.Dataflow.Benchmarks;

/// <summary>
/// The instrument every memory number in this harness is read off: a source that samples the live heap
/// from inside the stream it is feeding.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a source and not a sampling thread.</b> A thread that polls the heap from outside reads whatever
/// the collector happens to be holding, which is dominated by garbage nobody is retaining and by the
/// allocation budget of the moment. Sampling from inside the sequence puts the reading at a known position
/// in the stream — element <c>n</c> has been handed over, whatever the graph holds it holds now — and
/// makes two runs of different lengths comparable, because the sample positions are the same fraction of
/// the way through each.
/// </para>
/// <para>
/// <b>Why a forced collection at every sample.</b> The claim under measurement is retention: memory the
/// runtime is still holding. Uncollected garbage is not retention, and a reading that included it would
/// grow with throughput rather than with what the graph keeps. So every reading is taken immediately after
/// a blocking, compacting collection of every generation, which makes what is left the live set. That
/// costs two collections per sample, which is exactly why the timing pass runs with sampling off: a run
/// cannot be timed and weighed at once without one answer spoiling the other.
/// </para>
/// <para>
/// <b>Why the collection is performed here rather than asked for.</b> <c>GC.GetTotalMemory(true)</c> looks
/// like the obvious call and is the wrong one: it collects repeatedly until two readings agree to within
/// about five percent, and then returns. On a process whose live set is a few megabytes that tolerance is
/// hundreds of kilobytes — larger than everything these graphs retain put together. Measured rather than
/// reasoned: with that call the collecting control, which retains a megabyte by construction, reported a
/// peak of <em>zero</em>. Collecting explicitly and reading with <see langword="false"/> afterwards has no
/// tolerance in it.
/// </para>
/// <para>
/// <b>What the number is: the range of the run's own samples,</b> the fullest reading minus the quietest,
/// with the first sample taken before the first element is handed over so that the quietest is normally
/// the graph at rest. Nothing outside the run enters it — whatever else the process is holding is in every
/// sample and cancels — which is the property that matters, because these graphs retain kilobytes in a
/// process holding megabytes.
/// </para>
/// <para>
/// <b>Why the range and not a delta against a baseline taken before the run.</b> That is what this
/// measured first, and it broke twice. A previous run that has completed and whose handle has been
/// disposed is still holding its last accumulator — the async machinery that carried it keeps the
/// terminal's state reachable until the pool threads pick up other work — so a baseline taken then belongs
/// to the previous run: the deltas came out at minus ten megabytes, drifting upward as the old state was
/// finally let go. Churning the thread pool before the baseline fixed that one. The second was worse and
/// is why the baseline is gone: a residue that is reachable throughout a run but was <em>not</em>
/// reachable when the baseline was taken is indistinguishable from retention, and it was seen adding six
/// megabytes to a shape that holds twenty kilobytes. A residue that is stable across a run is in the
/// lowest and the highest sample alike, so a range subtracts it and a baseline cannot.
/// </para>
/// <para>
/// The churn survives that change for a different reason than it was introduced for: it releases the
/// previous run's state <em>before</em> this one starts, so it cannot be released halfway through and drag
/// the quietest sample below the fullest for reasons that have nothing to do with the graph.
/// </para>
/// </remarks>
internal sealed class HeapProbe
{
    /// <summary>How many samples a probing run takes.</summary>
    /// <remarks>
    /// Eight, spread evenly through the stream. Enough that a peak reached anywhere but the last few
    /// percent of the run is seen, few enough that eight blocking collections do not dominate a run that is
    /// not being timed anyway.
    /// </remarks>
    internal const int SampleCount = 8;

    /// <summary>How many rounds of churn and collection precede a weighed run.</summary>
    /// <remarks>
    /// Three. One round already recovers most of what the previous run was holding; three was where the
    /// reading stopped moving, and a round costs a few dozen empty work items and a collection.
    /// </remarks>
    private const int SettleRounds = 3;

    private long _stride;
    private long _lowest;
    private long _highest;
    private int _samples;

    /// <summary>Gets how much the live set grew between the quietest and the fullest sample of the last run.</summary>
    /// <value>The peak in bytes, or zero when the last run was not probing.</value>
    internal long PeakBytes => _samples == 0 ? 0 : _highest - _lowest;

    /// <summary>Gets how many samples the last run took.</summary>
    internal int Samples => _samples;

    /// <summary>Arms the probe for one run.</summary>
    /// <param name="elements">How many elements the run will carry.</param>
    /// <param name="sampling">
    /// <see langword="true"/> to sample the heap during the run; <see langword="false"/> for a timing run,
    /// which must not stop to collect.
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

    /// <summary>Emits the numbers the measured graphs run on, sampling the heap as it goes.</summary>
    /// <param name="count">How many numbers to emit, starting at one.</param>
    /// <returns>The sequence.</returns>
    /// <remarks>
    /// The sample is taken before the element is handed over, so a sample at position <c>n</c> reads a
    /// graph that has been given <c>n - 1</c> elements and has done with them whatever it does. The first
    /// element is always a sample point: that is the reading of the graph before anything has flowed
    /// through it, and without it a boundary that fills faster than one stride would never be seen filling.
    /// Re-enumerable, because a scenario is materialized once per run and every run walks the sequence
    /// again.
    /// </remarks>
    internal IEnumerable<long> Numbers(long count)
    {
        for (long index = 1; index <= count; index++)
        {
            if (_stride > 0 && (index == 1 || index % _stride == 0))
            {
                Sample();
            }

            yield return index;
        }
    }

    /// <summary>Waits until the previous run has finished letting go of what it held.</summary>
    /// <returns>A task that completes when the process is as quiet as this harness can make it.</returns>
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

    /// <summary>Reads the live set, exactly.</summary>
    /// <returns>How many bytes are reachable.</returns>
    /// <remarks>
    /// Two collections with a finalizer wait between them, because an object whose finalizer has not run
    /// yet is still reachable from the finalization queue and would be counted as live on the first pass.
    /// Compacting, so that two readings taken at different points in a run are not separated by
    /// fragmentation. The read itself asks for no collection: the collection has already happened.
    /// </remarks>
    private static long Live()
    {
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);

        return GC.GetTotalMemory(forceFullCollection: false);
    }

    /// <summary>Reads the live set and keeps it if it is the largest or the smallest so far.</summary>
    private void Sample()
    {
        long live = Live();

        _samples++;
        _lowest = Math.Min(_lowest, live);
        _highest = Math.Max(_highest, live);
    }
}
