namespace Orleans.Dataflow.Samples

open System.Collections.Generic
open System.Globalization

// Orleans.Dataflow itself is deliberately not opened anywhere in this project: its Source, Flow and Sink
// are the C# facade's spellings of the F# frontend's own concepts, and an open would put two of each name
// in scope. Everything from it is written out in full instead.

/// <summary>How big a run of the samples is.</summary>
/// <remarks>
/// Two sizes and no number, because what changes between a full run and a smoke run is not one dimension.
/// A scenario asks this value which of its two sizes to use and gets an answer without the runner having
/// to know what the sizes mean: <c>scale.Pick(full = 12, smoke = 4)</c> reads at the call site as the
/// sentence it is. Both authorings of a scenario are handed the same value, so they build the same graph.
/// </remarks>
[<Sealed>]
type SampleScale private (smoke: bool) =

    /// <summary>The ordinary size, which is what a reader running the samples by hand sees.</summary>
    static member val Full = SampleScale(false)

    /// <summary>The smallest size that still exercises every scenario, which is what CI runs.</summary>
    static member val Smoke = SampleScale(true)

    /// <summary>Gets whether this is the smoke size.</summary>
    member _.IsSmoke = smoke

    /// <summary>Chooses between the two sizes of one quantity.</summary>
    /// <param name="full">What a full run uses.</param>
    /// <param name="smokeSize">What a smoke run uses.</param>
    /// <returns>One of them.</returns>
    member _.Pick(full: int, smokeSize: int) : int = if smoke then smokeSize else full

/// <summary>One graph a scenario built, and the fingerprint of its document.</summary>
/// <remarks>
/// A scenario may build more than one graph — running one shape under two overflow policies is two
/// documents, not one — so a reading is named. The runner pairs the two authorings' readings by that name,
/// which is why the two frontends must use the same names for the same graphs.
/// </remarks>
[<Sealed>]
type GraphReading(name: string, fingerprint: Orleans.Dataflow.Definition.GraphFingerprint) =

    /// <summary>Gets what this graph is called within its scenario.</summary>
    member _.Name = name

    /// <summary>Gets the fingerprint of the graph's canonical document.</summary>
    /// <remarks>
    /// The SHA-256 of the canonically serialized document, which is the whole point of this sample: two
    /// frontends that agree produce the same 32 bytes, and two that have drifted apart cannot.
    /// </remarks>
    member _.Fingerprint = fingerprint

    /// <summary>Reads a closed graph.</summary>
    /// <param name="name">What this graph is called within its scenario.</param>
    /// <param name="graph">The closed graph.</param>
    /// <returns>The reading.</returns>
    static member Of(name: string, graph: Orleans.Dataflow.RunnableGraph) = GraphReading(name, graph.Fingerprint)

    /// <summary>Reads a pipeline definition.</summary>
    /// <param name="name">What this pipeline is called within its scenario.</param>
    /// <param name="pipeline">The pipeline.</param>
    /// <returns>The reading.</returns>
    /// <remarks>
    /// A pipeline's fingerprint is not its graph's: declaring an identity re-closes the document under that
    /// identity, so the two differ by design and the cluster scenario reports the deployable one.
    /// </remarks>
    static member Of(name: string, pipeline: Orleans.Dataflow.PipelineDefinition) =
        GraphReading(name, pipeline.Fingerprint)

/// <summary>One thing a scenario's run produced, named so that a reader knows what the number is.</summary>
/// <remarks>
/// Everything here must be a logical fact about the run and never a measurement of how long it took: the
/// runner compares the two authorings' observations element by element and fails the run when they differ,
/// so an elapsed millisecond count would turn the sample's central check into a coin toss.
/// </remarks>
[<Sealed>]
type Observation(name: string, value: string) =

    /// <summary>Gets what was observed.</summary>
    member _.Name = name

    /// <summary>Gets the observation, already rendered.</summary>
    member _.Value = value

    /// <summary>Records text.</summary>
    /// <param name="name">What was observed.</param>
    /// <param name="value">The text.</param>
    /// <returns>The observation.</returns>
    static member Of(name: string, value: string) = Observation(name, value)

    /// <summary>Records a count.</summary>
    /// <param name="name">What was observed.</param>
    /// <param name="value">The count.</param>
    /// <returns>The observation.</returns>
    static member Of(name: string, value: int) =
        Observation(name, value.ToString(CultureInfo.InvariantCulture))

    /// <summary>Records a count the runtime keeps as a 64-bit number.</summary>
    /// <param name="name">What was observed.</param>
    /// <param name="value">The count.</param>
    /// <returns>The observation.</returns>
    static member Of(name: string, value: int64) =
        Observation(name, value.ToString(CultureInfo.InvariantCulture))

    /// <summary>Records an answer to a yes-or-no question.</summary>
    /// <param name="name">The question.</param>
    /// <param name="value">The answer.</param>
    /// <returns>The observation.</returns>
    static member Of(name: string, value: bool) = Observation(name, (if value then "yes" else "no"))

/// <summary>Everything one authoring of one scenario produced.</summary>
/// <remarks>
/// The two authorings of a scenario answer with one of these each, and the runner's whole verdict is a
/// comparison of the two: the same graph names carrying the same fingerprints, and the same observations
/// carrying the same values. Nothing else about the two runs is inspected, and nothing else needs to be.
/// </remarks>
[<Sealed>]
type ScenarioOutcome(graphs: IReadOnlyList<GraphReading>, observations: IReadOnlyList<Observation>) =

    /// <summary>Gets the graphs this authoring built, in the order it built them.</summary>
    member _.Graphs = graphs

    /// <summary>Gets what the runs produced.</summary>
    member _.Observations = observations

    /// <summary>Collects one authoring's answer.</summary>
    /// <param name="graphs">The graphs it built.</param>
    /// <param name="observations">What the runs produced.</param>
    /// <returns>The outcome.</returns>
    static member Of(graphs: seq<GraphReading>, observations: seq<Observation>) =
        ScenarioOutcome(Array.ofSeq graphs, Array.ofSeq observations)

/// <summary>What a scenario is handed when it is asked to run.</summary>
/// <remarks>
/// <para>
/// The scale, plus the two things a scenario cannot build for itself: the client host of the silo the
/// runner started, and a factory for the checkpoint store the console application implements. Both are
/// supplied by the C# side, because both are deployment decisions rather than authoring ones — which silo
/// a pipeline runs on, and what a run's checkpoints are kept in.
/// </para>
/// <para>
/// Both are optional. Starting a silo costs seconds and seven of the eight scenarios have no use for one,
/// so a scenario that asks for a cluster the runner did not start is a mistake in the runner and says so
/// rather than dereferencing nothing.
/// </para>
/// </remarks>
[<Sealed>]
type SampleRun
    (
        scale: SampleScale,
        cluster: Orleans.Dataflow.Hosting.OrleansDataflowHost | null,
        checkpoints: System.Func<Orleans.Dataflow.Hosting.ICheckpointStore> | null
    ) =

    /// <summary>Initializes a run that has neither a cluster nor a checkpoint store behind it.</summary>
    /// <param name="scale">How big the run is.</param>
    new(scale: SampleScale) = SampleRun(scale, null, null)

    /// <summary>Gets how big this run is.</summary>
    member _.Scale = scale

    /// <summary>Gets the client host of the silo this run was started with.</summary>
    /// <exception cref="T:System.InvalidOperationException">The runner started no silo for this run.</exception>
    member _.Cluster: Orleans.Dataflow.Hosting.OrleansDataflowHost =
        match cluster with
        | null ->
            invalidOp
                "This run was started without a silo, so there is no cluster host to materialize a pipeline through. The runner starts one only for the scenarios that declare they need it."
        | host -> host

    /// <summary>Builds a fresh, empty checkpoint store.</summary>
    /// <returns>The store.</returns>
    /// <exception cref="T:System.InvalidOperationException">The runner supplied no store for this run.</exception>
    /// <remarks>
    /// A factory rather than a store, because the durable scenario is about two hosts sharing one store and
    /// therefore has to be able to say when a store is new. The implementation is the console application's
    /// own <c>SampleCheckpointStore</c>, which is fifty lines and is itself one of the lessons.
    /// </remarks>
    member _.NewCheckpointStore() : Orleans.Dataflow.Hosting.ICheckpointStore =
        match checkpoints with
        | null ->
            invalidOp
                "This run was started without a checkpoint store factory, and the durable scenario needs one. The runner supplies it."
        | factory -> factory.Invoke()
