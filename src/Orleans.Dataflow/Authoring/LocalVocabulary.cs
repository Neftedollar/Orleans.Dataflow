using System.Globalization;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// Every definition-plane identity the local, lambda-implemented authoring vocabulary writes into a graph
/// document, in one place.
/// </summary>
/// <remarks>
/// <para>
/// The definition plane forbids CLR type names as contract identity, and a local graph is typed by C#
/// generics rather than by registered contracts. The local vocabulary is therefore deliberately blind: every
/// local port declares the single element contract <see cref="ElementContract"/>, which carries no
/// element-type information at all. Document-level contract checking is what registered stages are for; a
/// local document says only what it can honestly say, and the C# compiler is what actually rejects a
/// <c>Sink&lt;string&gt;</c> under a <c>Source&lt;int&gt;</c>.
/// </para>
/// <para>
/// Delegates, captured state, the fold seed, and the value a source repeats never appear here, because they
/// never appear in a document at all (AGENTS.md). They live in the authoring-side binding table that
/// <see cref="Orleans.Dataflow.RunnableGraph"/> carries for the local runtime.
/// </para>
/// <para>
/// The four derivations at the end — the stage reference, the parameter contract and its check, which ports
/// a shape declares, and which result contract it produces — are the whole of what a
/// <see cref="LocalStageKind"/> means to a document.
/// <see cref="Orleans.Dataflow.LocalStageCatalog"/> and <see cref="LocalStageDescriptor"/> both read them,
/// so a catalog specification and the occurrence validated against it cannot disagree.
/// </para>
/// <para>
/// The fields are initialized in textual order, and every field that composes another one is declared after
/// it.
/// </para>
/// </remarks>
internal static class LocalVocabulary
{
    /// <summary>The prefix of every automatically allocated node identifier.</summary>
    /// <remarks>
    /// ADR 0004 fixes the spelling: an unnamed occurrence is <c>stage-0001</c>, <c>stage-0002</c>, and so
    /// on in authoring order. Positional identifiers are not edit-stable, which is why a document that
    /// contains one declares <see cref="EphemeralIdentity"/>.
    /// </remarks>
    internal const string AutoNamePrefix = "stage-";

    /// <summary>The highest position an automatically allocated node identifier can name.</summary>
    /// <remarks>
    /// The invariant this bound buys is that a document's canonical node order — ordinal over identifier
    /// text — is the authoring order of the occurrences it was built from, for every graph whose
    /// occurrences are automatically named. Four digits sort correctly against each other and five do
    /// not, so the numbering has to end somewhere; it ends here rather than silently becoming
    /// <c>stage-10000</c>, which would sort between <c>stage-0001</c> and <c>stage-0002</c> and quietly
    /// break the invariant for the one graph large enough to reach it.
    /// </remarks>
    internal const int MaxAutoNamedPosition = 9999;

    /// <summary>The numeric format that pads a position to the four digits <see cref="MaxAutoNamedPosition"/> allows.</summary>
    private const string AutoNameNumberFormat = "D4";

    /// <summary>The greatest number of outputs a broadcast, a balance, or a partition declares.</summary>
    /// <remarks>
    /// A stage specification declares a port list rather than an arity, so a junction's legs have to be
    /// ports that exist whether or not a given document wires them; the bound is where that list stops. It
    /// is stated rather than implied because every bound in this runtime is: a graph that needs a ninth leg
    /// says so and gets a diagnostic, instead of silently losing one.
    /// </remarks>
    internal const int MaxFanOut = 8;

    /// <summary>The smallest number of outputs a junction declares.</summary>
    /// <remarks>
    /// Both of the first two ports are required, which is what makes "this is a junction" a fact the graph
    /// compiler checks rather than a promise the author makes.
    /// </remarks>
    internal const int MinFanOut = 2;

    /// <summary>The greatest number of inputs a fan-in junction declares.</summary>
    /// <remarks>
    /// The mirror of <see cref="MaxFanOut"/> and the same number for the same reason: a stage specification
    /// declares a port list rather than an arity, so a junction's inputs have to be ports that exist whether
    /// or not a given document wires them, and the list has to stop somewhere. A graph that needs a ninth
    /// input says so and gets a diagnostic instead of silently losing one.
    /// </remarks>
    internal const int MaxFanIn = 8;

    /// <summary>The smallest number of inputs a fan-in junction declares.</summary>
    /// <remarks>
    /// Both of the first two ports are required, which is what makes "this is a junction" a fact the graph
    /// compiler checks rather than a promise the author makes. A fan-in wired at one input is a chain
    /// written the long way, and one wired at none is a source that produces nothing.
    /// </remarks>
    internal const int MinFanIn = 2;

    /// <summary>The prefix of every numbered fan-out port name.</summary>
    private const string FanOutPortPrefix = "out-";

    /// <summary>The prefix of every numbered fan-in port name.</summary>
    private const string FanInPortPrefix = "in-";

    /// <summary>The provider every local stage belongs to.</summary>
    internal static readonly ProviderId Provider = ProviderId.Create("local");

    /// <summary>The stage reference of a source over an in-memory sequence.</summary>
    internal static readonly StageRef FromEnumerable =
        StageRef.Create(Provider, StageId.Create("from-enumerable"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a source that emits nothing.</summary>
    internal static readonly StageRef Empty =
        StageRef.Create(Provider, StageId.Create("empty"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a source that emits one element.</summary>
    internal static readonly StageRef Single =
        StageRef.Create(Provider, StageId.Create("single"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a source that emits one element a declared number of times.</summary>
    internal static readonly StageRef Repeat =
        StageRef.Create(Provider, StageId.Create("repeat"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a source over a run of consecutive integers.</summary>
    internal static readonly StageRef Range =
        StageRef.Create(Provider, StageId.Create("range"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a source over the value of one task.</summary>
    internal static readonly StageRef FromTask =
        StageRef.Create(Provider, StageId.Create("from-task"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a source that fails.</summary>
    internal static readonly StageRef Failed =
        StageRef.Create(Provider, StageId.Create("failed"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a source driven by a generator over its own state.</summary>
    internal static readonly StageRef Unfold =
        StageRef.Create(Provider, StageId.Create("unfold"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a source over an asynchronous sequence.</summary>
    internal static readonly StageRef FromAsyncEnumerable =
        StageRef.Create(Provider, StageId.Create("from-async-enumerable"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a source over a factory of one element.</summary>
    internal static readonly StageRef FromFactory =
        StageRef.Create(Provider, StageId.Create("from-factory"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a source over an asynchronous factory of one element.</summary>
    internal static readonly StageRef FromAsyncFactory =
        StageRef.Create(Provider, StageId.Create("from-async-factory"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a source that emits nothing and never ends.</summary>
    internal static readonly StageRef Never =
        StageRef.Create(Provider, StageId.Create("never"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a source that repeats a sequence endlessly.</summary>
    internal static readonly StageRef Cycle =
        StageRef.Create(Provider, StageId.Create("cycle"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a source driven by an asynchronous generator over its own state.</summary>
    internal static readonly StageRef UnfoldAsync =
        StageRef.Create(Provider, StageId.Create("unfold-async"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a source over a bounded ingress queue of its own.</summary>
    internal static readonly StageRef Queue =
        StageRef.Create(Provider, StageId.Create("queue"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a source over a channel the author owns.</summary>
    internal static readonly StageRef FromChannel =
        StageRef.Create(Provider, StageId.Create("from-channel"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a source that emits the number of every tick of an interval.</summary>
    internal static readonly StageRef Tick =
        StageRef.Create(Provider, StageId.Create("tick"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that holds every element for a declared duration.</summary>
    internal static readonly StageRef Delay =
        StageRef.Create(Provider, StageId.Create("delay"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that holds the first element until a duration has passed.</summary>
    internal static readonly StageRef InitialDelay =
        StageRef.Create(Provider, StageId.Create("initial-delay"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that fails the run when the stream goes quiet.</summary>
    internal static readonly StageRef Timeout =
        StageRef.Create(Provider, StageId.Create("timeout"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that ends the stream after a declared duration.</summary>
    internal static readonly StageRef TakeWithin =
        StageRef.Create(Provider, StageId.Create("take-within"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that drops elements for a declared duration.</summary>
    internal static readonly StageRef SkipWithin =
        StageRef.Create(Provider, StageId.Create("skip-within"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that holds a stream to a declared rate.</summary>
    internal static readonly StageRef Throttle =
        StageRef.Create(Provider, StageId.Create("throttle"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that holds elements while its control is closed.</summary>
    internal static readonly StageRef Valve =
        StageRef.Create(Provider, StageId.Create("valve"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a mapping stage.</summary>
    internal static readonly StageRef Select =
        StageRef.Create(Provider, StageId.Create("select"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a filtering stage.</summary>
    internal static readonly StageRef Where =
        StageRef.Create(Provider, StageId.Create("where"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a running fold that emits its intermediate states.</summary>
    internal static readonly StageRef Scan =
        StageRef.Create(Provider, StageId.Create("scan"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that passes a declared number of elements.</summary>
    internal static readonly StageRef Take =
        StageRef.Create(Provider, StageId.Create("take"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that drops a declared number of elements.</summary>
    internal static readonly StageRef Skip =
        StageRef.Create(Provider, StageId.Create("skip"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that passes elements while a predicate holds.</summary>
    internal static readonly StageRef TakeWhile =
        StageRef.Create(Provider, StageId.Create("take-while"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that passes elements up to and including the one a predicate accepts.</summary>
    internal static readonly StageRef TakeThrough =
        StageRef.Create(Provider, StageId.Create("take-through"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that drops elements while a predicate holds.</summary>
    internal static readonly StageRef SkipWhile =
        StageRef.Create(Provider, StageId.Create("skip-while"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that drops repeated elements.</summary>
    internal static readonly StageRef Distinct =
        StageRef.Create(Provider, StageId.Create("distinct"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that runs one substream per key.</summary>
    internal static readonly StageRef GroupBy =
        StageRef.Create(Provider, StageId.Create("group-by"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that drops an element equal to the one before it.</summary>
    internal static readonly StageRef DeduplicateConsecutive =
        StageRef.Create(Provider, StageId.Create("deduplicate-consecutive"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that flattens one sequence per element.</summary>
    internal static readonly StageRef SelectMany =
        StageRef.Create(Provider, StageId.Create("select-many"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that flattens several sequences at once.</summary>
    internal static readonly StageRef MergeMap =
        StageRef.Create(Provider, StageId.Create("merge-map"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a running fold whose function is asynchronous.</summary>
    internal static readonly StageRef ScanAsync =
        StageRef.Create(Provider, StageId.Create("scan-async"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that collects a declared number of elements per group.</summary>
    internal static readonly StageRef Grouped =
        StageRef.Create(Provider, StageId.Create("grouped"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that emits a window of a declared size and step.</summary>
    internal static readonly StageRef Sliding =
        StageRef.Create(Provider, StageId.Create("sliding"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that closes a group by a count or by a window.</summary>
    internal static readonly StageRef GroupedWithin =
        StageRef.Create(Provider, StageId.Create("grouped-within"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that closes a group by a count, a weight, or a window.</summary>
    internal static readonly StageRef GroupedWeightedWithin =
        StageRef.Create(
            Provider,
            StageId.Create("grouped-weighted-within"),
            StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a bounded buffer.</summary>
    internal static readonly StageRef Buffer =
        StageRef.Create(Provider, StageId.Create("buffer"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of an order-preserving asynchronous mapping stage.</summary>
    internal static readonly StageRef SelectAsync =
        StageRef.Create(Provider, StageId.Create("select-async"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of an asynchronous mapping stage that emits in completion order.</summary>
    internal static readonly StageRef SelectAsyncUnordered =
        StageRef.Create(Provider, StageId.Create("select-async-unordered"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of an order-preserving asynchronous mapping stage over value tasks.</summary>
    internal static readonly StageRef SelectValueTaskAsync =
        StageRef.Create(Provider, StageId.Create("select-value-task-async"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a value-task mapping stage that emits in completion order.</summary>
    internal static readonly StageRef SelectValueTaskAsyncUnordered =
        StageRef.Create(
            Provider,
            StageId.Create("select-value-task-async-unordered"),
            StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a junction that delivers every element to every output.</summary>
    internal static readonly StageRef Broadcast =
        StageRef.Create(Provider, StageId.Create("broadcast"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a junction that delivers each element to one output with room.</summary>
    internal static readonly StageRef Balance =
        StageRef.Create(Provider, StageId.Create("balance"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a junction that delivers each element to the output its function names.</summary>
    internal static readonly StageRef Partition =
        StageRef.Create(Provider, StageId.Create("partition"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a junction that delivers a row's halves to two outputs.</summary>
    internal static readonly StageRef Unzip =
        StageRef.Create(Provider, StageId.Create("unzip"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a junction that emits whichever input has an element.</summary>
    internal static readonly StageRef Merge =
        StageRef.Create(Provider, StageId.Create("merge"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a junction that emits one input to its end before the next.</summary>
    internal static readonly StageRef Concat =
        StageRef.Create(Provider, StageId.Create("concat"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a junction that emits a declared number of elements per input.</summary>
    internal static readonly StageRef Interleave =
        StageRef.Create(Provider, StageId.Create("interleave"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a junction that emits one row per element from each input.</summary>
    internal static readonly StageRef Zip =
        StageRef.Create(Provider, StageId.Create("zip"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a junction that emits a row of every input's latest element.</summary>
    internal static readonly StageRef CombineLatest =
        StageRef.Create(Provider, StageId.Create("combine-latest"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a folding sink.</summary>
    internal static readonly StageRef Fold =
        StageRef.Create(Provider, StageId.Create("fold"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a folding sink whose function is asynchronous.</summary>
    internal static readonly StageRef FoldAsync =
        StageRef.Create(Provider, StageId.Create("fold-async"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a discarding sink.</summary>
    internal static readonly StageRef Ignore =
        StageRef.Create(Provider, StageId.Create("ignore"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a sink that hands every element to a synchronous callback.</summary>
    internal static readonly StageRef ForEach =
        StageRef.Create(Provider, StageId.Create("for-each"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a sink that hands every element to an asynchronous callback.</summary>
    internal static readonly StageRef ForEachAsync =
        StageRef.Create(Provider, StageId.Create("for-each-async"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a sink that takes the first element and requires one.</summary>
    internal static readonly StageRef First =
        StageRef.Create(Provider, StageId.Create("first"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a sink that takes the first element or the default value.</summary>
    internal static readonly StageRef FirstOrDefault =
        StageRef.Create(Provider, StageId.Create("first-or-default"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a counting sink.</summary>
    internal static readonly StageRef Count =
        StageRef.Create(Provider, StageId.Create("count"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a sink that keeps the last element and requires one.</summary>
    internal static readonly StageRef Last =
        StageRef.Create(Provider, StageId.Create("last"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a sink that keeps the last element or the default value.</summary>
    internal static readonly StageRef LastOrDefault =
        StageRef.Create(Provider, StageId.Create("last-or-default"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a bounded collecting sink.</summary>
    internal static readonly StageRef Collect =
        StageRef.Create(Provider, StageId.Create("collect"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a sink that writes into a channel the author owns.</summary>
    internal static readonly StageRef ToChannel =
        StageRef.Create(Provider, StageId.Create("to-channel"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a sink that hands every element to a receiver that asks for it.</summary>
    internal static readonly StageRef SinkProbe =
        StageRef.Create(Provider, StageId.Create("sink-probe"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that throws where its declared arming says to.</summary>
    internal static readonly StageRef FaultPoint =
        StageRef.Create(Provider, StageId.Create("fault-point"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage that answers the failures of the chain it owns.</summary>
    internal static readonly StageRef Supervised =
        StageRef.Create(Provider, StageId.Create("supervised"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a stage whose chain's state survives a resume.</summary>
    internal static readonly StageRef Durable =
        StageRef.Create(Provider, StageId.Create("durable"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a sink whose commit mark advances after its side effect.</summary>
    internal static readonly StageRef MarkingSink =
        StageRef.Create(Provider, StageId.Create("marking-sink"), StageRef.FirstMajorVersion);

    /// <summary>The one element contract every local port declares.</summary>
    /// <remarks>
    /// One opaque contract for every local element type is the honest encoding of a graph whose element
    /// types exist only in the C# type system. Two local documents therefore agree on element contracts
    /// whatever their lambdas do, and a local graph's element typing is proven by the compiler, not by the
    /// graph compiler.
    /// </remarks>
    internal static readonly ContractReference ElementContract =
        ContractReference.Create(ContractId.Create("local-opaque"), ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a local stage whose whole behavior is a delegate declares.</summary>
    /// <remarks>
    /// Such a stage has no parameters that could be written down: its behavior is a delegate, and a
    /// delegate is never durable topology. The payload is therefore always the empty object.
    /// </remarks>
    internal static readonly ContractReference ParameterContract =
        ContractReference.Create(ContractId.Create("local-parameters"), ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a buffer declares.</summary>
    /// <remarks>
    /// A buffer's capacity and overflow policy are configuration rather than behavior, so unlike a
    /// delegate they belong in the document. <see cref="LocalBufferParameters"/> owns the shape.
    /// </remarks>
    internal static readonly ContractReference BufferParameterContract =
        ContractReference.Create(
            ContractId.Create("local-buffer-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract an asynchronous stage declares.</summary>
    /// <remarks>
    /// The concurrency bound is configuration and is written down; the callback is behavior and is not.
    /// <see cref="LocalParallelismParameters"/> owns the shape.
    /// </remarks>
    internal static readonly ContractReference ParallelismParameterContract =
        ContractReference.Create(
            ContractId.Create("local-parallelism-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a stage counted in elements declares.</summary>
    /// <remarks>
    /// One contract for every stage counted in elements, because a count is a count: they carry the same
    /// member under the same rules, and which of them is meant is the stage reference's job to say.
    /// <see cref="ParameterContractOf"/> is where which shapes those are is stated, and
    /// <see cref="LocalCountParameters"/> owns the shape.
    /// </remarks>
    internal static readonly ContractReference CountParameterContract =
        ContractReference.Create(
            ContractId.Create("local-count-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a range source declares.</summary>
    /// <remarks>
    /// A range says everything about itself in two numbers and binds no behavior at all, which makes it
    /// the second shape after the buffer whose document states it completely.
    /// <see cref="LocalRangeParameters"/> owns the shape.
    /// </remarks>
    internal static readonly ContractReference RangeParameterContract =
        ContractReference.Create(
            ContractId.Create("local-range-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a distinct stage declares.</summary>
    /// <remarks>
    /// The bound on tracked keys is configuration and is written down; the element type's equality is
    /// behavior and is not. <see cref="LocalDistinctParameters"/> owns the shape.
    /// </remarks>
    internal static readonly ContractReference DistinctParameterContract =
        ContractReference.Create(
            ContractId.Create("local-distinct-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a keyed stage declares.</summary>
    /// <remarks>
    /// The bound on active keys, what the key past it costs, and the chain one key's substream is made of
    /// are all configuration and are written down; the key selector, the key type's equality, and the
    /// delegates inside that chain are behavior and are not. <see cref="LocalGroupByParameters"/> owns the
    /// shape.
    /// </remarks>
    internal static readonly ContractReference GroupByParameterContract =
        ContractReference.Create(
            ContractId.Create("local-group-by-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract an interleave declares.</summary>
    /// <remarks>
    /// The rotation's segment size is configuration and is written down; how many inputs the rotation runs
    /// over is not, because the edges already say it. <see cref="LocalInterleaveParameters"/> owns the
    /// shape.
    /// </remarks>
    internal static readonly ContractReference InterleaveParameterContract =
        ContractReference.Create(
            ContractId.Create("local-interleave-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a stage configured by one duration declares.</summary>
    /// <remarks>
    /// One contract for the initial delay, the two windows, and the timeout, because a duration is a
    /// duration: which of them a node is is the stage reference's job to say, exactly as it is for the
    /// stages that share a count. <see cref="LocalDurationParameters"/> owns the shape.
    /// </remarks>
    internal static readonly ContractReference DurationParameterContract =
        ContractReference.Create(
            ContractId.Create("local-duration-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a tick source declares.</summary>
    /// <remarks>
    /// Two durations rather than one, so a contract of its own for the reason a range has one:
    /// the delay before the first tick and the interval between ticks are two numbers that mean different
    /// things. <see cref="LocalTickParameters"/> owns the shape.
    /// </remarks>
    internal static readonly ContractReference TickParameterContract =
        ContractReference.Create(ContractId.Create("local-tick-parameters"), ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a delay declares.</summary>
    /// <remarks>
    /// A duration and a holdback — a capacity and an overflow policy — which is more than the shared
    /// duration contract can say and more than a buffer's contract can.
    /// <see cref="LocalDelayParameters"/> owns the shape.
    /// </remarks>
    internal static readonly ContractReference DelayParameterContract =
        ContractReference.Create(ContractId.Create("local-delay-parameters"), ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a throttle declares.</summary>
    /// <remarks>
    /// The rate, the period, the burst, and the mode are configuration and are written down; what an
    /// element costs is behavior and is not. <see cref="LocalThrottleParameters"/> owns the shape.
    /// </remarks>
    internal static readonly ContractReference ThrottleParameterContract =
        ContractReference.Create(
            ContractId.Create("local-throttle-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a valve declares.</summary>
    /// <remarks>
    /// The state the valve starts a run in is configuration and is written down; what an author does to it
    /// while the run is running is not. <see cref="LocalValveParameters"/> owns the shape.
    /// </remarks>
    internal static readonly ContractReference ValveParameterContract =
        ContractReference.Create(ContractId.Create("local-valve-parameters"), ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a sliding window declares.</summary>
    /// <remarks>
    /// A size and a step, which is two numbers that mean different things and therefore a contract of its
    /// own rather than a share of the count contract. <see cref="LocalWindowParameters"/> owns the shape.
    /// </remarks>
    internal static readonly ContractReference WindowParameterContract =
        ContractReference.Create(
            ContractId.Create("local-window-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a batch closed by a count or a window declares.</summary>
    /// <remarks>
    /// A count and a duration together, which neither the shared count contract nor the shared duration
    /// contract can say. <see cref="LocalGroupedWithinParameters"/> owns the shape.
    /// </remarks>
    internal static readonly ContractReference GroupedWithinParameterContract =
        ContractReference.Create(
            ContractId.Create("local-grouped-within-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a batch closed by a count, a weight, or a window declares.</summary>
    /// <remarks>
    /// The three bounds are configuration and are written down; what an element weighs is behavior and is
    /// not. A contract of its own beside <see cref="GroupedWithinParameterContract"/> rather than an
    /// optional member on it, because a document that could leave the weight out could describe a stage
    /// whose binding table disagreed with it. <see cref="LocalGroupedWeightedParameters"/> owns the shape.
    /// </remarks>
    internal static readonly ContractReference GroupedWeightedParameterContract =
        ContractReference.Create(
            ContractId.Create("local-grouped-weighted-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a fault point declares.</summary>
    /// <remarks>
    /// When the stage throws and which arrival is the first to do it are configuration and are written
    /// down; what it throws is behavior and is not, for the reason the element a <c>single</c> source emits
    /// is not. <see cref="LocalFaultPointParameters"/> owns the shape.
    /// </remarks>
    internal static readonly ContractReference FaultPointParameterContract =
        ContractReference.Create(
            ContractId.Create("local-fault-point-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a supervision scope declares.</summary>
    /// <remarks>
    /// The form, the retrying form's attempts, ladder, and exhaustion answer, and <em>which stages the
    /// scope is</em> are configuration a document states; the delegates inside that chain and the fallback a
    /// recovering scope emits are behavior and are not.
    /// <see cref="LocalSupervisionParameters"/> owns the shape.
    /// </remarks>
    internal static readonly ContractReference SupervisionParameterContract =
        ContractReference.Create(
            ContractId.Create("local-supervision-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a durable scope declares.</summary>
    /// <remarks>
    /// Which stages the scope is made of and what each of them is configured with are what a document
    /// states; what each of them does, and how a scan's state becomes a canonical value, are behavior and
    /// are not. <see cref="LocalDurableParameters"/> owns the shape.
    /// </remarks>
    internal static readonly ContractReference DurableParameterContract =
        ContractReference.Create(
            ContractId.Create("local-durable-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a collecting sink declares.</summary>
    /// <remarks>
    /// The bound on collected elements is configuration and is written down; the element type is not, for
    /// the reason no local contract names one. <see cref="LocalCollectParameters"/> owns the shape.
    /// </remarks>
    internal static readonly ContractReference CollectParameterContract =
        ContractReference.Create(
            ContractId.Create("local-collect-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>The result contract the <c>result</c> port of a local fold declares.</summary>
    internal static readonly ContractReference FoldResultContract =
        ContractReference.Create(ContractId.Create("local-fold-result"), ContractReference.FirstMajorVersion);

    /// <summary>The result contract the <c>result</c> port of every other local sink declares.</summary>
    /// <remarks>
    /// Opaque for the same reason <see cref="ElementContract"/> is: a local result's type lives in the C#
    /// type system and never in the document. It is a second identity rather than one shared with
    /// <see cref="FoldResultContract"/> because a contract identifier is durable — a document already
    /// written names <c>local-fold-result</c>, and renaming it to cover sinks that do not fold would
    /// rewrite an identity rather than add one.
    /// </remarks>
    internal static readonly ContractReference ResultContract =
        ContractReference.Create(ContractId.Create("local-result"), ContractReference.FirstMajorVersion);

    /// <summary>The input port name of every local stage that consumes elements.</summary>
    internal static readonly PortId InputPort = PortId.Create("in");

    /// <summary>The output port name of every local stage that produces elements into one chain.</summary>
    internal static readonly PortId OutputPort = PortId.Create("out");

    /// <summary>The output port name of the left half of an unzipped row.</summary>
    internal static readonly PortId LeftPort = PortId.Create("left");

    /// <summary>The output port name of the right half of an unzipped row.</summary>
    internal static readonly PortId RightPort = PortId.Create("right");

    /// <summary>The input ports of every shape that consumes elements, which is the one element port.</summary>
    private static readonly InputPortSpecification[] ElementInput =
        [InputPortSpecification.Create(InputPort, ElementContract)];

    /// <summary>The input ports of a source, which are none.</summary>
    private static readonly InputPortSpecification[] NoInputs = [];

    /// <summary>The input ports a fan-in junction declares, in rotation order.</summary>
    /// <remarks>
    /// The mirror of <see cref="FanOutOutputs"/> and the same reasoning read backwards. The first two are
    /// wired or the document does not validate; the rest are optional, which is the definition plane's own
    /// spelling of "an input a graph may leave unwired", so the edges of the document are what say how many
    /// inputs this junction joins. Ordinal order over the names is the order a concat consumes its inputs
    /// in and the order an interleave rotates through them, which is why the port list and the rotation are
    /// one statement rather than two.
    /// </remarks>
    private static readonly InputPortSpecification[] FanInInputs = FanInPorts();

    /// <summary>The output ports of every shape that produces one stream, which is the one element port.</summary>
    private static readonly OutputPortSpecification[] ElementOutput =
        [OutputPortSpecification.Create(OutputPort, ElementContract)];

    /// <summary>The output ports of a terminal, which are none.</summary>
    private static readonly OutputPortSpecification[] NoOutputs = [];

    /// <summary>The output ports an unzip declares, which are the two halves of a row.</summary>
    private static readonly OutputPortSpecification[] RowOutputs =
    [
        OutputPortSpecification.Create(LeftPort, ElementContract),
        OutputPortSpecification.Create(RightPort, ElementContract),
    ];

    /// <summary>The output ports a broadcast, a balance, or a partition declares, in rotation order.</summary>
    /// <remarks>
    /// The first two are wired or the document does not validate — a junction with one leg is a chain
    /// written the long way, and one with none is a discarding sink. The rest are ignorable, which is the
    /// definition plane's own spelling of "an output a graph may leave unwired": the edges of the document
    /// are what say how many legs this junction has, and the runtime reads exactly them. That is why a
    /// junction carries no parameter payload at all — an arity written down beside the edges would be a
    /// second statement of the same fact, and two statements can disagree.
    /// </remarks>
    private static readonly OutputPortSpecification[] FanOutOutputs = FanOutPorts();

    /// <summary>The result contract the <c>control</c> port of a local ingress queue declares.</summary>
    /// <remarks>
    /// A third result identity rather than a reuse of <see cref="ResultContract"/>, because a control is
    /// not a result: its value exists at the start of a run rather than at its end, and a document that
    /// said the two were one contract would lose the only statement it makes about when a slot resolves.
    /// The value itself stays opaque for the reason every local contract is opaque — its type lives in the
    /// C# type system and never in the document.
    /// </remarks>
    internal static readonly ContractReference ControlContract =
        ContractReference.Create(ContractId.Create("local-control"), ContractReference.FirstMajorVersion);

    /// <summary>The result port name of every local sink that produces a result.</summary>
    internal static readonly PortId ResultPort = PortId.Create("result");

    /// <summary>The result port name of every local stage that produces a runtime control.</summary>
    /// <remarks>
    /// A port of its own beside <see cref="ResultPort"/>, so that a node can one day declare both and so
    /// that a reader of a document can tell a control from a result without knowing which stage produced
    /// it.
    /// </remarks>
    internal static readonly PortId ControlPort = PortId.Create("control");

    /// <summary>The parameter payload a local stage whose whole behavior is a delegate carries.</summary>
    /// <remarks>
    /// Empty because there is nothing to say, not because payloads are forbidden: the counted, ranged,
    /// buffered, distinct, and asynchronous stages write real ones.
    /// <see cref="LocalStageDescriptor.Parameters"/> is what decides which a given occurrence carries.
    /// </remarks>
    internal static readonly CanonicalJsonValue EmptyParameters = CanonicalJsonValue.Parse("{}");

    /// <summary>Every shape of this vocabulary by the text its stage reference renders as.</summary>
    /// <remarks>
    /// Declared here rather than beside <see cref="TryReadStage"/> because it composes every stage
    /// reference above it, and the fields of this type are initialized in textual order. It is built from
    /// <see cref="StageOf"/> so that the two directions cannot disagree, and it is ordinal because a stage
    /// reference is machine text: <c>local/take@v1</c> is one stage and nothing about a reader's culture
    /// changes that.
    /// </remarks>
    private static readonly Dictionary<string, LocalStageKind> Shapes = Enum
        .GetValues<LocalStageKind>()
        .ToDictionary(kind => StageOf(kind).ToString(), kind => kind, StringComparer.Ordinal);

    /// <summary>The capability token a document with automatically named occurrences declares.</summary>
    /// <remarks>
    /// This is the well-known token of ADR 0004 section 6, promoted onto
    /// <see cref="CapabilityToken"/> beside <see cref="CapabilityToken.Nondeployable"/>; the alias here
    /// keeps the vocabulary's callers reading in one place.
    /// </remarks>
    internal static readonly CapabilityToken EphemeralIdentity = CapabilityToken.EphemeralIdentity;

    /// <summary>The capabilities every local stage requires of the document that contains it.</summary>
    /// <remarks>
    /// One list, read both by <see cref="Orleans.Dataflow.LocalStageCatalog"/> when it declares what each
    /// local stage requires and by <see cref="LocalStageDescriptor"/> when an occurrence states what its
    /// document must declare. They have to agree exactly — the graph compiler's
    /// <c>undeclared-capability</c> rule rejects a document that declares less than its stages require —
    /// and one list is how they agree by construction rather than by two constants that happen to match.
    /// This is also the whole of "nondeployable if and only if the graph holds a lambda stage": every
    /// local stage requires the token and no registered one does, so the closed document's tokens are a
    /// fact derived from its occurrences.
    /// </remarks>
    internal static readonly IReadOnlyList<CapabilityToken> RequiredCapabilities =
        Array.AsReadOnly<CapabilityToken>([CapabilityToken.Nondeployable]);

    /// <summary>The token a graph declaring state that survives a resume carries.</summary>
    /// <remarks>
    /// ADR 0007's <c>durable-state</c>, which has existed as a word since M0 and earns its keep here: a
    /// checkpoint carries the state of the stages inside a scope that declares this token and of nothing
    /// else, so the token is what tells a host that this graph expects state to survive a process. It is
    /// created here rather than promoted onto <see cref="CapabilityToken"/> beside
    /// <see cref="CapabilityToken.Nondeployable"/>, because a durable scope is a stage of <em>this</em>
    /// vocabulary: the Abstractions package has no concept a shared static would serve, and the token
    /// grammar is open precisely so a feature can name its own.
    /// </remarks>
    internal static readonly CapabilityToken DurableState = CapabilityToken.Create("durable-state");

    /// <summary>The capabilities every stage of this vocabulary requires, plus a durable scope's own.</summary>
    /// <remarks>
    /// Read by <see cref="Orleans.Dataflow.LocalStageCatalog"/> and by
    /// <see cref="LocalStageDescriptor"/> alike, so what a specification requires and what an occurrence
    /// declares agree by construction rather than by two lists that happen to match — the same rule
    /// <see cref="RequiredCapabilities"/> already carried, read one shape further.
    /// </remarks>
    private static readonly IReadOnlyList<CapabilityToken> DurableCapabilities =
        Array.AsReadOnly<CapabilityToken>([CapabilityToken.Nondeployable, DurableState]);

    /// <summary>The graph identity every locally authored, unnamed graph carries.</summary>
    /// <remarks>
    /// A <see cref="GraphDocument"/> always has an identity, and a graph built from lambdas has no author
    /// who gave it one, so every such document carries the same placeholder. That is deliberate rather than
    /// unfortunate: ADR 0004 section 4 binds a result slot to the document's
    /// <see cref="Definition.GraphFingerprint"/>, which requires two content-identical documents to be
    /// byte-identical, and a per-instance identity would defeat exactly that. Named deployable pipelines
    /// keep <see cref="GraphId"/> plus revision as their upgrade lineage; this constant is what stands in
    /// its place until they exist.
    /// </remarks>
    internal static readonly GraphId AnonymousGraph = GraphId.Create("anonymous");

    /// <summary>The revision every locally authored, unnamed graph carries.</summary>
    internal static readonly GraphRevision FirstRevision =
        GraphRevision.Create(GraphRevision.FirstRevisionNumber);

    /// <summary>Returns the capabilities an occurrence of <paramref name="kind"/> requires of its document.</summary>
    /// <param name="kind">The stage shape.</param>
    /// <returns>The tokens the containing document has to declare.</returns>
    /// <remarks>
    /// <c>nondeployable</c> for every shape without exception, and <c>durable-state</c> for the one shape
    /// that asks a host to keep state across a process. A document's tokens stay a fact derived from its
    /// occurrences rather than something an author remembers to write.
    /// </remarks>
    internal static IReadOnlyList<CapabilityToken> RequiredCapabilitiesOf(LocalStageKind kind) =>
        kind is LocalStageKind.Durable ? DurableCapabilities : RequiredCapabilities;

    /// <summary>Returns the stage reference an occurrence of <paramref name="kind"/> declares.</summary>
    /// <param name="kind">The stage shape.</param>
    /// <returns>The stage reference.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="kind"/> is not a declared member.</exception>
    internal static StageRef StageOf(LocalStageKind kind) => kind switch
    {
        LocalStageKind.FromEnumerable => FromEnumerable,
        LocalStageKind.Empty => Empty,
        LocalStageKind.Single => Single,
        LocalStageKind.Repeat => Repeat,
        LocalStageKind.Range => Range,
        LocalStageKind.FromTask => FromTask,
        LocalStageKind.Failed => Failed,
        LocalStageKind.Unfold => Unfold,
        LocalStageKind.FromAsyncEnumerable => FromAsyncEnumerable,
        LocalStageKind.FromFactory => FromFactory,
        LocalStageKind.FromAsyncFactory => FromAsyncFactory,
        LocalStageKind.Never => Never,
        LocalStageKind.Cycle => Cycle,
        LocalStageKind.UnfoldAsync => UnfoldAsync,
        LocalStageKind.Queue => Queue,
        LocalStageKind.FromChannel => FromChannel,
        LocalStageKind.Tick => Tick,
        LocalStageKind.Select => Select,
        LocalStageKind.Where => Where,
        LocalStageKind.Scan => Scan,
        LocalStageKind.Take => Take,
        LocalStageKind.Skip => Skip,
        LocalStageKind.TakeWhile => TakeWhile,
        LocalStageKind.TakeThrough => TakeThrough,
        LocalStageKind.SkipWhile => SkipWhile,
        LocalStageKind.Distinct => Distinct,
        LocalStageKind.GroupBy => GroupBy,
        LocalStageKind.DeduplicateConsecutive => DeduplicateConsecutive,
        LocalStageKind.SelectMany => SelectMany,
        LocalStageKind.MergeMap => MergeMap,
        LocalStageKind.ScanAsync => ScanAsync,
        LocalStageKind.Grouped => Grouped,
        LocalStageKind.Sliding => Sliding,
        LocalStageKind.GroupedWithin => GroupedWithin,
        LocalStageKind.GroupedWeightedWithin => GroupedWeightedWithin,
        LocalStageKind.Buffer => Buffer,
        LocalStageKind.SelectAsync => SelectAsync,
        LocalStageKind.SelectAsyncUnordered => SelectAsyncUnordered,
        LocalStageKind.SelectValueTaskAsync => SelectValueTaskAsync,
        LocalStageKind.SelectValueTaskAsyncUnordered => SelectValueTaskAsyncUnordered,
        LocalStageKind.Delay => Delay,
        LocalStageKind.InitialDelay => InitialDelay,
        LocalStageKind.Timeout => Timeout,
        LocalStageKind.TakeWithin => TakeWithin,
        LocalStageKind.SkipWithin => SkipWithin,
        LocalStageKind.Throttle => Throttle,
        LocalStageKind.Valve => Valve,
        LocalStageKind.Broadcast => Broadcast,
        LocalStageKind.Balance => Balance,
        LocalStageKind.Partition => Partition,
        LocalStageKind.Unzip => Unzip,
        LocalStageKind.Merge => Merge,
        LocalStageKind.Concat => Concat,
        LocalStageKind.Interleave => Interleave,
        LocalStageKind.Zip => Zip,
        LocalStageKind.CombineLatest => CombineLatest,
        LocalStageKind.Fold => Fold,
        LocalStageKind.FoldAsync => FoldAsync,
        LocalStageKind.Ignore => Ignore,
        LocalStageKind.ForEach => ForEach,
        LocalStageKind.ForEachAsync => ForEachAsync,
        LocalStageKind.First => First,
        LocalStageKind.FirstOrDefault => FirstOrDefault,
        LocalStageKind.Count => Count,
        LocalStageKind.Last => Last,
        LocalStageKind.LastOrDefault => LastOrDefault,
        LocalStageKind.Collect => Collect,
        LocalStageKind.ToChannel => ToChannel,
        LocalStageKind.SinkProbe => SinkProbe,
        LocalStageKind.FaultPoint => FaultPoint,
        LocalStageKind.Supervised => Supervised,
        LocalStageKind.Durable => Durable,
        LocalStageKind.MarkingSink => MarkingSink,
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    /// <summary>Returns the parameter contract an occurrence of <paramref name="kind"/> declares.</summary>
    /// <param name="kind">The stage shape.</param>
    /// <returns>The contract reference.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="kind"/> is not a declared member.</exception>
    /// <remarks>
    /// The distinction is not "which stages happen to have options" but "which stages have options a
    /// document can state honestly", and honest is wider than numeric: the arms below carry capacities,
    /// concurrency bounds, counts, ranges, durations written as ticks, names drawn from closed sets — an
    /// overflow policy, a valve's starting position, a supervision form, a retry exhaustion answer — and,
    /// for the three scope-bearing shapes, a whole inner chain. A delegate is the one kind of value
    /// disqualified, because a delegate is never durable topology. The last arm is the empty payload, and
    /// the arms are themselves the enumeration: which shapes carry what is read off this method, and a
    /// count written here would stop being true the next time the vocabulary grew.
    /// </remarks>
    internal static ContractReference ParameterContractOf(LocalStageKind kind) => kind switch
    {
        LocalStageKind.Buffer or LocalStageKind.Queue => BufferParameterContract,
        LocalStageKind.SelectAsync or
            LocalStageKind.SelectAsyncUnordered or
            LocalStageKind.SelectValueTaskAsync or
            LocalStageKind.SelectValueTaskAsyncUnordered or
            LocalStageKind.ForEachAsync or
            LocalStageKind.MergeMap => ParallelismParameterContract,
        LocalStageKind.Take or
            LocalStageKind.Skip or
            LocalStageKind.Repeat or
            LocalStageKind.Grouped => CountParameterContract,
        LocalStageKind.Sliding => WindowParameterContract,
        LocalStageKind.GroupedWithin => GroupedWithinParameterContract,
        LocalStageKind.GroupedWeightedWithin => GroupedWeightedParameterContract,
        LocalStageKind.Range => RangeParameterContract,
        LocalStageKind.Tick => TickParameterContract,
        LocalStageKind.Delay => DelayParameterContract,
        LocalStageKind.Throttle => ThrottleParameterContract,
        LocalStageKind.Valve => ValveParameterContract,
        LocalStageKind.InitialDelay or
            LocalStageKind.Timeout or
            LocalStageKind.TakeWithin or
            LocalStageKind.SkipWithin => DurationParameterContract,
        LocalStageKind.Distinct => DistinctParameterContract,
        LocalStageKind.GroupBy => GroupByParameterContract,
        LocalStageKind.FaultPoint => FaultPointParameterContract,
        LocalStageKind.Supervised => SupervisionParameterContract,
        LocalStageKind.Durable => DurableParameterContract,
        LocalStageKind.Collect => CollectParameterContract,
        LocalStageKind.Interleave => InterleaveParameterContract,
        LocalStageKind.FromEnumerable or
            LocalStageKind.Empty or
            LocalStageKind.Single or
            LocalStageKind.FromTask or
            LocalStageKind.Failed or
            LocalStageKind.Unfold or
            LocalStageKind.FromAsyncEnumerable or
            LocalStageKind.FromFactory or
            LocalStageKind.FromAsyncFactory or
            LocalStageKind.Never or
            LocalStageKind.Cycle or
            LocalStageKind.UnfoldAsync or
            LocalStageKind.FromChannel or
            LocalStageKind.Select or
            LocalStageKind.Broadcast or
            LocalStageKind.Balance or
            LocalStageKind.Partition or
            LocalStageKind.Unzip or
            LocalStageKind.Merge or
            LocalStageKind.Concat or
            LocalStageKind.Zip or
            LocalStageKind.CombineLatest or
            LocalStageKind.Where or
            LocalStageKind.DeduplicateConsecutive or
            LocalStageKind.SelectMany or
            LocalStageKind.Scan or
            LocalStageKind.ScanAsync or
            LocalStageKind.FoldAsync or
            LocalStageKind.TakeWhile or
            LocalStageKind.TakeThrough or
            LocalStageKind.SkipWhile or
            LocalStageKind.Fold or
            LocalStageKind.Ignore or
            LocalStageKind.ForEach or
            LocalStageKind.First or
            LocalStageKind.FirstOrDefault or
            LocalStageKind.Count or
            LocalStageKind.Last or
            LocalStageKind.LastOrDefault or
            LocalStageKind.ToChannel or
            LocalStageKind.SinkProbe or
            LocalStageKind.MarkingSink => ParameterContract,
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    /// <summary>Returns the check an occurrence of <paramref name="kind"/> applies to its payload.</summary>
    /// <param name="kind">The stage shape.</param>
    /// <returns>The validator, or <see langword="null"/> when the shape carries the empty payload.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="kind"/> is not a declared member.</exception>
    /// <remarks>
    /// A shape with no parameters needs no validator: the contract match already rejects every payload but
    /// the empty object this vocabulary writes, and there is nothing inside it to disagree with. Every
    /// shape that does carry numbers brings the very reader the runtime uses, so what the catalog accepts
    /// is exactly what a run can execute.
    /// </remarks>
    internal static IStageParameterValidator? ParameterValidatorOf(LocalStageKind kind)
    {
        ContractReference contract = ParameterContractOf(kind);

        return contract switch
        {
            _ when contract == BufferParameterContract => LocalBufferParameters.Validator,
            _ when contract == ParallelismParameterContract => LocalParallelismParameters.Validator,
            _ when contract == CountParameterContract => LocalCountParameters.Validator,
            _ when contract == RangeParameterContract => LocalRangeParameters.Validator,
            _ when contract == DistinctParameterContract => LocalDistinctParameters.Validator,
            _ when contract == GroupByParameterContract => LocalGroupByParameters.Validator,
            _ when contract == FaultPointParameterContract => LocalFaultPointParameters.Validator,
            _ when contract == SupervisionParameterContract => LocalSupervisionParameters.Validator,
            _ when contract == DurableParameterContract => LocalDurableParameters.Validator,
            _ when contract == WindowParameterContract => LocalWindowParameters.Validator,
            _ when contract == GroupedWithinParameterContract => LocalGroupedWithinParameters.Validator,
            _ when contract == GroupedWeightedParameterContract => LocalGroupedWeightedParameters.Validator,
            _ when contract == CollectParameterContract => LocalCollectParameters.Validator,
            _ when contract == InterleaveParameterContract => LocalInterleaveParameters.Validator,
            _ when contract == DurationParameterContract => LocalDurationParameters.Validator,
            _ when contract == TickParameterContract => LocalTickParameters.Validator,
            _ when contract == DelayParameterContract => LocalDelayParameters.Validator,
            _ when contract == ThrottleParameterContract => LocalThrottleParameters.Validator,
            _ when contract == ValveParameterContract => LocalValveParameters.Validator,
            _ => null,
        };
    }

    /// <summary>Returns where in a chain an occurrence of <paramref name="kind"/> stands.</summary>
    /// <param name="kind">The stage shape.</param>
    /// <returns>The place.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="kind"/> is not a declared member.</exception>
    /// <remarks>
    /// The one exhaustive classification of the vocabulary, from which the declared ports follow: a source
    /// consumes nothing and a terminal produces nothing. A shape added without a place named here fails to
    /// compile into a specification at all, rather than becoming a stage with the ports of whichever arm it
    /// fell into.
    /// </remarks>
    internal static LocalStagePlace PlaceOf(LocalStageKind kind) => kind switch
    {
        LocalStageKind.FromEnumerable or
            LocalStageKind.Empty or
            LocalStageKind.Single or
            LocalStageKind.Repeat or
            LocalStageKind.Range or
            LocalStageKind.FromTask or
            LocalStageKind.Failed or
            LocalStageKind.Unfold or
            LocalStageKind.FromAsyncEnumerable or
            LocalStageKind.FromFactory or
            LocalStageKind.FromAsyncFactory or
            LocalStageKind.Never or
            LocalStageKind.Cycle or
            LocalStageKind.UnfoldAsync or
            LocalStageKind.Queue or
            LocalStageKind.FromChannel or
            LocalStageKind.Tick => LocalStagePlace.Source,
        LocalStageKind.Select or
            LocalStageKind.Where or
            LocalStageKind.DeduplicateConsecutive or
            LocalStageKind.SelectMany or
            LocalStageKind.Scan or
            LocalStageKind.Take or
            LocalStageKind.Skip or
            LocalStageKind.TakeWhile or
            LocalStageKind.TakeThrough or
            LocalStageKind.SkipWhile or
            LocalStageKind.Distinct or
            LocalStageKind.GroupBy or
            LocalStageKind.Grouped or
            LocalStageKind.Sliding or
            LocalStageKind.GroupedWithin or
            LocalStageKind.GroupedWeightedWithin or
            LocalStageKind.Buffer or
            LocalStageKind.SelectAsync or
            LocalStageKind.SelectAsyncUnordered or
            LocalStageKind.SelectValueTaskAsync or
            LocalStageKind.SelectValueTaskAsyncUnordered or
            LocalStageKind.MergeMap or
            LocalStageKind.ScanAsync or
            LocalStageKind.Delay or
            LocalStageKind.InitialDelay or
            LocalStageKind.Timeout or
            LocalStageKind.TakeWithin or
            LocalStageKind.SkipWithin or
            LocalStageKind.Throttle or
            LocalStageKind.FaultPoint or
            LocalStageKind.Supervised or
            LocalStageKind.Durable or
            LocalStageKind.Valve => LocalStagePlace.Operator,
        LocalStageKind.Broadcast or
            LocalStageKind.Balance or
            LocalStageKind.Partition or
            LocalStageKind.Unzip => LocalStagePlace.FanOut,
        LocalStageKind.Merge or
            LocalStageKind.Concat or
            LocalStageKind.Interleave or
            LocalStageKind.Zip or
            LocalStageKind.CombineLatest => LocalStagePlace.FanIn,
        LocalStageKind.Fold or
            LocalStageKind.FoldAsync or
            LocalStageKind.Ignore or
            LocalStageKind.ForEach or
            LocalStageKind.ForEachAsync or
            LocalStageKind.First or
            LocalStageKind.FirstOrDefault or
            LocalStageKind.Count or
            LocalStageKind.Last or
            LocalStageKind.LastOrDefault or
            LocalStageKind.Collect or
            LocalStageKind.ToChannel or
            LocalStageKind.SinkProbe or
            LocalStageKind.MarkingSink => LocalStagePlace.Terminal,
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    /// <summary>Reports whether an occurrence of <paramref name="kind"/> may stand inside a group flow.</summary>
    /// <param name="kind">The stage shape.</param>
    /// <returns><see langword="true"/> for the shapes a keyed stage can run one instance of per key.</returns>
    /// <remarks>
    /// <para>
    /// The admitted list is exactly the shapes that are <em>a function of an element and their own state</em>
    /// — nothing else can be instantiated per key inside one fused stage. An asynchronous stage, a
    /// merge-map, and a buffer all want a segment and a channel of their own; a junction wants several; a
    /// clock-reading stage wants a run to attach to and, for two of them, a timer that can complete or fail
    /// the run; a source and a terminal are not stages of a chain at all.
    /// </para>
    /// <para>
    /// Two of the admitted-looking shapes are refused for reasons of this operator's own rather than of
    /// their machinery. <see cref="LocalStageKind.SelectMany"/> would have its inner sequence
    /// <em>materialized</em> rather than streamed, because what a keyed stage hands the run is one sequence
    /// per element and the run reads it after the stage has returned — so an author's endless inner sequence
    /// would stop being bounded by the boundary below, which is the one thing this operator exists to
    /// promise. And <see cref="LocalStageKind.GroupBy"/> inside a group flow would be a second bound and a
    /// second key table per key of the first, which is a real feature and is not this one.
    /// </para>
    /// <para>
    /// Two shapes added in M5.1 are refused here too. A <see cref="LocalStageKind.Supervised"/> scope reads
    /// the run's clock, so it falls under the clause above that refuses every stage wanting a run of its
    /// own. A <see cref="LocalStageKind.FaultPoint"/> counts the arrivals it has seen, and one counter per
    /// key is not what "fail the second element" means to the test that wrote it; refusing it by name is
    /// honest, and a fault point placed before or after a keyed stage says exactly what it meant.
    /// </para>
    /// </remarks>
    internal static bool RunsInsideAGroup(LocalStageKind kind) => kind switch
    {
        LocalStageKind.Select or
            LocalStageKind.Where or
            LocalStageKind.Scan or
            LocalStageKind.Take or
            LocalStageKind.Skip or
            LocalStageKind.TakeWhile or
            LocalStageKind.TakeThrough or
            LocalStageKind.SkipWhile or
            LocalStageKind.Distinct or
            LocalStageKind.DeduplicateConsecutive or
            LocalStageKind.Grouped or
            LocalStageKind.Sliding => true,
        _ => false,
    };

    /// <summary>Reports whether an occurrence of <paramref name="kind"/> may stand inside a supervision scope.</summary>
    /// <param name="kind">The stage shape.</param>
    /// <returns><see langword="true"/> for the shapes a scope can own the per-element execution of.</returns>
    /// <remarks>
    /// <para>
    /// The group flow's list plus the fault point, and the two differences are the whole of what a scope is
    /// against what a keyed stage is. A scope owns <b>one</b> instance of its chain rather than one per key,
    /// so a fault point's arrival counter means what a test wrote down; and a scope has to be able to see a
    /// failure raised inside it, which is what rules the remaining shapes out for a reason of this
    /// operator's own rather than of their machinery.
    /// </para>
    /// <para>
    /// <see cref="LocalStageKind.SelectMany"/> is the sharpest of those. What a scope hands the run for a
    /// flattening stage is a sequence the run reads <em>after</em> the scope's own method has returned, so a
    /// failure raised while that sequence is enumerated would happen outside the scope it appears to be
    /// inside — supervision that silently did not apply, which is worse than a refusal. A nested
    /// <see cref="LocalStageKind.Supervised"/> and a <see cref="LocalStageKind.GroupBy"/> are refused as
    /// this version's honesty: a policy inside a policy and a key table whose reset is a scope's business
    /// are each a real feature with a contract to state, and neither is this one.
    /// </para>
    /// </remarks>
    internal static bool RunsInsideAScope(LocalStageKind kind) =>
        kind is LocalStageKind.FaultPoint || RunsInsideAGroup(kind);

    /// <summary>Reports whether an occurrence of <paramref name="kind"/> may stand inside a durable scope.</summary>
    /// <param name="kind">The stage shape.</param>
    /// <returns><see langword="true"/> for the shapes whose state a checkpoint could carry at all.</returns>
    /// <remarks>
    /// <para>
    /// The shortest of the three inner-chain lists, and the reason it is short is the one thing a durable
    /// scope promises: a stage inside one has to be able to hand its state over <b>as a canonical value</b>.
    /// A <c>select</c> and a <c>where</c> hold nothing between elements; a <c>take</c> and a <c>skip</c> hold
    /// a count, which is a number any document plane can carry; a <c>scan</c> holds a value of a type no
    /// document names and can therefore export only when its author bound a codec, which is a fact of the
    /// binding and is checked when the plan is built rather than here. A fault point holds no state of the
    /// author's at all — its arrival counter belongs to the run, exactly as M5.1 said of a restart — so it
    /// composes with a durable scope and exports nothing.
    /// </para>
    /// <para>
    /// Everything else is refused <b>by name</b>, which is what the facet buys. A <c>distinct</c> remembers
    /// keys of an unnamed type, a <c>grouped</c> and a <c>sliding</c> hold elements of one, and a
    /// <c>take-while</c> and a <c>skip-while</c> hold a latch that only means anything beside a predicate
    /// the document does not carry either. Admitting any of them would produce a resume that silently reset
    /// state the scope had promised to keep, which is strictly worse than a refusal an author can read.
    /// </para>
    /// </remarks>
    internal static bool RunsInsideADurableScope(LocalStageKind kind) => kind switch
    {
        LocalStageKind.Select or
            LocalStageKind.Where or
            LocalStageKind.Scan or
            LocalStageKind.Take or
            LocalStageKind.Skip or
            LocalStageKind.FaultPoint => true,
        _ => false,
    };

    /// <summary>Recovers the shape a stage reference names, when this vocabulary declares one.</summary>
    /// <param name="stage">The reference as a document spells it, such as <c>local/take@v1</c>.</param>
    /// <param name="kind">
    /// When this method returns <see langword="true"/>, the shape; otherwise an unspecified value.
    /// </param>
    /// <returns><see langword="true"/> when the text names a stage of this vocabulary.</returns>
    /// <remarks>
    /// The one place that reads a stage reference back into a shape, and it is built from
    /// <see cref="StageOf"/> rather than written out, so a stage added to the vocabulary is recoverable
    /// here without anybody remembering to add it. A group flow's payload is the only thing that needs
    /// this: everywhere else a document's node carries its reference as a value and nothing has to parse
    /// one.
    /// </remarks>
    internal static bool TryReadStage(string stage, out LocalStageKind kind) =>
        Shapes.TryGetValue(stage, out kind);

    /// <summary>Reports whether an occurrence of <paramref name="kind"/> consumes elements.</summary>
    /// <param name="kind">The stage shape.</param>
    /// <returns><see langword="true"/> for every shape but a source.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="kind"/> is not a declared member.</exception>
    internal static bool ConsumesElements(LocalStageKind kind) =>
        PlaceOf(kind) is not LocalStagePlace.Source;

    /// <summary>Reports whether an occurrence of <paramref name="kind"/> produces elements.</summary>
    /// <param name="kind">The stage shape.</param>
    /// <returns><see langword="true"/> for every shape but a terminal.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="kind"/> is not a declared member.</exception>
    internal static bool ProducesElements(LocalStageKind kind) =>
        PlaceOf(kind) is not LocalStagePlace.Terminal;

    /// <summary>Returns the input ports an occurrence of <paramref name="kind"/> declares.</summary>
    /// <param name="kind">The stage shape.</param>
    /// <returns>
    /// The numbered inputs for a fan-in junction, the one element input port for every other consuming
    /// shape, and an empty list for a source.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="kind"/> is not a declared member.</exception>
    /// <remarks>
    /// A fan-out has one input by definition and every operator and terminal has one because a branch is a
    /// chain; the fan-in junctions are the only shapes whose answer is a list, and it is the same list for
    /// every one of them because what differs between them is which input they read next rather than how
    /// many they have.
    /// </remarks>
    internal static IReadOnlyList<InputPortSpecification> InputPortsOf(LocalStageKind kind) =>
        PlaceOf(kind) switch
        {
            LocalStagePlace.Source => NoInputs,
            LocalStagePlace.FanIn => FanInInputs,
            _ => ElementInput,
        };

    /// <summary>Returns the output ports an occurrence of <paramref name="kind"/> declares.</summary>
    /// <param name="kind">The stage shape.</param>
    /// <returns>
    /// The two halves of a row for an unzip, the numbered legs for the other two junctions, the one element
    /// output port for every other producing shape, and an empty list for a terminal.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="kind"/> is not a declared member.</exception>
    internal static IReadOnlyList<OutputPortSpecification> OutputPortsOf(LocalStageKind kind) => kind switch
    {
        LocalStageKind.Unzip => RowOutputs,
        LocalStageKind.Broadcast or LocalStageKind.Balance or LocalStageKind.Partition => FanOutOutputs,
        _ => ProducesElements(kind) ? ElementOutput : NoOutputs,
    };

    /// <summary>Builds the numbered output ports of a broadcast, a balance, or a partition.</summary>
    /// <returns>The ports, <see cref="MinFanOut"/> of them required and the rest ignorable.</returns>
    /// <remarks>
    /// Ordinal order over the names is rotation order, which is what a balance distributes in, what a
    /// broadcast asks for room in, and what a partition's routing function answers with. The number is
    /// formatted with the invariant culture for the reason every
    /// identifier in this vocabulary is: a culture with non-ASCII digits would otherwise produce a name the
    /// identifier grammar rejects.
    /// </remarks>
    private static OutputPortSpecification[] FanOutPorts()
    {
        OutputPortSpecification[] ports = new OutputPortSpecification[MaxFanOut];

        for (int index = 0; index < ports.Length; index++)
        {
            ports[index] = OutputPortSpecification.Create(
                FanOutPort(index),
                ElementContract,
                isIgnorable: index >= MinFanOut);
        }

        return ports;
    }

    /// <summary>Builds the name of one numbered fan-out port.</summary>
    /// <param name="leg">The zero-based position of the leg.</param>
    /// <returns>The port name, such as <c>out-0</c>.</returns>
    internal static PortId FanOutPort(int leg) =>
        PortId.Create(FanOutPortPrefix + leg.ToString(CultureInfo.InvariantCulture));

    /// <summary>Builds the numbered input ports of a fan-in junction.</summary>
    /// <returns>The ports, <see cref="MinFanIn"/> of them required and the rest optional.</returns>
    /// <remarks>
    /// Ordinal order over the names is the order a concat consumes and an interleave rotates in, and it is
    /// where a merge's rotation starts. The number is formatted with the invariant culture for the reason
    /// every identifier in this vocabulary is: a culture with non-ASCII digits would otherwise produce a
    /// name the identifier grammar rejects.
    /// </remarks>
    private static InputPortSpecification[] FanInPorts()
    {
        InputPortSpecification[] ports = new InputPortSpecification[MaxFanIn];

        for (int index = 0; index < ports.Length; index++)
        {
            ports[index] = InputPortSpecification.Create(
                FanInPort(index),
                ElementContract,
                isOptional: index >= MinFanIn);
        }

        return ports;
    }

    /// <summary>Builds the name of one numbered fan-in port.</summary>
    /// <param name="input">The zero-based position of the input.</param>
    /// <returns>The port name, such as <c>in-0</c>.</returns>
    internal static PortId FanInPort(int input) =>
        PortId.Create(FanInPortPrefix + input.ToString(CultureInfo.InvariantCulture));

    /// <summary>Returns the result port an occurrence of <paramref name="kind"/> declares.</summary>
    /// <param name="kind">The stage shape.</param>
    /// <returns>The port and its contract, or <see langword="null"/> when the shape declares neither.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="kind"/> is not a declared member.</exception>
    /// <remarks>
    /// Two kinds of value are declared here and the port name is what tells them apart. A sink's
    /// <c>result</c> is what the run accumulated and resolves when the run ends; a <c>control</c> is a
    /// per-run object an author reaches by name and resolves when the run starts — the queue producers push
    /// through at the head of a chain, the receiver a probe sink hands its elements to at the end of one.
    /// Both are result slots, which is what ADR 0002 said when it listed a queue control beside a fold
    /// result, and both travel through the same declaration in a document.
    /// </remarks>
    internal static ResultPortSpecification? ResultPortOf(LocalStageKind kind)
    {
        // Asked for its rejection rather than for its answer: a value no member declares is not a shape
        // with no result, and returning null for one would let a cast from an arbitrary integer become a
        // node this vocabulary appears to describe.
        _ = PlaceOf(kind);

        return kind switch
        {
            LocalStageKind.Fold or
                LocalStageKind.FoldAsync => ResultPortSpecification.Create(ResultPort, FoldResultContract),
            LocalStageKind.First or
                LocalStageKind.FirstOrDefault or
                LocalStageKind.Count or
                LocalStageKind.Last or
                LocalStageKind.LastOrDefault or
                LocalStageKind.Collect => ResultPortSpecification.Create(ResultPort, ResultContract),
            LocalStageKind.Queue or
                LocalStageKind.Valve or
                LocalStageKind.SinkProbe or
                LocalStageKind.MarkingSink or
                LocalStageKind.FaultPoint => ResultPortSpecification.Create(ControlPort, ControlContract),
            _ => null,
        };
    }

    /// <summary>Builds the node identifier of the occurrence at one position of an authoring chain.</summary>
    /// <param name="position">
    /// The one-based position in authoring order, which must not exceed
    /// <see cref="MaxAutoNamedPosition"/>; the caller enforces that bound before allocating anything.
    /// </param>
    /// <returns>The identifier, such as <c>stage-0001</c>.</returns>
    /// <remarks>
    /// The position is padded to four digits, so identifiers of one graph sort ordinally in the order they
    /// were authored in: unpadded, <c>stage-10</c> sorts before <c>stage-2</c>, and a document's canonical
    /// node order would stop being its authoring order at the tenth occurrence. The number is formatted
    /// with the invariant culture, so the identifier is the same text under every ambient culture; a
    /// culture with non-ASCII digits would otherwise produce a value the identifier grammar rejects.
    /// </remarks>
    internal static NodeId AutoName(int position) =>
        NodeId.Create(AutoNamePrefix + position.ToString(AutoNameNumberFormat, CultureInfo.InvariantCulture));
}
