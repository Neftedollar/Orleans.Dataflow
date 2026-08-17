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

    /// <summary>The greatest number of outputs a broadcast or a balance declares.</summary>
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

    /// <summary>The stage reference of a folding sink.</summary>
    internal static readonly StageRef Fold =
        StageRef.Create(Provider, StageId.Create("fold"), StageRef.FirstMajorVersion);

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
    /// One contract for <c>take</c>, <c>skip</c>, and <c>repeat</c>, because a count is a count: the three
    /// carry the same member under the same rules, and which of them is meant is the stage reference's job
    /// to say. <see cref="LocalCountParameters"/> owns the shape.
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

    /// <summary>The output ports a broadcast or a balance declares, in rotation order.</summary>
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
        LocalStageKind.Select => Select,
        LocalStageKind.Where => Where,
        LocalStageKind.Scan => Scan,
        LocalStageKind.Take => Take,
        LocalStageKind.Skip => Skip,
        LocalStageKind.TakeWhile => TakeWhile,
        LocalStageKind.TakeThrough => TakeThrough,
        LocalStageKind.SkipWhile => SkipWhile,
        LocalStageKind.Distinct => Distinct,
        LocalStageKind.Buffer => Buffer,
        LocalStageKind.SelectAsync => SelectAsync,
        LocalStageKind.SelectAsyncUnordered => SelectAsyncUnordered,
        LocalStageKind.SelectValueTaskAsync => SelectValueTaskAsync,
        LocalStageKind.SelectValueTaskAsyncUnordered => SelectValueTaskAsyncUnordered,
        LocalStageKind.Broadcast => Broadcast,
        LocalStageKind.Balance => Balance,
        LocalStageKind.Unzip => Unzip,
        LocalStageKind.Merge => Merge,
        LocalStageKind.Concat => Concat,
        LocalStageKind.Interleave => Interleave,
        LocalStageKind.Fold => Fold,
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
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    /// <summary>Returns the parameter contract an occurrence of <paramref name="kind"/> declares.</summary>
    /// <param name="kind">The stage shape.</param>
    /// <returns>The contract reference.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="kind"/> is not a declared member.</exception>
    /// <remarks>
    /// The distinction is not "which stages happen to have options" but "which stages have options a
    /// document can state honestly": a capacity, a concurrency bound, a count, a range, and a key bound are
    /// numbers, and none of them is a delegate. Every other shape carries the empty payload.
    /// </remarks>
    internal static ContractReference ParameterContractOf(LocalStageKind kind) => kind switch
    {
        LocalStageKind.Buffer or LocalStageKind.Queue => BufferParameterContract,
        LocalStageKind.SelectAsync or
            LocalStageKind.SelectAsyncUnordered or
            LocalStageKind.SelectValueTaskAsync or
            LocalStageKind.SelectValueTaskAsyncUnordered or
            LocalStageKind.ForEachAsync => ParallelismParameterContract,
        LocalStageKind.Take or LocalStageKind.Skip or LocalStageKind.Repeat => CountParameterContract,
        LocalStageKind.Range => RangeParameterContract,
        LocalStageKind.Distinct => DistinctParameterContract,
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
            LocalStageKind.Unzip or
            LocalStageKind.Merge or
            LocalStageKind.Concat or
            LocalStageKind.Where or
            LocalStageKind.Scan or
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
            LocalStageKind.SinkProbe => ParameterContract,
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
            _ when contract == CollectParameterContract => LocalCollectParameters.Validator,
            _ when contract == InterleaveParameterContract => LocalInterleaveParameters.Validator,
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
            LocalStageKind.FromChannel => LocalStagePlace.Source,
        LocalStageKind.Select or
            LocalStageKind.Where or
            LocalStageKind.Scan or
            LocalStageKind.Take or
            LocalStageKind.Skip or
            LocalStageKind.TakeWhile or
            LocalStageKind.TakeThrough or
            LocalStageKind.SkipWhile or
            LocalStageKind.Distinct or
            LocalStageKind.Buffer or
            LocalStageKind.SelectAsync or
            LocalStageKind.SelectAsyncUnordered or
            LocalStageKind.SelectValueTaskAsync or
            LocalStageKind.SelectValueTaskAsyncUnordered => LocalStagePlace.Operator,
        LocalStageKind.Broadcast or
            LocalStageKind.Balance or
            LocalStageKind.Unzip => LocalStagePlace.FanOut,
        LocalStageKind.Merge or
            LocalStageKind.Concat or
            LocalStageKind.Interleave => LocalStagePlace.FanIn,
        LocalStageKind.Fold or
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
            LocalStageKind.SinkProbe => LocalStagePlace.Terminal,
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

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
    /// all three because what differs between them is which input they read next rather than how many they
    /// have.
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
        LocalStageKind.Broadcast or LocalStageKind.Balance => FanOutOutputs,
        _ => ProducesElements(kind) ? ElementOutput : NoOutputs,
    };

    /// <summary>Builds the numbered output ports of a broadcast or a balance.</summary>
    /// <returns>The ports, <see cref="MinFanOut"/> of them required and the rest ignorable.</returns>
    /// <remarks>
    /// Ordinal order over the names is rotation order, which is what a balance distributes in and what a
    /// broadcast asks for room in. The number is formatted with the invariant culture for the reason every
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
            LocalStageKind.Fold => ResultPortSpecification.Create(ResultPort, FoldResultContract),
            LocalStageKind.First or
                LocalStageKind.FirstOrDefault or
                LocalStageKind.Count or
                LocalStageKind.Last or
                LocalStageKind.LastOrDefault or
                LocalStageKind.Collect => ResultPortSpecification.Create(ResultPort, ResultContract),
            LocalStageKind.Queue or
                LocalStageKind.SinkProbe => ResultPortSpecification.Create(ControlPort, ControlContract),
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
