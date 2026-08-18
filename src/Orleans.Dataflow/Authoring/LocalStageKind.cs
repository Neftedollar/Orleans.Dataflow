namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The stage shapes the local, lambda-implemented authoring vocabulary knows.
/// </summary>
/// <remarks>
/// <para>
/// The kind decides four things at once: which <c>local</c> stage reference the occurrence writes into the
/// document, which ports the occurrence leaves open in its fragment, which parameter contract and payload
/// its node carries, and how the authoring-side binding is interpreted by the local runtime. Keeping the
/// derivations on one discriminator is what makes it impossible to write a node whose declared ports,
/// declared parameters, and bound behavior disagree; <see cref="LocalVocabulary"/> is where every one of
/// those derivations lives, and <see cref="Orleans.Dataflow.LocalStageCatalog"/> is built from the same
/// answers rather than from a second list.
/// </para>
/// <para>
/// The members are grouped by what a shape does to a chain: the shapes that begin one, the shapes that
/// transform elements inside one, the shapes that cut one into segments, the shapes that split one into
/// branches, and the shapes that end one. A source declares no input port, a terminal declares no output
/// port, and the three result-bearing terminals declare a result port on top of that.
/// </para>
/// <para>
/// Ten of the shapes are boundaries: <see cref="Buffer"/>, <see cref="SelectAsync"/>,
/// <see cref="SelectAsyncUnordered"/>, <see cref="SelectValueTaskAsync"/>,
/// <see cref="SelectValueTaskAsyncUnordered"/>, <see cref="ForEachAsync"/>, and <see cref="Delay"/> each cut
/// the chain into segments the runtime executes as separate loops joined by one bounded channel;
/// M4.3 wave 2 added <see cref="GroupedWithin"/> and <see cref="GroupedWeightedWithin"/> to them for a
/// reason of their own — a batch closed by a clock has to emit while nothing is arriving, and only a segment
/// waiting on its own input channel can be woken to do that — and M4.3 wave 3 added <see cref="MergeMap"/>,
/// whose loop sleeps on one outstanding step per open inner sequence and can therefore never be a pass of
/// somebody else's loop. Every other shape fuses, which is what makes fusion the default and a queue
/// something an author asked for. <see cref="ScanAsync"/> and <see cref="FoldAsync"/> are deliberately not
/// among them: one asynchronous fold runs at a time because the next one folds this one's answer, so there
/// is no window to hold and nothing a boundary would buy.
/// </para>
/// <para>
/// Eight of the shapes read a clock, and every one of them reads the run's own: <see cref="Tick"/>,
/// <see cref="Delay"/>, <see cref="InitialDelay"/>, <see cref="Timeout"/>, <see cref="TakeWithin"/>,
/// <see cref="SkipWithin"/>, <see cref="GroupedWithin"/>, and <see cref="GroupedWeightedWithin"/>, together
/// with <see cref="Throttle"/>, which reads one to measure a rate. The
/// clock is the host's <see cref="System.TimeProvider"/>, resolved at materialization and carried by the run
/// (ADR 0005); no stage of this vocabulary reads <see cref="System.TimeProvider.System"/> directly, which is
/// what makes a deterministic test of one possible at all.
/// </para>
/// <para>
/// Nine of the shapes are junctions. <see cref="Broadcast"/>, <see cref="Balance"/>,
/// <see cref="Partition"/>, and <see cref="Unzip"/> each declare several output ports;
/// <see cref="Merge"/>, <see cref="Concat"/>,
/// <see cref="Interleave"/>, <see cref="Zip"/>, and <see cref="CombineLatest"/> each declare several input
/// ports. Every one of them is a boundary on every port it declares, and none of them fuses with anything,
/// because a junction's pump shape — several channels on one side, one on the other, and a rule about which
/// of them moves next — is the whole of what it is. Their contracts are ADR 0005's two tables, and the
/// runtime holds them per junction rather than per graph.
/// </para>
/// <para>
/// <see cref="Valve"/> is the one shape that is neither a source nor a terminal and still declares a
/// control port. Its state is a runtime object an author flips while the run is running, which is what a
/// control is; the state it <i>starts</i> in is configuration and is written into the document like every
/// other number.
/// </para>
/// <para>
/// <see cref="SinkProbe"/> is the one shape no author-facing operator of this package builds: it is the
/// terminal of the demand-aware probes the testing package exposes. It lives here rather than there because
/// what it does — hold an element until a receiver asks for it, on the segment's own thread, under this
/// runtime's own stop and pause discipline — is runtime semantics, and a second implementation of those
/// beside this one is exactly what a vocabulary exists to prevent.
/// </para>
/// <para>
/// Five of the shapes hold elements back rather than answering each one as it arrives:
/// <see cref="Grouped"/>, <see cref="Sliding"/>, <see cref="GroupedWithin"/>, and
/// <see cref="GroupedWeightedWithin"/> each build a group, and <see cref="SelectMany"/> is the mirror image
/// — one element in, a sequence out. Both are new shapes of answer rather than new pumps: the run pushes a
/// residue or an inner element through the stages below the one that produced it, exactly as it pushes an
/// element that arrived. <see cref="MergeMap"/> is the sixth and is the one that <i>is</i> a new pump: it
/// answers one element with a sequence too, but with several of those sequences open at once, and reading
/// whichever of them has something is a loop rather than a walk.
/// </para>
/// <para>
/// The kind is never serialized. It is recoverable from the node's <see cref="Definition.StageNode.Stage"/>
/// in the document, which is the only durable statement about what an occurrence is.
/// </para>
/// </remarks>
internal enum LocalStageKind
{
    /// <summary>Emits the elements of an in-memory sequence; one output port, no input port.</summary>
    FromEnumerable,

    /// <summary>Emits nothing and completes at once; one output port, no input port.</summary>
    Empty,

    /// <summary>Emits one element and completes; one output port, no input port.</summary>
    Single,

    /// <summary>Emits one element a declared number of times; one output port, no input port.</summary>
    Repeat,

    /// <summary>Emits a declared run of consecutive integers; one output port, no input port.</summary>
    Range,

    /// <summary>Emits the value of one task, or fails with the task's failure; one output port, no input port.</summary>
    FromTask,

    /// <summary>Fails the run with one exception without emitting anything; one output port, no input port.</summary>
    Failed,

    /// <summary>Emits what a generator produces from a state it carries; one output port, no input port.</summary>
    Unfold,

    /// <summary>Emits the elements of an asynchronous sequence; one output port, no input port.</summary>
    FromAsyncEnumerable,

    /// <summary>Emits one element a factory produces per run; one output port, no input port.</summary>
    FromFactory,

    /// <summary>Emits one element an asynchronous factory produces per run; one output port, no input port.</summary>
    FromAsyncFactory,

    /// <summary>Emits nothing and never ends of its own accord; one output port, no input port.</summary>
    Never,

    /// <summary>Repeats an in-memory sequence for as long as it is pulled; one output port, no input port.</summary>
    Cycle,

    /// <summary>
    /// Emits what an asynchronous generator produces from a state it carries; one output port, no input
    /// port.
    /// </summary>
    UnfoldAsync,

    /// <summary>
    /// Emits what producers offer to a bounded queue of its own; one output port, one control result port,
    /// no input port.
    /// </summary>
    Queue,

    /// <summary>Emits the elements of a channel the author owns; one output port, no input port.</summary>
    FromChannel,

    /// <summary>
    /// Emits the number of every tick of a declared interval, skipping the ticks a slow consumer missed;
    /// one output port, no input port.
    /// </summary>
    Tick,

    /// <summary>Maps every element through a function; one input port and one output port.</summary>
    Select,

    /// <summary>Passes the elements a predicate accepts; one input port and one output port.</summary>
    Where,

    /// <summary>
    /// Folds every element into a running state and emits each intermediate state; one input port and one
    /// output port.
    /// </summary>
    Scan,

    /// <summary>Passes a declared number of elements and then completes; one input port and one output port.</summary>
    Take,

    /// <summary>Drops a declared number of elements and passes the rest; one input port and one output port.</summary>
    Skip,

    /// <summary>
    /// Passes elements while a predicate holds and completes at the first that fails it, without emitting
    /// that one; one input port and one output port.
    /// </summary>
    TakeWhile,

    /// <summary>
    /// Passes elements until a predicate holds, emits that one too, and completes; one input port and one
    /// output port.
    /// </summary>
    TakeThrough,

    /// <summary>
    /// Drops elements while a predicate holds and passes everything from the first that fails it; one input
    /// port and one output port.
    /// </summary>
    SkipWhile,

    /// <summary>
    /// Passes the first occurrence of every element and drops the repeats, tracking at most a declared
    /// number of keys; one input port and one output port.
    /// </summary>
    Distinct,

    /// <summary>
    /// Drops an element equal to the one immediately before it, remembering exactly one; one input port and
    /// one output port.
    /// </summary>
    DeduplicateConsecutive,

    /// <summary>
    /// Replaces every element with the elements of the sequence a function answers, one inner sequence at a
    /// time; one input port and one output port.
    /// </summary>
    SelectMany,

    /// <summary>
    /// Replaces every element with the elements of the sequence a function answers, reading a declared
    /// number of those sequences at once; one input port and one output port.
    /// </summary>
    MergeMap,

    /// <summary>
    /// Folds every element into a running state through an asynchronous function and emits each
    /// intermediate state; one input port and one output port.
    /// </summary>
    ScanAsync,

    /// <summary>
    /// Collects a declared number of elements into one list and emits the last partial one when the stream
    /// ends; one input port and one output port.
    /// </summary>
    Grouped,

    /// <summary>
    /// Emits a window of a declared size every time it holds one, advancing by a declared step; one input
    /// port and one output port.
    /// </summary>
    Sliding,

    /// <summary>
    /// Collects elements into groups closed by a declared count or by a declared window, whichever comes
    /// first; one input port and one output port.
    /// </summary>
    GroupedWithin,

    /// <summary>
    /// Collects elements into groups closed by a declared count, a declared weight, or a declared window,
    /// whichever comes first; one input port and one output port.
    /// </summary>
    GroupedWeightedWithin,

    /// <summary>
    /// Holds up to a declared number of elements between two segments; one input port and one output port.
    /// </summary>
    Buffer,

    /// <summary>
    /// Maps every element through an asynchronous callback and emits the results in input order; one input
    /// port and one output port.
    /// </summary>
    SelectAsync,

    /// <summary>
    /// Maps every element through an asynchronous callback and emits the results in completion order; one
    /// input port and one output port.
    /// </summary>
    SelectAsyncUnordered,

    /// <summary>
    /// Maps every element through a callback returning a value task and emits the results in input order;
    /// one input port and one output port.
    /// </summary>
    SelectValueTaskAsync,

    /// <summary>
    /// Maps every element through a callback returning a value task and emits the results in completion
    /// order; one input port and one output port.
    /// </summary>
    SelectValueTaskAsyncUnordered,

    /// <summary>
    /// Holds every element for a declared duration and emits it in arrival order, with a declared number of
    /// them being held at once; one input port and one output port.
    /// </summary>
    Delay,

    /// <summary>
    /// Holds the first element until a declared duration has passed since the run started, and nothing after
    /// it; one input port and one output port.
    /// </summary>
    InitialDelay,

    /// <summary>
    /// Fails the run when a declared duration passes with no element, counting from the previous element or
    /// from the start of the run; one input port and one output port.
    /// </summary>
    Timeout,

    /// <summary>
    /// Ends the stream when a declared duration has passed since the run started; one input port and one
    /// output port.
    /// </summary>
    TakeWithin,

    /// <summary>
    /// Drops every element until a declared duration has passed since the run started; one input port and
    /// one output port.
    /// </summary>
    SkipWithin,

    /// <summary>
    /// Holds a stream to a declared rate, waiting for budget or failing the run by its declared mode; one
    /// input port and one output port.
    /// </summary>
    Throttle,

    /// <summary>
    /// Holds every element while its control is closed and passes them while it is open; one input port,
    /// one output port, and one control result port.
    /// </summary>
    Valve,

    /// <summary>
    /// Delivers every element to every live output; one input port and between two and
    /// <see cref="LocalVocabulary.MaxFanOut"/> output ports.
    /// </summary>
    Broadcast,

    /// <summary>
    /// Delivers each element to exactly one output that has room; one input port and between two and
    /// <see cref="LocalVocabulary.MaxFanOut"/> output ports.
    /// </summary>
    Balance,

    /// <summary>
    /// Delivers each element to the one output its routing function names, waiting for that output alone;
    /// one input port and between two and <see cref="LocalVocabulary.MaxFanOut"/> output ports.
    /// </summary>
    Partition,

    /// <summary>
    /// Delivers the two halves of a row to two outputs that both have room; one input port and the two
    /// output ports <c>left</c> and <c>right</c>.
    /// </summary>
    Unzip,

    /// <summary>
    /// Emits whichever input has an element, in rotation among the ready ones, and completes when every
    /// input has; between two and <see cref="LocalVocabulary.MaxFanIn"/> input ports and one output port.
    /// </summary>
    Merge,

    /// <summary>
    /// Emits each input to its end in port order without reading the ones behind it, and completes when the
    /// last one has; between two and <see cref="LocalVocabulary.MaxFanIn"/> input ports and one output port.
    /// </summary>
    Concat,

    /// <summary>
    /// Emits a declared number of elements from each input in fixed rotation, continuing over the remainder
    /// when one completes; between two and <see cref="LocalVocabulary.MaxFanIn"/> input ports and one
    /// output port.
    /// </summary>
    Interleave,

    /// <summary>
    /// Emits one row per element from every input, pairing them positionally, and completes as soon as any
    /// input has; between two and <see cref="LocalVocabulary.MaxFanIn"/> input ports and one output port.
    /// </summary>
    Zip,

    /// <summary>
    /// Emits a row of the latest element of every input on every arrival, once every input has produced
    /// one, and completes when every input has; between two and <see cref="LocalVocabulary.MaxFanIn"/>
    /// input ports and one output port.
    /// </summary>
    CombineLatest,

    /// <summary>Folds every element into a state value; one input port and one result port.</summary>
    Fold,

    /// <summary>
    /// Folds every element into a state value through an asynchronous function; one input port and one
    /// result port.
    /// </summary>
    FoldAsync,

    /// <summary>Consumes and discards every element; one input port and nothing else.</summary>
    Ignore,

    /// <summary>Hands every element to a synchronous callback; one input port and nothing else.</summary>
    ForEach,

    /// <summary>
    /// Hands every element to an asynchronous callback, a declared number of them at a time; one input port
    /// and nothing else.
    /// </summary>
    ForEachAsync,

    /// <summary>
    /// Takes the first element, completes the run, and requires that there was one; one input port and one
    /// result port.
    /// </summary>
    First,

    /// <summary>
    /// Takes the first element and completes the run, resolving the element type's default value when there
    /// was none; one input port and one result port.
    /// </summary>
    FirstOrDefault,

    /// <summary>Counts every element; one input port and one result port.</summary>
    Count,

    /// <summary>
    /// Keeps the last element and requires that there was one; one input port and one result port.
    /// </summary>
    Last,

    /// <summary>
    /// Keeps the last element, resolving the element type's default value when there was none; one input
    /// port and one result port.
    /// </summary>
    LastOrDefault,

    /// <summary>
    /// Collects up to a declared number of elements into a list; one input port and one result port.
    /// </summary>
    Collect,

    /// <summary>Writes every element into a channel the author owns; one input port and nothing else.</summary>
    ToChannel,

    /// <summary>
    /// Hands every element to a receiver that asks for it, one at a time; one input port and one control
    /// result port.
    /// </summary>
    SinkProbe,
}
