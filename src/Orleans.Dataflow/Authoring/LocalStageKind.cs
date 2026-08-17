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
/// transform elements inside one, the shapes that cut one into segments, and the shapes that end one. A
/// source declares no input port, a terminal declares no output port, and the three result-bearing
/// terminals declare a result port on top of that.
/// </para>
/// <para>
/// Six of the shapes are boundaries: <see cref="Buffer"/>, <see cref="SelectAsync"/>,
/// <see cref="SelectAsyncUnordered"/>, <see cref="SelectValueTaskAsync"/>,
/// <see cref="SelectValueTaskAsyncUnordered"/>, and <see cref="ForEachAsync"/> each cut the chain into
/// segments the runtime executes as separate loops joined by one bounded channel. Every other shape fuses,
/// which is what makes fusion the default and a queue something an author asked for.
/// </para>
/// <para>
/// <see cref="SinkProbe"/> is the one shape no author-facing operator of this package builds: it is the
/// terminal of the demand-aware probes the testing package exposes. It lives here rather than there because
/// what it does — hold an element until a receiver asks for it, on the segment's own thread, under this
/// runtime's own stop and pause discipline — is runtime semantics, and a second implementation of those
/// beside this one is exactly what a vocabulary exists to prevent.
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

    /// <summary>Folds every element into a state value; one input port and one result port.</summary>
    Fold,

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
