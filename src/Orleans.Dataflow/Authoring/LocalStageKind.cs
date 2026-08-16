namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The eight stage shapes the local, lambda-implemented authoring vocabulary knows.
/// </summary>
/// <remarks>
/// <para>
/// The kind decides four things at once: which <c>local</c> stage reference the occurrence writes into the
/// document, which ports the occurrence leaves open in its fragment, which parameter contract and payload
/// its node carries, and how the authoring-side binding is interpreted by the local runtime. Keeping the
/// derivations on one discriminator is what makes it impossible to write a node whose declared ports,
/// declared parameters, and bound behavior disagree.
/// </para>
/// <para>
/// Three of the shapes are boundaries: <see cref="Buffer"/>, <see cref="SelectAsync"/>, and
/// <see cref="SelectAsyncUnordered"/> each cut the chain into segments the runtime executes as separate
/// loops joined by one bounded channel. The other five fuse, which is what makes fusion the default and a
/// queue something an author asked for.
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

    /// <summary>Maps every element through a function; one input port and one output port.</summary>
    Select,

    /// <summary>Passes the elements a predicate accepts; one input port and one output port.</summary>
    Where,

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

    /// <summary>Folds every element into a state value; one input port and one result port.</summary>
    Fold,

    /// <summary>Consumes and discards every element; one input port and nothing else.</summary>
    Ignore,
}
