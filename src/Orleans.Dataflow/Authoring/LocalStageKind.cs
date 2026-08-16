namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The five stage shapes the local, lambda-implemented authoring vocabulary knows.
/// </summary>
/// <remarks>
/// <para>
/// The kind decides three things at once: which <c>local</c> stage reference the occurrence writes into the
/// document, which ports the occurrence leaves open in its fragment, and how the authoring-side binding is
/// interpreted by the future local runtime. Keeping the three derivations on one discriminator is what
/// makes it impossible to write a node whose declared ports and bound behavior disagree.
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

    /// <summary>Folds every element into a state value; one input port and one result port.</summary>
    Fold,

    /// <summary>Consumes and discards every element; one input port and nothing else.</summary>
    Ignore,
}
