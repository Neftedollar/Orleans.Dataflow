namespace Orleans.Dataflow.Authoring;

/// <summary>
/// Where in a chain a stage shape stands, which is what decides the ports it declares.
/// </summary>
/// <remarks>
/// The local runtime executes one linear chain, so a shape is one of exactly three things: the one that
/// begins the chain and consumes nothing, the ones in the middle that do both, and the one that ends it and
/// produces nothing. <see cref="LocalVocabulary.PlaceOf"/> is the single classification, and both the stage
/// catalog and the authoring occurrence read their port lists from it rather than each declaring its own.
/// </remarks>
internal enum LocalStagePlace
{
    /// <summary>Begins a chain: one output port and no input port.</summary>
    Source,

    /// <summary>Stands inside a chain: one input port and one output port.</summary>
    Operator,

    /// <summary>Ends a chain: one input port, no output port, and possibly a result port.</summary>
    Terminal,
}
