namespace Orleans.Dataflow.Authoring;

/// <summary>
/// Where in a graph a stage shape stands, which is what decides the ports it declares.
/// </summary>
/// <remarks>
/// <para>
/// A branch of a local graph is a chain, so three of the four places are the chain's own: the shape that
/// begins one and consumes nothing, the ones in the middle that do both, and the one that ends it and
/// produces nothing. <see cref="LocalVocabulary.PlaceOf"/> is the single classification, and both the stage
/// catalog and the authoring occurrence read their port lists from it rather than each declaring its own.
/// </para>
/// <para>
/// <see cref="FanOut"/> is the fourth, and it is what makes a graph more than a chain: one input and several
/// outputs, each of which begins a branch of its own. It is a place rather than a flag because a shape's
/// place is exactly the question "what ports does it have", and a junction's answer is a list rather than a
/// single port.
/// </para>
/// </remarks>
internal enum LocalStagePlace
{
    /// <summary>Begins a chain: one output port and no input port.</summary>
    Source,

    /// <summary>Stands inside a chain: one input port and one output port.</summary>
    Operator,

    /// <summary>Ends a chain: one input port, no output port, and possibly a result port.</summary>
    Terminal,

    /// <summary>Splits a chain: one input port and several output ports, each one the head of a branch.</summary>
    FanOut,
}
