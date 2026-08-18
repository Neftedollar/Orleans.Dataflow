using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// One stage of a group flow as a document states it: which shape it is, and the payload that shape reads.
/// </summary>
/// <param name="Kind">The shape, recovered from the stage reference the payload spells.</param>
/// <param name="Parameters">The payload that shape's own reader is handed.</param>
/// <remarks>
/// The document half of one stage inside a keyed stage's group flow. It is deliberately not a
/// <see cref="Definition.StageNode"/>: a node has an identity, a position, and edges, and a group flow's stages have
/// none of the three — they are a chain fused per key, wired by their order in an array and by nothing else.
/// What a group stage <em>does</em> is not here either, for the same reason it is not in any other node: the
/// delegates are in the binding table.
/// </remarks>
internal readonly record struct LocalGroupStage(LocalStageKind Kind, CanonicalJsonValue Parameters);
