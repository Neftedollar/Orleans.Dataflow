using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// One stage of a declared inner chain as a document states it: which shape it is, and the payload that
/// shape reads.
/// </summary>
/// <param name="Kind">The shape, recovered from the stage reference the payload spells.</param>
/// <param name="Parameters">The payload that shape's own reader is handed.</param>
/// <remarks>
/// The document half of one stage inside another stage's payload. Two shapes carry such a chain: a keyed
/// stage carries the group flow it instantiates per key, and a supervision scope carries the chain it owns
/// the execution of. It is deliberately not a <see cref="Definition.StageNode"/>: a node has an identity, a
/// position, and edges, and the stages of an inner chain have none of the three — they are a chain fused in
/// order, wired by their place in an array and by nothing else. What such a stage <em>does</em> is not here
/// either, for the same reason it is not in any other node: the delegates are in the binding table.
/// </remarks>
internal readonly record struct LocalInnerStage(LocalStageKind Kind, CanonicalJsonValue Parameters);
