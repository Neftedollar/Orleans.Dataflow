namespace Orleans.Dataflow.FSharp

open Orleans.Dataflow.Identity

// Orleans.Dataflow itself is deliberately not opened: see the note in Source.fs.

/// <summary>Declares a closed graph as one revision of one durable pipeline.</summary>
/// <remarks>
/// <para>
/// One function, because a pipeline is one step past a closed graph and the step has one shape: a graph plus
/// an identity is a deployable document. Everything a pipeline value then answers —
/// <see cref="P:Orleans.Dataflow.PipelineDefinition.Document"/>,
/// <see cref="P:Orleans.Dataflow.PipelineDefinition.Fingerprint"/>, and the recovery of a typed result slot —
/// is read directly off the shared value. Those are members of a plain .NET class with no receiver-threading
/// to smooth over, and wrapping them would add a name to a completion list without adding a spelling.
/// </para>
/// <para>
/// The identity is taken as the text and the number an author writes rather than as the two identity structs,
/// so a call site says <c>Pipeline.define "orders" 1</c> and not <c>Pipeline.define (GraphId.Create "orders")
/// (GraphRevision.Create 1)</c>. Both identifiers own their own grammar and their own diagnostic, and this
/// module calls them rather than restating either: an invalid identifier or a revision below the first is
/// refused with the very exception a C# author gets from writing the same two calls, message and parameter
/// name included. An author who already holds the identity structs writes <c>graph.AsPipeline</c>, which is
/// public and reads no worse from F#.
/// </para>
/// </remarks>
[<RequireQualifiedAccess>]
module Pipeline =

    /// <summary>Declares a closed graph as one revision of one durable pipeline.</summary>
    /// <param name="id">The identity of the graph lineage this pipeline belongs to.</param>
    /// <param name="revision">The revision this pipeline is; the first is one.</param>
    /// <param name="graph">The closed graph, which is unchanged.</param>
    /// <returns>The pipeline definition, whose document carries the given identity.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="id"/> is not a valid identifier segment, or the graph's document declares a capability
    /// that denies it a durable identity. The deployability message is a numbered list of every violation
    /// found, so one call names every reason rather than one reason per call.
    /// </exception>
    /// <exception cref="T:System.ArgumentOutOfRangeException">
    /// <paramref name="revision"/> is below the first revision number.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The content is re-closed under the real identity rather than relabelled, so the pipeline's fingerprint
    /// differs from the graph's: a pipeline's fingerprint is the fingerprint of the deployable document, not
    /// of the placeholder identity an anonymous graph carries.
    /// </para>
    /// <para>
    /// A graph holding a lambda stage is refused, and so is one whose node identifiers are positional. Neither
    /// capability is stripped: a graph that has them is not a pipeline with a caveat, it is a different kind
    /// of graph. That is what makes the registered spellings — <c>Source.ofRegistered</c>,
    /// <c>Source.viaRegistered</c>, and the <c>toRegistered</c> family — the vocabulary a pipeline is written
    /// in, and it is checked here rather than believed.
    /// </para>
    /// </remarks>
    let define
        (id: string)
        (revision: int)
        (graph: Orleans.Dataflow.RunnableGraph)
        : Orleans.Dataflow.PipelineDefinition =
        graph.AsPipeline(GraphId.Create id, GraphRevision.Create revision)
