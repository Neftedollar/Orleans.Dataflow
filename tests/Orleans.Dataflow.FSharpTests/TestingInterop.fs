namespace Orleans.Dataflow.FSharpTests

open System.Collections.Generic
open Orleans.Dataflow.Authoring

/// <summary>The tests-only bridge from the Testing package's C#-facade values into F# shapes.</summary>
/// <remarks>
/// <para>
/// The Testing package answers in the C# facade's own types — <c>TestSink.Marking</c> is a C#
/// <c>Sink&lt;T&gt;</c>, <c>TestFlow.FaultPoint</c> a C# <c>Flow&lt;T, T&gt;</c> — and the F# frontend
/// deliberately has no conversion from them: a public one would invite the facade detour the binding rule
/// refuses. What the two frontends genuinely share is the occurrence chain underneath both, so this bridge
/// reads it off the C# value through this project's friendship with the core package and wraps it in the F#
/// shape it corresponds to. Two friendships, one line of substance each, tests only.
/// </para>
/// <para>
/// Nothing here may grow toward the product: an operator wanted in real F# programs is added to the F#
/// modules over the shared vocabulary, never bridged. The bridge exists so that the Testing package's
/// instruments — the mark that measures a commit window, the fault point that kills an attempt at a chosen
/// arrival — are usable from F#-authored arrangements without a second implementation of either.
/// </para>
/// </remarks>
module internal TestingInterop =

    /// <summary>Wraps a Testing-package sink in the F# shape a junctionless close consumes.</summary>
    /// <param name="sink">The C#-facade sink, whose occurrence chain is the value shared by both frontends.</param>
    /// <returns>The same chain as an F# sink.</returns>
    let sink (sink: Orleans.Dataflow.Sink<'T>) : Orleans.Dataflow.FSharp.Sink<'T> =
        Orleans.Dataflow.FSharp.Sink<'T>(sink.Stages)

    /// <summary>Wraps a Testing-package flow in the F# shape composition consumes.</summary>
    /// <param name="flow">The C#-facade flow, whose occurrence chain is the value shared by both frontends.</param>
    /// <returns>The same chain as an F# flow.</returns>
    let flow (flow: Orleans.Dataflow.Flow<'In, 'Out>) : Orleans.Dataflow.FSharp.Flow<'In, 'Out> =
        Orleans.Dataflow.FSharp.Flow<'In, 'Out>(flow.Stages)
