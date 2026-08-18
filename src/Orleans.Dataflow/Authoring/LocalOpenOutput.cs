using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// One output port a <see cref="LocalGraphShape"/> deliberately leaves unconnected: where the next
/// occurrence attaches.
/// </summary>
/// <param name="Stage">The position of the producing occurrence in authoring order.</param>
/// <param name="Port">The output port of that occurrence that is still open.</param>
/// <remarks>
/// A <see cref="Orleans.Dataflow.Source{T}"/> always has exactly one, which is why every linear operator
/// can attach without naming anything. A <see cref="Orleans.Dataflow.Fork{T1, T2}"/> has exactly two, which
/// is the whole reason it is a type of its own: two open outputs are a shape the one-open-output values
/// cannot hold, and the rejoin is the call that takes it back down to one.
/// </remarks>
internal readonly record struct LocalOpenOutput(int Stage, PortId Port);
