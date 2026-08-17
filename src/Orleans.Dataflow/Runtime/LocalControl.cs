using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// One runtime control of a plan: a per-run object an author reaches by name, and the queue it belongs to.
/// </summary>
/// <param name="Slot">The name the document declares the control under.</param>
/// <param name="Handle">The typed handle the author receives, already built for this run.</param>
/// <param name="Queue">
/// The queue behind the handle, which the run ends when it stops reading; <see langword="null"/> for a
/// control that stands at the other end of a chain, whose release is its terminal's.
/// </param>
/// <remarks>
/// <para>
/// A control is a result slot whose value exists at the start of a run rather than at its end, which is
/// exactly what ADR 0002 listed beside a fold result when it said slots carry results <em>and runtime
/// controls</em>. Nothing else about it is special: it is declared in the document, it is named, and it is
/// resolved through the same <see cref="RunHandle.GetValueAsync{TResult}"/>.
/// </para>
/// <para>
/// The handle is built when the plan is compiled, which is once per materialization, so two runs of one
/// graph never share a control. That is the same rule that gives them separate enumerators and separate
/// fold state.
/// </para>
/// <para>
/// A control is not only an ingress. A probe sink declares one at the end of a chain, where the thing an
/// author reaches by name is the demand side of the terminal rather than a queue in front of the source;
/// what the two have in common is the whole of what a control is — a per-run object, named in the
/// document, resolved when the run starts.
/// </para>
/// </remarks>
internal sealed record LocalControl(ResultSlotId Slot, object Handle, LocalIngressQueue? Queue);
