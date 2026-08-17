using System.Collections;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// Opens the sequence one run pulls its elements from.
/// </summary>
/// <param name="context">The tokens of the run being opened.</param>
/// <returns>The sequence, which the run enumerates exactly once.</returns>
/// <remarks>
/// <para>
/// A factory rather than a sequence, because the sources this runtime grew past checkpoint 3 need the run
/// they belong to: an asynchronous enumerable is opened with the run's token, an ingress queue and a
/// channel wait on it, and a source that never ends waits on nothing else. A sequence fixed when the plan
/// was built could not be told any of that.
/// </para>
/// <para>
/// The factory is invoked once per run, at the first pull, which is what keeps "a run stopped before its
/// first element never touches its source" true for every source rather than only for the ones that were
/// already lazy.
/// </para>
/// </remarks>
internal delegate IEnumerable LocalSource(LocalRunContext context);
