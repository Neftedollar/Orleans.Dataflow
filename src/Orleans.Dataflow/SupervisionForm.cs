namespace Orleans.Dataflow;

/// <summary>
/// What a supervision scope does with a failure raised inside it.
/// </summary>
/// <remarks>
/// <para>
/// The four forms of ADR 0007, and the whole of the vocabulary: a scope names one of them, the name is
/// written into the document, and a scope whose form changes is a different graph with a different
/// fingerprint. There is no fifth value for "fail the run" because that is not a form a scope takes — it is
/// what happens outside every scope, and it stays the default the engine has had since M2.
/// </para>
/// <para>
/// <b>No form names an exception type, and that is v1's honesty rather than an omission.</b> A policy that
/// filtered by type would need CLR type names in a document, which the definition plane forbids, or a
/// declared failure taxonomy, which is real design work owed evidence of its own. A scope therefore
/// supervises every failure raised inside it alike; the taxonomy is a recorded deferral.
/// </para>
/// </remarks>
public enum SupervisionForm
{
    /// <summary>The failing element is dropped and the scope's stage state is kept.</summary>
    /// <remarks>
    /// The default, and the cheapest form: a scan inside the scope keeps counting from where it was, a
    /// distinct keeps its keys, and a batch keeps its open group. The failure is counted rather than
    /// silent — the run's supervised-failure count moves — which is what makes "dropped" observable.
    /// </remarks>
    Resume,

    /// <summary>The failing element is dropped and every stage inside the scope resets to its seed.</summary>
    /// <remarks>
    /// What "reset" means is exact because the scope's chain is declared: a scan returns to its seed, a
    /// distinct forgets its keys, a batch abandons its open group. The stages are rebuilt from the very
    /// factories a fresh run builds them from, so a restarted scope is indistinguishable from one that has
    /// just started.
    /// </remarks>
    RestartStage,

    /// <summary>
    /// The element is offered to the scope again, up to a declared attempt count and with a declared
    /// backoff, and a declared answer for exhaustion.
    /// </summary>
    /// <remarks>
    /// <see cref="SupervisionOptions.MaxAttempts"/>, <see cref="SupervisionOptions.Backoff"/>, and
    /// <see cref="SupervisionOptions.OnExhaustion"/> are read only for this form and are refused on the
    /// others. Re-offering is to the scope's <em>first</em> stage, so a stateful stage inside a retry scope
    /// sees the element once per attempt; that is why the exhaustion answer can escalate to
    /// <see cref="RestartStage"/>.
    /// </remarks>
    Retry,

    /// <summary>The scope emits a declared fallback element and ends its stream successfully.</summary>
    /// <remarks>
    /// The one form that produces an element rather than dropping one, and the one that ends the stream:
    /// the fallback travels downstream, everything upstream of the scope stops, and the run reports
    /// success. Recovering with an <em>alternate source</em> is a different capability with a boundary of
    /// its own and is not this form.
    /// </remarks>
    Recover,
}
