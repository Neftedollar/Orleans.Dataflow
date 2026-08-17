namespace Orleans.Dataflow.Adapters;

/// <summary>
/// What one host knows about a named <see cref="IObservable{T}"/> a document may address.
/// </summary>
/// <remarks>
/// The registry side of the observable bridge. A document names an observable and never a CLR member, per
/// ADR 0001: graph data must not be able to cause code loading, so the deployment registers the sequence
/// under a name and a document may only address a name a host already publishes.
/// </remarks>
internal interface IObservableEntry
{
    /// <summary>Gets the name a document addresses this observable by.</summary>
    string Name { get; }

    /// <summary>Gets the contract of the elements this observable produces.</summary>
    Identity.ContractReference Output { get; }

    /// <summary>Opens one subscription.</summary>
    /// <param name="ingress">Where every notification goes.</param>
    /// <returns>The subscription, disposed on every terminal path of the run.</returns>
    IDisposable Subscribe(IPushIngress ingress);
}

/// <summary>
/// Where a push subscription puts what it was handed.
/// </summary>
/// <remarks>
/// One interface so that the typed observer a registration builds knows nothing about the run's bounded
/// ingress and the ingress knows nothing about <see cref="IObserver{T}"/>. The three members are the three
/// things an <see cref="IObserver{T}"/> can say.
/// </remarks>
internal interface IPushIngress
{
    /// <summary>Offers one pushed element.</summary>
    /// <param name="element">The element.</param>
    /// <remarks>
    /// Synchronous on purpose: <see cref="IObserver{T}.OnNext"/> has nothing to await, so the calling
    /// thread is the only thread this offer can wait on. That is the whole cost model of a push source
    /// under the backpressure policy, and it is stated on the stage rather than hidden here.
    /// </remarks>
    void Offer(object? element);

    /// <summary>Records that the sequence said it had ended.</summary>
    void Complete();

    /// <summary>Records that the sequence reported a failure.</summary>
    /// <param name="failure">The failure, which faults the run unchanged.</param>
    void Fail(Exception failure);
}

/// <summary>
/// One named <see cref="IObservable{T}"/> that heads a run, declared once and used twice.
/// </summary>
/// <typeparam name="T">The element type the sequence produces.</typeparam>
/// <remarks>
/// <para>
/// A deployment writes one of these and hands it to two places: to a host's registration surface, which
/// tells that host what the name means, and to <see cref="DotnetStages"/>, which turns it into the typed
/// authoring handle and into the parameter payload a node stores. One declaration, so a host and an author
/// cannot disagree about what a name produces without the disagreement being two different declarations.
/// </para>
/// <para>
/// The opener is invoked once per run, so two runs of one pipeline subscribe twice. Whether that means two
/// independent sequences or two subscriptions to one shared sequence is the observable's own business — a
/// cold observable gives each run its own producer, and a hot one gives them a shared one and therefore
/// shared elements. Nothing here makes a cold observable out of a hot one.
/// </para>
/// </remarks>
public sealed class ObservableBinding<T> : IObservableEntry
{
    private readonly Func<IObservable<T>> _open;

    /// <summary>Initializes a new instance of the <see cref="ObservableBinding{T}"/> class.</summary>
    /// <param name="name">The validated source name.</param>
    /// <param name="output">The validated output contract.</param>
    /// <param name="open">The opener.</param>
    internal ObservableBinding(string name, ElementContract<T> output, Func<IObservable<T>> open)
    {
        Name = name;
        Output = output;
        _open = open;
    }

    /// <summary>Gets the name a document addresses this observable by.</summary>
    public string Name { get; }

    /// <summary>Gets the contract of the elements this observable produces.</summary>
    public ElementContract<T> Output { get; }

    /// <inheritdoc/>
    Identity.ContractReference IObservableEntry.Output => Output.Reference;

    /// <summary>Returns a one-line diagnostic summary of this declaration.</summary>
    /// <returns>Text of the form <c>observable 'ticks' tick@v1 as Int64</c>.</returns>
    /// <remarks>The method never throws.</remarks>
    public override string ToString() => $"observable '{Name}' {Output}";

    /// <inheritdoc/>
    IDisposable IObservableEntry.Subscribe(IPushIngress ingress)
    {
        IObservable<T> sequence = _open() ??
            throw new InvalidOperationException(
                $"The observable binding '{Name}' returned no sequence. A binding opens one {nameof(IObservable<T>)} per run; returning null gives a run nothing to subscribe to.");

        return sequence.Subscribe(new Observer(ingress)) ??
            throw new InvalidOperationException(
                $"Subscribing to the observable '{Name}' returned no subscription. A run unsubscribes on every terminal path, and a null subscription is a producer nothing can stop.");
    }

    /// <summary>The typed observer one run's ingress is subscribed as.</summary>
    /// <param name="ingress">The run's bounded ingress.</param>
    /// <remarks>
    /// Nothing but forwarding. A full ingress under the backpressure policy blocks
    /// <see cref="IObserver{T}.OnNext"/>, which blocks whichever thread the observable pushes on; under a
    /// dropping policy the notification returns at once and the drop is counted. Either way this observer
    /// never decides anything the document did not declare.
    /// </remarks>
    private sealed class Observer(IPushIngress ingress) : IObserver<T>
    {
        /// <inheritdoc/>
        public void OnNext(T value) => ingress.Offer(value);

        /// <inheritdoc/>
        public void OnCompleted() => ingress.Complete();

        /// <inheritdoc/>
        public void OnError(Exception error) => ingress.Fail(error);
    }
}

/// <summary>
/// The factory that declares a named <see cref="IObservable{T}"/>.
/// </summary>
/// <remarks>
/// The factory lives on a non-generic companion class so that the type argument is inferred from the
/// contract declaration, per the rule that puts <see cref="ElementContract.For{T}(string, int)"/> on a
/// companion of <see cref="ElementContract{T}"/>.
/// </remarks>
public static class ObservableBinding
{
    /// <summary>Declares a named <see cref="IObservable{T}"/> that heads a graph.</summary>
    /// <typeparam name="T">The element type the sequence produces.</typeparam>
    /// <param name="name">The name a document addresses the observable by.</param>
    /// <param name="output">The contract of the elements the sequence produces.</param>
    /// <param name="open">
    /// The opener, invoked once per run, which returns the sequence that run subscribes to.
    /// </param>
    /// <returns>The binding.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> or <paramref name="open"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="name"/> is empty or white space, or <paramref name="output"/> is the default value.
    /// </exception>
    public static ObservableBinding<T> Create<T>(
        string name,
        ElementContract<T> output,
        Func<IObservable<T>> open)
    {
        DotnetBindingNames.Require(name);
        ArgumentNullException.ThrowIfNull(open);
        DotnetBindingNames.RequireContract(output.IsDefault, nameof(output));

        return new ObservableBinding<T>(name, output, open);
    }
}

/// <summary>
/// The checks every named .NET binding's factory applies to the name and the contracts it is given.
/// </summary>
/// <remarks>
/// One place, so that every factory refuses the same things in the same words. A name is deliberately only
/// checked for emptiness: it is a key in a deployment's own registry and never an identifier the definition
/// plane parses, so imposing the identifier grammar on it would refuse names a deployment is entitled to
/// use.
/// </remarks>
internal static class DotnetBindingNames
{
    /// <summary>Refuses a name that is null, empty, or white space.</summary>
    /// <param name="name">The name.</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="name"/> is empty or white space.</exception>
    internal static void Require(string name)
    {
        ArgumentNullException.ThrowIfNull(name);

        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException(
                "A named .NET binding is addressed by a non-empty name, because the name is what a document carries in place of a CLR member.",
                nameof(name));
        }
    }

    /// <summary>Refuses a contract declaration that names no contract.</summary>
    /// <param name="isDefault">Whether the declaration is the default value.</param>
    /// <param name="parameter">The parameter name to report it under.</param>
    /// <exception cref="ArgumentException"><paramref name="isDefault"/> is <see langword="true"/>.</exception>
    internal static void RequireContract(bool isDefault, string parameter)
    {
        if (isDefault)
        {
            throw new ArgumentException(
                "A named .NET binding declares created element contracts, because the contracts are what a host and an author are checked against; the default value names no contract.",
                parameter);
        }
    }
}
