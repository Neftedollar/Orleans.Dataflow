using Microsoft.Extensions.DependencyInjection;
using Orleans.BroadcastChannel;
using Orleans.Dataflow.Identity;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Dataflow.Adapters;

/// <summary>
/// What one silo knows about an element contract carried over an Orleans stream: which CLR type carries it
/// here, and how to subscribe to and publish that type.
/// </summary>
/// <remarks>
/// The registry side of a stream adapter, and the reason it exists is a probed Orleans fact rather than a
/// preference: one process may open one stream identity under exactly one element type, and a second
/// <c>GetStream</c> under a different type is refused with a stream-type mismatch. An adapter that opened
/// every stream as <see cref="object"/> would therefore break every deployment whose own grains open the
/// same stream as their own type. So the element type is a registration, the document names the contract,
/// and the typed operations live behind this interface.
/// </remarks>
internal interface IStreamElementEntry
{
    /// <summary>Gets the contract the elements of this stream declare.</summary>
    ContractReference Contract { get; }

    /// <summary>Gets the CLR type this silo binds to <see cref="Contract"/>.</summary>
    Type ElementType { get; }

    /// <summary>Subscribes an ingress to one stream.</summary>
    /// <param name="provider">The stream provider.</param>
    /// <param name="stream">The stream identity.</param>
    /// <param name="ingress">The ingress every delivery is offered to.</param>
    /// <returns>The subscription handle, to be handed back to <see cref="UnsubscribeAsync"/>.</returns>
    Task<object> SubscribeAsync(IStreamProvider provider, StreamId stream, IStreamIngress ingress);

    /// <summary>Cancels one subscription.</summary>
    /// <param name="handle">The handle <see cref="SubscribeAsync"/> returned.</param>
    /// <returns>A task that completes when the subscription is gone.</returns>
    Task UnsubscribeAsync(object handle);

    /// <summary>Publishes one element to a stream.</summary>
    /// <param name="provider">The stream provider.</param>
    /// <param name="stream">The stream identity.</param>
    /// <param name="element">The element, which is an instance of <see cref="ElementType"/>.</param>
    /// <returns>A task that completes when the provider has accepted the element.</returns>
    Task PublishAsync(IStreamProvider provider, StreamId stream, object? element);
}

/// <summary>
/// Where a stream subscription puts what it was delivered.
/// </summary>
/// <remarks>
/// One interface so that the typed observer a registration builds knows nothing about the run's bounded
/// ingress and the ingress knows nothing about Orleans. The three members are the three things an
/// <see cref="IAsyncObserver{T}"/> can say.
/// </remarks>
internal interface IStreamIngress
{
    /// <summary>Offers one delivered element.</summary>
    /// <param name="element">The element.</param>
    /// <returns>A task that completes when the element has been admitted, dropped, or refused.</returns>
    ValueTask OfferAsync(object? element);

    /// <summary>Records that the stream said it had ended.</summary>
    void Complete();

    /// <summary>Records that the stream reported a failure.</summary>
    /// <param name="failure">The failure.</param>
    void Fail(Exception failure);
}

/// <summary>
/// What one silo knows about a named awaited grain call that transforms elements.
/// </summary>
internal interface IGrainCallEntry
{
    /// <summary>Gets the name a document addresses this call by.</summary>
    string Name { get; }

    /// <summary>Gets the contract of the elements this call consumes.</summary>
    ContractReference Input { get; }

    /// <summary>Gets the contract of the elements this call produces.</summary>
    ContractReference Output { get; }

    /// <summary>Invokes the call.</summary>
    /// <param name="grains">The silo's grain factory.</param>
    /// <param name="element">The element to send.</param>
    /// <param name="cancellationToken">The token that cancels this call.</param>
    /// <returns>The reply.</returns>
    Task<object?> InvokeAsync(IGrainFactory grains, object? element, CancellationToken cancellationToken);
}

/// <summary>
/// What one silo knows about a named keyed grain call: the routing function beside the call itself.
/// </summary>
/// <remarks>
/// The key extractor is the difference between this and <see cref="IGrainCallEntry"/>, and it is code, so it
/// is a registration rather than anything a document could carry — ADR 0001's rule applied to routing. A
/// document names the call and the silo owns the function that says which partition an element belongs to.
/// </remarks>
internal interface IKeyedGrainCallEntry
{
    /// <summary>Gets the name a document addresses this call by.</summary>
    string Name { get; }

    /// <summary>Gets the contract of the elements this call consumes.</summary>
    ContractReference Input { get; }

    /// <summary>Gets the contract of the elements this call produces.</summary>
    ContractReference Output { get; }

    /// <summary>Reads the key one element belongs to.</summary>
    /// <param name="element">The element.</param>
    /// <returns>The key, which is never empty.</returns>
    /// <exception cref="ArgumentException">
    /// The element is not the type this silo binds to the call, or the extractor produced no key.
    /// </exception>
    string KeyOf(object? element);

    /// <summary>Invokes the call.</summary>
    /// <param name="grains">The silo's grain factory.</param>
    /// <param name="element">The element to send.</param>
    /// <param name="cancellationToken">The token that cancels this call.</param>
    /// <returns>The reply.</returns>
    Task<object?> InvokeAsync(IGrainFactory grains, object? element, CancellationToken cancellationToken);
}

/// <summary>
/// What one silo knows about a named awaited grain call whose reply is discarded.
/// </summary>
internal interface IGrainCallSinkEntry
{
    /// <summary>Gets the name a document addresses this call by.</summary>
    string Name { get; }

    /// <summary>Gets the contract of the elements this call consumes.</summary>
    ContractReference Input { get; }

    /// <summary>Invokes the call.</summary>
    /// <param name="grains">The silo's grain factory.</param>
    /// <param name="element">The element to send.</param>
    /// <param name="cancellationToken">The token that cancels this call.</param>
    /// <returns>A task that completes when the grain has replied.</returns>
    Task InvokeAsync(IGrainFactory grains, object? element, CancellationToken cancellationToken);
}

/// <summary>
/// What one silo knows about a named grain enumeration that heads a run.
/// </summary>
internal interface IGrainEnumerableEntry
{
    /// <summary>Gets the name a document addresses this source by.</summary>
    string Name { get; }

    /// <summary>Gets the contract of the elements this source produces.</summary>
    ContractReference Output { get; }

    /// <summary>Opens one enumeration.</summary>
    /// <param name="grains">The silo's grain factory.</param>
    /// <param name="cancellationToken">The run's own token.</param>
    /// <returns>The sequence, enumerated once and disposed on every terminal path of the run.</returns>
    IAsyncEnumerable<object?> Open(IGrainFactory grains, CancellationToken cancellationToken);
}

/// <summary>
/// What one silo knows about a named observer bridge: which CLR type its pushes carry.
/// </summary>
internal interface IObserverBridgeEntry
{
    /// <summary>Gets the name a document addresses this bridge by.</summary>
    string Name { get; }

    /// <summary>Gets the contract of the elements pushed at this bridge.</summary>
    ContractReference Output { get; }

    /// <summary>Checks that a pushed element is the type this silo binds to the bridge.</summary>
    /// <param name="element">The element as it arrived over the wire.</param>
    /// <exception cref="ArgumentException">The element is of some other type.</exception>
    void RequireElement(object? element);
}

/// <summary>
/// What one silo knows about a named Broadcast Channel publication.
/// </summary>
internal interface IBroadcastSinkEntry
{
    /// <summary>Gets the contract the elements of this publication declare.</summary>
    ContractReference Contract { get; }

    /// <summary>Gets the CLR type this silo binds to <see cref="Contract"/>.</summary>
    Type ElementType { get; }

    /// <summary>Publishes one element to a channel.</summary>
    /// <param name="services">The silo's container, which the named provider is resolved from.</param>
    /// <param name="provider">The broadcast provider's registration name.</param>
    /// <param name="channelNamespace">The channel's namespace.</param>
    /// <param name="key">The channel's key.</param>
    /// <param name="element">The element, which is an instance of <see cref="ElementType"/>.</param>
    /// <returns>A task that completes when the provider has accepted the element.</returns>
    Task PublishAsync(
        IServiceProvider services,
        string provider,
        string channelNamespace,
        string key,
        object? element);
}

/// <summary>
/// One element contract carried over Orleans streams, declared once and used twice.
/// </summary>
/// <typeparam name="T">The CLR type that carries the contract in this process.</typeparam>
/// <remarks>
/// <para>
/// A deployment writes one of these and hands it to two places: to
/// <c>IOrleansDataflowBuilder.AddStreamElement</c>, which tells a silo that streams may carry this contract
/// and which type it is here, and to <see cref="OrleansStages"/>, which turns it into the typed authoring
/// handle and into the parameter payload a node stores. That is the whole point of the shape — one
/// declaration, so a silo and an author cannot disagree about what a stream carries without the disagreement
/// being visible as two different declarations.
/// </para>
/// <para>
/// <typeparamref name="T"/> must satisfy Orleans serialization: an element crossing a stream is serialized
/// by the provider, so the author's type carries <c>[GenerateSerializer]</c> and per-member <c>[Id]</c>, or
/// a registered serializer. That is checked at first use by Orleans itself rather than here, because
/// nothing at registration time can see the codecs a silo will load.
/// </para>
/// </remarks>
public sealed class StreamElementBinding<T> : IStreamElementEntry
{
    /// <summary>Initializes a new instance of the <see cref="StreamElementBinding{T}"/> class.</summary>
    /// <param name="contract">The validated element contract declaration.</param>
    internal StreamElementBinding(ElementContract<T> contract) => Element = contract;

    /// <summary>Gets the element contract this declaration binds to <typeparamref name="T"/>.</summary>
    public ElementContract<T> Element { get; }

    /// <inheritdoc/>
    ContractReference IStreamElementEntry.Contract => Element.Reference;

    /// <inheritdoc/>
    Type IStreamElementEntry.ElementType => typeof(T);

    /// <summary>Returns a one-line diagnostic summary of this declaration.</summary>
    /// <returns>Text of the form <c>stream element order@v1 as Order</c>.</returns>
    /// <remarks>The method never throws.</remarks>
    public override string ToString() => $"stream element {Element}";

    /// <inheritdoc/>
    async Task<object> IStreamElementEntry.SubscribeAsync(
        IStreamProvider provider,
        StreamId stream,
        IStreamIngress ingress) =>
        await provider.GetStream<T>(stream).SubscribeAsync(new Observer(ingress)).ConfigureAwait(false);

    /// <inheritdoc/>
    Task IStreamElementEntry.UnsubscribeAsync(object handle) =>
        ((StreamSubscriptionHandle<T>)handle).UnsubscribeAsync();

    /// <inheritdoc/>
    Task IStreamElementEntry.PublishAsync(IStreamProvider provider, StreamId stream, object? element) =>
        provider.GetStream<T>(stream).OnNextAsync((T)element!);

    /// <summary>The typed observer one run's ingress is subscribed as.</summary>
    /// <param name="ingress">The run's bounded ingress.</param>
    /// <remarks>
    /// The delivery is awaited into the ingress and nothing else happens here. A full ingress under the
    /// backpressure policy therefore delays this delivery, which is Orleans' own backpressure onto the
    /// provider's pulling agent; a full ingress under a dropping policy answers at once and the drop is
    /// counted. Either way the observer never decides anything the document did not declare.
    /// </remarks>
    private sealed class Observer(IStreamIngress ingress) : IAsyncObserver<T>
    {
        /// <inheritdoc/>
        public async Task OnNextAsync(T item, StreamSequenceToken? token = null) =>
            await ingress.OfferAsync(item).ConfigureAwait(false);

        /// <inheritdoc/>
        public Task OnCompletedAsync()
        {
            ingress.Complete();

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public Task OnErrorAsync(Exception ex)
        {
            ingress.Fail(ex);

            return Task.CompletedTask;
        }
    }
}

/// <summary>
/// The factory that declares an element contract carried over Orleans streams.
/// </summary>
/// <remarks>
/// The factory lives on a non-generic companion class so that the type argument is inferred from the
/// contract declaration, per the rule that puts <see cref="ElementContract.For{T}(string, int)"/> on a
/// companion of <see cref="ElementContract{T}"/>.
/// </remarks>
public static class StreamElementBinding
{
    /// <summary>Declares that Orleans streams may carry one element contract, as one CLR type.</summary>
    /// <typeparam name="T">The CLR type that carries the contract in this process.</typeparam>
    /// <param name="element">The element contract declaration.</param>
    /// <returns>The binding.</returns>
    /// <exception cref="ArgumentException"><paramref name="element"/> is the default value.</exception>
    public static StreamElementBinding<T> Create<T>(ElementContract<T> element)
    {
        if (element.IsDefault)
        {
            throw new ArgumentException(
                $"A stream element binding requires a created {nameof(ElementContract<T>)}; the default value names no contract.",
                nameof(element));
        }

        return new StreamElementBinding<T>(element);
    }
}

/// <summary>
/// One named awaited grain call that transforms elements, declared once and used twice.
/// </summary>
/// <typeparam name="TIn">The element type the call consumes.</typeparam>
/// <typeparam name="TOut">The element type the call produces.</typeparam>
/// <remarks>
/// <para>
/// A document names a call and never a CLR member, which is ADR 0001's rule applied to grain calls: graph
/// data must not be able to cause code loading, so the deployment registers the callable thing under a name
/// and a document may only address a name a silo already published. What travels is
/// <c>{"call":"price-order", ...}</c>, and a document naming a call this silo does not register is refused
/// before a run exists.
/// </para>
/// <para>
/// The two element contracts are part of the declaration and not decoration. They are written into the
/// node's payload by the authoring helper and compared against this silo's registration when the document is
/// validated, so a silo that publishes <c>price-order</c> over different types than the author compiled
/// against refuses the document rather than failing at the first element. That is the contract-to-contract
/// check the CLR types themselves could never make across a deployment boundary.
/// </para>
/// </remarks>
public sealed class GrainCallBinding<TIn, TOut> : IGrainCallEntry
{
    private readonly Func<IGrainFactory, TIn, CancellationToken, Task<TOut>> _call;

    /// <summary>Initializes a new instance of the <see cref="GrainCallBinding{TIn, TOut}"/> class.</summary>
    /// <param name="name">The validated call name.</param>
    /// <param name="input">The validated input contract.</param>
    /// <param name="output">The validated output contract.</param>
    /// <param name="call">The call itself.</param>
    internal GrainCallBinding(
        string name,
        ElementContract<TIn> input,
        ElementContract<TOut> output,
        Func<IGrainFactory, TIn, CancellationToken, Task<TOut>> call)
    {
        Name = name;
        Input = input;
        Output = output;
        _call = call;
    }

    /// <summary>Gets the name a document addresses this call by.</summary>
    public string Name { get; }

    /// <summary>Gets the contract of the elements this call consumes.</summary>
    public ElementContract<TIn> Input { get; }

    /// <summary>Gets the contract of the elements this call produces.</summary>
    public ElementContract<TOut> Output { get; }

    /// <inheritdoc/>
    ContractReference IGrainCallEntry.Input => Input.Reference;

    /// <inheritdoc/>
    ContractReference IGrainCallEntry.Output => Output.Reference;

    /// <summary>Returns a one-line diagnostic summary of this declaration.</summary>
    /// <returns>Text of the form <c>grain call 'price-order' order@v1 as Order -&gt; price@v1 as Price</c>.</returns>
    /// <remarks>The method never throws.</remarks>
    public override string ToString() => $"grain call '{Name}' {Input} -> {Output}";

    /// <inheritdoc/>
    async Task<object?> IGrainCallEntry.InvokeAsync(
        IGrainFactory grains,
        object? element,
        CancellationToken cancellationToken) =>
        await _call(grains, (TIn)element!, cancellationToken).ConfigureAwait(false);
}

/// <summary>
/// The factory that declares a named awaited grain call.
/// </summary>
public static class GrainCallBinding
{
    /// <summary>Declares a named awaited grain call that transforms elements.</summary>
    /// <typeparam name="TIn">The element type the call consumes.</typeparam>
    /// <typeparam name="TOut">The element type the call produces.</typeparam>
    /// <param name="name">The name a document addresses the call by.</param>
    /// <param name="input">The contract of the elements the call consumes.</param>
    /// <param name="output">The contract of the elements the call produces.</param>
    /// <param name="call">
    /// The call, which receives the silo's grain factory, one element, and a token that is cancelled when
    /// the run stops and when the stage's declared timeout elapses.
    /// </param>
    /// <returns>The binding.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> or <paramref name="call"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="name"/> is empty or white space, or either contract is the default value.
    /// </exception>
    public static GrainCallBinding<TIn, TOut> Create<TIn, TOut>(
        string name,
        ElementContract<TIn> input,
        ElementContract<TOut> output,
        Func<IGrainFactory, TIn, CancellationToken, Task<TOut>> call)
    {
        OrleansBindingNames.Require(name);
        ArgumentNullException.ThrowIfNull(call);
        OrleansBindingNames.RequireContract(input.IsDefault, nameof(input));
        OrleansBindingNames.RequireContract(output.IsDefault, nameof(output));

        return new GrainCallBinding<TIn, TOut>(name, input, output, call);
    }
}

/// <summary>
/// One named keyed grain call, declared once and used twice: the call, and the function that says which key
/// an element belongs to.
/// </summary>
/// <typeparam name="TIn">The element type the call consumes.</typeparam>
/// <typeparam name="TOut">The element type the call produces.</typeparam>
/// <remarks>
/// <para>
/// The key extractor is what makes this a different registration from
/// <see cref="GrainCallBinding{TIn, TOut}"/> rather than the same one with an option. Routing is behavior:
/// it decides which activation an element reaches and, when the stage is distributed, which grain the
/// cluster has to place. A document may name the call and may not carry the function, exactly as it may
/// name a call and not a method.
/// </para>
/// <para>
/// The key is text and it becomes part of a grain key when the stage is distributed, so the whole of what
/// this declaration requires of it is that it exists: any non-empty string is a partition. An extractor that
/// returns nothing is a programming error rather than an element that belongs nowhere, and it is refused at
/// the element that produced it with both the call's name and the element's type in the message.
/// </para>
/// </remarks>
public sealed class KeyedGrainCallBinding<TIn, TOut> : IKeyedGrainCallEntry
{
    private readonly Func<TIn, string> _key;
    private readonly Func<IGrainFactory, TIn, CancellationToken, Task<TOut>> _call;

    /// <summary>Initializes a new instance of the <see cref="KeyedGrainCallBinding{TIn, TOut}"/> class.</summary>
    /// <param name="name">The validated call name.</param>
    /// <param name="input">The validated input contract.</param>
    /// <param name="output">The validated output contract.</param>
    /// <param name="key">The routing function.</param>
    /// <param name="call">The call itself.</param>
    internal KeyedGrainCallBinding(
        string name,
        ElementContract<TIn> input,
        ElementContract<TOut> output,
        Func<TIn, string> key,
        Func<IGrainFactory, TIn, CancellationToken, Task<TOut>> call)
    {
        Name = name;
        Input = input;
        Output = output;
        _key = key;
        _call = call;
    }

    /// <summary>Gets the name a document addresses this call by.</summary>
    public string Name { get; }

    /// <summary>Gets the contract of the elements this call consumes.</summary>
    public ElementContract<TIn> Input { get; }

    /// <summary>Gets the contract of the elements this call produces.</summary>
    public ElementContract<TOut> Output { get; }

    /// <inheritdoc/>
    ContractReference IKeyedGrainCallEntry.Input => Input.Reference;

    /// <inheritdoc/>
    ContractReference IKeyedGrainCallEntry.Output => Output.Reference;

    /// <summary>Returns a one-line diagnostic summary of this declaration.</summary>
    /// <returns>Text of the form <c>keyed grain call 'price-order' order@v1 as Order -&gt; price@v1 as Price</c>.</returns>
    /// <remarks>The method never throws.</remarks>
    public override string ToString() => $"keyed grain call '{Name}' {Input} -> {Output}";

    /// <inheritdoc/>
    string IKeyedGrainCallEntry.KeyOf(object? element)
    {
        // The element may have arrived over a hop, so its type is checked here rather than assumed. A cast
        // that failed inside the extractor would report the author's routing function as the problem.
        if (element is not TIn typed)
        {
            throw new ArgumentException(
                $"The keyed grain call '{Name}' carries '{typeof(TIn).FullName}' in this silo, and the element handed to it is '{element?.GetType().FullName ?? "null"}'.",
                nameof(element));
        }

        string key = _key(typed);

        return string.IsNullOrEmpty(key)
            ? throw new ArgumentException(
                $"The key extractor registered for the keyed grain call '{Name}' produced no key for an element of '{typeof(TIn).FullName}'. Every element belongs to exactly one partition, and an empty key names none.",
                nameof(element))
            : key;
    }

    /// <inheritdoc/>
    async Task<object?> IKeyedGrainCallEntry.InvokeAsync(
        IGrainFactory grains,
        object? element,
        CancellationToken cancellationToken)
    {
        if (element is not TIn typed)
        {
            throw new ArgumentException(
                $"The keyed grain call '{Name}' carries '{typeof(TIn).FullName}' in this silo, and the element handed to it is '{element?.GetType().FullName ?? "null"}'.",
                nameof(element));
        }

        return await _call(grains, typed, cancellationToken).ConfigureAwait(false);
    }
}

/// <summary>
/// The factory that declares a named keyed grain call.
/// </summary>
public static class KeyedGrainCallBinding
{
    /// <summary>Declares a named keyed grain call and the function that partitions its elements.</summary>
    /// <typeparam name="TIn">The element type the call consumes.</typeparam>
    /// <typeparam name="TOut">The element type the call produces.</typeparam>
    /// <param name="name">The name a document addresses the call by.</param>
    /// <param name="input">The contract of the elements the call consumes.</param>
    /// <param name="output">The contract of the elements the call produces.</param>
    /// <param name="key">
    /// The routing function, which reads the key an element belongs to. It runs once per element on the
    /// run's own thread, so it must be quick and must not block; and it must be a function of the element
    /// alone, because two elements of one key are ordered by their key and nothing else.
    /// </param>
    /// <param name="call">
    /// The call, which receives the silo's grain factory, one element, and a token that is cancelled when
    /// the run stops and when the stage's declared timeout elapses.
    /// </param>
    /// <returns>The binding.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/>, <paramref name="key"/>, or <paramref name="call"/> is
    /// <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="name"/> is empty or white space, or either contract is the default value.
    /// </exception>
    public static KeyedGrainCallBinding<TIn, TOut> Create<TIn, TOut>(
        string name,
        ElementContract<TIn> input,
        ElementContract<TOut> output,
        Func<TIn, string> key,
        Func<IGrainFactory, TIn, CancellationToken, Task<TOut>> call)
    {
        OrleansBindingNames.Require(name);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(call);
        OrleansBindingNames.RequireContract(input.IsDefault, nameof(input));
        OrleansBindingNames.RequireContract(output.IsDefault, nameof(output));

        return new KeyedGrainCallBinding<TIn, TOut>(name, input, output, key, call);
    }
}

/// <summary>
/// One named awaited grain call whose reply is discarded, declared once and used twice.
/// </summary>
/// <typeparam name="TIn">The element type the call consumes.</typeparam>
/// <remarks>
/// The sink form of <see cref="GrainCallBinding{TIn, TOut}"/>, and a separate declaration rather than the
/// same one with a dropped reply: what a sink promises is that the grain answered, and a call whose reply
/// type is part of its identity would make two stages out of one registration.
/// </remarks>
public sealed class GrainCallSinkBinding<TIn> : IGrainCallSinkEntry
{
    private readonly Func<IGrainFactory, TIn, CancellationToken, Task> _call;

    /// <summary>Initializes a new instance of the <see cref="GrainCallSinkBinding{TIn}"/> class.</summary>
    /// <param name="name">The validated call name.</param>
    /// <param name="input">The validated input contract.</param>
    /// <param name="call">The call itself.</param>
    internal GrainCallSinkBinding(
        string name,
        ElementContract<TIn> input,
        Func<IGrainFactory, TIn, CancellationToken, Task> call)
    {
        Name = name;
        Input = input;
        _call = call;
    }

    /// <summary>Gets the name a document addresses this call by.</summary>
    public string Name { get; }

    /// <summary>Gets the contract of the elements this call consumes.</summary>
    public ElementContract<TIn> Input { get; }

    /// <inheritdoc/>
    ContractReference IGrainCallSinkEntry.Input => Input.Reference;

    /// <summary>Returns a one-line diagnostic summary of this declaration.</summary>
    /// <returns>Text of the form <c>grain call sink 'record-order' order@v1 as Order</c>.</returns>
    /// <remarks>The method never throws.</remarks>
    public override string ToString() => $"grain call sink '{Name}' {Input}";

    /// <inheritdoc/>
    Task IGrainCallSinkEntry.InvokeAsync(
        IGrainFactory grains,
        object? element,
        CancellationToken cancellationToken) =>
        _call(grains, (TIn)element!, cancellationToken);
}

/// <summary>
/// The factory that declares a named awaited grain call whose reply is discarded.
/// </summary>
public static class GrainCallSinkBinding
{
    /// <summary>Declares a named awaited grain call that terminates a graph.</summary>
    /// <typeparam name="TIn">The element type the call consumes.</typeparam>
    /// <param name="name">The name a document addresses the call by.</param>
    /// <param name="input">The contract of the elements the call consumes.</param>
    /// <param name="call">
    /// The call, which receives the silo's grain factory, one element, and a token that is cancelled when
    /// the stage's declared timeout elapses.
    /// </param>
    /// <returns>The binding.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> or <paramref name="call"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="name"/> is empty or white space, or <paramref name="input"/> is the default value.
    /// </exception>
    public static GrainCallSinkBinding<TIn> Create<TIn>(
        string name,
        ElementContract<TIn> input,
        Func<IGrainFactory, TIn, CancellationToken, Task> call)
    {
        OrleansBindingNames.Require(name);
        ArgumentNullException.ThrowIfNull(call);
        OrleansBindingNames.RequireContract(input.IsDefault, nameof(input));

        return new GrainCallSinkBinding<TIn>(name, input, call);
    }
}

/// <summary>
/// One named grain enumeration that heads a run, declared once and used twice.
/// </summary>
/// <typeparam name="T">The element type the enumeration produces.</typeparam>
/// <remarks>
/// The enumeration is opened once per run under the run's own token and disposed on every terminal path,
/// including the ones where reading it is what went wrong. Cancellation is cooperative end to end: Orleans
/// carries the token to the grain, and a grain that ignores it delays the run's stop until it next yields.
/// </remarks>
public sealed class GrainEnumerableBinding<T> : IGrainEnumerableEntry
{
    private readonly Func<IGrainFactory, CancellationToken, IAsyncEnumerable<T>> _open;

    /// <summary>Initializes a new instance of the <see cref="GrainEnumerableBinding{T}"/> class.</summary>
    /// <param name="name">The validated source name.</param>
    /// <param name="output">The validated output contract.</param>
    /// <param name="open">The opener.</param>
    internal GrainEnumerableBinding(
        string name,
        ElementContract<T> output,
        Func<IGrainFactory, CancellationToken, IAsyncEnumerable<T>> open)
    {
        Name = name;
        Output = output;
        _open = open;
    }

    /// <summary>Gets the name a document addresses this source by.</summary>
    public string Name { get; }

    /// <summary>Gets the contract of the elements this source produces.</summary>
    public ElementContract<T> Output { get; }

    /// <inheritdoc/>
    ContractReference IGrainEnumerableEntry.Output => Output.Reference;

    /// <summary>Returns a one-line diagnostic summary of this declaration.</summary>
    /// <returns>Text of the form <c>grain enumerable 'orders-feed' order@v1 as Order</c>.</returns>
    /// <remarks>The method never throws.</remarks>
    public override string ToString() => $"grain enumerable '{Name}' {Output}";

    /// <inheritdoc/>
    async IAsyncEnumerable<object?> IGrainEnumerableEntry.Open(
        IGrainFactory grains,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (T element in _open(grains, cancellationToken)
            .WithCancellation(cancellationToken)
            .ConfigureAwait(false))
        {
            yield return element;
        }
    }
}

/// <summary>
/// The factory that declares a named grain enumeration.
/// </summary>
public static class GrainEnumerableBinding
{
    /// <summary>Declares a named grain enumeration that heads a graph.</summary>
    /// <typeparam name="T">The element type the enumeration produces.</typeparam>
    /// <param name="name">The name a document addresses the source by.</param>
    /// <param name="output">The contract of the elements the enumeration produces.</param>
    /// <param name="open">
    /// The opener, which receives the silo's grain factory and the run's own token and returns the sequence
    /// to enumerate.
    /// </param>
    /// <returns>The binding.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> or <paramref name="open"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="name"/> is empty or white space, or <paramref name="output"/> is the default value.
    /// </exception>
    public static GrainEnumerableBinding<T> Create<T>(
        string name,
        ElementContract<T> output,
        Func<IGrainFactory, CancellationToken, IAsyncEnumerable<T>> open)
    {
        OrleansBindingNames.Require(name);
        ArgumentNullException.ThrowIfNull(open);
        OrleansBindingNames.RequireContract(output.IsDefault, nameof(output));

        return new GrainEnumerableBinding<T>(name, output, open);
    }
}

/// <summary>
/// One named observer bridge that heads a run, declared once and used twice.
/// </summary>
/// <typeparam name="T">The element type the bridge accepts in this process.</typeparam>
/// <remarks>
/// <para>
/// A bridge registration says only two things, and neither of them is behavior: what the bridge is called,
/// and what a push at it carries. There is nothing to bind to, because the code on the far side of a bridge
/// is the caller's rather than the deployment's — which is exactly what makes a bridge a bridge and not a
/// source.
/// </para>
/// <para>
/// The element type is the check a caller cannot make for itself. A push arrives as
/// <see cref="object"/> over the wire, so a caller pushing the wrong type would otherwise reach a cast
/// somewhere inside the run; this registration is what turns that into a refusal that names both types at
/// the push.
/// </para>
/// <para>
/// <typeparamref name="T"/> must satisfy Orleans serialization, because a push crosses a grain boundary.
/// That is checked by Orleans at first use rather than here.
/// </para>
/// </remarks>
public sealed class ObserverBridgeBinding<T> : IObserverBridgeEntry
{
    /// <summary>Initializes a new instance of the <see cref="ObserverBridgeBinding{T}"/> class.</summary>
    /// <param name="name">The validated bridge name.</param>
    /// <param name="output">The validated element contract.</param>
    internal ObserverBridgeBinding(string name, ElementContract<T> output)
    {
        Name = name;
        Output = output;
    }

    /// <summary>Gets the name a document addresses this bridge by.</summary>
    public string Name { get; }

    /// <summary>Gets the contract of the elements pushed at this bridge.</summary>
    public ElementContract<T> Output { get; }

    /// <inheritdoc/>
    ContractReference IObserverBridgeEntry.Output => Output.Reference;

    /// <summary>Returns a one-line diagnostic summary of this declaration.</summary>
    /// <returns>Text of the form <c>observer bridge 'orders' order@v1 as Order</c>.</returns>
    /// <remarks>The method never throws.</remarks>
    public override string ToString() => $"observer bridge '{Name}' {Output}";

    /// <inheritdoc/>
    void IObserverBridgeEntry.RequireElement(object? element)
    {
        if (element is T || (element is null && default(T) is null))
        {
            return;
        }

        throw new ArgumentException(
            $"The observer bridge '{Name}' carries '{typeof(T).FullName}' in this silo, and the element pushed at it is '{element?.GetType().FullName ?? "null"}'.",
            nameof(element));
    }
}

/// <summary>
/// The factory that declares a named observer bridge.
/// </summary>
public static class ObserverBridgeBinding
{
    /// <summary>Declares a named observer bridge that heads a graph.</summary>
    /// <typeparam name="T">The element type the bridge accepts.</typeparam>
    /// <param name="name">The name a document, and a caller's bridge address, carry.</param>
    /// <param name="output">The contract of the elements pushed at the bridge.</param>
    /// <returns>The binding.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="name"/> is empty or white space, or <paramref name="output"/> is the default value.
    /// </exception>
    public static ObserverBridgeBinding<T> Create<T>(string name, ElementContract<T> output)
    {
        OrleansBindingNames.Require(name);
        OrleansBindingNames.RequireContract(output.IsDefault, nameof(output));

        return new ObserverBridgeBinding<T>(name, output);
    }
}

/// <summary>
/// One element contract carried over an Orleans Broadcast Channel, declared once and used twice.
/// </summary>
/// <typeparam name="T">The CLR type that carries the contract in this process.</typeparam>
/// <remarks>
/// A separate declaration from <see cref="StreamElementBinding{T}"/> and not a reuse of it: a channel and a
/// stream are different providers with different guarantees, and one registration that served both would
/// make a deployment publishing to a channel look as though it had also registered a stream.
/// </remarks>
public sealed class BroadcastElementBinding<T> : IBroadcastSinkEntry
{
    /// <summary>Initializes a new instance of the <see cref="BroadcastElementBinding{T}"/> class.</summary>
    /// <param name="contract">The validated element contract declaration.</param>
    internal BroadcastElementBinding(ElementContract<T> contract) => Element = contract;

    /// <summary>Gets the element contract this declaration binds to <typeparamref name="T"/>.</summary>
    public ElementContract<T> Element { get; }

    /// <inheritdoc/>
    ContractReference IBroadcastSinkEntry.Contract => Element.Reference;

    /// <inheritdoc/>
    Type IBroadcastSinkEntry.ElementType => typeof(T);

    /// <summary>Returns a one-line diagnostic summary of this declaration.</summary>
    /// <returns>Text of the form <c>broadcast element order@v1 as Order</c>.</returns>
    /// <remarks>The method never throws.</remarks>
    public override string ToString() => $"broadcast element {Element}";

    /// <inheritdoc/>
    Task IBroadcastSinkEntry.PublishAsync(
        IServiceProvider services,
        string provider,
        string channelNamespace,
        string key,
        object? element) =>
        services.GetRequiredKeyedService<IBroadcastChannelProvider>(provider)
            .GetChannelWriter<T>(ChannelId.Create(channelNamespace, key))
            .Publish((T)element!);
}

/// <summary>
/// The factory that declares an element contract carried over a Broadcast Channel.
/// </summary>
public static class BroadcastElementBinding
{
    /// <summary>Declares that a Broadcast Channel may carry one element contract, as one CLR type.</summary>
    /// <typeparam name="T">The CLR type that carries the contract in this process.</typeparam>
    /// <param name="element">The element contract declaration.</param>
    /// <returns>The binding.</returns>
    /// <exception cref="ArgumentException"><paramref name="element"/> is the default value.</exception>
    public static BroadcastElementBinding<T> Create<T>(ElementContract<T> element)
    {
        OrleansBindingNames.RequireContract(element.IsDefault, nameof(element));

        return new BroadcastElementBinding<T>(element);
    }
}

/// <summary>
/// The checks every named binding's factory applies to the name and the contracts it is given.
/// </summary>
/// <remarks>
/// One place, so that four factories refuse the same things in the same words. A name is deliberately only
/// checked for emptiness: it is a key in a deployment's own registry and never an identifier the definition
/// plane parses, so imposing the identifier grammar on it would refuse names a deployment is entitled to
/// use.
/// </remarks>
internal static class OrleansBindingNames
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
                "A named Orleans binding is addressed by a non-empty name, because the name is what a document carries in place of a CLR member.",
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
                "A named Orleans binding declares created element contracts, because the contracts are what a silo and an author are checked against; the default value names no contract.",
                parameter);
        }
    }
}
