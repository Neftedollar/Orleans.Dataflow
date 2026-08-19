namespace Orleans.Dataflow.Samples

open System.Collections.Generic

/// <summary>An order as it arrives, before the pipeline has looked at it.</summary>
/// <remarks>
/// <para>
/// Every scenario in this sample carries these two types and nothing else, so that eight programs read as
/// one story rather than as eight vocabularies. They are the same words the repository README uses in its
/// opening snippet — <c>orderEvents</c>, <c>OrderDocument</c> — because scenario one <em>is</em> that
/// snippet, made runnable.
/// </para>
/// <para>
/// The domain lives in the F# project even though this repository is C#-first, and the reason is the
/// reference direction rather than a preference: the console application references this library so that
/// both authorings of a scenario run in one process, so anything both authorings must agree about has to
/// live on this side of that arrow. A graph document never names a CLR type — a local port declares one
/// blind element contract and a registered port declares a contract identifier — so two frontends could in
/// principle each carry their own copy of these records and still fingerprint identically. One copy is
/// simply harder to get wrong.
/// </para>
/// </remarks>
type OrderEvent =
    {
        /// <summary>Where this order sits in the feed, counted from zero.</summary>
        /// <remarks>
        /// Carried because two scenarios are about order rather than about content: the asynchronous one
        /// shows what an unordered mapping does to it, and the backpressure one names the elements a full
        /// buffer dropped. A real order event would not need it.
        /// </remarks>
        Sequence: int

        /// <summary>The identifier a person would quote when asking about this order.</summary>
        OrderId: string

        /// <summary>The region the order was placed in, which is what the keyed scenarios group by.</summary>
        Region: string

        /// <summary>What the order is worth.</summary>
        Amount: decimal

        /// <summary>Whether the order passed the checks its originating system runs.</summary>
        /// <remarks>The predicate of the first scenario's filter, and the only reason it has a filter.</remarks>
        IsValid: bool
    }

/// <summary>An order the pipeline has accepted, in the shape the rest of a system would store.</summary>
/// <remarks>
/// Deliberately not the same type as <see cref="T:Orleans.Dataflow.Samples.OrderEvent"/>: a mapping stage
/// that answered its own input type would demonstrate nothing about typing, and every pipeline here would
/// still compile if the map were deleted.
/// </remarks>
type OrderDocument =
    {
        /// <summary>Where the originating event sat in the feed.</summary>
        Sequence: int

        /// <summary>The identifier carried over from the event.</summary>
        OrderId: string

        /// <summary>The region carried over from the event.</summary>
        Region: string

        /// <summary>What the order is worth.</summary>
        Amount: decimal
    }

    /// <summary>Accepts one order event as a document.</summary>
    /// <param name="order">The event.</param>
    /// <returns>The document.</returns>
    /// <remarks>
    /// A static member so that the C# authoring can pass it as a method group — <c>Select(OrderDocument
    /// .FromEvent)</c> — which is exactly how the README's snippet is written.
    /// </remarks>
    static member FromEvent(order: OrderEvent) : OrderDocument =
        {
            Sequence = order.Sequence
            OrderId = order.OrderId
            Region = order.Region
            Amount = order.Amount
        }

/// <summary>The same acceptance, spelled the way an F# author reaches for it.</summary>
/// <remarks>
/// One function calling the static member rather than a second implementation, because two spellings of one
/// mapping that could drift are two spellings too many. The module shares its name with the type, which is
/// what <c>ModuleSuffix</c> is for.
/// </remarks>
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module OrderDocument =

    /// <summary>Accepts one order event as a document.</summary>
    /// <param name="order">The event.</param>
    /// <returns>The document.</returns>
    let ofEvent (order: OrderEvent) : OrderDocument = OrderDocument.FromEvent order

/// <summary>The feed every scenario reads its orders from.</summary>
/// <remarks>
/// <para>
/// Generated rather than written out, so that a scenario can ask for four orders in a smoke run and twelve
/// in a full one and get the same story at two sizes. Nothing about it is random: the same count always
/// produces the same orders, which is what lets the runner compare two authorings' answers at all.
/// </para>
/// <para>
/// A class with a static method rather than an F# module, because the C# side calls it too and
/// <c>SampleOrders.Take(12)</c> should read the same in both languages.
/// </para>
/// </remarks>
[<AbstractClass; Sealed>]
type SampleOrders =

    /// <summary>Builds the first orders of the feed.</summary>
    /// <param name="count">How many orders to build.</param>
    /// <returns>The orders, in feed order.</returns>
    /// <remarks>
    /// Every fourth order fails its originating system's checks, and the amounts and regions cycle, so a
    /// filter, a grouping, and a windowing operator all have something to bite on at any size from four
    /// orders upwards.
    /// </remarks>
    static member Take(count: int) : IReadOnlyList<OrderEvent> =
        let regions = [| "north"; "south"; "east" |]

        [| for index in 0 .. count - 1 ->
               {
                   Sequence = index
                   OrderId = sprintf "order-%03d" index
                   Region = regions[index % regions.Length]
                   Amount = decimal (10 + ((index * 7) % 90))
                   IsValid = index % 4 <> 3
               } |]
