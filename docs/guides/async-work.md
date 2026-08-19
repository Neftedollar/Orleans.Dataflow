# Doing asynchronous work

**The problem.** Each element needs something slow done to it — an HTTP call, a
query, a grain invocation — and you want several of those in flight at once, but
a number of them you choose rather than a number the machine happens to allow.

## The whole program

Eight readings, four calls allowed in flight, and one call arranged to finish
*after* the rest of its batch so the difference between ordered and unordered
emission is a fact rather than a race. Then the same shape again, arranged into
the one deadlock this operator has.

```csharp
using Orleans.Dataflow;

const int Declared = 4;

int[] readings = [1, 2, 3, 4, 5, 6, 7, 8];
ParallelismOptions inFours = new() { MaxConcurrency = Declared };
LocalDataflowHost host = new();

// ---- 1. ordered and unordered, side by side ------------------------------

await LookUpAsync("ordered", unordered: false);
await LookUpAsync("unordered", unordered: true);

async Task LookUpAsync(string name, bool unordered)
{
    Concurrency concurrency = new(Declared);
    Countdown rest = new(Declared - 1);
    List<int> arrived = [];

    // One call per reading. Reading 1's call is held until the rest of its batch
    // has gone past, so the difference between the two operators is a fact rather
    // than a race.
    async Task<int> PriceAsync(int reading, CancellationToken token)
    {
        await concurrency.EnterAsync(token);

        if (reading == 1)
        {
            await rest.WaitAsync(token);
        }
        else if (!unordered)
        {
            rest.Signal();
        }

        return reading * 10;
    }

    Source<int> feed = Source.From(readings);

    // Which of the two announces the rest of the batch is forced by the operator.
    // Unordered is about emission, and a call returning is not emission — its
    // result is still on its way to the sink — so there the sink announces, and
    // reading 1 cannot come out first however the calls happen to be scheduled.
    // Ordered cannot do the same: it holds a finished result until everything
    // before it has been emitted, so a reading 1 waiting on emissions would be
    // waiting for ones that cannot happen until reading 1 is emitted. There the
    // calls announce themselves, and the answer is the operator's guarantee
    // rather than the arrangement's.
    RunnableGraph graph = (unordered
            ? feed.SelectAsyncUnordered(inFours, PriceAsync)
            : feed.SelectAsync(inFours, PriceAsync))
        .To(s => s.ForEach(price =>
        {
            arrived.Add(price);

            if (unordered)
            {
                rest.Signal();
            }
        }));

    await using RunHandle run = await host.MaterializeAsync(graph);

    await run.Completion;

    bool inFeedOrder = arrived.SequenceEqual(readings.Select(reading => reading * 10));

    Console.WriteLine($"{name,-9}  peak calls in flight {concurrency.Peak}  emitted {string.Join(' ', arrived)}");
    Console.WriteLine($"{name,-9}  in feed order: {inFeedOrder}   first reading emitted first: {arrived[0] == 10}");
}

// ---- 2. the deadlock: an ordered call waiting on a later element ---------

await WaitOnALaterElementAsync("ordered", unordered: false);
await WaitOnALaterElementAsync("unordered", unordered: true);

async Task WaitOnALaterElementAsync(string name, bool unordered)
{
    TaskCompletionSource fifth = new(TaskCreationOptions.RunContinuationsAsynchronously);
    List<int> arrived = [];

    async Task<int> PriceAsync(int reading, CancellationToken token)
    {
        if (reading == 5)
        {
            fifth.TrySetResult();
        }
        else if (reading == 1)
        {
            // Reading 5 is outside the window of four this graph declared.
            await fifth.Task.WaitAsync(token);
        }

        return reading * 10;
    }

    Source<int> feed = Source.From(readings);

    RunnableGraph graph = (unordered
            ? feed.SelectAsyncUnordered(inFours, PriceAsync)
            : feed.SelectAsync(inFours, PriceAsync))
        .To(s => s.ForEach(arrived.Add));

    using CancellationTokenSource budget = new(TimeSpan.FromSeconds(2));

    await using RunHandle run = await host.MaterializeAsync(graph, budget.Token);

    try
    {
        await run.Completion;

        Console.WriteLine($"{name,-9}  finished, emitted {string.Join(' ', arrived)}");
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine($"{name,-9}  still waiting when the two-second budget ran out, emitted {arrived.Count}");
    }
}

/// <summary>Counts how many calls are inside the stage at once, and holds them until the bound is reached.</summary>
internal sealed class Concurrency(int declared)
{
    private readonly Lock _padlock = new();
    private readonly TaskCompletionSource _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _inFlight;
    private int _peak;

    /// <summary>The greatest number of calls seen inside the stage at one time.</summary>
    public int Peak
    {
        get { lock (_padlock) { return _peak; } }
    }

    public async Task EnterAsync(CancellationToken cancellationToken)
    {
        lock (_padlock)
        {
            _inFlight++;
            _peak = Math.Max(_peak, _inFlight);

            if (_inFlight >= declared)
            {
                _reached.TrySetResult();
            }
        }

        await _reached.Task.WaitAsync(cancellationToken);

        lock (_padlock)
        {
            _inFlight--;
        }
    }
}

/// <summary>A tally that completes once a declared number of things have announced themselves.</summary>
internal sealed class Countdown(int count)
{
    private readonly Lock _padlock = new();
    private readonly TaskCompletionSource _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _remaining = count;

    public void Signal()
    {
        lock (_padlock)
        {
            if (_remaining > 0 && --_remaining == 0)
            {
                _reached.TrySetResult();
            }
        }
    }

    public Task WaitAsync(CancellationToken cancellationToken) => _reached.Task.WaitAsync(cancellationToken);
}
```

```console
dotnet run
```

```
ordered    peak calls in flight 4  emitted 10 20 30 40 50 60 70 80
ordered    in feed order: True   first reading emitted first: True
unordered  peak calls in flight 4  emitted 20 30 40 10 50 70 80 60
unordered  in feed order: False   first reading emitted first: False
ordered    still waiting when the two-second budget ran out, emitted 0
unordered  finished, emitted 20 30 40 50 10 70 80 60
```

The `unordered` lists are one real run: their exact order varies between runs,
which is the whole meaning of the word. The two lines under each are the parts
that never vary.

## The bound is exact

`peak calls in flight 4` is not an approximation. The `Concurrency` helper holds
every call until four of them are inside the stage together, so a run whose
declared bound the engine did not honour would *hang* rather than print a wrong
number. Four means four.

That matters more than it sounds. `ParallelismOptions.MaxConcurrency` is a
[bound](../reference/glossary.md#parallelism), not a target: the engine never
runs more, and runs fewer when there is less to do. It is also a statement about
memory rather than only about throughput, because an asynchronous stage is a
[boundary](../reference/glossary.md#boundary) — a call in flight is holding an
element. Four concurrent lookups is four live elements, and elements reach the
stage through a bounded channel, so the arithmetic stops there.

## Ordered and unordered

Both operators run calls concurrently. They differ only in when a *result* is
allowed out.

- **`SelectAsync`** — [ordered](../reference/glossary.md#ordered--unordered).
  Results are emitted in the order their elements arrived. Reading 1's call
  finished last, and reading 1 was still emitted first, because everything behind
  it waited.
- **`SelectAsyncUnordered`** — each result is emitted as soon as it exists. Reading
  1 came out after the rest of its batch, because that is when it was ready.

The arrangement that produces those two lines is worth a second look, because it
is the same trap in miniature. `first reading emitted first` is a statement about
*emission*, and a call returning is not that — its result is still travelling to
the sink, and the gap between the two events is real. So the unordered run has the
sink announce each reading as it emits it. Only the ordered run can let the calls
announce themselves, and it must: an ordered run whose calls waited on emission
would deadlock for exactly the reason the next section gives.

Ordered is the sensible default: most downstream code has an opinion about order
even when it does not say so. Choose unordered when the calls vary a lot in
duration — one slow element should not hold up ten fast ones — or when the
callbacks wait on each other, which is the next section.

There are `ValueTask` spellings of both, `SelectValueTaskAsync` and
`SelectValueTaskAsyncUnordered`, for callbacks that usually complete
synchronously. And `ForEachAsync(options, callback)` is the sink-side version if
the slow thing is the write rather than the transform.

## The deadlock, and the fix

Read the fifth line of output again: **`ordered  still waiting when the
two-second budget ran out, emitted 0`.**

That run had eight readings, a bound of four, and one call — reading 1's — that
waited for reading 5 to start. It never finished. Here is why, and it is worth
holding on to:

1. Readings 1 to 4 enter; the bound is full.
2. Readings 2, 3 and 4 finish. Their results **cannot be emitted**, because
   ordered emission means nothing goes out before reading 1's result does.
3. A finished-but-unemitted result is still occupying its slot. So the stage is
   still full.
4. Reading 5 is therefore never admitted.
5. Reading 1 is waiting for reading 5. Nobody moves.

**The rule: inside an ordered asynchronous mapping, a callback must never wait
on anything that depends on a later element.** Anything outside the declared
concurrency window can never be admitted while a call ahead of it is blocked, so
"wait for the batch to be complete", "wait for a shared lock the next element
holds", and "wait for a downstream reply that arrives with a later element" are
all the same deadlock.

Three fixes, in order of preference:

1. **Use `SelectAsyncUnordered`.** The last line of output is the same graph with
   the same callback and the same bound, unordered, and it finished. Results are
   free to leave as they are ready, so nothing occupies a slot waiting on
   reading 1.
2. **Make the callback self-contained.** A call that only needs its own element
   cannot deadlock, whatever the bound.
3. **Group first, then map.** If the work genuinely needs several elements, use
   `Grouped` or `GroupedWithin` to make the batch *one element*, and do the whole
   batch in one call. See [Windows and keys](windows-and-keys.md).

There is a fourth fix that works only sometimes, and it is worth being precise
about because the arithmetic is tempting. **Raising `MaxConcurrency` until the
awaited element fits inside the window does genuinely remove this deadlock.**
Change `const int Declared = 4;` at the top of the program above to `5` and run
it again: reading 5 is now inside the window, and the ordered run that printed
`still waiting` prints `finished, emitted 10 20 30 40 50 60 70 80` instead.

That is a real fix when the distance is fixed and you know it. It is not a fix
when the distance is data-dependent, because there is no number that is big
enough for "somewhere later in the stream" — and a bound raised to cover the
worst case is a bound that no longer bounds anything. Reach for it only when you
can say the number out loud and defend it.

## The trade-offs

**Concurrency is per stage, not per pipeline.** Two asynchronous stages with a
bound of four each can have eight calls in flight between them. If your
downstream has one budget, express it once — one stage, or a shared limiter your
callbacks go through.

**Cancellation reaches the callback.** The token your callback is handed is the
run's own, so a cancelled run cancels the calls in flight. Pass it to whatever
you are calling; if you drop it, cancellation stops meaning very much.

**Failure in a callback fails the run**, exactly like a synchronous stage. Wrap
it in a [supervision scope](../reference/glossary.md#supervision-scope) if you
want retries — see [Handling failure](handling-failure.md).

**Unordered has no stable output**, and that is not a bug to be worked around
with a sort downstream. If you need order, ask for order.

## The failure modes

| Symptom | Cause |
|---|---|
| The run hangs and never completes | An ordered mapping whose callback waits on a later element. Try `SelectAsyncUnordered`; if it then completes, that was it. |
| Peak concurrency is lower than you declared | Nothing wrong. The bound is a ceiling; a source that cannot keep up will not fill it. |
| Your downstream is overloaded despite the bound | Count the asynchronous stages. Each carries its own bound, and they add up. |
| Results arrive out of order and you did not expect it | You used `SelectAsyncUnordered`, or a [merge](../reference/glossary.md#merge) downstream, or a [group-by](../reference/glossary.md#group-by) whose substreams interleave. |
| `OperationCanceledException` from calls you did not cancel | The run was cancelled or disposed. The callback's token is the run's. |

## Where to look next

- [Bounding memory](bounding-memory.md) — the other declared boundary.
- [Handling failure](handling-failure.md) — what to do when the slow call throws.
- The repository's asynchronous sample runs this in both languages:
  [`samples/Orleans.Dataflow.Samples/CSharp/AsyncWork.cs`](../../samples/Orleans.Dataflow.Samples/CSharp/AsyncWork.cs).
  Run it with `dotnet run --project samples/Orleans.Dataflow.Samples -- --only async-work`.
