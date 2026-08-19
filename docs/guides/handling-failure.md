# Handling failure

**The problem.** A stage throws — a call times out, a parse fails, a downstream
system is briefly unavailable — and you need to decide *in advance* what the
pipeline does about it, rather than discovering the answer in production.

The default is worth knowing before anything else: **an unhandled throw fails the
run**, and everything behind it stops. Everything on this page is a deliberate
weakening of that rule inside a region you draw.

## The whole program

Six readings, and a stage that refuses reading 3. The same shape five times,
under five different answers.

```csharp
using Orleans.Dataflow;

int[] readings = [1, 2, 3, 4, 5, 6];

LocalDataflowHost host = new();
TimeSpan[] ladder = [TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(20)];

// ---- 1. no scope: one throw ends the run --------------------------------

{
    List<int> arrived = [];
    Flaky station = new(refuses: 3, times: int.MaxValue);

    RunnableGraph graph = Source.From(readings)
        .Select(station.Read)
        .To(s => s.ForEach(arrived.Add));

    await using RunHandle run = await host.MaterializeAsync(graph);

    try
    {
        await run.Completion;
    }
    catch (InvalidOperationException failure)
    {
        Console.WriteLine($"unsupervised  {failure.Message}");
    }

    Report("unsupervised", run.Snapshot(), arrived);
}

// ---- 2. Retry: two failures inside a scope that allows three attempts ---

{
    List<int> arrived = [];
    Flaky station = new(refuses: 3, times: 2);

    RunnableGraph graph = Source.From(readings)
        .Supervised(
            new SupervisionOptions
            {
                Form = SupervisionForm.Retry,
                MaxAttempts = 3,
                Backoff = ladder,
                OnExhaustion = RetryExhaustion.Fail,
            },
            Flow.For<int>().Select(station.Read))
        .To(s => s.ForEach(arrived.Add));

    await using RunHandle run = await host.MaterializeAsync(graph);

    await run.Completion;

    Report("retry", run.Snapshot(), arrived);
}

// ---- 3. Retry, exhausted, and the two answers to that ------------------

foreach (RetryExhaustion exhaustion in (RetryExhaustion[])[RetryExhaustion.Fail, RetryExhaustion.Resume])
{
    List<int> arrived = [];
    Flaky station = new(refuses: 3, times: int.MaxValue);

    RunnableGraph graph = Source.From(readings)
        .Supervised(
            new SupervisionOptions
            {
                Form = SupervisionForm.Retry,
                MaxAttempts = 3,
                Backoff = ladder,
                OnExhaustion = exhaustion,
            },
            Flow.For<int>().Select(station.Read))
        .To(s => s.ForEach(arrived.Add));

    await using RunHandle run = await host.MaterializeAsync(graph);

    try
    {
        await run.Completion;
    }
    catch (InvalidOperationException failure)
    {
        Console.WriteLine($"exhausted/{exhaustion,-6}  {failure.Message}");
    }

    Report($"exhausted/{exhaustion}", run.Snapshot(), arrived);
}

// ---- 4. Recover: a declared fallback in place of the failing element ---

{
    List<int> arrived = [];
    Flaky station = new(refuses: 3, times: int.MaxValue);

    RunnableGraph graph = Source.From(readings)
        .Supervised(
            new SupervisionOptions { Form = SupervisionForm.Recover },
            Flow.For<int>().Select(station.Read),
            fallback: -1)
        .To(s => s.ForEach(arrived.Add));

    await using RunHandle run = await host.MaterializeAsync(graph);

    await run.Completion;

    Report("recover", run.Snapshot(), arrived);
}

static void Report(string name, RunSnapshot snapshot, List<int> arrived) =>
    Console.WriteLine(
        $"{name,-18}  status {snapshot.Status,-9}  delivered [{string.Join(' ', arrived)}]  " +
        $"supervised failures {snapshot.SupervisedFailures}  poison {snapshot.PoisonElements}  " +
        $"dropped {snapshot.DroppedElements}");

/// <summary>A stage that refuses one reading, for as long as it has refusals left to spend.</summary>
internal sealed class Flaky(int refuses, int times)
{
    private readonly Lock _padlock = new();
    private int _raised;

    public int Read(int reading)
    {
        if (reading == refuses)
        {
            lock (_padlock)
            {
                if (_raised < times)
                {
                    _raised++;

                    throw new InvalidOperationException($"The station could not be reached for reading {reading}.");
                }
            }
        }

        return reading;
    }
}
```

```console
dotnet run
```

```
unsupervised  The station could not be reached for reading 3.
unsupervised        status Failed     delivered [1 2]  supervised failures 0  poison 0  dropped 0
retry               status Completed  delivered [1 2 3 4 5 6]  supervised failures 2  poison 0  dropped 0
exhausted/Fail    The station could not be reached for reading 3.
exhausted/Fail      status Failed     delivered [1 2]  supervised failures 3  poison 1  dropped 0
exhausted/Resume    status Completed  delivered [1 2 4 5 6]  supervised failures 3  poison 1  dropped 0
recover             status Completed  delivered [1 2 -1]  supervised failures 1  poison 0  dropped 0
```

## What happens by default

Line one and two: no scope, one throw, `status Failed`, and readings 4, 5 and 6
never arrive. The exception you wrote is the exception `run.Completion` faults
with — not wrapped, not replaced.

This is the rule the rest of the page bends, and it is the right default. A
pipeline that swallowed failures would deliver a wrong answer that looks like a
right one.

Note the counters: `supervised failures 0`. That counter counts failures a scope
*handled*. An unsupervised throw is not a handled failure; it is the end of the
run.

## Retries with a backoff ladder

```csharp
.Supervised(
    new SupervisionOptions
    {
        Form = SupervisionForm.Retry,
        MaxAttempts = 3,
        Backoff = [TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(20)],
        OnExhaustion = RetryExhaustion.Fail,
    },
    Flow.For<int>().Select(station.Read))
```

A [supervision scope](../reference/glossary.md#supervision-scope) is a region of
the graph that answers the failures raised inside it. `Supervised` takes the
policy and the flow the policy covers — so the scope's extent is the flow you
hand it, and it is visible in the code rather than inferred.

Line three of the output: **all six readings delivered, run completed, two
supervised failures**. The stage refused reading 3 twice; the third attempt
succeeded; nothing was lost.

Four things about that options record are worth knowing:

- **`MaxAttempts` counts [attempts](../reference/glossary.md#attempt), not
  retries.** Three means one offer and two re-offers. `MaxAttempts = 1` is legal
  and means no re-offer at all.
- **`Backoff` is a ladder, not a base and a multiplier.** Two rungs here — five
  milliseconds before the second attempt, twenty before the third. Whoever reads
  the pipeline sees the exact waits it will take, and no reader has to reproduce
  an arithmetic nobody wrote down.
- **The last rung repeats.** A ladder shorter than the attempt count means "and
  then this long every time". An empty ladder means every re-offer happens at
  once.
- **`Form` is required and has no default.** There is no supervision an author
  could have meant without saying it.

The waits are not jittered. Jitter answers a question a per-element retry inside
one run does not ask — it spreads a fleet's restarts, and there is no fleet
inside a run.

## What happens to an element that runs out of attempts

An element that has used every attempt its scope allowed is a
[poison element](../reference/glossary.md#poison-element). `OnExhaustion` says
what to do with it, and lines four to six of the output are the two answers you
will actually reach for.

**`RetryExhaustion.Fail`** — the default, and it fails the run. `status Failed`,
readings 4 to 6 never arrive, and the counters say what happened: `supervised
failures 3` (one per attempt) and `poison 1`. The exception is still yours,
unchanged.

**`RetryExhaustion.Resume`** — skip the element and carry on. `status
Completed`, and the delivered list is `1 2 4 5 6`: reading 3 is simply absent.
The counters are identical to the failing case — three supervised failures, one
poison element — which is the point of having them. "The run succeeded" and
"nothing went wrong" are two different readings, and the counters are where the
difference lives.

There is also `RetryExhaustion.RestartStage`, which restarts the stage rather
than skipping the element.

## A fallback

```csharp
.Supervised(
    new SupervisionOptions { Form = SupervisionForm.Recover },
    Flow.For<int>().Select(station.Read),
    fallback: -1)
```

`SupervisionForm.Recover` substitutes a value you declared. The last line:
`delivered [1 2 -1]`, run completed, one supervised failure, **zero** poison
elements — because with a fallback in hand nothing was ever poisoned.

**Read that delivered list again: readings 4, 5 and 6 are not there.** Recover
emits the fallback *and ends the scope's stream*. Everything below the scope
drains normally and the run reports success — with fewer elements than it started
with. That is the shape of the operator, and it is the one thing about it people
get wrong. If you want a fallback *per element* with the stream carrying on, you
want retries with `RetryExhaustion.Resume` and a value your downstream can
recognise, or a `Select` that catches inside itself.

`SupervisionForm.Resume` and `SupervisionForm.RestartStage` are the other two
forms: skip the failing element, or restart the stage.

## What a scope may contain

A scope owns the execution of its chain element by element, which means it holds
**element stages only**. Put anything else inside one and it is refused where you
placed it:

```
A supervision scope owns the execution of its chain element by element, so it
holds element stages only: 'local/supervised@v1' at position 1. An asynchronous
stage, a buffer, a junction, and a stage that reads the clock each want a
segment, a channel, or a run of their own. A flattening stage is refused because
its sequence is read after the scope has returned, so a failure inside it would
fall outside the scope it appears to be in; …
```

So: no `SelectAsync`, no `Buffer`, no junction, no `Grouped` or `GroupedWithin`,
no `SelectMany`, no `GroupBy`, and **no scope inside a scope**. Which of two
nested policies wins is not something the library will guess at, so "retry, and
if that runs out substitute a fallback" is not one scope with two answers — it is
a choice between them.

What a scope *does* hold is `Select`, `Where`, and the other per-element stages,
which is where the throw you are trying to answer usually lives. If the failing
thing is an asynchronous call, put the scope around a synchronous wrapper, or
handle it inside the callback.

## Seeing all of it afterwards

Every number above came from one call:

```csharp
RunSnapshot snapshot = run.Snapshot();
```

A [snapshot](../reference/glossary.md#snapshot) is one reading of a run's
observable state: its status, plus five counters.

| Member | What it counts |
|---|---|
| `Status` | `Running`, `Completed`, `Failed`, or `Canceled`. |
| `SupervisedFailures` | Failures a scope handled — one per attempt, so a three-attempt exhaustion counts three. |
| `PoisonElements` | Elements that used up every attempt their scope allowed. |
| `DroppedElements` | Elements a buffer's overflow policy discarded. See [Bounding memory](bounding-memory.md). |
| `Checkpoints` | Checkpoints written, for a durable run. |
| `TotalCheckpointHold` | How long checkpoints held the run in total. |

It is not a consistent cut across the whole run — each number is exact on its
own. On a cluster the same reading is `await run.SnapshotAsync()`, because the
run lives elsewhere.

The same counters are published as OpenTelemetry metrics under the meter name
`Orleans.Dataflow`, so you do not have to poll a handle to see them in production.

## The trade-offs

**A retry re-runs your callback, so it must be safe to run twice.** The library
re-offers the element; it cannot un-send whatever your code already sent. If the
first attempt half-succeeded, the second one meets that.

**A backoff ladder is time the run is not doing anything else.** The waits are
per element and they are taken on the run's own clock, so a long ladder on a
frequent failure is throughput you have spent.

**Retries hide a broken dependency until they cannot.** A pipeline that quietly
retries forever looks healthy right up to the moment it does not. Watch
`SupervisedFailures` rather than only `Status`.

**`Resume` and `Recover` both lose data on purpose.** That is what makes them
useful and what makes them worth writing down where somebody will read it. The
counters are how you find out how much.

**Failure is not the same as cancellation.** A failed run has an
[ending](../reference/glossary.md#ending) of `Failed`; a cancelled run has no
ending at all, because cancelling abandons a run rather than finishing it.

## The failure modes

| Symptom | Cause |
|---|---|
| The run fails on the first throw although you wrote a scope | The throwing stage is outside the scope. `Supervised` covers exactly the flow you hand it — check where the parentheses close. |
| `A scope whose Form is Resume never re-offers an element, so MaxAttempts, Backoff, and OnExhaustion say nothing about what it does and are refused …` | You set the retry members on a form that does not retry. Set `Form = SupervisionForm.Retry`, or drop the members. |
| `A retrying scope offers an element at least once, so MaxAttempts must be 1 or more.` | `MaxAttempts = 0`. One means the exhaustion answer applies to the first failure, which is the smallest legal ladder. |
| `A supervision scope owns the execution of its chain element by element, so it holds element stages only …` | Something other than a per-element stage is inside the scope. See the list above. |
| A `Recover` scope silently shortens your stream | Working as designed. Recover ends the scope's stream after the fallback. Use retries with `RetryExhaustion.Resume` if you want the stream to continue. |
| `Status` is `Completed` but elements are missing | Look at `PoisonElements` and `DroppedElements`. Success and completeness are different claims. |

## Where to look next

- [Bounding memory](bounding-memory.md) — the other counter on the snapshot, and
  the other way elements go missing on purpose.
- [Surviving a crash](../start/surviving-a-crash.md) — what happens when it is
  the whole process that fails rather than one element.
- [Testing and observability](testing-and-observability.md) — testing a pipeline
  deterministically, and seeing what a run is doing in production.
- The repository's failure sample runs a retrying scope and a recovering one in
  both languages:
  [`samples/Orleans.Dataflow.Samples/CSharp/Failure.cs`](../../samples/Orleans.Dataflow.Samples/CSharp/Failure.cs).
  Run it with `dotnet run --project samples/Orleans.Dataflow.Samples -- --only failure`.
