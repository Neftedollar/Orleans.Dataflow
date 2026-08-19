# Failure and supervision

*What happens when your code throws, and what can you say about it in advance?*

The short answer to the first half is: the run ends and your exception reaches
you. That is the default, it is not configurable globally, and it is the right
default. This page is about the second half — the places where you can declare,
in advance and in the graph, that a particular region should answer a failure
differently.

## The default: a throw ends the run

```csharp
RunnableGraph throwing = Source.From(new[] { 1, 2, 3, 4 })
    .Select(n => n == 3 ? throw new InvalidOperationException("element 3 is bad") : n)
    .To(s => s.Aggregate(0L, (count, _) => count + 1), "seen", out ResultSlot<long> seen);
```

```text
Completion threw InvalidOperationException: element 3 is bad
slot threw InvalidOperationException: element 3 is bad
WatchTermination resolved: failed with System.InvalidOperationException: element 3 is bad
snapshot: Failed: dropped 0, supervised 0, poison 0, checkpoints 0, held 00:00:00
```

Four things happen, and all four are worth naming.

- **Failure wins.** No element is observed after a failing one, anywhere in the
  graph, including on a [junction](../reference/glossary.md#junction) leg that
  was not the one being read.
- **Your exception reaches you unwrapped.** Awaiting
  [`Completion`](../reference/glossary.md#completion) rethrows that very
  instance, not something wrapping it.
- **Every result slot faults with the same exception.** A slot carries the run's
  outcome; there is no reading in which the run failed and a fold still resolved.
- **The [ending](../reference/glossary.md#ending) is a value you can read
  instead.** `WatchTermination` resolves — successfully — with `Failed` carrying
  the type name and message, which is the shape a supervisor or a metric wants.

That is the whole default. Everything below is you choosing to weaken it, in one
declared region, on purpose.

## A supervision scope

A [supervision scope](../reference/glossary.md#supervision-scope) is a region of
your pipeline that you have declared a failure policy for. It is not an
annotation and not a runtime flag — **it is a stage**, and the policy is written
into the [graph document](../reference/glossary.md#graph-document). Two graphs
whose scopes differ in form, in attempt count, or in one rung of backoff are two
different graphs with two different fingerprints.

That matters beyond tidiness: because the policy is in the document, it is
something a cluster could honor, a checkpoint could be taken across, and a
reviewer could read off the pipeline without reading the code.

```csharp
RunnableGraph retrying = Source.From(orders)
    .Supervised(
        new SupervisionOptions
        {
            Form = SupervisionForm.Retry,
            MaxAttempts = Attempts,
            Backoff = Backoff,
            OnExhaustion = RetryExhaustion.Fail,
        },
        Flow.For<OrderEvent>().Select(order => flaky.Pass(order)))
    .Select(OrderDocument.FromEvent)
    .To(s => s.ForEach(document => retried.Add(document.OrderId)));
```

> From the `failure` scenario,
> [`samples/Orleans.Dataflow.Samples/CSharp/Failure.cs`](../../samples/Orleans.Dataflow.Samples/CSharp/Failure.cs).

The second argument is the scope: the flow whose failures this policy answers
for. Everything outside it keeps the default.

## The four forms

`Form` is `required` and has no default, because there is no supervision you
could have meant without saying it.

| Form | What it does with the failing element | What it does with the scope's state |
|---|---|---|
| `Resume` | Drops it. | **Keeps** it. A scan goes on counting; a half-filled batch stays open. |
| `RestartStage` | Drops it. | **Resets** it. A scan returns to its seed, a distinct forgets its keys, a batch abandons its open group. |
| `Retry` | Offers it again, up to a declared count, waiting a declared ladder between attempts. | Kept between attempts; the exhaustion answer decides what happens after. |
| `Recover` | Replaces it with a declared fallback **and ends the scope's stream successfully.** | n/a — the stream is over. |

There is no fifth value for "fail the run", because that is not a form a scope
takes. It is what happens *outside* every scope, and it stays the default.

`Resume` and `RestartStage` differ in exactly one word and the difference is
visible in the output. The same graph, the same elements, the same injected
failure, one enumeration member apart — a scope holding a running total, with the
third element failing:

```text
Resume        [1, 4, 8]
RestartStage  [1, 3, 7]
```

`Resume` kept the running total across the failure, so the survivors continue
from where the count was. `RestartStage` reset it. A test that counted elements
would pass for both, which is why the difference is shown by value.

## Retry, by the numbers

Three fields are read **only** for `Retry` and are refused on the other three
forms — an attempt count on a scope that does not retry is a number nothing
reads, and admitting it would put a statement in the document the graph cannot
honor.

**`MaxAttempts`** counts [attempts](../reference/glossary.md#attempt), not
retries. Three means one offer and two re-offers. One is legal and means "no
re-offer": the exhaustion answer is applied to the first failure.

**`Backoff`** is an explicit ladder of `TimeSpan`s rather than a base and a
multiplier, because a ladder is what a document can state exactly — a reader of
the graph sees the waits the run will actually take, and nobody has to reproduce
an arithmetic that was never written down.

- **The last rung repeats.** A ladder shorter than the attempt count reads as
  "and then this long every time".
- **An empty ladder means every re-offer happens at once.**
- **A rung of zero is admitted**, which is the one place this library's rule
  against zero durations bends: "try again now" is the ordinary shape of a first
  rung.
- **There is no jitter.** Jitter answers a question a per-element retry inside
  one run does not ask — it spreads a *fleet's* restarts, and there is no fleet
  inside one run. Adding a random source would also make the one thing worth
  proving about a ladder, that the waits are exactly what the document says, a
  statistical claim instead of an exact one.
- The waits are taken on the run's own clock, so they are released by a shutdown
  or a cancellation rather than holding a stop for their duration.

**Re-offering goes to the scope's *first* stage.** A stateful stage inside a
retrying scope therefore sees the element once **per attempt** — which is a real
consequence, and the reason to keep a retrying scope small. It is also why the
exhaustion answer is allowed to escalate to `RestartStage`: after three attempts,
a scan inside the scope has counted one element three times.

A retry that succeeds looks like this:

```text
threw 2 times, delivered [1, 2, 3]
snapshot: Completed: dropped 0, supervised 2, poison 0, checkpoints 0, held 00:00:00
```

Nothing was lost. Note `supervised 2`: the counter moves **once per failed
attempt**, because an attempt that failed was swallowed and "how much did this
run swallow" is the question that counter answers. The run reporting success and
nothing having gone wrong are two different readings, and the counters are where
the difference lives.

## The element that exhausts them

An element that has used every attempt its scope allowed is a
[poison element](../reference/glossary.md#poison-element). `OnExhaustion` says
what happens to it, and there are exactly three answers:

| `OnExhaustion` | Result |
|---|---|
| `Fail` (default) | The run fails with the exception of the **last** attempt — your instance, not a wrapper naming the attempts. |
| `Resume` | The element is dropped, the scope's state is kept, the run carries on. |
| `RestartStage` | The element is dropped, the scope's state is reset, the run carries on. |

The same graph over `[1, 2, 3, 4]` with an element that never works, at three
attempts, under each answer:

```text
Fail          attempts 3  delivered [1]        failed: this element never works
              Failed: dropped 0, supervised 3, poison 1, checkpoints 0, held 00:00:00
Resume        attempts 3  delivered [1, 3, 4]  completed
              Completed: dropped 0, supervised 3, poison 1, checkpoints 0, held 00:00:00
RestartStage  attempts 3  delivered [1, 3, 4]  completed
              Completed: dropped 0, supervised 3, poison 1, checkpoints 0, held 00:00:00
```

Read the counters. `supervised 3` — three failed attempts. `poison 1` — one
element used them all. Both move for `Fail` as well, which is what distinguishes
a run that failed after exhausting its retries from one that failed on its first
element.

There is no `Retry` among the exhaustion answers, because retrying an element
that has run out of retries is not an answer; and no `Recover`, because ending
the stream after a fallback is a decision about the *stream* rather than about
this element.

## Fail, drop, or substitute

Three outcomes, and they are genuinely different things. Choose deliberately.

**Fail the run.** The default, and `OnExhaustion = Fail`. Nothing is lost and
nothing is hidden; the pipeline stops and you find out. Choose it when a failure
means the pipeline's assumptions are wrong.

**Drop the element.** `Resume` and `RestartStage`. The stream carries on with one
fewer element. It is not silent — the supervised-failure count moves, and the
poison count moves if the drop followed exhausted attempts — but it *is* a loss,
and the downstream fold will be short by one. Choose it when one bad element is
noise rather than news.

**Substitute a fallback.** `Recover`, and it is the odd one out in two ways: it
produces an element rather than dropping one, and it **ends the scope's stream**.
Everything above the scope stops, everything below it drains, the result slots
resolve, and the run reports **success** with fewer elements than it started
with:

```csharp
RunnableGraph recovering = Source.From(orders)
    .Supervised(
        new SupervisionOptions { Form = SupervisionForm.Recover },
        Flow.For<OrderEvent>().Select(order => poison.Pass(order)),
        fallback)
    .Select(OrderDocument.FromEvent)
    .To(s => s.ForEach(document => recovered.Add(document.OrderId)));
```

```text
orders-in-the-feed                  6
recover/times-the-stage-threw       1
recover/orders-delivered            order-000 order-001 order-fallback
recover/run-status                  Completed
recover/supervised-failures         1
```

Six orders in, three out, and the run says it succeeded. That is the contract,
not a surprise: `Recover` means "when this fails, emit this instead and we are
done", so it is the right answer for a pipeline whose downstream needs a
well-formed final element and the wrong answer for one that must process every
element. Recovering with an *alternate source* — carry on from somewhere else —
is a different capability and is deliberately not a setting here.

## What supervision does not cover

Five lines, each a different kind of limit. None of them is hidden.

**A failure outside every scope fails the run.** That is the whole point of a
scope being a *region*. A failure one stage earlier than the scope, one stage
later, or on a junction leg beside it is unsupervised and ends the run.

**A cancellation is not a failure, and no form weakens it.** A scope catches
`OperationCanceledException` only to rethrow it. A scope that swallowed
cancellation would turn a stop request into a stream that will not stop:

```text
cancelled; snapshot Canceled: dropped 0, supervised 0, poison 0, checkpoints 0, held 00:00:00
```

`supervised 0` — the scope was there and did not count anything, because there
was nothing to supervise.

**No form names an exception type.** A policy that filtered by type would need
CLR type names in a document, which the definition plane forbids, or a declared
failure taxonomy, which does not exist. A scope supervises every failure raised
inside it alike. If you need to distinguish, distinguish in your own code and
rethrow what you want the scope to see.

**Observability is per run, not per scope.** A graph with three scopes reports
one `supervised` number and one `poison` number for all of them.

**A failure raised while a stream is ending is not supervised.** When the stream
is closing, the walk that hands over whatever a stage was still holding — a
partial batch, say — has no failing element to drop, nothing to re-offer, and no
fallback question to ask. A failure there travels to the run like any
unsupervised one.

## Why a scope inside a scope is refused

You will want to write "retry three times, and if the retries run out, substitute
a fallback". It is not one scope with two answers, and it is not two nested
scopes. It is **a choice between them**, and the library refuses the nesting by
name rather than picking a winner:

```csharp
Source.From(new[] { 1 })
    .Supervised(
        new SupervisionOptions { Form = SupervisionForm.Resume },
        Flow.For<int>().Supervised(
            new SupervisionOptions { Form = SupervisionForm.Retry, MaxAttempts = 3 },
            Flow.For<int>().Select(n => n)))
    .To(Sink.Ignore<int>());
```

```text
A supervision scope owns the execution of its chain element by element, so it holds element
stages only: 'local/supervised@v1' at position 1. […] a nested scope and a group-by are
refused as this version's honesty, and both are stated in the documentation.
```

The reason is that "which of two nested policies wins" is a contract nobody has
written. Consider: an inner scope retries, an outer scope restarts on failure —
does the outer restart reset the inner one's attempt counter? Does an inner
`Recover` that ends its stream end the outer scope's stream too? Does an outer
`RestartStage` rebuild the inner scope, and if so, what happens to an element the
inner scope was mid-retry on? Every one of those has a defensible answer and no
two people pick the same set. Rather than ship a nesting whose meaning nobody
could predict, the library refuses it — loudly, at authoring time, with the
offending stage named.

So the sample that wants both behaviors is **two graphs rather than one**, and it
says so.

### What else a scope's chain refuses

The same rule refuses four more shapes, and each has its own reason:

- **An asynchronous stage, a buffer, a junction, or a stage that reads the
  clock.** Each of those wants a segment, a channel, or a run of its own, and a
  scope executes its chain element by element inside one call.
- **A flattening stage** (`SelectMany`). This is the sharpest one. What a scope
  hands back for a flattening stage is a *sequence*, which the run reads after
  the scope's own work has returned — so a failure raised while that sequence was
  enumerated would happen **outside the scope it appears to be inside**.
  Supervision that silently did not apply is worse than a refusal.
- **A group-by.** A key table whose reset is a scope's business is a second
  feature. The composition that is *not* refused is the useful one: a keyed stage
  **beside** a scope rather than inside it.
- **A scope inside a group flow**, for the mirror reason: a scope reads the run's
  clock, and one instance of it per key is not something a fused per-key chain
  can hold.

Every one of these is refused at authoring time, before the run has an element to
supervise. A disagreement between what a document says and what the runtime can
execute fails materialization; everything a scope can answer for happens after
the plan was accepted.

## Composing supervision with durability

A supervision scope and a [durable](durability.md) scope are separate things and
that separation is deliberate. They answer different questions — what a *failing
element* costs, and what a *dead process* costs — and folding them together would
force every author who wants durable state to declare a failure policy, and every
author who wants a retry to decide about durability.

There is also a place where they would contradict each other: `RestartStage`
resets every state in its scope, and a durable scope keeps every state across a
resume. A single scope that was both would have a contract with a hole in it.
Kept apart, each says exactly one thing, and the composition you actually want —
a durable scope inside a supervised section — stays a composition.

## Where to go next

- [Handling failure](../guides/handling-failure.md) — complete programs for
  retry, fallback, drop, and fail.
- [Runs and results](runs-and-results.md) — what a failed run does to your
  results and to completion.
- [Durability](durability.md) — surviving a process death rather than a bad
  element.
- [Errors](../reference/errors.md) — every exception the library raises, what
  causes it, and what to do.
- [Options](../reference/options.md) — every field of `SupervisionOptions` with
  its default.
