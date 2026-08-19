# Errors

Every public exception the library defines: what it means, what causes it, what a
caller does about it, and whether it ends the run.

If you arrived here from a message in a log, use your browser's find: the message
texts below are the ones the library produces, with the numbers it substitutes
shown in braces.

**The message texts are the library's own.** Each was taken from the code that
builds it; the collect-overflow and buffer-overflow texts below were additionally
provoked in a scratch program and compared against what this page prints. There
are no examples on this page because an exception is not something you write.

**Where a failure surfaces.** A run that fails faults its
[`Completion`](run-handles.md#runhandle--a-run-in-this-process) and every result
slot with the same exception. Locally that is *the very instance* a stage threw;
across a grain boundary it is a
[`PipelineRunFailedException`](#pipelinerunfailedexception) carrying the type name
and message, because carrying the object would require every failure type in
every pipeline to be Orleans-serializable — and then a run whose stage threw an
unprepared exception would fail to report that it had failed.

**Seventeen public exception types ship**, in four assemblies: six in
`Orleans.Dataflow`, seven in `Orleans.Dataflow.Orleans`, one in
`Orleans.Dataflow.Abstractions`, and three in `Orleans.Dataflow.Testing`. Seven of
them end a run; the rest refuse something before a run exists, report a run that
already ended, or belong to the testing package.

| Exception | Assembly | Ends the run |
|---|---|---|
| [`BufferOverflowException`](#bufferoverflowexception) | `Orleans.Dataflow` | ✅ |
| [`CollectOverflowException`](#collectoverflowexception) | `Orleans.Dataflow` | ✅ |
| [`TrackedKeyOverflowException`](#trackedkeyoverflowexception) | `Orleans.Dataflow` | ✅ |
| [`RateLimitExceededException`](#ratelimitexceededexception) | `Orleans.Dataflow` | ✅ |
| [`StreamTimeoutException`](#streamtimeoutexception) | `Orleans.Dataflow` | ✅ |
| [`CheckpointConflictException`](#checkpointconflictexception) | `Orleans.Dataflow` | ✅ |
| [`GrainCallTimeoutException`](#graincalltimeoutexception) | `Orleans.Dataflow.Orleans` | ✅ |
| [`PipelineRejectedException`](#pipelinerejectedexception) | `Orleans.Dataflow.Orleans` | nothing started |
| [`PipelineResumeRefusedException`](#pipelineresumerefusedexception) | `Orleans.Dataflow.Orleans` | nothing started |
| [`PipelineFencingException`](#pipelinefencingexception) | `Orleans.Dataflow.Orleans` | the call is refused |
| [`PipelineRunFailedException`](#pipelinerunfailedexception) | `Orleans.Dataflow.Orleans` | reports one that did |
| [`PipelineRunLostException`](#pipelinerunlostexception) | `Orleans.Dataflow.Orleans` | reports one that is gone |
| [`ResultTooLargeException`](#resulttoolargeexception) | `Orleans.Dataflow.Orleans` | ❌ — the read fails, the run does not |
| [`GraphDocumentFormatException`](#graphdocumentformatexception) | `Orleans.Dataflow.Abstractions` | nothing started |
| [`FaultInjectedException`](#the-testing-package) | `Orleans.Dataflow.Testing` | ✅ (that is its job) |
| [`ProbeTerminatedException`](#the-testing-package) | `Orleans.Dataflow.Testing` | ❌ |
| [`ProviderConformanceException`](#the-testing-package) | `Orleans.Dataflow.Testing` | ❌ |

Every one of them is a type of its own rather than a general-purpose exception
with a recognizable message, and the reason is the same each time: a caller that
wants to tell one of these apart from every other way a run can fail has to be
able to write the `catch`.

---

## Bounds a graph declared

These four are a bound the author wrote being reached. The fix is always one of
three things — raise the bound, reduce what reaches it, or choose the policy that
says what to lose.

### `BufferOverflowException`

`Orleans.Dataflow`. Sealed, derives from `Exception`. No custom state.

**What it means.** A [buffer](options.md#bufferoptions) declared with
`OverflowPolicy.Fail` was full when an element was offered to it.

**Message.**

> A buffer of capacity {capacity} was full when an element was offered to it, and
> its overflow policy is 'Fail'. Raise the capacity, slow the source, or choose a
> policy that drops.

The parameterless constructor carries: *A buffer declared with the fail overflow
policy was full when an element was offered to it.*

**What a caller does.** Raise `Capacity`, slow the producer, or choose one of the
four dropping policies. **Overflow is the only condition this type reports** —
the other four policies never raise it: they wait or they drop, and dropping is
counted rather than thrown, on the run's
[`DroppedElements`](run-handles.md#runsnapshot).

**Ends the run.** The run faults with this very instance.

### `CollectOverflowException`

`Orleans.Dataflow`. Sealed, derives from `Exception`. No custom state.

**What it means.** A [collecting sink](options.md#collectoptions) was handed one
more element than its declared bound allows.

**Message.**

> A collecting sink bounded at {maxElements} elements was handed one more. Raise
> MaxElements, or bound the stream with Take; the sink does not truncate, because
> a shortened list is a wrong result that looks like a right one.

The parameterless constructor carries: *A collecting sink was handed more
elements than its declared bound allows.*

**What a caller does.** Raise `MaxElements`, or bound the stream with `Take(n)`,
which says out loud that you want the first *n*. The sink deliberately does not
truncate.

**Ends the run.**

### `TrackedKeyOverflowException`

`Orleans.Dataflow`. Sealed, derives from `Exception`. No custom state.

**What it means.** A stage that remembers keys was asked to remember one more
than its bound allows. Two operators raise it, and their messages differ by
exactly what each can usefully say.

**Message, from `Distinct`.**

> A distinct stage tracking at most {maxTrackedKeys} keys was handed an element
> with one more. Raise MaxTrackedKeys, or deduplicate over a narrower key; the
> stage does not evict, because evicting a key would let an element it has
> already emitted through a second time.

**Message, from `GroupBy`.**

> A keyed stage holding a substream for at most {maxActiveKeys} keys at once was
> handed an element whose key {key} would have been one more. Raise
> MaxActiveKeys, group over a coarser key, or declare
> ActiveKeyOverflowPolicy.EvictIdle; the stage does not evict by default, because
> an evicted key's substream ends where it stood and the same key can then appear
> downstream a second time.

The parameterless constructor carries: *A stage was asked to remember more
distinct keys than its declared bound allows.*

**The key is in the `GroupBy` message and it is usually the whole diagnosis** — a
null, an identifier that was meant to be coarse, a timestamp used as a key. It is
rendered by the key type's own `ToString`, and a null key is spelled `null`
without quotation marks so it cannot be confused with a key whose text is that
word.

**The rendering is truncated at 64 characters, and the message says when it has
been**, adding the full length: *'…' (the first {n} characters of {length}; a key
this long is the diagnosis)*. This is the one place in the runtime where a value
out of your own data reaches a failure message, and a failure message travels —
it is stored on the run, returned to every caller that polls, and for a durable
run written into the coordinator's persistent state, which nothing prunes. The
cut never lands between the halves of a surrogate pair.

**What a caller does.** Raise the bound, group or deduplicate over a coarser key,
or declare the evicting policy and accept what it changes about the operator's
meaning.

**Ends the run.**

### `RateLimitExceededException`

`Orleans.Dataflow`. Sealed, derives from `Exception`. No custom state.

**What it means.** Either of two things, and they are different defects with
different messages.

**Message, the ordinary one.**

> An element of cost {cost} arrived at a throttle declared as {elements} per
> {period} with {available} of budget available, and its mode is 'Enforcing'.
> Slow the source, raise the rate or the burst, or choose the shaping mode, which
> waits instead of failing.

**Message, the unsatisfiable one.**

> An element of cost {cost} arrived at a throttle whose greatest burst is {burst},
> so no amount of waiting could ever admit it. Raise the burst to at least the
> largest cost the stream can produce, or give the cost function a range the
> throttle can satisfy.

The parameterless constructor carries: *A throttle declared with the enforcing
mode received an element the declared rate had no budget for.*

**What a caller does.** For the first: slow the source, raise `Elements` or
`MaximumBurst`, or choose `ThrottleMode.Shaping`, which waits. For the second:
the element's cost exceeds the whole bucket, so waiting can never help — raise
`MaximumBurst` past the largest cost the stream can produce. **This is the one
place a shaping throttle raises at all**; otherwise it only ever waits.

**Ends the run.**

---

## Time

### `StreamTimeoutException`

`Orleans.Dataflow`. Sealed, derives from **`System.TimeoutException`** — a
subclass rather than the base itself, so a caller who wants to tell a stream's own
silence apart from a timed-out call inside their own callback can write that
`catch` too. No custom state.

**What it means.** The gap between two elements at a `Timeout` stage — or between
the run starting and its first element — exceeded the declared one.

**Messages.**

> No element reached a timeout stage within {gap} of the run starting.

> No element reached a timeout stage within {gap} of the previous one, after
> {elements} of them.

The parameterless constructor carries: *No element reached a timeout stage within
the declared gap.*

The two are separate reports because they are different facts for an author:
nothing arrived at all, or the stream stopped after so many elements.

**What it reports is silence and never slowness.** An element that takes a long
time to travel through the stages *below* the timeout is not a gap, because the
gap is measured where the stage stands.

**The clock is the host's.** A run held by `PauseAsync` for longer than the
declared gap fails when the timer fires — a pause holds the elements, not the
clock.

**Ends the run.**

---

## Durability

### `CheckpointConflictException`

`Orleans.Dataflow.Hosting`, from the core package. Sealed, derives from
`Exception`.

| Member | Type | What it is |
|---|---|---|
| `Presented` | `string?` | the [ETag](glossary.md#etag) the refused writer presented; `null` when it believed the store held nothing |
| `Stored` | `string?` | the ETag the store actually holds; `null` when it holds nothing for the pair |
| `CheckpointConflictException.Superseded(graph, run, presented, stored)` | static | builds the exception a store raises |

**What it means.** A checkpoint write presented an ETag the store no longer
holds, so this writer has been superseded.

**Message.**

> The checkpoint write for the run '{run}' of the graph '{graph}' presents the
> ETag '{presented}' and the store holds '{stored}'. Somebody else wrote this
> checkpoint after this writer read it, so this attempt no longer owns the run and
> its snapshot is refused rather than applied.

"None" is spelled `<none>` rather than left blank, so a first write racing a first
write reads as what it is.

**What a caller does.** *Stop.* This is the one exception on this page whose
answer is not "retry": retrying with the fresh ETag would overwrite a live
attempt's truth with a snapshot of a run that owns nothing, which is exactly the
corruption the ETag exists to prevent. Telling "I have been fenced out" apart
from "the store is unreachable" is why this is a type of its own — the two
answers are opposites.

Two runs under one identity are two writers of one document, so the ordinary
cause is a second materialization under a name that is already live. Starting
fresh over a live run's identity meets this at the first capture, because a fresh
run presents no ETag.

**Ends the run.** Deliberately not swallowed and deliberately not retried.

---

## Refusals before a run exists

### `PipelineRejectedException`

`Orleans.Dataflow.Grains`, from the Orleans package. Sealed, derives from
`Exception`. No custom state — everything travels in the message.

**What it means.** A silo refused a document before starting anything from it. It
is an exception rather than a returned failure state because a start either
produces a ticket or produces nothing: there is no partially started run, no
identity to hand back, and nothing a caller could poll.

**What causes it.** The message says which:

- the bytes are not a canonical graph document — the
  [`GraphDocumentFormatException`](#graphdocumentformatexception) message is
  folded into the text rather than attached as an inner exception;
- the document is for a different pipeline than the coordinator addressed;
- the document does not validate against this silo's catalog, in which case
  **every** compiler diagnostic is in the message rather than only the first — a
  rolling upgrade that removed a stage produces exactly this, and a caller needs
  the whole report;
- a durable run was declared and this silo registered no
  [checkpoint store](hosting.md#silo-settings);
- the declared run identifier is not a valid one;
- a coordinator limit was reached (below).

**The coordinator's limits, and why they exist.** All three are refusals on the
coordinator's own turn, where nothing else about the pipeline is answered while
the work runs:

| Limit | Value | Why |
|---|---|---|
| document size | 4 MiB | Decoding happens on the coordinator's turn, so the size of a document is a bound on what one caller may cost everybody else. |
| nodes per document | 10 000 | Validating, resolving every stage, and compiling the plan are linear or worse in the node count. |
| declared durable run identities per pipeline | 1 000 | Every record keeps the whole document it names and the register is rewritten as one state document on every declaration. |

The last one is the one a long-lived deployment meets. The answer is to retire
identities that are finished with —
`OrleansDataflowHost.RetireDurableRunAsync(pipelineId, runId)` — or to give a
pipeline's runs fewer, longer-lived names. See
[runbooks](../operations/runbooks.md).

**Nothing started.**

### `PipelineResumeRefusedException`

`Orleans.Dataflow.Grains`, from the Orleans package. Sealed, derives from
`Exception`.

| Member | Type | What it is |
|---|---|---|
| `StoredFingerprint` | `string?` | the fingerprint the checkpoint was taken of |
| `DeclaredFingerprint` | `string?` | the fingerprint of the document being offered |
| `PipelineResumeRefusedException.Mismatched(run, stored, declared)` | static | builds the refusal |

**What it means.** A durable run could not be continued, because what the cluster
was asked to continue is not what the checkpoint describes. The resume rule is
**same document, same revision**.

**Message.**

> The durable run '{run}' belongs to the document {stored} and this is an attempt
> to continue it with {declared}. […] Reconcile the document, or run the new one
> under a run identity of its own.

**Two paths reach it**, and they are the same refusal seen from different sides:
a *declaration* of a run identity that already carries a different document is
refused before anything starts — which is where an author who edited a pipeline
and kept its run name meets it; and a *resumed activation* whose checkpoint was
taken of another fingerprint or another revision is refused at the poll that woke
it — which is where a checkpoint written by somebody else meets it.

**What a caller does.** Reconcile the document, or give the new pipeline a run
identity of its own. It is a type of its own precisely because the answer differs
from the other two refusals: this one means "the run exists, its position is on
disk, and the document you handed me cannot continue it", which is a different
action from "fix the deployment" and from "the attempt is gone".

**Nothing started.**

### `GraphDocumentFormatException`

`Orleans.Dataflow.Serialization`, from the abstractions package. Sealed, derives
from `Exception`. No custom state.

**What it means.** Bytes are not the canonical serialization of a graph document.
A document has exactly one canonical byte form, so a reader accepts exactly the
bytes a writer produces and rejects everything else. It is the only exception
document deserialization raises for input it will not accept: a malformed or
non-canonical document never surfaces as a raw parser error, and it is never
repaired on a best-effort basis.

**Message.** Names what was found, the JSON path it was found at — such as
`$.nodes[2].stageRef.majorVersion` — and the rule that rejects it. When the
rejection originates in a lower layer, the original error is the
`InnerException` rather than being flattened into text.

**Nothing started.** Reaching a coordinator, it is re-reported as a
[`PipelineRejectedException`](#pipelinerejectedexception) whose message carries
this one's text, because an exception chain crossing a grain boundary is only as
serializable as its least prepared link.

---

## Cluster ownership and reporting

### `PipelineFencingException`

`Orleans.Dataflow.Grains`, from the Orleans package. Sealed, derives from
`Exception`, and carries Orleans serializer annotations because it crosses the
grain boundary.

| Member | Type | What it is |
|---|---|---|
| `CurrentEpoch` | `long` | the epoch the run was actually started with |
| `CallerEpoch` | `long` | the epoch the refused call carried |

**What it means.** A control call carried an ownership
[epoch](glossary.md#epoch) that is not the one the run it addressed was started
with. The call is either older than the run — a caller holding a ticket from
before some other start — or newer, which is a caller from a future this
activation has not seen. Both are answered the same way, loudly, because a stale
owner silently succeeding is precisely the split brain the epoch exists to
prevent.

**Message.**

> The call carries the ownership epoch {callerEpoch}, and this run was started
> with the epoch {currentEpoch}. A control call restates the claim its ticket was
> issued under, so a different epoch is a claim to a run this is not.

**What a caller does.** Usually nothing — an
[`OrleansRunHandle`](run-handles.md#the-epoch-and-why-a-durable-handle-follows-it)
for a *durable* run adopts the current epoch from this refusal and carries on,
because a resume is the same run continuing. A handle for an *ordinary* run does
not and must not: there, the refusal means somebody else's claim.

**Ownership and existence are different questions and this type answers only the
first.** A call to a grain where no run is active at all is answered by
`PipelineRunLostException`, because "your claim is out of date" and "there is
nothing here to claim" send a caller to different places.

### `PipelineRunFailedException`

`Orleans.Dataflow.Grains`, from the Orleans package. Sealed, derives from
`Exception`, with serializer annotations.

| Member | Type | What it is |
|---|---|---|
| `FailureType` | `string?` | the CLR type name of what the run threw |
| `FailureMessage` | `string?` | its message |
| `RunId` | `string?` | which run |

**What it means.** A run ended because a stage, a source, or a sink threw, and
this is how that reaches a caller on the other side of a grain boundary.

**Message.**

> The run '{runId}' failed with {failureType}: {failureMessage}

When the type was not reported, the text reads *an exception of an unreported
type* in place of the type name.

**What is lost, stated plainly.** The stack, the instance identity, and the
ability to catch by the original type. What is bought is that *every* failure is
reportable — including one whose type has no Orleans serializer.

**A `FailureType` you will not find in this reference.** A distributed keyed grain
call that fails inside its executor reports
`Orleans.Dataflow.Grains.KeyedExecutionFailedException` as its failure type. That
type is internal — you cannot catch it, and it is not public API — but the name
travels as text, so it is a string you can see in a log or in a
[`RunEnding.FailureType`](run-handles.md#runending). Its message reads *The keyed
call '{call}' failed in the executor '{executor}' with {failureType}:
{failureMessage}*, where the executor is `{graph}/{run}/{node}/{key}` — so a
failure names the run, the occurrence, and the partition that produced it.

### `PipelineRunLostException`

`Orleans.Dataflow.Grains`, from the Orleans package. Sealed, derives from
`Exception`. No custom state.

**What it means.** A run that was executing is gone, because the activation
hosting it was recycled and there was nothing to continue it from. A run grain
holds its engine in memory; an activation that goes away takes the attempt with
it, and the fresh activation reports `RunPhase.NotStarted`. A client that had
already seen the run executing translates that into this exception rather than
waiting forever for a terminal state that will never arrive.

**Message.** Names the run and what can no longer be done, for example:

> The run '{runId}' is no longer active in the cluster, so there is no state to
> read. The activation hosting it was recycled and left nothing to continue.

**This is a lost attempt and not a failed pipeline.** Nothing retries and nothing
restarts on its own, because a quiet restart would produce a second execution of
every side effect the lost attempt had already performed.

**A durable run that has written a checkpoint never reports this.** Addressing
the run is what continues it: the activation finds the stored position, claims a
fresh epoch, and is executing by the time the poll is answered. A durable run
that died *before* its first capture reports the loss like any other, because
there is no position to continue from.

**What a caller does.** For an ordinary run: start it again, knowing the previous
attempt's effects happened. For a durable run: this means the run never
checkpointed, so declare a cadence.

### `ResultTooLargeException`

`Orleans.Dataflow.Grains`, from the Orleans package. Sealed, derives from
`Exception`.

| Member | Type | What it is |
|---|---|---|
| `SlotName` | `string?` | which result |
| `Bytes` | `long` | the exact serialized size |
| `MaximumBytes` | `int` | the silo's declared cap |

**What it means.** A run's result is larger than the silo's declared bound, so it
was not sent.

**Message.**

> The result '{slotName}' serializes to {bytes} bytes, and this silo caps a result
> at {maximumBytes}. The run itself completed and its other results resolve
> normally; what is refused is sending this one. Either narrow what the terminal
> accumulates — a Collect over a cluster is the shape this cap exists for — or
> raise the bound with LimitResultSize when the silo is built.

**It does not end the run.** This is the one failure on this page that fails
*that read* and nothing else: the run stays completed, its completion stays
successful, and its other slots resolve normally, because reading a result is not
an event in a run's life.

**The size is exact rather than estimated** — the number of bytes Orleans' own
serializer produces, measured through a writer that counts and discards. The
measurement costs one serialization of a result that is about to be serialized
again, paid once per read of one slot and never per element.

**What a caller does.** Narrow what the terminal accumulates, or raise the bound
with [`LimitResultSize`](hosting.md#silo-settings). The default is 1 MiB. The cap
is the silo's rather than the pipeline's on purpose: how much a host is willing to
put on one message is a property of the deployment and its network.

### `GrainCallTimeoutException`

`Orleans.Dataflow.Adapters`, from the Orleans package. Sealed, derives from
`Exception`. No custom state.

**What it means.** A grain call did not reply within the timeout its stage
declared.

**Message.**

> The grain call '{call}' did not reply within the {timeout} ms this stage
> declared. The call was asked to cancel and the element it was handed is not
> retried; a run's first failure is what the run reports.

**A type of its own rather than an `OperationCanceledException`**, because the two
mean opposite things to a caller: a cancelled run resolves nothing and was asked
to stop; a timed-out call is a run that failed and has a diagnosis. Folding the
timeout into a cancellation would make every expired call look like a shutdown
somebody requested.

**The timeout is this adapter's own** and is enforced whether or not the
registered call honors the token it was given: the wait is bounded here, and the
token is cancelled beside it so that a call which *does* honor it stops rather
than running on unobserved.

**Ends the run.** Nothing retries the element; a retry belongs inside the
registered call, where the duplicate window it opens is the deployment's own to
state.

---

## The testing package

`Orleans.Dataflow.Testing` is a test-support assembly and its three exceptions
belong to tests rather than to production code.

| Exception | Members | What it means |
|---|---|---|
| `FaultInjectedException` | `Arrival` (`long`) | The failure a `TestFlow.FaultPoint` throws when the test has not said what it should throw. It carries the one-based position of the arrival that threw, so a run that fails at the third element when the arming said the second is a run that re-offered one. |
| `ProbeTerminatedException` (derives from `InvalidOperationException`) | — | A probe's wait became impossible because the run it belongs to has ended. A probe that kept waiting would hang the test rather than fail it, and a test that hangs reports nothing at all. The message names the outcome the run actually reached. |
| `ProviderConformanceException` | `Failures` (`IReadOnlyList<string>`) | A provider broke at least one rule a [conformance check](provider-sdk.md#the-conformance-kit) states. The message is a *numbered list of every* failure the check found rather than the first, because a provider author who learns the contract one rejection per run learns it very slowly. |

---

## Exceptions the library does not define

Not every failure gets a type. These are the framework exceptions you will meet,
and what they mean here.

| Exception | When |
|---|---|
| `ArgumentNullException` | An operator or a host was handed `null` where it requires a value. |
| `ArgumentOutOfRangeException` | An options value is outside its bounds — see [options](options.md). Raised where the stage is placed, so the message names the operator's own parameter. |
| `ArgumentException` | A slot presented to the wrong run, an occurrence name that is not a valid identifier, a supervision form with retry-only members set, a registered handle whose shape does not match its catalog entry, or a duplicate registration on a host builder. |
| `InvalidOperationException` | A graph that does not validate against the host's catalog, or is not one the runtime executes. `LocalDataflowHost.MaterializeAsync` names *every* diagnostic rather than the first. |
| `OperationCanceledException` | A run was cancelled, or a caller's own wait token fired. A cancelled run resolves nothing: `Completion` and every result slot cancel. |
| `NotSupportedException` | Only from the four [compile-error guard overloads](operators.md#closing-a-graph), which cannot be reached by compiling code. |
| `TimeoutException` | Its subclass [`StreamTimeoutException`](#streamtimeoutexception) — and Orleans' own response timeout, which is the cluster's rather than this library's. |

---

## Related

- [Run handles](run-handles.md) — where a failure surfaces and how to read it.
- [Options](options.md) — the bounds these exceptions report.
- [Failure and supervision](../concepts/failure-and-supervision.md) — why a
  failure fails a run, and what changes that.
- [Handling failure](../guides/handling-failure.md) — retry, fall back, drop, or
  fail, in a working program.
- [Runbooks](../operations/runbooks.md) — what to do about the cluster-side ones
  in production.
