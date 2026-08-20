# ADR 0009: Deployable plumbing

- Status: Proposed
- Date: 2026-08-20
- Depends on: [ADR 0001](0001-definition-runtime-authoring-planes.md) (no
  delegate enters a document — the rule this ADR does not weaken and leans on),
  [ADR 0004](0004-csharp-api-baseline.md) (the authoring surface whose operators
  gain a name), [ADR 0005](0005-junction-semantics.md) (the buffer boundary a
  cycle needs — the clearest case of plumbing that is not behaviour)
- Related: [ADR 0008](0008-code-bound-pipelines.md), which proposes carrying
  behaviour that this one deliberately does not

## The problem, measured

A pipeline of registered stages is deployable. Put one buffer in it and it is
not. Measured, on the current sources:

| Graph | Capabilities declared | `AsPipeline` |
|---|---|---|
| registered source → registered sink, both named | none | accepted |
| the same, with `.Buffer(new BufferOptions { Capacity = 8 })` between them | `ephemeral-identity`, `nondeployable` | refused |

The buffer carries no delegate. Its whole configuration is the number 8, the
document can state it, and a reader for it already exists — the local runtime
plans a buffer by reading its capacity out of the canonical payload, not out of
a CLR field. Nothing about a buffer is bound to the process that authored it,
and the library refuses it anyway.

That refusal is the reason an author who wants backpressure, batching, a take, a
skip, or a delay inside a deployable pipeline must publish a stage of their own
for it — a provider identity, a catalog entry, a factory branch, and a version,
for a queue of eight.

## Why it happens

Two independent rules, both correct in general and both too broad here.

**`nondeployable` is applied to every local stage without exception.**
`LocalStageCatalog` builds one specification per member of `LocalStageKind` and
requires the token on all of them. That is right for `Select` and `Where`, whose
behaviour is a delegate. It is not right for the twenty-one shapes whose
descriptor is constructed with `behavior: null` — the delegate slot is empty
because there is no delegate to put there.

**`ephemeral-identity` is added once per unnamed occurrence.** The mechanism is
already the right one: a named occurrence contributes nothing, and the token is
"this document's node identifiers are positions". What is missing is a spelling —
the local operators take no name, so every one of them is unnamed by
construction.

## What is and is not publishable

Counted rather than estimated, over the twenty-one delegate-free constructions:

| Kind | Publishable through the existing seam? |
|---|---|
| `Empty`, `Never`, `Range`, `Tick` | yes — `DataflowStageRuntime.Source` |
| `Broadcast`, `Balance` | yes — fan-out |
| `Merge`, `Concat`, `Interleave` | yes — fan-in |
| `Ignore`, `First`, `Count`, `Last` | yes — `Terminal` |
| `FirstOrDefault(d)`, `LastOrDefault(d)` | no — `d` is an arbitrary CLR value with no payload spelling |
| `Take`, `Skip`, `Buffer`, `Delay`, `TakeWithin`, `SkipWithin`, `Valve` | **no — there is no runtime shape for them** |

The second "no" is the important one, and it is the reason this ADR exists
rather than a pull request that registers a `local` factory. `DataflowStageRuntime`
has six shapes and refuses to grow past what the engine runs. A buffer is not a
source, an element, or a terminal; it is a queueing boundary the engine
implements structurally, which is exactly why a cycle is relieved by one and not
by a delay. Publishing buffers through the provider seam would mean adding
engine primitives to a public interface that says, in its own documentation, that
a stage wanting a seventh shape is asking for a new engine primitive.

**And the publishable half alone is worth almost nothing.** A pipeline built only
from `Range`, `Merge` and `Count` performs no transformation, because every
transforming operator carries a lambda. The value of this vocabulary is not that
it makes pipelines on its own. It is that it is the plumbing *between* stages
that are already registered.

## The decision

Do not register a `local` provider factory. Instead, let the deployable path
rehydrate a delegate-free local node into the descriptor the planner already
knows how to plan.

The planner's entry point is already document-driven:

```
Compile(document, bindings, binder, runIdentity, clock)
```

Each node is asked of `bindings` first and of `binder` second. On the local path
`bindings` holds every local node; on the deployable path it is empty, so a
`local/buffer@v1` node reaches the binder and there is no factory for it. The
change is to fill `bindings` on the deployable path for exactly the delegate-free
kinds, from the document alone:

```
node whose stage is local/<kind>@v1 and whose kind is delegate-free
    → LocalStageDescriptor(kind, behavior: null, seed: <the kind's own>, node.Parameters)
```

Nothing downstream changes. Fusion, the buffer boundary rule, cycle relief, and
every payload reader are the same code reading the same bytes, which is the whole
argument for this shape over a second implementation: **there is no second
implementation to keep in step.**

Two rules make it safe:

- **The kind must be delegate-free**, decided by the vocabulary rather than by a
  list kept beside it, so a shape that grows a delegate stops being rehydratable
  the moment it does.
- **A `local` reference that is not rehydratable is refused by name**, at
  validation, saying which stage and why. A silo must never accept a document it
  will fail to build.

And correspondingly:

- `LocalStageCatalog` requires `nondeployable` on the kinds that have behaviour
  and not on the kinds that do not.
- Every delegate-free operator takes an optional occurrence name, so an author
  can drop `ephemeral-identity`.

## The name is the author's, and the default is not a name

An unnamed occurrence must keep contributing `ephemeral-identity`. Generating a
name is tempting and the two obvious generators are both wrong:

- **Random** is not a name at all. A run of the same program would produce a
  different document, a different fingerprint, and a checkpoint that anchors to
  nothing.
- **Positional** is what the token already warns about. `node-3` renames itself
  when a stage is inserted above it, which is precisely the anchoring failure
  `ephemeral-identity` names.

A derived name — `buffer`, then `buffer-2` for a second one — is stable across
builds and survives an unrelated insertion, but still shifts when a sibling of
the same kind is inserted before it. That is better than positional and is still
not something to hand a checkpoint silently. So: an author who wants a durable
run names their stages, and the refusal says so. A derived default may be
reconsidered for the non-durable tier, where nothing anchors, and is out of scope
here.

## What this does not do

**It does not make lambdas deployable.** `Select` and `Where` are unchanged and
still declare `nondeployable`. That is [ADR 0008](0008-code-bound-pipelines.md)'s
subject, and this ADR is deliberately the part that needs no new trust model:
every byte a rehydrated stage is built from is already in the document, already
fingerprinted, and already covered by the identity.

**It does not change any document.** A graph that is deployable today is
byte-identical after this change; a graph that was refused becomes acceptable
without its bytes moving. The tokens a document declares change, and tokens are
in the document — so a graph that used to declare `nondeployable` and now does
not has a different fingerprint. That is correct and is the point: it is a
different document, because it says something different about itself.

**It does not cover `FirstOrDefault` and `LastOrDefault`.** Their default is a
CLR value with no canonical spelling. They keep `nondeployable` and the refusal
names them.

**It does not claim the deployable and local paths run identically.** They run
the same planner over the same payloads, which is a much stronger start than two
implementations, but the deployable path has no binding table and a different
run identity, and the tests have to be written against a silo rather than
inferred from the local ones.
