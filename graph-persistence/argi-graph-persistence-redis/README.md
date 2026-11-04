# ARGI Redis Graph Persistence

This module provides the maintained Redis checkpoint savers, the Redis-backed
long-term Store implementation and the Redis `RunStore` for ARGI extensions.

## RedisSaver

`RedisSaver` keeps the existing checkpoint key layout and serialized checkpoint
content for compatibility with the legacy Core Redis saver. Applications can read
checkpoints written by either implementation without migration.

Checkpoint reads now distinguish a missing checkpoint from Redis lock contention:

- missing active thread metadata still returns empty `get` and `list` results;
- a read lock timeout throws `IllegalStateException`;
- an interrupted read restores the interrupt flag before throwing
  `IllegalStateException`.

## RedisStore

`RedisStore` is the adopted Redis implementation of the Core `VersionedStore`
contract. It requires a caller-owned `RedissonClient` and a matching Core build
that contains `VersionedStore` and `VersionedStoreItem`; older Core main
artifacts without those contracts cannot consume this implementation. Import the
Extensions Redis Store explicitly because Core still has a legacy class with the
same simple name:

```java
import io.github.agentic.ai.graph.persistence.redis.RedisStore;

RedisStore store = new RedisStore(redisson);
RedisStore isolated = new RedisStore(redisson, "my:store:hash");
```

The default Redis hash key is `argi:store:items:v1`. Custom storage keys isolate
stores from each other.

Each Store record is one Redis hash field. The field name reuses `BaseStore`'s
namespace/key encoding, and the value is a single JSON envelope containing:

- the current version;
- the serialized `StoreItem`;
- a tombstone flag.

Conditional writes use Redisson's atomic `putIfAbsent` and exact-value
`replace` operations against the complete serialized envelope. Version arithmetic
uses Java `Math.addExact`, so overflow fails before mutation. Negative expected
versions are rejected.

Adopt conditional writes by reading the current version, preparing the next
`StoreItem`, then writing only if that version is still current:

```java
long version = store.getVersionedItem(namespace, key).version();
StoreItem next = StoreItem.of(namespace, key, value);

if (!store.putItemIfVersion(next, version)) {
    // Another writer changed the item. Reload and re-plan instead of overwriting.
}
```

Deletes and `clear()` retain persistent tombstones. A deleted key keeps its
positive version so stale writers cannot recreate it with expected version zero.
Unconditional writes, deletes, and clear operations use bounded read/CAS retries.

Search, namespace listing, size, and clear scan the hash and ignore tombstones.
These scans are not cross-item snapshot transactions; `clear()` requires
quiescent writers for a complete purge. Tombstone reclamation needs a future
retention design.

## RedisVersionedSaver

`RedisVersionedSaver` is the Redis implementation of Core's
`VersionedCheckpointSaver` contract for checkpoint compare-and-set execution.
It is an explicit new-format saver; it does not read, migrate, update, or delete
the legacy `RedisSaver` keys.

```java
import io.github.agentic.ai.graph.persistence.redis.RedisVersionedSaver;

var saver = RedisVersionedSaver.builder()
    .redisson(redisson)
    .stateSerializer(StateGraph.DEFAULT_JACKSON_SERIALIZER)
    .build();
```

The builder requires a caller-owned `RedissonClient`, accepts the same
`StateSerializer` style as `RedisSaver`, and can be isolated with
`storageKey(String)`. The default Redis hash key is
`argi:checkpoint:versioned:v1`.

Each checkpoint namespace is one `RedisStore` item:

- Store namespace: `["checkpoints"]`;
- Store key: `BaseCheckpointSaver.checkpointThreadId(config)`;
- Store value: `content`, a Base64 checkpoint-history payload written with
  `CheckPointSerializer`.

Versioned writes and releases use `RedisStore.putItemIfVersion` against the
single Store envelope. A stale conditional mutation throws
`CheckpointConflictException` and does not fall back to an unconditional write.
Successful puts and releases advance the Store version exactly once. Release
writes an empty history item, including release of a never-written namespace, so
the positive tombstone revision is retained and stale version-zero writers
cannot recreate the namespace.

Plain `put` and `release` remain compatibility methods and use bounded CAS
reload/retry loops. Graph runtime paths use `putIfVersion` and
`releaseIfVersion` through Core's versioned scope.

## RedisLeasedCheckpointSaver

`RedisLeasedCheckpointSaver` is the opt-in Redis implementation of Core's
`LeasedCheckpointSaver` contract. It adds distributed admission plus checkpoint
fencing for leased graph execution. Applications must select it explicitly:

```java
import io.github.agentic.ai.graph.persistence.redis.RedisLeasedCheckpointSaver;

var saver = RedisLeasedCheckpointSaver.builder()
    .redisson(redisson)
    .stateSerializer(StateGraph.DEFAULT_JACKSON_SERIALIZER)
    .leaseOptions(LeaseOptions.defaults())
    .build();
```

The builder requires a caller-owned `RedissonClient`, accepts the same
`StateSerializer` style as the other savers, and can be isolated with
`storageKeyPrefix(String)`. The default key prefix is
`argi:checkpoint:leased:v1`. Physical keys are one Redis hash per resolved
checkpoint namespace:

```text
<prefix>:{<Base64 URL without padding of checkpointThreadId(config)>}
```

Each hash stores only the leased saver fields:

- `owner`, the current lease owner UUID;
- `fence`, the retained decimal int64 fencing token;
- `expires`, the Redis server epoch millisecond expiry;
- `revision`, the decimal int64 checkpoint revision;
- `history`, the Base64 checkpoint-history payload written with
  `CheckPointSerializer`.

This keyspace is separate from both `RedisSaver` legacy keys and
`RedisVersionedSaver` store envelopes. It does not read, migrate, update, or
delete either older format. Switching an existing conversation to the leased
saver therefore requires explicit migration or a new namespace.

Lease acquire, renew, release, versioned reads, fenced puts, and fenced releases
use Redisson `RScript` with `StringCodec` and `READ_WRITE` routing to the
primary. The Lua scripts receive exactly one key, use Redis `TIME` for expiry,
validate counters and field pairs before writes, keep counters as exact decimal
strings, and retain the fence and checkpoint revision without whole-key TTLs.

`put`, `release`, `putIfVersion`, and `releaseIfVersion` remain ownerless Core
compatibility methods and fail with `LeaseRequiredException`. Runtime graph
paths acquire a lease first and then call the leased mutation methods. Reads
remain available without a lease.

Redis or Valkey failover is not a consensus fencing system. The saver assumes one
authoritative retained hash history; asynchronous HA failover, backup restore, or
data rollback can roll back owner/fence/checkpoint state. Leases also do not
provide message queues, worker recovery, tool receipt tracking, or exactly-once
side-effect execution. External side effects still need their own idempotency or
receipt protocol.

## RedisRunStore

`RedisRunStore` is the Redis/Valkey implementation of Core's `RunStore`
contract: idempotent run submission, one nonterminal run per session
(reservation), namespace leases with fencing tokens, and fenced, revision-checked
commits of run state, checkpoint, tasks, journal and outbox. It passes the shared
Core `RunStoreContractTest`.

```java
import io.github.agentic.ai.graph.persistence.redis.RedisRunStore;

RedisRunStore store = RedisRunStore.builder()
    .redisson(redisson)               // required, caller-owned
    .clock(Clock.systemUTC())         // default; the only time source
    .keyPrefix("argi:run:v1")         // default
    .durableDeployment(false)         // default; see below
    .build();
```

### Key layout

All keys of one namespace (tenant, application, session) share one cluster hash
tag `T`, the Base64 URL SHA-256 of the namespace identity:

```text
<prefix>:{T}:ns                 reservation, fence, lease, submission dedup
<prefix>:{T}:run:<h>            run record (spec, revision, state, checkpoint)
<prefix>:{T}:run:<h>:tasks      task records
<prefix>:{T}:run:<h>:journal    append-only journal
<prefix>:{T}:run:<h>:outbox     append-only EVENT outbox
```

`<h>` is the Base64 URL SHA-256 of the run id. The key prefix must not contain
`{` or `}`. Keys never get a TTL: runs, dedup mappings and fences are kept
forever, and there is no retention or cleanup yet. This keyspace is separate from
the checkpoint savers and `RedisStore`.

Every operation is one Lua script routed to the primary, sent without Redisson
client-side retries. Scripts re-check reservation, owner, fence, lease expiry and
revision, and validate stored data, before their first write. Corrupt or
unexpected stored data, including a key of the wrong Redis type, fails with
`IllegalStateException` and is never repaired. Transport errors and timeouts are
rethrown unchanged; after a timeout the outcome of the call is unknown and the
caller should re-read with `find` before re-planning.

Each commit reads the whole run, so commit cost grows with the size of the run's
journal and outbox.

### Clock

The injected `Clock` is the only time source; Redis server time is never used.
Lease expiry keeps the clock's nanosecond precision, like `MemoryRunStore`. All
JVMs that share a namespace need synchronized clocks (NTP), with skew far below
the shortest lease TTL. Clock instants before 1970 or after 2262 are rejected with
`IllegalStateException`.

### Deployment requirements

`capabilities().persistent()` is `false` unless `durableDeployment(true)` is set.
Enable it only when all of the following hold:

- `maxmemory-policy noeviction`, so fences, reservations and dedup mappings are
  never evicted;
- AOF enabled with `appendfsync always`; with `everysec` a crash can lose the
  last fence increment and re-issue a fencing token;
- no asynchronous failover, backup restore or other rollback of acknowledged
  writes. Redis replication is asynchronous: a promoted replica can miss the
  latest fence, reservation, dedup or commit, so two owners can hold the same
  fencing token and an acknowledged commit can disappear. Use a single primary
  without automatic failover, or a synchronously replicated service.

Fencing protects writes to this store only. External side effects (tool calls,
messages) still need their own idempotency or receipt protocol.

## RedisLeasedProgressStore

`RedisLeasedProgressStore` is a new opt-in adapter implementing the
engine-neutral `LeasedProgressStore` contract: one session lease
(claim/renew/release) whose owner/fence guards run state, checkpoint, journal
and outbox writes together. It removes the split state between an independent
run lock and an independent checkpoint lock: after a takeover, the previous
owner loses heartbeat, release and every progress write at once. The existing
`RedisRunStore`, `RedisLeasedCheckpointSaver` and legacy savers are unchanged,
and this adapter never reads or migrates their keys.

```java
import io.github.agentic.ai.graph.persistence.redis.RedisLeasedProgressStore;
import io.github.agentic.ai.graph.persistence.redis.LeasedProgressStore.ProgressCommit;
import io.github.agentic.ai.graph.persistence.redis.LeasedProgressStore.CheckpointWrite;

RedisLeasedProgressStore store = RedisLeasedProgressStore.builder()
    .redisson(redisson)               // required, caller-owned
    .keyPrefix("argi:progress:v1")    // default; must not contain '{' or '}'
    .build();                         // verifies single-slot layout at startup

var lease = store.claim(sessionId, runId, ownerId, Duration.ofSeconds(30));
store.commit(lease, new ProgressCommit("RUNNING", 0,
    new CheckpointWrite(0, checkpointPayload), journalEntries, outboxEntries));
```

### Key layout and lease semantics

All keys of one session share one cluster hash tag `T`, the Base64 URL SHA-256
of the session id:

```text
<prefix>:{T}:ns                 reservation, retained fence, live owner/expiry
<prefix>:{T}:run:<h>            run record (revision, state, checkpoint, cp_revision)
<prefix>:{T}:run:<h>:journal    append-only journal
<prefix>:{T}:run:<h>:outbox     append-only outbox
```

`<h>` is the Base64 URL SHA-256 of the run id. Keys never get a TTL; fences and
progress are retained. Claiming bumps the retained fence, so fencing tokens
increase monotonically per session. Lease expiry is measured only by Redis
server `TIME` in the Lua scripts, so callers need no synchronized clocks.
Two run ids of one session share the single live owner: a second claim is
rejected with `LeaseBusyException` until the live lease expires or is released,
after which the new claim fences the old owner out. An expired or superseded
owner's `renew` throws `LeaseLostException`, `release` returns `false`, and
`commit` is rejected. Releasing keeps the fence and run progress; a released
owner cannot commit anymore.

Every mutation is one Lua script routed to the primary and sent without
client-side retries. A commit re-checks reservation, owner, fence, expiry, the
run revision and, when a checkpoint is supplied, the checkpoint revision, plus
journal/outbox entry id uniqueness, before its first write: any failed check
aborts the whole commit, leaving no half checkpoint or half run progress. There
is no ownerless write path and no fallback to unguarded writes. Revision and
append-only conflicts throw `ProgressConflictException`; after a transport error
or timeout the outcome is unknown and callers should re-read with `snapshot`
before re-planning. Reads never require a lease.

At startup the builder verifies that the key layout is single-slot and that the
deployment accepts multi-key scripts over it; a cross-slot or unsupported
deployment fails fast (`verifySlotsAtStartup(false)` skips this check and is
intended for tests only).

Asynchronous HA failover is not consensus fencing: a promoted replica that
missed recent writes can re-issue a fencing token and lose acknowledged
progress, so no RPO0 is promised. Use a single primary without automatic
failover, or a synchronously replicated service, when progress durability
matters. Durable worker polling, expired-run recovery and independent-JVM
takeover verification are separate milestones (T11/T13) that build on this
contract; the neutral interface carries only opaque payloads and state strings,
leaving checkpoint serialization and run state machines to the caller.

## Opt-in single-primary advisory index repair (Refs #8)

`RedisProgressIndexRepairer` is a small discovery/repair component, constructed
explicitly with a single-server Redisson client, the existing progress prefix,
an application state adapter, and a `RedisProgressAdvisoryIndex`. It does not
change the store API, keys, record format, lease scripts or default behavior.
It is independent of Core/Starter wiring and supplies no polling Worker.

```java
var index = new RedisProgressAdvisoryIndex(redisson, "my-app:progress-hints:v1");
var repairer = new RedisProgressIndexRepairer(redisson,
        RedisLeasedProgressStore.DEFAULT_KEY_PREFIX,
        metadata -> "APP_READY".equals(metadata.state())
                ? Optional.of(new RedisProgressAdvisoryIndex.Hint(metadata, 0))
                : Optional.empty(),
        index);
var cursor = repairer.start();
// Run one batch when the application's scheduler/capacity budget allows it.
var batch = repairer.repair(cursor, 25, 100);
cursor = batch.cursor(); // retain this entire continuation, including pending keys
var candidates = index.due(25);
```

`APP_READY` is an example application policy, not a built-in runnable state.
The adapter must explicitly interpret the application's opaque state and return
an absolute due time in milliseconds (zero means immediately due), or empty.
It must preserve the projected metadata and produce a deterministic decision/time
for a given revision. There is no generic event adapter: outbox payloads remain
opaque and are not read. Applications requiring event-only eligibility need a
separate bounded, versioned event projection; they must not infer runnable from
arbitrary state strings or the mere existence of an outbox. Due queries compare
scores with Redis `TIME`; retries, execution and heartbeat are not supplied.

Discovery scans the authoritative progress keyspace, derives each session hash
key from the run key, and atomically projects the stored session/run IDs, schema
versions, revision and state. It verifies that the recovered IDs hash back to
the discovered key. It never relies on a prior global index and never reads
checkpoint/journal/outbox payloads or calls `snapshot`. Before `HGET`, `HSTRLEN`
checks enforce 1024 UTF-8 bytes per ID, 4096 bytes for state, one byte per schema
version and 19 bytes for revision. Oversized, wrong-type or corrupt metadata
stops the call with an error, leaving authority unchanged; applications need an
alert/reconciliation policy for such records. These are limits of this opt-in
reader, not new restrictions on the existing store. The projection also rejects
state fields at revision zero and missing/empty state at a committed revision,
matching the existing run-record format without interpreting application state.
It validates only the projected fields, not checkpoint/journal/outbox integrity.
No records are silently truncated or declared repaired.

Each call processes at most `maxKeys` returned keys (1–1000), performs at most
one `SCAN`, and reads at most one small metadata projection per matching run.
`scanCount` (1–10000) is only Redis's work hint. **SCAN reply bytes, server CPU
and pending-page memory have no hard bound**: even COUNT=1 may return a large
page. The continuation retains that whole page with an offset; no same-page
keys are dropped and no new SCAN is issued until it is drained. Restart from
`start()` after a failed/unknown call; idempotent hints tolerate replay. Cursors
are in-memory and must not be moved to another database/keyspace. A completed
sweep covers retained keys in a quiescent keyspace; concurrent changes require
repeated sweeps. A hard discovery budget needs a future authoritative bounded
directory/protocol or server support and is not solved by this component.

The reference sink is a separate durable ZSET, with a bounded encoded
`(sessionId, runId, revision)` member and absolute due-time score. `ZADD NX` makes
repeated/concurrent publication idempotent. Different revisions coexist, so a late
old publication cannot overwrite a newer revision. The two-argument constructor
preserves unbounded cardinality; the capacity overload and `poll`/`ack`/`retry`
protocol below provide bounded admission and visibility delivery. `due(limit)`
remains a diagnostic query and repeatedly returns unacknowledged candidates.
Only this schema may write the dedicated index and its auxiliary keys.

A candidate is always advisory. The reference consumer below claims a session
lease and atomically revalidates authoritative metadata, reservation, owner/fence
and Redis TIME before invoking the application's executor. Neither discovery nor
the reference consumer is wired into Core/Starter or a production polling Worker.
No exactly-once or HA RPO0 guarantee is made. Only single-server Redisson client
configurations are accepted for discovery; the caller must supply a single
primary without automatic failover. The component cannot detect server topology
or failover policy. Cluster discovery and independent-JVM takeover are unverified.

`RedisProgressIndexRepairerTest` uses real `valkey/valkey:8.1.2` with AOF and
`appendfsync always`. The failure-window fixture commits run/outbox, deliberately
never calls a publisher, shuts down the producing connection, and creates a new
repairer/connection. It is a skipped-publisher/connection-boundary simulation,
not an OS process kill, Redis crash/restart or two-JVM takeover test. The suite
also covers concurrent/repeated repair, oversized SCAN-page continuation,
explicit unknown/terminal-state policy, stale revisions, projection limits,
large historical payload isolation, server-time due queries, and Redis
wrong-type publication errors that preserve authoritative data. Testcontainers
skips without Docker must be reported as skips, not backend success.

## Opt-in bounded membership directory and reference consumption (Refs #8)

The SCAN repairer above remains available with its original soft bounds. For a
**new or explicitly migrated prefix**, all producers can instead opt into a
retained membership directory:

```java
var store = RedisLeasedProgressStore.builder()
        .redisson(redisson)
        .keyPrefix("my-app:progress:v2")
        .directoryCapacity(1000)
        .build();
var directory = new RedisProgressDirectory(redisson, "my-app:progress:v2", 1000);
var hints = new RedisProgressAdvisoryIndex(redisson, "my-app:hints:v2", 1000);
var projection = new RedisProgressIndexRepairer(redisson, "my-app:progress:v2",
        m -> Optional.empty(), hints);
var batch = directory.repair("", 25, projection,
        m -> "APP_READY".equals(m.state())
                ? Optional.of(new RedisProgressAdvisoryIndex.Hint(m, 0))
                : Optional.empty(), hints);
// Persist/report batch.issues(), then continue from batch.page().after().
// Once page.done() is true, a later sweep starts again from "".
```

The directory is **authoritative for retained membership**, not for runnable
state, progress or lease ownership. It adds two dedicated keys, outside the
existing session hash tags: `<prefix>:directory:v1:config` (capacity) and
`<prefix>:directory:v1:members` (ZSET, all scores zero). `claim` validates directory
type/configuration/capacity before any authority write, runs the original claim
script, then registers membership in that **same Lua invocation** on success.
A process/connection loss after a successful claim cannot leave an accepted new
run unregistered. Commit updates the already registered run; there is no second
post-commit registration call. A full directory rejects a new identity before
creating session/run authority. Existing registered identities can still reclaim
leases. Busy/corrupt claims create no member. Redis errors/timeouts propagate;
timeout outcomes remain unknown and need re-reading.

Capacity is immutable per prefix, 1..10000 retained session/run identities,
including terminal and revision-zero records. No directory GC/deletion is
provided: it never drops authority to admit more work. Exhaustion stops new
admission until the application explicitly provisions a new prefix or a future
retention protocol is selected. IDs are nonblank and at most 1024 UTF-8 bytes;
new commits through directory mode also require state <=4096 UTF-8 bytes. Default
store builders retain their old acceptance and behavior. Existing progress keys,
opaque state semantics, Redis TIME leases and exact int64 fences are unchanged.
The cross-session directory script requires a single primary and is deliberately
incompatible with Cluster/Sentinel/replicated client configurations.

`page(after, limit)` uses exclusive lexicographic `ZRANGEBYLEX ... LIMIT 0 limit`,
with limit 1..1000. It returns at most that many members and a single bounded
cursor, with no overflow buffer. Immutable retained membership makes concurrent
sweeps safe; inserts behind an active cursor are visited on the next sweep. A
quiescent sweep covers all registered identities. There is no global snapshot
of concurrent submissions.

Resource claims are specific to directory discovery and hint operations:

- Directory members contain two Base64 URL IDs, at most 2733 ASCII bytes. A RESP2
  page response is at most `16 + 2742 * limit` bytes, excluding transport framing.
  Retained member payload is at most `capacity * 2733` bytes. This is a payload
  bound, not a bound on Redis allocator/index/client-object overhead.
- Directory lookup/insert is a fixed number of commands over at most capacity
  members. Sorted-set range work is conventionally O(log capacity + limit);
  compact encodings may scan up to capacity entries. It never scans the unrelated
  database. A repair call performs one page query, at most limit projections and
  at most limit publications. Application adapters must themselves be bounded.
- Each projection reads only the six length-checked fields described above;
  its RESP2 metadata reply is below 6250 bytes. The leased variant additionally
  length-checks owner/fence/expiry/reservation and validates them atomically with
  metadata. Checkpoint, journal and outbox payloads are not projected.
- Hint members are at most 2753 ASCII bytes. A due reply is at most
  `16 + 2762 * limit` RESP2 bytes. A poll reply interleaves members and exact int64
  delivery tokens, at most `16 + 2788 * limit` RESP2 bytes. At most capacity hint
  members and their delivery-token fields are retained when the capacity overload
  is used, plus fixed configuration and a retained sequence counter.

These bounds require dedicated schema-owned keys, no eviction/TTL/deletion of
protocol keys and no bypass writers. They do **not** certify host CPU milliseconds,
RSS, Redis OOM/script failures or overall store claim/commit/snapshot cost. Existing
store scripts still read checkpoint/progress history; that cost is not made
bounded by enabling the directory. Lua isolates other clients but does not roll
back an unexpected runtime/OOM error after a write. Provision memory and preserve
the original single-primary durability assumptions; no performance or HA
certification is claimed.

For migration, quiesce legacy producers, obtain and retain an operator-controlled
inventory of known `(sessionId, runId)` pairs, then call
`directory.registerExisting(sessionId, runId)` for each pair before enabling
polling. It validates the bounded stored identity and registers it atomically;
it is duplicate-safe and capacity checked. Audit the inventory against the source
of submissions, resolve any rejected identity, and require **every** future
producer to use directory mode. This cannot prove completeness for an unknown
legacy keyspace. Obtaining an inventory via SCAN is a separate, potentially
unbounded offline migration; no automatic full-keyspace bounded compatibility is
claimed. Oversized IDs require a migration decision rather than silent truncation.

For registered records, bad/oversized/wrong-type metadata and missing authority
become bounded `RepairBatch.issues()` entries while the page cursor advances.
Persist or alert on these issues before persisting the new cursor; no record is
removed or declared repaired. Retry on later sweeps after operator correction.
A transport/publication exception aborts the call; retry the **input** cursor,
since partial successful publications are idempotent. A full hint sink likewise
requires consumption before retrying repair. Dedicated directory/index key
corruption by unsupported writers is a protocol failure, not silently skipped.

`poll(limit, visibilityMillis)` atomically moves delivered hints to Redis
`TIME + visibilityMillis` and returns `(candidate, deliveryToken)`. Polling
workers do not each get the same first page. `ack(delivery)` removes exactly that
revision/delivery and its token; an old acknowledgement cannot delete a newer
revision or a redelivery. `retry(delivery, backoffMillis)` compares the same token,
postpones against Redis TIME and invalidates that token; repeated calls are no-ops.
Delays must be 1..86400000 ms. A lost response/crashed consumer redelivers after
visibility. Scores now encode visibility/retry, while initial publication remains
NX. Delivery sequence overflow fails before mutations. Terminal/stale hints are
pruned by ack; busy/error hints are deferred so existing due work can progress.
This is bounded-page progress for finite retained work, not a scheduler QoS
promise under unlimited competing publication. Repair may republish a pruned
old revision; authority validation makes it harmless.

`RedisProgressReferenceConsumer` takes an explicit application runnable predicate
and an executor. Missing authority is not created from hints. For a present run
it claims the session, **then** atomically validates metadata/reservation/live
owner/fence/Redis TIME, rejects stale revisions and unknown/ineligible states,
and validates again immediately before dispatch. `Execution.guard()` renews the
lease and repeats that bounded metadata/lease validation; an executor should use
it before each effect. A successful executor returns a revision-matching
`ProgressCommit`; the store fences the commit, then the consumer acknowledges the
hint. The owner UUID must be unique per attempt. Exceptions schedule retry and
release the lease; an unknown commit result is reconciled by authority on
redelivery. Busy/lease-lost/metadata-error results defer the hint. Metadata errors
are returned for operator attention, not converted into runnable states.

An external effect is outside the Lua transaction. A lease can expire after the
last guard, or an effect can succeed before a crash/timeout prevents its commit.
External fencing/idempotency/receipts and unsafe-recovery reconciliation remain
application responsibilities. The fake-executor test deliberately observes the
same effect twice across that boundary; this is not exactly-once execution or
timeout rollback. No automatic heartbeat thread, Core/Starter integration,
production Worker, event-only eligibility or independent-JVM takeover is supplied.

`RedisProgressDirectoryTest` exercises real Valkey, concurrent capacity admission
and cursors, exact UTF-8/RESP page boundaries, explicit backfill, corrupt/large
records with unrelated progress, revision/delivery races, Redis-time visibility,
capacity exhaustion, competing consumers, expired/superseded leases and fake
execution failure windows. These tests run in one JVM. Graceful producer-client
shutdown and direct corruption/expiry fixtures do not imply OS kill, backend
restart, network-partition or independent-process takeover coverage.

## Boundaries

This module does not change legacy in-memory Redis-like Store classes in Core,
does not add Redis dependencies elsewhere, and does not provide Worker failover
or graph checkpoint recovery guarantees. Except for the explicit
`RedisLeasedCheckpointSaver` and `RedisRunStore` opt-in modes described above, it
also does not provide message-queue delivery, leases, fencing, exactly-once
side-effect execution, automatic failover, TTL management, or legacy checkpoint
migration.
Durable scheduling, side-effect coordination, and tombstone reclamation remain
separate milestones.
