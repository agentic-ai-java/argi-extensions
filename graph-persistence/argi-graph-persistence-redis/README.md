# ARGI Redis Graph Persistence

This module provides the maintained Redis checkpoint saver and the Redis-backed
long-term Store implementation for ARGI extensions.

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

## Boundaries

This module does not change legacy in-memory Redis-like Store classes in Core,
does not add Redis dependencies elsewhere, and does not provide Worker failover
or graph checkpoint recovery guarantees. It also does not provide message-queue
delivery, leases, fencing, exactly-once side-effect execution, automatic
failover, TTL management, or legacy checkpoint migration. Durable scheduling,
side-effect coordination, and tombstone reclamation remain separate milestones.
