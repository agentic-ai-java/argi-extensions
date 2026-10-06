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
contract. It requires a caller-owned `RedissonClient`:

```java
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

Deletes and `clear()` retain persistent tombstones. A deleted key keeps its
positive version so stale writers cannot recreate it with expected version zero.
Unconditional writes, deletes, and clear operations use bounded read/CAS retries.

Search, namespace listing, size, and clear scan the hash and ignore tombstones.
These scans are not cross-item snapshot transactions; `clear()` requires
quiescent writers for a complete purge. Tombstone reclamation needs a future
retention design.

## Boundaries

This module does not change legacy in-memory Redis-like Store classes in Core,
does not add Redis dependencies elsewhere, and does not provide Worker failover
or graph checkpoint recovery guarantees. Checkpoint-executor CAS, leases,
fencing, durable scheduling, and automatic recovery remain separate milestones.
