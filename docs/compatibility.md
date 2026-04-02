# VortexDB Command Compatibility Matrix

This document lists every Redis command and its implementation status in VortexDB v0.1-alpha.

**Legend:**
- ✅ **Implemented** — Fully functional, tested, benchmarked
- 🔌 **Stub** — Recognized by the parser, returns appropriate error or no-op
- 🔄 **Planned** — Scheduled for a future phase
- ❌ **Won't Implement** — Out of scope or superseded by VortexDB-specific design

---

## String Commands (20 of 22)

| Command | Status | Phase | Notes |
|---------|--------|-------|-------|
| `SET` | ✅ | 3 | Full Redis 7 syntax: EX/PX/EXAT/PXAT/NX/XX/GET/KEEPTTL |
| `GET` | ✅ | 3 | Lazy expiry on read |
| `SETNX` | ✅ | 3 | |
| `SETEX` | ✅ | 3 | |
| `PSETEX` | ✅ | 3 | |
| `MSET` | ✅ | 3 | Software prefetch pipeline for batch insert |
| `MSETNX` | ✅ | 3 | Two-pass atomicity: check all → insert all |
| `MGET` | ✅ | 3 | Software prefetch pipeline for batch lookup |
| `GETSET` | ✅ | 3 | |
| `GETDEL` | ✅ | 3 | |
| `GETEX` | ✅ | 3 | PERSIST/EX/PX/EXAT/PXAT options |
| `GETRANGE` | ✅ | 3 | Supports negative indices |
| `SETRANGE` | ✅ | 3 | Zero-padding for out-of-range offsets |
| `INCR` | ✅ | 3 | In-place i64 mutation, overflow detection |
| `DECR` | ✅ | 3 | |
| `INCRBY` | ✅ | 3 | |
| `DECRBY` | ✅ | 3 | |
| `INCRBYFLOAT` | ✅ | 3 | ryu f64→string serialization |
| `APPEND` | ✅ | 3 | Inline extension stays inline ≤23 bytes |
| `STRLEN` | ✅ | 3 | |
| `SUBSTR` | 🔄 | 4+ | Alias for GETRANGE (deprecated in Redis) |
| `LCS` | 🔄 | 4+ | Longest Common Substring |

---

## Key Management Commands (20 of 21)

| Command | Status | Phase | Notes |
|---------|--------|-------|-------|
| `DEL` | ✅ | 3 | Multi-key support |
| `UNLINK` | ✅ | 3 | Synchronous in v0.1 (async deallocation planned) |
| `EXISTS` | ✅ | 3 | Multi-key count support |
| `EXPIRE` | ✅ | 3 | NX/XX/GT/LT flags |
| `PEXPIRE` | ✅ | 3 | NX/XX/GT/LT flags |
| `EXPIREAT` | ✅ | 3 | NX/XX/GT/LT flags |
| `PEXPIREAT` | ✅ | 3 | NX/XX/GT/LT flags |
| `PERSIST` | ✅ | 3 | Removes TTL from key |
| `TTL` | ✅ | 3 | Returns -2 for missing, -1 for no expiry |
| `PTTL` | ✅ | 3 | Millisecond precision |
| `EXPIRETIME` | ✅ | 3 | Absolute Unix timestamp |
| `PEXPIRETIME` | ✅ | 3 | Absolute Unix ms timestamp |
| `TYPE` | ✅ | 3 | Returns Redis-compatible type strings |
| `RENAME` | ✅ | 3 | O(1) via remove + insert, preserves TTL |
| `RENAMENX` | ✅ | 3 | |
| `KEYS` | ✅ | 3 | Glob pattern matching: `*`, `?`, `[abc]`, `[a-z]`, `[^abc]`, `\x` |
| `SCAN` | ✅ | 3 | Reverse-bit cursor (Redis-compatible resize safety) |
| `RANDOMKEY` | ✅ | 3 | xorshift64 random slot selection |
| `TOUCH` | ✅ | 3 | Multi-key, returns count of existing keys |
| `COPY` | ✅ | 3 | Via get + clone + insert, preserves TTL |
| `SORT` | 🔄 | 4+ | Requires List/Set/Sorted Set data structures |

---

## Server & Connection Commands (15 of 30)

| Command | Status | Phase | Notes |
|---------|--------|-------|-------|
| `PING` | ✅ | 1 | No args → static `+PONG\r\n`; with arg → bulk string echo |
| `ECHO` | ✅ | 1 | |
| `QUIT` | ✅ | 1 | Closes connection after `+OK` flush |
| `DBSIZE` | ✅ | 3 | O(1) — reads table length field |
| `FLUSHDB` | ✅ | 3 | Accepts ASYNC/SYNC option (synchronous in v0.1) |
| `FLUSHALL` | ✅ | 3 | Same as FLUSHDB in single-DB mode |
| `INFO` | ✅ | 3 | 4 sections: server, clients, memory, keyspace |
| `COMMAND` | ✅ | 3 | COUNT, INFO, DOCS, LIST, GETKEYS subcommands |
| `SELECT` | ✅ | 3 | Accepts DB 0, rejects DB >0 (single-database design) |
| `TIME` | ✅ | 3 | Returns [seconds, microseconds] array |
| `MULTI` | 🔌 | 5+ | Returns `-ERR not yet implemented` |
| `EXEC` | 🔌 | 5+ | Returns `-ERR not yet implemented` |
| `DISCARD` | 🔌 | 5+ | Returns `-ERR not yet implemented` |
| `WATCH` | 🔌 | 5+ | Returns `-ERR not yet implemented` |
| `UNWATCH` | ✅ | 3 | No-op `+OK` (safe to call unconditionally) |
| `AUTH` | 🔄 | 6 | ACL phase |
| `HELLO` | 🔄 | 4+ | RESP3 negotiation |
| `RESET` | 🔄 | 4+ | |
| `CLIENT` | 🔄 | 4+ | CLIENT ID, LIST, GETNAME, SETNAME, etc. |
| `CONFIG` | 🔄 | 4+ | CONFIG GET/SET/REWRITE/RESETSTAT |
| `DEBUG` | ❌ | — | Security risk, not implementing |
| `SAVE` | 🔄 | 5 | Persistence phase |
| `BGSAVE` | 🔄 | 5 | |
| `BGREWRITEAOF` | 🔄 | 5 | |
| `LASTSAVE` | 🔄 | 5 | |
| `SLOWLOG` | 🔄 | 4 | Metrics phase |
| `WAIT` | 🔄 | 6 | Replication phase |
| `SHUTDOWN` | 🔄 | 4+ | Graceful shutdown via command |
| `SWAPDB` | ❌ | — | Single-database design |
| `OBJECT` | 🔄 | 4+ | OBJECT ENCODING, REFCOUNT, IDLETIME, HELP |

---

## List Commands (0 of 20)

| Command | Status | Phase | Notes |
|---------|--------|-------|-------|
| `LPUSH` | 🔄 | 4 | Phase 4: Data Structures |
| `RPUSH` | 🔄 | 4 | |
| `LPUSHX` | 🔄 | 4 | |
| `RPUSHX` | 🔄 | 4 | |
| `LPOP` | 🔄 | 4 | |
| `RPOP` | 🔄 | 4 | |
| `LLEN` | 🔄 | 4 | |
| `LINDEX` | 🔄 | 4 | |
| `LINSERT` | 🔄 | 4 | |
| `LSET` | 🔄 | 4 | |
| `LRANGE` | 🔄 | 4 | |
| `LTRIM` | 🔄 | 4 | |
| `LREM` | 🔄 | 4 | |
| `LPOS` | 🔄 | 4 | |
| `LMOVE` | 🔄 | 4 | |
| `LMPOP` | 🔄 | 4 | |
| `BLPOP` | 🔄 | 4 | Blocking variant |
| `BRPOP` | 🔄 | 4 | Blocking variant |
| `BLMOVE` | 🔄 | 4 | Blocking variant |
| `BLMPOP` | 🔄 | 4 | Blocking variant |

---

## Hash Commands (0 of 15)

| Command | Status | Phase | Notes |
|---------|--------|-------|-------|
| `HSET` | 🔄 | 4 | Phase 4: Data Structures |
| `HGET` | 🔄 | 4 | |
| `HSETNX` | 🔄 | 4 | |
| `HMSET` | 🔄 | 4 | |
| `HMGET` | 🔄 | 4 | |
| `HDEL` | 🔄 | 4 | |
| `HEXISTS` | 🔄 | 4 | |
| `HLEN` | 🔄 | 4 | |
| `HKEYS` | 🔄 | 4 | |
| `HVALS` | 🔄 | 4 | |
| `HGETALL` | 🔄 | 4 | |
| `HINCRBY` | 🔄 | 4 | |
| `HINCRBYFLOAT` | 🔄 | 4 | |
| `HRANDFIELD` | 🔄 | 4 | |
| `HSCAN` | 🔄 | 4 | |

---

## Set Commands (0 of 17)

| Command | Status | Phase | Notes |
|---------|--------|-------|-------|
| `SADD` | 🔄 | 4 | Phase 4: Data Structures |
| `SREM` | 🔄 | 4 | |
| `SISMEMBER` | 🔄 | 4 | |
| `SMISMEMBER` | 🔄 | 4 | |
| `SMEMBERS` | 🔄 | 4 | |
| `SCARD` | 🔄 | 4 | |
| `SPOP` | 🔄 | 4 | |
| `SRANDMEMBER` | 🔄 | 4 | |
| `SINTER` | 🔄 | 4 | |
| `SINTERCARD` | 🔄 | 4 | |
| `SINTERSTORE` | 🔄 | 4 | |
| `SUNION` | 🔄 | 4 | |
| `SUNIONSTORE` | 🔄 | 4 | |
| `SDIFF` | 🔄 | 4 | |
| `SDIFFSTORE` | 🔄 | 4 | |
| `SMOVE` | 🔄 | 4 | |
| `SSCAN` | 🔄 | 4 | |

---

## Sorted Set Commands (0 of 32)

| Command | Status | Phase | Notes |
|---------|--------|-------|-------|
| `ZADD` | 🔄 | 4 | Phase 4: Data Structures |
| `ZREM` | 🔄 | 4 | |
| `ZSCORE` | 🔄 | 4 | |
| `ZMSCORE` | 🔄 | 4 | |
| `ZINCRBY` | 🔄 | 4 | |
| `ZCARD` | 🔄 | 4 | |
| `ZCOUNT` | 🔄 | 4 | |
| `ZLEXCOUNT` | 🔄 | 4 | |
| `ZRANGE` | 🔄 | 4 | |
| `ZRANGESTORE` | 🔄 | 4 | |
| `ZRANGEBYLEX` | 🔄 | 4 | |
| `ZRANGEBYSCORE` | 🔄 | 4 | |
| `ZREVRANGE` | 🔄 | 4 | |
| `ZREVRANGEBYLEX` | 🔄 | 4 | |
| `ZREVRANGEBYSCORE` | 🔄 | 4 | |
| `ZREVRANK` | 🔄 | 4 | |
| `ZRANK` | 🔄 | 4 | |
| `ZRANDMEMBER` | 🔄 | 4 | |
| `ZPOPMIN` | 🔄 | 4 | |
| `ZPOPMAX` | 🔄 | 4 | |
| `BZPOPMIN` | 🔄 | 4 | Blocking variant |
| `BZPOPMAX` | 🔄 | 4 | Blocking variant |
| `ZINTERSTORE` | 🔄 | 4 | |
| `ZUNIONSTORE` | 🔄 | 4 | |
| `ZDIFFSTORE` | 🔄 | 4 | |
| `ZINTER` | 🔄 | 4 | |
| `ZUNION` | 🔄 | 4 | |
| `ZDIFF` | 🔄 | 4 | |
| `ZINTERCARD` | 🔄 | 4 | |
| `ZSCAN` | 🔄 | 4 | |
| `ZMPOP` | 🔄 | 4 | |
| `BZMPOP` | 🔄 | 4 | Blocking variant |

---

## Pub/Sub Commands (0 of 6)

| Command | Status | Phase | Notes |
|---------|--------|-------|-------|
| `SUBSCRIBE` | 🔄 | 4 | Phase 4: Pub/Sub |
| `UNSUBSCRIBE` | 🔄 | 4 | |
| `PSUBSCRIBE` | 🔄 | 4 | Pattern subscribe |
| `PUNSUBSCRIBE` | 🔄 | 4 | |
| `PUBLISH` | 🔄 | 4 | |
| `PUBSUB` | 🔄 | 4 | CHANNELS, NUMSUB, NUMPAT |

---

## Scripting Commands (0 of 5)

| Command | Status | Phase | Notes |
|---------|--------|-------|-------|
| `EVAL` | 🔄 | 8 | Phase 8: Lua Scripting |
| `EVALSHA` | 🔄 | 8 | |
| `EVALRO` | 🔄 | 8 | Read-only variant |
| `EVALSHA_RO` | 🔄 | 8 | |
| `SCRIPT` | 🔄 | 8 | LOAD, EXISTS, FLUSH, DEBUG |

---

## HyperLogLog Commands (0 of 3)

| Command | Status | Phase | Notes |
|---------|--------|-------|-------|
| `PFADD` | 🔄 | 4+ | Probabilistic cardinality estimation |
| `PFCOUNT` | 🔄 | 4+ | |
| `PFMERGE` | 🔄 | 4+ | |

---

## Bitmap Commands (0 of 7)

| Command | Status | Phase | Notes |
|---------|--------|-------|-------|
| `SETBIT` | 🔄 | 4+ | |
| `GETBIT` | 🔄 | 4+ | |
| `BITCOUNT` | 🔄 | 4+ | |
| `BITOP` | 🔄 | 4+ | AND, OR, XOR, NOT |
| `BITPOS` | 🔄 | 4+ | |
| `BITFIELD` | 🔄 | 4+ | |
| `BITFIELD_RO` | 🔄 | 4+ | Read-only variant |

---

## Geo Commands (0 of 10)

| Command | Status | Phase | Notes |
|---------|--------|-------|-------|
| `GEOADD` | 🔄 | 4+ | Requires Sorted Set |
| `GEODIST` | 🔄 | 4+ | |
| `GEOHASH` | 🔄 | 4+ | |
| `GEOPOS` | 🔄 | 4+ | |
| `GEORADIUS` | 🔄 | 4+ | Deprecated in Redis 6.2+ |
| `GEORADIUS_RO` | 🔄 | 4+ | |
| `GEORADIUSBYMEMBER` | 🔄 | 4+ | Deprecated in Redis 6.2+ |
| `GEORADIUSBYMEMBER_RO` | 🔄 | 4+ | |
| `GEOSEARCH` | 🔄 | 4+ | Replacement for GEORADIUS |
| `GEOSEARCHSTORE` | 🔄 | 4+ | |

---

## Stream Commands (0 of 15)

| Command | Status | Phase | Notes |
|---------|--------|-------|-------|
| `XADD` | 🔄 | 4+ | Phase 4+: Streams |
| `XLEN` | 🔄 | 4+ | |
| `XRANGE` | 🔄 | 4+ | |
| `XREVRANGE` | 🔄 | 4+ | |
| `XREAD` | 🔄 | 4+ | Blocking variant available |
| `XREADGROUP` | 🔄 | 4+ | |
| `XACK` | 🔄 | 4+ | |
| `XCLAIM` | 🔄 | 4+ | |
| `XAUTOCLAIM` | 🔄 | 4+ | |
| `XDEL` | 🔄 | 4+ | |
| `XTRIM` | 🔄 | 4+ | |
| `XINFO` | 🔄 | 4+ | |
| `XGROUP` | 🔄 | 4+ | |
| `XPENDING` | 🔄 | 4+ | |
| `XSETID` | 🔄 | 4+ | |

---

## Cluster Commands (0 of 4)

| Command | Status | Phase | Notes |
|---------|--------|-------|-------|
| `CLUSTER` | 🔄 | 7 | Phase 7: Clustering |
| `ASKING` | 🔄 | 7 | |
| `READONLY` | 🔄 | 7 | |
| `READWRITE` | 🔄 | 7 | |

---

## Persistence Commands (0 of 4)

| Command | Status | Phase | Notes |
|---------|--------|-------|-------|
| `DUMP` | 🔄 | 5 | Phase 5: Persistence |
| `RESTORE` | 🔄 | 5 | |
| `SAVE` | 🔄 | 5 | |
| `BGSAVE` | 🔄 | 5 | |

---

## Summary

| Category | Implemented | Stub | Planned | Won't Impl | Total |
|----------|-------------|------|---------|-------------|-------|
| String | 20 | 0 | 2 | 0 | 22 |
| Key | 20 | 0 | 1 | 0 | 21 |
| Server/Connection | 11 | 4 | 13 | 2 | 30 |
| List | 0 | 0 | 20 | 0 | 20 |
| Hash | 0 | 0 | 15 | 0 | 15 |
| Set | 0 | 0 | 17 | 0 | 17 |
| Sorted Set | 0 | 0 | 32 | 0 | 32 |
| Pub/Sub | 0 | 0 | 6 | 0 | 6 |
| Scripting | 0 | 0 | 5 | 0 | 5 |
| HyperLogLog | 0 | 0 | 3 | 0 | 3 |
| Bitmap | 0 | 0 | 7 | 0 | 7 |
| Geo | 0 | 0 | 10 | 0 | 10 |
| Stream | 0 | 0 | 15 | 0 | 15 |
| Cluster | 0 | 0 | 4 | 0 | 4 |
| Persistence | 0 | 0 | 4 | 0 | 4 |
| **Total** | **51** | **4** | **154** | **2** | **211** |

**v0.1-alpha coverage:** 55 commands operational (51 fully implemented + 4 stubs), covering all String, Key, and core Server commands. This represents the complete Redis String + Key workload — sufficient for key-value use cases, session stores, counters, and caches.

---

## VortexDB-Specific Behaviors

| Behavior | VortexDB | Redis | Notes |
|----------|----------|-------|-------|
| Database count | 1 (DB 0 only) | 16 | `SELECT` accepts 0, rejects >0 |
| Threading model | Thread-per-core, shard-per-reactor | Single-threaded + io-threads for I/O | No `WAIT` needed for replication in v0.1 |
| UNLINK | Synchronous in v0.1 | Async deallocation | Async planned for future phases |
| FLUSHDB / FLUSHALL | Identical (single DB) | Flush specific DB / all DBs | |
| MULTI/EXEC | Stub — returns error | Full transaction support | Planned for Phase 5+ |
| Key distribution | Per-reactor shard (connection affinity) | Single keyspace | All keys accessible from any connection |
