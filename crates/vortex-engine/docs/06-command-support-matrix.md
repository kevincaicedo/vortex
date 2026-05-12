# Vortex Engine Command Support Matrix

Date: 2026-05-12

This document inventories Redis 8.6 command coverage for Vortex. The Redis command source is `research/redis-8-6-commands.md`. The Vortex support source is the current engine and reactor command surface:

- `vortex/crates/vortex-engine/src/commands/*`
- `vortex/crates/vortex-engine/src/engine/domain/*`
- `vortex/crates/vortex-io/src/reactor/dispatch.rs`
- `vortex/crates/vortex-io/src/reactor/aof.rs`
- `vortex/crates/vortex-io/src/reactor/transaction.rs`

The table includes every Redis 8.6 command in a category when Vortex supports at least one command from that category. Categories with no supported commands are listed separately as not supported.

## Status Legend

| Status | Meaning |
| --- | --- |
| Supported | Vortex has an active handler for the command's current alpha semantics. |
| Partial | Vortex recognizes the command or implements a useful subset, but Redis behavior is incomplete, stubbed, synchronous where Redis is asynchronous, single-DB only, or reactor-owned with caveats. |
| Pending | Redis command exists in a category where Vortex supports other commands, but this command has no Vortex implementation yet. |
| Not supported | No command in this Redis category is currently supported. |

Complexity values are Redis reference complexity unless the notes call out a Vortex-specific cost.

## Current Vortex Command Surface

```text
String:      APPEND, DECR, DECRBY, GET, GETDEL, GETEX, GETRANGE, GETSET,
             INCR, INCRBY, INCRBYFLOAT, MGET, MSET, MSETNX, PSETEX,
             SET, SETEX, SETNX, SETRANGE, STRLEN
Generic:     COPY, DEL, EXISTS, EXPIRE, EXPIREAT, EXPIRETIME, KEYS,
             PERSIST, PEXPIRE, PEXPIREAT, PEXPIRETIME, PTTL, RANDOMKEY,
             RENAME, RENAMENX, SCAN, TOUCH, TTL, TYPE, UNLINK
Connection:  ECHO, PING, QUIT, SELECT
Transaction: DISCARD, EXEC, MULTI, UNWATCH, WATCH
Server:      BGREWRITEAOF, COMMAND, COMMAND COUNT, COMMAND INFO,
             CONFIG GET, CONFIG SET, DBSIZE, FLUSHALL, FLUSHDB, INFO, TIME
```

Transaction commands and some server commands are reactor-owned because they depend on per-connection state, file descriptors, AOF runtime state, or queued command payloads. They are included here because they are part of the Vortex command surface and depend on `vortex-engine` keyspace APIs.

## Unsupported Categories

| Category | Status | Notes |
| --- | --- | --- |
| Hash commands | Not supported | No `H*` command handlers or hash value type command surface are implemented yet. |
| List commands | Not supported | No `L*`, `BL*`, or list-blocking command handlers are implemented yet. |
| Set commands | Not supported | No `S*` set command handlers are implemented yet. |
| Sorted set commands | Not supported | No `Z*`, blocking sorted-set, or score/range command handlers are implemented yet. |
| Stream commands | Not supported | No stream data type, consumer group, or `X*` command handlers are implemented yet. |
| Bitmap commands | Not supported | No `GETBIT`, `SETBIT`, `BITFIELD`, `BITOP`, or related bitmap command handlers are implemented yet. |
| HyperLogLog commands | Not supported | No `PF*` command handlers are implemented yet. |
| Geospatial commands | Not supported | No `GEO*` command handlers are implemented yet. |
| JSON commands | Not supported | Redis Stack JSON commands are not part of the current engine surface. |
| Search commands | Not supported | Redis Stack search commands are not part of the current engine surface. |
| Time series commands | Not supported | Redis Stack time series commands are not part of the current engine surface. |
| Vector set commands | Not supported | Redis 8 vector-set commands are not part of the current engine surface. |
| Pub/Sub commands | Not supported | No Pub/Sub state, channel registry, pattern subscription, or push-message support is implemented yet. |
| Scripting commands | Not supported | No Lua, functions, script cache, or `EVAL` command surface is implemented yet. |
| Cluster commands | Not supported | Vortex alpha uses one shared keyspace and has no Redis Cluster command surface. |

## String Commands

| Name | Description | Status | Time complexity | Syntax | Notes |
| --- | --- | --- | --- | --- | --- |
| APPEND | Append bytes to a string value, creating the key when missing. | Supported | O(1) amortized in Redis; Vortex may copy/grow value bytes. | `APPEND key value` | Goes through mutation admission, TTL preservation, WATCH/AOF effects, and string growth logic. |
| DECR | Decrement an integer value by one, using 0 for a missing key. | Supported | O(1) | `DECR key` | Uses checked integer mutation and returns Redis integer errors on parse or overflow failure. |
| DECRBY | Decrement an integer value by a specified amount. | Supported | O(1) | `DECRBY key decrement` | Same mutation path as `INCRBY` with a negative delta. |
| DELEX | Conditionally delete a key based on value or digest comparison. | Pending | O(1) or O(N) depending on Redis mode. | `DELEX key [IFEQ value or IFNE value or IFDEQ digest or IFDNE digest]` | Redis 8.4 command; no Vortex conditional delete handler yet. |
| DIGEST | Return a digest of a string value. | Pending | O(N) | `DIGEST key` | No string digest command yet. |
| GET | Return the string value of a key. | Supported | O(1) in Redis; response copy is proportional to value bytes. | `GET key` | Treats expired keys as missing and records access metadata when access-aware eviction is active. |
| GETDEL | Return a string value and delete the key. | Supported | O(1) | `GETDEL key` | Deletes through domain mutation so TTL counters, WATCH, LSN, and AOF effects stay ordered. |
| GETEX | Return a string value and optionally change its TTL. | Supported | O(1) | `GETEX key [EX seconds or PX milliseconds or EXAT unix-seconds or PXAT unix-ms or PERSIST]` | Supports Redis TTL options listed here. AOF payload uses absolute expire/persist forms where needed. |
| GETRANGE | Return a byte range from a string value. | Supported | O(N) for returned bytes. | `GETRANGE key start end` | Handles Redis-style offsets over Vortex string/integer value representation. |
| GETSET | Set a new value and return the old value. | Supported | O(1) | `GETSET key value` | Implemented as value replacement with old-value response and normal mutation effects. |
| INCR | Increment an integer value by one, using 0 for a missing key. | Supported | O(1) | `INCR key` | Uses checked integer mutation and Redis integer error behavior. |
| INCRBY | Increment an integer value by a specified amount. | Supported | O(1) | `INCRBY key increment` | Handles missing keys, parse errors, overflow, memory admission, and AOF. |
| INCRBYFLOAT | Increment a floating-point value. | Supported | O(1) in Redis; formatting cost depends on result length. | `INCRBYFLOAT key increment` | Computes the decimal result and writes the resulting string value. |
| LCS | Find the longest common substring between two string values. | Pending | O(N*M) | `LCS key1 key2 [LEN] [IDX] [MINMATCHLEN n] [WITHMATCHLEN]` | No dynamic-programming LCS command yet. |
| MGET | Return values for multiple keys. | Supported | O(N) keys | `MGET key [key ...]` | Groups keys by shard, preserves response order, and records access metadata for live hits. |
| MSET | Set multiple key/value pairs atomically. | Supported | O(N) pairs | `MSET key value [key value ...]` | Deduplicates final logical writes for projection, locks shards in sorted order, and emits one command outcome. |
| MSETEX | Set multiple keys with a shared expiration. | Pending | O(N) keys | `MSETEX key value [key value ...] [expiration options]` | Redis 8 command; no Vortex batch TTL SET command yet. |
| MSETNX | Set multiple keys only when none already exist. | Supported | O(N) keys | `MSETNX key value [key value ...]` | Uses multi-key planning and returns `0` without mutation when any target exists. |
| PSETEX | Set a string value and TTL in milliseconds. | Supported | O(1) | `PSETEX key milliseconds value` | Stores monotonic nanosecond TTL internally and emits absolute AOF TTL when needed. |
| SET | Set a string value. | Supported | O(1) in Redis; Vortex cost includes value allocation and memory admission. | `SET key value [EX seconds or PX milliseconds or EXAT unix-seconds or PXAT unix-ms or KEEPTTL] [NX or XX] [GET]` | Plain `SET key value` has a fast path. Options are parsed in the slow path. |
| SETEX | Set a string value and TTL in seconds. | Supported | O(1) | `SETEX key seconds value` | Wrapper around TTL-aware SET domain path. |
| SETNX | Set a string value only if the key does not exist. | Supported | O(1) | `SETNX key value` | Uses `SET` options with `NX`; returns integer `1` or `0`. |
| SETRANGE | Overwrite bytes starting at an offset. | Supported | O(1) for small updates in Redis; Vortex may allocate up to new string length. | `SETRANGE key offset value` | Uses prepared value mutation where possible and preserves existing TTL. |
| STRLEN | Return string length. | Supported | O(1) | `STRLEN key` | Returns `0` for missing keys and supports integer/string value views. |
| SUBSTR | Alias for `GETRANGE`. | Pending | O(N) for returned bytes. | `SUBSTR key start end` | No alias dispatch is registered yet; use `GETRANGE`. |

## Generic Commands

| Name | Description | Status | Time complexity | Syntax | Notes |
| --- | --- | --- | --- | --- | --- |
| COPY | Copy a key to a destination key. | Partial | O(N) for copied value bytes. | `COPY source destination [DB destination-db] [REPLACE]` | Supports `REPLACE`. `DB` is parsed and ignored because Vortex alpha has one logical database. |
| DEL | Delete one or more keys. | Supported | O(N) keys | `DEL key [key ...]` | Single-key fast path avoids argument collection; batch path locks sorted shards. |
| DUMP | Return serialized key payload. | Pending | O(N) serialized bytes | `DUMP key` | No Redis RDB-style serialization command yet. |
| EXISTS | Count existing keys. | Supported | O(N) keys | `EXISTS key [key ...]` | Expired keys are treated as missing and may be lazily cleaned. Duplicate keys count like Redis. |
| EXPIRE | Set TTL in seconds. | Supported | O(1) | `EXPIRE key seconds [NX or XX or GT or LT]` | Supports `NX`, `XX`, `GT`, and `LT`; updates entry deadline and TTL counters. |
| EXPIREAT | Set TTL by Unix seconds timestamp. | Supported | O(1) | `EXPIREAT key unix-time-seconds [NX or XX or GT or LT]` | Converts wall-clock timestamp to monotonic deadline. |
| EXPIRETIME | Return expiration Unix time in seconds. | Supported | O(1) | `EXPIRETIME key` | Returns Redis `-1` and `-2` sentinel values for no TTL and missing keys. |
| KEYS | Return keys matching a glob pattern. | Supported | O(total slots) | `KEYS pattern` | Scans the whole keyspace and filters expired entries; intended for admin/debug use. |
| MIGRATE | Transfer keys to another Redis instance. | Pending | Varies | `MIGRATE host port key db timeout [COPY] [REPLACE] [AUTH password] [KEYS key ...]` | No networking/key migration command yet. |
| MOVE | Move a key to another logical database. | Pending | O(1) | `MOVE key db` | Vortex alpha has one logical database. |
| OBJECT ENCODING | Return Redis object encoding. | Pending | O(1) | `OBJECT ENCODING key` | No Redis `OBJECT` command family yet. |
| OBJECT FREQ | Return Redis LFU counter. | Pending | O(1) | `OBJECT FREQ key` | Vortex has Morris counters and an LFU sketch, but no public `OBJECT FREQ` command. |
| OBJECT IDLETIME | Return idle time. | Pending | O(1) | `OBJECT IDLETIME key` | Vortex does not maintain exact idle timestamps. |
| OBJECT REFCOUNT | Return object reference count. | Pending | O(1) | `OBJECT REFCOUNT key` | Not meaningful for current Vortex value ownership model. |
| PERSIST | Remove a key's TTL. | Supported | O(1) | `PERSIST key` | Clears entry TTL and decrements expiry counters when needed. |
| PEXPIRE | Set TTL in milliseconds. | Supported | O(1) | `PEXPIRE key milliseconds [NX or XX or GT or LT]` | Same domain path as `EXPIRE` with millisecond conversion. |
| PEXPIREAT | Set TTL by Unix milliseconds timestamp. | Supported | O(1) | `PEXPIREAT key unix-time-milliseconds [NX or XX or GT or LT]` | Converts absolute Unix time to monotonic deadline. |
| PEXPIRETIME | Return expiration Unix time in milliseconds. | Supported | O(1) | `PEXPIRETIME key` | Returns Redis sentinel values for no TTL and missing keys. |
| PTTL | Return remaining TTL in milliseconds. | Supported | O(1) | `PTTL key` | Uses monotonic deadline and current monotonic time. |
| RANDOMKEY | Return a random key. | Supported | O(shards + probe window) in Vortex | `RANDOMKEY` | Samples shards and table slots, skips expired entries, returns nil when empty. |
| RENAME | Rename a key and overwrite destination. | Supported | O(1) average plus value move/copy cost | `RENAME key newkey` | Handles same-key case, destination overwrite, TTL movement, memory admission, WATCH, and AOF. |
| RENAMENX | Rename a key only if destination is absent. | Supported | O(1) average plus value move/copy cost | `RENAMENX key newkey` | Returns `0` if destination exists. |
| RESTORE | Create key from serialized payload. | Pending | O(N) serialized bytes | `RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]` | No Redis dump/restore serialization support yet. |
| SCAN | Incrementally iterate keys. | Supported | O(count plus scanned slots) | `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]` | Cursor encodes shard and slot. Supports `MATCH`, `COUNT`, and `TYPE` over current value type names. |
| SORT | Sort list/set/zset elements. | Pending | O(N log N) | `SORT key [BY pattern] [LIMIT offset count] [GET pattern] [ASC or DESC] [ALPHA] [STORE destination]` | Depends on data types not implemented yet. |
| SORT_RO | Read-only sort. | Pending | O(N log N) | `SORT_RO key [BY pattern] [LIMIT offset count] [GET pattern] [ASC or DESC] [ALPHA]` | Depends on list/set/zset data types not implemented yet. |
| TOUCH | Touch keys and return count. | Partial | O(N) keys | `TOUCH key [key ...]` | Currently behaves like `EXISTS`; normal access recording may update LRU/LFU metadata when maxmemory is active. |
| TTL | Return remaining TTL in seconds. | Supported | O(1) | `TTL key` | Returns Redis sentinel values for no TTL and missing keys. |
| TYPE | Return value type. | Supported | O(1) average | `TYPE key` | Current visible values are string/integer string-family values; expired keys return `none` and may be cleaned. |
| UNLINK | Asynchronously delete keys. | Partial | O(N) keys | `UNLINK key [key ...]` | Currently aliases `DEL`; deletion is synchronous from the command perspective. |
| WAIT | Wait for replication acknowledgements. | Pending | O(1) plus blocking wait | `WAIT numreplicas timeout` | No Redis replication support yet. |
| WAITAOF | Wait for AOF persistence acknowledgements. | Pending | O(1) plus blocking wait | `WAITAOF numlocal numreplicas timeout` | Vortex has AOF durability policy, but no `WAITAOF` command. |

## Connection Commands

| Name | Description | Status | Time complexity | Syntax | Notes |
| --- | --- | --- | --- | --- | --- |
| AUTH | Authenticate the connection. | Pending | O(N) password bytes | `AUTH [username] password` | No ACL/authentication command surface yet. |
| CLIENT CACHING | Control tracking for the next request. | Pending | O(1) | `CLIENT CACHING YES or NO` | No client-side caching support yet. |
| CLIENT GETNAME | Return connection name. | Pending | O(1) | `CLIENT GETNAME` | Connection names are not exposed. |
| CLIENT GETREDIR | Return tracking redirect client id. | Pending | O(1) | `CLIENT GETREDIR` | No tracking redirect support. |
| CLIENT ID | Return connection id. | Pending | O(1) | `CLIENT ID` | Reactor has connection slots, but no Redis client id command. |
| CLIENT INFO | Return current connection information. | Pending | O(1) | `CLIENT INFO` | No command-level client info response yet. |
| CLIENT KILL | Kill client connections. | Pending | O(N) clients | `CLIENT KILL [filters]` | No admin client-kill command yet. |
| CLIENT LIST | List connections. | Pending | O(N) clients | `CLIENT LIST [filters]` | Runtime metrics expose counts, not Redis client listings. |
| CLIENT NO-EVICT | Toggle client eviction mode. | Pending | O(1) | `CLIENT NO-EVICT ON or OFF` | No per-client eviction exemption state. |
| CLIENT NO-TOUCH | Toggle LRU/LFU touch behavior. | Pending | O(1) | `CLIENT NO-TOUCH ON or OFF` | No per-client access-recording override. |
| CLIENT PAUSE | Pause command processing. | Pending | O(1) | `CLIENT PAUSE timeout [WRITE or ALL]` | No pause gate exposed as Redis command. |
| CLIENT REPLY | Control replies for this connection. | Pending | O(1) | `CLIENT REPLY ON or OFF or SKIP` | Reactor always writes normal replies today. |
| CLIENT SETINFO | Set client library metadata. | Pending | O(1) | `CLIENT SETINFO LIB-NAME name or LIB-VER ver` | No client metadata command yet. |
| CLIENT SETNAME | Set connection name. | Pending | O(1) | `CLIENT SETNAME connection-name` | No connection-name storage. |
| CLIENT TRACKING | Configure client-side caching. | Pending | O(1) | `CLIENT TRACKING ON or OFF [options]` | No tracking invalidation protocol. |
| CLIENT TRACKINGINFO | Return tracking settings. | Pending | O(1) | `CLIENT TRACKINGINFO` | No tracking support. |
| CLIENT UNBLOCK | Unblock a blocked client. | Pending | O(log N) in Redis | `CLIENT UNBLOCK client-id [TIMEOUT or ERROR]` | Blocking commands and client ids are not exposed. |
| CLIENT UNPAUSE | Resume paused clients. | Pending | O(1) | `CLIENT UNPAUSE` | No pause state. |
| ECHO | Return a message. | Supported | O(M) message bytes | `ECHO message` | Engine returns a bulk string copy and does not touch the keyspace. |
| HELLO | RESP handshake and protocol negotiation. | Pending | O(1) | `HELLO [protover [AUTH username password] [SETNAME name]]` | No RESP3 handshake command yet. |
| PING | Return server liveness response or echo message. | Supported | O(1) without message, O(M) with message. | `PING [message]` | No-message path returns static `PONG`. |
| QUIT | Close the connection after replying. | Supported | O(1) | `QUIT` | Engine returns `OK`; reactor owns socket close. |
| RESET | Reset connection state. | Pending | O(1) | `RESET` | No Redis `RESET` command yet. |
| SELECT | Select logical database. | Partial | O(1) | `SELECT index` | Only `SELECT 0` succeeds because Vortex alpha has one logical database. |

## Transaction Commands

| Name | Description | Status | Time complexity | Syntax | Notes |
| --- | --- | --- | --- | --- | --- |
| DISCARD | Discard queued transaction commands. | Supported | O(N + W) in reactor | `DISCARD` | Reactor clears per-connection queue and watches. Engine fallback returns Redis error outside `MULTI`. |
| EXEC | Execute queued transaction commands. | Supported | O(N + W) in reactor | `EXEC` | Reactor validates WATCH registrations under transaction gates, executes queued commands, and batches AOF. |
| MULTI | Start transaction queueing. | Supported | O(1) | `MULTI` | Reactor-owned; not dispatched through `commands::execute_command`. |
| UNWATCH | Clear watched keys. | Supported | O(W) in reactor | `UNWATCH` | Reactor clears registrations; engine fallback returns `OK` when no per-connection state is present. |
| WATCH | Watch keys for optimistic transaction abort. | Supported | O(N) watched keys plus shard lookups | `WATCH key [key ...]` | Reactor owns per-connection watch list; engine owns entry LSNs, absent-key watch registry, and validation. |

## Server Commands

| Name | Description | Status | Time complexity | Syntax | Notes |
| --- | --- | --- | --- | --- | --- |
| ACL CAT | List ACL categories or commands in a category. | Pending | Varies | `ACL CAT [category]` | No ACL subsystem yet. |
| ACL DELUSER | Delete ACL users. | Pending | O(N) users | `ACL DELUSER username [username ...]` | No ACL subsystem yet. |
| ACL DRYRUN | Simulate command execution under ACL rules. | Pending | Varies | `ACL DRYRUN username command [arg ...]` | No ACL subsystem yet. |
| ACL GENPASS | Generate a secure password. | Pending | O(1) | `ACL GENPASS [bits]` | No ACL subsystem yet. |
| ACL GETUSER | Return ACL user rules. | Pending | O(N) rules | `ACL GETUSER username` | No ACL subsystem yet. |
| ACL LIST | List ACL rules. | Pending | O(N) users | `ACL LIST` | No ACL subsystem yet. |
| ACL LOAD | Reload ACL file. | Pending | O(N) rules | `ACL LOAD` | No ACL file support. |
| ACL LOG | Return ACL security log. | Pending | O(N) entries | `ACL LOG [count or RESET]` | No ACL log support. |
| ACL SAVE | Persist ACL rules. | Pending | O(N) rules | `ACL SAVE` | No ACL file support. |
| ACL SETUSER | Create or modify ACL user. | Pending | O(N) rules | `ACL SETUSER username [rule ...]` | No ACL subsystem yet. |
| ACL USERS | List ACL users. | Pending | O(N) users | `ACL USERS` | No ACL subsystem yet. |
| ACL WHOAMI | Return current ACL username. | Pending | O(1) | `ACL WHOAMI` | No authentication identity yet. |
| BGREWRITEAOF | Rewrite AOF in background. | Partial | O(1) to request in Redis | `BGREWRITEAOF` | Reactor recognizes it but currently returns a disabled error; rewrite implementation is pending. |
| BGSAVE | Save database in background. | Pending | O(1) to request in Redis | `BGSAVE [SCHEDULE]` | No RDB background save support. |
| COMMAND | Return command metadata. | Partial | O(N) commands | `COMMAND` | Returns alpha-visible metadata. Coverage follows Vortex supported/published commands, not full Redis. |
| COMMAND COUNT | Return command count. | Partial | O(1) | `COMMAND COUNT` | Counts alpha-visible metadata entries. |
| COMMAND DOCS | Return command docs. | Partial | O(N) in Redis | `COMMAND DOCS [command ...]` | Current engine returns an empty array stub. |
| COMMAND GETKEYS | Extract keys from a command. | Partial | O(N) args in Redis | `COMMAND GETKEYS command [arg ...]` | Current engine returns an empty array stub. |
| COMMAND GETKEYSANDFLAGS | Extract keys and flags from a command. | Pending | O(N) args | `COMMAND GETKEYSANDFLAGS command [arg ...]` | No handler. |
| COMMAND INFO | Return metadata for selected commands. | Partial | O(N) requested commands | `COMMAND INFO command [command ...]` | Returns metadata for alpha-visible commands and null for unknown commands. |
| COMMAND LIST | Return command names. | Partial | O(N) commands | `COMMAND LIST [FILTERBY ...]` | Current engine returns an empty array stub. |
| CONFIG GET | Return runtime config values. | Partial | O(1) for supported params | `CONFIG GET parameter` | Reactor supports `appendonly`, `appendfsync`, `maxmemory`, and `maxmemory-policy`. Other params fall through and are not implemented. |
| CONFIG RESETSTAT | Reset server statistics. | Pending | O(1) | `CONFIG RESETSTAT` | No reset-stat command. |
| CONFIG REWRITE | Persist config file. | Pending | O(1) request plus file I/O | `CONFIG REWRITE` | No config-file rewrite support. |
| CONFIG SET | Set runtime config values. | Partial | O(1) for supported params plus AOF I/O when toggling appendonly. | `CONFIG SET parameter value` | Reactor supports `appendonly`, `maxmemory`, and `maxmemory-policy`; appendonly runtime changes are restricted in multi-reactor alpha. |
| DBSIZE | Return number of keys. | Supported | Redis O(1); Vortex exact count scans shards/slots. | `DBSIZE` | Counts only live, non-expired keys via exact keyspace count. |
| FAILOVER | Coordinate failover to replica. | Pending | O(1) request | `FAILOVER [TO host port [FORCE]] [ABORT] [TIMEOUT milliseconds]` | No replication/failover support. |
| FLUSHALL | Remove all keys from all databases. | Partial | O(N) keys | `FLUSHALL [ASYNC or SYNC]` | Vortex has one database. Handler clears shared keyspace synchronously and emits AOF LSN when active. |
| FLUSHDB | Remove all keys from current database. | Partial | O(N) keys | `FLUSHDB [ASYNC or SYNC]` | Same effect as `FLUSHALL` in one-DB alpha. |
| HOTKEYS | Container for hotkey tracking commands. | Pending | Varies | `HOTKEYS subcommand [arg ...]` | Redis 8.6 feature; Vortex has LFU metadata but no HOTKEYS command surface. |
| HOTKEYS GET | Return tracked hotkeys. | Pending | Varies | `HOTKEYS GET [options]` | No hotkey reporting command. |
| HOTKEYS RESET | Reset hotkey tracking. | Pending | Varies | `HOTKEYS RESET` | No hotkey reporting command. |
| HOTKEYS START | Start hotkey tracking. | Pending | Varies | `HOTKEYS START [options]` | No hotkey reporting command. |
| HOTKEYS STOP | Stop hotkey tracking. | Pending | Varies | `HOTKEYS STOP` | No hotkey reporting command. |
| INFO | Return server statistics. | Partial | Redis O(1); Vortex sections may scan shards/slots. | `INFO [section]` | Supports `server`, `clients`, `memory`, `runtime`, `keyspace`, and `all`; not a full Redis INFO surface. |
| LASTSAVE | Return last successful save timestamp. | Pending | O(1) | `LASTSAVE` | No RDB save tracking command. |
| LATENCY DOCTOR | Return latency analysis. | Pending | O(1) or O(N) samples | `LATENCY DOCTOR` | Runtime metrics exist, but Redis latency command family is not implemented. |
| LATENCY GRAPH | Return latency graph. | Pending | O(N) samples | `LATENCY GRAPH event` | Not implemented. |
| LATENCY HISTOGRAM | Return latency histograms. | Pending | O(N) commands | `LATENCY HISTOGRAM [command ...]` | Not implemented. |
| LATENCY HISTORY | Return latency history. | Pending | O(N) samples | `LATENCY HISTORY event` | Not implemented. |
| LATENCY LATEST | Return latest latency events. | Pending | O(N) events | `LATENCY LATEST` | Not implemented. |
| LATENCY RESET | Reset latency events. | Pending | O(N) events | `LATENCY RESET [event ...]` | Not implemented. |
| LOLWUT | Display Redis art/version. | Pending | O(1) | `LOLWUT [VERSION version]` | Not implemented. |
| MEMORY DOCTOR | Return memory diagnostics. | Pending | O(1) or O(N) stats | `MEMORY DOCTOR` | Vortex exposes memory fields through `INFO memory`, not Redis `MEMORY` commands. |
| MEMORY MALLOC-STATS | Return allocator stats. | Pending | O(1) plus allocator reporting | `MEMORY MALLOC-STATS` | `INFO memory` reads jemalloc stats, but this command is not implemented. |
| MEMORY PURGE | Ask allocator to purge memory. | Pending | O(1) request | `MEMORY PURGE` | Not implemented. |
| MEMORY STATS | Return memory details. | Pending | O(1) or O(N) stats | `MEMORY STATS` | Not implemented as Redis command. |
| MEMORY USAGE | Estimate memory for one key. | Pending | O(N) samples or value size | `MEMORY USAGE key [SAMPLES count]` | Engine has internal memory accounting but no per-key command. |
| MODULE LIST | List loaded modules. | Pending | O(N) modules | `MODULE LIST` | No module system. |
| MODULE LOAD | Load module. | Pending | O(1) request plus module work | `MODULE LOAD path [arg ...]` | No module system. |
| MODULE LOADEX | Load module with extended parameters. | Pending | O(1) request plus module work | `MODULE LOADEX path [CONFIG name value ...] [ARGS arg ...]` | No module system. |
| MODULE UNLOAD | Unload module. | Pending | O(1) request plus module cleanup | `MODULE UNLOAD name` | No module system. |
| MONITOR | Stream every command received. | Pending | O(N) clients per command in Redis | `MONITOR` | No monitor streaming command. |
| PSYNC | Internal replication partial sync. | Pending | Varies | `PSYNC replicationid offset` | No replication protocol. |
| REPLCONF | Internal replication config. | Pending | O(1) | `REPLCONF option value [option value ...]` | No replication protocol. |
| REPLICAOF | Configure replica/master role. | Pending | O(1) request | `REPLICAOF host port` | No replication role support. |
| RESTORE-ASKING | Internal cluster migration restore. | Pending | O(N) serialized bytes | `RESTORE-ASKING key ttl serialized-value [options]` | No cluster migration support. |
| ROLE | Return replication role. | Pending | O(1) | `ROLE` | No replication role state. |
| SAVE | Synchronously save database. | Pending | O(N) keys | `SAVE` | No RDB save command. |
| SHUTDOWN | Save and shut down server. | Pending | O(N) if saving | `SHUTDOWN [NOSAVE or SAVE] [NOW] [FORCE] [ABORT]` | No Redis shutdown command. |
| SLAVEOF | Legacy replica configuration command. | Pending | O(1) request | `SLAVEOF host port` | No replication role support. |
| SLOWLOG GET | Return slow log entries. | Pending | O(N) entries | `SLOWLOG GET [count]` | No Redis slowlog command. |
| SLOWLOG LEN | Return slow log length. | Pending | O(1) | `SLOWLOG LEN` | No Redis slowlog command. |
| SLOWLOG RESET | Clear slow log. | Pending | O(N) entries | `SLOWLOG RESET` | No Redis slowlog command. |
| SWAPDB | Swap two logical databases. | Pending | O(1) | `SWAPDB index1 index2` | Vortex alpha has one logical database. |
| SYNC | Internal full replication sync. | Pending | Varies | `SYNC` | No replication protocol. |
| TIME | Return server wall-clock time. | Supported | O(1) | `TIME` | Uses reactor-provided Unix clock when available. |

