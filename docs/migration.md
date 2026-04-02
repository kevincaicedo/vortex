# Migrating from Redis to VortexDB

This guide walks you through replacing Redis with VortexDB for key-value workloads.

---

## Overview

VortexDB v0.1-alpha is a drop-in replacement for Redis **String and Key commands**. It speaks the same RESP2 wire protocol, listens on the same default port (6379), and works with existing Redis client libraries without code changes.

**What works today:**
- All 20 String commands (GET, SET, INCR, MGET, MSET, APPEND, etc.)
- All 20 Key management commands (DEL, EXPIRE, TTL, SCAN, RENAME, etc.)
- Core server commands (PING, INFO, DBSIZE, FLUSHDB, COMMAND, TIME, etc.)
- Any Redis client library that speaks RESP2

**What doesn't work yet:**
- Data structures: Lists, Hashes, Sets, Sorted Sets, Streams
- Pub/Sub
- Lua scripting (EVAL)
- Transactions (MULTI/EXEC)
- Persistence (AOF/RDB import)
- Replication
- Cluster mode
- ACL authentication (AUTH with ACL rules)

See [compatibility.md](compatibility.md) for the full command matrix.

---

## Step 1: Install VortexDB

### From Source

```sh
# Clone the repository
git clone https://github.com/vortexdb/vortex.git
cd vortex

# Build the release binary
cargo build --release --bin vortex-server

# Binary is at target/release/vortex-server
```

### Docker

```sh
# Build the production image
docker build -t vortexdb:latest .

# Run
docker run -p 6379:6379 vortexdb:latest
```

### Docker Compose

```yaml
services:
  vortex:
    image: vortexdb:latest
    ports:
      - "6379:6379"
    command: ["--bind", "0.0.0.0:6379", "--threads", "0"]
    deploy:
      resources:
        limits:
          cpus: "4"
          memory: 2g
```

---

## Step 2: Configuration Mapping

### Redis → VortexDB Config

| Redis Config | VortexDB Equivalent | Notes |
|-------------|---------------------|-------|
| `bind 127.0.0.1` | `--bind 127.0.0.1:6379` | VortexDB includes port in bind address |
| `port 6379` | `--bind 127.0.0.1:6379` | Combined with bind |
| `maxclients 10000` | `--max-clients 10000` | Same default |
| `maxmemory 1gb` | `--max-memory 1073741824` | In bytes (eviction not yet active) |
| `maxmemory-policy allkeys-lru` | `--eviction-policy allkeys-lru` | Recognized but not yet enforced |
| `timeout 300` | `--connection-timeout-secs 300` | Same default |
| `io-threads 4` | `--threads 0` | Auto-detect; VortexDB uses thread-per-core |
| `requirepass secret` | `--requirepass secret` | Simple password auth |
| `loglevel notice` | `--log-level info` | Levels: trace, debug, info, warn, error |
| `databases 16` | N/A | VortexDB uses a single database (DB 0) |
| `appendonly yes` | `--aof-enabled` | AOF support planned |
| `appendfsync everysec` | `--aof-fsync everysec` | Options: always, everysec, no |

### Environment Variables

Every CLI flag has a corresponding environment variable with the `VORTEX_` prefix:

```sh
VORTEX_BIND=0.0.0.0:6379
VORTEX_THREADS=4
VORTEX_MAX_CLIENTS=10000
VORTEX_MAX_MEMORY=1073741824
VORTEX_LOG_LEVEL=info
VORTEX_REQUIREPASS=secret
```

### TOML Config File

```toml
# vortex.toml
bind = "127.0.0.1:6379"
threads = 0
max_clients = 10000
max_memory = 0
log_level = "info"
connection_timeout_secs = 300
io_backend = "auto"
```

Load with: `vortex-server -c vortex.toml`

---

## Step 3: Client Library Compatibility

VortexDB speaks RESP2. Any Redis client library works without modification.

### Tested Libraries

| Language | Library | Status |
|----------|---------|--------|
| Python | `redis-py` | ✅ Works |
| Node.js | `ioredis` | ✅ Works |
| Node.js | `redis` (node-redis) | ✅ Works |
| Go | `go-redis` | ✅ Works |
| Rust | `redis-rs` | ✅ Works |
| Java | `Jedis` | ✅ Works |
| Java | `Lettuce` | ✅ Works |
| C | `hiredis` | ✅ Works |

### Connection Example

```python
# Python — no code changes needed
import redis
r = redis.Redis(host='localhost', port=6379)
r.set('hello', 'world')
print(r.get('hello'))  # b'world'
```

```javascript
// Node.js with ioredis — no code changes
const Redis = require('ioredis');
const redis = new Redis(6379, '127.0.0.1');
await redis.set('hello', 'world');
console.log(await redis.get('hello')); // 'world'
```

```go
// Go with go-redis — no code changes
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
rdb.Set(ctx, "hello", "world", 0)
val, _ := rdb.Get(ctx, "hello").Result()
fmt.Println(val) // "world"
```

---

## Step 4: Verify Compatibility

After starting VortexDB, verify your workload:

```sh
# Check connectivity
redis-cli -p 6379 PING
# PONG

# Check server info
redis-cli -p 6379 INFO server

# Run your test suite against VortexDB
# (point your tests to VortexDB's port)

# Verify command support
redis-cli -p 6379 COMMAND COUNT
# (integer) 55
```

---

## Step 5: Performance Tuning

### Thread Count

VortexDB auto-detects CPU count by default (`--threads 0`). For dedicated servers:

```sh
# Use all cores (default)
vortex-server --threads 0

# Pin to specific core count
vortex-server --threads 4
```

### I/O Backend

```sh
# Auto-detect (io_uring on Linux, kqueue on macOS)
vortex-server --io-backend auto

# Force io_uring (Linux only, best performance)
vortex-server --io-backend uring

# Force polling (cross-platform fallback)
vortex-server --io-backend polling
```

### Buffer Sizing

For workloads with large values:

```sh
# Increase I/O buffer size (default 16 KiB)
vortex-server --buffer-size 65536

# Increase fixed buffer count for high connection counts
vortex-server --fixed-buffers 4096
```

### Connection Limits

```sh
# High-connection deployment
vortex-server --max-clients 50000 --connection-timeout-secs 60
```

---

## Behavioral Differences

### Single Database

VortexDB uses a single database. `SELECT 0` succeeds; `SELECT 1` through `SELECT 15` return an error. If your application uses multiple Redis databases, consolidate keys into a single namespace with prefixes:

```
# Before (Redis with SELECT 1)
SELECT 1
SET user:123 "data"

# After (VortexDB, single DB with prefix)
SET db1:user:123 "data"
```

### UNLINK vs DEL

In Redis, `UNLINK` is an async variant of `DEL` that defers memory reclamation. In VortexDB v0.1, both `DEL` and `UNLINK` are synchronous. Performance is identical for both commands.

### MULTI/EXEC Transactions

Not yet implemented. Commands that depend on transactions need adjustment:

```
# This will fail in VortexDB v0.1
MULTI
SET key1 value1
SET key2 value2
EXEC

# Alternative: use MSET for atomic multi-key writes
MSET key1 value1 key2 value2
```

### INFO Output

VortexDB's `INFO` command returns 4 sections: `server`, `clients`, `memory`, `keyspace`. The format is Redis-compatible but field values reflect VortexDB's architecture (thread-per-core, jemalloc, etc.).

---

## Rollback Plan

Since VortexDB speaks the same protocol, rollback is straightforward:

1. Stop VortexDB
2. Start Redis on the same port
3. Reload data from your application or Redis persistence files

**Note:** VortexDB v0.1 does not support RDB import or AOF replay. Data exists only in memory. Plan your migration for workloads where data loss on restart is acceptable (caches, session stores, rate limiters).

---

## Data Import (Future)

RDB import and AOF replay are planned for Phase 5. Once available:

```sh
# Import from Redis RDB file (planned)
vortex-server --import-rdb dump.rdb

# Live migration via replication (planned for Phase 6)
# VortexDB will support PSYNC as a follower
```

---

## FAQ

**Q: Can I use VortexDB as a cache in front of a database?**
A: Yes. All the commands needed for a caching layer are implemented: GET, SET with EX/PX for TTL, DEL, EXISTS, EXPIRE, MGET, MSET.

**Q: Does VortexDB persist data?**
A: Not in v0.1-alpha. All data is in-memory only. AOF persistence is planned for Phase 5.

**Q: Can I use Redis Sentinel or Cluster with VortexDB?**
A: Not yet. Replication (Phase 6) and Cluster (Phase 7) are planned.

**Q: What happens if VortexDB doesn't recognize a command?**
A: It returns `-ERR unknown command '<name>'` — the same error format Redis uses. Client libraries handle this gracefully.

**Q: Is VortexDB safe for production?**
A: v0.1-alpha is suitable for non-critical workloads: caches, development environments, and performance-sensitive applications where data loss on restart is acceptable.
