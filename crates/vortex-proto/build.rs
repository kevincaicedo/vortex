use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// Shorthand for KeyRange in generated code.
const KR_NONE: &str = "KeyRange::NONE";

fn kr_single(pos: i16) -> String {
    format!("KeyRange::single({pos})")
}

fn kr_all(first: i16) -> String {
    format!("KeyRange::all_from({first})")
}

fn kr(first: i16, last: i16, step: i16) -> String {
    format!("KeyRange {{ first: {first}, last: {last}, step: {step} }}")
}

fn main() {
    let path = Path::new(&env::var("OUT_DIR").unwrap()).join("command_table.rs");
    let mut file = BufWriter::new(File::create(&path).unwrap());

    let mut map = phf_codegen::Map::new();

    // ── Helper closure ──────────────────────────────────────────────────
    // (name, arity, flags_expr, key_range_expr)
    let commands: &[(&str, i16, &str, String)] = &[
        // ── Connection ──────────────────────────────────────────────────
        ("PING", -1, "CommandFlags::FAST", KR_NONE.into()),
        ("ECHO", 2, "CommandFlags::FAST", KR_NONE.into()),
        ("QUIT", 1, "CommandFlags::FAST", KR_NONE.into()),
        ("AUTH", -2, "CommandFlags::FAST", KR_NONE.into()),
        ("SELECT", 2, "CommandFlags::FAST", KR_NONE.into()),
        ("HELLO", -1, "CommandFlags::FAST", KR_NONE.into()),
        ("RESET", 1, "CommandFlags::FAST", KR_NONE.into()),
        ("CLIENT", -2, "CommandFlags::SLOW", KR_NONE.into()),
        // ── Server ──────────────────────────────────────────────────────
        ("COMMAND", -1, "CommandFlags::SLOW", KR_NONE.into()),
        ("INFO", -1, "CommandFlags::SLOW", KR_NONE.into()),
        (
            "DBSIZE",
            1,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            KR_NONE.into(),
        ),
        (
            "FLUSHDB",
            -1,
            "CommandFlags::WRITE.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        (
            "FLUSHALL",
            -1,
            "CommandFlags::WRITE.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        (
            "CONFIG",
            -2,
            "CommandFlags::ADMIN.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        (
            "DEBUG",
            -2,
            "CommandFlags::ADMIN.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        (
            "SAVE",
            1,
            "CommandFlags::ADMIN.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        ("BGSAVE", -1, "CommandFlags::ADMIN", KR_NONE.into()),
        ("BGREWRITEAOF", 1, "CommandFlags::ADMIN", KR_NONE.into()),
        (
            "LASTSAVE",
            1,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            KR_NONE.into(),
        ),
        (
            "TIME",
            1,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            KR_NONE.into(),
        ),
        ("SLOWLOG", -2, "CommandFlags::ADMIN", KR_NONE.into()),
        ("MULTI", 1, "CommandFlags::FAST", KR_NONE.into()),
        ("EXEC", 1, "CommandFlags::SLOW", KR_NONE.into()),
        ("DISCARD", 1, "CommandFlags::FAST", KR_NONE.into()),
        ("WATCH", -2, "CommandFlags::FAST", kr_all(1)),
        ("UNWATCH", 1, "CommandFlags::FAST", KR_NONE.into()),
        (
            "WAIT",
            3,
            "CommandFlags::SLOW.union(CommandFlags::BLOCKING)",
            KR_NONE.into(),
        ),
        (
            "SHUTDOWN",
            -1,
            "CommandFlags::ADMIN.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        (
            "SWAPDB",
            3,
            "CommandFlags::WRITE.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        (
            "OBJECT",
            -2,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_single(2),
        ),
        (
            "TYPE",
            2,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        ("DUMP", 2, "CommandFlags::READ", kr_single(1)),
        ("RESTORE", -4, "CommandFlags::WRITE", kr_single(1)),
        (
            "SCAN",
            -2,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        ("RANDOMKEY", 1, "CommandFlags::READ", KR_NONE.into()),
        (
            "KEYS",
            2,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        ("COPY", -3, "CommandFlags::WRITE", kr(1, 2, 1)),
        // ── String ──────────────────────────────────────────────────────
        ("SET", -3, "CommandFlags::WRITE", kr_single(1)),
        (
            "SETNX",
            3,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        ("SETEX", 4, "CommandFlags::WRITE", kr_single(1)),
        ("PSETEX", 4, "CommandFlags::WRITE", kr_single(1)),
        ("MSET", -3, "CommandFlags::WRITE", kr(1, -1, 2)),
        ("MSETNX", -3, "CommandFlags::WRITE", kr(1, -1, 2)),
        (
            "GET",
            2,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "GETDEL",
            2,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "GETEX",
            -2,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        ("GETRANGE", 4, "CommandFlags::READ", kr_single(1)),
        ("SETRANGE", 4, "CommandFlags::WRITE", kr_single(1)),
        (
            "MGET",
            -2,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_all(1),
        ),
        ("GETSET", 3, "CommandFlags::WRITE", kr_single(1)),
        (
            "INCR",
            2,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "INCRBY",
            3,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "INCRBYFLOAT",
            3,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "DECR",
            2,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "DECRBY",
            3,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        ("APPEND", 3, "CommandFlags::WRITE", kr_single(1)),
        (
            "STRLEN",
            2,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        ("SUBSTR", 4, "CommandFlags::READ", kr_single(1)),
        ("LCS", -3, "CommandFlags::READ", kr(1, 2, 1)),
        // ── Key ─────────────────────────────────────────────────────────
        ("DEL", -2, "CommandFlags::WRITE", kr_all(1)),
        (
            "UNLINK",
            -2,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_all(1),
        ),
        (
            "EXISTS",
            -2,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_all(1),
        ),
        (
            "EXPIRE",
            3,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "EXPIREAT",
            3,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "EXPIRETIME",
            2,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "PEXPIRE",
            3,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "PEXPIREAT",
            3,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "PEXPIRETIME",
            2,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "PERSIST",
            2,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "TTL",
            2,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "PTTL",
            2,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        ("RENAME", 3, "CommandFlags::WRITE", kr(1, 2, 1)),
        ("RENAMENX", 3, "CommandFlags::WRITE", kr(1, 2, 1)),
        (
            "TOUCH",
            -2,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_all(1),
        ),
        (
            "SORT",
            -2,
            "CommandFlags::WRITE.union(CommandFlags::SLOW)",
            kr_single(1),
        ),
        (
            "SORT_RO",
            -2,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_single(1),
        ),
        // ── List ────────────────────────────────────────────────────────
        (
            "LPUSH",
            -3,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "RPUSH",
            -3,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "LPUSHX",
            -3,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "RPUSHX",
            -3,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "LPOP",
            -2,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "RPOP",
            -2,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "LLEN",
            2,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        ("LINDEX", 3, "CommandFlags::READ", kr_single(1)),
        ("LINSERT", 5, "CommandFlags::WRITE", kr_single(1)),
        ("LSET", 4, "CommandFlags::WRITE", kr_single(1)),
        ("LRANGE", 4, "CommandFlags::READ", kr_single(1)),
        ("LTRIM", 4, "CommandFlags::WRITE", kr_single(1)),
        ("LREM", 4, "CommandFlags::WRITE", kr_single(1)),
        ("LPOS", -3, "CommandFlags::READ", kr_single(1)),
        ("LMOVE", 5, "CommandFlags::WRITE", kr(1, 2, 1)),
        ("LMPOP", -4, "CommandFlags::WRITE", kr_all(1)),
        (
            "BLPOP",
            -3,
            "CommandFlags::WRITE.union(CommandFlags::BLOCKING)",
            kr_all(1),
        ),
        (
            "BRPOP",
            -3,
            "CommandFlags::WRITE.union(CommandFlags::BLOCKING)",
            kr_all(1),
        ),
        (
            "BLMOVE",
            6,
            "CommandFlags::WRITE.union(CommandFlags::BLOCKING)",
            kr(1, 2, 1),
        ),
        (
            "BLMPOP",
            -5,
            "CommandFlags::WRITE.union(CommandFlags::BLOCKING)",
            kr_all(2),
        ),
        // ── Hash ────────────────────────────────────────────────────────
        (
            "HSET",
            -4,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "HGET",
            3,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "HSETNX",
            4,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "HMSET",
            -4,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "HMGET",
            -3,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "HDEL",
            -3,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "HEXISTS",
            3,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "HLEN",
            2,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "HKEYS",
            2,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_single(1),
        ),
        (
            "HVALS",
            2,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_single(1),
        ),
        (
            "HGETALL",
            2,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_single(1),
        ),
        (
            "HINCRBY",
            4,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "HINCRBYFLOAT",
            4,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        ("HRANDFIELD", -2, "CommandFlags::READ", kr_single(1)),
        (
            "HSCAN",
            -3,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_single(1),
        ),
        // ── Set ─────────────────────────────────────────────────────────
        (
            "SADD",
            -3,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "SREM",
            -3,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "SISMEMBER",
            3,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "SMISMEMBER",
            -3,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "SMEMBERS",
            2,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_single(1),
        ),
        (
            "SCARD",
            2,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "SPOP",
            -2,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        ("SRANDMEMBER", -2, "CommandFlags::READ", kr_single(1)),
        (
            "SINTER",
            -2,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_all(1),
        ),
        (
            "SINTERCARD",
            -3,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_all(2),
        ),
        (
            "SINTERSTORE",
            -3,
            "CommandFlags::WRITE.union(CommandFlags::SLOW)",
            kr_all(1),
        ),
        (
            "SUNION",
            -2,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_all(1),
        ),
        (
            "SUNIONSTORE",
            -3,
            "CommandFlags::WRITE.union(CommandFlags::SLOW)",
            kr_all(1),
        ),
        (
            "SDIFF",
            -2,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_all(1),
        ),
        (
            "SDIFFSTORE",
            -3,
            "CommandFlags::WRITE.union(CommandFlags::SLOW)",
            kr_all(1),
        ),
        (
            "SMOVE",
            4,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr(1, 2, 1),
        ),
        (
            "SSCAN",
            -3,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_single(1),
        ),
        // ── Sorted Set ──────────────────────────────────────────────────
        (
            "ZADD",
            -4,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "ZREM",
            -3,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "ZSCORE",
            3,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "ZMSCORE",
            -3,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "ZINCRBY",
            4,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "ZCARD",
            2,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "ZCOUNT",
            4,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "ZLEXCOUNT",
            4,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        ("ZRANGE", -4, "CommandFlags::READ", kr_single(1)),
        ("ZRANGESTORE", -5, "CommandFlags::WRITE", kr(1, 2, 1)),
        ("ZRANGEBYLEX", -4, "CommandFlags::READ", kr_single(1)),
        ("ZRANGEBYSCORE", -4, "CommandFlags::READ", kr_single(1)),
        ("ZREVRANGE", 4, "CommandFlags::READ", kr_single(1)),
        ("ZREVRANGEBYLEX", -4, "CommandFlags::READ", kr_single(1)),
        ("ZREVRANGEBYSCORE", -4, "CommandFlags::READ", kr_single(1)),
        (
            "ZREVRANK",
            3,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "ZRANK",
            3,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        ("ZRANDMEMBER", -2, "CommandFlags::READ", kr_single(1)),
        (
            "ZPOPMIN",
            -2,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "ZPOPMAX",
            -2,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "BZPOPMIN",
            -3,
            "CommandFlags::WRITE.union(CommandFlags::BLOCKING)",
            kr_all(1),
        ),
        (
            "BZPOPMAX",
            -3,
            "CommandFlags::WRITE.union(CommandFlags::BLOCKING)",
            kr_all(1),
        ),
        (
            "ZINTERSTORE",
            -4,
            "CommandFlags::WRITE.union(CommandFlags::SLOW)",
            kr_single(1),
        ),
        (
            "ZUNIONSTORE",
            -4,
            "CommandFlags::WRITE.union(CommandFlags::SLOW)",
            kr_single(1),
        ),
        (
            "ZDIFFSTORE",
            -4,
            "CommandFlags::WRITE.union(CommandFlags::SLOW)",
            kr_single(1),
        ),
        (
            "ZINTER",
            -3,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_all(1),
        ),
        (
            "ZUNION",
            -3,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_all(1),
        ),
        (
            "ZDIFF",
            -3,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_all(1),
        ),
        (
            "ZINTERCARD",
            -3,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_all(2),
        ),
        (
            "ZSCAN",
            -3,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_single(1),
        ),
        ("ZMPOP", -4, "CommandFlags::WRITE", kr_all(2)),
        (
            "BZMPOP",
            -5,
            "CommandFlags::WRITE.union(CommandFlags::BLOCKING)",
            kr_all(3),
        ),
        // ── Pub/Sub ─────────────────────────────────────────────────────
        (
            "SUBSCRIBE",
            -2,
            "CommandFlags::PUBSUB.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        (
            "UNSUBSCRIBE",
            -1,
            "CommandFlags::PUBSUB.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        (
            "PSUBSCRIBE",
            -2,
            "CommandFlags::PUBSUB.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        (
            "PUNSUBSCRIBE",
            -1,
            "CommandFlags::PUBSUB.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        (
            "PUBLISH",
            3,
            "CommandFlags::PUBSUB.union(CommandFlags::FAST)",
            KR_NONE.into(),
        ),
        (
            "PUBSUB",
            -2,
            "CommandFlags::PUBSUB.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        // ── Scripting ───────────────────────────────────────────────────
        (
            "EVAL",
            -3,
            "CommandFlags::SCRIPTING.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        (
            "EVALSHA",
            -3,
            "CommandFlags::SCRIPTING.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        (
            "EVALRO",
            -3,
            "CommandFlags::SCRIPTING.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        (
            "EVALSHA_RO",
            -3,
            "CommandFlags::SCRIPTING.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        (
            "SCRIPT",
            -2,
            "CommandFlags::SCRIPTING.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        // ── HyperLogLog ─────────────────────────────────────────────────
        (
            "PFADD",
            -2,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "PFCOUNT",
            -2,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_all(1),
        ),
        (
            "PFMERGE",
            -2,
            "CommandFlags::WRITE.union(CommandFlags::SLOW)",
            kr_all(1),
        ),
        // ── Bitmap ──────────────────────────────────────────────────────
        ("SETBIT", 4, "CommandFlags::WRITE", kr_single(1)),
        (
            "GETBIT",
            3,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        ("BITCOUNT", -2, "CommandFlags::READ", kr_single(1)),
        (
            "BITOP",
            -4,
            "CommandFlags::WRITE.union(CommandFlags::SLOW)",
            kr_all(2),
        ),
        ("BITPOS", -3, "CommandFlags::READ", kr_single(1)),
        ("BITFIELD", -2, "CommandFlags::WRITE", kr_single(1)),
        (
            "BITFIELD_RO",
            -2,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        // ── Geo ─────────────────────────────────────────────────────────
        ("GEOADD", -5, "CommandFlags::WRITE", kr_single(1)),
        ("GEODIST", -4, "CommandFlags::READ", kr_single(1)),
        ("GEOHASH", -3, "CommandFlags::READ", kr_single(1)),
        ("GEOPOS", -3, "CommandFlags::READ", kr_single(1)),
        (
            "GEORADIUS",
            -6,
            "CommandFlags::WRITE.union(CommandFlags::SLOW)",
            kr_single(1),
        ),
        (
            "GEORADIUS_RO",
            -6,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_single(1),
        ),
        (
            "GEORADIUSBYMEMBER",
            -5,
            "CommandFlags::WRITE.union(CommandFlags::SLOW)",
            kr_single(1),
        ),
        (
            "GEORADIUSBYMEMBER_RO",
            -5,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_single(1),
        ),
        ("GEOSEARCH", -7, "CommandFlags::READ", kr_single(1)),
        ("GEOSEARCHSTORE", -8, "CommandFlags::WRITE", kr(1, 2, 1)),
        // ── Stream ──────────────────────────────────────────────────────
        (
            "XADD",
            -5,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        (
            "XLEN",
            2,
            "CommandFlags::READ.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        ("XRANGE", -4, "CommandFlags::READ", kr_single(1)),
        ("XREVRANGE", -4, "CommandFlags::READ", kr_single(1)),
        (
            "XREAD",
            -4,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        (
            "XREADGROUP",
            -7,
            "CommandFlags::WRITE.union(CommandFlags::SLOW)",
            KR_NONE.into(),
        ),
        (
            "XACK",
            -4,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        ("XCLAIM", -6, "CommandFlags::WRITE", kr_single(1)),
        ("XAUTOCLAIM", -7, "CommandFlags::WRITE", kr_single(1)),
        (
            "XDEL",
            -3,
            "CommandFlags::WRITE.union(CommandFlags::FAST)",
            kr_single(1),
        ),
        ("XTRIM", -4, "CommandFlags::WRITE", kr_single(1)),
        (
            "XINFO",
            -2,
            "CommandFlags::READ.union(CommandFlags::SLOW)",
            kr_single(2),
        ),
        (
            "XGROUP",
            -2,
            "CommandFlags::WRITE.union(CommandFlags::SLOW)",
            kr_single(2),
        ),
        ("XPENDING", -3, "CommandFlags::READ", kr_single(1)),
        ("XSETID", 3, "CommandFlags::WRITE", kr_single(1)),
        // ── Cluster ─────────────────────────────────────────────────────
        ("CLUSTER", -2, "CommandFlags::ADMIN", KR_NONE.into()),
        ("ASKING", 1, "CommandFlags::FAST", KR_NONE.into()),
        ("READONLY", 1, "CommandFlags::FAST", KR_NONE.into()),
        ("READWRITE", 1, "CommandFlags::FAST", KR_NONE.into()),
        // ── Misc ────────────────────────────────────────────────────────
    ];

    for &(name, arity, flags, ref key_range) in commands {
        let val = format!(
            "&CommandMeta {{ name: \"{name}\", arity: {arity}, flags: {flags}, key_range: {key_range} }}"
        );
        map.entry(name, &val);
    }

    writeln!(
        &mut file,
        "/// Compile-time perfect hash table for O(1) command dispatch.\n\
         static COMMAND_TABLE: phf::Map<&'static str, &'static CommandMeta> = {};",
        map.build()
    )
    .unwrap();
}
