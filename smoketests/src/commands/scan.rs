use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn scan_collects_all_keys(ctx: &mut SmokeContext) -> Result<()> {
    for key in ["scan:1", "scan:2", "scan:3"] {
        ctx.set(key, "v")?;
    }

    let mut cursor = 0u64;
    let mut keys = Vec::new();
    for _ in 0..8 {
        let (next, batch): (u64, Vec<String>) =
            ctx.exec(&["SCAN", &cursor.to_string(), "COUNT", "100"])?;
        keys.extend(batch);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }

    keys.sort();
    keys.dedup();
    assert_eq!(
        keys,
        vec![
            "scan:1".to_string(),
            "scan:2".to_string(),
            "scan:3".to_string(),
        ]
    );
    Ok(())
}

fn scan_match_filters_results(ctx: &mut SmokeContext) -> Result<()> {
    for key in ["user:1", "user:2", "other:1"] {
        ctx.set(key, "v")?;
    }

    let mut cursor = 0u64;
    let mut keys = Vec::new();
    for _ in 0..8 {
        let (next, batch): (u64, Vec<String>) = ctx.exec(&[
            "SCAN",
            &cursor.to_string(),
            "MATCH",
            "user:*",
            "COUNT",
            "100",
        ])?;
        keys.extend(batch);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }

    keys.sort();
    keys.dedup();
    assert_eq!(keys, vec!["user:1".to_string(), "user:2".to_string()]);
    Ok(())
}

fn scan_rejects_malformed_arguments(ctx: &mut SmokeContext) -> Result<()> {
    for args in [
        &["SCAN", "-1"][..],
        &["SCAN", "0", "COUNT"][..],
        &["SCAN", "0", "COUNT", "0"][..],
        &["SCAN", "0", "COUNT", "nope"][..],
    ] {
        ctx.exec_error(args)?;
    }

    for args in [
        &["SCAN", "0", "MATCH"][..],
        &["SCAN", "0", "TYPE"][..],
        &["SCAN", "0", "UNKNOWN"][..],
    ] {
        ctx.exec_error(args)?;
    }

    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("SCAN", CommandGroup::Key, SupportLevel::Supported)
        .summary("Incrementally iterates the keyspace using a Redis-compatible cursor.")
        .syntax(&["SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]"])
        .tested(&[
            "Cursor iteration reaches all inserted keys",
            "MATCH filtering",
            "Malformed cursor and option arguments are rejected",
        ])
        .not_tested(&["Cursor stability across concurrent writes"])
        .case(CaseDef::new(
            "collects all keys",
            "Repeated SCAN calls should eventually return every inserted key.",
            scan_collects_all_keys,
        ))
        .case(CaseDef::new(
            "match filters results",
            "SCAN MATCH should return only keys that satisfy the pattern.",
            scan_match_filters_results,
        ))
        .case(CaseDef::new(
            "rejects malformed arguments",
            "SCAN should reject negative cursors, invalid COUNT values, missing option values, and unknown options.",
            scan_rejects_malformed_arguments,
        ))
}
