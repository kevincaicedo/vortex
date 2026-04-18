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

pub fn spec() -> CommandSpec {
    CommandSpec::new("SCAN", CommandGroup::Key, SupportLevel::Supported)
        .summary("Incrementally iterates the keyspace using a Redis-compatible cursor.")
        .syntax(&["SCAN cursor [MATCH pattern] [COUNT count]"])
        .tested(&["Cursor iteration reaches all inserted keys"])
        .not_tested(&[
            "MATCH filtering",
            "Cursor stability across concurrent writes",
        ])
        .case(CaseDef::new(
            "collects all keys",
            "Repeated SCAN calls should eventually return every inserted key.",
            scan_collects_all_keys,
        ))
}
