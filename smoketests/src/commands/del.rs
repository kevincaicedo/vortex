use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn deletes_single_existing_key(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("a", "1")?;
    let removed: i64 = ctx.exec(&["DEL", "a"])?;
    assert_eq!(removed, 1);
    assert_eq!(ctx.get("a")?, None);
    Ok(())
}

fn deletes_multiple_keys_and_counts_hits(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("a", "1")?;
    ctx.set("b", "2")?;
    let removed: i64 = ctx.exec(&["DEL", "a", "b", "missing"])?;
    assert_eq!(removed, 2);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("DEL", CommandGroup::Key, SupportLevel::Supported)
        .summary("Deletes one or more keys and returns the number of keys removed.")
        .syntax(&["DEL key [key ...]"])
        .tested(&["Single-key delete", "Multi-key count semantics"])
        .not_tested(&["Concurrent delete races"])
        .case(CaseDef::new(
            "single key delete",
            "DEL should remove an existing key and return 1.",
            deletes_single_existing_key,
        ))
        .case(CaseDef::new(
            "multi key delete counts hits",
            "DEL should count only keys that existed.",
            deletes_multiple_keys_and_counts_hits,
        ))
}
