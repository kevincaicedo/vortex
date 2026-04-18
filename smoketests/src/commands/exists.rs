use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn single_key_hit_and_miss(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("a", "1")?;
    let hit: i64 = ctx.exec(&["EXISTS", "a"])?;
    let miss: i64 = ctx.exec(&["EXISTS", "missing"])?;
    assert_eq!(hit, 1);
    assert_eq!(miss, 0);
    Ok(())
}

fn duplicates_are_counted_like_redis(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("a", "1")?;
    let count: i64 = ctx.exec(&["EXISTS", "a", "a", "missing"])?;
    assert_eq!(count, 2);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("EXISTS", CommandGroup::Key, SupportLevel::Supported)
        .summary("Counts how many of the specified keys currently exist.")
        .syntax(&["EXISTS key [key ...]"])
        .tested(&["Single-key hit/miss", "Duplicate-key count semantics"])
        .not_tested(&["Large multi-key batches"])
        .case(CaseDef::new(
            "single key hit miss",
            "EXISTS should return 1 for a present key and 0 for a missing key.",
            single_key_hit_and_miss,
        ))
        .case(CaseDef::new(
            "duplicates are counted",
            "EXISTS should count duplicated key arguments like Redis.",
            duplicates_are_counted_like_redis,
        ))
}
