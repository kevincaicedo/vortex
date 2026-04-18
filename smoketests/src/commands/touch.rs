use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn counts_existing_keys(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("a", "1")?;
    ctx.set("b", "2")?;
    let touched: i64 = ctx.exec(&["TOUCH", "a", "b", "missing"])?;
    assert_eq!(touched, 2);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("TOUCH", CommandGroup::Key, SupportLevel::Supported)
        .summary("Counts existing keys without modifying their values.")
        .syntax(&["TOUCH key [key ...]"])
        .tested(&["Count semantics across existing and missing keys"])
        .not_tested(&["LRU/LFU side effects because eviction is not implemented yet"])
        .case(CaseDef::new(
            "counts existing keys",
            "TOUCH should count only keys that currently exist.",
            counts_existing_keys,
        ))
}
