use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn missing_key_is_created(ctx: &mut SmokeContext) -> Result<()> {
    let created: i64 = ctx.exec(&["SETNX", "leader", "node-1"])?;
    assert_eq!(created, 1);
    assert_eq!(ctx.get("leader")?, Some("node-1".to_string()));
    Ok(())
}

fn existing_key_is_not_overwritten(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("leader", "node-1")?;
    let created: i64 = ctx.exec(&["SETNX", "leader", "node-2"])?;
    assert_eq!(created, 0);
    assert_eq!(ctx.get("leader")?, Some("node-1".to_string()));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("SETNX", CommandGroup::String, SupportLevel::Supported)
        .summary("Sets a string value only when the key is missing.")
        .syntax(&["SETNX key value"])
        .tested(&["Missing key creation", "Existing key is preserved"])
        .not_tested(&["Cross-client races"])
        .case(CaseDef::new(
            "missing key is created",
            "SETNX should create a missing key and return 1.",
            missing_key_is_created,
        ))
        .case(CaseDef::new(
            "existing key is preserved",
            "SETNX should not overwrite an existing key.",
            existing_key_is_not_overwritten,
        ))
}
