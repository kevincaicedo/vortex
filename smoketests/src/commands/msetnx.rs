use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn all_new_keys_are_set(ctx: &mut SmokeContext) -> Result<()> {
    let written: i64 = ctx.exec(&["MSETNX", "a", "1", "b", "2"])?;
    assert_eq!(written, 1);
    assert_eq!(ctx.get("a")?, Some("1".to_string()));
    assert_eq!(ctx.get("b")?, Some("2".to_string()));
    Ok(())
}

fn existing_key_aborts_entire_write(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("a", "1")?;
    let written: i64 = ctx.exec(&["MSETNX", "a", "9", "c", "3"])?;
    assert_eq!(written, 0);
    assert_eq!(ctx.get("a")?, Some("1".to_string()));
    assert_eq!(ctx.get("c")?, None);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("MSETNX", CommandGroup::String, SupportLevel::Supported)
        .summary("Sets multiple string keys only if none of them already exist.")
        .syntax(&["MSETNX key value [key value ...]"])
        .tested(&["All-new write", "Abort-on-existing atomicity"])
        .not_tested(&["Large-key multi-shard contention scenarios"])
        .case(CaseDef::new(
            "all keys new",
            "MSETNX should write every pair when none of the keys exist.",
            all_new_keys_are_set,
        ))
        .case(CaseDef::new(
            "existing key aborts write",
            "MSETNX should reject the entire batch if any key already exists.",
            existing_key_aborts_entire_write,
        ))
}
