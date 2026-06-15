use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn sets_multiple_pairs_atomically(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["MSET", "a", "1", "b", "2", "c", "3"])?;
    assert_eq!(ctx.get("a")?, Some("1".to_string()));
    assert_eq!(ctx.get("b")?, Some("2".to_string()));
    assert_eq!(ctx.get("c")?, Some("3".to_string()));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("MSET", CommandGroup::String, SupportLevel::Supported)
        .summary("Atomically sets multiple string keys.")
        .syntax(&["MSET key value [key value ...]"])
        .tested(&["Multiple key/value pairs commit together"])
        .not_tested(&["Cross-client atomic visibility"])
        .case(CaseDef::new(
            "sets all key value pairs",
            "MSET should store all supplied pairs.",
            sets_multiple_pairs_atomically,
        ))
}
