use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn removes_existing_keys_and_counts_hits(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("a", "1")?;
    ctx.set("b", "2")?;
    let removed: i64 = ctx.exec(&["UNLINK", "a", "b", "missing"])?;
    assert_eq!(removed, 2);
    assert_eq!(ctx.dbsize()?, 0);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("UNLINK", CommandGroup::Key, SupportLevel::Supported)
        .summary("Deletes one or more keys using the current synchronous implementation.")
        .syntax(&["UNLINK key [key ...]"])
        .tested(&["Delete count semantics"])
        .not_tested(&["Asynchronous free semantics because Vortex currently aliases DEL"])
        .case(CaseDef::new(
            "counts removed keys",
            "UNLINK should count only keys that existed.",
            removes_existing_keys_and_counts_hits,
        ))
}
