use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn empty_db_is_zero(ctx: &mut SmokeContext) -> Result<()> {
    assert_eq!(ctx.dbsize()?, 0);
    Ok(())
}

fn counts_inserted_keys(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("a", "1")?;
    ctx.set("b", "2")?;
    ctx.set("c", "3")?;
    assert_eq!(ctx.dbsize()?, 3);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("DBSIZE", CommandGroup::Server, SupportLevel::Supported)
        .summary("Returns the number of keys stored in the current database.")
        .syntax(&["DBSIZE"])
        .tested(&["Empty database", "Counting inserted keys"])
        .not_tested(&["Large keyspaces under concurrent mutation"])
        .case(CaseDef::new(
            "empty db is zero",
            "DBSIZE should return zero on a clean database.",
            empty_db_is_zero,
        ))
        .case(CaseDef::new(
            "counts inserted keys",
            "DBSIZE should reflect the number of written keys.",
            counts_inserted_keys,
        ))
}
