use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn select_zero_is_allowed(ctx: &mut SmokeContext) -> Result<()> {
    let reply: String = ctx.exec(&["SELECT", "0"])?;
    assert_eq!(reply, "OK");
    Ok(())
}

fn non_zero_db_is_rejected(ctx: &mut SmokeContext) -> Result<()> {
    let err = ctx.exec_error(&["SELECT", "1"])?;
    assert!(
        err.to_string()
            .to_ascii_lowercase()
            .contains("db index is out of range")
    );
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("SELECT", CommandGroup::Server, SupportLevel::Partial)
        .summary("Supports only DB 0 in the current single-database Vortex architecture.")
        .syntax(&["SELECT index"])
        .tested(&["DB 0 success path", "Non-zero DB rejection"])
        .not_tested(&["Multiple logical databases because they are not implemented"])
        .case(CaseDef::new(
            "db zero is allowed",
            "SELECT 0 should succeed.",
            select_zero_is_allowed,
        ))
        .case(CaseDef::new(
            "non zero db is rejected",
            "SELECT should reject database indexes other than zero.",
            non_zero_db_is_rejected,
        ))
}
