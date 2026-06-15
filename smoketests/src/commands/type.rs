use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn string_key_reports_string(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("name", "vortex")?;
    let kind: String = ctx.exec(&["TYPE", "name"])?;
    assert_eq!(kind, "string");
    Ok(())
}

fn missing_key_reports_none(ctx: &mut SmokeContext) -> Result<()> {
    let kind: String = ctx.exec(&["TYPE", "missing"])?;
    assert_eq!(kind, "none");
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("TYPE", CommandGroup::Key, SupportLevel::Supported)
        .summary("Returns the logical Redis type of a key.")
        .syntax(&["TYPE key"])
        .tested(&["String keys report string", "Missing keys report none"])
        .not_tested(&["Additional Redis data types beyond strings"])
        .case(CaseDef::new(
            "string key reports string",
            "TYPE should report `string` for the currently supported value type.",
            string_key_reports_string,
        ))
        .case(CaseDef::new(
            "missing key reports none",
            "TYPE should report `none` for a missing key.",
            missing_key_reports_none,
        ))
}
