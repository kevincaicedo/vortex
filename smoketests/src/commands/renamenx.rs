use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn destination_missing_allows_rename(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("old", "value")?;
    let renamed: i64 = ctx.exec(&["RENAMENX", "old", "new"])?;
    assert_eq!(renamed, 1);
    assert_eq!(ctx.get("new")?, Some("value".to_string()));
    Ok(())
}

fn destination_existing_blocks_rename(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("old", "value")?;
    ctx.set("new", "taken")?;
    let renamed: i64 = ctx.exec(&["RENAMENX", "old", "new"])?;
    assert_eq!(renamed, 0);
    assert_eq!(ctx.get("old")?, Some("value".to_string()));
    assert_eq!(ctx.get("new")?, Some("taken".to_string()));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("RENAMENX", CommandGroup::Key, SupportLevel::Supported)
        .summary("Renames a key only when the destination does not already exist.")
        .syntax(&["RENAMENX key newkey"])
        .tested(&["Destination-missing success", "Destination-existing no-op"])
        .not_tested(&[])
        .case(CaseDef::new(
            "destination missing allows rename",
            "RENAMENX should move the key when the destination is absent.",
            destination_missing_allows_rename,
        ))
        .case(CaseDef::new(
            "destination existing blocks rename",
            "RENAMENX should leave both keys untouched when the destination exists.",
            destination_existing_blocks_rename,
        ))
}
