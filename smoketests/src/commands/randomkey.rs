use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn empty_db_returns_nil(ctx: &mut SmokeContext) -> Result<()> {
    let key: Option<String> = ctx.exec(&["RANDOMKEY"])?;
    assert_eq!(key, None);
    Ok(())
}

fn non_empty_db_returns_existing_key(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("a", "1")?;
    ctx.set("b", "2")?;
    let key: Option<String> = ctx.exec(&["RANDOMKEY"])?;
    assert!(matches!(key.as_deref(), Some("a") | Some("b")));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("RANDOMKEY", CommandGroup::Key, SupportLevel::Supported)
        .summary("Returns a random existing key from the current database.")
        .syntax(&["RANDOMKEY"])
        .tested(&[
            "Empty DB returns nil",
            "Non-empty DB returns one existing key",
        ])
        .not_tested(&["Distribution quality"])
        .case(CaseDef::new(
            "empty db returns nil",
            "RANDOMKEY should return nil when the database is empty.",
            empty_db_returns_nil,
        ))
        .case(CaseDef::new(
            "non empty db returns existing key",
            "RANDOMKEY should return one of the keys currently present.",
            non_empty_db_returns_existing_key,
        ))
}
