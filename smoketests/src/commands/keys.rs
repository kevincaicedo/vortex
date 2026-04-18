use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn matches_glob_pattern(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("user:1", "a")?;
    ctx.set("user:2", "b")?;
    ctx.set("post:1", "c")?;

    let mut keys: Vec<String> = ctx.exec(&["KEYS", "user:*"])?;
    keys.sort();
    assert_eq!(keys, vec!["user:1".to_string(), "user:2".to_string()]);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("KEYS", CommandGroup::Key, SupportLevel::Supported)
        .summary("Returns keys matching a Redis glob pattern.")
        .syntax(&["KEYS pattern"])
        .tested(&["Basic wildcard matching"])
        .not_tested(&["Character classes and escaped glob special cases"])
        .case(CaseDef::new(
            "matches wildcard pattern",
            "KEYS should return only keys that match the supplied glob.",
            matches_glob_pattern,
        ))
}
