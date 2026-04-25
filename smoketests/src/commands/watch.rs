use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn watch_aborts_on_conflict(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("watched:key", "v1")?;
    ctx.assert_ok(&["WATCH", "watched:key"])?;

    let mut other = SmokeContext::connect(ctx.server_url())?;
    other.set("watched:key", "v2")?;

    ctx.assert_ok(&["MULTI"])?;
    let queued: String = ctx.exec(&["SET", "watched:key", "v3"])?;
    assert_eq!(queued, "QUEUED");
    let replies: Option<Vec<redis::Value>> = ctx.exec(&["EXEC"])?;
    assert!(replies.is_none());
    assert_eq!(ctx.get("watched:key")?, Some("v2".to_string()));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("WATCH", CommandGroup::Server, SupportLevel::Supported)
        .summary("Watches keys and aborts EXEC when another client modifies them.")
        .syntax(&["WATCH key [key ...]"])
        .tested(&["Optimistic conflict abort"])
        .not_tested(&["Multi-key watch conflict matrices"])
        .case(CaseDef::new(
            "watch aborts on conflict",
            "WATCH should make EXEC return a nil array after an external write.",
            watch_aborts_on_conflict,
        ))
}
