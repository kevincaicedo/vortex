use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn quit_returns_ok_before_close(ctx: &mut SmokeContext) -> Result<()> {
    let reply: String = ctx.exec(&["QUIT"])?;
    assert_eq!(reply, "OK");
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("QUIT", CommandGroup::Server, SupportLevel::Supported)
        .summary("Returns OK and closes the connection.")
        .syntax(&["QUIT"])
        .tested(&["OK reply before connection close"])
        .not_tested(&["Client-side reconnect behavior after close"])
        .case(CaseDef::new(
            "quit returns ok",
            "QUIT should acknowledge the request before the socket closes.",
            quit_returns_ok_before_close,
        ))
}
