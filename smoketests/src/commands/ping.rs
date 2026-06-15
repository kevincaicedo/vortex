use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn ping_without_payload_returns_pong(ctx: &mut SmokeContext) -> Result<()> {
    let reply: String = ctx.exec(&["PING"])?;
    assert_eq!(reply, "PONG");
    Ok(())
}

fn ping_with_payload_echoes_payload(ctx: &mut SmokeContext) -> Result<()> {
    let reply: String = ctx.exec(&["PING", "hello"])?;
    assert_eq!(reply, "hello");
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("PING", CommandGroup::Server, SupportLevel::Supported)
        .summary("Replies with PONG or echoes the supplied message.")
        .syntax(&["PING", "PING message"])
        .tested(&["No-arg PONG", "Payload echo"])
        .not_tested(&[])
        .case(CaseDef::new(
            "ping returns pong",
            "PING should return PONG when called without an argument.",
            ping_without_payload_returns_pong,
        ))
        .case(CaseDef::new(
            "ping echoes payload",
            "PING should echo the payload when called with one argument.",
            ping_with_payload_echoes_payload,
        ))
}
