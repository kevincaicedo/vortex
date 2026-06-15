use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn echoes_payload(ctx: &mut SmokeContext) -> Result<()> {
    let reply: String = ctx.exec(&["ECHO", "hello"])?;
    assert_eq!(reply, "hello");
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("ECHO", CommandGroup::Server, SupportLevel::Supported)
        .summary("Returns the supplied bulk-string payload unchanged.")
        .syntax(&["ECHO message"])
        .tested(&["Payload round-trip"])
        .not_tested(&[])
        .case(CaseDef::new(
            "echoes payload",
            "ECHO should return the exact string that was sent.",
            echoes_payload,
        ))
}
