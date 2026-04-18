use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn returns_info_sections(ctx: &mut SmokeContext) -> Result<()> {
    let info: String = ctx.exec(&["INFO"])?;
    assert!(info.contains("# Server"));
    assert!(info.contains("# Keyspace"));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("INFO", CommandGroup::Server, SupportLevel::Partial)
        .summary("Returns a reduced Redis-compatible INFO payload for server diagnostics.")
        .syntax(&["INFO [section]"])
        .tested(&["Default INFO output contains server and keyspace sections"])
        .not_tested(&["Full Redis INFO field parity"])
        .case(CaseDef::new(
            "returns key sections",
            "INFO should include the server and keyspace sections Vortex exposes.",
            returns_info_sections,
        ))
}
