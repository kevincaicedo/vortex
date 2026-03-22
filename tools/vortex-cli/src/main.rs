//! TODO: VortexDB interactive CLI — Phase 2 implements full REPL.

use clap::Parser;

#[derive(Parser)]
#[command(name = "vortex-cli", about = "VortexDB interactive CLI client")]
struct Cli {
    /// Server hostname
    #[arg(short = 'h', long, default_value = "127.0.0.1")]
    host: String,

    /// Server port
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    /// Password for AUTH
    #[arg(short, long)]
    auth: Option<String>,
}

fn main() {
    let cli = Cli::parse();
    eprintln!(
        "vortex-cli — connecting to {}:{} (REPL not yet implemented)",
        cli.host, cli.port,
    );
    // TODO: Phase 2: rustyline REPL, RESP framing over TCP.
}
