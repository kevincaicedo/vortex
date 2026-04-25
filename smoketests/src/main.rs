use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use vortex_smoketests::commands;
use vortex_smoketests::runner::{RunOptions, Selection, run, selected_specs};
use vortex_smoketests::server::{SpawnOptions, spawn_redis, spawn_vortex};

#[derive(Parser, Debug)]
#[command(name = "vortex-smoketests")]
#[command(about = "Client-driven smoke tests for VortexDB command compatibility")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Run(RunArgs),
    List(ListArgs),
}

#[derive(Args, Debug, Clone)]
struct SelectionArgs {
    #[arg(long = "command", value_delimiter = ',')]
    commands: Vec<String>,
    #[arg(long = "group", value_delimiter = ',')]
    groups: Vec<String>,
    #[arg(long, default_value_t = true)]
    include_stubbed: bool,
}

impl SelectionArgs {
    fn selection(&self) -> Selection {
        Selection {
            commands: self.commands.clone(),
            groups: self.groups.clone(),
            include_stubbed: self.include_stubbed,
        }
    }
}

#[derive(Args, Debug)]
struct RunArgs {
    #[command(flatten)]
    selection: SelectionArgs,
    #[arg(long, default_value = "redis://127.0.0.1:6379/")]
    server_url: String,
    #[arg(long)]
    baseline_url: Option<String>,
    #[arg(long)]
    spawn_vortex: bool,
    #[arg(long)]
    spawn_redis_baseline: bool,
    #[arg(long)]
    bind: Option<String>,
    #[arg(long)]
    vortex_bin: Option<PathBuf>,
    #[arg(long = "vortex-arg")]
    vortex_args: Vec<String>,
    #[arg(long)]
    redis_bin: Option<PathBuf>,
    #[arg(long = "redis-arg")]
    redis_args: Vec<String>,
    #[arg(long)]
    fail_fast: bool,
    #[arg(long, default_value_t = 1)]
    repeat: usize,
    #[arg(long)]
    report: Option<PathBuf>,
}

#[derive(Args, Debug)]
struct ListArgs {
    #[command(flatten)]
    selection: SelectionArgs,
    #[arg(long)]
    verbose: bool,
}

fn main() -> ExitCode {
    match run_cli() {
        Ok(code) => code,
        Err(err) => {
            eprintln!("error: {err:#}");
            ExitCode::from(1)
        }
    }
}

fn run_cli() -> Result<ExitCode> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Run(args) => run_cmd(args),
        Commands::List(args) => list_cmd(args),
    }
}

fn run_cmd(args: RunArgs) -> Result<ExitCode> {
    let selection = args.selection.selection();
    let mut vortex_args = args.vortex_args.clone();
    let has_threads_arg = vortex_args
        .iter()
        .any(|arg| arg == "--threads" || arg.starts_with("--threads="));
    if args.spawn_vortex && !has_threads_arg {
        vortex_args.push("--threads".to_string());
        vortex_args.push("1".to_string());
    }

    let report_path = args
        .report
        .unwrap_or_else(|| PathBuf::from("smoketests/.artifacts/last-run.md"));

    let mut spawned = None;
    let mut baseline_spawned = None;
    let server_url = if args.spawn_vortex {
        let server = spawn_vortex(&SpawnOptions {
            bind: args.bind.clone(),
            vortex_bin: args.vortex_bin.clone(),
            vortex_args,
            ready_timeout: Duration::from_secs(20),
        })?;
        println!(
            "spawned vortex-server at {} (log: {})",
            server.url(),
            server.log_path().display()
        );
        let url = server.url().to_string();
        spawned = Some(server);
        url
    } else {
        args.server_url.clone()
    };

    let baseline_url = if args.spawn_redis_baseline {
        let baseline = spawn_redis(&SpawnOptions {
            bind: None,
            vortex_bin: args.redis_bin.clone(),
            vortex_args: args.redis_args.clone(),
            ready_timeout: Duration::from_secs(20),
        })?;
        println!(
            "spawned redis baseline at {} (log: {})",
            baseline.url(),
            baseline.log_path().display()
        );
        let url = baseline.url().to_string();
        baseline_spawned = Some(baseline);
        Some(url)
    } else {
        args.baseline_url.clone()
    };

    let summary = run(
        &server_url,
        &selection,
        &RunOptions {
            fail_fast: args.fail_fast,
            repeat: args.repeat,
            baseline_url,
        },
    )?;
    summary.write_markdown(&report_path)?;
    println!(
        "summary: {} commands, {} cases, {} failures",
        summary.command_count, summary.case_count, summary.failed_cases
    );
    println!("report: {}", report_path.display());

    drop(baseline_spawned);
    drop(spawned);

    if summary.failed_cases > 0 {
        Ok(ExitCode::from(1))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

fn list_cmd(args: ListArgs) -> Result<ExitCode> {
    let specs = selected_specs(&args.selection.selection());
    for spec in specs {
        println!(
            "{}\t{}\t{}\t{} cases",
            spec.name,
            spec.group.as_str(),
            spec.support.as_str(),
            spec.cases.len()
        );
        if args.verbose {
            println!("  summary: {}", spec.summary);
            if !spec.syntax.is_empty() {
                println!("  syntax:");
                for syntax in spec.syntax {
                    println!("    - {}", syntax);
                }
            }
            if !spec.tested.is_empty() {
                println!("  tested:");
                for item in spec.tested {
                    println!("    - {}", item);
                }
            }
            if !spec.not_tested.is_empty() {
                println!("  not tested:");
                for item in spec.not_tested {
                    println!("    - {}", item);
                }
            }
        }
    }

    println!("total: {} commands", commands::all_specs().len());
    Ok(ExitCode::SUCCESS)
}
