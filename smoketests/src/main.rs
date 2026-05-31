use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;
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
#[allow(clippy::large_enum_variant)]
enum Commands {
    Run(RunArgs),
    List(ListArgs),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum TargetMode {
    Local,
    HostPort,
    SshManaged,
    SshAttach,
}

impl TargetMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::HostPort => "host-port",
            Self::SshManaged => "ssh-managed",
            Self::SshAttach => "ssh-attach",
        }
    }

    const fn is_ssh(self) -> bool {
        matches!(self, Self::SshManaged | Self::SshAttach)
    }
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
    #[arg(long)]
    artifact_root: Option<PathBuf>,
    #[arg(long, value_enum)]
    target_mode: Option<TargetMode>,
    #[arg(long)]
    ssh_target: Option<String>,
    #[arg(long)]
    ssh_start_command: Option<String>,
    #[arg(long)]
    ssh_stop_command: Option<String>,
    #[arg(long)]
    ssh_log_path: Option<String>,
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
    let target_mode = infer_target_mode(&args);
    validate_target_mode(&args, target_mode)?;
    let workspace_root = workspace_root();
    let started_at_ms = unix_millis();
    let session_dir =
        smoke_session_dir(&workspace_root, args.artifact_root.as_deref(), target_mode)?;
    let logs_dir = session_dir.join("logs");
    fs::create_dir_all(&logs_dir)
        .with_context(|| format!("failed to create {}", logs_dir.display()))?;

    let mut vortex_args = args.vortex_args.clone();
    let has_threads_arg = vortex_args
        .iter()
        .any(|arg| arg == "--threads" || arg.starts_with("--threads="));
    if args.spawn_vortex && !has_threads_arg {
        vortex_args.push("--threads".to_string());
        vortex_args.push("1".to_string());
    }

    write_environment(&session_dir.join("environment.json"))?;
    prepare_ssh_target(&args, target_mode, &logs_dir)?;

    let mut spawned = None;
    let mut baseline_spawned = None;
    let mut server_log_path = None;
    let mut baseline_log_path = None;
    let server_url = if args.spawn_vortex {
        let server = spawn_vortex(&SpawnOptions {
            bind: args.bind.clone(),
            vortex_bin: args.vortex_bin.clone(),
            vortex_args,
            ready_timeout: Duration::from_secs(20),
            log_dir: Some(logs_dir.clone()),
        })?;
        println!(
            "spawned vortex-server at {} (log: {})",
            server.url(),
            server.log_path().display()
        );
        let url = server.url().to_string();
        server_log_path = Some(server.log_path().to_path_buf());
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
            log_dir: Some(logs_dir.clone()),
        })?;
        println!(
            "spawned redis baseline at {} (log: {})",
            baseline.url(),
            baseline.log_path().display()
        );
        let url = baseline.url().to_string();
        baseline_log_path = Some(baseline.log_path().to_path_buf());
        baseline_spawned = Some(baseline);
        Some(url)
    } else {
        args.baseline_url.clone()
    };

    capture_runtime_config(&server_url, &session_dir.join("runtime-config.txt"))?;

    let summary = run(
        &server_url,
        &selection,
        &RunOptions {
            fail_fast: args.fail_fast,
            repeat: args.repeat,
            baseline_url,
        },
    )?;
    let report_path = session_dir.join("report.md");
    let report_json_path = session_dir.join("report.json");
    let client_log_path = logs_dir.join("client.log");
    let reproducer_path = session_dir.join("reproducers.md");
    summary.write_markdown(&report_path)?;
    summary.write_json(&report_json_path)?;
    summary.write_client_log(&client_log_path)?;
    summary.write_reproducers(&reproducer_path)?;
    if let Some(compat_report_path) = &args.report {
        if compat_report_path != &report_path {
            summary.write_markdown(compat_report_path)?;
        }
    }
    write_session_json(&SessionRecord {
        tool: "vortex-smoketests",
        version: env!("CARGO_PKG_VERSION"),
        target_mode: target_mode.as_str(),
        managed_target: matches!(target_mode, TargetMode::Local | TargetMode::SshManaged),
        target_url: &server_url,
        baseline_url: summary.baseline_url.as_deref(),
        command_line: &std::env::args().collect::<Vec<_>>().join(" "),
        cwd: &std::env::current_dir()
            .context("failed to read current directory")?
            .display()
            .to_string(),
        artifact_dir: &session_dir.display().to_string(),
        started_at_unix_ms: started_at_ms,
        ended_at_unix_ms: unix_millis(),
        exit_code: if summary.failed_cases > 0 { 1 } else { 0 },
        vortex_binary: args
            .vortex_bin
            .as_ref()
            .map(|path| path.display().to_string()),
        git_revision: git_output(&workspace_root, &["rev-parse", "HEAD"]).ok(),
        git_dirty: git_dirty(&workspace_root).unwrap_or(true),
        report_md: &report_path.display().to_string(),
        report_json: &report_json_path.display().to_string(),
        session_json: &session_dir.join("session.json").display().to_string(),
        client_log: &client_log_path.display().to_string(),
        server_log: server_log_path
            .as_ref()
            .map(|path| path.display().to_string()),
        baseline_log: baseline_log_path
            .as_ref()
            .map(|path| path.display().to_string()),
        environment_json: &session_dir.join("environment.json").display().to_string(),
        runtime_config: &session_dir.join("runtime-config.txt").display().to_string(),
        reproducers: &reproducer_path.display().to_string(),
        command_count: summary.command_count,
        case_count: summary.case_count,
        failed_cases: summary.failed_cases,
    })?;
    refresh_latest_reports(&session_dir, target_mode)?;
    println!(
        "summary: {} commands, {} cases, {} failures",
        summary.command_count, summary.case_count, summary.failed_cases
    );
    println!("report: {}", report_path.display());
    println!("artifacts: {}", session_dir.display());

    drop(baseline_spawned);
    drop(spawned);
    finish_ssh_target(&args, target_mode, &logs_dir)?;

    if summary.failed_cases > 0 {
        Ok(ExitCode::from(1))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

#[derive(Serialize)]
struct SessionRecord<'a> {
    tool: &'a str,
    version: &'a str,
    target_mode: &'a str,
    managed_target: bool,
    target_url: &'a str,
    baseline_url: Option<&'a str>,
    command_line: &'a str,
    cwd: &'a str,
    artifact_dir: &'a str,
    started_at_unix_ms: u128,
    ended_at_unix_ms: u128,
    exit_code: i32,
    vortex_binary: Option<String>,
    git_revision: Option<String>,
    git_dirty: bool,
    report_md: &'a str,
    report_json: &'a str,
    session_json: &'a str,
    client_log: &'a str,
    server_log: Option<String>,
    baseline_log: Option<String>,
    environment_json: &'a str,
    runtime_config: &'a str,
    reproducers: &'a str,
    command_count: usize,
    case_count: usize,
    failed_cases: usize,
}

fn infer_target_mode(args: &RunArgs) -> TargetMode {
    args.target_mode.unwrap_or(if args.spawn_vortex {
        TargetMode::Local
    } else {
        TargetMode::HostPort
    })
}

fn validate_target_mode(args: &RunArgs, target_mode: TargetMode) -> Result<()> {
    match target_mode {
        TargetMode::Local => {
            if !args.spawn_vortex {
                bail!("--target-mode local requires --spawn-vortex");
            }
        }
        TargetMode::HostPort => {
            if args.spawn_vortex {
                bail!(
                    "--target-mode host-port attaches to an external server and cannot use --spawn-vortex"
                );
            }
        }
        TargetMode::SshManaged => {
            if args.spawn_vortex {
                bail!("--target-mode ssh-managed uses a remote start command, not --spawn-vortex");
            }
            if args.ssh_target.is_none() {
                bail!("--target-mode ssh-managed requires --ssh-target");
            }
            if args.ssh_start_command.is_none() {
                bail!("--target-mode ssh-managed requires --ssh-start-command");
            }
        }
        TargetMode::SshAttach => {
            if args.spawn_vortex {
                bail!(
                    "--target-mode ssh-attach attaches to a remote server and cannot use --spawn-vortex"
                );
            }
            if args.ssh_target.is_none() {
                bail!("--target-mode ssh-attach requires --ssh-target");
            }
        }
    }
    Ok(())
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("smoketests lives under workspace root")
        .to_path_buf()
}

fn smoke_session_dir(
    workspace_root: &Path,
    artifact_root: Option<&Path>,
    target_mode: TargetMode,
) -> Result<PathBuf> {
    let root = artifact_root
        .map(Path::to_path_buf)
        .unwrap_or_else(|| workspace_root.join(".artifacts/smoke"));
    let session_dir = root.join(target_mode.as_str()).join(timestamp());
    fs::create_dir_all(&session_dir)
        .with_context(|| format!("failed to create {}", session_dir.display()))?;
    Ok(session_dir)
}

fn timestamp() -> String {
    let output = Command::new("date")
        .arg("-u")
        .arg("+%Y%m%d-%H%M%S")
        .output();
    if let Ok(output) = output {
        if output.status.success() {
            let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !value.is_empty() {
                return value;
            }
        }
    }
    format!("session-{}", unix_millis())
}

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn write_environment(path: &Path) -> Result<()> {
    let mut env_vars: Vec<_> = std::env::vars()
        .filter(|(key, _)| key.starts_with("VORTEX_") || key.starts_with("RUST_"))
        .map(|(key, value)| {
            let redacted = key.contains("PASS")
                || key.contains("PASSWORD")
                || key.contains("TOKEN")
                || key.contains("SECRET")
                || key.contains("KEY");
            serde_json::json!({
                "name": key,
                "value": if redacted { "<redacted>".to_string() } else { value },
            })
        })
        .collect();
    env_vars.sort_by_key(|value| value["name"].as_str().unwrap_or("").to_string());
    let payload = serde_json::json!({
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "current_dir": std::env::current_dir()?.display().to_string(),
        "vars": env_vars,
    });
    fs::write(path, serde_json::to_string_pretty(&payload)?)?;
    Ok(())
}

fn capture_runtime_config(server_url: &str, path: &Path) -> Result<()> {
    let mut out = String::new();
    match redis::Client::open(server_url).and_then(|client| client.get_connection()) {
        Ok(mut connection) => {
            let info: redis::RedisResult<String> =
                redis::cmd("INFO").arg("runtime").query(&mut connection);
            match info {
                Ok(info) => {
                    out.push_str("# INFO runtime\n");
                    out.push_str(&info);
                    if !info.ends_with('\n') {
                        out.push('\n');
                    }
                }
                Err(err) => out.push_str(&format!("INFO runtime unavailable: {err}\n")),
            }
            let config: redis::RedisResult<Vec<String>> = redis::cmd("CONFIG")
                .arg("GET")
                .arg("*")
                .query(&mut connection);
            match config {
                Ok(values) => {
                    out.push_str("\n# CONFIG GET *\n");
                    for pair in values.chunks(2) {
                        if let [key, value] = pair {
                            out.push_str(key);
                            out.push('=');
                            out.push_str(value);
                            out.push('\n');
                        }
                    }
                }
                Err(err) => out.push_str(&format!("CONFIG GET * unavailable: {err}\n")),
            }
        }
        Err(err) => out.push_str(&format!("runtime config unavailable: {err}\n")),
    }
    fs::write(path, out)?;
    Ok(())
}

fn prepare_ssh_target(args: &RunArgs, target_mode: TargetMode, logs_dir: &Path) -> Result<()> {
    if !target_mode.is_ssh() {
        return Ok(());
    }
    let target = args.ssh_target.as_deref().expect("validated ssh target");
    run_ssh_capture(
        target,
        "uname -a; date -u +%Y-%m-%dT%H:%M:%SZ; pwd",
        &logs_dir.join("remote-metadata.txt"),
    )?;
    if target_mode == TargetMode::SshManaged {
        let command = args
            .ssh_start_command
            .as_deref()
            .expect("validated ssh start command");
        run_ssh_capture(target, command, &logs_dir.join("remote-start.log"))?;
    }
    Ok(())
}

fn finish_ssh_target(args: &RunArgs, target_mode: TargetMode, logs_dir: &Path) -> Result<()> {
    if !target_mode.is_ssh() {
        return Ok(());
    }
    let target = args.ssh_target.as_deref().expect("validated ssh target");
    if let Some(remote_log) = &args.ssh_log_path {
        run_ssh_capture(
            target,
            &format!("cat {}", sh_quote(remote_log)),
            &logs_dir.join("remote-server.log"),
        )?;
    }
    if target_mode == TargetMode::SshManaged {
        if let Some(command) = &args.ssh_stop_command {
            run_ssh_capture(target, command, &logs_dir.join("remote-stop.log"))?;
        }
    }
    Ok(())
}

fn run_ssh_capture(target: &str, remote_command: &str, output_path: &Path) -> Result<()> {
    let output = Command::new("ssh")
        .arg(target)
        .arg(remote_command)
        .output()
        .with_context(|| format!("failed to run ssh command on {target}"))?;
    let mut text = String::new();
    text.push_str("$ ssh ");
    text.push_str(target);
    text.push(' ');
    text.push_str(remote_command);
    text.push_str("\n\n# stdout\n");
    text.push_str(&String::from_utf8_lossy(&output.stdout));
    text.push_str("\n# stderr\n");
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    text.push_str(&format!(
        "\nexit_code={}\n",
        output.status.code().unwrap_or(-1)
    ));
    fs::write(output_path, text)?;
    if !output.status.success() {
        bail!(
            "ssh command failed for {target}; see {}",
            output_path.display()
        );
    }
    Ok(())
}

fn sh_quote(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ':'))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\"'\"'"))
    }
}

fn write_session_json(record: &SessionRecord<'_>) -> Result<()> {
    fs::write(record.session_json, serde_json::to_string_pretty(record)?)?;
    Ok(())
}

fn refresh_latest_reports(session_dir: &Path, target_mode: TargetMode) -> Result<()> {
    let Some(mode_dir) = session_dir.parent() else {
        return Ok(());
    };
    let latest = mode_dir.join("reports/latest");
    fs::create_dir_all(&latest)?;
    for name in ["report.md", "report.json", "session.json"] {
        let source = session_dir.join(name);
        if source.exists() {
            fs::copy(&source, latest.join(name)).with_context(|| {
                format!(
                    "failed to refresh latest {} smoke report {}",
                    target_mode.as_str(),
                    latest.join(name).display()
                )
            })?;
        }
    }
    Ok(())
}

fn git_output(workspace_root: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(workspace_root)
        .output()
        .context("failed to run git")?;
    if !output.status.success() {
        bail!("git {} failed", args.join(" "));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn git_dirty(workspace_root: &Path) -> Result<bool> {
    let output = Command::new("git")
        .arg("diff")
        .arg("--quiet")
        .current_dir(workspace_root)
        .output()
        .context("failed to run git diff --quiet")?;
    Ok(!output.status.success())
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
