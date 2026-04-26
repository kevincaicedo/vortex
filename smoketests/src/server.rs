use std::fs::{self, File};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

#[derive(Debug, Clone)]
pub struct SpawnOptions {
    pub bind: Option<String>,
    pub vortex_bin: Option<PathBuf>,
    pub vortex_args: Vec<String>,
    pub ready_timeout: Duration,
}

impl Default for SpawnOptions {
    fn default() -> Self {
        Self {
            bind: None,
            vortex_bin: None,
            vortex_args: Vec::new(),
            ready_timeout: Duration::from_secs(20),
        }
    }
}

pub struct SpawnedServer {
    child: Child,
    url: String,
    log_path: PathBuf,
}

impl SpawnedServer {
    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn log_path(&self) -> &Path {
        &self.log_path
    }
}

impl Drop for SpawnedServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn spawn_vortex(options: &SpawnOptions) -> Result<SpawnedServer> {
    let workspace_root = workspace_root();
    let bind = match &options.bind {
        Some(bind) => bind.clone(),
        None => free_bind_addr()?,
    };
    let server_url = format!("redis://{bind}/");

    let bin_path = match &options.vortex_bin {
        Some(path) => path.clone(),
        None => {
            let path = workspace_root.join("target/debug/vortex-server");
            ensure_vortex_binary(&workspace_root, &path)?;
            path
        }
    };

    let artifacts_dir = workspace_root.join("smoketests/.artifacts");
    fs::create_dir_all(&artifacts_dir)
        .with_context(|| format!("failed to create artifacts dir {}", artifacts_dir.display()))?;
    let log_path = log_path_for(&artifacts_dir, "vortex-server", &bind);
    let stdout = File::create(&log_path)
        .with_context(|| format!("failed to open {}", log_path.display()))?;
    let stderr = stdout
        .try_clone()
        .with_context(|| format!("failed to clone {}", log_path.display()))?;

    let mut command = Command::new(&bin_path);
    command
        .arg("--bind")
        .arg(&bind)
        .args(&options.vortex_args)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .current_dir(&workspace_root);

    let mut child = command
        .spawn()
        .with_context(|| format!("failed to spawn vortex-server from {}", bin_path.display()))?;

    wait_until_ready(&mut child, &server_url, options.ready_timeout).with_context(|| {
        format!(
            "vortex-server did not become ready on {server_url}; see {}",
            log_path.display()
        )
    })?;

    Ok(SpawnedServer {
        child,
        url: server_url,
        log_path,
    })
}

pub fn spawn_redis(options: &SpawnOptions) -> Result<SpawnedServer> {
    let workspace_root = workspace_root();
    let bind = match &options.bind {
        Some(bind) => bind.clone(),
        None => free_bind_addr()?,
    };
    let (host, port) = bind
        .split_once(':')
        .with_context(|| format!("invalid bind address for redis baseline: {bind}"))?;
    let server_url = format!("redis://{bind}/");
    let bin_path = options
        .vortex_bin
        .clone()
        .unwrap_or_else(|| PathBuf::from("redis-server"));

    let artifacts_dir = workspace_root.join("smoketests/.artifacts");
    fs::create_dir_all(&artifacts_dir)
        .with_context(|| format!("failed to create artifacts dir {}", artifacts_dir.display()))?;
    let log_path = log_path_for(&artifacts_dir, "redis-server", &bind);
    let stdout = File::create(&log_path)
        .with_context(|| format!("failed to open {}", log_path.display()))?;
    let stderr = stdout
        .try_clone()
        .with_context(|| format!("failed to clone {}", log_path.display()))?;

    let mut command = Command::new(&bin_path);
    command
        .arg("--bind")
        .arg(host)
        .arg("--port")
        .arg(port)
        .arg("--save")
        .arg("")
        .arg("--appendonly")
        .arg("no")
        .args(&options.vortex_args)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .current_dir(&workspace_root);

    let mut child = command.spawn().with_context(|| {
        format!(
            "failed to spawn redis-server baseline from {}",
            bin_path.display()
        )
    })?;

    wait_until_ready(&mut child, &server_url, options.ready_timeout).with_context(|| {
        format!(
            "redis-server baseline did not become ready on {server_url}; see {}",
            log_path.display()
        )
    })?;

    Ok(SpawnedServer {
        child,
        url: server_url,
        log_path,
    })
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("smoketests lives under workspace root")
        .to_path_buf()
}

fn log_path_for(artifacts_dir: &Path, label: &str, bind: &str) -> PathBuf {
    let sanitized_bind: String = bind
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect();
    artifacts_dir.join(format!("{label}-{sanitized_bind}.log"))
}

fn free_bind_addr() -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").context("failed to reserve local port")?;
    let port = listener
        .local_addr()
        .context("failed to read reserved local port")?
        .port();
    drop(listener);
    Ok(format!("127.0.0.1:{port}"))
}

fn ensure_vortex_binary(workspace_root: &Path, binary: &Path) -> Result<()> {
    let status = Command::new("cargo")
        .arg("build")
        .arg("-p")
        .arg("vortex-server")
        .arg("--bin")
        .arg("vortex-server")
        .current_dir(workspace_root)
        .status()
        .context("failed to run cargo build for vortex-server")?;

    if !status.success() {
        bail!("cargo build -p vortex-server failed");
    }

    if !binary.exists() {
        bail!(
            "expected vortex-server binary at {} after build",
            binary.display()
        );
    }

    Ok(())
}

fn wait_until_ready(child: &mut Child, server_url: &str, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .context("failed to poll smoke target process state")?
        {
            bail!("smoke target exited before becoming ready with status {status}");
        }

        if Instant::now() >= deadline {
            bail!("timed out waiting for smoke target {server_url}");
        }

        if let Ok(client) = redis::Client::open(server_url) {
            if let Ok(mut connection) = client.get_connection() {
                if let Ok(reply) = redis::cmd("PING").query::<String>(&mut connection) {
                    if reply == "PONG" {
                        return Ok(());
                    }
                }
            }
        }

        std::thread::sleep(Duration::from_millis(100));
    }
}
