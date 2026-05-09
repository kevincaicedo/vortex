use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use vortex_engine::keyspace::AofLsn;
use vortex_persist::aof::{AofFileWriter, AofFsyncPolicy, AofReactorId, AofRecordBytes};
use vortex_smoketests::context::SmokeContext;
use vortex_smoketests::server::{SpawnOptions, spawn_vortex};

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(label: &str) -> Result<Self> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "vortex-smoketests-{label}-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&path)
            .with_context(|| format!("failed to create temp dir {}", path.display()))?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("smoketests lives under workspace root")
        .to_path_buf()
}

fn reserve_bind_addr() -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").context("failed to reserve local port")?;
    let port = listener
        .local_addr()
        .context("failed to read reserved local port")?
        .port();
    drop(listener);
    Ok(format!("127.0.0.1:{port}"))
}

fn sanitized_bind(bind: &str) -> String {
    bind.chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn vortex_log_path(bind: &str) -> PathBuf {
    workspace_root()
        .join("smoketests/.artifacts")
        .join(format!("vortex-server-{}.log", sanitized_bind(bind)))
}

fn shard_aof_path(base: &Path, reactor_id: usize) -> PathBuf {
    if reactor_id == 0 {
        return base.to_path_buf();
    }

    let stem = base.file_stem().unwrap_or_default().to_string_lossy();
    let ext = base.extension().unwrap_or_default().to_string_lossy();
    base.with_file_name(format!("{stem}-shard{reactor_id}.{ext}"))
}

fn write_record(path: &Path, reactor_id: u16, lsn: u64, payload: &[u8]) -> Result<()> {
    let mut writer =
        AofFileWriter::open(path, AofReactorId::from_u16(reactor_id), AofFsyncPolicy::No)
            .with_context(|| format!("failed to open fixture {}", path.display()))?;
    writer
        .append_with_lsn(
            AofLsn::try_from_raw(lsn).map_err(|error| {
                anyhow!(
                    "fixture LSN {} exceeds representable max {}",
                    error.attempted,
                    error.max
                )
            })?,
            AofRecordBytes::from_resp(payload),
        )
        .with_context(|| format!("failed to append fixture record to {}", path.display()))?;
    writer
        .flush_buffer()
        .with_context(|| format!("failed to flush fixture {}", path.display()))?;
    Ok(())
}

fn spawn_with_aof(bind: &str, aof_path: &Path) -> Result<vortex_smoketests::server::SpawnedServer> {
    spawn_with_aof_policy(bind, aof_path, "everysec", None)
}

fn spawn_with_aof_policy(
    bind: &str,
    aof_path: &Path,
    fsync: &str,
    max_pending_fsync_bytes: Option<u64>,
) -> Result<vortex_smoketests::server::SpawnedServer> {
    let mut vortex_args = vec![
        "--threads".to_string(),
        "2".to_string(),
        "--aof-enabled".to_string(),
        "--aof-fsync".to_string(),
        fsync.to_string(),
        "--aof-path".to_string(),
        aof_path.display().to_string(),
        "--max-clients".to_string(),
        "64".to_string(),
        "--fixed-buffers".to_string(),
        "128".to_string(),
    ];
    if let Some(bytes) = max_pending_fsync_bytes {
        vortex_args.push("--aof-max-pending-fsync-bytes".to_string());
        vortex_args.push(bytes.to_string());
    }

    spawn_vortex(&SpawnOptions {
        bind: Some(bind.to_string()),
        vortex_args,
        ready_timeout: Duration::from_secs(5),
        ..SpawnOptions::default()
    })
}

fn info_counter(info: &str, name: &str) -> Option<u64> {
    let prefix = format!("{name}:");
    info.lines()
        .find_map(|line| line.strip_prefix(&prefix)?.parse::<u64>().ok())
}

#[test]
fn real_server_replay_truncates_partial_merge_tail() -> Result<()> {
    let fixture = TestDir::new("merge-truncated")?;
    let base_path = fixture.path().join("appendonly.aof");
    let reactor0 = shard_aof_path(&base_path, 0);
    let reactor1 = shard_aof_path(&base_path, 1);

    write_record(
        &reactor0,
        0,
        1,
        b"*3\r\n$3\r\nSET\r\n$7\r\nmerge:a\r\n$1\r\n1\r\n",
    )?;
    write_record(
        &reactor1,
        1,
        2,
        b"*3\r\n$3\r\nSET\r\n$7\r\nmerge:b\r\n$1\r\n2\r\n",
    )?;

    let valid_len = fs::metadata(&reactor1)
        .with_context(|| format!("failed to stat {}", reactor1.display()))?
        .len();
    let mut file = OpenOptions::new()
        .append(true)
        .open(&reactor1)
        .with_context(|| format!("failed to reopen {}", reactor1.display()))?;
    file.write_all(b"\x03\x00\x00\x00\x00\x00\x00\x00*3\r\n$3\r\nSET\r\n$7\r\nmerge:c\r\n$1\r\n")
        .with_context(|| format!("failed to append truncated tail to {}", reactor1.display()))?;
    let corrupted_len = fs::metadata(&reactor1)
        .with_context(|| format!("failed to stat {}", reactor1.display()))?
        .len();
    assert!(corrupted_len > valid_len);

    let bind = reserve_bind_addr()?;
    let server = spawn_with_aof(&bind, &base_path)?;

    assert_eq!(
        fs::metadata(&reactor1)
            .with_context(|| format!("failed to stat {}", reactor1.display()))?
            .len(),
        valid_len
    );

    let mut ctx = SmokeContext::connect(server.url())?;
    assert_eq!(ctx.get("merge:a")?, Some("1".to_string()));
    assert_eq!(ctx.get("merge:b")?, Some("2".to_string()));

    drop(server);
    Ok(())
}

#[test]
fn real_server_replay_rejects_mid_file_merge_corruption() -> Result<()> {
    let fixture = TestDir::new("merge-corrupt")?;
    let base_path = fixture.path().join("appendonly.aof");
    let reactor0 = shard_aof_path(&base_path, 0);
    let reactor1 = shard_aof_path(&base_path, 1);

    write_record(
        &reactor0,
        0,
        1,
        b"*3\r\n$3\r\nSET\r\n$7\r\nmerge:a\r\n$1\r\n1\r\n",
    )?;
    write_record(
        &reactor1,
        1,
        2,
        b"*3\r\n$3\r\nSET\r\n$7\r\nmerge:b\r\n$1\r\n2\r\n",
    )?;

    let mut file = OpenOptions::new()
        .append(true)
        .open(&reactor1)
        .with_context(|| format!("failed to reopen {}", reactor1.display()))?;
    file.write_all(&3u64.to_le_bytes())
        .with_context(|| format!("failed to append lsn to {}", reactor1.display()))?;
    file.write_all(b"!broken\r\n")
        .with_context(|| format!("failed to append corruption to {}", reactor1.display()))?;
    file.write_all(&4u64.to_le_bytes())
        .with_context(|| format!("failed to append trailing lsn to {}", reactor1.display()))?;
    file.write_all(b"*3\r\n$3\r\nSET\r\n$7\r\nmerge:c\r\n$1\r\n3\r\n")
        .with_context(|| format!("failed to append trailing record to {}", reactor1.display()))?;

    let bind = reserve_bind_addr()?;
    let log_path = vortex_log_path(&bind);
    let error = match spawn_with_aof(&bind, &base_path) {
        Ok(server) => {
            drop(server);
            panic!("corrupted merge input should fail startup");
        }
        Err(error) => error,
    };
    let message = format!("{error:#}");
    assert!(
        message.contains("exited before becoming ready"),
        "unexpected startup failure: {message}"
    );

    let log = fs::read_to_string(&log_path)
        .with_context(|| format!("failed to read startup log {}", log_path.display()))?;
    assert!(log.contains("failed to spawn reactor pool"));
    assert!(
        log.contains("lsn=3"),
        "startup log missing corrupt LSN: {log}"
    );
    assert!(
        log.contains(
            reactor1
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .as_ref()
        ),
        "startup log missing corrupt file path: {log}"
    );

    Ok(())
}

#[test]
fn real_server_aof_always_replays_smoke_workload() -> Result<()> {
    let fixture = TestDir::new("aof-always-smoke")?;
    let base_path = fixture.path().join("appendonly.aof");

    let bind = reserve_bind_addr()?;
    {
        let server = spawn_with_aof_policy(&bind, &base_path, "always", None)?;
        let mut ctx = SmokeContext::connect(server.url())?;

        ctx.set("aof:smoke:string", "value-1")?;
        let counter: i64 = ctx.exec(&["INCR", "aof:smoke:counter"])?;
        assert_eq!(counter, 1);
        let counter: i64 = ctx.exec(&["INCR", "aof:smoke:counter"])?;
        assert_eq!(counter, 2);

        assert_eq!(ctx.get("aof:smoke:string")?, Some("value-1".to_string()));
        assert_eq!(ctx.get("aof:smoke:counter")?, Some("2".to_string()));
        drop(ctx);
        drop(server);
    }

    let restart_bind = reserve_bind_addr()?;
    let server = spawn_with_aof_policy(&restart_bind, &base_path, "always", None)?;
    let mut ctx = SmokeContext::connect(server.url())?;
    assert_eq!(ctx.get("aof:smoke:string")?, Some("value-1".to_string()));
    assert_eq!(ctx.get("aof:smoke:counter")?, Some("2".to_string()));

    drop(server);
    Ok(())
}

#[test]
fn real_server_aof_everysec_backpressure_smoke_reports_telemetry() -> Result<()> {
    let fixture = TestDir::new("aof-everysec-backpressure")?;
    let base_path = fixture.path().join("appendonly.aof");
    let bind = reserve_bind_addr()?;
    let server = spawn_with_aof_policy(&bind, &base_path, "everysec", Some(1))?;
    let mut ctx = SmokeContext::connect(server.url())?;

    ctx.set("aof:pressure", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")?;
    let info: String = ctx.exec(&["INFO"])?;
    let events = info_counter(&info, "reactor_aof_backpressure_events")
        .context("INFO persistence missing reactor_aof_backpressure_events")?;
    assert!(
        events > 0,
        "expected AOF backpressure telemetry after low pending-byte limit; INFO: {info}"
    );

    drop(server);
    Ok(())
}
