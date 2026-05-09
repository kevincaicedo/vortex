use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Instant;

use vortex_engine::ConcurrentKeyspace;
use vortex_engine::keyspace::AofLsn;
use vortex_persist::aof::{
    AOF_HEADER_SIZE, AofFileWriter, AofFsyncPolicy, AofReactorId, AofReader, AofRecordBytes,
    AofReplayConfig,
};

struct Options {
    records: u64,
    files: usize,
    value_size: usize,
    read_chunk_bytes: usize,
    max_record_bytes: usize,
    dir: PathBuf,
    keep: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            records: 1_000_000,
            files: 4,
            value_size: 64,
            read_chunk_bytes: 64 * 1024,
            max_record_bytes: 1024 * 1024,
            dir: PathBuf::from(".artifacts/profiling/aof-replay-memory-probe"),
            keep: false,
        }
    }
}

fn parse_u64_arg(name: &str, value: Option<String>) -> io::Result<u64> {
    let value = value.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("missing value for {name}"),
        )
    })?;
    value.parse::<u64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {name} value {value:?}: {error}"),
        )
    })
}

fn parse_usize_arg(name: &str, value: Option<String>) -> io::Result<usize> {
    let value = value.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("missing value for {name}"),
        )
    })?;
    value.parse::<usize>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {name} value {value:?}: {error}"),
        )
    })
}

fn parse_options() -> io::Result<Options> {
    let mut options = Options::default();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--records" => options.records = parse_u64_arg("--records", args.next())?,
            "--files" => options.files = parse_usize_arg("--files", args.next())?,
            "--value-size" => options.value_size = parse_usize_arg("--value-size", args.next())?,
            "--read-chunk" => {
                options.read_chunk_bytes = parse_usize_arg("--read-chunk", args.next())?;
            }
            "--max-record" => {
                options.max_record_bytes = parse_usize_arg("--max-record", args.next())?;
            }
            "--dir" => {
                options.dir = PathBuf::from(args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --dir")
                })?);
            }
            "--keep" => options.keep = true,
            "--help" | "-h" => {
                println!(
                    "usage: aof_replay_memory_probe [--records N] [--files N] [--value-size N] \\
                     [--read-chunk N] [--max-record N] [--dir PATH] [--keep]"
                );
                std::process::exit(0);
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown argument {other:?}"),
                ));
            }
        }
    }

    if options.files == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--files must be greater than zero",
        ));
    }
    if options.files > AofReactorId::MAX {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("--files exceeds AOF reactor id range {}", AofReactorId::MAX),
        ));
    }
    if options.records == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--records must be greater than zero",
        ));
    }

    Ok(options)
}

fn make_set_resp(buf: &mut Vec<u8>, key: &[u8], value: &[u8]) -> io::Result<()> {
    buf.clear();
    write!(buf, "*3\r\n$3\r\nSET\r\n${}\r\n", key.len())?;
    buf.extend_from_slice(key);
    write!(buf, "\r\n${}\r\n", value.len())?;
    buf.extend_from_slice(value);
    buf.extend_from_slice(b"\r\n");
    Ok(())
}

fn make_key(buf: &mut Vec<u8>, lsn: u64) -> io::Result<()> {
    buf.clear();
    write!(buf, "alpha:{lsn:010}")?;
    Ok(())
}

fn write_probe_files(options: &Options) -> io::Result<Vec<PathBuf>> {
    fs::create_dir_all(&options.dir)?;
    let mut paths = Vec::with_capacity(options.files);
    let value = vec![b'x'; options.value_size];
    let mut key = Vec::with_capacity(32);
    let mut resp = Vec::with_capacity(options.value_size + 96);

    for file_idx in 0..options.files {
        let path = options.dir.join(format!("replay-r{file_idx}.aof"));
        if path.exists() {
            fs::remove_file(&path)?;
        }

        let reactor_id = AofReactorId::try_from_usize(file_idx)?;
        let mut writer = AofFileWriter::open(&path, reactor_id, AofFsyncPolicy::No)?;
        let mut lsn = file_idx as u64 + 1;
        while lsn <= options.records {
            make_key(&mut key, lsn)?;
            make_set_resp(&mut resp, &key, &value)?;
            let aof_lsn = AofLsn::try_from_raw(lsn).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "record LSN {} exceeds maximum {}",
                        error.attempted, error.max
                    ),
                )
            })?;
            writer.append_with_lsn(aof_lsn, AofRecordBytes::from_resp(&resp))?;
            lsn = lsn.saturating_add(options.files as u64);
        }
        writer.flush_buffer()?;
        paths.push(path);
    }

    Ok(paths)
}

fn total_data_bytes(paths: &[PathBuf]) -> io::Result<u64> {
    paths.iter().try_fold(0u64, |total, path| {
        let len = fs::metadata(path)?.len();
        Ok(total.saturating_add(len.saturating_sub(AOF_HEADER_SIZE as u64)))
    })
}

fn main() -> io::Result<()> {
    let options = parse_options()?;
    let setup_start = Instant::now();
    let paths = write_probe_files(&options)?;
    let data_bytes = total_data_bytes(&paths)?;
    let setup_ms = setup_start.elapsed().as_millis();

    let config = AofReplayConfig {
        read_chunk_bytes: options.read_chunk_bytes,
        max_record_bytes: options.max_record_bytes,
    };
    let keyspace = ConcurrentKeyspace::new(64);
    let replay_start = Instant::now();
    let stats = AofReader::replay_merge_with_config(&paths, &keyspace, config)?;
    let replay_ms = replay_start.elapsed().as_millis();

    assert_eq!(stats.commands_replayed, options.records);
    assert_eq!(stats.max_persisted_lsn, options.records);
    assert_eq!(keyspace.current_lsn(), options.records + 1);

    println!("records={}", options.records);
    println!("files={}", options.files);
    println!("value_size={}", options.value_size);
    println!("read_chunk_bytes={}", options.read_chunk_bytes);
    println!("max_record_bytes={}", options.max_record_bytes);
    println!("total_aof_data_bytes={data_bytes}");
    println!("setup_ms={setup_ms}");
    println!("replay_ms={replay_ms}");
    println!("commands_replayed={}", stats.commands_replayed);
    println!("bytes_read={}", stats.bytes_read);
    println!("bytes_truncated={}", stats.bytes_truncated);
    println!("files_merged={}", stats.files_merged);
    println!(
        "peak_replay_buffer_bytes={}",
        stats.peak_replay_buffer_bytes
    );
    println!("max_persisted_lsn={}", stats.max_persisted_lsn);
    println!("corrupt_records={}", stats.corrupt_records);

    if !options.keep {
        for path in paths {
            let _ = fs::remove_file(path);
        }
    }

    Ok(())
}
