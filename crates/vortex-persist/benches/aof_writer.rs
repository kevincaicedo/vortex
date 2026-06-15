use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use vortex_engine::keyspace::AofLsn;
use vortex_persist::aof::{AofFileWriter, AofFsyncPolicy, AofReactorId, AofRecordBytes};

const SET_RECORD: &[u8] = b"*3\r\n$3\r\nSET\r\n$7\r\nbench:k\r\n$16\r\n0123456789abcdef\r\n";

fn temp_aof_path(name: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let mut path = std::env::temp_dir();
    path.push(format!(
        "vortex-persist-bench-{name}-{}-{}.aof",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    path
}

fn bench_append_with_lsn_no_fsync(c: &mut Criterion) {
    c.bench_function("append_with_lsn_no_fsync_64b", |b| {
        b.iter_custom(|iterations| {
            let path = temp_aof_path("append-with-lsn");
            let mut writer =
                AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No)
                    .expect("open benchmark AOF");

            let start = Instant::now();
            for i in 0..iterations {
                let lsn = AofLsn::try_from_raw(i + 1).expect("benchmark LSN within range");
                let outcome = writer
                    .append_with_lsn(lsn, AofRecordBytes::from_resp(black_box(SET_RECORD)))
                    .expect("append benchmark AOF record");
                black_box(outcome);
            }
            let elapsed = start.elapsed();

            writer.flush_buffer().expect("flush benchmark AOF");
            drop(writer);
            let _ = fs::remove_file(path);
            elapsed
        });
    });
}

criterion_group!(benches, bench_append_with_lsn_no_fsync);
criterion_main!(benches);
