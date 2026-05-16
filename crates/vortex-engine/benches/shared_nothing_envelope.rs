use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use bytes::{Bytes, BytesMut};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use vortex_engine::owner::{
    LocalBorrowedEnvelope, RemoteBufferLease, RemoteCommandEnvelope, RemoteReplyBuffer,
};

const LARGE_VALUE_BYTES: usize = 4096;
const ARTIFACT_RELATIVE_ROOT: &str = ".artifacts/benchmarks/shared-nothing-sn005-envelope-20260514";

static REPORT_ARTIFACT: OnceLock<()> = OnceLock::new();

fn bench_remote_set_copied_large_value(c: &mut Criterion) {
    write_report_artifact();

    let key = b"sn005-remote-key";
    let value = vec![b'v'; LARGE_VALUE_BYTES];
    c.bench_function("sn005_remote_set_copied_large_value_4k", |b| {
        b.iter(|| {
            let envelope = RemoteCommandEnvelope::set_plain_copied(
                black_box(key),
                black_box(value.as_slice()),
            );
            let converted = envelope.into_set_key_value();
            black_box(converted);
        });
    });
}

fn bench_remote_set_leased_large_value(c: &mut Criterion) {
    let key_len = b"sn005-remote-key".len();
    let mut wire = BytesMut::with_capacity(key_len + LARGE_VALUE_BYTES);
    wire.extend_from_slice(b"sn005-remote-key");
    wire.extend_from_slice(&vec![b'v'; LARGE_VALUE_BYTES]);
    let lease = RemoteBufferLease::new(wire.freeze());

    c.bench_function("sn005_remote_set_leased_large_value_4k", |b| {
        b.iter(|| {
            let key = lease.slice(0..key_len);
            let value = lease.slice(key_len..key_len + LARGE_VALUE_BYTES);
            let envelope =
                RemoteCommandEnvelope::set_plain_leased(black_box(key), black_box(value));
            let converted = envelope.into_set_key_value();
            black_box(converted);
        });
    });
}

fn bench_remote_set_small_inline(c: &mut Criterion) {
    let key = b"sn005-small-key";
    let value = b"value";
    c.bench_function("sn005_remote_set_small_inline", |b| {
        b.iter(|| {
            let envelope =
                RemoteCommandEnvelope::set_plain_copied(black_box(key), black_box(value));
            let converted = envelope.into_set_key_value();
            black_box(converted);
        });
    });
}

fn bench_local_borrowed_construct(c: &mut Criterion) {
    let key = b"sn005-local-key";
    let value = b"value";
    c.bench_function("sn005_local_borrowed_set_construct", |b| {
        b.iter(|| {
            let envelope = LocalBorrowedEnvelope::set_plain(black_box(key), black_box(value));
            black_box(envelope.key_bytes());
            black_box(envelope.value_bytes());
        });
    });
}

fn bench_reply_buffer_accounting(c: &mut Criterion) {
    let response = Bytes::from_static(b"$5\r\nvalue\r\n");
    c.bench_function("sn005_remote_reply_buffer_accounting", |b| {
        b.iter(|| {
            let mut replies = RemoteReplyBuffer::default();
            replies.push_owned(black_box(response.clone()));
            black_box(replies.bytes());
        });
    });
}

fn write_report_artifact() {
    REPORT_ARTIFACT.get_or_init(|| {
        let root = workspace_artifact_root();
        fs::create_dir_all(&root).expect("artifact root created");

        let key = b"sn005-remote-key";
        let value = vec![b'v'; LARGE_VALUE_BYTES];
        let copied = RemoteCommandEnvelope::set_plain_copied(key, &value).accounting();

        let mut wire = BytesMut::with_capacity(key.len() + LARGE_VALUE_BYTES);
        wire.extend_from_slice(key);
        wire.extend_from_slice(&value);
        let lease = RemoteBufferLease::new(wire.freeze());
        let leased = RemoteCommandEnvelope::set_plain_leased(
            lease.slice(0..key.len()),
            lease.slice(key.len()..key.len() + LARGE_VALUE_BYTES),
        )
        .accounting();

        let small = RemoteCommandEnvelope::set_plain_copied(b"small-key", b"value").accounting();

        let mut csv = String::new();
        writeln!(
            csv,
            "scenario,payload_bytes,inline_bytes,copied_bytes,leased_bytes"
        )
        .unwrap();
        write_accounting_row(&mut csv, "large_set_copied_4k", copied);
        write_accounting_row(&mut csv, "large_set_leased_4k", leased);
        write_accounting_row(&mut csv, "small_set_inline", small);
        fs::write(root.join("envelope-accounting.csv"), csv)
            .expect("envelope accounting artifact written");
    });
}

fn write_accounting_row(
    csv: &mut String,
    scenario: &str,
    accounting: vortex_engine::owner::RemoteEnvelopeAccounting,
) {
    writeln!(
        csv,
        "{},{},{},{},{}",
        scenario,
        accounting.payload_bytes,
        accounting.inline_bytes,
        accounting.copied_bytes,
        accounting.leased_bytes
    )
    .unwrap();
}

fn workspace_artifact_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .join(ARTIFACT_RELATIVE_ROOT)
}

criterion_group!(
    benches,
    bench_remote_set_copied_large_value,
    bench_remote_set_leased_large_value,
    bench_remote_set_small_inline,
    bench_local_borrowed_construct,
    bench_reply_buffer_accounting,
);
criterion_main!(benches);
