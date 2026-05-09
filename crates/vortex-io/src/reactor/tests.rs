use super::*;
use crate::shutdown::ShutdownCoordinator;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use vortex_engine::commands::execute_command;
use vortex_persist::aof::{
    AOF_HEADER_SIZE, AofFsyncPolicy, reader::AofReader, writer::AofFileWriter,
};
use vortex_proto::RespTape;

static RESP_PONG: &[u8] = b"+PONG\r\n";

fn production_region(source: &'static str) -> &'static str {
    source.split("\n#[cfg(test").next().unwrap_or(source)
}

#[test]
fn reactor_does_not_hold_engine_table_internals() {
    let forbidden = [
        "read_shard",
        "read_shard_by_index",
        "try_read_shard_by_index",
        "write_shard",
        "write_shard_by_index",
        "try_write_shard_by_index",
        "SwissTable",
        "EntryValue",
        "vortex_engine::table",
        "vortex_engine::entry",
    ];
    let files = [
        ("reactor.rs", include_str!("../reactor.rs")),
        ("reactor/admission.rs", include_str!("admission.rs")),
        ("reactor/aof.rs", include_str!("aof.rs")),
        ("reactor/command_scope.rs", include_str!("command_scope.rs")),
        ("reactor/config.rs", include_str!("config.rs")),
        ("reactor/dispatch.rs", include_str!("dispatch.rs")),
        ("reactor/maintenance.rs", include_str!("maintenance.rs")),
        ("reactor/read_path.rs", include_str!("read_path.rs")),
        ("reactor/state.rs", include_str!("state.rs")),
        ("reactor/transaction.rs", include_str!("transaction.rs")),
        ("reactor/types.rs", include_str!("types.rs")),
        ("reactor/write_path.rs", include_str!("write_path.rs")),
    ];

    for (name, source) in files {
        for token in forbidden {
            assert!(
                !source.contains(token),
                "{name} must not import or hold engine table internals via `{token}`"
            );
        }
    }
}

#[test]
fn backend_modules_do_not_depend_on_engine_or_persist_contracts() {
    let forbidden = [
        "vortex_engine::",
        "ConcurrentKeyspace",
        "SwissTable",
        "Entry",
        "vortex_persist::",
        "AofRecord",
        "AofCommit",
        "AofFsync",
    ];
    let files = [
        ("backend/mod.rs", include_str!("../backend/mod.rs")),
        ("backend/polling.rs", include_str!("../backend/polling.rs")),
    ];

    for (name, source) in files {
        let source = production_region(source);
        for token in forbidden {
            assert!(
                !source.contains(token),
                "{name} must not depend on engine or persistence internals via `{token}`"
            );
        }
    }

    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
        let source = production_region(include_str!("../backend/uring.rs"));
        for token in forbidden {
            assert!(
                !source.contains(token),
                "backend/uring.rs must not depend on engine or persistence internals via `{token}`"
            );
        }
    }
}

#[test]
fn reactor_dispatch_uses_shared_keyspace_executor_boundary() {
    let source = production_region(include_str!("dispatch.rs"));
    assert!(
        source.contains("command_executor.execute"),
        "reactor dispatch must route command execution through SharedKeyspaceExecutor"
    );
    assert!(
        !source.contains("execute_command("),
        "reactor dispatch must not call engine command dispatch directly"
    );
}

fn test_reactor() -> Reactor {
    let config = ReactorConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        ..Default::default()
    };
    Reactor::new(0, config, Arc::new(ShutdownCoordinator::new(1))).unwrap()
}

#[derive(Default)]
struct MockBackendState {
    reads: Vec<CompletionToken>,
    writevs: Vec<(CompletionToken, usize)>,
    cancels: Vec<(CompletionToken, CompletionToken)>,
    closes: Vec<CompletionToken>,
    completions: VecDeque<Completion>,
    flushes: usize,
    fail_next_submit_sq_full: bool,
    fail_retry_submit_sq_full: bool,
}

struct MockBackend {
    state: Arc<Mutex<MockBackendState>>,
}

impl MockBackend {
    fn maybe_fail_submit(&self) -> Result<(), SubmitError> {
        let mut state = self.state.lock().unwrap();
        if state.fail_next_submit_sq_full {
            state.fail_next_submit_sq_full = false;
            return Err(SubmitError::QueueFull);
        }
        if state.fail_retry_submit_sq_full {
            state.fail_retry_submit_sq_full = false;
            return Err(SubmitError::QueueFull);
        }
        Ok(())
    }
}

impl crate::backend::BackendDriver for MockBackend {
    fn submit_accept(
        &mut self,
        _listener_fd: ListenerFd,
        _token: CompletionToken,
    ) -> Result<(), SubmitError> {
        self.maybe_fail_submit()?;
        Ok(())
    }

    fn submit_read(
        &mut self,
        _lease: ReadLease,
        token: CompletionToken,
    ) -> Result<(), SubmitError> {
        self.maybe_fail_submit()?;
        self.state.lock().unwrap().reads.push(token);
        Ok(())
    }

    fn submit_write(
        &mut self,
        _lease: WriteLease,
        _token: CompletionToken,
    ) -> Result<(), SubmitError> {
        self.maybe_fail_submit()?;
        Ok(())
    }

    fn submit_writev(
        &mut self,
        batch: IovecBatch,
        token: CompletionToken,
    ) -> Result<(), SubmitError> {
        self.maybe_fail_submit()?;
        self.state
            .lock()
            .unwrap()
            .writevs
            .push((token, batch.count()));
        Ok(())
    }

    fn submit_cancel(
        &mut self,
        target: CompletionToken,
        cancel: CompletionToken,
    ) -> Result<(), SubmitError> {
        self.state.lock().unwrap().cancels.push((target, cancel));
        Ok(())
    }

    fn submit_close(&mut self, _fd: ConnFd, token: CompletionToken) -> Result<(), SubmitError> {
        self.state.lock().unwrap().closes.push(token);
        Ok(())
    }

    fn flush(&mut self) -> Result<usize, SubmitError> {
        self.state.lock().unwrap().flushes += 1;
        Ok(0)
    }

    fn completions(&mut self, out: &mut Vec<Completion>) -> std::io::Result<usize> {
        let mut state = self.state.lock().unwrap();
        let start = out.len();
        while let Some(cqe) = state.completions.pop_front() {
            out.push(cqe);
        }
        Ok(out.len() - start)
    }

    fn drain_cq(&mut self, out: &mut Vec<Completion>) -> std::io::Result<usize> {
        self.completions(out)
    }
}

fn test_reactor_with_backend(state: Arc<Mutex<MockBackendState>>) -> Reactor {
    let config = ReactorConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        ..Default::default()
    };
    test_reactor_with_backend_config(state, config)
}

fn test_reactor_with_backend_config(
    state: Arc<Mutex<MockBackendState>>,
    config: ReactorConfig,
) -> Reactor {
    Reactor::with_keyspace_and_backend(
        0,
        config,
        Arc::new(ShutdownCoordinator::new(1)),
        Arc::new(ConcurrentKeyspace::new(DEFAULT_SHARD_COUNT)),
        Backend::test(MockBackend { state }),
        AofRuntime::new(Arc::new(AofCoordinator::new(1)), None),
        1,
    )
    .unwrap()
}

fn insert_test_connection(reactor: &mut Reactor, fd: RawFd, bytes: &[u8]) -> usize {
    let read_idx = reactor.buffer_pool.lease_index().unwrap();
    let mut meta = ConnectionMeta::new(fd, read_idx as u32);
    meta.read_buf_len = bytes.len() as u32;
    let conn_id = reactor.connections.insert(meta);
    reactor.generations[conn_id] = 1;
    if !bytes.is_empty() {
        // SAFETY: read_idx is leased by this connection and bytes fit in the
        // reactor's configured fixed read buffer in these tests.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                reactor.buffer_pool.ptr(read_idx),
                bytes.len(),
            );
        }
    }
    conn_id
}

fn copy_into_read_buffer(reactor: &mut Reactor, conn_id: usize, bytes: &[u8]) {
    let read_idx = reactor.connections.get(conn_id).unwrap().read_buf_offset as usize;
    assert!(bytes.len() <= reactor.buffer_pool.buffer_size());
    // SAFETY: read_idx is leased by this connection and bytes fit in the
    // configured fixed read buffer.
    unsafe {
        std::ptr::copy_nonoverlapping(
            bytes.as_ptr(),
            reactor.buffer_pool.ptr(read_idx),
            bytes.len(),
        );
    }
}

fn pending_writev_bytes(reactor: &Reactor, conn_id: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for iov in reactor.writev_states[conn_id].remaining_iovecs() {
        // SAFETY: test reads stable iovec backing owned by PendingWritev
        // before completing the write.
        let bytes = unsafe { std::slice::from_raw_parts(iov.iov_base.cast::<u8>(), iov.iov_len) };
        out.extend_from_slice(bytes);
    }
    out
}

fn resp_command(parts: &[&[u8]]) -> Vec<u8> {
    let mut wire = Vec::new();
    push_resp_array_len(&mut wire, parts.len());
    for part in parts {
        push_resp_bulk_string(&mut wire, part);
    }
    wire
}

fn socket_pair() -> (RawFd, RawFd) {
    let mut fds = [0; 2];
    // SAFETY: `fds` points to two valid integers for libc to initialize.
    let rc = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
    assert_eq!(rc, 0, "socketpair failed: {}", io::Error::last_os_error());
    (fds[0], fds[1])
}

fn peer_observes_eof(peer_fd: RawFd) -> bool {
    let mut byte = 0u8;
    // SAFETY: `peer_fd` remains open in the test and `byte` is valid for one byte.
    let rc = unsafe {
        libc::recv(
            peer_fd,
            (&mut byte as *mut u8).cast::<libc::c_void>(),
            1,
            libc::MSG_DONTWAIT,
        )
    };
    rc == 0
}

fn temp_aof_path(suffix: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut path = std::env::temp_dir();
    path.push(format!(
        "vortex-reactor-test-{}-{}-{suffix}.aof",
        std::process::id(),
        COUNTER.fetch_add(1, AtomicOrdering::Relaxed),
    ));
    path
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_file(path);
}

#[test]
fn backend_plan_polling_only_mode_selects_polling() {
    let config = ReactorConfig {
        io_backend: IoBackendMode::Polling,
        ..Default::default()
    };

    let (plan, _backend) = make_backend(&config).unwrap();

    assert_eq!(plan.requested, IoBackendMode::Polling);
    assert_eq!(plan.effective, BackendKind::Polling);
    assert!(!plan.capabilities.fixed_buffers);
    assert!(!plan.capabilities.sqpoll);
    assert!(!plan.capabilities.multishot_accept);
    assert_eq!(
        plan.capabilities.accept4,
        cfg!(any(target_os = "linux", target_os = "android"))
    );
    assert!(!plan.capabilities.close_opcode);
    assert!(plan.capabilities.async_cancel);
    assert!(plan.capabilities.nonblocking_drain);
}

#[cfg(not(all(target_os = "linux", feature = "io-uring")))]
#[test]
fn backend_plan_auto_falls_back_to_polling_when_uring_unavailable() {
    let config = ReactorConfig {
        io_backend: IoBackendMode::Auto,
        ..Default::default()
    };

    let (plan, _backend) = make_backend(&config).unwrap();

    assert_eq!(plan.requested, IoBackendMode::Auto);
    assert_eq!(plan.effective, BackendKind::Polling);
    assert_eq!(
        runtime_effective_backend(plan.effective).as_str(),
        "polling"
    );
}

#[cfg(not(all(target_os = "linux", feature = "io-uring")))]
#[test]
fn backend_plan_strict_uring_fails_when_unavailable() {
    let config = ReactorConfig {
        io_backend: IoBackendMode::Uring,
        ..Default::default()
    };

    let Err(error) = make_backend(&config) else {
        panic!("strict io_uring backend unexpectedly started");
    };

    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
}

#[test]
fn backend_runtime_snapshot_formats_capability_contract() {
    let config = ReactorConfig {
        io_backend: IoBackendMode::Auto,
        ring_size: 1024,
        ..Default::default()
    };
    let plan = BackendPlan {
        requested: IoBackendMode::Auto,
        effective: BackendKind::Polling,
        capabilities: BackendCapabilities {
            fixed_buffers: false,
            sqpoll: false,
            multishot_accept: false,
            accept4: false,
            close_opcode: false,
            async_cancel: true,
            nonblocking_drain: true,
        },
    };

    let snapshot = Reactor::runtime_backend_snapshot(&config, plan, false);

    assert_eq!(snapshot.requested.as_str(), "auto");
    assert_eq!(snapshot.effective.as_str(), "polling");
    assert!(!snapshot.fixed_buffers_registered);
    assert!(snapshot.cancel_support);
    assert_eq!(snapshot.requested_ring_size, 1024);
    assert_eq!(snapshot.effective_ring_size, 0);
}

#[cfg(feature = "profile-telemetry")]
#[test]
fn telemetry_mode_gates_profile_timer_starts() {
    let mut reactor = test_reactor();
    assert_eq!(reactor.profile_metric_start(), None);
    assert_eq!(reactor.elapsed_profile_metric_nanos(None), 0);
    assert!(!reactor.keyspace.runtime_profile_timers_enabled());

    reactor.config.telemetry_mode = RuntimeTelemetryMode::Profile;
    reactor
        .keyspace
        .set_runtime_telemetry_mode(RuntimeTelemetryMode::Profile);

    assert!(reactor.profile_metric_start().is_some());
    assert!(reactor.keyspace.runtime_profile_timers_enabled());
}

fn enable_test_aof(reactor: &mut Reactor, path: &Path) -> AofEpoch {
    enable_test_aof_with_policy(reactor, path, AofFsyncPolicy::No)
}

fn enable_test_aof_with_policy(
    reactor: &mut Reactor,
    path: &Path,
    policy: AofFsyncPolicy,
) -> AofEpoch {
    let epoch = reactor.aof_coordinator.begin_startup_enable().unwrap();
    let writer =
        AofFileWriter::open(path, Reactor::aof_writer_id(reactor.id).unwrap(), policy).unwrap();
    reactor.aof_writer = Some(AofWriterSlot::new(epoch, writer));
    reactor.config.aof_config = Some(AofConfig {
        path: path.to_path_buf(),
        fsync_policy: policy,
        max_pending_fsync_bytes: DEFAULT_EVERYSEC_MAX_PENDING_BYTES,
    });
    reactor
        .aof_coordinator
        .commit_enable(epoch, &reactor.keyspace)
        .unwrap();
    epoch
}

fn handle_config_wire(reactor: &mut Reactor, wire: &[u8]) -> (CommandResponse, bool) {
    let tape = RespTape::parse_pipeline(wire).expect("valid RESP");
    let frame = tape.iter().next().expect("at least one frame");
    reactor.handle_config(&frame).expect("handled config")
}

fn dispatch_reactor_wire_on(
    reactor: &mut Reactor,
    conn_id: usize,
    wire: &[u8],
) -> (CommandResponse, bool) {
    let tape = RespTape::parse_pipeline(wire).expect("valid RESP");
    let frame = tape.iter().next().expect("at least one frame");
    reactor.dispatch_command(conn_id, &frame)
}

fn dispatch_reactor_wire(reactor: &mut Reactor, wire: &[u8]) -> (CommandResponse, bool) {
    dispatch_reactor_wire_on(reactor, 0, wire)
}

fn response_bytes(resp: CommandResponse) -> Vec<u8> {
    match resp {
        CommandResponse::Static(bytes) => bytes.to_vec(),
        CommandResponse::Inline(inline) => inline.as_bytes().to_vec(),
        CommandResponse::Owned(bytes) => bytes.into_vec(),
        CommandResponse::Frame(frame) => {
            let mut out = Vec::new();
            append_resp_frame(&mut out, &frame);
            out
        }
    }
}

fn keyspace_get_response(keyspace: &ConcurrentKeyspace, key: &[u8]) -> Vec<u8> {
    let mut wire = Vec::with_capacity(32 + key.len());
    push_resp_array_len(&mut wire, 2);
    push_resp_bulk_string(&mut wire, b"GET");
    push_resp_bulk_string(&mut wire, key);
    let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
    let frame = tape.iter().next().unwrap();
    let result = execute_command(
        keyspace,
        b"GET",
        &frame,
        CommandClock::new(Timestamp::now().as_nanos(), current_unix_time_nanos()),
    )
    .unwrap();

    match result.response {
        CmdResult::Static(bytes) => bytes.to_vec(),
        CmdResult::Inline(inline) => inline.as_bytes().to_vec(),
        CmdResult::Resp(frame) => {
            let mut out = Vec::new();
            append_resp_frame(&mut out, &frame);
            out
        }
    }
}

fn replayed_get_response(path: &Path, key: &[u8]) -> Vec<u8> {
    let replayed = ConcurrentKeyspace::new(DEFAULT_SHARD_COUNT);
    AofReader::new(path)
        .replay_into_keyspace(&replayed)
        .expect("replay should succeed");
    keyspace_get_response(&replayed, key)
}

fn expect_config_pair(resp: CommandResponse, expected_name: &[u8], expected_value: &[u8]) {
    match resp {
        CommandResponse::Frame(RespFrame::Array(Some(items))) => {
            assert_eq!(items.len(), 2);
            assert_eq!(bulk_bytes(&items[0]), expected_name);
            assert_eq!(bulk_bytes(&items[1]), expected_value);
        }
        other => panic!("expected config pair response, got {other:?}"),
    }
}

fn bulk_bytes(frame: &RespFrame) -> &[u8] {
    match frame {
        RespFrame::BulkString(Some(bytes)) => bytes.as_ref(),
        other => panic!("expected bulk string, got {other:?}"),
    }
}

/// Helper: parse a single RESP wire command, route through engine dispatch.
fn dispatch_wire(wire: &[u8]) -> (CommandResponse, bool) {
    let tape = RespTape::parse_pipeline(wire).expect("valid RESP");
    let frame = tape.iter().next().expect("at least one frame");
    let mut router = CommandRouter::new();
    let keyspace = ConcurrentKeyspace::new(DEFAULT_SHARD_COUNT);
    let now = Timestamp::now().as_nanos();
    match router.dispatch(&frame) {
        DispatchResult::Dispatch { meta, name, .. } => {
            match execute_command(&keyspace, name, &frame, now) {
                Some(executed) => match executed.response {
                    CmdResult::Static(buf) => {
                        let close = meta.name == "QUIT";
                        (CommandResponse::Static(buf), close)
                    }
                    CmdResult::Inline(inline) => (CommandResponse::Inline(inline), false),
                    CmdResult::Resp(f) => (CommandResponse::Frame(f), false),
                },
                None => (
                    CommandResponse::Static(b"-ERR command not yet implemented\r\n"),
                    false,
                ),
            }
        }
        DispatchResult::WrongArity { .. } => (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false),
        DispatchResult::UnknownCommand => (CommandResponse::Static(RESP_ERR_UNKNOWN), false),
    }
}

#[test]
fn dispatch_ping() {
    let (resp, close) = dispatch_wire(b"*1\r\n$4\r\nPING\r\n");
    assert!(matches!(resp, CommandResponse::Static(b) if b == RESP_PONG));
    assert!(!close);
}

#[test]
fn dispatch_unknown() {
    // FOOBAR is truly unknown
    let (resp, _) = dispatch_wire(b"*1\r\n$6\r\nFOOBAR\r\n");
    assert!(matches!(resp, CommandResponse::Static(b) if b == RESP_ERR_UNKNOWN));
}

#[test]
fn dispatch_set_returns_ok() {
    // SET key value → now goes through the engine, returns +OK
    let (resp, _) = dispatch_wire(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    assert!(matches!(resp, CommandResponse::Static(b) if b == b"+OK\r\n"));
}

#[test]
fn command_gate_scope_uses_key_ranges_and_full_keyspace_fallbacks() {
    let set_wire = resp_command(&[b"SET", b"foo", b"bar"]);
    let tape = RespTape::parse_pipeline(&set_wire).expect("valid SET");
    let frame = tape.iter().next().expect("SET frame");
    let set = vortex_proto::command::lookup_command("SET").expect("SET metadata");
    let scope = command_keyspace_gate_scope(set, &frame);
    assert!(!scope.full);
    assert_eq!(scope.keys.as_slice(), &[b"foo".as_slice()]);

    let ping_wire = resp_command(&[b"PING"]);
    let tape = RespTape::parse_pipeline(&ping_wire).expect("valid PING");
    let frame = tape.iter().next().expect("PING frame");
    let ping = vortex_proto::command::lookup_command("PING").expect("PING metadata");
    let scope = command_keyspace_gate_scope(ping, &frame);
    assert!(!scope.full);
    assert!(scope.keys.is_empty());

    let dbsize_wire = resp_command(&[b"DBSIZE"]);
    let tape = RespTape::parse_pipeline(&dbsize_wire).expect("valid DBSIZE");
    let frame = tape.iter().next().expect("DBSIZE frame");
    let dbsize = vortex_proto::command::lookup_command("DBSIZE").expect("DBSIZE metadata");
    let scope = command_keyspace_gate_scope(dbsize, &frame);
    assert!(scope.full);
    assert!(scope.keys.is_empty());
}

#[test]
fn multi_key_command_gate_scope_preserves_command_table_order() {
    let wire = resp_command(&[b"MSET", b"k1", b"v1", b"k2", b"v2"]);
    let tape = RespTape::parse_pipeline(&wire).expect("valid MSET");
    let frame = tape.iter().next().expect("MSET frame");
    let mset = vortex_proto::command::lookup_command("MSET").expect("MSET metadata");

    let scope = command_keyspace_gate_scope(mset, &frame);
    assert!(!scope.full);
    assert_eq!(scope.keys.as_slice(), &[b"k1".as_slice(), b"k2".as_slice()]);
}

#[test]
fn multi_exec_queues_and_returns_array() {
    let mut reactor = test_reactor();

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");

    let (resp, _) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
    );
    assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), b"*2\r\n+OK\r\n$3\r\nbar\r\n");
}

#[test]
fn multi_exec_handles_cross_key_and_duplicate_key_plans() {
    let mut reactor = test_reactor();

    assert_eq!(
        response_bytes(dispatch_reactor_wire(&mut reactor, &resp_command(&[b"MULTI"])).0),
        b"+OK\r\n"
    );
    assert_eq!(
        response_bytes(
            dispatch_reactor_wire(&mut reactor, &resp_command(&[b"SET", b"alpha", b"one"])).0
        ),
        b"+QUEUED\r\n"
    );
    assert_eq!(
        response_bytes(
            dispatch_reactor_wire(&mut reactor, &resp_command(&[b"SET", b"beta", b"two"])).0
        ),
        b"+QUEUED\r\n"
    );
    assert_eq!(
        response_bytes(
            dispatch_reactor_wire(&mut reactor, &resp_command(&[b"SET", b"alpha", b"three"])).0
        ),
        b"+QUEUED\r\n"
    );

    let (resp, _) = dispatch_reactor_wire(&mut reactor, &resp_command(&[b"EXEC"]));
    assert_eq!(response_bytes(resp), b"*3\r\n+OK\r\n+OK\r\n+OK\r\n");
    assert_eq!(
        keyspace_get_response(&reactor.keyspace, b"alpha"),
        b"$5\r\nthree\r\n"
    );
    assert_eq!(
        keyspace_get_response(&reactor.keyspace, b"beta"),
        b"$3\r\ntwo\r\n"
    );
}

#[test]
fn watch_aborts_when_other_connection_modifies_key() {
    let mut reactor = test_reactor();

    let (resp, _) =
        dispatch_reactor_wire_on(&mut reactor, 0, b"*2\r\n$5\r\nWATCH\r\n$3\r\nfoo\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");

    let (resp, _) = dispatch_reactor_wire_on(
        &mut reactor,
        1,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$5\r\nother\r\n",
    );
    assert_eq!(response_bytes(resp), b"+OK\r\n");

    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire_on(
        &mut reactor,
        0,
        b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$6\r\nqueued\r\n",
    );
    assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), b"*-1\r\n");

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
    assert_eq!(response_bytes(resp), b"$-1\r\n");
}

#[test]
fn watch_missing_key_aborts_after_set_delete_churn() {
    let mut reactor = test_reactor();

    let (resp, _) =
        dispatch_reactor_wire_on(&mut reactor, 0, b"*2\r\n$5\r\nWATCH\r\n$7\r\nmissing\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");

    let (resp, _) = dispatch_reactor_wire_on(
        &mut reactor,
        1,
        b"*3\r\n$3\r\nSET\r\n$7\r\nmissing\r\n$5\r\nvalue\r\n",
    );
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) =
        dispatch_reactor_wire_on(&mut reactor, 1, b"*2\r\n$3\r\nDEL\r\n$7\r\nmissing\r\n");
    assert_eq!(response_bytes(resp), b":1\r\n");

    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire_on(
        &mut reactor,
        0,
        b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$6\r\nqueued\r\n",
    );
    assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), b"*-1\r\n");
}

#[test]
fn watch_aborts_after_flushall() {
    let mut reactor = test_reactor();

    let (resp, _) = dispatch_reactor_wire_on(
        &mut reactor,
        0,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
    );
    assert_eq!(response_bytes(resp), b"+OK\r\n");

    let (resp, _) =
        dispatch_reactor_wire_on(&mut reactor, 0, b"*2\r\n$5\r\nWATCH\r\n$3\r\nfoo\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");

    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 1, b"*1\r\n$8\r\nFLUSHALL\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");

    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire_on(
        &mut reactor,
        0,
        b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$6\r\nqueued\r\n",
    );
    assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), b"*-1\r\n");
}

#[test]
fn watch_aborts_after_pexpire_only_mutation() {
    let mut reactor = test_reactor();

    let (resp, _) = dispatch_reactor_wire_on(
        &mut reactor,
        0,
        b"*3\r\n$3\r\nSET\r\n$9\r\nwatch:ttl\r\n$2\r\nv1\r\n",
    );
    assert_eq!(response_bytes(resp), b"+OK\r\n");

    let (resp, _) =
        dispatch_reactor_wire_on(&mut reactor, 0, b"*2\r\n$5\r\nWATCH\r\n$9\r\nwatch:ttl\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");

    let (resp, _) = dispatch_reactor_wire_on(
        &mut reactor,
        1,
        b"*3\r\n$7\r\nPEXPIRE\r\n$9\r\nwatch:ttl\r\n$5\r\n60000\r\n",
    );
    assert_eq!(response_bytes(resp), b":1\r\n");

    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire_on(
        &mut reactor,
        0,
        b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$6\r\nqueued\r\n",
    );
    assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), b"*-1\r\n");
}

#[test]
fn watch_aborts_after_persist_only_mutation() {
    let mut reactor = test_reactor();

    let (resp, _) = dispatch_reactor_wire_on(
        &mut reactor,
        0,
        b"*5\r\n$3\r\nSET\r\n$13\r\nwatch:persist\r\n$2\r\nv1\r\n$2\r\nEX\r\n$2\r\n60\r\n",
    );
    assert_eq!(response_bytes(resp), b"+OK\r\n");

    let (resp, _) = dispatch_reactor_wire_on(
        &mut reactor,
        0,
        b"*2\r\n$5\r\nWATCH\r\n$13\r\nwatch:persist\r\n",
    );
    assert_eq!(response_bytes(resp), b"+OK\r\n");

    let (resp, _) = dispatch_reactor_wire_on(
        &mut reactor,
        1,
        b"*2\r\n$7\r\nPERSIST\r\n$13\r\nwatch:persist\r\n",
    );
    assert_eq!(response_bytes(resp), b":1\r\n");

    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire_on(
        &mut reactor,
        0,
        b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$6\r\nqueued\r\n",
    );
    assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), b"*-1\r\n");
}

#[test]
fn watch_aborts_when_read_lazily_expires_watched_key() {
    let mut reactor = test_reactor();
    reactor.cached_nanos = 0;
    reactor.cached_unix_nanos = 0;

    let (resp, _) = dispatch_reactor_wire_on(
        &mut reactor,
        1,
        b"*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$3\r\n100\r\n",
    );
    assert_eq!(response_bytes(resp), b"+OK\r\n");

    let (resp, _) =
        dispatch_reactor_wire_on(&mut reactor, 0, b"*2\r\n$5\r\nWATCH\r\n$3\r\nfoo\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");

    reactor.cached_nanos = 200_000_000;
    reactor.cached_unix_nanos = 200_000_000;
    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 1, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    assert_eq!(response_bytes(resp), b"$-1\r\n");

    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire_on(
        &mut reactor,
        0,
        b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$6\r\nqueued\r\n",
    );
    assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), b"*-1\r\n");

    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
    assert_eq!(response_bytes(resp), b"$-1\r\n");
}

#[test]
fn exec_appends_queued_writes_to_aof() {
    let path = temp_aof_path("transaction-aof");
    let mut reactor = test_reactor();
    enable_test_aof(&mut reactor, &path);

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$5\r\ntxkey\r\n$5\r\nvalue\r\n",
    );
    assert_eq!(response_bytes(resp), b"+QUEUED\r\n");
    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), b"*1\r\n+OK\r\n");
    reactor
        .aof_writer
        .as_mut()
        .expect("AOF writer")
        .writer
        .flush_buffer()
        .unwrap();

    let replayed = ConcurrentKeyspace::new(DEFAULT_SHARD_COUNT);
    let stats = AofReader::new(&path)
        .replay_into_keyspace(&replayed)
        .unwrap();
    assert_eq!(stats.commands_replayed, 1);

    let tape = RespTape::parse_pipeline(b"*2\r\n$3\r\nGET\r\n$5\r\ntxkey\r\n").expect("valid RESP");
    let frame = tape.iter().next().unwrap();
    let result = execute_command(
        &replayed,
        b"GET",
        &frame,
        CommandClock::new(Timestamp::now().as_nanos(), current_unix_time_nanos()),
    )
    .unwrap();
    assert!(matches!(result.response, CmdResult::Inline(inline) if inline.payload() == b"value"));

    cleanup(&path);
}

#[test]
fn exec_aof_batch_replays_all_or_none_when_tail_truncated() {
    let path = temp_aof_path("transaction-aof-truncated");
    let mut reactor = test_reactor();
    enable_test_aof(&mut reactor, &path);

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$3\r\ntx1\r\n$3\r\none\r\n",
    );
    assert_eq!(response_bytes(resp), b"+QUEUED\r\n");
    let (resp, _) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$3\r\ntx2\r\n$3\r\ntwo\r\n",
    );
    assert_eq!(response_bytes(resp), b"+QUEUED\r\n");
    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), b"*2\r\n+OK\r\n+OK\r\n");
    reactor
        .aof_writer
        .as_mut()
        .expect("AOF writer")
        .writer
        .flush_buffer()
        .unwrap();

    let full = ConcurrentKeyspace::new(DEFAULT_SHARD_COUNT);
    let stats = AofReader::new(&path).replay_into_keyspace(&full).unwrap();
    assert_eq!(stats.commands_replayed, 2);

    let file_len = std::fs::metadata(&path).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(file_len - 1)
        .unwrap();

    let truncated = ConcurrentKeyspace::new(DEFAULT_SHARD_COUNT);
    let stats = AofReader::new(&path)
        .replay_into_keyspace(&truncated)
        .unwrap();
    assert_eq!(stats.commands_replayed, 0);
    assert_eq!(stats.bytes_truncated, file_len - 1 - AOF_HEADER_SIZE as u64);

    let tape = RespTape::parse_pipeline(b"*2\r\n$3\r\nGET\r\n$3\r\ntx1\r\n").expect("valid RESP");
    let frame = tape.iter().next().unwrap();
    let result = execute_command(
        &truncated,
        b"GET",
        &frame,
        CommandClock::new(Timestamp::now().as_nanos(), current_unix_time_nanos()),
    )
    .unwrap();
    assert!(matches!(result.response, CmdResult::Static(b"$-1\r\n")));

    cleanup(&path);
}

#[test]
fn discard_clears_transaction_and_watches() {
    let mut reactor = test_reactor();

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$5\r\nWATCH\r\n$3\r\nfoo\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
    );
    assert_eq!(response_bytes(resp), b"+QUEUED\r\n");
    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$7\r\nDISCARD\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    assert_eq!(response_bytes(resp), b"$-1\r\n");
}

#[test]
fn multi_wrong_arity_does_not_enter_transaction() {
    let mut reactor = test_reactor();

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$5\r\nMULTI\r\n$5\r\nextra\r\n");
    assert_eq!(response_bytes(resp), RESP_ERR_WRONG_ARGC);

    let (resp, _) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
    );
    assert_eq!(response_bytes(resp), b"+OK\r\n");

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), RESP_ERR_EXEC_WITHOUT_MULTI);
}

#[test]
fn queue_time_wrong_arity_aborts_exec_without_mutating() {
    let mut reactor = test_reactor();

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
    );
    assert_eq!(response_bytes(resp), b"+QUEUED\r\n");
    let (resp, _) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$4\r\nINCR\r\n$3\r\nfoo\r\n$5\r\nextra\r\n",
    );
    assert_eq!(response_bytes(resp), RESP_ERR_WRONG_ARGC);

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), RESP_ERR_EXECABORT);

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    assert_eq!(response_bytes(resp), b"$-1\r\n");
}

#[test]
fn nested_multi_returns_error_without_dirtying_transaction() {
    let mut reactor = test_reactor();

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), RESP_ERR_NESTED_MULTI);
    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), b"*0\r\n");
}

#[test]
fn watch_inside_multi_returns_error_without_dirtying_transaction() {
    let mut reactor = test_reactor();

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$5\r\nWATCH\r\n$3\r\nfoo\r\n");
    assert_eq!(response_bytes(resp), RESP_ERR_WATCH_INSIDE_MULTI);
    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), b"*0\r\n");
}

#[test]
fn exec_wrong_arity_inside_multi_returns_error_without_dirtying_transaction() {
    let mut reactor = test_reactor();

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$4\r\nEXEC\r\n$5\r\nextra\r\n");
    assert_eq!(response_bytes(resp), RESP_ERR_WRONG_ARGC);
    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), b"*0\r\n");
}

#[test]
fn discard_wrong_arity_inside_multi_returns_error_without_dirtying_transaction() {
    let mut reactor = test_reactor();

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$7\r\nDISCARD\r\n$5\r\nextra\r\n");
    assert_eq!(response_bytes(resp), RESP_ERR_WRONG_ARGC);
    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), b"*0\r\n");
}

#[test]
fn exec_empty_transaction_returns_empty_array() {
    let mut reactor = test_reactor();

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), b"*0\r\n");
}

#[test]
fn exec_runtime_error_does_not_abort_later_commands() {
    let mut reactor = test_reactor();

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$3\r\nnum\r\n$5\r\nnoint\r\n",
    );
    assert_eq!(response_bytes(resp), b"+QUEUED\r\n");
    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$4\r\nINCR\r\n$3\r\nnum\r\n");
    assert_eq!(response_bytes(resp), b"+QUEUED\r\n");
    let (resp, _) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$5\r\nafter\r\n$2\r\nok\r\n",
    );
    assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(
        response_bytes(resp),
        b"*3\r\n+OK\r\n-ERR value is not an integer or out of range\r\n+OK\r\n"
    );

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$3\r\nGET\r\n$5\r\nafter\r\n");
    assert_eq!(response_bytes(resp), b"$2\r\nok\r\n");
}

#[test]
fn watch_aborts_on_same_connection_pre_multi_write() {
    let mut reactor = test_reactor();

    let (resp, _) =
        dispatch_reactor_wire_on(&mut reactor, 0, b"*2\r\n$5\r\nWATCH\r\n$3\r\nfoo\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire_on(
        &mut reactor,
        0,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$4\r\nself\r\n",
    );
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), b"*-1\r\n");
}

#[test]
fn unwatch_clears_watches_before_conflicting_write() {
    let mut reactor = test_reactor();

    let (resp, _) =
        dispatch_reactor_wire_on(&mut reactor, 0, b"*2\r\n$5\r\nWATCH\r\n$3\r\nfoo\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$7\r\nUNWATCH\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire_on(
        &mut reactor,
        1,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$5\r\nother\r\n",
    );
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire_on(
        &mut reactor,
        0,
        b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$2\r\nok\r\n",
    );
    assert_eq!(response_bytes(resp), b"+QUEUED\r\n");
    let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), b"*1\r\n+OK\r\n");
}

#[test]
fn watch_and_unwatch_wrong_arity_are_rejected() {
    let mut reactor = test_reactor();

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nWATCH\r\n");
    assert_eq!(response_bytes(resp), RESP_ERR_WRONG_ARGC);

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$7\r\nUNWATCH\r\n$3\r\nfoo\r\n");
    assert_eq!(response_bytes(resp), RESP_ERR_WRONG_ARGC);
}

#[test]
fn fatal_aof_state_rejects_writes_before_mutation() {
    let path = temp_aof_path("fatal-write-reject");
    let mut reactor = test_reactor();
    enable_test_aof(&mut reactor, &path);
    reactor.aof_coordinator.mark_failed(
        reactor.id,
        "test",
        &std::io::Error::other("forced AOF failure"),
    );

    let (resp, close) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
    );
    assert!(matches!(resp, CommandResponse::Static(b) if b == RESP_ERR_AOF_MISCONF));
    assert!(!close);

    let (read_resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    assert!(matches!(read_resp, CommandResponse::Static(b) if b == b"$-1\r\n"));

    cleanup(&path);
}

#[test]
fn aof_append_failure_enters_write_stop_for_normal_write() {
    let path = temp_aof_path("normal-append-failure");
    let mut reactor = test_reactor();
    enable_test_aof(&mut reactor, &path);
    reactor.aof_append_fail_after = Some(0);

    let (resp, close) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$4\r\nfail\r\n$5\r\nvalue\r\n",
    );
    assert_eq!(response_bytes(resp), RESP_ERR_AOF_MISCONF);
    assert!(!close);
    assert!(reactor.aof_coordinator.is_failed());

    let (read_resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$3\r\nGET\r\n$4\r\nfail\r\n");
    assert_eq!(response_bytes(read_resp), b"$5\r\nvalue\r\n");
    assert_eq!(replayed_get_response(&path, b"fail"), b"$-1\r\n");

    let (resp, _) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$5\r\nafter\r\n$2\r\nok\r\n",
    );
    assert_eq!(response_bytes(resp), RESP_ERR_AOF_MISCONF);

    let (read_resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$3\r\nGET\r\n$5\r\nafter\r\n");
    assert_eq!(response_bytes(read_resp), b"$-1\r\n");

    cleanup(&path);
}

#[test]
fn aof_fsync_failure_enters_write_stop_for_always_write() {
    let path = temp_aof_path("always-fsync-failure");
    let mut reactor = test_reactor();
    enable_test_aof_with_policy(&mut reactor, &path, AofFsyncPolicy::Always);
    reactor.aof_fsync_fail_after = Some(0);

    let (resp, close) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$4\r\nsync\r\n$5\r\nvalue\r\n",
    );
    assert_eq!(response_bytes(resp), RESP_ERR_AOF_MISCONF);
    assert!(!close);
    assert!(reactor.aof_coordinator.is_failed());

    let (read_resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$3\r\nGET\r\n$4\r\nsync\r\n");
    assert_eq!(response_bytes(read_resp), b"$5\r\nvalue\r\n");

    let (resp, _) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$5\r\nafter\r\n$2\r\nok\r\n",
    );
    assert_eq!(response_bytes(resp), RESP_ERR_AOF_MISCONF);

    cleanup(&path);
}

#[test]
fn aof_append_failure_enters_write_stop_for_exec() {
    let path = temp_aof_path("exec-append-failure");
    let mut reactor = test_reactor();
    enable_test_aof(&mut reactor, &path);

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$5\r\ntxkey\r\n$5\r\nvalue\r\n",
    );
    assert_eq!(response_bytes(resp), RESP_QUEUED);

    reactor.aof_append_fail_after = Some(0);
    let (resp, close) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), RESP_ERR_AOF_MISCONF);
    assert!(!close);
    assert!(reactor.aof_coordinator.is_failed());

    let (read_resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$3\r\nGET\r\n$5\r\ntxkey\r\n");
    assert_eq!(response_bytes(read_resp), b"$-1\r\n");
    assert_eq!(replayed_get_response(&path, b"txkey"), b"$-1\r\n");

    let (resp, _) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$5\r\nafter\r\n$2\r\nok\r\n",
    );
    assert_eq!(response_bytes(resp), RESP_ERR_AOF_MISCONF);

    cleanup(&path);
}

#[test]
fn aof_fsync_failure_enters_write_stop_for_exec() {
    let path = temp_aof_path("exec-fsync-failure");
    let mut reactor = test_reactor();
    enable_test_aof_with_policy(&mut reactor, &path, AofFsyncPolicy::Always);

    let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    let (resp, _) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$5\r\ntxkey\r\n$5\r\nvalue\r\n",
    );
    assert_eq!(response_bytes(resp), RESP_QUEUED);

    reactor.aof_fsync_fail_after = Some(0);
    let (resp, close) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
    assert_eq!(response_bytes(resp), RESP_ERR_AOF_MISCONF);
    assert!(!close);
    assert!(reactor.aof_coordinator.is_failed());

    let (read_resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$3\r\nGET\r\n$5\r\ntxkey\r\n");
    assert_eq!(response_bytes(read_resp), b"$-1\r\n");
    assert_eq!(replayed_get_response(&path, b"txkey"), b"$-1\r\n");

    cleanup(&path);
}

#[test]
fn multi_reactor_runtime_appendonly_enable_is_rejected_without_writer() {
    let mut reactor = test_reactor();
    reactor.aof_coordinator = Arc::new(AofCoordinator::new(2));
    let before_lsn = reactor.keyspace.current_lsn();

    let (resp, close) = handle_config_wire(
        &mut reactor,
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$10\r\nappendonly\r\n$3\r\nyes\r\n",
    );
    assert_eq!(response_bytes(resp), RESP_ERR_CONFIG_SET_APPENDONLY_MULTI);
    assert!(!close);
    assert!(reactor.aof_writer.is_none());

    let (resp, _) = handle_config_wire(
        &mut reactor,
        b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$10\r\nappendonly\r\n",
    );
    expect_config_pair(resp, b"appendonly", b"no");

    let (resp, _) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$5\r\nplain\r\n$2\r\nok\r\n",
    );
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    assert_eq!(reactor.keyspace.current_lsn(), before_lsn);
}

#[test]
fn multi_reactor_runtime_appendonly_disable_is_rejected_and_writer_remains() {
    let path = temp_aof_path("multi-disable-reject");
    let mut reactor = test_reactor();
    reactor.aof_coordinator = Arc::new(AofCoordinator::new(2));
    enable_test_aof(&mut reactor, &path);

    let (resp, close) = handle_config_wire(
        &mut reactor,
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$10\r\nappendonly\r\n$2\r\nno\r\n",
    );
    assert_eq!(response_bytes(resp), RESP_ERR_CONFIG_SET_APPENDONLY_MULTI);
    assert!(!close);
    assert!(reactor.aof_writer.is_some());

    let (resp, _) = handle_config_wire(
        &mut reactor,
        b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$10\r\nappendonly\r\n",
    );
    expect_config_pair(resp, b"appendonly", b"yes");

    cleanup(&path);
}

#[test]
fn single_reactor_runtime_appendonly_enable_disable_uses_coordinator() {
    let path = temp_aof_path("single-runtime-toggle");
    let mut reactor = test_reactor();
    reactor.config.aof_config = Some(AofConfig {
        path: path.clone(),
        fsync_policy: AofFsyncPolicy::No,
        max_pending_fsync_bytes: DEFAULT_EVERYSEC_MAX_PENDING_BYTES,
    });

    let (resp, close) = handle_config_wire(
        &mut reactor,
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$10\r\nappendonly\r\n$3\r\nyes\r\n",
    );
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    assert!(!close);
    assert!(reactor.aof_writer.is_some());

    let (resp, _) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$7\r\nruntime\r\n$2\r\non\r\n",
    );
    assert_eq!(response_bytes(resp), b"+OK\r\n");

    let (resp, close) = handle_config_wire(
        &mut reactor,
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$10\r\nappendonly\r\n$2\r\nno\r\n",
    );
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    assert!(!close);
    assert!(reactor.aof_writer.is_none());

    cleanup(&path);
}

#[test]
fn dispatch_ping_lowercase() {
    let (resp, _) = dispatch_wire(b"*1\r\n$4\r\nping\r\n");
    assert!(matches!(resp, CommandResponse::Static(b) if b == RESP_PONG));
}

#[test]
fn dispatch_attribute_wrapped_ping() {
    // Attribute-wrapped frames: |1\r\n+meta\r\n+value\r\n*1\r\n$4\r\nPING\r\n
    // The CommandRouter correctly extracts PING from inside the attribute
    // envelope. The engine receives the outer (attribute) FrameRef, whose
    // element_count() differs from a plain array — cmd_ping may treat the
    // attribute child as a message arg and return a bulk string instead of
    // the static +PONG. Verify routing succeeds (no error / no panic).
    let (resp, close) = dispatch_wire(b"|1\r\n+meta\r\n+value\r\n*1\r\n$4\r\nPING\r\n");
    assert!(!close);
    // Acceptable outcomes: static PONG or a bulk-string echo of the attribute child.
    match resp {
        CommandResponse::Static(b) => assert_eq!(b, RESP_PONG),
        CommandResponse::Inline(_) | CommandResponse::Frame(_) | CommandResponse::Owned(_) => {
            /* bulk string from attribute child — acceptable */
        }
    }
}

#[test]
fn dispatch_wrong_arity() {
    // GET with no key (only 1 element, arity=2)
    let (resp, _) = dispatch_wire(b"*1\r\n$3\r\nGET\r\n");
    assert!(matches!(resp, CommandResponse::Static(b) if b.starts_with(b"-ERR wrong number")));
}

#[test]
fn dispatch_quit_signals_close() {
    let (resp, close) = dispatch_wire(b"*1\r\n$4\r\nQUIT\r\n");
    assert!(matches!(resp, CommandResponse::Static(b) if b == b"+OK\r\n"));
    assert!(close);
}

#[test]
fn dispatch_uses_monotonic_clock_for_relative_expiry() {
    let mut reactor = test_reactor();
    let monotonic_now = 5 * 1_000_000_000;
    let unix_now = 4_102_444_800 * 1_000_000_000;
    reactor.cached_nanos = monotonic_now;
    reactor.cached_unix_nanos = unix_now;

    let (resp, close) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$3\r\nSET\r\n$7\r\nsession\r\n$5\r\ntoken\r\n",
    );
    assert!(matches!(resp, CommandResponse::Static(b) if b == b"+OK\r\n"));
    assert!(!close);

    let (resp, close) = dispatch_reactor_wire(
        &mut reactor,
        b"*3\r\n$7\r\nPEXPIRE\r\n$7\r\nsession\r\n$1\r\n1\r\n",
    );
    assert!(matches!(resp, CommandResponse::Static(b) if b == b":1\r\n"));
    assert!(!close);

    let tape =
        RespTape::parse_pipeline(b"*2\r\n$3\r\nGET\r\n$7\r\nsession\r\n").expect("valid RESP");
    let frame = tape.iter().next().expect("at least one frame");
    let expired = execute_command(
        &reactor.keyspace,
        b"GET",
        &frame,
        CommandClock::new(monotonic_now + 2_000_000, unix_now + 2_000_000),
    )
    .expect("GET is implemented");
    assert!(matches!(expired.response, CmdResult::Static(b) if b == b"$-1\r\n"));
}

#[test]
fn config_get_reads_runtime_eviction_state() {
    let mut reactor = test_reactor();
    reactor.keyspace.set_max_memory(4096);
    reactor
        .keyspace
        .set_eviction_policy(EvictionPolicy::VolatileTtl);

    let (resp, close) = handle_config_wire(
        &mut reactor,
        b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$9\r\nmaxmemory\r\n",
    );
    assert!(!close);
    expect_config_pair(resp, b"maxmemory", b"4096");

    let (resp, close) = handle_config_wire(
        &mut reactor,
        b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$16\r\nmaxmemory-policy\r\n",
    );
    assert!(!close);
    expect_config_pair(resp, b"maxmemory-policy", b"volatile-ttl");
}

#[test]
fn config_set_updates_runtime_eviction_state() {
    let mut reactor = test_reactor();

    let (resp, close) = handle_config_wire(
        &mut reactor,
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$9\r\nmaxmemory\r\n$4\r\n8192\r\n",
    );
    assert!(matches!(resp, CommandResponse::Static(b) if b == b"+OK\r\n"));
    assert!(!close);
    assert_eq!(reactor.keyspace.max_memory(), 8192);

    let (resp, close) = handle_config_wire(
        &mut reactor,
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$16\r\nmaxmemory-policy\r\n$15\r\nvolatile-random\r\n",
    );
    assert!(matches!(resp, CommandResponse::Static(b) if b == b"+OK\r\n"));
    assert!(!close);
    assert_eq!(
        reactor.keyspace.eviction_policy(),
        EvictionPolicy::VolatileRandom
    );
}

#[test]
fn config_set_accepts_lfu_policy() {
    let mut reactor = test_reactor();
    reactor
        .keyspace
        .set_eviction_policy(EvictionPolicy::NoEviction);

    let (resp, close) = handle_config_wire(
        &mut reactor,
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$16\r\nmaxmemory-policy\r\n$11\r\nallkeys-lfu\r\n",
    );
    assert!(matches!(resp, CommandResponse::Static(b) if b == b"+OK\r\n"));
    assert!(!close);
    assert_eq!(
        reactor.keyspace.eviction_policy(),
        EvictionPolicy::AllKeysLfu
    );
}

#[test]
fn reactor_rejects_buffer_count_below_connection_minimum() {
    let config = ReactorConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 4,
        buffer_count: 3,
        ..Default::default()
    };

    let error = match Reactor::new(0, config, Arc::new(ShutdownCoordinator::new(1))) {
        Ok(_) => panic!("reactor creation should fail when buffer_count < max_connections"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("buffer_count"));
}

#[test]
fn bgrewriteaof_is_disabled_for_alpha() {
    let mut reactor = test_reactor();

    let (resp, close) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$12\r\nBGREWRITEAOF\r\n");

    assert!(!close);
    assert_eq!(response_bytes(resp), RESP_ERR_BGREWRITEAOF_DISABLED);
}

#[test]
fn fixed_buffer_policy_rejects_strict_oversized_range() {
    let caps = BackendCapabilities {
        fixed_buffers: true,
        sqpoll: false,
        multishot_accept: false,
        accept4: false,
        close_opcode: false,
        async_cancel: true,
        nonblocking_drain: true,
    };

    let error = Reactor::fixed_buffer_policy(
        FixedBufferRegistrationMode::Auto,
        IoBackendMode::Uring,
        BackendKind::Test,
        caps,
        FixedBufferId::MAX_BUFFER_COUNT + 1,
    )
    .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("exceeds fixed-buffer"));
}

#[test]
fn fixed_buffer_policy_disables_auto_oversized_range() {
    let caps = BackendCapabilities {
        fixed_buffers: true,
        sqpoll: false,
        multishot_accept: false,
        accept4: false,
        close_opcode: false,
        async_cancel: true,
        nonblocking_drain: true,
    };

    let policy = Reactor::fixed_buffer_policy(
        FixedBufferRegistrationMode::Auto,
        IoBackendMode::Auto,
        BackendKind::Test,
        caps,
        FixedBufferId::MAX_BUFFER_COUNT + 1,
    )
    .unwrap();

    assert_eq!(policy, FixedBufferPolicy::Disabled);
}

#[test]
fn fixed_buffer_policy_skips_non_fixed_backends() {
    let caps = BackendCapabilities {
        fixed_buffers: false,
        sqpoll: false,
        multishot_accept: false,
        accept4: false,
        close_opcode: false,
        async_cancel: true,
        nonblocking_drain: true,
    };

    let policy = Reactor::fixed_buffer_policy(
        FixedBufferRegistrationMode::Auto,
        IoBackendMode::Auto,
        BackendKind::Test,
        caps,
        FixedBufferId::MAX_BUFFER_COUNT + 1,
    )
    .unwrap();

    assert_eq!(policy, FixedBufferPolicy::Disabled);
}

#[test]
fn fixed_buffer_policy_off_disables_capable_backend_registration() {
    let caps = BackendCapabilities {
        fixed_buffers: true,
        sqpoll: false,
        multishot_accept: false,
        accept4: false,
        close_opcode: false,
        async_cancel: true,
        nonblocking_drain: true,
    };

    let policy = Reactor::fixed_buffer_policy(
        FixedBufferRegistrationMode::Off,
        IoBackendMode::Uring,
        BackendKind::Test,
        caps,
        1024,
    )
    .unwrap();

    assert_eq!(policy, FixedBufferPolicy::Disabled);
}

#[test]
fn fixed_buffer_policy_on_requires_capable_backend() {
    let caps = BackendCapabilities {
        fixed_buffers: false,
        sqpoll: false,
        multishot_accept: false,
        accept4: false,
        close_opcode: false,
        async_cancel: true,
        nonblocking_drain: true,
    };

    let error = Reactor::fixed_buffer_policy(
        FixedBufferRegistrationMode::On,
        IoBackendMode::Auto,
        BackendKind::Test,
        caps,
        1024,
    )
    .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("does not support"));
}

#[test]
fn parser_resume_counter_moves_on_incomplete_frame() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let mut reactor = test_reactor_with_backend(state);
    let conn_id = insert_test_connection(&mut reactor, 123, b"*2\r\n$3\r\nGET\r\n");

    reactor.process_commands(conn_id, 123);

    let runtime = reactor.keyspace.runtime_metrics();
    assert_eq!(runtime.parser_resumes, 1);
    assert_eq!(runtime.writev_chunks, 0);
    assert!(reactor.inflight_ops[conn_id].read);
}

#[test]
fn command_budget_yields_pipeline_and_resumes_after_write() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let mut reactor = test_reactor_with_backend(Arc::clone(&state));
    reactor.config.budgets.command = CommandBudget::new(2).unwrap();
    let mut pipeline = Vec::new();
    for _ in 0..3 {
        pipeline.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
    }
    let conn_id = insert_test_connection(&mut reactor, 123, &pipeline);

    reactor.process_commands(conn_id, 123);

    let first_token = {
        let state = state.lock().unwrap();
        assert_eq!(state.writevs.len(), 1);
        assert_eq!(state.writevs[0].1, 2);
        state.writevs[0].0
    };
    reactor.flush_local_runtime_metrics();
    let runtime = reactor.keyspace.runtime_metrics();
    assert_eq!(runtime.command_budget_exhaustions, 1);
    assert_eq!(runtime.yielded_connections, 1);
    assert_eq!(runtime.command_batch_total, 2);
    assert_eq!(
        reactor.connections.get(conn_id).unwrap().read_buf_len as usize,
        b"*1\r\n$4\r\nPING\r\n".len()
    );

    reactor.handle_completion(&Completion {
        token: first_token,
        result: (RESP_PONG.len() * 2) as i32,
        flags: 0,
    });

    let state = state.lock().unwrap();
    assert_eq!(state.writevs.len(), 2);
    assert_eq!(state.writevs[1].1, 1);
    drop(state);
    reactor.flush_local_runtime_metrics();
    let runtime = reactor.keyspace.runtime_metrics();
    assert_eq!(runtime.command_batch_total, 3);
}

#[test]
fn large_bulk_value_streams_across_read_buffer() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let config = ReactorConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 8,
        buffer_size: 4096,
        max_request_bytes: 16 * 1024,
        buffer_count: 16,
        ..Default::default()
    };
    let mut reactor = test_reactor_with_backend_config(Arc::clone(&state), config);
    let value = vec![b'v'; 6000];
    let wire = resp_command(&[b"SET", b"large", value.as_slice()]);
    assert!(wire.len() > reactor.buffer_pool.buffer_size());

    let first = reactor.buffer_pool.buffer_size();
    let conn_id = insert_test_connection(&mut reactor, 123, &wire[..first]);
    reactor.process_commands(conn_id, 123);

    assert_eq!(reactor.connections.get(conn_id).unwrap().read_buf_len, 0);
    assert_eq!(reactor.command_accumulators[conn_id].bytes.len(), first);
    assert_eq!(state.lock().unwrap().reads.len(), 1);

    let tail = &wire[first..];
    copy_into_read_buffer(&mut reactor, conn_id, tail);
    reactor.handle_read(
        conn_id,
        &Completion {
            token: CompletionToken::from_raw(0),
            result: tail.len() as i32,
            flags: 0,
        },
    );

    assert!(reactor.command_accumulators[conn_id].is_empty());
    assert_eq!(pending_writev_bytes(&reactor, conn_id), b"+OK\r\n");
    assert_eq!(keyspace_get_response(&reactor.keyspace, b"large"), {
        let mut expected = Vec::new();
        push_resp_bulk_string(&mut expected, value.as_slice());
        expected
    });
}

#[test]
fn oversized_accumulated_request_returns_error_and_closes() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let config = ReactorConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 8,
        buffer_size: 4096,
        max_request_bytes: 5000,
        buffer_count: 16,
        ..Default::default()
    };
    let mut reactor = test_reactor_with_backend_config(Arc::clone(&state), config);
    let value = vec![b'v'; 6000];
    let wire = resp_command(&[b"SET", b"too-large", value.as_slice()]);
    let first = reactor.buffer_pool.buffer_size();
    let conn_id = insert_test_connection(&mut reactor, 123, &wire[..first]);
    reactor.process_commands(conn_id, 123);

    let tail = &wire[first..];
    copy_into_read_buffer(&mut reactor, conn_id, tail);
    reactor.handle_read(
        conn_id,
        &Completion {
            token: CompletionToken::from_raw(0),
            result: tail.len() as i32,
            flags: 0,
        },
    );

    assert_eq!(
        pending_writev_bytes(&reactor, conn_id),
        RESP_ERR_REQUEST_TOO_LARGE
    );
    assert!(
        reactor.connections.get(conn_id).unwrap().flags & ConnectionFlags::CLOSE_AFTER_WRITE != 0
    );
    assert_eq!(
        keyspace_get_response(&reactor.keyspace, b"too-large"),
        b"$-1\r\n"
    );
    assert_eq!(reactor.keyspace.runtime_metrics().request_cap_exceeded, 1);
}

#[test]
fn pending_response_cap_bounds_deep_pipeline_slow_reader() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let config = ReactorConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 8,
        buffer_size: 4096,
        buffer_count: 16,
        connection_caps: ConnectionMemoryCaps {
            max_pending_response_bytes: 96,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut reactor = test_reactor_with_backend_config(Arc::clone(&state), config);
    let mut wire = Vec::new();
    for _ in 0..128 {
        wire.extend_from_slice(&resp_command(&[b"PING"]));
    }

    let conn_id = insert_test_connection(&mut reactor, 72, &wire);
    reactor.process_commands(conn_id, 72);

    let written = pending_writev_bytes(&reactor, conn_id);
    assert!(written.len() <= 96, "pending response bytes exceeded cap");
    assert!(written.starts_with(b"+PONG\r\n"));
    let conn = reactor.connections.get(conn_id).unwrap();
    assert_ne!(conn.flags & ConnectionFlags::CLOSE_AFTER_WRITE, 0);
    assert_eq!(reactor.keyspace.runtime_metrics().response_cap_exceeded, 1);
}

#[test]
fn capped_pipeline_client_does_not_block_small_clients() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let config = ReactorConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 128,
        buffer_size: 4096,
        buffer_count: 128,
        connection_caps: ConnectionMemoryCaps {
            max_pending_response_bytes: 96,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut reactor = test_reactor_with_backend_config(Arc::clone(&state), config);
    let mut deep_pipeline = Vec::new();
    for _ in 0..128 {
        deep_pipeline.extend_from_slice(&resp_command(&[b"PING"]));
    }
    let capped_id = insert_test_connection(&mut reactor, 80, &deep_pipeline);
    reactor.process_commands(capped_id, 80);
    assert_eq!(reactor.keyspace.runtime_metrics().response_cap_exceeded, 1);

    for idx in 0..100 {
        let fd = 1000 + idx as RawFd;
        let conn_id = insert_test_connection(&mut reactor, fd, &resp_command(&[b"PING"]));
        reactor.process_commands(conn_id, fd);
        assert_eq!(pending_writev_bytes(&reactor, conn_id), b"+PONG\r\n");
    }

    assert_eq!(reactor.keyspace.runtime_metrics().response_cap_exceeded, 1);
    assert!(
        state.lock().unwrap().writevs.len() >= 101,
        "capped client should not prevent later latency-sensitive writes"
    );
}

#[test]
fn multi_queue_command_cap_dirties_transaction() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let config = ReactorConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 8,
        buffer_size: 4096,
        buffer_count: 16,
        connection_caps: ConnectionMemoryCaps {
            max_multi_queue_commands: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut reactor = test_reactor_with_backend_config(Arc::clone(&state), config);
    let mut wire = Vec::new();
    wire.extend_from_slice(&resp_command(&[b"MULTI"]));
    wire.extend_from_slice(&resp_command(&[b"SET", b"a", b"1"]));
    wire.extend_from_slice(&resp_command(&[b"SET", b"b", b"2"]));
    wire.extend_from_slice(&resp_command(&[b"SET", b"c", b"3"]));
    wire.extend_from_slice(&resp_command(&[b"EXEC"]));

    let conn_id = insert_test_connection(&mut reactor, 73, &wire);
    reactor.process_commands(conn_id, 73);

    let written = pending_writev_bytes(&reactor, conn_id);
    assert!(
        written
            .windows(RESP_ERR_TX_QUEUE_FULL.len())
            .any(|w| w == RESP_ERR_TX_QUEUE_FULL)
    );
    assert!(
        written
            .windows(RESP_ERR_EXECABORT.len())
            .any(|w| w == RESP_ERR_EXECABORT)
    );
    assert_eq!(
        reactor
            .keyspace
            .runtime_metrics()
            .multi_queue_command_cap_exceeded,
        1
    );
}

#[test]
fn multi_queue_byte_cap_dirties_transaction() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let config = ReactorConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 8,
        buffer_size: 4096,
        buffer_count: 16,
        connection_caps: ConnectionMemoryCaps {
            max_multi_queue_bytes: 64,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut reactor = test_reactor_with_backend_config(Arc::clone(&state), config);
    let value = vec![b'v'; 256];
    let mut wire = Vec::new();
    wire.extend_from_slice(&resp_command(&[b"MULTI"]));
    wire.extend_from_slice(&resp_command(&[b"SET", b"a", value.as_slice()]));
    wire.extend_from_slice(&resp_command(&[b"EXEC"]));

    let conn_id = insert_test_connection(&mut reactor, 74, &wire);
    reactor.process_commands(conn_id, 74);

    let written = pending_writev_bytes(&reactor, conn_id);
    assert!(
        written
            .windows(RESP_ERR_TX_QUEUE_BYTES.len())
            .any(|w| w == RESP_ERR_TX_QUEUE_BYTES)
    );
    assert!(
        written
            .windows(RESP_ERR_EXECABORT.len())
            .any(|w| w == RESP_ERR_EXECABORT)
    );
    assert_eq!(
        reactor
            .keyspace
            .runtime_metrics()
            .multi_queue_bytes_cap_exceeded,
        1
    );
}

#[test]
fn watch_registration_cap_rejects_without_partial_registration() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let config = ReactorConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 8,
        buffer_size: 4096,
        buffer_count: 16,
        connection_caps: ConnectionMemoryCaps {
            max_watch_registrations: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut reactor = test_reactor_with_backend_config(Arc::clone(&state), config);
    let wire = resp_command(&[b"WATCH", b"a", b"b", b"c"]);

    let conn_id = insert_test_connection(&mut reactor, 75, &wire);
    reactor.process_commands(conn_id, 75);

    let written = pending_writev_bytes(&reactor, conn_id);
    assert!(
        written
            .windows(RESP_ERR_WATCH_LIMIT.len())
            .any(|w| w == RESP_ERR_WATCH_LIMIT)
    );
    assert!(reactor.transaction_states[conn_id].watched.is_empty());
    assert_eq!(reactor.keyspace.runtime_metrics().watch_cap_exceeded, 1);
}

#[test]
fn writev_chunk_cap_compacts_deferred_segments() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let config = ReactorConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 8,
        buffer_size: 4096,
        buffer_count: 16,
        budgets: ReactorBudgets {
            writev: WritevBudget::new(8).unwrap(),
            ..Default::default()
        },
        connection_caps: ConnectionMemoryCaps {
            max_writev_chunks: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut reactor = test_reactor_with_backend_config(Arc::clone(&state), config);
    let mut wire = Vec::new();
    for _ in 0..32 {
        wire.extend_from_slice(&resp_command(&[b"PING"]));
    }

    let conn_id = insert_test_connection(&mut reactor, 76, &wire);
    reactor.process_commands(conn_id, 76);

    let writes = state.lock().unwrap().writevs.clone();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].1, 1, "chunk cap should compact to one iovec");
    assert_eq!(
        reactor.keyspace.runtime_metrics().writev_chunk_cap_exceeded,
        1
    );
    assert_eq!(
        pending_writev_bytes(&reactor, conn_id).len(),
        32 * b"+PONG\r\n".len()
    );
}

#[test]
fn capped_pending_write_can_close_after_terminal_write() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let config = ReactorConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 8,
        buffer_size: 4096,
        buffer_count: 16,
        connection_caps: ConnectionMemoryCaps {
            max_pending_response_bytes: 96,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut reactor = test_reactor_with_backend_config(Arc::clone(&state), config);
    let mut wire = Vec::new();
    for _ in 0..128 {
        wire.extend_from_slice(&resp_command(&[b"PING"]));
    }

    let conn_id = insert_test_connection(&mut reactor, 77, &wire);
    reactor.process_commands(conn_id, 77);
    let write_token = state.lock().unwrap().writevs[0].0;
    let total = reactor.connections.get(conn_id).unwrap().write_buf_len as i32;

    reactor.handle_write(
        conn_id,
        OpType::Writev,
        &Completion {
            token: write_token,
            result: total,
            flags: 0,
        },
    );

    assert!(reactor.connections.is_closing(conn_id));
    assert!(reactor.writev_states[conn_id].remaining_iovecs().is_empty());
}

#[test]
fn overload_response_pressure_disables_and_resumes_reads() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let config = ReactorConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 8,
        buffer_size: 4096,
        buffer_count: 16,
        overload_policy: ReactorOverloadPolicy {
            read_disable_pending_response_bytes: 16,
            writev_backlog_bytes: 16,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut reactor = test_reactor_with_backend_config(Arc::clone(&state), config);
    let pressure_id = insert_test_connection(&mut reactor, 201, b"");
    let victim_id = insert_test_connection(&mut reactor, 202, b"");

    reactor.set_connection_write_len(pressure_id, 32);
    reactor.submit_read_for(victim_id, 202);

    assert!(state.lock().unwrap().reads.is_empty());
    assert_eq!(reactor.keyspace.runtime_metrics().overload_read_disabled, 1);

    reactor.set_connection_write_len(pressure_id, 0);
    reactor.run_admission_resume_slice();

    assert_eq!(state.lock().unwrap().reads.len(), 1);
    assert_eq!(reactor.keyspace.runtime_metrics().overload_read_resumed, 1);
}

#[test]
fn overload_accept_threshold_drops_new_connection_without_slot() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let config = ReactorConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 2,
        buffer_size: 4096,
        buffer_count: 4,
        overload_policy: ReactorOverloadPolicy {
            accept_throttle_connection_percent: 50,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut reactor = test_reactor_with_backend_config(Arc::clone(&state), config);
    let _existing = insert_test_connection(&mut reactor, 210, b"");
    let (server_fd, peer_fd) = socket_pair();

    reactor.handle_accepted_fd(server_fd);

    assert_eq!(reactor.connection_count(), 1);
    assert!(peer_observes_eof(peer_fd));
    // SAFETY: peer_fd is not owned by the reactor.
    unsafe {
        libc::close(peer_fd);
    }
    let runtime = reactor.keyspace.runtime_metrics();
    assert_eq!(runtime.overload_accept_throttled, 1);
    assert_eq!(runtime.overload_connections_dropped, 1);
}

#[test]
fn overload_aof_backlog_defers_write_but_allows_read() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let config = ReactorConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 8,
        buffer_size: 4096,
        buffer_count: 16,
        overload_policy: ReactorOverloadPolicy {
            aof_pending_bytes: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut reactor = test_reactor_with_backend_config(Arc::clone(&state), config);
    let path = temp_aof_path("overload-aof-defer");
    enable_test_aof(&mut reactor, &path);

    let (resp, close) = dispatch_reactor_wire(&mut reactor, &resp_command(&[b"SET", b"a", b"1"]));
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    assert!(!close);
    assert!(reactor.aof_pending_bytes() >= 1);

    let read_id = insert_test_connection(&mut reactor, 220, &resp_command(&[b"GET", b"a"]));
    reactor.process_commands(read_id, 220);
    assert_eq!(pending_writev_bytes(&reactor, read_id), b"$1\r\n1\r\n");

    let write_id = insert_test_connection(&mut reactor, 221, &resp_command(&[b"SET", b"b", b"2"]));
    reactor.process_commands(write_id, 221);
    assert!(pending_writev_bytes(&reactor, write_id).is_empty());
    assert_ne!(reactor.connections.get(write_id).unwrap().read_buf_len, 0);
    assert_eq!(keyspace_get_response(&reactor.keyspace, b"b"), b"$-1\r\n");
    assert_eq!(
        reactor.keyspace.runtime_metrics().overload_command_deferred,
        1
    );

    reactor
        .aof_writer
        .as_mut()
        .unwrap()
        .writer
        .flush_and_sync()
        .unwrap();
    reactor.run_admission_resume_slice();

    assert_eq!(pending_writev_bytes(&reactor, write_id), b"+OK\r\n");
    assert_eq!(
        keyspace_get_response(&reactor.keyspace, b"b"),
        b"$1\r\n2\r\n"
    );
    assert_eq!(
        reactor.keyspace.runtime_metrics().overload_command_resumed,
        1
    );
    cleanup(&path);
}

#[test]
fn overload_close_storm_debt_disables_reads_until_drain_progress() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let config = ReactorConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 8,
        buffer_size: 4096,
        buffer_count: 16,
        connection_timeout: 0,
        overload_policy: ReactorOverloadPolicy {
            maintenance_debt: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut reactor = test_reactor_with_backend_config(Arc::clone(&state), config);
    #[cfg(feature = "profile-telemetry")]
    {
        reactor.config.telemetry_mode = RuntimeTelemetryMode::Profile;
        reactor
            .keyspace
            .set_runtime_telemetry_mode(RuntimeTelemetryMode::Profile);
    }
    let closing_id = insert_test_connection(&mut reactor, 230, b"");
    let victim_id = insert_test_connection(&mut reactor, 231, b"");
    reactor
        .connections
        .transition_to_closing(closing_id)
        .unwrap();
    reactor.close_started_nanos[closing_id] = 1;
    reactor.maybe_finalize_close(closing_id);

    reactor.submit_read_for(victim_id, 231);
    assert!(state.lock().unwrap().reads.is_empty());
    assert_eq!(reactor.keyspace.runtime_metrics().overload_read_disabled, 1);

    let run = reactor.run_close_drain_slice();
    assert!(run.did_work);
    reactor.run_admission_resume_slice();

    assert_eq!(state.lock().unwrap().reads.len(), 1);
    let runtime = reactor.keyspace.runtime_metrics();
    #[cfg(feature = "profile-telemetry")]
    assert!(runtime.close_drain_nanos_total >= 1);
    #[cfg(not(feature = "profile-telemetry"))]
    assert_eq!(runtime.close_drain_nanos_total, 0);
    assert_eq!(runtime.overload_read_resumed, 1);
}

#[test]
fn overload_ttl_expiry_debt_disables_reads_until_expiry_progress() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let config = ReactorConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 8,
        buffer_size: 4096,
        buffer_count: 16,
        connection_timeout: 0,
        overload_policy: ReactorOverloadPolicy {
            maintenance_debt: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut reactor = test_reactor_with_backend_config(Arc::clone(&state), config);
    let key = b"overload:ttl";
    let wire = resp_command(&[b"SET", key, b"value", b"PX", b"1"]);
    let (resp, close) = dispatch_reactor_wire(&mut reactor, &wire);
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    assert!(!close);

    let victim_id = insert_test_connection(&mut reactor, 232, b"");
    reactor.cached_nanos = reactor.cached_nanos.saturating_add(2_000_000);
    reactor.next_active_expiry_nanos = 0;

    reactor.submit_read_for(victim_id, 232);
    assert!(state.lock().unwrap().reads.is_empty());
    assert_eq!(reactor.keyspace.runtime_metrics().overload_read_disabled, 1);

    let shard_idx = reactor.keyspace.shard_index(key);
    for _ in 0..512 {
        reactor.next_active_expiry_nanos = 0;
        reactor.expiry_shard_cursor = shard_idx;
        let _ = reactor.run_active_expiry_slice();
        if keyspace_get_response(&reactor.keyspace, key) == b"$-1\r\n" {
            break;
        }
    }
    assert_eq!(keyspace_get_response(&reactor.keyspace, key), b"$-1\r\n");

    reactor.run_admission_resume_slice();
    assert_eq!(state.lock().unwrap().reads.len(), 1);
    assert_eq!(reactor.keyspace.runtime_metrics().overload_read_resumed, 1);
}

#[test]
fn overload_eviction_pressure_debt_disables_reads_until_eviction_progress() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let config = ReactorConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 8,
        buffer_size: 4096,
        buffer_count: 16,
        connection_timeout: 0,
        overload_policy: ReactorOverloadPolicy {
            maintenance_debt: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut reactor = test_reactor_with_backend_config(Arc::clone(&state), config);
    let value = vec![b'v'; 512];
    for idx in 0..128usize {
        let key = format!("overload:evict:{idx}");
        let wire = resp_command(&[b"SET", key.as_bytes(), value.as_slice()]);
        let (resp, close) = dispatch_reactor_wire(&mut reactor, &wire);
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        assert!(!close);
    }

    let used = reactor.keyspace.memory_used();
    assert!(used > 0);
    reactor
        .keyspace
        .configure_eviction(used.saturating_sub(1), EvictionPolicy::AllKeysLru);
    assert!(reactor.keyspace.eviction_pressure_active());

    let victim_id = insert_test_connection(&mut reactor, 233, b"");
    reactor.submit_read_for(victim_id, 233);
    assert!(state.lock().unwrap().reads.is_empty());
    assert_eq!(reactor.keyspace.runtime_metrics().overload_read_disabled, 1);

    for _ in 0..reactor.keyspace.num_shards().saturating_mul(8) {
        if !reactor.keyspace.eviction_pressure_active() {
            break;
        }
        let _ = reactor.run_eviction_pressure_slice();
    }
    assert!(!reactor.keyspace.eviction_pressure_active());

    reactor.run_admission_resume_slice();
    assert_eq!(state.lock().unwrap().reads.len(), 1);
    assert_eq!(reactor.keyspace.runtime_metrics().overload_read_resumed, 1);
}

#[test]
fn transaction_and_watch_state_survive_command_budget_requeue() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let mut reactor = test_reactor_with_backend(Arc::clone(&state));
    reactor.config.budgets.command = CommandBudget::new(2).unwrap();
    let mut wire = resp_command(&[b"WATCH", b"watched"]);
    wire.extend_from_slice(&resp_command(&[b"MULTI"]));
    wire.extend_from_slice(&resp_command(&[b"SET", b"queued", b"value"]));
    wire.extend_from_slice(&resp_command(&[b"EXEC"]));
    let conn_id = insert_test_connection(&mut reactor, 123, &wire);

    reactor.process_commands(conn_id, 123);

    assert_eq!(pending_writev_bytes(&reactor, conn_id), b"+OK\r\n+OK\r\n");
    assert!(reactor.transaction_states[conn_id].queueing);
    assert_eq!(reactor.transaction_states[conn_id].watched.len(), 1);
    assert_eq!(
        reactor
            .keyspace
            .runtime_metrics()
            .command_budget_exhaustions,
        1
    );

    let (resp, _) = dispatch_reactor_wire_on(
        &mut reactor,
        1,
        &resp_command(&[b"SET", b"watched", b"changed"]),
    );
    assert_eq!(response_bytes(resp), b"+OK\r\n");

    let first_token = state.lock().unwrap().writevs[0].0;
    reactor.handle_completion(&Completion {
        token: first_token,
        result: b"+OK\r\n+OK\r\n".len() as i32,
        flags: 0,
    });

    assert_eq!(
        pending_writev_bytes(&reactor, conn_id),
        b"+QUEUED\r\n*-1\r\n"
    );
    assert!(!reactor.transaction_states[conn_id].queueing);
    assert!(reactor.transaction_states[conn_id].watched.is_empty());
    assert_eq!(
        keyspace_get_response(&reactor.keyspace, b"queued"),
        b"$-1\r\n"
    );
}

#[test]
fn pipeline_depths_resume_in_response_order() {
    for depth in [1usize, 16, 256, 4096] {
        let state = Arc::new(Mutex::new(MockBackendState::default()));
        let budgets = ReactorBudgets {
            command: CommandBudget::new(1024).unwrap(),
            ..Default::default()
        };
        let config = ReactorConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_connections: 8,
            buffer_size: 64 * 1024,
            max_request_bytes: 128 * 1024,
            buffer_count: 16,
            budgets,
            ..Default::default()
        };
        let mut reactor = test_reactor_with_backend_config(Arc::clone(&state), config);
        let mut wire = Vec::new();
        for _ in 0..depth {
            wire.extend_from_slice(&resp_command(&[b"PING"]));
        }
        let conn_id = insert_test_connection(&mut reactor, 123, &wire);
        reactor.process_commands(conn_id, 123);

        let mut observed = Vec::new();
        let mut write_index = 0usize;
        loop {
            let pending = pending_writev_bytes(&reactor, conn_id);
            if pending.is_empty() {
                break;
            }
            observed.extend_from_slice(&pending);
            let token = {
                let state = state.lock().unwrap();
                state.writevs[write_index].0
            };
            write_index += 1;
            reactor.handle_completion(&Completion {
                token,
                result: pending.len() as i32,
                flags: 0,
            });
        }

        let mut expected = Vec::new();
        for _ in 0..depth {
            expected.extend_from_slice(RESP_PONG);
        }
        assert_eq!(observed, expected, "pipeline depth {depth}");
    }
}

#[test]
fn completion_budget_requeues_overflow_in_reactor_order() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let mut reactor = test_reactor_with_backend(state);
    let mut budget = SliceBudget::new(1);
    let mut completions = vec![
        Completion {
            token: CompletionToken::from_raw(0xFE),
            result: 0,
            flags: 0,
        },
        Completion {
            token: CompletionToken::from_raw(0xFF),
            result: 0,
            flags: 0,
        },
    ];

    assert!(reactor.process_completion_vec(&mut completions, &mut budget));
    assert_eq!(reactor.invalid_completion_tokens, 1);
    assert_eq!(reactor.pending_completions.len(), 1);

    let mut budget = SliceBudget::new(1);
    assert!(reactor.process_pending_completion_queue(&mut budget));
    assert_eq!(reactor.invalid_completion_tokens, 2);
    assert!(reactor.pending_completions.is_empty());
}

#[test]
fn writev_and_response_byte_counters_move_on_response_submit() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let mut reactor = test_reactor_with_backend(state);
    let conn_id = insert_test_connection(&mut reactor, 123, b"*1\r\n$4\r\nPING\r\n");

    reactor.process_commands(conn_id, 123);

    reactor.flush_local_runtime_metrics();
    let runtime = reactor.keyspace.runtime_metrics();
    assert_eq!(runtime.writev_chunks, 1);
    assert!(runtime.writev_iovecs_total >= 1);
    assert!(runtime.writev_iovecs_max >= 1);
    assert_eq!(
        runtime.queued_response_bytes_total,
        b"+PONG\r\n".len() as u64
    );
    assert_eq!(runtime.queued_response_bytes_max, b"+PONG\r\n".len() as u64);
    assert!(reactor.inflight_ops[conn_id].writev);
}

#[test]
fn iov_max_plus_one_pipeline_is_chunked_before_backend_submit() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let mut reactor = test_reactor_with_backend(Arc::clone(&state));
    let mut pipeline = Vec::with_capacity(16 * (IovecBatch::MAX_SEGMENTS + 1));
    for _ in 0..=IovecBatch::MAX_SEGMENTS {
        pipeline.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
    }

    let conn_id = insert_test_connection(&mut reactor, 123, &pipeline);
    reactor.process_commands(conn_id, 123);

    let first_token = {
        let state = state.lock().unwrap();
        assert_eq!(state.writevs.len(), 1);
        assert_eq!(state.writevs[0].1, IovecBatch::MAX_SEGMENTS);
        state.writevs[0].0
    };

    reactor.handle_completion(&Completion {
        token: first_token,
        result: (RESP_PONG.len() * IovecBatch::MAX_SEGMENTS) as i32,
        flags: 0,
    });

    let state = state.lock().unwrap();
    assert_eq!(state.writevs.len(), 2);
    assert_eq!(state.writevs[1].1, 1);
}

#[test]
fn writev_budget_chunks_before_backend_iov_limit() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let mut reactor = test_reactor_with_backend(Arc::clone(&state));
    reactor.config.budgets.writev = WritevBudget::new(2).unwrap();
    let mut pipeline = Vec::new();
    for _ in 0..3 {
        pipeline.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
    }

    let conn_id = insert_test_connection(&mut reactor, 123, &pipeline);
    reactor.process_commands(conn_id, 123);

    let first_token = {
        let state = state.lock().unwrap();
        assert_eq!(state.writevs.len(), 1);
        assert_eq!(state.writevs[0].1, 2);
        state.writevs[0].0
    };
    assert_eq!(
        reactor.keyspace.runtime_metrics().writev_budget_exhaustions,
        1
    );

    reactor.handle_completion(&Completion {
        token: first_token,
        result: (RESP_PONG.len() * 2) as i32,
        flags: 0,
    });

    let state = state.lock().unwrap();
    assert_eq!(state.writevs.len(), 2);
    assert_eq!(state.writevs[1].1, 1);
}

#[test]
fn writev_eagain_completion_resubmits_without_advancing() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let mut reactor = test_reactor_with_backend(Arc::clone(&state));
    let mut pipeline = Vec::new();
    for _ in 0..3 {
        pipeline.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
    }

    let conn_id = insert_test_connection(&mut reactor, 123, &pipeline);
    reactor.process_commands(conn_id, 123);
    let before = pending_writev_bytes(&reactor, conn_id);
    let first_token = state.lock().unwrap().writevs[0].0;

    reactor.handle_completion(&Completion {
        token: first_token,
        result: -libc::EAGAIN,
        flags: 0,
    });

    assert_eq!(pending_writev_bytes(&reactor, conn_id), before);
    assert!(reactor.inflight_ops[conn_id].writev);
    let state = state.lock().unwrap();
    assert_eq!(state.writevs.len(), 2);
    assert_eq!(state.writevs[1].1, 3);
}

#[test]
fn pending_writev_reuses_raw_iovec_capacity_after_finalize() {
    let mut pending = PendingWritev::new();
    pending.push_static(b"+PONG\r\n");
    pending.finalize();
    let warm_capacity = pending.raw_iovec_capacity();
    assert!(warm_capacity >= 1);

    pending.clear();
    pending.push_static(b"+PONG\r\n");
    pending.finalize();

    assert_eq!(pending.raw_iovec_capacity(), warm_capacity);
    assert_eq!(pending.remaining_len(), b"+PONG\r\n".len());
}

#[test]
fn pending_writev_advances_across_segments() {
    let mut pending = PendingWritev::new();
    pending.push_static(b"abc");
    pending.push_static(b"defg");
    pending.finalize();

    pending.advance(4);

    assert_eq!(pending.remaining_len(), 3);
    let remaining = pending.remaining_iovecs();
    assert_eq!(remaining.len(), 1);
    // SAFETY: remaining iovec points to static storage used in the test.
    let bytes = unsafe {
        std::slice::from_raw_parts(remaining[0].iov_base.cast::<u8>(), remaining[0].iov_len)
    };
    assert_eq!(bytes, b"efg");
}

#[test]
fn pending_writev_chunks_at_backend_iov_limit() {
    let mut pending = PendingWritev::new();
    for _ in 0..=IovecBatch::MAX_SEGMENTS {
        pending.push_static(b"x");
    }
    pending.finalize();

    let first = pending.remaining_iovec_batch(DEFAULT_WRITEV_BUDGET);
    assert_eq!(first.len(), IovecBatch::MAX_SEGMENTS);

    pending.advance(IovecBatch::MAX_SEGMENTS);
    let second = pending.remaining_iovec_batch(DEFAULT_WRITEV_BUDGET);
    assert_eq!(second.len(), 1);
    assert_eq!(pending.remaining_len(), 1);
}

#[test]
fn close_waits_for_inflight_io_before_releasing_buffers() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let mut reactor = test_reactor_with_backend(state.clone());
    #[cfg(feature = "profile-telemetry")]
    {
        reactor.config.telemetry_mode = RuntimeTelemetryMode::Profile;
        reactor
            .keyspace
            .set_runtime_telemetry_mode(RuntimeTelemetryMode::Profile);
    }

    let read_idx = reactor.buffer_pool.lease_index().unwrap();
    let write_idx = reactor.buffer_pool.lease_index().unwrap();
    let mut meta = ConnectionMeta::new(123, 0);
    meta.read_buf_offset = read_idx as u32;
    meta.write_buf_offset = write_idx as u32;
    let conn_id = reactor.connections.insert(meta);
    reactor.generations[conn_id] = 7;
    reactor.writev_states[conn_id].push_static(b"pending");
    reactor.writev_states[conn_id].finalize();
    reactor.inflight_ops[conn_id].read = true;
    reactor.inflight_ops[conn_id].writev = true;

    reactor.close_connection(conn_id);

    let cgen = reactor.generations[conn_id];
    let read_token = encode_token(conn_id, cgen, OpType::Read).unwrap();
    let writev_token = encode_token(conn_id, cgen, OpType::Writev).unwrap();
    let close_token = encode_token(conn_id, cgen, OpType::Close).unwrap();
    let read_cancel = CompletionToken::cancel(read_token).unwrap();
    let writev_cancel = CompletionToken::cancel(writev_token).unwrap();

    {
        let state = state.lock().unwrap();
        assert_eq!(
            state.cancels,
            vec![(read_token, read_cancel), (writev_token, writev_cancel)]
        );
        assert_eq!(state.closes, vec![close_token]);
    }
    assert!(reactor.connections.is_closing(conn_id));
    assert_eq!(reactor.buffer_pool.outstanding(), 2);

    reactor.handle_close(conn_id);
    assert!(reactor.connections.get(conn_id).is_some());
    assert_eq!(reactor.buffer_pool.outstanding(), 2);

    reactor.handle_completion(&Completion {
        token: read_cancel,
        result: 0,
        flags: 0,
    });
    reactor.handle_completion(&Completion {
        token: writev_cancel,
        result: 0,
        flags: 0,
    });
    assert!(reactor.connections.get(conn_id).is_some());
    assert_eq!(reactor.buffer_pool.outstanding(), 2);

    reactor.handle_read(
        conn_id,
        &Completion {
            token: read_token,
            result: -libc::ECANCELED,
            flags: 0,
        },
    );
    assert!(reactor.connections.get(conn_id).is_some());
    assert_eq!(reactor.buffer_pool.outstanding(), 2);

    reactor.handle_write(
        conn_id,
        OpType::Writev,
        &Completion {
            token: writev_token,
            result: -libc::ECANCELED,
            flags: 0,
        },
    );

    assert!(reactor.connections.get(conn_id).is_some());
    assert_eq!(reactor.buffer_pool.outstanding(), 2);
    assert_eq!(reactor.pending_close_finalization.len(), 1);
    let close_slice = reactor.run_close_drain_slice();
    assert!(close_slice.did_work);
    assert!(reactor.connections.get(conn_id).is_none());
    assert_eq!(reactor.buffer_pool.outstanding(), 0);
    assert_eq!(reactor.writev_states[conn_id].remaining_len(), 0);
    let runtime = reactor.keyspace.runtime_metrics();
    #[cfg(feature = "profile-telemetry")]
    {
        assert!(runtime.close_drain_nanos_total >= 1);
        assert!(runtime.close_drain_nanos_max >= 1);
    }
    #[cfg(not(feature = "profile-telemetry"))]
    {
        assert_eq!(runtime.close_drain_nanos_total, 0);
        assert_eq!(runtime.close_drain_nanos_max, 0);
    }
}

#[test]
fn close_drain_scheduler_respects_one_connection_slice() {
    let mut reactor = test_reactor();
    let (first_fd, first_peer) = socket_pair();
    let (second_fd, second_peer) = socket_pair();
    let first = insert_test_connection(&mut reactor, first_fd, b"");
    let second = insert_test_connection(&mut reactor, second_fd, b"");
    unsafe {
        libc::close(first_peer);
        libc::close(second_peer);
    }

    reactor.connections.transition_to_closing(first).unwrap();
    reactor.connections.transition_to_closing(second).unwrap();
    reactor.close_started_nanos[first] = 1;
    reactor.close_started_nanos[second] = 1;
    reactor.maybe_finalize_close(first);
    reactor.maybe_finalize_close(second);

    assert_eq!(reactor.pending_close_finalization.len(), 2);
    let first_slice = reactor.run_close_drain_slice();
    assert!(first_slice.did_work);
    assert_eq!(reactor.pending_close_finalization.len(), 1);
    assert_eq!(reactor.connection_count(), 1);

    let second_slice = reactor.run_close_drain_slice();
    assert!(second_slice.did_work);
    assert!(reactor.pending_close_finalization.is_empty());
    assert_eq!(reactor.connection_count(), 0);
}

#[test]
fn maintenance_budget_exhaustion_reports_pending_close_drain() {
    let mut reactor = test_reactor();
    reactor.config.budgets.maintenance = MaintenanceBudget::new(1).unwrap();
    let (first_fd, first_peer) = socket_pair();
    let (second_fd, second_peer) = socket_pair();
    let first = insert_test_connection(&mut reactor, first_fd, b"");
    let second = insert_test_connection(&mut reactor, second_fd, b"");
    unsafe {
        libc::close(first_peer);
        libc::close(second_peer);
    }

    reactor.connections.transition_to_closing(first).unwrap();
    reactor.connections.transition_to_closing(second).unwrap();
    reactor.close_started_nanos[first] = 1;
    reactor.close_started_nanos[second] = 1;
    reactor.maybe_finalize_close(first);
    reactor.maybe_finalize_close(second);

    reactor.run_maintenance_scheduler();

    assert_eq!(reactor.connection_count(), 1);
    assert_eq!(
        reactor
            .keyspace
            .runtime_metrics()
            .maintenance_budget_exhaustions,
        1
    );
}

#[test]
fn active_expiry_scheduler_records_bounded_slice() {
    let mut reactor = test_reactor();
    #[cfg(feature = "profile-telemetry")]
    {
        reactor.config.telemetry_mode = RuntimeTelemetryMode::Profile;
        reactor
            .keyspace
            .set_runtime_telemetry_mode(RuntimeTelemetryMode::Profile);
    }
    let key = b"maint:ttl";
    let wire = resp_command(&[b"SET", key, b"value", b"PX", b"1"]);
    let (resp, close) = dispatch_reactor_wire(&mut reactor, &wire);
    assert_eq!(response_bytes(resp), b"+OK\r\n");
    assert!(!close);

    let shard_idx = reactor.keyspace.shard_index(key);
    reactor.cached_nanos = reactor.cached_nanos.saturating_add(2_000_000);
    for _ in 0..512 {
        reactor.next_active_expiry_nanos = 0;
        reactor.expiry_shard_cursor = shard_idx;
        let run = reactor.run_active_expiry_slice();
        if keyspace_get_response(&reactor.keyspace, key) == b"$-1\r\n" {
            assert!(run.did_work);
            break;
        }
    }

    let runtime = reactor.keyspace.runtime_metrics();
    assert!(runtime.active_expiry_runs > 0);
    #[cfg(feature = "profile-telemetry")]
    assert!(runtime.active_expiry_nanos_total > 0);
    #[cfg(not(feature = "profile-telemetry"))]
    assert_eq!(runtime.active_expiry_nanos_total, 0);
    assert_eq!(keyspace_get_response(&reactor.keyspace, key), b"$-1\r\n");
}

#[test]
fn eviction_pressure_scheduler_records_bounded_slice() {
    let mut reactor = test_reactor();
    #[cfg(feature = "profile-telemetry")]
    {
        reactor.config.telemetry_mode = RuntimeTelemetryMode::Profile;
        reactor
            .keyspace
            .set_runtime_telemetry_mode(RuntimeTelemetryMode::Profile);
    }
    let value = vec![b'v'; 512];
    for idx in 0..128usize {
        let key = format!("maint:evict:{idx}");
        let wire = resp_command(&[b"SET", key.as_bytes(), value.as_slice()]);
        let (resp, close) = dispatch_reactor_wire(&mut reactor, &wire);
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        assert!(!close);
    }

    let used = reactor.keyspace.memory_used();
    assert!(used > 0);
    reactor
        .keyspace
        .configure_eviction(used / 2, EvictionPolicy::AllKeysLru);

    for _ in 0..reactor.keyspace.num_shards().saturating_mul(4) {
        if !reactor.keyspace.eviction_pressure_active() {
            break;
        }
        let _ = reactor.run_eviction_pressure_slice();
    }

    let runtime = reactor.keyspace.runtime_metrics();
    assert!(runtime.eviction_slots_sampled > 0);
    #[cfg(feature = "profile-telemetry")]
    assert!(runtime.eviction_nanos_total > 0);
    #[cfg(not(feature = "profile-telemetry"))]
    assert_eq!(runtime.eviction_nanos_total, 0);
}

#[cfg(feature = "profile-telemetry")]
#[test]
fn metrics_flush_scheduler_records_cold_flush_time() {
    let mut reactor = test_reactor();
    reactor.config.telemetry_mode = RuntimeTelemetryMode::Profile;
    reactor
        .keyspace
        .set_runtime_telemetry_mode(RuntimeTelemetryMode::Profile);
    reactor.next_metrics_flush_nanos = 0;

    let run = reactor.run_metrics_flush_slice();

    assert!(run.did_work);
    assert!(reactor.keyspace.runtime_metrics().metrics_flush_nanos_total > 0);
}

#[test]
fn shutdown_drain_waits_for_terminal_cqes_before_releasing_buffers() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let mut reactor = test_reactor_with_backend(state.clone());

    let read_idx = reactor.buffer_pool.lease_index().unwrap();
    let write_idx = reactor.buffer_pool.lease_index().unwrap();
    let mut meta = ConnectionMeta::new(123, 0);
    meta.read_buf_offset = read_idx as u32;
    meta.write_buf_offset = write_idx as u32;
    let conn_id = reactor.connections.insert(meta);
    reactor.generations[conn_id] = 9;
    reactor.writev_states[conn_id].push_static(b"pending");
    reactor.writev_states[conn_id].finalize();
    reactor.inflight_ops[conn_id].read = true;
    reactor.inflight_ops[conn_id].writev = true;

    let cgen = reactor.generations[conn_id];
    let read_token = encode_token(conn_id, cgen, OpType::Read).unwrap();
    let writev_token = encode_token(conn_id, cgen, OpType::Writev).unwrap();
    let close_token = encode_token(conn_id, cgen, OpType::Close).unwrap();
    let read_cancel = CompletionToken::cancel(read_token).unwrap();
    let writev_cancel = CompletionToken::cancel(writev_token).unwrap();
    {
        let mut state = state.lock().unwrap();
        state.completions.push_back(Completion {
            token: read_cancel,
            result: 0,
            flags: 0,
        });
        state.completions.push_back(Completion {
            token: writev_cancel,
            result: 0,
            flags: 0,
        });
        state.completions.push_back(Completion {
            token: close_token,
            result: 0,
            flags: 0,
        });
        state.completions.push_back(Completion {
            token: read_token,
            result: -libc::ECANCELED,
            flags: 0,
        });
        state.completions.push_back(Completion {
            token: writev_token,
            result: -libc::ECANCELED,
            flags: 0,
        });
    }

    assert_eq!(reactor.buffer_pool.outstanding(), 2);
    reactor.drain_inflight_io();

    assert!(reactor.connections.get(conn_id).is_none());
    assert_eq!(reactor.buffer_pool.outstanding(), 0);
    assert_eq!(reactor.writev_states[conn_id].remaining_len(), 0);

    let state = state.lock().unwrap();
    assert!(state.cancels.contains(&(read_token, read_cancel)));
    assert!(state.cancels.contains(&(writev_token, writev_cancel)));
    assert_eq!(state.closes, vec![close_token]);
}

#[test]
fn cancel_completion_does_not_release_buffer_before_target_terminal() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let mut reactor = test_reactor_with_backend(state);

    let read_idx = reactor.buffer_pool.lease_index().unwrap();
    let write_idx = reactor.buffer_pool.lease_index().unwrap();
    let mut meta = ConnectionMeta::new(123, 0);
    meta.read_buf_offset = read_idx as u32;
    meta.write_buf_offset = write_idx as u32;
    let conn_id = reactor.connections.insert(meta);
    reactor.generations[conn_id] = 13;
    reactor.inflight_ops[conn_id].read = true;

    reactor.close_connection(conn_id);

    let read_token = encode_token(conn_id, reactor.generations[conn_id], OpType::Read).unwrap();
    reactor.handle_completion(&Completion {
        token: CompletionToken::cancel(read_token).unwrap(),
        result: 0,
        flags: 0,
    });

    assert!(reactor.connections.get(conn_id).is_some());
    assert!(reactor.inflight_ops[conn_id].read);
    assert!(!reactor.inflight_ops[conn_id].cancel_read);
    assert_eq!(reactor.buffer_pool.outstanding(), 2);
}

#[test]
fn malformed_completion_token_is_counted_and_dropped() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let mut reactor = test_reactor_with_backend(state);

    reactor.handle_completion(&Completion {
        token: CompletionToken::from_raw(0xFE),
        result: 0,
        flags: 0,
    });

    assert_eq!(reactor.invalid_completion_tokens, 1);
    assert_eq!(reactor.connection_count(), 0);
}

#[test]
fn late_accept_during_drain_closes_fd_without_allocating_connection() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let mut reactor = test_reactor_with_backend(state.clone());
    let (accepted_fd, peer_fd) = socket_pair();
    let before_outstanding = reactor.buffer_pool.outstanding();

    reactor.draining = true;
    reactor.handle_accept(&Completion {
        token: CompletionToken::accept(),
        result: accepted_fd,
        flags: 0,
    });

    assert!(peer_observes_eof(peer_fd));
    assert_eq!(reactor.connection_count(), 0);
    assert_eq!(reactor.buffer_pool.outstanding(), before_outstanding);
    assert!(state.lock().unwrap().reads.is_empty());

    // SAFETY: peer_fd is the still-open half of the socketpair.
    unsafe {
        libc::close(peer_fd);
    }
}

#[test]
fn stale_slot_reuse_cqes_are_dropped_by_generation() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let mut reactor = test_reactor_with_backend(state);

    let old_read_idx = reactor.buffer_pool.lease_index().unwrap();
    let old_meta = ConnectionMeta::new(123, old_read_idx as u32);
    let conn_id = reactor.connections.insert(old_meta);
    reactor.generations[conn_id] = 7;
    let old_tokens = [
        encode_token(conn_id, 7, OpType::Read).unwrap(),
        encode_token(conn_id, 7, OpType::Write).unwrap(),
        encode_token(conn_id, 7, OpType::Writev).unwrap(),
        encode_token(conn_id, 7, OpType::Close).unwrap(),
    ];

    reactor.buffer_pool.release_index(old_read_idx);
    reactor.connections.remove(conn_id);

    let new_read_idx = reactor.buffer_pool.lease_index().unwrap();
    let new_meta = ConnectionMeta::new(124, new_read_idx as u32);
    let reused_id = reactor.connections.insert(new_meta);
    assert_eq!(reused_id, conn_id);
    reactor.generations[reused_id] = 8;
    reactor.inflight_ops[reused_id] = InflightSet::default();

    for token in old_tokens {
        reactor.handle_completion(&Completion {
            token,
            result: 4,
            flags: 0,
        });
    }

    let conn = reactor.connections.get(reused_id).unwrap();
    assert_eq!(conn.fd, 124);
    assert_eq!(conn.read_buf_len, 0);
    assert_eq!(reactor.connection_count(), 1);
    assert_eq!(reactor.unexpected_completion_tokens, 0);
}

#[test]
fn same_generation_completion_without_inflight_state_is_dropped() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let mut reactor = test_reactor_with_backend(state);
    let conn_id = insert_test_connection(&mut reactor, 123, b"");
    let token = encode_token(conn_id, reactor.generations[conn_id], OpType::Read).unwrap();

    reactor.handle_completion(&Completion {
        token,
        result: 4,
        flags: 0,
    });

    assert_eq!(reactor.unexpected_completion_tokens, 1);
    assert_eq!(reactor.connections.get(conn_id).unwrap().read_buf_len, 0);
    assert!(reactor.connections.get(conn_id).is_some());
}

#[test]
fn duplicate_read_submit_is_ignored_while_read_is_inflight() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let mut reactor = test_reactor_with_backend(state.clone());
    let conn_id = insert_test_connection(&mut reactor, 123, b"");
    reactor.inflight_ops[conn_id].read = true;

    reactor.submit_read_for(conn_id, 123);

    assert!(state.lock().unwrap().reads.is_empty());
    assert!(reactor.inflight_ops[conn_id].read);
}

#[test]
fn cancel_success_does_not_complete_target_operation() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let mut reactor = test_reactor_with_backend(state);
    let conn_id = insert_test_connection(&mut reactor, 123, b"");
    reactor.inflight_ops[conn_id].read = true;

    let read_token = encode_token(conn_id, reactor.generations[conn_id], OpType::Read).unwrap();
    let cancel = CompletionToken::cancel(read_token).unwrap();
    reactor.handle_completion(&Completion {
        token: cancel,
        result: 0,
        flags: 0,
    });

    assert!(reactor.inflight_ops[conn_id].read);
    assert_eq!(
        reactor.classify_cancel_completion(read_token, 0),
        CancelResult::Canceled
    );
}

#[test]
fn cancel_enoent_tracks_not_found_vs_already_terminal() {
    let state = Arc::new(Mutex::new(MockBackendState::default()));
    let mut reactor = test_reactor_with_backend(state);
    let conn_id = insert_test_connection(&mut reactor, 123, b"");
    reactor.inflight_ops[conn_id].read = true;

    let read_token = encode_token(conn_id, reactor.generations[conn_id], OpType::Read).unwrap();
    assert_eq!(
        reactor.classify_cancel_completion(read_token, -libc::ENOENT),
        CancelResult::NotFound
    );

    reactor.mark_inflight_completed(conn_id, OpType::Read);
    assert_eq!(
        reactor.classify_cancel_completion(read_token, -libc::ENOENT),
        CancelResult::AlreadyTerminal
    );
}

#[test]
fn submit_read_sq_full_flushes_and_retries_once() {
    let state = Arc::new(Mutex::new(MockBackendState {
        fail_next_submit_sq_full: true,
        ..Default::default()
    }));
    let mut reactor = test_reactor_with_backend(state.clone());

    let read_idx = reactor.buffer_pool.lease_index().unwrap();
    let write_idx = reactor.buffer_pool.lease_index().unwrap();
    let mut meta = ConnectionMeta::new(123, 0);
    meta.read_buf_offset = read_idx as u32;
    meta.write_buf_offset = write_idx as u32;
    let conn_id = reactor.connections.insert(meta);

    reactor.submit_read_for(conn_id, 123);

    assert!(reactor.inflight_ops[conn_id].read);
    assert_eq!(state.lock().unwrap().flushes, 1);

    let runtime = reactor.keyspace.runtime_metrics();
    assert_eq!(runtime.submit_sq_full_retries, 1);
    assert_eq!(runtime.submit_failures, 0);
}

#[test]
fn submit_read_sq_full_retry_failure_closes_connection() {
    let state = Arc::new(Mutex::new(MockBackendState {
        fail_next_submit_sq_full: true,
        fail_retry_submit_sq_full: true,
        ..Default::default()
    }));
    let mut reactor = test_reactor_with_backend(state.clone());

    let read_idx = reactor.buffer_pool.lease_index().unwrap();
    let write_idx = reactor.buffer_pool.lease_index().unwrap();
    let mut meta = ConnectionMeta::new(123, 0);
    meta.read_buf_offset = read_idx as u32;
    meta.write_buf_offset = write_idx as u32;
    let conn_id = reactor.connections.insert(meta);
    reactor.generations[conn_id] = 11;

    reactor.submit_read_for(conn_id, 123);

    assert!(reactor.connections.is_closing(conn_id));
    assert_eq!(state.lock().unwrap().flushes, 1);

    let runtime = reactor.keyspace.runtime_metrics();
    assert_eq!(runtime.submit_sq_full_retries, 1);
    assert_eq!(runtime.submit_failures, 1);
}
