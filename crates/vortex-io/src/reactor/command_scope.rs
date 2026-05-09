use smallvec::SmallVec;
use vortex_engine::commands::arg_count;
use vortex_proto::{CommandFlags, CommandMeta, FrameRef, KeyRange};

pub(super) struct KeyspaceGateScope<'a> {
    pub(super) keys: SmallVec<[&'a [u8]; 16]>,
    pub(super) full: bool,
}

impl<'a> KeyspaceGateScope<'a> {
    #[inline]
    pub(super) fn none() -> Self {
        Self {
            keys: SmallVec::new(),
            full: false,
        }
    }

    #[inline]
    pub(super) fn full() -> Self {
        Self {
            keys: SmallVec::new(),
            full: true,
        }
    }

    #[inline]
    pub(super) fn keys(keys: SmallVec<[&'a [u8]; 16]>) -> Self {
        KeyspaceGateScope { keys, full: false }
    }
}

#[inline]
pub(super) fn command_keyspace_gate_scope<'a>(
    meta: &CommandMeta,
    frame: &FrameRef<'a>,
) -> KeyspaceGateScope<'a> {
    if !meta.flags.contains(CommandFlags::READ) && !meta.flags.contains(CommandFlags::WRITE) {
        return KeyspaceGateScope::none();
    }

    if meta.key_range.is_empty() {
        return if command_requires_full_keyspace_scope(meta.name) {
            KeyspaceGateScope::full()
        } else {
            KeyspaceGateScope::none()
        };
    }

    let keys = collect_keys(meta.key_range, frame);
    if keys.is_empty() {
        KeyspaceGateScope::none()
    } else {
        KeyspaceGateScope::keys(keys)
    }
}

#[inline]
pub(super) fn command_requires_full_keyspace_scope(name: &str) -> bool {
    matches!(
        name.as_bytes(),
        b"DBSIZE" | b"FLUSHALL" | b"FLUSHDB" | b"KEYS" | b"RANDOMKEY" | b"SCAN" | b"SWAPDB"
    )
}

fn collect_keys<'a>(range: KeyRange, frame: &FrameRef<'a>) -> SmallVec<[&'a [u8]; 16]> {
    let first = usize::try_from(range.first).unwrap_or(0);
    let step = usize::try_from(range.step).unwrap_or(0);
    if first == 0 || step == 0 {
        return SmallVec::new();
    }

    let argc = arg_count(frame);
    let last = if range.last == -1 {
        argc.saturating_sub(1)
    } else {
        usize::try_from(range.last).unwrap_or(0)
    };
    if first > last {
        return SmallVec::new();
    }

    let Some(children) = frame.children() else {
        return SmallVec::new();
    };

    let mut keys = SmallVec::new();
    for (index, child) in children.enumerate() {
        if index < first {
            continue;
        }
        if index > last {
            break;
        }
        if (index - first) % step != 0 {
            continue;
        }
        let Some(key) = child.as_bytes() else {
            return SmallVec::new();
        };
        keys.push(key);
    }
    keys
}
