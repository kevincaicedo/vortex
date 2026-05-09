//! Concrete command-execution adapters for engine-owned keyspace topology.
//!
//! Alpha uses [`SharedKeyspaceExecutor`]: commands execute on the receiving
//! reactor against the shared [`ConcurrentKeyspace`]. A future owner-shard
//! experiment should add a different executor/router behind the reactor
//! boundary instead of leaking routing into backend, parser, or response code.

use std::sync::Arc;

use vortex_proto::FrameRef;

use crate::commands::{CommandClock, ExecutedCommand, execute_command};
use crate::keyspace::{CommandGateGuard, ConcurrentKeyspace};

/// Transaction-gate scope required while executing one command.
#[derive(Clone, Copy, Debug)]
pub enum CommandExecutionScope<'a> {
    /// No transaction gate is required.
    None,
    /// Gate the shards touched by the borrowed command keys.
    Keys(&'a [&'a [u8]]),
    /// Gate every shard for whole-keyspace commands.
    Full,
}

impl CommandExecutionScope<'_> {
    #[inline]
    pub const fn is_none(self) -> bool {
        matches!(self, Self::None)
    }
}

/// Zero-cost alpha executor for the shared [`ConcurrentKeyspace`] topology.
///
/// This is a concrete newtype over `Arc<ConcurrentKeyspace>`, not a trait
/// object. Its hot methods inline down to the existing command dispatch and
/// shard-gate calls, preserving the current shared-keyspace semantics while
/// giving the reactor one command-execution boundary to swap in a future
/// owner-shard/router experiment.
#[derive(Clone)]
#[repr(transparent)]
pub struct SharedKeyspaceExecutor {
    keyspace: Arc<ConcurrentKeyspace>,
}

impl SharedKeyspaceExecutor {
    /// Create an executor over the alpha shared keyspace.
    #[inline]
    pub fn new(keyspace: Arc<ConcurrentKeyspace>) -> Self {
        Self { keyspace }
    }

    /// Return the underlying keyspace for cold reactor maintenance and tests.
    #[inline]
    pub fn keyspace(&self) -> &ConcurrentKeyspace {
        &self.keyspace
    }

    /// Return the shared keyspace handle for ownership handoff code.
    #[inline]
    pub fn keyspace_arc(&self) -> &Arc<ConcurrentKeyspace> {
        &self.keyspace
    }

    /// Execute one already-routed command without adding a transaction gate.
    #[inline]
    pub fn execute(
        &self,
        name: &[u8],
        frame: &FrameRef<'_>,
        clock: impl Into<CommandClock>,
    ) -> Option<ExecutedCommand> {
        execute_command(self.keyspace(), name, frame, clock)
    }

    /// Execute one command inside the transaction-gate scope selected by the reactor.
    #[inline]
    pub fn execute_scoped(
        &self,
        scope: CommandExecutionScope<'_>,
        reactor_id: usize,
        name: &[u8],
        frame: &FrameRef<'_>,
        clock: impl Into<CommandClock>,
    ) -> Option<ExecutedCommand> {
        match scope {
            CommandExecutionScope::None => self.execute(name, frame, clock),
            CommandExecutionScope::Keys([]) => self.execute(name, frame, clock),
            CommandExecutionScope::Keys([key]) => {
                let _gate = self.enter_command_gate_for_key(key, reactor_id);
                self.execute(name, frame, clock)
            }
            CommandExecutionScope::Keys(keys) => {
                let _gate = self.enter_command_gate_for_keys(keys, reactor_id);
                self.execute(name, frame, clock)
            }
            CommandExecutionScope::Full => {
                let _gate = self.enter_all_shard_command_gate(reactor_id);
                self.execute(name, frame, clock)
            }
        }
    }

    #[inline]
    fn enter_command_gate_for_keys(
        &self,
        keys: &[&[u8]],
        reactor_id: usize,
    ) -> CommandGateGuard<'_> {
        self.keyspace()
            .enter_command_gate_for_keys(keys, reactor_id)
    }

    #[inline]
    fn enter_command_gate_for_key(&self, key: &[u8], reactor_id: usize) -> CommandGateGuard<'_> {
        self.keyspace().enter_command_gate_for_key(key, reactor_id)
    }

    #[inline]
    fn enter_all_shard_command_gate(&self, reactor_id: usize) -> CommandGateGuard<'_> {
        self.keyspace().enter_all_shard_command_gate(reactor_id)
    }
}
