use vortex_common::VortexResult;
use vortex_proto::{CommandFlags, RespFrame};

use crate::Shard;

/// Execution context passed to every command handler.
///
/// Holds a mutable reference to the target shard and connection-level
/// metadata that commands may inspect (e.g. authenticated user, database
/// index). Intentionally kept minimal in Phase 0 — fields will be added
/// as features land.
pub struct CommandContext<'a> {
    /// The shard this command operates on.
    pub shard: &'a mut Shard,
    /// The selected database index (default 0).
    pub db: u16,
}

impl<'a> CommandContext<'a> {
    /// Creates a new command context targeting the given shard.
    pub fn new(shard: &'a mut Shard) -> Self {
        Self { shard, db: 0 }
    }
}

/// Trait implemented by every VortexDB command (GET, SET, PING, …).
///
/// Each concrete command is a zero-sized struct that implements this trait.
/// The engine dispatches parsed frames to the appropriate implementation
/// via the perfect-hash command table.
pub trait Command: Send + Sync {
    /// Executes the command and returns a RESP frame to send back.
    fn execute(&self, ctx: &mut CommandContext<'_>, args: &[RespFrame]) -> VortexResult<RespFrame>;

    /// The canonical command name (uppercase, e.g. `"GET"`).
    fn name(&self) -> &'static str;

    /// Command arity: positive = exact arg count, negative = minimum.
    fn arity(&self) -> i16;

    /// Behavioral flags for this command.
    fn flags(&self) -> CommandFlags;
}

#[cfg(all(test, not(miri)))]
mod tests {
    use vortex_common::ShardId;

    use super::*;

    /// Smoke-test: a trivial PING command to verify the trait compiles.
    struct PingCmd;

    impl Command for PingCmd {
        fn execute(
            &self,
            _ctx: &mut CommandContext<'_>,
            args: &[RespFrame],
        ) -> VortexResult<RespFrame> {
            if args.is_empty() {
                Ok(RespFrame::simple_string("PONG"))
            } else {
                Ok(args[0].clone())
            }
        }

        fn name(&self) -> &'static str {
            "PING"
        }

        fn arity(&self) -> i16 {
            -1
        }

        fn flags(&self) -> CommandFlags {
            CommandFlags::FAST
        }
    }

    #[test]
    fn ping_no_args() {
        let mut shard = Shard::new(ShardId::new(0));
        let mut ctx = CommandContext::new(&mut shard);
        let result = PingCmd.execute(&mut ctx, &[]).unwrap();
        assert!(matches!(result, RespFrame::SimpleString(ref s) if s.as_ref() == b"PONG"));
    }

    #[test]
    fn ping_with_message() {
        let mut shard = Shard::new(ShardId::new(0));
        let mut ctx = CommandContext::new(&mut shard);
        let msg = RespFrame::bulk_string("hello");
        let result = PingCmd
            .execute(&mut ctx, std::slice::from_ref(&msg))
            .unwrap();
        assert_eq!(result, msg);
    }

    #[test]
    fn command_is_object_safe() {
        // Ensure the trait can be used as a trait object.
        let cmd: Box<dyn Command> = Box::new(PingCmd);
        assert_eq!(cmd.name(), "PING");
        assert_eq!(cmd.arity(), -1);
        assert!(cmd.flags().contains(CommandFlags::FAST));
    }
}
