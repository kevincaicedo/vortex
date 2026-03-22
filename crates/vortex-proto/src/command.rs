/// Command metadata for dispatch table entries.
pub struct CommandMeta {
    /// Command name (uppercase).
    pub name: &'static str,
    /// Arity: positive = exact, negative = minimum (abs value).
    pub arity: i16,
    /// Command flags.
    pub flags: CommandFlags,
}

/// Command behavior flags (bitfield).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandFlags(u16);

impl CommandFlags {
    pub const READ: Self = Self(1 << 0);
    pub const WRITE: Self = Self(1 << 1);
    pub const FAST: Self = Self(1 << 2);
    pub const SLOW: Self = Self(1 << 3);
    pub const BLOCKING: Self = Self(1 << 4);
    pub const PUBSUB: Self = Self(1 << 5);
    pub const SCRIPTING: Self = Self(1 << 6);
    pub const ADMIN: Self = Self(1 << 7);

    /// Returns true if this flag set contains the given flag.
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    /// Combines two flag sets.
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

// Include the generated perfect hash command table.
include!(concat!(env!("OUT_DIR"), "/command_table.rs"));

/// Looks up command metadata by name (case-insensitive uppercase expected).
pub fn lookup_command(name: &str) -> Option<&'static CommandMeta> {
    COMMAND_TABLE.get(name).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_known_commands() {
        let ping = lookup_command("PING").expect("PING should exist");
        assert_eq!(ping.name, "PING");
        assert!(ping.flags.contains(CommandFlags::FAST));

        let set = lookup_command("SET").expect("SET should exist");
        assert_eq!(set.name, "SET");
        assert!(set.flags.contains(CommandFlags::WRITE));

        let get = lookup_command("GET").expect("GET should exist");
        assert_eq!(get.name, "GET");
        assert!(get.flags.contains(CommandFlags::READ));
    }

    #[test]
    fn lookup_unknown_command() {
        assert!(lookup_command("NONEXISTENT").is_none());
    }
}
