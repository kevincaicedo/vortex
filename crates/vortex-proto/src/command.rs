/// Command metadata for dispatch table entries.
pub struct CommandMeta {
    /// Command name (uppercase).
    pub name: &'static str,
    /// Arity: positive = exact, negative = minimum (abs value).
    pub arity: i16,
    /// Command flags.
    pub flags: CommandFlags,
    /// Which arguments are keys (for cluster shard routing).
    pub key_range: KeyRange,
}

/// Specification for which command arguments are keys.
///
/// Matches Redis `COMMAND INFO` key-spec: `first`, `last`, `step`.
/// - `first`: 1-based index of the first key argument.
/// - `last`: 1-based index of the last key, or -1 = last argument.
/// - `step`: stride between key arguments (e.g., 2 for MSET key val key val).
/// - `KeyRange::NONE` (`{ 0, 0, 0 }`): command has no key arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyRange {
    pub first: i16,
    pub last: i16,
    pub step: i16,
}

impl KeyRange {
    /// No keys (e.g., PING, QUIT, COMMAND).
    pub const NONE: Self = Self {
        first: 0,
        last: 0,
        step: 0,
    };

    /// Single key at argument position `pos` (1-based).
    pub const fn single(pos: i16) -> Self {
        Self {
            first: pos,
            last: pos,
            step: 1,
        }
    }

    /// All remaining arguments are keys (e.g., DEL key1 key2 ...).
    pub const fn all_from(first: i16) -> Self {
        Self {
            first,
            last: -1,
            step: 1,
        }
    }

    /// Returns `true` if this command has no key arguments.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.first == 0 && self.last == 0 && self.step == 0
    }
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

// ── SWAR Uppercase Normalization ────────────────────────────────────────────

/// In-register ASCII uppercase normalization — branchless SWAR.
///
/// Converts lowercase ASCII a-z → A-Z using SWAR (SIMD Within A Register).
/// For ≤8 byte command names (all Redis commands): single u64 operation.
/// For >8 bytes: processes 8 bytes at a time with a scalar tail.
///
/// Uses the observation that lowercase ASCII letters have bit 5 set (0x20),
/// while uppercase do not. We identify alphabetic bytes via SWAR range check
/// and clear bit 5 only for those bytes.
#[inline]
pub fn uppercase_inplace(buf: &mut [u8]) {
    let len = buf.len();
    let ptr = buf.as_mut_ptr();
    let mut i = 0;

    // Process 8 bytes at a time via SWAR.
    while i + 8 <= len {
        // SAFETY: i + 8 <= len, so ptr.add(i) points to 8 valid bytes.
        let word = unsafe { (ptr.add(i) as *const u64).read_unaligned() };
        let uppercased = swar_upper_u64(word);
        unsafe { (ptr.add(i) as *mut u64).write_unaligned(uppercased) };
        i += 8;
    }

    // Scalar tail for remaining bytes.
    while i < len {
        unsafe {
            let b = *ptr.add(i);
            // Branchless: if b is in a-z range, clear bit 5.
            let is_lower = b.wrapping_sub(b'a') < 26;
            *ptr.add(i) = b & !(0x20 * is_lower as u8);
        }
        i += 1;
    }
}

/// SWAR uppercase for a single u64 word — converts a-z → A-Z branchlessly.
///
/// Algorithm:
/// 1. Detect bytes in the a-z range (0x61..=0x7A) using SWAR subtraction and
///    overflow detection.
/// 2. Build a mask with 0x20 set only for lowercase letter bytes.
/// 3. XOR the mask to flip bit 5 (converting lowercase → uppercase).
#[inline(always)]
fn swar_upper_u64(word: u64) -> u64 {
    // Step 1: Find bytes >= 'a' (0x61).
    // Subtract 0x61 from each byte; if the byte was < 0x61, the high bit is set.
    let ge_a = word.wrapping_sub(0x6161_6161_6161_6161);

    // Step 2: Find bytes > 'z' (0x7A), i.e., bytes >= 0x7B.
    // Subtract 0x7B from each byte.
    let gt_z = word.wrapping_sub(0x7B7B_7B7B_7B7B_7B7B);

    // Step 3: A byte is lowercase if it didn't underflow in step 1 (high bit clear)
    // AND it DID underflow in step 2 (high bit set).
    // In other words: (NOT ge_a) AND gt_z gives us bytes where high bit is set
    // for lowercase ASCII letters.
    let lowercase_mask = (!ge_a & gt_z) & 0x8080_8080_8080_8080;

    // Step 4: Convert the high-bit mask to a 0x20 mask.
    // Shift right by 2 to turn 0x80 → 0x20.
    let flip_mask = lowercase_mask >> 2;

    // Step 5: XOR to flip bit 5 (lowercase → uppercase).
    word ^ flip_mask
}

// ── Command Router ──────────────────────────────────────────────────────────

/// Result of command dispatch.
pub enum DispatchResult<'a> {
    /// Successfully resolved command with metadata and argument slice.
    Dispatch {
        meta: &'static CommandMeta,
        /// The raw command name bytes (already uppercased in scratch).
        name: &'a [u8],
        /// Argument count (including the command name itself).
        argc: usize,
    },
    /// Unknown command.
    UnknownCommand,
    /// Wrong number of arguments.
    WrongArity { meta: &'static CommandMeta },
}

/// Zero-allocation command router using PHF lookup.
///
/// Extracts the command name from a tape frame, uppercases it in a stack
/// scratch buffer, performs an O(1) PHF lookup, validates arity, and returns
/// the dispatch result — all without any heap allocation.
pub struct CommandRouter {
    /// Stack scratch buffer for uppercasing command names.
    /// 32 bytes covers all Redis command names (longest: XAUTOCLAIM = 10).
    scratch: [u8; 32],
}

impl CommandRouter {
    /// Create a new command router.
    #[inline]
    pub const fn new() -> Self {
        Self { scratch: [0u8; 32] }
    }

    /// Dispatch a parsed tape frame to a command handler.
    ///
    /// Returns `DispatchResult::Dispatch` on success, or an error variant
    /// for unknown commands / wrong arity.
    ///
    /// This function:
    /// 1. Extracts the command name bytes from the first array element.
    /// 2. Copies into a stack scratch buffer and uppercases via SWAR.
    /// 3. Performs O(1) PHF lookup.
    /// 4. Validates arity.
    ///
    /// Total overhead: ~3-8ns for common commands (single cache line).
    #[inline]
    pub fn dispatch<'a>(&'a mut self, frame: &crate::FrameRef<'_>) -> DispatchResult<'a> {
        // Extract command name from the tape frame.
        let cmd_bytes = match frame.command_name() {
            Some(b) => b,
            None => return DispatchResult::UnknownCommand,
        };

        let len = cmd_bytes.len();
        if len == 0 || len > 32 {
            return DispatchResult::UnknownCommand;
        }

        // Copy to scratch and uppercase in place.
        self.scratch[..len].copy_from_slice(cmd_bytes);
        uppercase_inplace(&mut self.scratch[..len]);

        // PHF lookup. SAFETY: scratch contains valid UTF-8 (uppercase ASCII).
        let name_str = unsafe { std::str::from_utf8_unchecked(&self.scratch[..len]) };
        let meta = match COMMAND_TABLE.get(name_str).copied() {
            Some(m) => m,
            None => return DispatchResult::UnknownCommand,
        };

        // Get argument count (total elements in the array, including command name).
        let argc = frame.element_count().unwrap_or(0) as usize;

        // Arity validation.
        // Positive arity: exact match required.
        // Negative arity: minimum of abs(arity) arguments required.
        let arity = meta.arity as isize;
        if arity > 0 {
            if argc != arity as usize {
                return DispatchResult::WrongArity { meta };
            }
        } else if (argc as isize) < -arity {
            return DispatchResult::WrongArity { meta };
        }

        DispatchResult::Dispatch {
            meta,
            name: &self.scratch[..len],
            argc,
        }
    }
}

impl Default for CommandRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RespTape;

    /// Helper: parse wire bytes and dispatch.
    fn dispatch_wire<'a>(router: &'a mut CommandRouter, wire: &[u8]) -> DispatchResult<'a> {
        let tape = RespTape::parse_pipeline(wire).expect("valid RESP");
        let frame = tape.iter().next().expect("at least one frame");
        router.dispatch(&frame)
    }

    // ── SWAR uppercase tests ──

    #[test]
    fn uppercase_short() {
        let mut buf = *b"ping";
        uppercase_inplace(&mut buf);
        assert_eq!(&buf, b"PING");
    }

    #[test]
    fn uppercase_mixed_case() {
        let mut buf = *b"pInG";
        uppercase_inplace(&mut buf);
        assert_eq!(&buf, b"PING");
    }

    #[test]
    fn uppercase_already_upper() {
        let mut buf = *b"SET";
        uppercase_inplace(&mut buf);
        assert_eq!(&buf, b"SET");
    }

    #[test]
    fn uppercase_long() {
        let mut buf = *b"incrbyfloat!";
        uppercase_inplace(&mut buf);
        assert_eq!(&buf, b"INCRBYFLOAT!");
    }

    #[test]
    fn uppercase_preserves_nonalpha() {
        let mut buf = *b"123-abc";
        uppercase_inplace(&mut buf);
        assert_eq!(&buf, b"123-ABC");
    }

    #[test]
    fn uppercase_empty() {
        let mut buf: [u8; 0] = [];
        uppercase_inplace(&mut buf);
    }

    #[test]
    fn uppercase_boundary_chars() {
        // '`' is 0x60 (just below 'a'), '{' is 0x7B (just above 'z')
        let mut buf = *b"`{az";
        uppercase_inplace(&mut buf);
        assert_eq!(&buf, b"`{AZ");
    }

    // ── KeyRange tests ──

    #[test]
    fn key_range_none() {
        assert!(KeyRange::NONE.is_empty());
        assert!(!KeyRange::single(1).is_empty());
    }

    // ── PHF lookup tests ──

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

    // ── CommandRouter dispatch tests ──

    #[test]
    fn dispatch_ping() {
        let mut router = CommandRouter::new();
        match dispatch_wire(&mut router, b"*1\r\n$4\r\nPING\r\n") {
            DispatchResult::Dispatch { meta, .. } => {
                assert_eq!(meta.name, "PING");
            }
            _ => panic!("expected Dispatch"),
        }
    }

    #[test]
    fn dispatch_ping_lowercase() {
        let mut router = CommandRouter::new();
        match dispatch_wire(&mut router, b"*1\r\n$4\r\nping\r\n") {
            DispatchResult::Dispatch { meta, .. } => {
                assert_eq!(meta.name, "PING");
            }
            _ => panic!("expected Dispatch"),
        }
    }

    #[test]
    fn dispatch_ping_mixed() {
        let mut router = CommandRouter::new();
        match dispatch_wire(&mut router, b"*1\r\n$4\r\npInG\r\n") {
            DispatchResult::Dispatch { meta, .. } => {
                assert_eq!(meta.name, "PING");
            }
            _ => panic!("expected Dispatch"),
        }
    }

    #[test]
    fn dispatch_unknown() {
        let mut router = CommandRouter::new();
        assert!(matches!(
            dispatch_wire(&mut router, b"*1\r\n$7\r\nINVALID\r\n"),
            DispatchResult::UnknownCommand
        ));
    }

    #[test]
    fn dispatch_set_correct_arity() {
        let mut router = CommandRouter::new();
        // SET key value → 3 args, arity -3 → OK
        match dispatch_wire(
            &mut router,
            b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
        ) {
            DispatchResult::Dispatch { meta, argc, .. } => {
                assert_eq!(meta.name, "SET");
                assert_eq!(argc, 3);
            }
            _ => panic!("expected Dispatch"),
        }
    }

    #[test]
    fn dispatch_set_wrong_arity() {
        let mut router = CommandRouter::new();
        // SET with only 2 args (command + 1 arg), arity -3 → needs at least 3
        match dispatch_wire(&mut router, b"*2\r\n$3\r\nSET\r\n$3\r\nfoo\r\n") {
            DispatchResult::WrongArity { meta } => {
                assert_eq!(meta.name, "SET");
            }
            _ => panic!("expected WrongArity"),
        }
    }

    #[test]
    fn dispatch_del_wrong_arity() {
        let mut router = CommandRouter::new();
        // DEL with 0 keys (only command name), arity -2 → needs at least 2
        match dispatch_wire(&mut router, b"*1\r\n$3\r\nDEL\r\n") {
            DispatchResult::WrongArity { meta } => {
                assert_eq!(meta.name, "DEL");
            }
            _ => panic!("expected WrongArity"),
        }
    }

    #[test]
    fn dispatch_ping_variable_arity() {
        let mut router = CommandRouter::new();
        // PING with 0 extra args → OK (arity -1)
        assert!(matches!(
            dispatch_wire(&mut router, b"*1\r\n$4\r\nPING\r\n"),
            DispatchResult::Dispatch { .. }
        ));
        // PING with 1 extra arg → OK
        assert!(matches!(
            dispatch_wire(&mut router, b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n"),
            DispatchResult::Dispatch { .. }
        ));
    }

    #[test]
    fn dispatch_get_exact_arity() {
        let mut router = CommandRouter::new();
        // GET key → 2 args, arity 2 → OK
        assert!(matches!(
            dispatch_wire(&mut router, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"),
            DispatchResult::Dispatch { .. }
        ));
        // GET with 3 args → arity 2 exact, should fail
        assert!(matches!(
            dispatch_wire(
                &mut router,
                b"*3\r\n$3\r\nGET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
            ),
            DispatchResult::WrongArity { .. }
        ));
    }

    #[test]
    fn dispatch_key_range_set() {
        let set = lookup_command("SET").unwrap();
        assert_eq!(set.key_range, KeyRange::single(1));
    }

    #[test]
    fn dispatch_key_range_del() {
        let del = lookup_command("DEL").unwrap();
        assert_eq!(del.key_range, KeyRange::all_from(1));
    }

    #[test]
    fn dispatch_key_range_ping() {
        let ping = lookup_command("PING").unwrap();
        assert!(ping.key_range.is_empty());
    }

    // ── SWAR u64 correctness ──

    #[test]
    fn swar_upper_all_lowercase() {
        let word = u64::from_le_bytes(*b"abcdefgh");
        let upper = swar_upper_u64(word);
        assert_eq!(upper.to_le_bytes(), *b"ABCDEFGH");
    }

    #[test]
    fn swar_upper_all_uppercase() {
        let word = u64::from_le_bytes(*b"ABCDEFGH");
        let upper = swar_upper_u64(word);
        assert_eq!(upper.to_le_bytes(), *b"ABCDEFGH");
    }

    #[test]
    fn swar_upper_digits() {
        let word = u64::from_le_bytes(*b"12345678");
        let upper = swar_upper_u64(word);
        assert_eq!(upper.to_le_bytes(), *b"12345678");
    }

    #[test]
    fn swar_upper_mixed() {
        let word = u64::from_le_bytes(*b"HeLlO-wO");
        let upper = swar_upper_u64(word);
        assert_eq!(upper.to_le_bytes(), *b"HELLO-WO");
    }
}
