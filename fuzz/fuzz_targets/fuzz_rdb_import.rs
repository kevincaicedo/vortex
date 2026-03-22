#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Stub: RDB import parsing will be implemented in Phase 5.
    // For now, just exercise any future import path without panicking.
    let _ = data;
});
