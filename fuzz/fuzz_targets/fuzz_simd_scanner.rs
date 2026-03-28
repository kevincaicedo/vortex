#![no_main]

use libfuzzer_sys::fuzz_target;
use vortex_proto::scanner::{scalar_scan_crlf, scan_crlf};

fuzz_target!(|data: &[u8]| {
    let simd: Vec<_> = scan_crlf(data).iter().collect();
    let scalar: Vec<_> = scalar_scan_crlf(data).iter().collect();

    assert_eq!(simd, scalar, "SIMD scanner diverged from scalar oracle");

    for position in simd {
        assert!(position + 1 < data.len());
        assert_eq!(data[position], b'\r');
        assert_eq!(data[position + 1], b'\n');
    }
});