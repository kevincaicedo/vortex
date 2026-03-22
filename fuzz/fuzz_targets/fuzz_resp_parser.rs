#![no_main]
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use vortex_proto::{RespParser, RespSerializer};

fuzz_target!(|data: &[u8]| {
    // The parser must never panic on arbitrary input.
    if let Ok((frame, consumed)) = RespParser::parse(data) {
        // Roundtrip: serialize the parsed frame and re-parse it.
        // The re-parsed frame must equal the original.
        let mut buf = BytesMut::new();
        RespSerializer::serialize(&frame, &mut buf);
        if let Ok((roundtrip_frame, _)) = RespParser::parse(&buf) {
            assert_eq!(frame, roundtrip_frame, "roundtrip mismatch");
        }

        // Consumed bytes must not exceed input length.
        assert!(consumed <= data.len());
    }
});
