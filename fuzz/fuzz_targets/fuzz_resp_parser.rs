#![no_main]
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use vortex_proto::{RespParser, RespSerializer};

fuzz_target!(|data: &[u8]| {
    if let Ok((frames, consumed)) = RespParser::parse_pipeline(data) {
        let mut reparsed = Vec::new();
        let mut cursor = 0;

        while cursor < consumed {
            let (frame, used) = RespParser::parse(&data[cursor..consumed]).unwrap();
            reparsed.push(frame);
            cursor += used;
        }

        assert_eq!(frames, reparsed, "pipeline mismatch");
        assert!(consumed <= data.len());
    }

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
