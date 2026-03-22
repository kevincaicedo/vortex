#![no_main]
use libfuzzer_sys::fuzz_target;
use vortex_proto::RespParser;

fuzz_target!(|data: &[u8]| {
    // The parser must never panic on arbitrary input.
    let _ = RespParser::parse(data);
});
