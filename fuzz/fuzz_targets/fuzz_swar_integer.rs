#![no_main]

use libfuzzer_sys::fuzz_target;
use vortex_proto::swar_parse_int;

fn scalar_parse_int(buf: &[u8]) -> Option<(i64, usize)> {
    let first = *buf.first()?;
    let sign_len = usize::from(first == b'-' || first == b'+');

    let mut end = sign_len;
    while end < buf.len() && buf[end].is_ascii_digit() {
        end += 1;
    }

    if end == sign_len {
        return None;
    }

    let text = std::str::from_utf8(&buf[..end]).ok()?;
    text.parse::<i64>().ok().map(|value| (value, end))
}

fuzz_target!(|data: &[u8]| {
    let swar = swar_parse_int(data);
    let scalar = scalar_parse_int(data);

    assert_eq!(swar, scalar, "SWAR parser diverged from scalar reference");

    if let Some((_, consumed)) = swar {
        assert!(consumed <= data.len());
        assert!(consumed > 0);
    }
});