use std::{fs, path::PathBuf};

use bytes::BytesMut;
use vortex_proto::{RespParser, RespSerializer};

fn fuzz_resp_parser_corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus/fuzz_resp_parser")
}

fn assert_fuzzer_invariants(name: &str, data: &[u8]) {
    if let Ok((frames, consumed)) = RespParser::parse_pipeline(data) {
        let mut reparsed = Vec::new();
        let mut cursor = 0;

        while cursor < consumed {
            let (frame, used) = RespParser::parse(&data[cursor..consumed])
                .unwrap_or_else(|error| panic!("{name}: incremental parse failed: {error:?}"));
            assert!(used > 0, "{name}: parser made no progress");
            reparsed.push(frame);
            cursor += used;
        }

        assert_eq!(frames, reparsed, "{name}: pipeline mismatch");
        assert!(consumed <= data.len(), "{name}: consumed past input");
    }

    if let Ok((frame, consumed)) = RespParser::parse(data) {
        let mut buf = BytesMut::new();
        RespSerializer::serialize(&frame, &mut buf);
        if let Ok((roundtrip_frame, _)) = RespParser::parse(&buf) {
            assert_eq!(frame, roundtrip_frame, "{name}: roundtrip mismatch");
        }
        assert!(consumed <= data.len(), "{name}: consumed past input");
    }
}

fn corpus_entries() -> Vec<(String, Vec<u8>)> {
    let corpus_dir = fuzz_resp_parser_corpus_dir();
    let mut entries: Vec<_> = fs::read_dir(&corpus_dir)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", corpus_dir.display()))
        .collect::<Result<_, _>>()
        .unwrap_or_else(|error| panic!("failed to walk {}: {error}", corpus_dir.display()));
    entries.sort_by_key(|entry| entry.file_name());

    assert!(
        !entries.is_empty(),
        "fuzz corpus is empty: {}",
        corpus_dir.display()
    );

    entries
        .into_iter()
        .filter_map(|entry| {
            let path = entry.path();
            if !path.is_file() {
                return None;
            }
            let data = fs::read(&path)
                .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("<invalid corpus name>")
                .to_owned();
            Some((name, data))
        })
        .collect()
}

fn mutated_inputs(seed: &[u8]) -> Vec<(&'static str, Vec<u8>)> {
    let mut cases = Vec::new();

    let split = seed.len() / 2;
    cases.push(("prefix", seed[..split].to_vec()));
    cases.push(("suffix", seed[split..].to_vec()));

    if !seed.is_empty() {
        let mut flipped_marker = seed.to_vec();
        flipped_marker[0] = match flipped_marker[0] {
            b'*' => b'$',
            b'$' => b'*',
            b'+' => b':',
            b':' => b'+',
            b'-' => b'*',
            other => other ^ 0x5a,
        };
        cases.push(("marker_flip", flipped_marker));

        let mut bit_flip = seed.to_vec();
        let pivot = bit_flip.len() / 2;
        bit_flip[pivot] ^= 0x80;
        cases.push(("high_bit_flip", bit_flip));

        let mut deleted = seed.to_vec();
        deleted.remove(pivot);
        cases.push(("delete_pivot", deleted));
    }

    let mut no_lf = seed.to_vec();
    no_lf.retain(|byte| *byte != b'\n');
    cases.push(("drop_lf", no_lf));

    let mut inserted_crlf = seed.to_vec();
    inserted_crlf.splice(split..split, b"\r\n".iter().copied());
    cases.push(("insert_crlf", inserted_crlf));

    let mut appended_frame = seed.to_vec();
    appended_frame.extend_from_slice(b"+OK\r\n");
    cases.push(("append_frame", appended_frame));

    cases
}

#[test]
fn resp_parser_fuzz_corpus_replays_without_panic() {
    for (name, data) in corpus_entries() {
        assert_fuzzer_invariants(&name, &data);
    }
}

#[test]
fn resp_parser_fuzz_corpus_mutations_replay_without_panic() {
    for (name, data) in corpus_entries() {
        for (mutation, mutated) in mutated_inputs(&data) {
            assert_fuzzer_invariants(&format!("{name}:{mutation}"), &mutated);
        }
    }
}
