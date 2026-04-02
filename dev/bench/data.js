window.BENCHMARK_DATA = {
  "lastUpdate": 1775107522728,
  "repoUrl": "https://github.com/kevincaicedo/vortex",
  "entries": {
    "VortexDB Benchmarks": [
      {
        "commit": {
          "author": {
            "email": "ing.sys.kevincaicedo@gmail.com",
            "name": "Kevin Caicedo",
            "username": "kevincaicedo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e6ad405119536dff0820cea0e0e54ebcd049685c",
          "message": "Merge pull request #11 from kevincaicedo/feat/simd-accelerated-resp3\n\nfeat: vortex engine swiss tables + SIMD accelerated resp",
          "timestamp": "2026-04-02T01:07:09-04:00",
          "tree_id": "41e7f08d292f28c204ab915ddff7740a52fd2300",
          "url": "https://github.com/kevincaicedo/vortex/commit/e6ad405119536dff0820cea0e0e54ebcd049685c"
        },
        "date": 1775107522406,
        "tool": "cargo",
        "benches": [
          {
            "name": "swiss_table_insert/100",
            "value": 12502,
            "range": "± 1166",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_insert/10000",
            "value": 1567911,
            "range": "± 331194",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_insert/1000000",
            "value": 385730076,
            "range": "± 7628894",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_insert_single/100",
            "value": 6772,
            "range": "± 1246",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_insert_single/10000",
            "value": 1325022,
            "range": "± 397965",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_insert_single/1000000",
            "value": 285331842,
            "range": "± 5625748",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_lookup_hit/100",
            "value": 11,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_lookup_hit/10000",
            "value": 11,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_lookup_hit/1000000",
            "value": 11,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_lookup_miss/100",
            "value": 7,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_lookup_miss/10000",
            "value": 7,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_lookup_miss/1000000",
            "value": 7,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_delete/100",
            "value": 9316,
            "range": "± 695",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_delete/10000",
            "value": 1028873,
            "range": "± 126878",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_delete_single/100",
            "value": 2993,
            "range": "± 576",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_delete_single/10000",
            "value": 368530,
            "range": "± 143983",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_delete_single/1000000",
            "value": 98019454,
            "range": "± 1891792",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_resize_amortized/1000",
            "value": 166593,
            "range": "± 387",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_resize_amortized/10000",
            "value": 1784732,
            "range": "± 5084",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_resize_amortized/100000",
            "value": 23931200,
            "range": "± 1149103",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_mixed_50_50/10000",
            "value": 86076,
            "range": "± 482",
            "unit": "ns/iter"
          },
          {
            "name": "swiss_table_mixed_50_50/1000000",
            "value": 13778444,
            "range": "± 439076",
            "unit": "ns/iter"
          },
          {
            "name": "entry_write_inline",
            "value": 17,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "entry_read_inline",
            "value": 0,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "entry_matches_key",
            "value": 5,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "entry_is_expired",
            "value": 0,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "entry_write_integer",
            "value": 16,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "entry_read_integer",
            "value": 1,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "expiry_register",
            "value": 5,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "expiry_tick_20",
            "value": 73,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lazy_expiry_overhead",
            "value": 11,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "active_sweep_100K",
            "value": 5109,
            "range": "± 195",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_get_inline",
            "value": 53,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_set_inline",
            "value": 64,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_get_miss",
            "value": 28,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_incr",
            "value": 34,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_mget_100",
            "value": 6894,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_mset_100",
            "value": 6702,
            "range": "± 68",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_append_inline",
            "value": 69,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_del_inline",
            "value": 64,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_exists",
            "value": 34,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_expire",
            "value": 58,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_ttl",
            "value": 39,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_type",
            "value": 35,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_scan_10k",
            "value": 811338,
            "range": "± 2849",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_keys_star_10k",
            "value": 605279,
            "range": "± 698",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_ping",
            "value": 5,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_dbsize",
            "value": 6,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_info",
            "value": 156,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_mget_100_1m",
            "value": 7124,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "table_batch_100_1m_prefetch",
            "value": 2514,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "table_batch_100_1m_no_prefetch",
            "value": 2543,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_del_100_1m",
            "value": 12294,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "cmd_exists_100_1m",
            "value": 7077,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "throughput_get_set_mix",
            "value": 10499439,
            "range": "± 15520",
            "unit": "ns/iter"
          },
          {
            "name": "throughput_get_set_1m",
            "value": 14802905,
            "range": "± 27942",
            "unit": "ns/iter"
          },
          {
            "name": "memory_per_entry_1m",
            "value": 433321947,
            "range": "± 7625234",
            "unit": "ns/iter"
          },
          {
            "name": "latency_get_p50_p99_p999",
            "value": 1103657,
            "range": "± 2104",
            "unit": "ns/iter"
          },
          {
            "name": "latency_set_p50_p99_p999",
            "value": 1387874,
            "range": "± 1604",
            "unit": "ns/iter"
          },
          {
            "name": "hashmap_baseline_lookup/hit/10000",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hashmap_baseline_lookup/miss/10000",
            "value": 20,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hashmap_baseline_lookup/hit/1000000",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hashmap_baseline_lookup/miss/1000000",
            "value": 23,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hashmap_baseline_insert/10000",
            "value": 1126187,
            "range": "± 368159",
            "unit": "ns/iter"
          },
          {
            "name": "hashmap_baseline_insert/1000000",
            "value": 215206128,
            "range": "± 2344824",
            "unit": "ns/iter"
          },
          {
            "name": "hashmap_baseline_delete/10000",
            "value": 472684,
            "range": "± 141141",
            "unit": "ns/iter"
          },
          {
            "name": "hashmap_baseline_delete/1000000",
            "value": 140263435,
            "range": "± 1893049",
            "unit": "ns/iter"
          },
          {
            "name": "hashmap_baseline_mixed/10000",
            "value": 73308,
            "range": "± 730",
            "unit": "ns/iter"
          },
          {
            "name": "hashmap_baseline_mixed/1000000",
            "value": 10448872,
            "range": "± 409537",
            "unit": "ns/iter"
          },
          {
            "name": "crlf_scan/crlf_scan_1kb",
            "value": 100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "crlf_scan/crlf_scan_16kb",
            "value": 1685,
            "range": "± 26",
            "unit": "ns/iter"
          },
          {
            "name": "crlf_scan/crlf_scan_64kb",
            "value": 4488,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "crlf_scan/crlf_scan_no_crlf",
            "value": 1064,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "crlf_scan/crlf_scan_all_crlf",
            "value": 19712,
            "range": "± 130",
            "unit": "ns/iter"
          },
          {
            "name": "swar_integer_parse/swar_integer_1digit",
            "value": 16,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "swar_integer_parse/baseline_integer_1digit",
            "value": 5,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "swar_integer_parse/swar_integer_3digit",
            "value": 16,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "swar_integer_parse/baseline_integer_3digit",
            "value": 8,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "swar_integer_parse/swar_integer_5digit",
            "value": 16,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "swar_integer_parse/baseline_integer_5digit",
            "value": 11,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "swar_integer_parse/swar_integer_10digit",
            "value": 6,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "swar_integer_parse/baseline_integer_10digit",
            "value": 18,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "swar_integer_parse/swar_integer_18digit",
            "value": 14,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "swar_integer_parse/baseline_integer_18digit",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "swar_integer_parse/swar_integer_negative",
            "value": 16,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "swar_integer_parse/baseline_integer_negative",
            "value": 8,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_parse/simple_string",
            "value": 86,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_parse/bulk_string",
            "value": 93,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_parse/array_set_cmd",
            "value": 196,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_parse/pipeline_10",
            "value": 1666,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_parse/pipeline_100",
            "value": 18722,
            "range": "± 28",
            "unit": "ns/iter"
          },
          {
            "name": "resp_parse/pipeline_1000",
            "value": 183863,
            "range": "± 299",
            "unit": "ns/iter"
          },
          {
            "name": "resp_parse/pipeline_10_zerocopy",
            "value": 1644,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "resp_parse/pipeline_100_zerocopy",
            "value": 18627,
            "range": "± 74",
            "unit": "ns/iter"
          },
          {
            "name": "resp_parse/pipeline_1000_zerocopy",
            "value": 182672,
            "range": "± 751",
            "unit": "ns/iter"
          },
          {
            "name": "resp_parse/mixed_frame_types",
            "value": 334,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "resp_parse/large_bulk_string_64kb",
            "value": 5786,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "resp_parse/nested_array_depth_8",
            "value": 444,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "tape_parse/tape_pipeline_10",
            "value": 292,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "tape_parse/tape_pipeline_100",
            "value": 2642,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "tape_parse/tape_pipeline_1000",
            "value": 25979,
            "range": "± 241",
            "unit": "ns/iter"
          },
          {
            "name": "tape_parse/tape_pipeline_1000_zerocopy",
            "value": 25093,
            "range": "± 52",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize/ok",
            "value": 5,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize/integer_lut_42",
            "value": 10,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize/integer_lut_9999",
            "value": 11,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize/integer_itoa_123456",
            "value": 14,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize/bulk_string",
            "value": 14,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize/array_3",
            "value": 54,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize/pipeline_ok_100",
            "value": 617,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize/pipeline_integer_100",
            "value": 997,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize/slice_ok",
            "value": 3,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize/slice_integer_42",
            "value": 4,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize/slice_array_3",
            "value": 34,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize_iovecs/iovec_ok",
            "value": 5,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize_iovecs/iovec_integer_42",
            "value": 11,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize_iovecs/iovec_bulk_string",
            "value": 13,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize_iovecs/iovec_array_3",
            "value": 47,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize_iovecs/iovec_array_10",
            "value": 128,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize_iovecs/iovec_pipeline_ok_100",
            "value": 473,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize_iovecs/iovec_array_10_flatten",
            "value": 295,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize_iovecs/iovec_bulk_1kb",
            "value": 13,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "resp_serialize_iovecs/iovec_lrange_100",
            "value": 1179,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "command_dispatch/swar_upper_4b",
            "value": 3,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "command_dispatch/swar_upper_11b",
            "value": 9,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "command_dispatch/phf_lookup_ping",
            "value": 16,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "command_dispatch/upper_and_lookup_set",
            "value": 32,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "command_dispatch/dispatch_ping",
            "value": 38,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "command_dispatch/dispatch_set",
            "value": 38,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "command_dispatch/dispatch_hset",
            "value": 37,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "command_dispatch/dispatch_unknown",
            "value": 33,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "reactor_scaling/1",
            "value": 1243168,
            "range": "± 35607",
            "unit": "ns/iter"
          },
          {
            "name": "reactor_scaling/2",
            "value": 1251873,
            "range": "± 12873",
            "unit": "ns/iter"
          },
          {
            "name": "reactor_scaling/3",
            "value": 1605765,
            "range": "± 9611",
            "unit": "ns/iter"
          },
          {
            "name": "reactor_scaling/4",
            "value": 1966773,
            "range": "± 101661364",
            "unit": "ns/iter"
          },
          {
            "name": "spsc_throughput/spsc_100k",
            "value": 2301599,
            "range": "± 177516",
            "unit": "ns/iter"
          },
          {
            "name": "mpsc_throughput/2_producers",
            "value": 6326176,
            "range": "± 86710",
            "unit": "ns/iter"
          },
          {
            "name": "mpsc_throughput/8_producers",
            "value": 5865511,
            "range": "± 129924",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}