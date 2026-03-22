#!/usr/bin/env python3
"""Generate seed corpus files for the RESP parser fuzzer."""
import os

d = os.path.join(os.path.dirname(__file__), "corpus", "fuzz_resp_parser")
os.makedirs(d, exist_ok=True)

# Clean existing files
for f in os.listdir(d):
    fp = os.path.join(d, f)
    if os.path.isfile(fp):
        os.unlink(fp)

seeds = {
    "simple_string_ok": b"+OK\r\n",
    "simple_string_long": b"+hello world\r\n",
    "simple_string_empty": b"+\r\n",
    "error_msg": b"-ERR unknown command\r\n",
    "error_wrongtype": b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    "integer_pos": b":42\r\n",
    "integer_neg": b":-100\r\n",
    "integer_zero": b":0\r\n",
    "bulk_string": b"$5\r\nhello\r\n",
    "bulk_string_empty": b"$0\r\n\r\n",
    "bulk_string_nil": b"$-1\r\n",
    "bulk_string_large": b"$26\r\nabcdefghijklmnopqrstuvwxyz\r\n",
    "array_get": b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
    "array_set": b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
    "array_empty": b"*0\r\n",
    "array_nil": b"*-1\r\n",
    "array_nested": b"*1\r\n*2\r\n:1\r\n:2\r\n",
    "inline_ping": b"PING\r\n",
    "inline_set": b"SET foo bar\r\n",
    "pipeline_2": b"+OK\r\n+OK\r\n",
    "pipeline_set_get": b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
    "empty_input": b"",
    "bulk_huge_len": b"$999999\r\n",
    "array_incomplete": b"*2\r\n",
    "resp3_null": b"_\r\n",
    "resp3_bool_true": b"#t\r\n",
    "resp3_bool_false": b"#f\r\n",
    "resp3_double": b",3.14\r\n",
    "missing_crlf": b"+OK",
}

for name, data in seeds.items():
    with open(os.path.join(d, name), "wb") as f:
        f.write(data)

print(f"Created {len(seeds)} seed files in {d}")
