# Shopping Sessions — Builders Buildathon

A single-binary Rust implementation of the Shopping Sessions pipeline.

Reads the dirty CSV logs in `$DATA_DIR`, resolves each person's linked user IDs,
builds sessions, credits each session to the ad click that brought the shopper
in, counts assisting campaigns from the week before, and writes
`$OUTPUT_DIR/user_daily/` and `$OUTPUT_DIR/campaign_daily/` as Parquet.

## Approach

Rust rather than a dataframe engine, to avoid interpreter and engine start-up
cost: the clock runs from container start to container exit.

1. **Read** every input file into memory and leak the buffers, so all later
   stages work on `&'static [u8]` slices with no copying.
2. **Parse** events and clicks in parallel over line-aligned chunks with a
   hand-written CSV reader matching the spec's dialect exactly: `\n` only ends a
   line, `\r` is an ordinary character, quotes are syntax only when they wrap a
   whole field, and any other quote or a wrong field count makes the line
   malformed.
3. **Timestamps** are parsed by hand for the three accepted forms (F1 UTC, F2
   offset, F3 epoch millis), with full calendar validation including leap years.
   An F3 value too large for `i64` is treated as valid but outside the window.
4. **Dedup** events in independent ID shards. Every well-formed copy takes
   part in the earliest-ingest selection, so an invalid winner drops the event.
5. **Link** user IDs with union-find. Each person is named by the byte-wise
   smallest ID in their component. Events and clicks are routed by that name.
6. **Sessionize** in parallel by person shard. Count events by person to group
   them in linear time, then sort each person's slice by timestamp. Start a new
   session when the gap exceeds 30 minutes.
7. **Attribute** sessions with a sliding seven-day click window. The latest
   qualifying time and smallest click ID determine credit. Counts per campaign
   keep each active campaign's assist contribution to one per session.
8. **Aggregate** user days and campaign totals while walking the sessions.
   Write the user Parquet parts in parallel, then merge the campaign totals.

## Libraries

Open-source crates used, all from crates.io:

- `rayon` — data parallelism for file reading, parsing and sorting
- `rustc-hash` — FxHash maps for interning and dedup
- `memchr` — SIMD byte search for the CSV reader
- `parquet`, `arrow-array`, `arrow-schema` — Parquet output

No code was reused from before the event.

## Build and run

    docker build -t shopping-sessions .
    docker run --rm -v "$PWD/dev:/data:ro" -v "$PWD/out:/output" shopping-sessions

Locally, without Docker:

    cargo build --release
    DATA_DIR=dev OUTPUT_DIR=out ./target/release/pipeline

## Correctness

Passes all 36 published edge-case fixtures and matches the published 1M-event
dev answer exactly.

## Licence

MIT. See [LICENSE](LICENSE).
