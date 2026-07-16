# CLAUDE.md

Guidance for working in this repo.

## What this is

`depot` is a Go library for decoupled data exchange over object storage
(via [bfs](https://github.com/bsm/bfs)). A producer deposits a **versioned
snapshot**; consumers poll and collect the latest into an in-memory shape,
refreshing in the background. Producer and consumer are decoupled in both time
and process.

It is the successor to `bsm/feedx`, with a smaller, generics-based API. Do not
reintroduce feedx's `Job`/`Producer`/`Consumer`/`CronJob` types — the whole
point of this rewrite was to delete them.

## Layout

Low-level (ported from feedx, stable — change only with good reason):

- `reader.go` / `writer.go` — bfs-backed streaming decode/encode + `MultiReader`
- `format.go` — JSON / Protobuf / CBOR, auto-detected from the URL extension
- `compression.go` — gzip / flate / zstd, auto-detected from the URL extension
- `manifest.go` — incremental-feed manifest (data file list + version)
- `depot.go` — `Status`, `ErrNotModified`, the `X-Depot-Version` metadata header,
  `fetchRemoteVersion`, `skipSync`

Public API (the part that matters):

- `produce.go` — `Produce` / `ProduceIncremental`
- `consume.go` — `Subscribe` / `Subscription` / `Rows`
- `options.go` — `WithFormat`, `WithCompression`, `Force`, `WithIncremental`, `OnError`

## Core model

- Every snapshot carries its `version` (int64) in the `X-Depot-Version` object
  metadata header. `skipSync` compares local vs remote versions to decide
  whether to write/read at all. Keep this the single source of truth for skip
  decisions.
- `Subscribe` holds the built snapshot in an `atomic.Pointer` and swaps it whole
  on each refresh — `Load()` is lock-free and always returns a consistent
  snapshot. A failed refresh (decode or build error) keeps the previous one.
- `Subscribe[T]` decodes into a freshly-allocated `*T` per item and yields
  `iter.Seq[*T]`. This is deliberate: proto messages must not be copied by value
  (copylocks), and per-item allocation lets `build` retain pointers safely.
- Cancellation is first-class but Close is the subscription's lifecycle trigger:
  the Subscribe loop ctx is deliberately NOT derived from the caller's ctx —
  Close cancels it, aborting in-flight syncs. Decode/emit loops check ctx
  between items, and Produce never commits after cancellation. Preserve these
  guarantees when changing the sync paths.

## Testing

- `go test ./...` — full suite. `golangci-lint run ./...` must be clean.
- Tests address storage by URL via a registered **`mem://` scheme**
  (`mem_test.go`): one isolated in-memory bfs bucket per URL host. This is the
  only credential-free bfs backend that round-trips version metadata, so it's
  what exercises the skip logic.
- **Do not switch tests to `file://`.** `bfs/bfsfs` discards object metadata
  (`Create` ignores `WriteOptions`, `Head` returns no metadata), so version
  skipping is a no-op there and every `Skipped` / version assertion would fail.
  The same caveat is real in production — `file://` works but never skips.
- Test fixtures live in `depot_test.go` (`seed`, `writeN`, `readMessages`) and
  `mem_test.go` (`seedStore`, `collectMessages`). `internal/testdata` holds the
  protobuf `MockMessage` used across format/reader/writer tests.

## Conventions

- Go 1.26. Prefer `for range n` over C-style counters; iterators (`iter.Seq`)
  over manual decode loops.
- `bfs.Object.Close` / `bfs.Bucket.Close` / `Writer.Discard` are in the errcheck
  exclude list (`.golangci.yml`) — defer them bare; check everything else.
- `internal/testdata/testdata.pb.go` is generated: `make proto` after editing
  the `.proto`.
