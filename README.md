# depot

[![Go reference](https://pkg.go.dev/badge/github.com/bsm/depot.svg)](https://pkg.go.dev/github.com/bsm/depot)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Decoupled data exchange between services, over object storage.

A producer deposits a versioned snapshot of a dataset into a shared location
(S3, GCS, a local directory, ...). Consumers poll that location and collect the
latest snapshot into whatever in-memory shape they need, refreshing in the
background. Producer and consumer never talk directly and don't need to be up at
the same time — the store sits between them.

`depot` handles the versioning, skip-when-unchanged, encoding, compression and
the refresh loop. You provide the data and the shape you want it in.

## Install

```sh
go get github.com/bsm/depot
```

Storage backends come from [bfs](https://github.com/bsm/bfs) — import the driver
for the scheme you use, e.g. `github.com/bsm/bfs/bfss3` for `s3://` or
`github.com/bsm/bfs/bfsfs` for `file://`.

## Produce

Deposit a full snapshot at a version. The write is skipped when the remote is
already at that version or newer, so it is safe to run on a tight cron.

```go
type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// version is any monotonic stamp that changes when the data changes,
// e.g. max(updated_at) as unix nanos.
status, err := depot.Produce(ctx, "s3://my-bucket/users.ndjson.gz", version,
	func(emit func(*User) error) error {
		for _, u := range users {
			if err := emit(u); err != nil {
				return err
			}
		}
		return nil
	})
```

## Subscribe

Collect the snapshot once, then keep it fresh in the background. `build` turns
the decoded item stream into whatever you want `Load` to return — a slice, a
lookup map, a prebuilt index.

```go
sub, err := depot.Subscribe(ctx, "s3://my-bucket/users.ndjson.gz", time.Minute,
	func(rows iter.Seq[*User]) (map[int]*User, error) {
		byID := make(map[int]*User)
		for u := range rows {
			byID[u.ID] = u
		}
		return byID, nil
	})
if err != nil {
	return err
}
defer sub.Close()

// hot path: lock-free read of the latest snapshot
user := sub.Load()[42]
```

`Load` returns the last successfully built snapshot, swapped atomically on each
refresh. A refresh that finds no new version is skipped; one that fails to
decode or build leaves the previous snapshot in place (register `OnError` to
observe background failures). Pass `every == 0` for a one-shot load with no
background loop, and call `Refresh` to poll on demand.

## Versions & skipping

Each snapshot carries its `version` in object metadata. Producers skip writing
when the remote version is already current; consumers skip reading when they
already hold it. `Force()` overrides the producer check.

> **Backends must persist metadata.** The version is stored as an object
> metadata header, so skipping only works on backends that round-trip metadata
> (S3, GCS, in-memory). The local `file://` backend does **not** persist
> metadata: produce/subscribe stay correct but never skip — every run
> re-transfers the full snapshot.

## Formats & compression

Both are auto-detected from the URL extension, or set explicitly with
`WithFormat` / `WithCompression`:

| Format   | Extensions                   |
| -------- | ---------------------------- |
| JSON     | `.json`, `.ndjson`           |
| Protobuf | `.pb`, `.proto`, `.protobuf` |
| CBOR     | `.cbor`                      |

| Compression | Extensions                     |
| ----------- | ------------------------------ |
| gzip        | `.gz`, trailing `z` (`.jsonz`) |
| flate       | `.flate`                       |
| zstd        | `.zst`                         |

Protobuf items must be `proto.Message` pointers.

## Incremental feeds

For large, mostly-append datasets, `ProduceIncremental` writes only the items
changed since the last version and records data files in a manifest;
`Subscribe(..., WithIncremental())` reads them back as one stream.

```go
depot.ProduceIncremental(ctx, "s3://my-bucket/events/", version,
	func(since int64, emit func(*Event) error) error {
		for _, e := range eventsChangedAfter(since) {
			if err := emit(e); err != nil {
				return err
			}
		}
		return nil
	})
```

## Migrating from feedx

`depot` replaces [feedx](https://github.com/bsm/feedx). The concepts carry over;
the API is smaller and generic. The `Producer`, `Consumer`, `Job`, `CronJob`,
`IncrementalProducer` types and the `ProduceFunc` / `ConsumeFunc` / `VersionCheck`
/ `BeforeHook` / `AfterHook` callbacks are all gone.

| feedx                                        | depot                                     |
| -------------------------------------------- | ----------------------------------------- |
| `NewProducer` + `Producer.Produce`           | `Produce`                                 |
| `NewJob().WithVersionCheck(fn).Produce(...)` | compute the version, pass it to `Produce` |
| `NewIncrementalProducer` + `.Produce`        | `ProduceIncremental`                      |
| `NewConsumer` + `Job.Consume` + `RunEvery`   | `Subscribe`                               |
| `NewIncrementalConsumer(bucketURL)`          | `Subscribe(..., WithIncremental())`       |
| conditional version-check to force a write   | `Force()`                                 |
| `AfterSync(func(*Status, error))`            | `OnError(func(error))` / `Refresh` return |
| `Writer.Encode(v)`                           | the `emit` callback                       |
| `Reader.Decode(&v)` loop                     | range over `iter.Seq[*T]`                 |
| `WriterOptions{Format, Compression}`         | `WithFormat` / `WithCompression`          |

### Producing

```go
// feedx: version check and produce func are separate, threaded through a Job.
status, err := feedx.NewJob().
	WithVersionCheck(func(ctx context.Context) (int64, error) { return latest(ctx) }).
	Produce(ctx, url, func(w *feedx.Writer) error {
		for _, u := range users {
			if err := w.Encode(u); err != nil {
				return err
			}
		}
		return nil
	})

// depot: compute the version, then produce.
version, err := latest(ctx)
if err != nil {
	return err
}
status, err := depot.Produce(ctx, url, version, func(emit func(*User) error) error {
	for _, u := range users {
		if err := emit(u); err != nil {
			return err
		}
	}
	return nil
})
```

### Consuming

```go
// feedx: initial consume + a cron re-invoking the same call, decode loop by hand,
// snapshot stored in your own atomic.Pointer.
job := feedx.NewJob()
job.Consume(ctx, url, consume) // initial
cron := job.RunEvery(time.Minute, func(job *feedx.Job) (*feedx.Status, error) {
	return job.Consume(ctx, url, consume)
})
// ...where consume decodes in a for/EOF loop and swaps an atomic.Pointer.

// depot: one call owns the initial load, the refresh loop and the snapshot.
sub, err := depot.Subscribe(ctx, url, time.Minute,
	func(rows iter.Seq[*User]) (map[int]*User, error) {
		byID := make(map[int]*User)
		for u := range rows {
			byID[u.ID] = u
		}
		return byID, nil
	})
defer sub.Close()
byID := sub.Load()
```

`Subscribe` does the initial load synchronously and returns its error. Pass
`WithoutInitialSync()` to skip it (the first background `Refresh` populates the
snapshot instead) and poll `Ready()` to tell when a snapshot is available.

### Notes

- `Subscribe` yields `*T` (freshly allocated per item), not `T` — required for
  protobuf messages, which must not be copied by value.
- The metadata header is renamed `X-Feedx-Version` → `X-Depot-Version`. depot
  writes the new header but falls back to reading the legacy one, so a depot
  consumer tracks versions seamlessly on a feed still produced by feedx — no
  re-read on cutover.
- `Reader`, `Writer`, `MultiReader`, `Format` and `Compression` are unchanged
  and still available for lower-level use (see `Rows` for a typed reader).

## License

Apache 2.0 — see [LICENSE](LICENSE).
