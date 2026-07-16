package depot

import (
	"context"
	"time"

	"github.com/bsm/bfs"
)

// Produce deposits a full snapshot at url under the given version. The write is
// skipped when the remote is already at that version or newer, unless Force is set.
//
// records is invoked once and should emit every item via the emit callback;
// returning an error aborts the write and discards any partial output.
// Cancelling ctx aborts between items and never commits a partial snapshot.
func Produce[T any](ctx context.Context, url string, version int64, records func(emit func(T) error) error, opts ...Option) (*Status, error) {
	obj, err := bfs.NewObject(ctx, url)
	if err != nil {
		return nil, err
	}
	defer obj.Close()

	cfg := newConfig(opts)
	status := &Status{LocalVersion: version, Start: time.Now()}

	remoteVersion, err := fetchRemoteVersion(ctx, obj)
	if err != nil {
		return nil, err
	}
	status.RemoteVersion = remoteVersion

	if !cfg.force && skipSync(version, remoteVersion) {
		status.Skipped = true
		return status, nil
	}

	w := NewWriter(ctx, obj, cfg.writerOptions(version))
	defer w.Discard()

	if err := records(emitFunc[T](ctx, w)); err != nil {
		return nil, err
	}
	// never commit a truncated snapshot for a cancelled caller
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := w.Commit(); err != nil {
		return nil, err
	}

	status.NumItems = w.NumWritten()
	return status, nil
}

// ProduceIncremental appends a new data file to the incremental bucket at url
// and updates its manifest. The write is skipped when the manifest is already at
// version or newer, unless Force is set.
//
// records receives the previous remote version so it can emit only the items
// that changed since then.
func ProduceIncremental[T any](ctx context.Context, url string, version int64, records func(since int64, emit func(T) error) error, opts ...Option) (*Status, error) {
	bucket, err := bfs.Connect(ctx, url)
	if err != nil {
		return nil, err
	}
	defer bucket.Close()

	cfg := newConfig(opts)
	obj := bfs.NewObjectFromBucket(bucket, manifestName)
	defer obj.Close()

	status := &Status{LocalVersion: version, Start: time.Now()}

	mft, err := loadManifest(ctx, obj)
	if err != nil {
		return nil, err
	}
	status.RemoteVersion = mft.Version

	if !cfg.force && skipSync(version, mft.Version) {
		status.Skipped = true
		return status, nil
	}

	wopt := cfg.writerOptions(version)
	fname := mft.newDataFileName(wopt)

	dataObj := bfs.NewObjectFromBucket(bucket, fname)
	defer dataObj.Close()

	w := NewWriter(ctx, dataObj, wopt)
	defer w.Discard()

	if err := records(mft.Version, emitFunc[T](ctx, w)); err != nil {
		return nil, err
	}
	// never commit a truncated snapshot for a cancelled caller
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := w.Commit(); err != nil {
		return nil, err
	}

	mft.Files = append(mft.Files, fname)
	mft.Version = version
	if err := writeManifest(ctx, obj, mft, version); err != nil {
		return nil, err
	}

	status.NumItems = w.NumWritten()
	return status, nil
}

// emitFunc wraps a writer into an emit callback that obeys ctx between items:
// bfs writers may buffer locally until Commit, so a slow records loop would
// otherwise never notice a cancelled caller.
func emitFunc[T any](ctx context.Context, w *Writer) func(T) error {
	return func(v T) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return w.Encode(v)
	}
}

func writeManifest(ctx context.Context, obj *bfs.Object, mft *manifest, version int64) error {
	w := NewWriter(ctx, obj, &WriterOptions{Version: version})
	defer w.Discard()

	if err := w.Encode(mft); err != nil {
		return err
	}
	return w.Commit()
}
