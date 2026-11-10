// Package depot exchanges data between services over object storage.
//
// A producer deposits a versioned snapshot at a URL with Produce (or
// ProduceIncremental). Consumers collect the latest snapshot with Subscribe
// and keep it fresh in the background. Storage backends come from bfs.
//
// Each snapshot carries its version in the X-Depot-Version object metadata
// header. Produce skips the write when the remote is already at that version or
// newer (Force overrides this). Subscribe skips the read when it already holds
// the remote version.
//
// Skipping needs a backend that keeps object metadata (S3, GCS, in-memory).
// The bfs file:// backend drops it: Produce and Subscribe stay correct there,
// but never skip, so every run moves the full snapshot.
//
// The format and compression are detected from the URL extension (see
// DetectFormat and DetectCompression), or set with WithFormat and
// WithCompression.
package depot

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/bsm/bfs"
)

// ErrNotModified is used to signal that something has not been modified.
var ErrNotModified = errors.New("depot: not modified")

const (
	metaVersion = "X-Depot-Version"
	// metaVersionLegacy is feedx's header, read as a fallback so depot can
	// consume feeds still produced by feedx. depot only ever writes metaVersion.
	metaVersionLegacy = "X-Feedx-Version"
)

func fetchRemoteVersion(ctx context.Context, obj *bfs.Object) (int64, error) {
	info, err := obj.Head(ctx)
	if errors.Is(err, bfs.ErrNotFound) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	raw := info.Metadata.Get(metaVersion)
	if raw == "" {
		raw = info.Metadata.Get(metaVersionLegacy)
	}
	version, _ := strconv.ParseInt(raw, 10, 64)
	return version, nil
}

// Status is returned by sync processes.
type Status struct {
	// Skipped indicates the sync was skipped, because there were no new changes.
	Skipped bool
	// LocalVersion indicates the local version before sync.
	LocalVersion int64
	// RemoteVersion indicates the remote version before sync.
	RemoteVersion int64
	// NumItems returns the number of items processed, either read or written.
	NumItems int64
	// Start is the wall-clock time the sync began; callbacks can derive the
	// elapsed time with time.Since(Start).
	Start time.Time
}

func skipSync(srcVersion, targetVersion int64) bool {
	return (srcVersion != 0 || targetVersion != 0) && srcVersion <= targetVersion
}
