package depot

import (
	"context"
	"errors"
	"strconv"

	"github.com/bsm/bfs"
)

// manifest holds the current feed status.
// The current manifest is consumed before each push and a new manifest written after each push.
//
// feedx manifests also carry a "generation" counter. feedx never incremented
// it, so depot ignores it on read and does not write it.
type manifest struct {
	// Version holds the most recent version of the records included in Files.
	Version int64 `json:"version"`
	// Files holds the data files, in write order.
	Files []string `json:"files"`
}

func loadManifest(ctx context.Context, obj *bfs.Object) (*manifest, error) {
	m := new(manifest)

	r, err := NewReader(ctx, obj, nil)
	if errors.Is(err, bfs.ErrNotFound) {
		return m, nil
	} else if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()

	if err := r.Decode(m); errors.Is(err, bfs.ErrNotFound) { // some BFS implementations defer Open-ing the S3 object till first Decode call
		return m, nil
	} else if err != nil {
		return nil, err
	}

	return m, nil
}

func newDataFileName(wopt *WriterOptions) string {
	formatExt := ".json"
	switch wopt.Format {
	case ProtobufFormat:
		formatExt = ".pb"
	case CBORFormat:
		formatExt = ".cbor"
	}

	var compressionSuffix string
	switch wopt.Compression {
	case GZipCompression:
		compressionSuffix = "z"
	case FlateCompression:
		compressionSuffix = ".flate"
	case ZstdCompression:
		compressionSuffix = ".zst"
	}

	// The name depends only on the version, so a forced re-produce of the same
	// version replaces its data file instead of adding a second one.
	return "data-" + strconv.FormatInt(wopt.Version, 10) + formatExt + compressionSuffix
}
