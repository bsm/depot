package depot

import (
	"context"

	"github.com/bsm/bfs"
)

type NoFormat = noFormat

type Manifest manifest

func LoadManifest(ctx context.Context, obj *bfs.Object) (*Manifest, error) {
	m, err := loadManifest(ctx, obj)
	return (*Manifest)(m), err
}

const (
	MetaVersion       = metaVersion
	MetaVersionLegacy = metaVersionLegacy
)

func FetchRemoteVersion(ctx context.Context, obj *bfs.Object) (int64, error) {
	return fetchRemoteVersion(ctx, obj)
}
