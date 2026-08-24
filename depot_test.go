package depot_test

import (
	"context"
	"io"
	"iter"
	"net/url"
	"reflect"
	"sync"
	"testing"

	"github.com/bsm/bfs"
	"github.com/bsm/depot"
	"github.com/bsm/depot/internal/testdata"
)

type message = testdata.MockMessage

// checkStatus asserts got matches exp, after verifying Start was populated and
// clearing it (Start is a wall-clock time and not comparable by value).
func checkStatus(t *testing.T, got, exp *depot.Status) {
	t.Helper()
	if got != nil {
		if got.Start.IsZero() {
			t.Error("expected Start to be set")
		}
		got.Start = exp.Start
	}
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("expected %#v, got %#v", exp, got)
	}
}

func TestFetchRemoteVersion(t *testing.T) {
	write := func(t *testing.T, meta bfs.Metadata) *bfs.Object {
		t.Helper()
		obj := bfs.NewInMemObject("file.json")
		t.Cleanup(func() { _ = obj.Close() })

		w, err := obj.Create(t.Context(), &bfs.WriteOptions{Metadata: meta})
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		if _, err := w.Write([]byte("{}")); err != nil {
			t.Fatal("unexpected error", err)
		}
		if err := w.Commit(); err != nil {
			t.Fatal("unexpected error", err)
		}
		return obj
	}

	assert := func(t *testing.T, obj *bfs.Object, exp int64) {
		t.Helper()
		got, err := depot.FetchRemoteVersion(t.Context(), obj)
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		if got != exp {
			t.Errorf("expected %v, got %v", exp, got)
		}
	}

	t.Run("current header", func(t *testing.T) {
		assert(t, write(t, bfs.Metadata{depot.MetaVersion: "134"}), 134)
	})

	t.Run("legacy fallback", func(t *testing.T) {
		assert(t, write(t, bfs.Metadata{depot.MetaVersionLegacy: "101"}), 101)
	})

	t.Run("current takes precedence", func(t *testing.T) {
		assert(t, write(t, bfs.Metadata{depot.MetaVersion: "134", depot.MetaVersionLegacy: "101"}), 134)
	})
}

func seed() *testdata.MockMessage {
	return &testdata.MockMessage{
		Name:   "Joe",
		Enum:   testdata.MockEnum_FIRST,
		Height: 180,
	}
}

func seedN(n int) []*testdata.MockMessage {
	res := make([]*testdata.MockMessage, 0, n)
	for range n {
		res = append(res, seed())
	}
	return res
}

func writeN(obj *bfs.Object, numEntries int, version int64) error {
	w := depot.NewWriter(context.Background(), obj, &depot.WriterOptions{Version: version})
	defer w.Discard()

	for range numEntries {
		if err := w.Encode(seed()); err != nil {
			return err
		}
	}
	return w.Commit()
}

func readMessages(r interface{ Decode(any) error }) ([]*testdata.MockMessage, error) {
	var msgs []*testdata.MockMessage
	for {
		var msg testdata.MockMessage
		err := r.Decode(&msg)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, &msg)
	}
	return msgs, nil
}

// mem:// resolves to an in-memory bucket per URL host, so each test gets an
// isolated store addressable by URL (as the real s3://|file:// backends are).
var (
	memMu      sync.Mutex
	memBuckets = map[string]*bfs.InMem{}
)

func init() {
	bfs.Register("mem", func(_ context.Context, u *url.URL) (bfs.Bucket, error) {
		memMu.Lock()
		defer memMu.Unlock()
		b := memBuckets[u.Host]
		if b == nil {
			b = bfs.NewInMem()
			memBuckets[u.Host] = b
		}
		return b, nil
	})
}

// seedStore produces numEntries seed messages to url at the given version.
func seedStore(url string, numEntries int, version int64) error {
	_, err := depot.Produce(context.Background(), url, version,
		func(emit func(*testdata.MockMessage) error) error {
			for range numEntries {
				if err := emit(seed()); err != nil {
					return err
				}
			}
			return nil
		})
	return err
}

// collectMessages is a Subscribe build func gathering all decoded messages.
func collectMessages(rows iter.Seq[*testdata.MockMessage]) ([]*testdata.MockMessage, error) {
	var msgs []*testdata.MockMessage
	for msg := range rows {
		msgs = append(msgs, msg)
	}
	return msgs, nil
}
