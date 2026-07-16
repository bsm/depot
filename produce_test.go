package depot_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/bsm/bfs"
	"github.com/bsm/depot"
	"github.com/bsm/depot/internal/testdata"
)

func TestProduce(t *testing.T) {
	const url = "mem://produce/path/to/file.json"

	// first attempt
	testProduce(t, url, 101, nil, &depot.Status{
		LocalVersion: 101,
		NumItems:     10,
	})

	// second attempt, unchanged version, skipped
	testProduce(t, url, 101, nil, &depot.Status{
		LocalVersion:  101,
		RemoteVersion: 101,
		Skipped:       true,
	})

	// updated version
	testProduce(t, url, 134, nil, &depot.Status{
		LocalVersion:  134,
		RemoteVersion: 101,
		NumItems:      13,
	})

	obj, err := bfs.NewObject(t.Context(), url)
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	defer obj.Close()

	meta, err := obj.Head(t.Context())
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	if exp := (bfs.Metadata{"X-Depot-Version": "134"}); !reflect.DeepEqual(exp, meta.Metadata) {
		t.Errorf("expected %#v, got %#v", exp, meta)
	}
}

func TestProduce_cancelled(t *testing.T) {
	const url = "mem://produce-cancelled/file.json"

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	emitted := 0
	_, err := depot.Produce(ctx, url, 101, func(emit func(*testdata.MockMessage) error) error {
		for range 10 {
			if err := emit(seed()); err != nil {
				return err
			}
			emitted++
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if emitted != 0 {
		t.Errorf("expected emit to abort immediately, emitted %d", emitted)
	}

	// nothing must have been committed
	obj, err := bfs.NewObject(t.Context(), url)
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	defer obj.Close()
	if _, err := obj.Head(t.Context()); !errors.Is(err, bfs.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestProduce_force(t *testing.T) {
	const url = "mem://produce-force/file.json"

	testProduce(t, url, 101, nil, &depot.Status{LocalVersion: 101, NumItems: 10})

	// same version is normally skipped, but Force writes anyway
	testProduce(t, url, 101, []depot.Option{depot.Force()}, &depot.Status{
		LocalVersion:  101,
		RemoteVersion: 101,
		NumItems:      10,
	})
}

func testProduce(t *testing.T, url string, version int64, opts []depot.Option, exp *depot.Status) {
	t.Helper()

	status, err := depot.Produce(t.Context(), url, version, func(emit func(*testdata.MockMessage) error) error {
		for i := int64(0); i < version/10; i++ {
			if err := emit(seed()); err != nil {
				return err
			}
		}
		return nil
	}, opts...)
	if err != nil {
		t.Fatal("unexpected error", err)
	}

	checkStatus(t, status, exp)
}

func TestProduceIncremental(t *testing.T) {
	const url = "mem://produce-incremental/"

	// first produce
	testProduceIncremental(t, url, 101, &depot.Status{LocalVersion: 101, NumItems: 10})

	// second produce, unchanged version, skipped
	testProduceIncremental(t, url, 101, &depot.Status{Skipped: true, LocalVersion: 101, RemoteVersion: 101})

	// increment version
	testProduceIncremental(t, url, 134, &depot.Status{LocalVersion: 134, RemoteVersion: 101, NumItems: 3})

	obj := bfs.NewObjectFromBucket(mustBucket(t, url), "manifest.json")
	defer obj.Close()

	mft, err := depot.LoadManifest(t.Context(), obj)
	if err != nil {
		t.Fatal("unexpected error", err)
	} else if exp := (&depot.Manifest{
		Version: 134,
		Files:   []string{"data-0-101.json", "data-0-134.json"},
	}); !reflect.DeepEqual(exp, mft) {
		t.Errorf("expected %#v, got %#v", exp, mft)
	}
}

func testProduceIncremental(t *testing.T, url string, version int64, exp *depot.Status) {
	t.Helper()

	status, err := depot.ProduceIncremental(t.Context(), url, version,
		func(since int64, emit func(*testdata.MockMessage) error) error {
			n := (version - since) / 10
			for range n {
				if err := emit(seed()); err != nil {
					return err
				}
			}
			return nil
		})
	if err != nil {
		t.Fatal("unexpected error", err)
	}

	checkStatus(t, status, exp)
}

func mustBucket(t *testing.T, url string) bfs.Bucket {
	t.Helper()
	bucket, err := bfs.Connect(t.Context(), url)
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	return bucket
}
