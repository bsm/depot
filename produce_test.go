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

// An empty snapshot must still be published, or its version never reaches
// consumers and every run writes again.
func TestProduce_empty(t *testing.T) {
	const url = "mem://produce-empty/file.json"

	none := func(func(*testdata.MockMessage) error) error { return nil }
	if _, err := depot.Produce(t.Context(), url, 5, none); err != nil {
		t.Fatal("unexpected error", err)
	}

	status, err := depot.Produce(t.Context(), url, 5, none)
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	checkStatus(t, status, &depot.Status{LocalVersion: 5, RemoteVersion: 5, Skipped: true})

	sub, err := depot.Subscribe(t.Context(), url, 0, collectMessages)
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	defer func() { _ = sub.Close() }()
	if got := sub.Version(); got != 5 {
		t.Errorf("expected version 5, got %d", got)
	}
	if got := sub.Load(); len(got) != 0 {
		t.Errorf("expected no messages, got %d", len(got))
	}
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
		Files:   []string{"data-101.json", "data-134.json"},
	}); !reflect.DeepEqual(exp, mft) {
		t.Errorf("expected %#v, got %#v", exp, mft)
	}
}

// changesSince is a RecordsSinceFunc for a feed whose only change is a batch
// of n items at version at: asked for the changes since then, it emits none.
func changesSince(at int64, n int) depot.RecordsSinceFunc[*testdata.MockMessage] {
	return func(since int64, emit func(*testdata.MockMessage) error) error {
		if since >= at {
			return nil
		}
		for range n {
			if err := emit(seed()); err != nil {
				return err
			}
		}
		return nil
	}
}

// A forced re-produce of a published version must keep that version's batch:
// it neither overwrites the batch with an empty one nor lists it twice.
func TestProduceIncremental_force(t *testing.T) {
	const url = "mem://produce-incremental-force/"

	for range 2 {
		if _, err := depot.ProduceIncremental(t.Context(), url, 7, changesSince(7, 3), depot.Force()); err != nil {
			t.Fatal("unexpected error", err)
		}
	}
	testIncrementalItems(t, url, 3)
}

// Same as above, on a manifest written by feedx with its "data-0-<version>"
// file names.
func TestProduceIncremental_forceFeedxManifest(t *testing.T) {
	const url = "mem://produce-incremental-feedx/"

	bucket := mustBucket(t, url)
	data := bfs.NewObjectFromBucket(bucket, "data-0-101.json")
	defer data.Close()
	if err := writeN(data, 3, 101); err != nil {
		t.Fatal("unexpected error", err)
	}

	obj := bfs.NewObjectFromBucket(bucket, "manifest.json")
	defer obj.Close()
	w := depot.NewWriter(t.Context(), obj, &depot.WriterOptions{Version: 101})
	defer w.Discard()
	if err := w.Encode(&depot.Manifest{Version: 101, Files: []string{"data-0-101.json"}}); err != nil {
		t.Fatal("unexpected error", err)
	}
	if err := w.Commit(); err != nil {
		t.Fatal("unexpected error", err)
	}

	if _, err := depot.ProduceIncremental(t.Context(), url, 101, changesSince(101, 3), depot.Force()); err != nil {
		t.Fatal("unexpected error", err)
	}
	testIncrementalItems(t, url, 3)
}

func testIncrementalItems(t *testing.T, url string, exp int) {
	t.Helper()

	sub, err := depot.Subscribe(t.Context(), url, 0, collectMessages, depot.WithIncremental())
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	defer func() { _ = sub.Close() }()
	if got := len(sub.Load()); got != exp {
		t.Errorf("expected %d messages, got %d", exp, got)
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
