package depot_test

import (
	"reflect"
	"testing"

	"github.com/bsm/depot"
)

func TestSubscribe(t *testing.T) {
	t.Run("consumes", func(t *testing.T) {
		const url = "mem://subscribe-consumes/file.json"
		if err := seedStore(url, 2, 101); err != nil {
			t.Fatal("unexpected error", err)
		}

		sub, err := depot.Subscribe(t.Context(), url, 0, collectMessages)
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		defer func() { _ = sub.Close() }()

		// initial load
		if exp, got := int64(101), sub.Version(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
		if exp, got := 2, len(sub.Load()); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}

		// manual refresh, unchanged version, skipped
		status, err := sub.Refresh(t.Context())
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		if exp := (&depot.Status{LocalVersion: 101, RemoteVersion: 101, Skipped: true}); !reflect.DeepEqual(exp, status) {
			t.Errorf("expected %#v, got %#v", exp, status)
		}
	})

	t.Run("always if no version", func(t *testing.T) {
		const url = "mem://subscribe-noversion/file.json"
		if err := seedStore(url, 2, 0); err != nil {
			t.Fatal("unexpected error", err)
		}

		sub, err := depot.Subscribe(t.Context(), url, 0, collectMessages)
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		defer func() { _ = sub.Close() }()

		// a zero version is never skipped
		status, err := sub.Refresh(t.Context())
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		if exp := (&depot.Status{NumItems: 2}); !reflect.DeepEqual(exp, status) {
			t.Errorf("expected %#v, got %#v", exp, status)
		}
	})

	t.Run("without initial sync", func(t *testing.T) {
		const url = "mem://subscribe-lazy/file.json"
		if err := seedStore(url, 2, 101); err != nil {
			t.Fatal("unexpected error", err)
		}

		sub, err := depot.Subscribe(t.Context(), url, 0, collectMessages, depot.WithoutInitialSync())
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		defer func() { _ = sub.Close() }()

		// nothing loaded yet
		if sub.Ready() {
			t.Error("expected not ready before first refresh")
		}
		if got := sub.Load(); got != nil {
			t.Errorf("expected nil snapshot, got %v", got)
		}

		// a manual refresh loads it and flips Ready
		if _, err := sub.Refresh(t.Context()); err != nil {
			t.Fatal("unexpected error", err)
		}
		if !sub.Ready() {
			t.Error("expected ready after refresh")
		}
		if exp, got := 2, len(sub.Load()); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
	})

	t.Run("incremental", func(t *testing.T) {
		const url = "mem://subscribe-incremental/"
		if _, err := depot.ProduceIncremental(t.Context(), url, 101,
			func(_ int64, emit func(*message) error) error {
				return emitN(emit, 4)
			}); err != nil {
			t.Fatal("unexpected error", err)
		}

		sub, err := depot.Subscribe(t.Context(), url, 0, collectMessages, depot.WithIncremental())
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		defer func() { _ = sub.Close() }()

		if exp, got := int64(101), sub.Version(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
		if exp, got := 4, len(sub.Load()); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
	})
}

func emitN(emit func(*message) error, n int) error {
	for range n {
		if err := emit(seed()); err != nil {
			return err
		}
	}
	return nil
}
