package depot_test

import (
	"context"
	"errors"
	"testing"
	"time"

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
		checkStatus(t, status, &depot.Status{LocalVersion: 101, RemoteVersion: 101, Skipped: true})
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
		checkStatus(t, status, &depot.Status{NumItems: 2})
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

	t.Run("OnSync instrumentation", func(t *testing.T) {
		const url = "mem://subscribe-onsync/file.json"
		if err := seedStore(url, 2, 101); err != nil {
			t.Fatal("unexpected error", err)
		}

		synced := make(chan *depot.Status, 16)
		sub, err := depot.Subscribe(t.Context(), url, time.Millisecond, collectMessages,
			depot.OnSync(func(s *depot.Status) { synced <- s }))
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		defer func() { _ = sub.Close() }()

		// the remote is unchanged since the initial load, so the first background
		// refresh is a skip — and it carries a populated Start
		select {
		case st := <-synced:
			if !st.Skipped {
				t.Errorf("expected skipped, got %+v", st)
			}
			if st.Start.IsZero() {
				t.Error("expected Start to be set")
			}
		case <-time.After(time.Second):
			t.Fatal("OnSync was not called")
		}
	})

	t.Run("aborts on ctx cancel", func(t *testing.T) {
		const url = "mem://subscribe-cancel/file.json"
		if err := seedStore(url, 2, 101); err != nil {
			t.Fatal("unexpected error", err)
		}

		sub, err := depot.Subscribe(t.Context(), url, 0, collectMessages)
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		defer func() { _ = sub.Close() }()

		// a newer version is available, but the caller's ctx is cancelled:
		// the refresh must abort mid-decode and keep the previous snapshot
		if err := seedStore(url, 4, 202); err != nil {
			t.Fatal("unexpected error", err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		if _, err := sub.Refresh(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if exp, got := int64(101), sub.Version(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
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

func TestSubscribe_DeferredInitialSyncIsImmediate(t *testing.T) {
	const url = "mem://subscribe-deferred/file.json"
	if err := seedStore(url, 2, 101); err != nil {
		t.Fatal("unexpected error", err)
	}

	// With an hour between ticks, only an immediate deferred sync can flip
	// Ready within the deadline.
	synced := make(chan struct{})
	sub, err := depot.Subscribe(t.Context(), url, time.Hour, collectMessages,
		depot.WithoutInitialSync(),
		depot.OnSync(func(*depot.Status) { close(synced) }))
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	defer func() { _ = sub.Close() }()

	select {
	case <-synced:
	case <-time.After(5 * time.Second):
		t.Fatal("deferred initial sync did not run immediately")
	}
	if !sub.Ready() {
		t.Error("expected ready after the deferred initial sync")
	}
	if exp, got := 2, len(sub.Load()); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}
}
