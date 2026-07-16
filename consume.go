package depot

import (
	"context"
	"errors"
	"io"
	"iter"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsm/bfs"
)

// Subscribe collects a snapshot from url and keeps it up to date. It performs an
// initial synchronous load and, when every > 0, refreshes in the background on
// that interval. Refreshes that find no new version are skipped cheaply.
//
// build is called on every change with the decoded item stream (each item a
// freshly-allocated *T) and returns the snapshot to publish; it should drain the
// sequence. Use Load to read the most recent snapshot. A decode error or a build
// error aborts the refresh and leaves the previous snapshot in place.
//
// ctx applies to the initial load only, not to the subscription's lifetime.
// Close stops the refresh loop and aborts any in-flight sync, so a terminating
// app (e.g. on SIGTERM) is never blocked by a slow read.
func Subscribe[T, S any](ctx context.Context, url string, every time.Duration, build func(iter.Seq[*T]) (S, error), opts ...Option) (*Subscription[S], error) {
	cfg := newConfig(opts)
	sub := &Subscription[S]{cfg: cfg}

	if cfg.incremental {
		bucket, err := bfs.Connect(ctx, url)
		if err != nil {
			return nil, err
		}
		sub.bucket = bucket
		sub.remote = bfs.NewObjectFromBucket(bucket, manifestName)
	} else {
		obj, err := bfs.NewObject(ctx, url)
		if err != nil {
			return nil, err
		}
		sub.remote = obj
	}

	sub.sync = func(ctx context.Context) (*Status, error) {
		return consumeInto(ctx, sub, cfg, build)
	}

	// initial synchronous load, unless opted out
	if !cfg.skipInitial {
		if _, err := sub.sync(ctx); err != nil {
			_ = sub.Close()
			return nil, err
		}
	}

	if every > 0 {
		// deliberately not derived from the caller's ctx: the subscription is
		// long-lived and Close is its lifecycle trigger. Close cancels this ctx,
		// which aborts any in-flight background sync.
		bg, cancel := context.WithCancel(context.Background())
		sub.cancel = cancel
		sub.wait.Add(1)
		go sub.loop(bg, every)
	}
	return sub, nil
}

// Subscription is a live subscription holding the latest snapshot.
type Subscription[S any] struct {
	cfg    *config
	remote *bfs.Object
	bucket bfs.Bucket

	sync     func(context.Context) (*Status, error)
	snapshot atomic.Pointer[S]
	version  atomic.Int64
	ready    atomic.Bool

	cancel context.CancelFunc
	wait   sync.WaitGroup
}

// Load returns the most recently built snapshot, or the zero value before the
// first successful build.
func (s *Subscription[S]) Load() S {
	if p := s.snapshot.Load(); p != nil {
		return *p
	}
	var zero S
	return zero
}

// Version returns the most recently consumed remote version.
func (s *Subscription[S]) Version() int64 { return s.version.Load() }

// Ready reports whether at least one snapshot has been built and is available
// via Load. It is always true after Subscribe returns unless WithoutInitialSync
// was set, in which case it flips true once the first refresh succeeds.
func (s *Subscription[S]) Ready() bool { return s.ready.Load() }

// Refresh triggers a synchronous refresh and returns its status. Background
// refreshes call this on the configured interval; it is also safe to call
// directly (e.g. from a webhook) to poll on demand.
func (s *Subscription[S]) Refresh(ctx context.Context) (*Status, error) { return s.sync(ctx) }

// Close stops the background refresh loop, aborting any in-flight sync, and
// releases the remote. It blocks until the loop has exited.
func (s *Subscription[S]) Close() error {
	if s.cancel != nil {
		s.cancel()
		s.wait.Wait()
	}

	var err error
	if s.remote != nil {
		if e := s.remote.Close(); e != nil {
			err = errors.Join(err, e)
		}
		s.remote = nil
	}
	if s.bucket != nil {
		if e := s.bucket.Close(); e != nil {
			err = errors.Join(err, e)
		}
		s.bucket = nil
	}
	return err
}

func (s *Subscription[S]) loop(ctx context.Context, every time.Duration) {
	defer s.wait.Done()

	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		status, err := s.sync(ctx)
		switch {
		case err == nil:
			if s.cfg.onSync != nil {
				s.cfg.onSync(status)
			}
		case ctx.Err() != nil:
			return // shutting down, not a real refresh failure
		default:
			if s.cfg.onError != nil {
				s.cfg.onError(err)
			}
		}
	}
}

func consumeInto[T, S any](ctx context.Context, sub *Subscription[S], cfg *config, build func(iter.Seq[*T]) (S, error)) (*Status, error) {
	localVersion := sub.version.Load()
	status := &Status{LocalVersion: localVersion, Start: time.Now()}

	remoteVersion, reader, err := sub.openReader(ctx, cfg)
	if err != nil {
		return nil, err
	}
	status.RemoteVersion = remoteVersion

	if skipSync(remoteVersion, localVersion) {
		status.Skipped = true
		return status, nil
	}
	defer reader.Close()

	var decErr error
	seq := func(yield func(*T) bool) {
		for {
			// obey ctx between items: bfs reads are ctx-aware, but decoding
			// buffered data is not and must not outlive a cancelled caller
			if err := ctx.Err(); err != nil {
				decErr = err
				return
			}
			v := new(T)
			switch err := reader.Decode(v); {
			case errors.Is(err, io.EOF):
				return
			case err != nil:
				decErr = err
				return
			}
			if !yield(v) {
				return
			}
		}
	}

	snap, err := build(seq)
	if decErr != nil {
		return nil, decErr
	}
	if err != nil {
		return nil, err
	}

	status.NumItems = reader.NumRead()
	sub.snapshot.Store(&snap)
	sub.version.Store(remoteVersion)
	sub.ready.Store(true)
	return status, nil
}

// openReader resolves the remote version and a reader for it. For incremental
// feeds the version comes from the manifest and the reader spans its data files.
// The reader is only opened when there is something to read.
func (s *Subscription[S]) openReader(ctx context.Context, cfg *config) (int64, *Reader, error) {
	if !cfg.incremental {
		version, err := fetchRemoteVersion(ctx, s.remote)
		if err != nil {
			return 0, nil, err
		}
		if skipSync(version, s.version.Load()) {
			return version, nil, nil
		}
		reader, err := NewReader(ctx, s.remote, cfg.readerOptions())
		return version, reader, err
	}

	mft, err := loadManifest(ctx, s.remote)
	if err != nil {
		return 0, nil, err
	}
	if skipSync(mft.Version, s.version.Load()) {
		return mft.Version, nil, nil
	}

	remotes := make([]*bfs.Object, 0, len(mft.Files))
	for _, f := range mft.Files {
		remotes = append(remotes, bfs.NewObjectFromBucket(s.bucket, f))
	}
	reader := MultiReader(ctx, remotes, cfg.readerOptions())
	reader.ownRemotes = true
	return mft.Version, reader, nil
}

// Rows decodes a reader into a typed sequence, yielding a non-nil error as the
// final element if decoding fails. It is the low-level building block behind
// Subscribe, exposed for callers that manage their own reader.
func Rows[T any](r *Reader) iter.Seq2[*T, error] {
	return func(yield func(*T, error) bool) {
		for {
			v := new(T)
			switch err := r.Decode(v); {
			case errors.Is(err, io.EOF):
				return
			case err != nil:
				yield(nil, err)
				return
			}
			if !yield(v, nil) {
				return
			}
		}
	}
}
