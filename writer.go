package depot

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strconv"

	"github.com/bsm/bfs"
)

// WriterOptions configure the writer instance.
type WriterOptions struct {
	// Format specifies the format
	// Default: auto-detected from URL path.
	Format Format

	// Compression specifies the compression type.
	// Default: auto-detected from URL path.
	Compression Compression

	// Version is stored with the remote metadata.
	// Default: 0
	Version int64
}

func (o *WriterOptions) norm(name string) {
	if o.Format == nil {
		o.Format = DetectFormat(name)
	}
	if o.Compression == nil {
		o.Compression = DetectCompression(name)
	}
}

// Writer encodes feeds to remote locations.
type Writer struct {
	ctx    context.Context
	remote *bfs.Object
	opt    WriterOptions
	num    int64

	bw bfs.Writer
	cw io.WriteCloser // compression writer
	ww *bufio.Writer
	fe FormatEncoder

	// done is set by the first Commit or Discard, so the usual deferred
	// Discard after a Commit does not close the encoders a second time.
	done bool
}

// NewWriter inits a new feed writer.
func NewWriter(ctx context.Context, remote *bfs.Object, opt *WriterOptions) *Writer {
	var o WriterOptions
	if opt != nil {
		o = *opt
	}
	o.norm(remote.Name())

	return &Writer{
		ctx:    ctx,
		remote: remote,
		opt:    o,
	}
}

// Write writes raw bytes to the feed.
func (w *Writer) Write(p []byte) (int, error) {
	if err := w.ensureCreated(); err != nil {
		return 0, err
	}
	return w.ww.Write(p)
}

// WriteString writes a raw string to the feed.
func (w *Writer) WriteString(s string) (int, error) {
	if err := w.ensureCreated(); err != nil {
		return 0, err
	}
	return w.ww.WriteString(s)
}

// Encode appends a value to the feed.
func (w *Writer) Encode(v any) error {
	if err := w.ensureCreated(); err != nil {
		return err
	}

	if w.fe == nil {
		fe, err := w.opt.Format.NewEncoder(w.ww)
		if err != nil {
			return err
		}
		w.fe = fe
	}

	if err := w.fe.Encode(v); err != nil {
		return err
	}

	w.num++
	return nil
}

// NumWritten returns the number of written values.
func (w *Writer) NumWritten() int64 {
	return w.num
}

// Discard closes the writer and discards the contents. It does nothing after
// Commit.
func (w *Writer) Discard() error {
	if w.done {
		return nil
	}
	w.done = true

	err := w.close()
	if w.bw != nil {
		if e := w.bw.Discard(); e != nil {
			err = errors.Join(err, e)
		}
	}
	return err
}

// Commit closes the writer and persists the contents. A writer with nothing
// written still creates the remote: an empty snapshot is a valid snapshot,
// and without the object its version would never reach consumers.
func (w *Writer) Commit() error {
	if w.done {
		return errors.New("depot: writer already closed")
	}
	w.done = true

	if err := w.ensureCreated(); err != nil {
		_ = w.close()
		if w.bw != nil {
			_ = w.bw.Discard()
		}
		return err
	}
	err := w.close()
	if w.bw != nil {
		if e := w.bw.Commit(); e != nil {
			err = errors.Join(err, e)
		}
	}
	return err
}

func (w *Writer) close() (err error) {
	if w.fe != nil {
		if e := w.fe.Close(); e != nil {
			err = errors.Join(err, e)
		}
	}
	if w.ww != nil {
		if e := w.ww.Flush(); e != nil {
			err = errors.Join(err, e)
		}
	}
	if w.cw != nil {
		if e := w.cw.Close(); e != nil {
			err = errors.Join(err, e)
		}
	}
	return err
}

func (w *Writer) ensureCreated() error {
	if w.bw == nil {
		bw, err := w.remote.Create(w.ctx, &bfs.WriteOptions{
			Metadata: bfs.Metadata{metaVersion: strconv.FormatInt(w.opt.Version, 10)},
		})
		if err != nil {
			return err
		}
		w.bw = bw
	}

	if w.cw == nil {
		cw, err := w.opt.Compression.NewWriter(w.bw)
		if err != nil {
			return err
		}
		w.cw = cw
	}

	if w.ww == nil {
		w.ww = bufio.NewWriter(w.cw)
	}

	return nil
}
