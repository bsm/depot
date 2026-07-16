package depot

const manifestName = "manifest.json"

// Option configures a Produce or Subscribe call.
type Option func(*config)

type config struct {
	format      Format
	compression Compression
	force       bool
	incremental bool
	skipInitial bool
	onError     func(error)
}

func newConfig(opts []Option) *config {
	c := new(config)
	for _, o := range opts {
		o(c)
	}
	return c
}

func (c *config) writerOptions(version int64) *WriterOptions {
	return &WriterOptions{Format: c.format, Compression: c.compression, Version: version}
}

func (c *config) readerOptions() *ReaderOptions {
	return &ReaderOptions{Format: c.format, Compression: c.compression}
}

// WithFormat overrides the data format. Default: auto-detected from the URL.
func WithFormat(f Format) Option { return func(c *config) { c.format = f } }

// WithCompression overrides the compression. Default: auto-detected from the URL.
func WithCompression(cn Compression) Option { return func(c *config) { c.compression = cn } }

// Force makes Produce write even when the remote is already up to date.
func Force() Option { return func(c *config) { c.force = true } }

// WithIncremental treats the URL as a bucket of incremental data files described
// by a manifest, rather than a single object.
func WithIncremental() Option { return func(c *config) { c.incremental = true } }

// WithoutInitialSync makes Subscribe return immediately without the synchronous
// initial load. The snapshot is populated by the first background refresh (so it
// only makes sense with every > 0) or by a manual Refresh. Use Ready to tell
// whether a snapshot is available yet.
func WithoutInitialSync() Option { return func(c *config) { c.skipInitial = true } }

// OnError registers a callback for errors from background Subscribe refreshes.
// The initial refresh returns its error directly and does not invoke this callback.
func OnError(fn func(error)) Option { return func(c *config) { c.onError = fn } }
