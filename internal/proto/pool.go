package proto

import (
	"bytes"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

// encoderPool reuses *msgpack.Encoder + *bytes.Buffer pairs across
// the dispatch hot path. Each goroutine dispatching a job calls
// encodeMsgpack once or twice per job (MoveToActiveOpts +
// MoveToFinishedOpts + JobFields), so without pooling each call
// allocates a fresh encoder, a fresh buffer, and msgpack's internal
// scratch. With the pool, all three are reused; the only allocation
// per call is the returned []byte (which msgpack would have allocated
// anyway as the encoder's output).
//
// The pool's pairs reset on Get so callers don't need to clear state
// before encoding. Returned buffers shrink to a sane cap on Put to
// avoid pinning oversize buffers from outlier-large encodes.
var encoderPool = sync.Pool{
	New: func() any {
		buf := &bytes.Buffer{}
		// Pre-grow to a typical opts size (~150B for MoveToFinished's
		// keepJobs map). Avoids the first few realloc cycles.
		buf.Grow(256)
		return &pooledEncoder{
			buf: buf,
			enc: msgpack.NewEncoder(buf),
		}
	},
}

type pooledEncoder struct {
	buf *bytes.Buffer
	enc *msgpack.Encoder
}

// encodeMsgpack runs fn against a pooled encoder and returns a fresh
// []byte snapshot of the encoded bytes. The pooled buffer is reset
// and returned to the pool before the function returns; the caller
// owns the returned slice independently.
//
// Callers MUST NOT retain references to the pooled encoder beyond
// fn's return. msgpack.Encoder.Reset is called on every Get so any
// stale state from a previous encode is overwritten.
//
// shrinkCap caps the buffer's underlying capacity on Put so a one-off
// huge encode (e.g. a large JobFields list with many extra HASH
// fields) doesn't pin a multi-MB buffer in the pool.
const shrinkCap = 4096

func encodeMsgpack(fn func(*msgpack.Encoder) error) ([]byte, error) {
	pe := encoderPool.Get().(*pooledEncoder)
	defer func() {
		// Don't pool encoders that grew past the shrink threshold —
		// returning them would let outlier encodes pin large buffers
		// in the pool indefinitely.
		if pe.buf.Cap() > shrinkCap {
			return
		}
		pe.buf.Reset()
		// Re-Reset the encoder onto the freshly-cleared buffer so the
		// next Get sees a clean writer (msgpack's internal stream
		// state is also reset).
		pe.enc.Reset(pe.buf)
		encoderPool.Put(pe)
	}()
	if err := fn(pe.enc); err != nil {
		return nil, err
	}
	// Copy out: pe.buf is about to be reset and reused. The caller
	// retains the returned slice independently.
	out := make([]byte, pe.buf.Len())
	copy(out, pe.buf.Bytes())
	return out, nil
}
