// Package obs emits the machine-readable event stream the evaluation depends on.
//
// Measurement is wired in from the first commit on purpose. The expensive
// mistake is running a failure-injection campaign, then discovering the
// timestamps needed to decompose the latency were never emitted.
//
// The five intervals the paper reports are all differences between events in
// this stream:
//
//	detection      = failure_detected      - (external t0)
//	report         = health_report_received - failure_detected
//	control_plane  = slice_patched          - health_report_received
//	dns_convergence= (external t4)          - slice_patched
//	user_outage    = (external t5)          - (external t0)
package obs

import (
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"
)

// Recorder writes one JSON object per line. It is safe for concurrent use.
type Recorder struct {
	mu sync.Mutex
	w  io.Writer
	c  io.Closer
}

// NewRecorder opens path for append. An empty path discards events.
func NewRecorder(path string) (*Recorder, error) {
	if path == "" {
		return &Recorder{w: io.Discard}, nil
	}
	if path == "-" {
		return &Recorder{w: os.Stdout}, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Recorder{w: f, c: f}, nil
}

// Close releases the underlying file, if any.
func (r *Recorder) Close() error {
	if r == nil || r.c == nil {
		return nil
	}
	return r.c.Close()
}

// Emit writes one event. Fields are key/value pairs; an odd trailing key is
// dropped rather than panicking, because instrumentation must never take down
// the controller.
func (r *Recorder) Emit(event string, kv ...any) {
	if r == nil {
		return
	}
	rec := make(map[string]any, len(kv)/2+2)
	rec["ts"] = time.Now().UnixNano()
	rec["event"] = event
	for i := 0; i+1 < len(kv); i += 2 {
		k, ok := kv[i].(string)
		if !ok {
			continue
		}
		rec[k] = kv[i+1]
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, _ = r.w.Write(append(b, '\n'))
}
