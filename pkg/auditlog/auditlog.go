package auditlog

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type Labels struct {
	Region           string `json:"region"`
	Isolation        string `json:"isolation"`
	EncryptedStorage bool   `json:"encrypted_storage"`
	EncryptedTraffic bool   `json:"encrypted_traffic"`
}

type Entry struct {
	Seq      uint64 `json:"seq"`
	Time     string `json:"time"`
	Kind     string `json:"kind"`
	Instance int    `json:"instance"`

	Namespace  string `json:"namespace"`
	Pod        string `json:"pod"`
	Residency  bool   `json:"residency"`
	Isolation  bool   `json:"isolation"`
	Encryption bool   `json:"encryption"`

	Permitted    []string       `json:"permitted_regions,omitempty"`
	Path         string         `json:"path,omitempty"`
	RankingID    uint64         `json:"ranking_id,omitempty"`
	RankedFor    string         `json:"ranked_for,omitempty"`
	Excluded     map[string]int `json:"excluded,omitempty"`
	Node         string         `json:"node,omitempty"`
	NodeLabels   *Labels        `json:"node_labels,omitempty"`
	RecheckDrops int            `json:"recheck_drops,omitempty"`

	Outcome string `json:"outcome,omitempty"`

	Prev string `json:"prev"`
	Hash string `json:"hash"`
}

func hashOf(e Entry) (string, error) {
	e.Hash = ""
	b, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte(e.Prev))
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil)), nil
}

type Mode string

const (
	Async Mode = "async"

	Sync Mode = "sync"
)

const maxBatch = 256

type request struct {
	e    Entry
	done chan error
}

type Writer struct {
	mode        Mode
	ch          chan request
	f           *os.File
	bw          *bufio.Writer
	anchor      *os.File
	anchorEvery uint64
	seq         uint64
	prev        string
	wg          sync.WaitGroup
	mu          sync.Mutex
	err         error
	bytes       int64
	closeOnce   sync.Once

	allocated  int64
	noPrealloc bool
}

const preallocChunk = 16 << 20

func Open(path string, mode Mode, anchorEvery uint64) (*Writer, error) {
	if mode != Async && mode != Sync {
		return nil, fmt.Errorf("auditlog: unknown mode %q", mode)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	a, err := os.OpenFile(path+".anchor", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		f.Close()
		return nil, err
	}
	return NewWriter(f, a, mode, anchorEvery)
}

func NewWriter(f, anchor *os.File, mode Mode, anchorEvery uint64) (*Writer, error) {
	if mode != Async && mode != Sync {
		return nil, fmt.Errorf("auditlog: unknown mode %q", mode)
	}
	if anchorEvery == 0 {
		anchorEvery = 1000
	}
	w := &Writer{mode: mode, ch: make(chan request, 4*maxBatch), f: f, bw: bufio.NewWriterSize(f, 1<<20),
		anchor: anchor, anchorEvery: anchorEvery}
	w.wg.Add(1)
	go w.loop()
	return w, nil
}

func (w *Writer) Append(e Entry, wait bool) error {
	r := request{e: e}
	if wait && w.mode == Sync {
		r.done = make(chan error, 1)
	}
	w.ch <- r
	if r.done != nil {
		return <-r.done
	}
	return nil
}

func (w *Writer) Mode() Mode { return w.mode }

func (w *Writer) loop() {
	defer w.wg.Done()
	for first := range w.ch {
		batch := []request{first}
	drain:
		for len(batch) < maxBatch {
			select {
			case r, ok := <-w.ch:
				if !ok {
					break drain
				}
				batch = append(batch, r)
			default:
				break drain
			}
		}
		err := w.commit(batch)
		for _, r := range batch {
			if r.done != nil {
				r.done <- err
			}
		}
	}
}

func (w *Writer) commit(batch []request) error {
	var err error
	if w.mode == Sync && !w.noPrealloc && w.bytes+2*int64(w.bw.Size()) > w.allocated {
		if perr := preallocate(w.f, w.allocated, preallocChunk); perr != nil {
			w.noPrealloc = true
		} else {
			w.allocated += preallocChunk
		}
	}
	for _, r := range batch {
		w.seq++
		e := r.e
		e.Seq, e.Prev = w.seq, w.prev
		if e.Time == "" {
			e.Time = time.Now().UTC().Format(time.RFC3339Nano)
		}
		if e.Hash, err = hashOf(e); err != nil {
			break
		}
		b, _ := json.Marshal(e)
		b = append(b, '\n')
		if _, err = w.bw.Write(b); err != nil {
			break
		}
		w.bytes += int64(len(b))
		w.prev = e.Hash
		if w.seq%w.anchorEvery == 0 {
			if err = w.flush(); err != nil {
				break
			}
			if err = w.writeAnchor(); err != nil {
				break
			}
		}
	}
	if err == nil {
		if w.mode == Sync {
			err = w.flush()
		} else {
			err = w.bw.Flush()
		}
	}
	if err != nil {
		w.mu.Lock()
		if w.err == nil {
			w.err = err
		}
		w.mu.Unlock()
	}
	return err
}

func (w *Writer) flush() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return datasync(w.f)
}

func (w *Writer) writeAnchor() error {
	if _, err := fmt.Fprintf(w.anchor, "%d %s\n", w.seq, w.prev); err != nil {
		return err
	}
	return w.anchor.Sync()
}

func (w *Writer) Close() error {
	w.closeOnce.Do(func() {
		close(w.ch)
		w.wg.Wait()
		keep := func(err error) {
			if err != nil && w.err == nil {
				w.err = err
			}
		}

		keep(w.flush())
		if w.seq%w.anchorEvery != 0 && w.seq > 0 {
			keep(w.writeAnchor())
		}

		if w.allocated > 0 {
			keep(w.f.Truncate(w.bytes))
			keep(w.f.Sync())
		}
		w.f.Close()
		w.anchor.Close()
	})
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *Writer) Stats() (entries uint64, bytes int64) {
	return w.seq, w.bytes
}
