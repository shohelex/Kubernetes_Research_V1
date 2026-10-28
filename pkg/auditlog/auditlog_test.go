package auditlog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func writeLog(t *testing.T, mode Mode, n, writers int, anchorEvery uint64) (string, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "decisions.jsonl")
	w, err := Open(path, mode, anchorEvery)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := g; i < n; i += writers {
				pod := fmt.Sprintf("pod-%d", i)
				node := fmt.Sprintf("node-%d", i%7)
				if err := w.Append(Entry{Kind: "decision", Instance: g, Namespace: "ns", Pod: pod, Residency: true,
					Permitted: []string{"Europe"}, Path: "fresh", Node: node,
					NodeLabels: &Labels{Region: "Europe", Isolation: "dedicated", EncryptedStorage: true}}, true); err != nil {
					t.Error(err)
				}
				if err := w.Append(Entry{Kind: "outcome", Instance: g, Namespace: "ns", Pod: pod, Node: node, Outcome: "bound"}, false); err != nil {
					t.Error(err)
				}
			}
		}(g)
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got, _ := w.Stats(); got != uint64(2*n) {
		t.Fatalf("%d entries written, want %d", got, 2*n)
	}
	return path, path + ".anchor"
}

func verifyFiles(t *testing.T, logPath, anchorPath string, check func(Entry) []string) (Report, []Entry) {
	t.Helper()
	l, _ := os.Open(logPath)
	defer l.Close()
	a, _ := os.Open(anchorPath)
	defer a.Close()
	return Verify(l, a, check)
}

func TestChainVerifies(t *testing.T) {
	for _, mode := range []Mode{Sync, Async} {
		logPath, anchorPath := writeLog(t, mode, 500, 10, 64)
		rep, entries := verifyFiles(t, logPath, anchorPath, nil)
		if !rep.ChainOK || !rep.AnchorOK || rep.Entries != 1000 || rep.Decisions != 500 || rep.Outcomes != 500 || len(entries) != 1000 {
			t.Fatalf("%s: %+v", mode, rep)
		}
	}
}

func TestTamperingIsDetected(t *testing.T) {
	logPath, anchorPath := writeLog(t, Sync, 25, 1, 10)
	orig, _ := os.ReadFile(logPath)
	lines := strings.Split(strings.TrimRight(string(orig), "\n"), "\n")
	rehash := func(line string, edit func(*Entry)) string {
		var e Entry
		json.Unmarshal([]byte(line), &e)
		edit(&e)
		e.Hash, _ = hashOf(e)
		b, _ := json.Marshal(e)
		return string(b)
	}
	cases := []struct {
		name  string
		lines func() []string
		chain bool
	}{
		{"edited record", func() []string {
			l := append([]string{}, lines...)
			l[20] = strings.Replace(l[20], `"node-`, `"forged-node-`, 1)
			return l
		}, true},
		{"edited and re-hashed record", func() []string {
			l := append([]string{}, lines...)
			l[20] = rehash(l[20], func(e *Entry) { e.Node = "forged-node" })
			return l
		}, true},
		{"deleted record", func() []string { return append(append([]string{}, lines[:20]...), lines[21:]...) }, true},
		{"inserted record", func() []string {
			l := append([]string{}, lines[:20]...)
			l = append(l, lines[19])
			return append(l, lines[20:]...)
		}, true},
		{"reordered records", func() []string {
			l := append([]string{}, lines...)
			l[20], l[21] = l[21], l[20]
			return l
		}, true},
		{"truncated log", func() []string { return lines[:44] }, false},
	}
	for _, c := range cases {
		tampered := filepath.Join(t.TempDir(), "tampered.jsonl")
		os.WriteFile(tampered, []byte(strings.Join(c.lines(), "\n")+"\n"), 0o644)
		rep, _ := verifyFiles(t, tampered, anchorPath, nil)
		if rep.OK() {
			t.Fatalf("%s: not detected: %+v", c.name, rep)
		}
		if c.chain == rep.ChainOK {
			t.Fatalf("%s: chain_ok=%v, want %v (%+v)", c.name, rep.ChainOK, !c.chain, rep)
		}
	}
}

func TestBadDecisionReported(t *testing.T) {
	logPath, anchorPath := writeLog(t, Sync, 10, 1, 100)
	check := func(e Entry) []string {
		if e.Pod == "pod-3" {
			return []string{"residency"}
		}
		return nil
	}
	rep, _ := verifyFiles(t, logPath, anchorPath, check)
	if len(rep.BadDecisions) != 1 || !strings.Contains(rep.BadDecisions[0], "pod-3") || rep.OK() {
		t.Fatalf("%+v", rep)
	}
}

func TestCrossCheck(t *testing.T) {
	logPath, anchorPath := writeLog(t, Sync, 4, 1, 100)
	rep, entries := verifyFiles(t, logPath, anchorPath, nil)
	placements := map[string]string{
		"ns/pod-0": "node-0",
		"ns/pod-1": "node-6",
		"ns/pod-2": "node-2",
		"ns/pod-9": "node-2",
	}
	CrossCheck(&rep, entries, placements)
	if len(rep.Unrecorded) != 1 || len(rep.Unplaced) != 1 || len(rep.NodeMismatch) != 1 || rep.OK() {
		t.Fatalf("%+v", rep)
	}
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(rep)
	for _, want := range []string{"pod-9", "pod-3", "pod-1"} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("report lacks %s: %s", want, buf.String())
		}
	}
}

func TestSyncLogTrimmedAtClose(t *testing.T) {
	logPath, _ := writeLog(t, Sync, 300, 4, 64)
	b, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.IndexByte(b, 0) >= 0 {
		t.Fatal("a closed log must hold no zero byte")
	}
	if n := bytes.Count(b, []byte("\n")); n != 600 {
		t.Fatalf("%d lines, want 600", n)
	}
}

func TestVerifyPreallocated(t *testing.T) {
	logPath, anchorPath := writeLog(t, Sync, 100, 2, 50)
	b, _ := os.ReadFile(logPath)
	padded := append(append([]byte{}, b...), make([]byte, 3<<20)...)
	a, _ := os.ReadFile(anchorPath)
	rep, entries := Verify(bytes.NewReader(padded), bytes.NewReader(a), nil)
	if !rep.ChainOK || !rep.AnchorOK || len(entries) != 200 {
		t.Fatalf("padded log: %+v, %d entries", rep, len(entries))
	}
}

func TestAsyncLogCompleteAtClose(t *testing.T) {
	logPath, anchorPath := writeLog(t, Async, 1000, 10, 300)
	rep, entries := verifyFiles(t, logPath, anchorPath, nil)
	if !rep.ChainOK || !rep.AnchorOK || len(entries) != 2000 {
		t.Fatalf("%+v, %d entries", rep, len(entries))
	}
	a, _ := os.ReadFile(anchorPath)
	lines := strings.Split(strings.TrimSpace(string(a)), "\n")
	if last := lines[len(lines)-1]; !strings.HasPrefix(last, "2000 ") {
		t.Fatalf("final anchor %q must name entry 2000", last)
	}
}
