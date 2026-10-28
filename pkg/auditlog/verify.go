package auditlog

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

type Report struct {
	Entries   int `json:"entries"`
	Decisions int `json:"decisions"`
	Outcomes  int `json:"outcomes"`

	ChainOK     bool   `json:"chain_ok"`
	BreakSeq    uint64 `json:"break_seq,omitempty"`
	BreakReason string `json:"break_reason,omitempty"`

	AnchorOK     bool   `json:"anchor_ok"`
	AnchorReason string `json:"anchor_reason,omitempty"`

	BadDecisions []string `json:"bad_decisions,omitempty"`

	Unrecorded   []string `json:"unrecorded_placements,omitempty"`
	Unplaced     []string `json:"unplaced_records,omitempty"`
	NodeMismatch []string `json:"node_mismatch,omitempty"`
}

func (r Report) OK() bool {
	return r.ChainOK && r.AnchorOK && len(r.BadDecisions) == 0 &&
		len(r.Unrecorded) == 0 && len(r.Unplaced) == 0 && len(r.NodeMismatch) == 0
}

func Verify(log io.Reader, anchor io.Reader, check func(Entry) []string) (Report, []Entry) {
	rep := Report{ChainOK: true, AnchorOK: true}
	var entries []Entry
	hashAt := map[uint64]string{}
	sc := bufio.NewScanner(&untilZero{r: log})
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	prev, line := "", 0
	for sc.Scan() {
		line++
		var e Entry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			rep.ChainOK, rep.BreakSeq, rep.BreakReason = false, uint64(line), fmt.Sprintf("line %d does not parse: %v", line, err)
			break
		}
		want, _ := hashOf(e)
		switch {
		case e.Seq != uint64(line):
			rep.ChainOK, rep.BreakSeq, rep.BreakReason = false, e.Seq, fmt.Sprintf("line %d has sequence number %d", line, e.Seq)
		case e.Prev != prev:
			rep.ChainOK, rep.BreakSeq, rep.BreakReason = false, e.Seq, "link to the previous record does not match"
		case e.Hash != want:
			rep.ChainOK, rep.BreakSeq, rep.BreakReason = false, e.Seq, "record does not match its hash"
		}
		if !rep.ChainOK {
			break
		}
		prev = e.Hash
		hashAt[e.Seq] = e.Hash
		entries = append(entries, e)
		rep.Entries++
		switch e.Kind {
		case "decision":
			rep.Decisions++
			if check != nil {
				if fails := check(e); len(fails) > 0 {
					rep.BadDecisions = append(rep.BadDecisions, fmt.Sprintf("seq %d %s/%s on %s: %s", e.Seq, e.Namespace, e.Pod, e.Node, strings.Join(fails, ",")))
				}
			}
		case "outcome":
			rep.Outcomes++
		}
	}
	if anchor != nil {
		as := bufio.NewScanner(anchor)
		var last uint64
		for as.Scan() {
			f := strings.Fields(as.Text())
			if len(f) != 2 {
				continue
			}
			seq, err := strconv.ParseUint(f[0], 10, 64)
			if err != nil {
				continue
			}
			last = seq
			if h, ok := hashAt[seq]; ok && h != f[1] {
				rep.AnchorOK, rep.AnchorReason = false, fmt.Sprintf("anchored record %d has another hash in the log", seq)
				break
			}
		}
		if rep.AnchorOK && rep.ChainOK && last > uint64(rep.Entries) {
			rep.AnchorOK, rep.AnchorReason = false, fmt.Sprintf("log ends at record %d but record %d was anchored (truncated)", rep.Entries, last)
		}
	}
	return rep, entries
}

func CrossCheck(rep *Report, entries []Entry, placements map[string]string) {
	decided := map[string]string{}
	lastDecision := map[string]string{}
	for _, e := range entries {
		key := e.Namespace + "/" + e.Pod
		switch e.Kind {
		case "decision":
			lastDecision[key] = e.Node
		case "outcome":
			if e.Outcome == "bound" || e.Outcome == "changed_during_bind" {
				decided[key] = lastDecision[key]
			}
		}
	}
	for pod, node := range placements {
		rec, ok := decided[pod]
		switch {
		case !ok:
			rep.Unrecorded = append(rep.Unrecorded, pod+" on "+node)
		case rec != node:
			rep.NodeMismatch = append(rep.NodeMismatch, fmt.Sprintf("%s recorded on %s, bound on %s", pod, rec, node))
		}
	}
	for pod, node := range decided {
		if _, ok := placements[pod]; !ok {
			rep.Unplaced = append(rep.Unplaced, pod+" recorded on "+node)
		}
	}
	sort.Strings(rep.Unrecorded)
	sort.Strings(rep.Unplaced)
	sort.Strings(rep.NodeMismatch)
}

type untilZero struct {
	r    io.Reader
	done bool
}

func (u *untilZero) Read(p []byte) (int, error) {
	if u.done {
		return 0, io.EOF
	}
	n, err := u.r.Read(p)
	if i := bytes.IndexByte(p[:n], 0); i >= 0 {
		u.done = true
		return i, nil
	}
	return n, err
}
