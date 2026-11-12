package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

const (
	tarHeader       = 512
	maxBatchEntries = 20000
)

// Batch is an independent, self-contained unit of transfer: either a tar
// archive of several files/symlinks or one byte range of a large file.
type Batch struct {
	Key     string   `json:"key"`
	Kind    string   `json:"kind"` // "tar" | "part"
	Paths   []string `json:"paths,omitempty"`
	Path    string   `json:"path,omitempty"` // part: file
	Index   int      `json:"index,omitempty"`
	Parts   int      `json:"parts,omitempty"`
	Offset  int64    `json:"offset,omitempty"` // in MiB blocks
	Count   int64    `json:"count,omitempty"`  // in MiB blocks
	RawSize int64    `json:"raw"`
	Files   int      `json:"files"`
	Seq     int      `json:"-"`
}

// Label is a short human description.
func (b *Batch) Label() string {
	if b.Kind == "part" {
		return fmt.Sprintf("%s [%d/%d]", b.Path, b.Index+1, b.Parts)
	}
	if len(b.Paths) == 1 {
		return b.Paths[0]
	}
	return fmt.Sprintf("%s (+%d)", b.Paths[0], len(b.Paths)-1)
}

func entryKey(h interface{ Write([]byte) (int, error) }, e Entry) {
	fmt.Fprintf(h, "%c\x00%s\x00%d\x00%d\x00%o\x00%s\x00", e.Type, e.Path, e.Size, e.Mtime, e.Mode, e.Target)
}

func sumKey(h interface{ Sum([]byte) []byte }) string { return hex.EncodeToString(h.Sum(nil)[:10]) }

// PlanOptions controls batching.
type PlanOptions struct {
	Block    int64 // bytes, multiple of 1 MiB
	Comp     string
	Level    int
	Previous []*Batch // plan from an interrupted run, reused when still valid
}

// MakePlan groups entries into batches. Files larger than Block are split
// into Block-sized parts. Tar batches from a previous run are kept verbatim
// when all their entries still need sending with identical metadata, so
// already staged data is reused after an interruption.
func MakePlan(send []Entry, o PlanOptions) []*Batch {
	byPath := make(map[string]Entry, len(send))
	for _, e := range send {
		byPath[e.Path] = e
	}
	tag := o.Comp + "/" + strconv.Itoa(o.Level)
	var out []*Batch
	used := map[string]bool{}

	for _, pb := range o.Previous {
		if pb.Kind != "tar" {
			continue
		}
		ok := len(pb.Paths) > 0
		var es []Entry
		for _, p := range pb.Paths {
			e, found := byPath[p]
			if !found || used[p] || (e.Type == 'f' && e.Size > o.Block) {
				ok = false
				break
			}
			es = append(es, e)
		}
		if !ok {
			continue
		}
		b := tarBatch(es, tag)
		if b.Key != pb.Key {
			continue
		}
		for _, p := range pb.Paths {
			used[p] = true
		}
		out = append(out, b)
	}

	var cur []Entry
	var curSize int64
	flush := func() {
		if len(cur) > 0 {
			out = append(out, tarBatch(cur, tag))
			cur, curSize = nil, 0
		}
	}
	for _, e := range send {
		if used[e.Path] {
			continue
		}
		if e.Type == 'f' && e.Size > o.Block {
			out = append(out, partBatches(e, o.Block, tag)...)
			continue
		}
		sz := tarHeader + (e.Size+511)/512*512
		if len(cur) > 0 && (curSize+sz > o.Block || len(cur) >= maxBatchEntries) {
			flush()
		}
		cur = append(cur, e)
		curSize += sz
	}
	flush()
	for i, b := range out {
		b.Seq = i + 1
	}
	return out
}

func tarBatch(es []Entry, tag string) *Batch {
	h := sha256.New()
	fmt.Fprintf(h, "tar\x00%s\x00", tag)
	b := &Batch{Kind: "tar"}
	for _, e := range es {
		entryKey(h, e)
		b.Paths = append(b.Paths, e.Path)
		b.RawSize += e.Size
		b.Files++
	}
	b.Key = "t" + sumKey(h)
	return b
}

func partBatches(e Entry, block int64, tag string) []*Batch {
	const mib = 1 << 20
	n := int((e.Size + block - 1) / block)
	per := block / mib
	var out []*Batch
	for i := 0; i < n; i++ {
		h := sha256.New()
		fmt.Fprintf(h, "part\x00%s\x00%d\x00%d\x00", tag, i, block)
		entryKey(h, e)
		raw := block
		if rem := e.Size - int64(i)*block; rem < raw {
			raw = rem
		}
		out = append(out, &Batch{
			Key: "p" + sumKey(h), Kind: "part", Path: e.Path, Index: i, Parts: n,
			Offset: int64(i) * per, Count: per, RawSize: raw, Files: boolInt(i == n-1),
		})
	}
	return out
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// SavePlan / LoadPlan persist batches between runs.
func SavePlan(path string, bs []*Batch) error {
	data, err := json.Marshal(bs)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func LoadPlan(path string) []*Batch {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var bs []*Batch
	if json.Unmarshal(data, &bs) != nil {
		return nil
	}
	return bs
}
