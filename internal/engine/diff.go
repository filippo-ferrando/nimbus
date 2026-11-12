package engine

import (
	"sort"
	"strings"
)

// Diff is what must happen on the destination.
type Diff struct {
	MkDirs    []string // directories to create
	Send      []Entry  // files and symlinks to transfer
	MetaFiles []Entry  // regular files whose content matches but mode/mtime differ
	DirMeta   []Entry  // directories whose mode/mtime must be (re)applied
	Replace   []string // destination entries removed before transfer (type change)
	Delete    []string // destination entries not in source (--delete)
	Blocked   []string // type conflicts that need --delete to resolve
	Unchanged int
	// CheckSum lists files (same size, different or equal mtime) that need a
	// content comparison when --checksum is enabled.
	CheckSum []Entry
}

// DiffOptions tunes the comparison.
type DiffOptions struct {
	Checksum bool
	Delete   bool
	// Protected lists destination directories that contain excluded entries:
	// they are never removed as a whole.
	Protected map[string]bool
}

// ComputeDiff compares source and destination listings. Paths are relative to
// the respective roots and directly comparable.
func ComputeDiff(src, dst []Entry, o DiffOptions) *Diff {
	d := &Diff{}
	dmap := make(map[string]*Entry, len(dst))
	for i := range dst {
		if isPart(dst[i].Path) {
			continue
		}
		dmap[dst[i].Path] = &dst[i]
	}
	smap := make(map[string]byte, len(src))
	touched := map[string]bool{} // dirs whose contents change

	markParent := func(p string) {
		dir, _ := splitPath(p)
		touched[dir] = true
	}
	removed := map[string]bool{}
	underRemoved := func(p string) bool {
		for dir, _ := splitPath(p); ; dir, _ = splitPath(dir) {
			if removed[dir] {
				return true
			}
			if dir == "" {
				return false
			}
		}
	}

	for _, s := range src {
		smap[s.Path] = s.Type
		de := dmap[s.Path]
		if de != nil && underRemoved(s.Path) {
			de = nil
		}
		if de != nil && de.Type != s.Type {
			if de.Type == 'd' && (!o.Delete || o.Protected[s.Path]) {
				d.Blocked = append(d.Blocked, s.Path)
				continue
			}
			d.Replace = append(d.Replace, s.Path)
			if de.Type == 'd' {
				removed[s.Path] = true
			}
			markParent(s.Path)
			de = nil
		}
		switch s.Type {
		case 'd':
			if de == nil {
				d.MkDirs = append(d.MkDirs, s.Path)
				d.DirMeta = append(d.DirMeta, s)
				markParent(s.Path)
			} else if de.Mode != s.Mode || de.Mtime != s.Mtime {
				d.DirMeta = append(d.DirMeta, s)
			}
		case 'l':
			if de == nil || de.Target != s.Target {
				d.Send = append(d.Send, s)
				markParent(s.Path)
			} else {
				d.Unchanged++
			}
		case 'f':
			switch {
			case de == nil || de.Size != s.Size:
				d.Send = append(d.Send, s)
				markParent(s.Path)
			case o.Checksum:
				d.CheckSum = append(d.CheckSum, s)
				markParent(s.Path) // may be resent: re-apply dir mtime
			case de.Mtime != s.Mtime:
				d.Send = append(d.Send, s)
				markParent(s.Path)
			case de.Mode != s.Mode:
				d.MetaFiles = append(d.MetaFiles, s)
			default:
				d.Unchanged++
			}
		}
	}

	if o.Delete {
		for _, e := range dst {
			if _, ok := smap[e.Path]; isPart(e.Path) || ok {
				continue
			}
			// Only the top-most missing entry (rm -rf handles the rest),
			// except below directories that hold excluded entries: those are
			// kept and only their other children are removed.
			if o.Protected[e.Path] {
				continue
			}
			if dir, _ := splitPath(e.Path); !topMost(e.Path, smap) && !(o.Protected[dir] && smap[dir] == 0) {
				continue
			}
			d.Delete = append(d.Delete, e.Path)
			markParent(e.Path)
		}
	}

	// Directories whose content changed get their mtime re-applied.
	have := map[string]bool{}
	for _, e := range d.DirMeta {
		have[e.Path] = true
	}
	for _, s := range src {
		if s.Type == 'd' && touched[s.Path] && !have[s.Path] {
			d.DirMeta = append(d.DirMeta, s)
			have[s.Path] = true
		}
	}
	// Deepest first so setting a child's metadata never disturbs its parent.
	sort.Slice(d.DirMeta, func(i, j int) bool {
		ci, cj := strings.Count(d.DirMeta[i].Path, "/"), strings.Count(d.DirMeta[j].Path, "/")
		if d.DirMeta[i].Path == "" {
			ci = -1
		}
		if d.DirMeta[j].Path == "" {
			cj = -1
		}
		if ci != cj {
			return ci > cj
		}
		return d.DirMeta[i].Path < d.DirMeta[j].Path
	})
	return d
}

// topMost reports whether p's parent is a directory in the source (so p is
// the highest destination-only entry on its branch).
func topMost(p string, smap map[string]byte) bool {
	dir, _ := splitPath(p)
	return smap[dir] == 'd'
}

// ApplyChecksums moves checksum candidates into Send/MetaFiles/Unchanged.
// srcSum/dstSum are aligned with d.CheckSum.
func (d *Diff) ApplyChecksums(dst []Entry, srcSum, dstSum []string) {
	dmap := make(map[string]*Entry, len(dst))
	for i := range dst {
		dmap[dst[i].Path] = &dst[i]
	}
	for i, s := range d.CheckSum {
		de := dmap[s.Path]
		switch {
		case srcSum[i] != dstSum[i]:
			d.Send = append(d.Send, s)
		case de != nil && (de.Mode != s.Mode || de.Mtime != s.Mtime):
			d.MetaFiles = append(d.MetaFiles, s)
		default:
			d.Unchanged++
		}
	}
	d.CheckSum = nil
	sort.Slice(d.Send, func(i, j int) bool { return d.Send[i].Path < d.Send[j].Path })
}

// Bytes returns the total payload size to send.
func (d *Diff) Bytes() int64 {
	var n int64
	for _, e := range d.Send {
		n += e.Size
	}
	return n
}
