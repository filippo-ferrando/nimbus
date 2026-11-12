package engine

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Entry is one filesystem object, Path relative to the endpoint root using
// "/" separators ("" is the root itself).
type Entry struct {
	Path   string
	Type   byte // 'f' file, 'd' dir, 'l' symlink
	Size   int64
	Mtime  int64 // unix seconds
	Mode   uint32
	Target string
}

// ErrNotFound means the listed location does not exist.
var ErrNotFound = errors.New("not found")

const partSuffix = ".nimbus-part"

func partName(p string) string {
	dir, base := splitPath(p)
	if dir == "" {
		return "." + base + partSuffix
	}
	return dir + "/." + base + partSuffix
}

func splitPath(p string) (dir, base string) {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[:i], p[i+1:]
	}
	return "", p
}

func isPart(p string) bool {
	_, b := splitPath(p)
	return strings.HasPrefix(b, ".") && strings.HasSuffix(b, partSuffix)
}

func cleanRel(p string) string {
	p = strings.TrimPrefix(p, "./")
	if p == "." {
		return ""
	}
	return p
}

// ParseListing decodes the agent "list" output. Unsupported entry types are
// returned in skipped.
func ParseListing(out []byte) (entries []Entry, skipped []string, err error) {
	nl := bytes.IndexByte(out, '\n')
	if nl < 0 {
		if len(bytes.TrimSpace(out)) == 0 {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("malformed listing")
	}
	header := string(out[:nl])
	body := out[nl+1:]
	switch header {
	case "#gfind":
		f := bytes.Split(body, []byte{0})
		for i := 0; i+2 < len(f); i += 3 {
			meta := strings.Fields(string(f[i]))
			if len(meta) != 4 {
				return nil, nil, fmt.Errorf("malformed listing record %q", f[i])
			}
			p := cleanRel(string(f[i+1]))
			e := Entry{Path: p, Target: string(f[i+2])}
			e.Size, _ = strconv.ParseInt(meta[1], 10, 64)
			t, _ := strconv.ParseFloat(meta[2], 64)
			e.Mtime = int64(math.Floor(t))
			m, _ := strconv.ParseUint(meta[3], 8, 32)
			e.Mode = uint32(m)
			switch meta[0] {
			case "f", "d", "l":
				e.Type = meta[0][0]
			default:
				skipped = append(skipped, p)
				continue
			}
			if e.Type != 'l' {
				e.Target = ""
			}
			entries = append(entries, e)
		}
	case "#stat-gnu", "#stat-bsd":
		base := 16
		if header == "#stat-bsd" {
			base = 8
		}
		var links []byte
		if i := bytes.Index(body, []byte("#links\n")); i >= 0 && (i == 0 || body[i-1] == '\n') {
			links = body[i+len("#links\n"):]
			body = body[:i]
		}
		targets := map[string]string{}
		lf := bytes.Split(links, []byte{0})
		for i := 0; i+1 < len(lf); i += 2 {
			targets[cleanRel(string(lf[i]))] = string(lf[i+1])
		}
		for _, line := range strings.Split(strings.TrimSuffix(string(body), "\n"), "\n") {
			if line == "" {
				continue
			}
			parts := strings.SplitN(line, " ", 4)
			if len(parts) != 4 {
				return nil, nil, fmt.Errorf("unsupported file name (newline?) near %q", line)
			}
			raw, err1 := strconv.ParseUint(parts[0], base, 32)
			size, err2 := strconv.ParseInt(parts[1], 10, 64)
			mt, err3 := strconv.ParseInt(parts[2], 10, 64)
			if err1 != nil || err2 != nil || err3 != nil {
				return nil, nil, fmt.Errorf("unsupported file name (newline?) near %q", line)
			}
			p := cleanRel(parts[3])
			e := Entry{Path: p, Size: size, Mtime: mt, Mode: uint32(raw) & 0o7777}
			switch raw & 0o170000 {
			case 0o100000:
				e.Type = 'f'
			case 0o040000:
				e.Type = 'd'
			case 0o120000:
				e.Type = 'l'
				e.Target = targets[p]
			default:
				skipped = append(skipped, p)
				continue
			}
			entries = append(entries, e)
		}
	default:
		return nil, nil, fmt.Errorf("unknown listing format %q", header)
	}
	finishEntries(entries)
	return entries, skipped, nil
}

func finishEntries(es []Entry) {
	for i := range es {
		if es[i].Type != 'f' {
			es[i].Size = 0
		}
	}
	sort.Slice(es, func(i, j int) bool { return es[i].Path < es[j].Path })
}

// ListLocal walks root/sub natively (faster than the agent and NUL-safe on
// every OS).
func ListLocal(root, sub string) (entries []Entry, skipped []string, err error) {
	root = ExpandTilde(root)
	start := root
	if sub != "" {
		start = filepath.Join(root, sub)
	}
	if _, err := os.Lstat(start); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}
	if st, err := os.Stat(root); err != nil || !st.IsDir() {
		return nil, nil, ErrNotFound
	}
	walkErr := filepath.WalkDir(start, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if rel == "." {
			rel = ""
		}
		info, err := os.Lstat(p)
		if err != nil {
			return err
		}
		e := Entry{Path: rel, Mtime: info.ModTime().Unix(), Mode: unixMode(info.Mode())}
		switch {
		case info.Mode().IsRegular():
			e.Type, e.Size = 'f', info.Size()
		case info.IsDir():
			e.Type = 'd'
		case info.Mode()&fs.ModeSymlink != 0:
			e.Type = 'l'
			e.Target, _ = os.Readlink(p)
		default:
			skipped = append(skipped, rel)
			return nil
		}
		entries = append(entries, e)
		return nil
	})
	if walkErr != nil {
		return nil, nil, walkErr
	}
	finishEntries(entries)
	return entries, skipped, nil
}

func unixMode(m fs.FileMode) uint32 {
	u := uint32(m.Perm())
	if m&fs.ModeSetuid != 0 {
		u |= 0o4000
	}
	if m&fs.ModeSetgid != 0 {
		u |= 0o2000
	}
	if m&fs.ModeSticky != 0 {
		u |= 0o1000
	}
	return u
}

func ExpandTilde(p string) string {
	if p == "~" || strings.HasPrefix(p, "~/") {
		if h, err := os.UserHomeDir(); err == nil {
			return h + p[1:]
		}
	}
	return p
}
