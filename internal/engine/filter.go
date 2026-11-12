package engine

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// Filter implements rsync-style --exclude patterns:
//   - "name"      matches a file or directory name at any depth
//   - "dir/"      matches directories only
//   - "/top"      is anchored at the transfer root
//   - "a/b"       (contains a slash) matches the end of the path
//   - "*" and "?" do not cross "/", "**" does; "[...]" is a character class
//
// An excluded directory excludes everything below it. Excluded destination
// entries are never deleted by --delete.
type Filter struct{ rules []rule }

type rule struct {
	re       *regexp.Regexp
	dirOnly  bool
	anchored bool
	hasSlash bool
	pattern  string
}

// NewFilter compiles patterns.
func NewFilter(patterns []string) (*Filter, error) {
	f := &Filter{}
	for _, p := range patterns {
		if p == "" {
			continue
		}
		r := rule{pattern: p}
		if strings.HasSuffix(p, "/") {
			r.dirOnly = true
			p = strings.TrimRight(p, "/")
		}
		if strings.HasPrefix(p, "/") {
			r.anchored = true
			p = strings.TrimLeft(p, "/")
		}
		r.hasSlash = strings.Contains(p, "/")
		re, err := globRegexp(p)
		if err != nil {
			return nil, fmt.Errorf("exclude %q: %w", r.pattern, err)
		}
		r.re = re
		f.rules = append(f.rules, r)
	}
	return f, nil
}

// ReadPatterns reads one pattern per line; blank lines and lines starting
// with '#' or ';' are ignored.
func ReadPatterns(path string) ([]string, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	var out []string
	sc := bufio.NewScanner(fh)
	for sc.Scan() {
		l := strings.TrimRight(sc.Text(), "\r")
		if l == "" || l[0] == '#' || l[0] == ';' {
			continue
		}
		out = append(out, l)
	}
	return out, sc.Err()
}

func globRegexp(g string) (*regexp.Regexp, error) {
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(g); i++ {
		c := g[i]
		switch c {
		case '*':
			if i+1 < len(g) && g[i+1] == '*' {
				b.WriteString(".*")
				i++
			} else {
				b.WriteString("[^/]*")
			}
		case '?':
			b.WriteString("[^/]")
		case '[':
			j := strings.IndexByte(g[i+1:], ']')
			if j < 0 {
				b.WriteString(`\[`)
				continue
			}
			class := g[i+1 : i+1+j]
			if strings.HasPrefix(class, "!") {
				class = "^" + class[1:]
			}
			b.WriteString("[" + strings.ReplaceAll(class, `\`, `\\`) + "]")
			i += j + 1
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	b.WriteString("$")
	return regexp.Compile(b.String())
}

// Empty reports whether the filter has no rules.
func (f *Filter) Empty() bool { return f == nil || len(f.rules) == 0 }

// Excluded reports whether rel (relative to the transfer root) is excluded.
func (f *Filter) Excluded(rel string, isDir bool) bool {
	if f.Empty() || rel == "" {
		return false
	}
	_, base := splitPath(rel)
	for _, r := range f.rules {
		if r.dirOnly && !isDir {
			continue
		}
		switch {
		case r.anchored:
			if r.re.MatchString(rel) {
				return true
			}
		case r.hasSlash:
			for p := rel; ; {
				if r.re.MatchString(p) {
					return true
				}
				i := strings.IndexByte(p, '/')
				if i < 0 {
					break
				}
				p = p[i+1:]
			}
		default:
			if r.re.MatchString(base) {
				return true
			}
		}
	}
	return false
}

// Apply drops excluded entries (and everything below excluded directories).
// sub is the transfer's top-level entry: patterns are relative to it.
func (f *Filter) Apply(es []Entry, sub string) []Entry {
	if f.Empty() {
		return es
	}
	prefix := ""
	if sub != "" {
		prefix = sub + "/"
	}
	pruned := map[string]bool{}
	out := es[:0]
	for _, e := range es {
		if under(e.Path, pruned) {
			continue
		}
		rel := e.Path
		if prefix != "" {
			if e.Path == sub {
				out = append(out, e)
				continue
			}
			rel = strings.TrimPrefix(e.Path, prefix)
		}
		if f.Excluded(rel, e.Type == 'd') {
			if e.Type == 'd' {
				pruned[e.Path] = true
			}
			continue
		}
		out = append(out, e)
	}
	return out
}

func under(p string, pruned map[string]bool) bool {
	if len(pruned) == 0 {
		return false
	}
	for dir, _ := splitPath(p); dir != ""; dir, _ = splitPath(dir) {
		if pruned[dir] {
			return true
		}
	}
	return false
}
