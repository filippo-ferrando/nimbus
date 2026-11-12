package engine

import (
	"fmt"
	"path"
	"strconv"
	"strings"
)

// Spec is a parsed command-line location: [user@]host:path or a local path.
type Spec struct {
	Raw    string
	User   string
	Host   string
	Path   string
	Remote bool
}

// ParseSpec follows rsync's rule: a colon before the first slash marks a
// remote location. "[v6addr]:path" is supported. Use "./a:b" for local files
// containing colons.
func ParseSpec(s string) (Spec, error) {
	sp := Spec{Raw: s, Path: s}
	if s == "" {
		return sp, fmt.Errorf("empty location")
	}
	colon := -1
	if strings.HasPrefix(s, "[") || strings.Contains(s, "@[") {
		end := strings.Index(s, "]:")
		if end > 0 {
			colon = end + 1
		}
	} else {
		slash := strings.IndexByte(s, '/')
		c := strings.IndexByte(s, ':')
		if c > 0 && (slash < 0 || c < slash) {
			colon = c
		}
	}
	if colon < 0 {
		return sp, nil
	}
	host := s[:colon]
	sp.Path = s[colon+1:]
	if at := strings.LastIndexByte(host, '@'); at >= 0 {
		sp.User = host[:at]
		host = host[at+1:]
	}
	host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	if host == "" {
		return sp, fmt.Errorf("%q: missing host", s)
	}
	sp.Host, sp.Remote = host, true
	if sp.Path == "" {
		sp.Path = "~/"
	}
	return sp, nil
}

// Dest returns "[user@]host" for ssh.
func (s Spec) Dest() string {
	h := s.Host
	if strings.Contains(h, ":") {
		h = "[" + h + "]"
	}
	if s.User != "" {
		return s.User + "@" + h
	}
	return h
}

func (s Spec) String() string {
	if !s.Remote {
		return s.Path
	}
	return s.Dest() + ":" + s.Path
}

// SplitSource applies rsync trailing-slash semantics: "dir" transfers the
// directory itself (root=parent, sub=dir), "dir/" transfers its contents
// (root=dir, sub="").
func SplitSource(p string) (root, sub string) {
	switch {
	case p == "~" || p == "." || p == ".." || strings.HasSuffix(p, "/") ||
		strings.HasSuffix(p, "/.") || strings.HasSuffix(p, "/.."):
		return p, ""
	}
	return path.Dir(p), path.Base(p)
}

// ParseSize parses "64M", "1G", "512k", "1048576".
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	s = strings.TrimSuffix(strings.TrimSuffix(s, "IB"), "B")
	mult := int64(1)
	if n := len(s); n > 0 {
		switch s[n-1] {
		case 'K':
			mult, s = 1<<10, s[:n-1]
		case 'M':
			mult, s = 1<<20, s[:n-1]
		case 'G':
			mult, s = 1<<30, s[:n-1]
		case 'T':
			mult, s = 1<<40, s[:n-1]
		}
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return int64(v * float64(mult)), nil
}
