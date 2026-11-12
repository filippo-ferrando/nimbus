package engine

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"git.fferrando.cc/rdfilippo/nimbus/internal/agent"
	"git.fferrando.cc/rdfilippo/nimbus/internal/transport"
)

func TestParseSpec(t *testing.T) {
	cases := []struct {
		in               string
		user, host, path string
		remote           bool
	}{
		{"/a/b", "", "", "/a/b", false},
		{"a/b:c", "", "", "a/b:c", false},
		{"./x:y", "", "", "./x:y", false},
		{"host:/data", "", "host", "/data", true},
		{"me@host:data/x", "me", "host", "data/x", true},
		{"host:", "", "host", "~/", true},
		{"[::1]:/tmp", "", "::1", "/tmp", true},
		{"u@[fe80::1]:x", "u", "fe80::1", "x", true},
	}
	for _, c := range cases {
		s, err := ParseSpec(c.in)
		if err != nil {
			t.Fatalf("%s: %v", c.in, err)
		}
		if s.User != c.user || s.Host != c.host || s.Path != c.path || s.Remote != c.remote {
			t.Errorf("%s: got %+v", c.in, s)
		}
	}
}

func TestSplitSource(t *testing.T) {
	cases := map[string][2]string{
		"dir":      {".", "dir"},
		"a/b/dir":  {"a/b", "dir"},
		"a/b/dir/": {"a/b/dir/", ""},
		"/":        {"/", ""},
		"/file":    {"/", "file"},
		"~":        {"~", ""},
		".":        {".", ""},
	}
	for in, want := range cases {
		r, s := SplitSource(in)
		if r != want[0] || s != want[1] {
			t.Errorf("%s: got %q %q want %q", in, r, s, want)
		}
	}
}

func TestDiff(t *testing.T) {
	src := []Entry{
		{Path: "", Type: 'd', Mode: 0o755, Mtime: 1},
		{Path: "same", Type: 'f', Size: 3, Mtime: 5, Mode: 0o644},
		{Path: "newer", Type: 'f', Size: 3, Mtime: 9, Mode: 0o644},
		{Path: "chmod", Type: 'f', Size: 3, Mtime: 5, Mode: 0o600},
		{Path: "new", Type: 'f', Size: 1, Mtime: 5, Mode: 0o644},
		{Path: "link", Type: 'l', Target: "b"},
		{Path: "wasdir", Type: 'f', Size: 1, Mtime: 5, Mode: 0o644},
		{Path: "d", Type: 'd', Mode: 0o755, Mtime: 1},
	}
	dst := []Entry{
		{Path: "", Type: 'd', Mode: 0o755, Mtime: 1},
		{Path: "same", Type: 'f', Size: 3, Mtime: 5, Mode: 0o644},
		{Path: "newer", Type: 'f', Size: 3, Mtime: 1, Mode: 0o644},
		{Path: "chmod", Type: 'f', Size: 3, Mtime: 5, Mode: 0o644},
		{Path: "link", Type: 'l', Target: "a"},
		{Path: "wasdir", Type: 'd'},
		{Path: "wasdir/x", Type: 'f'},
		{Path: "d", Type: 'f'},
		{Path: "extra", Type: 'd'},
		{Path: "extra/y", Type: 'f'},
		{Path: ".new.nimbus-part", Type: 'f'},
	}
	d := ComputeDiff(src, dst, DiffOptions{})
	var sent []string
	for _, s := range d.Send {
		sent = append(sent, s.Path)
	}
	if !reflect.DeepEqual(sent, []string{"newer", "new", "link"}) {
		t.Errorf("send = %v", sent)
	}
	if !reflect.DeepEqual(d.Blocked, []string{"wasdir"}) || len(d.Delete) != 0 {
		t.Errorf("blocked=%v delete=%v", d.Blocked, d.Delete)
	}
	if !reflect.DeepEqual(d.Replace, []string{"d"}) || !reflect.DeepEqual(d.MkDirs, []string{"d"}) {
		t.Errorf("replace=%v mkdirs=%v", d.Replace, d.MkDirs)
	}
	if len(d.MetaFiles) != 1 || d.MetaFiles[0].Path != "chmod" || d.Unchanged != 1 {
		t.Errorf("meta=%v unchanged=%d", d.MetaFiles, d.Unchanged)
	}

	d = ComputeDiff(src, dst, DiffOptions{Delete: true})
	if !reflect.DeepEqual(d.Delete, []string{"extra"}) || !reflect.DeepEqual(d.Replace, []string{"wasdir", "d"}) {
		t.Errorf("delete=%v replace=%v", d.Delete, d.Replace)
	}
	for _, s := range d.Send {
		if s.Path == "wasdir" {
			return
		}
	}
	t.Errorf("wasdir not sent after replace")
}

func TestPlanReuse(t *testing.T) {
	var send []Entry
	for i := 0; i < 50; i++ {
		send = append(send, Entry{Path: "f" + string(rune('a'+i%26)) + string(rune('a'+i/26)), Type: 'f', Size: 300 << 10})
	}
	send = append(send, Entry{Path: "zbig", Type: 'f', Size: 5<<20 + 1})
	o := PlanOptions{Block: 1 << 20, Comp: "zstd", Level: 3}
	p1 := MakePlan(send, o)
	parts := 0
	for _, b := range p1 {
		if b.Kind == "part" {
			parts++
			if b.Count != 1 {
				t.Errorf("part count %d", b.Count)
			}
		}
	}
	if parts != 6 {
		t.Fatalf("parts = %d", parts)
	}
	// Remove the files of batches 2 and 4 (done) and replan: the remaining
	// tar batches must keep their keys.
	done := map[string]bool{}
	for _, i := range []int{1, 3} {
		for _, p := range p1[i].Paths {
			done[p] = true
		}
	}
	var rest []Entry
	for _, e := range send {
		if !done[e.Path] {
			rest = append(rest, e)
		}
	}
	o.Previous = p1
	p2 := MakePlan(rest, o)
	keys := map[string]bool{}
	for _, b := range p1 {
		keys[b.Key] = true
	}
	for _, b := range p2 {
		if !keys[b.Key] {
			t.Errorf("batch %s not reused", b.Label())
		}
	}
	if len(p2) != len(p1)-2 {
		t.Errorf("len p2 = %d, p1 = %d", len(p2), len(p1))
	}
}

// TestAgentCompatListing runs the real agent with GNU find/tar --null
// disabled (the BusyBox / BSD code path) and checks it agrees with the native
// Go listing.
func TestAgentCompatListing(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("no sh")
	}
	for _, compat := range []string{"0", "1"} {
		t.Run("compat="+compat, func(t *testing.T) {
			root := t.TempDir()
			stage := filepath.Join(t.TempDir(), "nimbus-test")
			must := func(err error) {
				if err != nil {
					t.Fatal(err)
				}
			}
			must(os.MkdirAll(filepath.Join(root, "top", "dir with space", "sub"), 0o755))
			must(os.WriteFile(filepath.Join(root, "top", "dir with space", "f 1"), []byte("hello"), 0o600))
			must(os.WriteFile(filepath.Join(root, "top", "q'u\"o\\te"), []byte("x"), 0o644))
			must(os.WriteFile(filepath.Join(root, "top", "-dash"), []byte("x"), 0o755))
			must(os.WriteFile(filepath.Join(root, "top", "ünï ✓"), []byte("x"), 0o644))
			must(os.Symlink("dir with space/f 1", filepath.Join(root, "top", "ln")))
			t.Setenv("NIMBUS_FORCE_COMPAT", compat)

			ep := &Endpoint{T: transport.Local{}, Root: root, Sub: "top", Stage: stage}
			must(ep.Setup(context.Background()))
			want := compat == "0"
			if (ep.Info["gfind"] == "1") != want || (ep.Info["tarnull"] == "1") != want {
				t.Fatalf("probe: %v", ep.Info)
			}
			out, err := ep.callOut(context.Background(), "list", []string{agent.Arg(root), agent.Arg("top")}, nil)
			must(err)
			got, _, err := ParseListing(out)
			must(err)
			native, _, err := ListLocal(root, "top")
			must(err)
			if !reflect.DeepEqual(got, native) {
				t.Errorf("agent listing differs:\n agent  %+v\n native %+v", got, native)
			}
			sums, err := ep.Hash(context.Background(), []string{"top/dir with space/f 1", "top/q'u\"o\\te"})
			must(err)
			if sums[0] != "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824" {
				t.Errorf("hash = %v", sums)
			}
		})
	}
}

func TestAgentArgRoundTrip(t *testing.T) {
	stage := filepath.Join(t.TempDir(), "nimbus-x")
	ep := &Endpoint{T: transport.Local{}, Stage: stage, Root: "/"}
	if err := ep.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"plain", "with space", "o-starts-with-o", "r", "%d %s \\n", "ünï\t'\"$(x)`y`", "-n", ""} {
		// "dec" is exercised through list's SUB argument on a missing path:
		// instead call the decoder directly.
		cmd := exec.Command("sh", "-c", `. "$1"; dec "$2"`, "sh", filepath.Join(stage, "dec.sh"), agent.Arg(s))
		script := bytes.SplitN(agent.Script, []byte("\ncmd=${1:-}"), 2)[0]
		if err := os.WriteFile(filepath.Join(stage, "dec.sh"), script, 0o600); err != nil {
			t.Fatal(err)
		}
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("%q: %v", s, err)
		}
		if string(out) != s {
			t.Errorf("roundtrip %q -> %q (%s)", s, out, agent.Arg(s))
		}
	}
}

func TestFilter(t *testing.T) {
	f, err := NewFilter([]string{"*.tmp", "cache/", "/top.txt", "a/b", "logs/**/x?.log", "[!k]eep"})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		rel  string
		dir  bool
		want bool
	}{
		{"x.tmp", false, true},
		{"deep/er/x.tmp", false, true},
		{"x.tmpl", false, false},
		{"cache", true, true},
		{"sub/cache", true, true},
		{"cache", false, false}, // dir-only
		{"top.txt", false, true},
		{"sub/top.txt", false, false}, // anchored
		{"a/b", false, true},
		{"z/a/b", false, true},
		{"za/b", false, false},
		{"logs/1/2/x1.log", false, true},
		{"logs/x1.log", false, false},
		{"seep", false, true},
		{"keep", false, false},
	}
	for _, c := range cases {
		if got := f.Excluded(c.rel, c.dir); got != c.want {
			t.Errorf("%s (dir=%v): got %v", c.rel, c.dir, got)
		}
	}
	es := []Entry{
		{Path: "src", Type: 'd'},
		{Path: "src/cache", Type: 'd'},
		{Path: "src/cache/data", Type: 'f'},
		{Path: "src/keep", Type: 'f'},
		{Path: "src/top.txt", Type: 'f'},
	}
	got := f.Apply(es, "src")
	var paths []string
	for _, e := range got {
		paths = append(paths, e.Path)
	}
	if !reflect.DeepEqual(paths, []string{"src", "src/keep"}) {
		t.Errorf("apply = %v", paths)
	}
}

func TestDeleteKeepsExcluded(t *testing.T) {
	f, _ := NewFilter([]string{"*.tmp"})
	src := []Entry{{Path: "", Type: 'd'}, {Path: "keep", Type: 'f'}, {Path: "conflict", Type: 'f'}}
	full := []Entry{
		{Path: "", Type: 'd'}, {Path: "keep", Type: 'f'},
		{Path: "junk", Type: 'd'}, {Path: "junk/x.tmp", Type: 'f'}, {Path: "junk/y", Type: 'f'},
		{Path: "junk/sub", Type: 'd'}, {Path: "junk/sub/z", Type: 'f'},
		{Path: "gone", Type: 'd'}, {Path: "gone/w", Type: 'f'},
		{Path: "conflict", Type: 'd'}, {Path: "conflict/a.tmp", Type: 'f'},
	}
	dst := f.Apply(append([]Entry(nil), full...), "")
	prot := map[string]bool{"": true, "junk": true, "conflict": true}
	d := ComputeDiff(src, dst, DiffOptions{Delete: true, Protected: prot})
	sort.Strings(d.Delete)
	if !reflect.DeepEqual(d.Delete, []string{"gone", "junk/sub", "junk/y"}) {
		t.Errorf("delete = %v", d.Delete)
	}
	if !reflect.DeepEqual(d.Blocked, []string{"conflict"}) {
		t.Errorf("blocked = %v", d.Blocked)
	}
}
