// nimbus: resumable, parallel, compressed file synchronization over SSH.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	flag "github.com/spf13/pflag"

	"git.fferrando.cc/rdfilippo/nimbus/internal/engine"
	"git.fferrando.cc/rdfilippo/nimbus/internal/retry"
	"git.fferrando.cc/rdfilippo/nimbus/internal/transport"
	"git.fferrando.cc/rdfilippo/nimbus/internal/ui"
)

var version = "dev"

const usage = `nimbus - resumable, parallel, compressed sync over SSH

Usage:
  nimbus [options] SRC DST

Locations are local paths or [user@]host:path (rsync syntax).
  SRC  without trailing slash copies the directory itself: DST/name/...
  SRC/ with trailing slash copies its contents:             DST/...
DST is a directory (created if missing), except that a single-file SRC is
written as DST itself when DST has no trailing slash and is not a directory.

Options:
`

func main() {
	os.Exit(run())
}

func run() int {
	fs := flag.NewFlagSet("nimbus", flag.ContinueOnError)
	fs.SortFlags = false
	var (
		blockS   = fs.StringP("block", "b", "64M", "batch / chunk size (K, M, G suffixes; multiple of 1M)")
		jobs     = fs.IntP("jobs", "j", 4, "parallel pack / transfer / extract jobs")
		conns    = fs.Int("conns", 0, "SSH connections (0 = one per job)")
		comp     = fs.String("compress", "auto", "compression: auto, zstd, gzip, none")
		level    = fs.Int("level", 0, "compression level (0 = default: zstd 3, gzip 6)")
		checksum = fs.BoolP("checksum", "c", false, "compare file contents by sha256 instead of size+mtime")
		del      = fs.Bool("delete", false, "delete destination files that are not in the source")
		dry      = fs.BoolP("dry-run", "n", false, "show what would be done, change nothing")
		retries  = fs.Int("retries", 10, "attempts per operation before giving up")
		backoff  = fs.Duration("backoff", time.Second, "initial retry backoff (doubles, with jitter)")
		backoffM = fs.Duration("backoff-max", time.Minute, "maximum retry backoff")
		stall    = fs.Duration("timeout", 60*time.Second, "abort and retry a transfer with no progress for this long")
		sshCmd   = fs.StringP("ssh-cmd", "e", "", `use the system ssh binary, e.g. "ssh" or "ssh -J bastion"`)
		sshCfg   = fs.StringP("ssh-config", "F", "", "ssh config file (default ~/.ssh/config)")
		excludes = fs.StringArray("exclude", nil, "exclude matching files (rsync patterns, repeatable)")
		exclFrom = fs.StringArray("exclude-from", nil, "read exclude patterns from FILE")
		port     = fs.Int("port", 0, "SSH port")
		ident    = fs.String("identity", "", "SSH private key file")
		staging  = fs.String("staging", "/var/tmp", "staging directory on both endpoints")
		keep     = fs.Bool("keep-staging", false, "do not delete staged data after success")
		fresh    = fs.Bool("fresh", false, "ignore staged data from interrupted runs")
		plain    = fs.Bool("plain", false, "plain line output (no live progress)")
		noColor  = fs.Bool("no-color", false, "disable colors")
		quiet    = fs.BoolP("quiet", "q", false, "only print errors")
		verbose  = fs.BoolP("verbose", "v", false, "more details")
		showVer  = fs.BoolP("version", "V", false, "print version")
	)
	// rsync habits: accepted and ignored (nimbus always behaves like -a).
	for _, c := range rsyncNoops {
		fs.BoolP("rsync-"+c, c, false, "")
		_ = fs.MarkHidden("rsync-" + c)
	}
	fs.Usage = func() { printUsage(fs) }
	if err := fs.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintln(os.Stderr, "nimbus:", err)
		return 2
	}
	if *showVer {
		fmt.Println("nimbus", version)
		return 0
	}
	u := ui.New(ui.Options{Plain: *plain, Quiet: *quiet, Verbose: *verbose, NoColor: *noColor})
	fail := func(format string, a ...any) int {
		u.Error(format, a...)
		return 2
	}
	if fs.NArg() != 2 {
		fs.Usage()
		return 2
	}
	block, err := engine.ParseSize(*blockS)
	if err != nil {
		return fail("--block: %v", err)
	}
	if block < 1<<20 || block%(1<<20) != 0 {
		return fail("--block must be a multiple of 1M (got %s)", *blockS)
	}
	if *jobs < 1 || *jobs > 256 {
		return fail("--jobs must be between 1 and 256")
	}
	if *retries < 1 {
		return fail("--retries must be at least 1")
	}
	if !strings.HasPrefix(*staging, "/") {
		return fail("--staging must be an absolute path")
	}

	patterns := append([]string{}, *excludes...)
	for _, f := range *exclFrom {
		ps, err := engine.ReadPatterns(f)
		if err != nil {
			return fail("--exclude-from: %v", err)
		}
		patterns = append(patterns, ps...)
	}
	filter, err := engine.NewFilter(patterns)
	if err != nil {
		return fail("%v", err)
	}
	cfgFile := *sshCfg
	if cfgFile == "" {
		cfgFile = os.Getenv("NIMBUS_SSH_CONFIG")
	}
	if cfgFile != "" {
		if err := transport.SetConfigFile(cfgFile); err != nil {
			return fail("--ssh-config: %v", err)
		}
	}

	src, err := engine.ParseSpec(fs.Arg(0))
	if err != nil {
		return fail("source: %v", err)
	}
	dst, err := engine.ParseSpec(fs.Arg(1))
	if err != nil {
		return fail("destination: %v", err)
	}
	if src.Remote && dst.Remote {
		return fail("remote to remote transfers are not supported; run nimbus on one of the hosts")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	u.Banner(version, src.String(), dst.String())

	srcRoot, sub := engine.SplitSource(src.Path)
	if !src.Remote {
		if abs, err := filepath.Abs(engine.ExpandTilde(srcRoot)); err == nil {
			srcRoot = abs
		}
	}
	dstRoot := strings.TrimSuffix(dst.Path, "/")
	if dstRoot == "" {
		dstRoot = "/"
	}
	if !dst.Remote {
		if abs, err := filepath.Abs(engine.ExpandTilde(dstRoot)); err == nil {
			dstRoot = abs
		}
	}
	if !src.Remote && !dst.Remote {
		if within(filepath.Join(dstRoot, sub), filepath.Join(srcRoot, sub)) || within(filepath.Join(srcRoot, sub), filepath.Join(dstRoot, sub)) {
			return fail("source and destination overlap")
		}
	}

	// Transfer id: stable across runs of the same command, used for staging
	// directories and resume.
	h := sha256.Sum256([]byte(remoteKey(src) + "\x00" + srcRoot + "\x00" + sub + "\x00" + remoteKey(dst) + "\x00" + dstRoot))
	id := hex.EncodeToString(h[:6])
	stageBase := filepath.Join(*staging, "nimbus-"+id)

	shared := !src.Remote && !dst.Remote
	// Prevent two concurrent runs of the same transfer.
	lock, err := lockFile(filepath.Join(*staging, "nimbus-"+id+".lock"))
	if err != nil {
		u.Error("%v", err)
		return 1
	}
	defer lock()

	// Transport for the remote side.
	var remote transport.Transport = transport.Local{}
	if src.Remote || dst.Remote {
		rs := dst
		if src.Remote {
			rs = src
		}
		nconns := *conns
		if nconns <= 0 {
			nconns = *jobs
		}
		u.Phase("Connecting to %s", rs.Dest())
		logf := func(f string, a ...any) { u.Warn(f, a...) }
		cmdline := *sshCmd
		if cmdline == "" {
			if why := transport.NeedsSystemSSH(rs.Host); why != "" {
				u.Info("%s uses %s: using the system ssh client", rs.Host, why)
				cmdline = "ssh"
			}
		}
		if cmdline != "" {
			argv := splitWords(cmdline)
			if *sshCfg != "" {
				argv = append(argv, "-F", *sshCfg)
			}
			if *conns <= 0 && nconns > 1 && !transport.BatchModeWorks(ctx, argv, rs.Dest()) {
				u.Warn("ssh needs interactive authentication: using a single connection (set -c N to override)")
				nconns = 1
			}
			remote, err = transport.NewSystem(ctx, argv, rs.Dest(), transport.SystemOptions{Conns: nconns, ID: id, Port: *port, Identity: *ident, Logf: logf})
		} else {
			remote, err = transport.NewNative(ctx, transport.SSHTarget{User: rs.User, Host: rs.Host, Port: *port, Identity: *ident},
				transport.NativeOptions{Conns: nconns, Logf: logf})
		}
		if err != nil {
			u.Error("%v", err)
			return 1
		}
		defer remote.Close()
		u.OK("Connected to %s (%d connections)", remote.Name(), nconns)
	}

	mk := func(s engine.Spec, root, sub, stage string) *engine.Endpoint {
		var t transport.Transport = transport.Local{}
		if s.Remote {
			t = remote
		}
		return &engine.Endpoint{Spec: s, T: t, Root: root, Sub: sub, Stage: stage}
	}
	var srcEP, dstEP *engine.Endpoint
	if shared {
		srcEP = mk(src, srcRoot, sub, stageBase)
		dstEP = mk(dst, dstRoot, sub, stageBase)
	} else {
		srcEP = mk(src, srcRoot, sub, stageBase+"-s")
		dstEP = mk(dst, dstRoot, sub, stageBase+"-d")
	}

	eng := &engine.Engine{
		Src: srcEP, Dst: dstEP, Shared: shared, UI: u,
		O: engine.Options{
			Block: block, Jobs: *jobs, Comp: *comp, Level: *level,
			Checksum: *checksum, Delete: *del, DryRun: *dry,
			Retry:       retry.Policy{Attempts: *retries, Base: *backoff, Max: *backoffM},
			Stall:       *stall,
			KeepStaging: *keep, Fresh: *fresh,
			Exclude:      filter,
			RenameTarget: renameTarget(dst.Path, dstRoot),
		},
	}
	res, err := eng.Run(ctx)
	if errors.Is(err, context.Canceled) || ctx.Err() != nil {
		u.StopLive()
		u.Warn("interrupted: staged data kept, run the same command again to resume")
		return 130
	}
	printSummary(u, eng, res, err)
	if err != nil {
		if res == nil {
			u.Error("%v", err)
		}
		return 1
	}
	if res != nil && res.Blocked > 0 {
		u.Warn("%d entries could not be updated (see warnings above)", res.Blocked)
		return 23
	}
	return 0
}

func printSummary(u *ui.UI, eng *engine.Engine, res *engine.Result, err error) {
	if res == nil || res.Diff == nil {
		return
	}
	d := res.Diff
	if res.DryRun {
		u.Summary(true, "Dry run: nothing changed", []ui.Row{
			{Key: "Would send", Value: fmt.Sprintf("%s items, %s", ui.Count(int64(len(d.Send))), ui.Bytes(d.Bytes()))},
			{Key: "Would delete", Value: ui.Count(int64(len(d.Delete)))},
			{Key: "Unchanged", Value: ui.Count(int64(d.Unchanged))},
		})
		return
	}
	s := &eng.Stats
	total := res.End.Sub(res.Started)
	xfer := res.End.Sub(res.XferStart)
	if res.XferStart.IsZero() {
		xfer = 0
	}
	raw, wire := d.Bytes(), s.WireBytes.Load()
	rows := []ui.Row{
		{Key: "Files", Value: fmt.Sprintf("%s sent · %s unchanged · %s deleted", ui.Count(int64(len(d.Send))), ui.Count(int64(d.Unchanged)), ui.Count(int64(res.Deleted)))},
	}
	data := ui.Bytes(raw)
	if wire > 0 && res.Comp != "none" {
		data += fmt.Sprintf("  (wire %s, %s %.2fx)", ui.Bytes(wire), res.Comp, float64(s.RawSent.Load())/float64(max(wire, 1)))
	}
	rows = append(rows, ui.Row{Key: "Data", Value: data})
	rows = append(rows, ui.Row{Key: "Time", Value: fmt.Sprintf("%s total · %s transfer", ui.Duration(total), ui.Duration(xfer))})
	if xfer > 0 && raw > 0 {
		rows = append(rows, ui.Row{Key: "Speed", Value: fmt.Sprintf("%s/s effective", ui.Bytes(int64(float64(raw)/xfer.Seconds())))})
	}
	if r := s.Retries.Load(); r > 0 {
		rows = append(rows, ui.Row{Key: "Retries", Value: ui.Count(r)})
	}
	if err != nil {
		rows = append(rows, ui.Row{Key: "Error", Value: err.Error()})
		u.Summary(false, "Transfer incomplete", rows)
		return
	}
	u.Summary(true, "Transfer complete", rows)
}

func printUsage(fs *flag.FlagSet) {
	fmt.Fprint(os.Stderr, usage)
	type row struct{ left, right string }
	var rows []row
	width := 0
	fs.VisitAll(func(f *flag.Flag) {
		if f.Hidden {
			return
		}
		left := "    "
		if f.Shorthand != "" {
			left = "-" + f.Shorthand + ", "
		}
		left += "--" + f.Name
		if t := f.Value.Type(); t != "bool" {
			left += " " + map[string]string{"string": "STR", "int": "N", "duration": "DUR"}[t]
		}
		right := f.Usage
		if d := f.DefValue; d != "" && d != "false" && d != "0" {
			right += fmt.Sprintf(" (default %s)", d)
		}
		rows = append(rows, row{left, right})
		if len(left) > width {
			width = len(left)
		}
	})
	for _, r := range rows {
		fmt.Fprintf(os.Stderr, "  %-*s  %s\n", width, r.left, r.right)
	}
	fmt.Fprintf(os.Stderr, `
rsync compatibility: -%s are accepted and ignored.
nimbus always recurses and keeps symlinks, permissions and mtimes; ownership
is kept only when extracting as root. Hard links, ACLs and xattrs are not
preserved.

Exit codes: 0 ok, 1 transfer failed (rerun to resume), 2 usage error,
            23 some entries could not be updated, 130 interrupted.
`, strings.Join(rsyncNoops, " -"))
}

var rsyncNoops = []string{"a", "r", "l", "t", "p", "g", "o", "D", "P", "h", "z"}

// renameTarget returns the destination path eligible for rsync's single-file
// rename rule, or "" when DST explicitly names a directory.
func renameTarget(raw, root string) string {
	switch {
	case strings.HasSuffix(raw, "/"), raw == "~", raw == ".", raw == "..",
		strings.HasSuffix(raw, "/."), strings.HasSuffix(raw, "/.."), root == "/":
		return ""
	}
	return root
}

func remoteKey(s engine.Spec) string {
	if !s.Remote {
		return "local"
	}
	return s.Dest()
}

func within(p, parent string) bool {
	r, err := filepath.Rel(parent, p)
	return err == nil && r != ".." && !strings.HasPrefix(r, "../")
}

// splitWords splits a command line honoring simple quotes.
func splitWords(s string) []string {
	var out []string
	var cur strings.Builder
	var q rune
	has := false
	for _, r := range s {
		switch {
		case q != 0:
			if r == q {
				q = 0
			} else {
				cur.WriteRune(r)
			}
		case r == '\'' || r == '"':
			q, has = r, true
		case r == ' ' || r == '\t':
			if has || cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
				has = false
			}
		default:
			cur.WriteRune(r)
		}
	}
	if has || cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

func lockFile(path string) (func(), error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("another nimbus run of this transfer is in progress (%s)", path)
	}
	return func() {
		_ = os.Remove(path)
		f.Close()
	}, nil
}
