// Package engine plans and executes a transfer: list both sides, diff, group
// changes into independent batches and push them through a pipelined
// pack → transfer → verify → extract chain with retries.
package engine

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"git.fferrando.cc/rdfilippo/nimbus/internal/agent"
	"git.fferrando.cc/rdfilippo/nimbus/internal/retry"
	"git.fferrando.cc/rdfilippo/nimbus/internal/transport"
	"git.fferrando.cc/rdfilippo/nimbus/internal/ui"
)

// Options for a transfer.
type Options struct {
	Block       int64
	Jobs        int
	Comp        string // auto|zstd|gzip|none
	Level       int    // 0 = compressor default
	Checksum    bool
	Delete      bool
	DryRun      bool
	Retry       retry.Policy
	Stall       time.Duration
	KeepStaging bool
	Fresh       bool
}

// Engine runs one transfer.
type Engine struct {
	Src, Dst *Endpoint
	Shared   bool // both endpoints are the same local machine and share staging
	O        Options
	UI       *ui.UI
	Stats    ui.Stats

	comp  string
	level int

	partMu    sync.Mutex
	partsLeft map[string]int
	partEntry map[string]Entry

	srcState, dstState *stageState
	errMu              sync.Mutex
	failures           []string
}

// Result summarizes a finished run.
type Result struct {
	Diff      *Diff
	Batches   int
	Started   time.Time
	XferStart time.Time
	End       time.Time
	Comp      string
	Failed    int
	Deleted   int
	DryRun    bool
}

var errSrcMissing = errors.New("staged data missing on source")

func (e *Engine) logRetry(what string) func(int, error, time.Duration) {
	return func(a int, err error, wait time.Duration) {
		e.Stats.Retries.Add(1)
		e.UI.Retry("%s failed (attempt %d/%d): %v; retrying in %s", what, a, e.O.Retry.Attempts, shortErr(err), wait.Round(100*time.Millisecond))
	}
}

func shortErr(err error) string {
	s := err.Error()
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 160 {
		s = s[:157] + "..."
	}
	return s
}

// do runs fn with retries; connection errors trigger a reconnect of the
// involved remote endpoints before the next attempt.
func (e *Engine) do(ctx context.Context, what string, eps []*Endpoint, fn func(ctx context.Context) error) error {
	var last error
	return retry.Do(ctx, e.O.Retry, func(a int) error {
		if a > 0 && last != nil && transport.IsConnErr(last) {
			for _, ep := range eps {
				if !ep.T.IsLocal() {
					if err := ep.T.Reconnect(ctx); err != nil {
						last = err
						return err
					}
				}
			}
		}
		err := fn(ctx)
		switch transport.ExitCode(err) {
		case 6:
			err = retry.Permanent(err)
		}
		last = err
		return err
	}, e.logRetry(what))
}

// ---------------------------------------------------------------- run

// Run executes the transfer.
func (e *Engine) Run(ctx context.Context) (*Result, error) {
	res := &Result{Started: time.Now()}
	u := e.UI

	// 1. prepare endpoints
	u.Phase("Preparing endpoints")
	eps := []*Endpoint{e.Src}
	if !e.Shared {
		eps = append(eps, e.Dst)
	}
	if err := parallel(eps, func(ep *Endpoint) error {
		return e.do(ctx, "setup "+ep.Name(), []*Endpoint{ep}, ep.Setup)
	}); err != nil {
		return nil, err
	}
	if e.Shared {
		e.Dst.Info = e.Src.Info
	}
	for _, ep := range eps {
		u.Debug("%s: %s/%s tar=%s gfind=%s stat=%s sha=%q zstd=%s pigz=%s cpus=%s",
			ep.Name(), ep.Info["os"], ep.Info["arch"], ep.Info["tar"], ep.Info["gfind"], ep.Info["stat"],
			ep.Info["sha"], ep.Info["zstd"], ep.Info["pigz"], ep.Info["nproc"])
	}
	if err := e.chooseComp(); err != nil {
		return nil, err
	}
	res.Comp = e.comp

	// 2. list
	u.Phase("Scanning source and destination")
	var srcList, dstList []Entry
	var srcSkip, dstSkip []string
	err := parallel([]int{0, 1}, func(i int) error {
		if i == 0 {
			return e.do(ctx, "list source", []*Endpoint{e.Src}, func(ctx context.Context) error {
				var err error
				srcList, srcSkip, err = e.Src.List(ctx)
				if errors.Is(err, ErrNotFound) {
					return retry.Permanent(fmt.Errorf("source %s does not exist", e.Src.Spec))
				}
				return err
			})
		}
		return e.do(ctx, "list destination", []*Endpoint{e.Dst}, func(ctx context.Context) error {
			var err error
			dstList, dstSkip, err = e.Dst.List(ctx)
			if errors.Is(err, ErrNotFound) {
				dstList, dstSkip, err = nil, nil, nil
			}
			return err
		})
	})
	if err != nil {
		return nil, err
	}
	for _, s := range srcSkip {
		u.Warn("skipping special file %s", s)
	}
	_ = dstSkip
	if !e.Src.T.IsLocal() || e.Src.Info["tarnull"] != "1" || e.Dst.Info["tarnull"] != "1" {
		for _, s := range srcList {
			if strings.ContainsRune(s.Path, '\n') && (e.Src.Info["tarnull"] != "1") {
				return nil, fmt.Errorf("file name with newline not supported by %s's tar: %q", e.Src.Name(), s.Path)
			}
		}
	}

	d := ComputeDiff(srcList, dstList, DiffOptions{Checksum: e.O.Checksum, Delete: e.O.Delete})
	if len(d.CheckSum) > 0 {
		u.Phase("Comparing checksums of %s files", ui.Count(int64(len(d.CheckSum))))
		paths := make([]string, len(d.CheckSum))
		for i, s := range d.CheckSum {
			paths[i] = s.Path
		}
		var ss, ds []string
		err := parallel([]int{0, 1}, func(i int) error {
			if i == 0 {
				return e.do(ctx, "hash source", []*Endpoint{e.Src}, func(ctx context.Context) (err error) {
					ss, err = e.Src.Hash(ctx, paths)
					return
				})
			}
			return e.do(ctx, "hash destination", []*Endpoint{e.Dst}, func(ctx context.Context) (err error) {
				ds, err = e.Dst.Hash(ctx, paths)
				return
			})
		})
		if err != nil {
			return nil, err
		}
		d.ApplyChecksums(dstList, ss, ds)
	}
	res.Diff = d
	for _, b := range d.Blocked {
		u.Warn("%s is a directory on the destination but not in the source; use --delete to replace it", b)
	}
	if e.O.Delete && len(d.Delete) > 0 && len(srcList) <= 1 && (len(srcList) == 0 || srcList[0].Type == 'd') {
		u.Warn("source is empty: refusing to delete %d destination entries", len(d.Delete))
		d.Delete = nil
	}
	u.OK("%s to send (%s), %s unchanged, %s to delete",
		plural(len(d.Send), "item"), ui.Bytes(d.Bytes()), ui.Count(int64(d.Unchanged)), ui.Count(int64(len(d.Delete))))

	if e.O.DryRun {
		res.DryRun = true
		res.End = time.Now()
		printDryRun(d)
		return res, nil
	}

	// 3. plan
	planPath := filepath.Join(e.localStage(), "plan.json")
	var prev []*Batch
	if !e.O.Fresh {
		prev = LoadPlan(planPath)
	}
	batches := MakePlan(d.Send, PlanOptions{Block: e.O.Block, Comp: e.comp, Level: e.level, Previous: prev})
	if err := SavePlan(planPath, batches); err != nil {
		u.Warn("cannot save plan for resume: %v", err)
	}
	res.Batches = len(batches)

	// 4. staged state from a previous run
	if err := e.loadState(ctx); err != nil {
		return nil, err
	}

	// 5. prepare destination tree
	if err := e.prepareDest(ctx, d, batches); err != nil {
		return nil, err
	}

	// 6. pipeline
	res.XferStart = time.Now()
	if len(batches) > 0 {
		u.Phase("Transferring %s in %s (%s, %d jobs)", ui.Bytes(d.Bytes()), plural(len(batches), "batch"), e.compLabel(), e.O.Jobs)
		e.pipeline(ctx, batches)
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
	}
	res.Failed = int(e.Stats.Failed.Load())
	if res.Failed > 0 {
		res.End = time.Now()
		for _, f := range e.failures {
			u.Error("%s", f)
		}
		return res, fmt.Errorf("%d of %d batches failed; staged data kept, run the same command again to resume", res.Failed, len(batches))
	}

	// 7. metadata, deletions, cleanup
	if err := e.finalize(ctx, d); err != nil {
		res.End = time.Now()
		return res, err
	}
	res.Deleted = len(d.Delete)
	if !e.O.KeepStaging {
		for _, ep := range eps {
			if err := ep.call(ctx, "clean", nil, nil, nil); err != nil {
				u.Warn("%s: could not remove staging %s: %v", ep.Name(), ep.Stage, err)
			}
		}
	}
	res.End = time.Now()
	return res, nil
}

func plural(n int, s string) string {
	if n == 1 {
		return "1 " + s
	}
	if strings.HasSuffix(s, "y") {
		return ui.Count(int64(n)) + " " + s[:len(s)-1] + "ies"
	}
	if strings.HasSuffix(s, "ch") {
		return ui.Count(int64(n)) + " " + s + "es"
	}
	return ui.Count(int64(n)) + " " + s + "s"
}

func printDryRun(d *Diff) {
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	for _, p := range d.Replace {
		fmt.Fprintf(w, "replace  %s\n", p)
	}
	for _, p := range d.MkDirs {
		fmt.Fprintf(w, "mkdir    %s/\n", p)
	}
	for _, s := range d.Send {
		if s.Type == 'l' {
			fmt.Fprintf(w, "link     %s -> %s\n", s.Path, s.Target)
		} else {
			fmt.Fprintf(w, "send     %s (%s)\n", s.Path, ui.Bytes(s.Size))
		}
	}
	for _, s := range d.MetaFiles {
		fmt.Fprintf(w, "attrs    %s\n", s.Path)
	}
	for _, p := range d.Delete {
		fmt.Fprintf(w, "delete   %s\n", p)
	}
}

func (e *Engine) localStage() string {
	if e.Src.T.IsLocal() {
		return e.Src.Stage
	}
	return e.Dst.Stage
}

func (e *Engine) compLabel() string {
	if e.comp == "none" {
		return "uncompressed"
	}
	return fmt.Sprintf("%s -%d", e.comp, e.level)
}

func (e *Engine) chooseComp() error {
	c := e.O.Comp
	both := func(x string) bool { return e.Src.HasComp(x) && e.Dst.HasComp(x) }
	switch c {
	case "", "auto":
		switch {
		case e.Shared:
			c = "none"
		case both("zstd"):
			c = "zstd"
		case both("gzip"):
			c = "gzip"
		default:
			c = "none"
		}
	case "zstd", "gzip", "none":
		if !both(c) {
			return fmt.Errorf("compressor %s is not available on both endpoints (source: %v, destination: %v)", c, e.Src.HasComp(c), e.Dst.HasComp(c))
		}
	default:
		return fmt.Errorf("unknown compressor %q (auto, zstd, gzip, none)", c)
	}
	e.comp = c
	e.level = e.O.Level
	if e.level == 0 {
		switch c {
		case "zstd":
			e.level = 3
		case "gzip":
			e.level = 6
		default:
			e.level = 1
		}
	}
	return nil
}

// ---------------------------------------------------------------- state

type stageState struct {
	packed   map[string][2]string // key -> sha, size
	data     map[string]bool
	extracted map[string]bool
}

func parseState(out []byte) *stageState {
	s := &stageState{packed: map[string][2]string{}, data: map[string]bool{}, extracted: map[string]bool{}}
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) < 2 {
			continue
		}
		switch f[0] {
		case "P":
			if len(f) == 4 {
				s.packed[f[1]] = [2]string{f[2], f[3]}
			}
		case "R":
			s.data[f[1]] = true
		case "X":
			s.extracted[f[1]] = true
		}
	}
	return s
}

func (e *Engine) loadState(ctx context.Context) error {
	get := func(ep *Endpoint) (*stageState, error) {
		var out []byte
		err := e.do(ctx, "read staging "+ep.Name(), []*Endpoint{ep}, func(ctx context.Context) (err error) {
			out, err = ep.callOut(ctx, "state", nil, nil)
			return
		})
		return parseState(out), err
	}
	var err error
	if e.srcState, err = get(e.Src); err != nil {
		return err
	}
	if e.Shared {
		e.dstState = &stageState{packed: map[string][2]string{}, data: map[string]bool{}, extracted: e.srcState.extracted}
		return nil
	}
	e.dstState, err = get(e.Dst)
	return err
}

// ---------------------------------------------------------------- prepare

func (e *Engine) prepareDest(ctx context.Context, d *Diff, batches []*Batch) error {
	dst := e.Dst
	root := agent.Arg(dst.Root)
	if len(d.Replace) > 0 {
		if err := e.do(ctx, "remove replaced entries", []*Endpoint{dst}, func(ctx context.Context) error {
			return dst.call(ctx, "rmrf", []string{root}, nulList(d.Replace), nil)
		}); err != nil {
			return err
		}
	}
	dirs := append([]string{""}, d.MkDirs...)
	if err := e.do(ctx, "create directories", []*Endpoint{dst}, func(ctx context.Context) error {
		return dst.call(ctx, "mkdirs", []string{root}, nulList(dirs), nil)
	}); err != nil {
		return err
	}

	// Large files: count parts, drop stale partial files (no part extracted yet).
	e.partsLeft, e.partEntry = map[string]int{}, map[string]Entry{}
	byPath := map[string]Entry{}
	for _, s := range d.Send {
		byPath[s.Path] = s
	}
	started := map[string]bool{}
	for _, b := range batches {
		if b.Kind != "part" {
			continue
		}
		e.partsLeft[b.Path]++
		e.partEntry[b.Path] = byPath[b.Path]
		if e.dstState.extracted[b.Key] {
			started[b.Path] = true
		}
	}
	var stale []string
	for p := range e.partsLeft {
		if !started[p] {
			stale = append(stale, partName(p))
		}
	}
	if len(stale) > 0 {
		if err := e.do(ctx, "clean partial files", []*Endpoint{dst}, func(ctx context.Context) error {
			return dst.call(ctx, "rmrf", []string{root}, nulList(stale), nil)
		}); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------- pipeline

func (e *Engine) pipeline(ctx context.Context, batches []*Batch) {
	jobs := e.O.Jobs
	window := make(chan struct{}, 2*jobs+2)
	packSem := make(chan struct{}, jobs)
	xferSem := make(chan struct{}, jobs)
	extSem := make(chan struct{}, jobs)

	var total int64
	for _, b := range batches {
		total += b.RawSize
	}
	e.Stats.Batches.Store(int64(len(batches)))
	e.Stats.RawTotal.Store(total)

	title := "Sending"
	if e.Shared {
		title = "Copying"
	}
	e.UI.StartLive(title, &e.Stats)
	var wg sync.WaitGroup
	for _, b := range batches {
		select {
		case window <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(b *Batch) {
			defer wg.Done()
			defer func() { <-window }()
			if err := e.runBatch(ctx, b, packSem, xferSem, extSem); err != nil && ctx.Err() == nil {
				e.Stats.Failed.Add(1)
				e.errMu.Lock()
				e.failures = append(e.failures, fmt.Sprintf("batch #%d %s: %v", b.Seq, b.Label(), shortErr(err)))
				e.errMu.Unlock()
				e.UI.Error("batch #%d %s failed: %v", b.Seq, b.Label(), shortErr(err))
			}
		}(b)
	}
	wg.Wait()
	e.UI.StopLive()
}

func acquire(ctx context.Context, sem chan struct{}) error {
	select {
	case sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Engine) runBatch(ctx context.Context, b *Batch, packSem, xferSem, extSem chan struct{}) error {
	src, dst := e.Src, e.Dst
	tag := fmt.Sprintf("batch #%d", b.Seq)

	if b.Kind == "part" && e.dstState.extracted[b.Key] {
		// extracted by a previous run
		e.Stats.Packed.Add(1)
		e.Stats.Sent.Add(1)
		e.Stats.RawSent.Add(b.RawSize)
		e.Stats.Done.Add(1)
		return e.partDone(ctx, b)
	}

	received := !e.Shared && e.dstState.data[b.Key]
	if !received {
		for attempt := 0; ; attempt++ {
			sha, size, err := e.pack(ctx, b, packSem, tag)
			if err != nil {
				return err
			}
			if e.Shared {
				break
			}
			err = e.send(ctx, b, sha, size, xferSem, tag)
			if errors.Is(err, errSrcMissing) && attempt == 0 {
				_ = src.call(ctx, "drop", []string{b.Key}, nil, nil)
				e.partMu.Lock()
				delete(e.srcState.packed, b.Key)
				e.partMu.Unlock()
				e.Stats.Packed.Add(-1)
				continue
			}
			if err != nil {
				return err
			}
			break
		}
	} else {
		e.Stats.Packed.Add(1)
	}
	e.Stats.Sent.Add(1)
	if e.Shared || received {
		e.Stats.RawSent.Add(b.RawSize)
	}

	// extract
	if err := acquire(ctx, extSem); err != nil {
		return err
	}
	t := e.Stats.Begin(b.Seq, "extract", b.Label(), b.RawSize)
	args := []string{b.Key, b.Kind, e.comp, agent.Arg(dst.Root)}
	if b.Kind == "part" {
		args = append(args, agent.Arg(rel(partName(b.Path))), strconv.FormatInt(b.Offset, 10))
	}
	err := e.do(ctx, tag+" extract", []*Endpoint{dst}, func(ctx context.Context) error {
		return dst.call(ctx, "extract", args, nil, nil)
	})
	e.Stats.End(t)
	<-extSem
	if err != nil {
		return err
	}
	if !e.Shared {
		if err := e.do(ctx, tag+" cleanup", []*Endpoint{src}, func(ctx context.Context) error {
			return src.call(ctx, "drop", []string{b.Key}, nil, nil)
		}); err != nil {
			e.UI.Warn("%s: could not drop staged data: %v", tag, err)
		}
	} else {
		_ = src.call(ctx, "drop", []string{b.Key}, nil, nil)
	}
	e.Stats.Done.Add(1)
	if b.Kind == "part" {
		return e.partDone(ctx, b)
	}
	return nil
}

func (e *Engine) pack(ctx context.Context, b *Batch, sem chan struct{}, tag string) (sha string, size int64, err error) {
	e.partMu.Lock()
	p, ok := e.srcState.packed[b.Key]
	e.partMu.Unlock()
	if ok {
		e.Stats.Packed.Add(1)
		n, _ := strconv.ParseInt(p[1], 10, 64)
		return p[0], n, nil
	}
	if err := acquire(ctx, sem); err != nil {
		return "", 0, err
	}
	defer func() { <-sem }()
	t := e.Stats.Begin(b.Seq, "pack", b.Label(), b.RawSize)
	defer e.Stats.End(t)

	var list []byte
	args := []string{b.Key, b.Kind, e.comp, strconv.Itoa(e.level), agent.Arg(e.Src.Root)}
	if b.Kind == "tar" {
		list = nulList(b.Paths).(*bytes.Buffer).Bytes()
	} else {
		list = []byte(rel(b.Path))
		args = append(args, strconv.FormatInt(b.Offset, 10), strconv.FormatInt(b.Count, 10))
	}
	err = e.do(ctx, tag+" pack", []*Endpoint{e.Src}, func(ctx context.Context) error {
		out, err := e.Src.callOut(ctx, "pack", args, bytes.NewReader(list))
		if err != nil {
			return err
		}
		f := strings.Fields(string(out))
		if len(f) != 2 {
			return fmt.Errorf("unexpected pack output %q", out)
		}
		sha = f[0]
		size, err = strconv.ParseInt(f[1], 10, 64)
		return err
	})
	if err == nil {
		e.Stats.Packed.Add(1)
		e.Stats.RawPacked.Add(b.RawSize)
	}
	return sha, size, err
}

// progressWriter counts streamed bytes for the UI and the stall watchdog.
type progressWriter struct {
	w        io.Writer
	task     *ui.Task
	stats    *ui.Stats
	raw      int64
	wire     int64
	n        int64
	rawAdded int64
	last     atomic.Int64
}

func (p *progressWriter) Write(b []byte) (int, error) {
	n, err := p.w.Write(b)
	if n > 0 {
		p.n += int64(n)
		p.task.Done.Store(p.n)
		p.stats.WireBytes.Add(int64(n))
		if p.wire > 0 {
			r := p.raw * p.n / p.wire
			if r > p.raw {
				r = p.raw
			}
			p.stats.RawSent.Add(r - p.rawAdded)
			p.rawAdded = r
		}
		p.last.Store(time.Now().UnixNano())
	}
	return n, err
}

func (p *progressWriter) rollback() {
	p.stats.WireBytes.Add(-p.n)
	p.stats.RawSent.Add(-p.rawAdded)
	p.n, p.rawAdded = 0, 0
	p.task.Done.Store(0)
}

func (p *progressWriter) complete() {
	p.stats.RawSent.Add(p.raw - p.rawAdded)
	p.rawAdded = p.raw
}

var errStalled = fmt.Errorf("%w: transfer stalled", transport.ErrConn)

func (e *Engine) send(ctx context.Context, b *Batch, sha string, size int64, sem chan struct{}, tag string) error {
	if err := acquire(ctx, sem); err != nil {
		return err
	}
	defer func() { <-sem }()
	t := e.Stats.Begin(b.Seq, "send", b.Label(), size)
	defer e.Stats.End(t)

	return e.do(ctx, tag+" send", []*Endpoint{e.Src, e.Dst}, func(ctx context.Context) error {
		actx, cancel := context.WithCancelCause(ctx)
		defer cancel(nil)
		pr, pw := io.Pipe()
		pwr := &progressWriter{w: pw, task: t, stats: &e.Stats, raw: b.RawSize, wire: size}
		pwr.last.Store(time.Now().UnixNano())

		var sendErr, recvErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			sendErr = e.Src.T.Run(actx, e.Src.argv("send", b.Key), nil, pwr, nil)
			if sendErr != nil {
				cancel(sendErr)
			}
			pw.CloseWithError(sendErr)
		}()
		go func() {
			defer wg.Done()
			recvErr = e.Dst.T.Run(actx, e.Dst.argv("recv", b.Key, sha), pr, nil, nil)
			if recvErr != nil {
				cancel(recvErr)
			}
			pr.CloseWithError(errors.New("receiver closed"))
		}()
		stop := make(chan struct{})
		go func() {
			tk := time.NewTicker(time.Second)
			defer tk.Stop()
			for {
				select {
				case <-stop:
					return
				case <-tk.C:
					if time.Since(time.Unix(0, pwr.last.Load())) > e.O.Stall {
						cancel(errStalled)
						return
					}
				}
			}
		}()
		wg.Wait()
		close(stop)

		if sendErr == nil && recvErr == nil {
			pwr.complete()
			return nil
		}
		pwr.rollback()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if c := context.Cause(actx); errors.Is(c, errStalled) {
			return errStalled
		}
		if transport.ExitCode(sendErr) == 4 {
			return retry.Permanent(errSrcMissing)
		}
		// Report the root cause rather than the side that was cancelled.
		if sendErr != nil && !errors.Is(sendErr, context.Canceled) {
			return fmt.Errorf("source: %w", sendErr)
		}
		if recvErr != nil && !errors.Is(recvErr, context.Canceled) {
			if transport.ExitCode(recvErr) == 3 {
				return fmt.Errorf("destination: checksum mismatch")
			}
			return fmt.Errorf("destination: %w", recvErr)
		}
		return fmt.Errorf("%w: transfer interrupted", transport.ErrConn)
	})
}

func (e *Engine) partDone(ctx context.Context, b *Batch) error {
	e.partMu.Lock()
	e.partsLeft[b.Path]--
	last := e.partsLeft[b.Path] == 0
	ent := e.partEntry[b.Path]
	e.partMu.Unlock()
	if !last {
		return nil
	}
	dst := e.Dst
	root := agent.Arg(dst.Root)
	return e.do(ctx, "finish "+b.Path, []*Endpoint{dst}, func(ctx context.Context) error {
		if err := dst.call(ctx, "finish", []string{root}, bytes.NewReader(nulFields(rel(partName(b.Path)), rel(b.Path))), nil); err != nil {
			return err
		}
		return dst.call(ctx, "meta", []string{root}, bytes.NewReader(metaRecord(ent)), nil)
	})
}

func metaRecord(e Entry) []byte {
	t := time.Unix(e.Mtime, 0).UTC().Format("200601021504.05")
	return nulFields(fmt.Sprintf("%04o", e.Mode), t, rel(e.Path))
}

// ---------------------------------------------------------------- finalize

func (e *Engine) finalize(ctx context.Context, d *Diff) error {
	dst := e.Dst
	root := agent.Arg(dst.Root)
	if len(d.MetaFiles) > 0 {
		var buf bytes.Buffer
		for _, f := range d.MetaFiles {
			buf.Write(metaRecord(f))
		}
		if err := e.do(ctx, "update attributes", []*Endpoint{dst}, func(ctx context.Context) error {
			return dst.call(ctx, "meta", []string{root}, bytes.NewReader(buf.Bytes()), nil)
		}); err != nil {
			return err
		}
	}
	if len(d.Delete) > 0 {
		e.UI.Phase("Deleting %s on destination", plural(len(d.Delete), "entry"))
		if err := e.do(ctx, "delete", []*Endpoint{dst}, func(ctx context.Context) error {
			return dst.call(ctx, "rmrf", []string{root}, nulList(d.Delete), nil)
		}); err != nil {
			return err
		}
	}
	if len(d.DirMeta) > 0 {
		var buf bytes.Buffer
		for _, f := range d.DirMeta {
			buf.Write(metaRecord(f))
		}
		if err := e.do(ctx, "directory attributes", []*Endpoint{dst}, func(ctx context.Context) error {
			return dst.call(ctx, "meta", []string{root}, bytes.NewReader(buf.Bytes()), nil)
		}); err != nil {
			return err
		}
	}
	return nil
}

func parallel[T any](items []T, fn func(T) error) error {
	errs := make([]error, len(items))
	var wg sync.WaitGroup
	for i, it := range items {
		wg.Add(1)
		go func(i int, it T) {
			defer wg.Done()
			errs[i] = fn(it)
		}(i, it)
	}
	wg.Wait()
	return errors.Join(errs...)
}
