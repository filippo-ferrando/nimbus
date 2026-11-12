// Package ui renders nimbus progress: a live multi-line view on terminals and
// plain, log-friendly lines otherwise.
package ui

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"golang.org/x/term"
)

// ---------------------------------------------------------------- stats

// Stats is updated concurrently by the engine and read by the renderer.
type Stats struct {
	Batches   atomic.Int64
	Packed    atomic.Int64
	Sent      atomic.Int64
	Done      atomic.Int64
	Failed    atomic.Int64
	RawTotal  atomic.Int64
	RawSent   atomic.Int64 // source bytes whose compressed form reached the destination
	WireBytes atomic.Int64 // bytes on the wire
	RawPacked atomic.Int64
	Retries   atomic.Int64

	mu    sync.Mutex
	tasks map[*Task]struct{}
}

// Task is one in-flight stage of a batch, shown as a worker line.
type Task struct {
	Seq   int
	Stage string
	Label string
	Total int64
	Done  atomic.Int64
	Start time.Time
}

func (s *Stats) Begin(seq int, stage, label string, total int64) *Task {
	t := &Task{Seq: seq, Stage: stage, Label: label, Total: total, Start: time.Now()}
	s.mu.Lock()
	if s.tasks == nil {
		s.tasks = map[*Task]struct{}{}
	}
	s.tasks[t] = struct{}{}
	s.mu.Unlock()
	return t
}

func (s *Stats) End(t *Task) {
	s.mu.Lock()
	delete(s.tasks, t)
	s.mu.Unlock()
}

func (s *Stats) snapshot() []*Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Task, 0, len(s.tasks))
	for t := range s.tasks {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Seq != out[j].Seq {
			return out[i].Seq < out[j].Seq
		}
		return out[i].Stage < out[j].Stage
	})
	return out
}

// ---------------------------------------------------------------- styling

type palette struct{ reset, bold, dim, cyan, green, yellow, red, blue, magenta string }

func newPalette(on bool) palette {
	if !on {
		return palette{}
	}
	return palette{
		reset: "\x1b[0m", bold: "\x1b[1m", dim: "\x1b[2m",
		cyan: "\x1b[36m", green: "\x1b[32m", yellow: "\x1b[33m",
		red: "\x1b[31m", blue: "\x1b[34m", magenta: "\x1b[35m",
	}
}

// Options configures the UI.
type Options struct {
	Plain   bool // force line mode
	Quiet   bool
	Verbose bool
	NoColor bool
}

// UI is safe for concurrent use.
type UI struct {
	out     io.Writer
	fd      int
	rich    bool
	quiet   bool
	verbose bool
	p       palette

	mu       sync.Mutex
	live     bool
	lines    int
	stats    *Stats
	title    string
	stop     chan struct{}
	stopped  chan struct{}
	samples  []sample
	frame    int
	lastPlain time.Time
	startLive time.Time
}

type sample struct {
	t         time.Time
	raw, wire int64
}

// New creates a UI writing to stderr.
func New(o Options) *UI {
	fd := int(os.Stderr.Fd())
	tty := term.IsTerminal(fd) && os.Getenv("TERM") != "dumb"
	color := tty && !o.NoColor && os.Getenv("NO_COLOR") == ""
	return &UI{
		out: os.Stderr, fd: fd, rich: tty && !o.Plain, quiet: o.Quiet,
		verbose: o.Verbose, p: newPalette(color),
	}
}

func (u *UI) Rich() bool { return u.rich }

func (u *UI) width() int {
	if w, _, err := term.GetSize(u.fd); err == nil && w > 20 {
		return w
	}
	return 100
}

// print writes a log line above the live area.
func (u *UI) print(s string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.live && u.rich {
		u.clearLive()
		fmt.Fprintln(u.out, s)
		u.drawLive()
		return
	}
	fmt.Fprintln(u.out, s)
}

// Banner prints the header line.
func (u *UI) Banner(version, src, dst string) {
	if u.quiet {
		return
	}
	p := u.p
	u.print(fmt.Sprintf("%s%s☁ nimbus%s %s%s%s  %s%s%s %s→%s %s%s%s",
		p.bold, p.cyan, p.reset, p.dim, version, p.reset,
		p.bold, src, p.reset, p.cyan, p.reset, p.bold, dst, p.reset))
}

// Phase announces a new step.
func (u *UI) Phase(format string, a ...any) {
	if u.quiet {
		return
	}
	u.print(fmt.Sprintf("%s›%s %s", u.p.cyan, u.p.reset, fmt.Sprintf(format, a...)))
}

// OK reports a completed step.
func (u *UI) OK(format string, a ...any) {
	if u.quiet {
		return
	}
	u.print(fmt.Sprintf("%s✔%s %s", u.p.green, u.p.reset, fmt.Sprintf(format, a...)))
}

// Info prints secondary information.
func (u *UI) Info(format string, a ...any) {
	if u.quiet {
		return
	}
	u.print(fmt.Sprintf("  %s%s%s", u.p.dim, fmt.Sprintf(format, a...), u.p.reset))
}

// Debug prints only with --verbose.
func (u *UI) Debug(format string, a ...any) {
	if !u.verbose {
		return
	}
	u.print(fmt.Sprintf("  %s· %s%s", u.p.dim, fmt.Sprintf(format, a...), u.p.reset))
}

// Retry reports a retried operation.
func (u *UI) Retry(format string, a ...any) {
	if u.quiet && !u.verbose {
		return
	}
	u.print(fmt.Sprintf("%s↻%s %s", u.p.yellow, u.p.reset, fmt.Sprintf(format, a...)))
}

// Warn reports a non-fatal problem.
func (u *UI) Warn(format string, a ...any) {
	u.print(fmt.Sprintf("%s!%s %s", u.p.yellow, u.p.reset, fmt.Sprintf(format, a...)))
}

// Error reports a failure.
func (u *UI) Error(format string, a ...any) {
	u.print(fmt.Sprintf("%s%s✖ %s%s", u.p.bold, u.p.red, fmt.Sprintf(format, a...), u.p.reset))
}

// ---------------------------------------------------------------- live view

// StartLive begins rendering progress for stats.
func (u *UI) StartLive(title string, s *Stats) {
	u.mu.Lock()
	u.live, u.stats, u.title = true, s, title
	u.stop, u.stopped = make(chan struct{}), make(chan struct{})
	u.startLive = time.Now()
	u.samples = nil
	u.mu.Unlock()
	if u.rich {
		fmt.Fprint(u.out, "\x1b[?25l") // hide cursor
	}
	go func() {
		defer close(u.stopped)
		interval := 120 * time.Millisecond
		if !u.rich {
			interval = time.Second
		}
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-u.stop:
				return
			case <-t.C:
				u.tick()
			}
		}
	}()
}

// StopLive stops rendering and leaves a final progress line.
func (u *UI) StopLive() {
	u.mu.Lock()
	if !u.live {
		u.mu.Unlock()
		return
	}
	close(u.stop)
	u.mu.Unlock()
	<-u.stopped
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.rich {
		u.clearLive()
		fmt.Fprint(u.out, "\x1b[?25h")
	}
	u.live = false
}

func (u *UI) tick() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if !u.live {
		return
	}
	now := time.Now()
	u.samples = append(u.samples, sample{now, u.stats.RawSent.Load(), u.stats.WireBytes.Load()})
	for len(u.samples) > 2 && now.Sub(u.samples[0].t) > 6*time.Second {
		u.samples = u.samples[1:]
	}
	u.frame++
	if u.rich {
		u.clearLive()
		u.drawLive()
		return
	}
	if !u.quiet && now.Sub(u.lastPlain) >= 5*time.Second {
		u.lastPlain = now
		fmt.Fprintln(u.out, u.plainLine())
	}
}

func (u *UI) rates() (raw, wire float64) {
	if len(u.samples) < 2 {
		return 0, 0
	}
	a, b := u.samples[0], u.samples[len(u.samples)-1]
	dt := b.t.Sub(a.t).Seconds()
	if dt <= 0 {
		return 0, 0
	}
	return float64(b.raw-a.raw) / dt, float64(b.wire-a.wire) / dt
}

func (u *UI) eta(rawRate float64) string {
	left := u.stats.RawTotal.Load() - u.stats.RawSent.Load()
	if rawRate <= 1 || left <= 0 {
		return "--:--"
	}
	return Duration(time.Duration(float64(left)/rawRate) * time.Second)
}

func (u *UI) plainLine() string {
	s := u.stats
	tot, done := s.RawTotal.Load(), s.RawSent.Load()
	raw, wire := u.rates()
	return fmt.Sprintf("  %3.0f%%  %s / %s  %s/s (wire %s/s)  batches %d/%d  ETA %s",
		pct(done, tot), Bytes(done), Bytes(tot), Bytes(int64(raw)), Bytes(int64(wire)),
		s.Done.Load(), s.Batches.Load(), u.eta(raw))
}

var spinner = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

func pct(a, b int64) float64 {
	if b <= 0 {
		return 100
	}
	v := float64(a) * 100 / float64(b)
	if v > 100 {
		v = 100
	}
	return v
}

func (u *UI) bar(frac float64, width int, color string) string {
	if width < 1 {
		return ""
	}
	if frac < 0 {
		frac = 0
	}
	if frac > 1 {
		frac = 1
	}
	eighths := []string{"", "▏", "▎", "▍", "▌", "▋", "▊", "▉"}
	full := int(frac * float64(width))
	rem := int((frac*float64(width) - float64(full)) * 8)
	var b strings.Builder
	b.WriteString(color)
	b.WriteString(strings.Repeat("█", full))
	if full < width {
		b.WriteString(eighths[rem])
	}
	b.WriteString(u.p.reset)
	used := full
	if full < width && rem > 0 {
		used++
	}
	b.WriteString(u.p.dim)
	b.WriteString(strings.Repeat("·", width-used))
	b.WriteString(u.p.reset)
	return b.String()
}

func (u *UI) clearLive() {
	if u.lines > 0 {
		fmt.Fprintf(u.out, "\x1b[%dF\x1b[J", u.lines)
		u.lines = 0
	}
}

func truncLeft(s string, n int) string {
	if n <= 1 {
		return ""
	}
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return "…" + string(r[len(r)-n+1:])
}

func (u *UI) drawLive() {
	if u.stats == nil {
		return
	}
	p, s, w := u.p, u.stats, u.width()
	tot, done := s.RawTotal.Load(), s.RawSent.Load()
	raw, wire := u.rates()
	frac := 1.0
	if tot > 0 {
		frac = float64(done) / float64(tot)
	}
	var lines []string

	head := fmt.Sprintf("%s%s%s %s%-12s%s ", p.cyan, spinner[u.frame%len(spinner)], p.reset, p.bold, u.title, p.reset)
	tail := fmt.Sprintf(" %s%5.1f%%%s  %s / %s", p.bold, pct(done, tot), p.reset, Bytes(done), Bytes(tot))
	bw := w - visible(head) - visible(tail) - 1
	if bw > 50 {
		bw = 50
	}
	lines = append(lines, head+u.bar(frac, bw, p.cyan)+tail)

	ratio := ""
	if wb := s.WireBytes.Load(); wb > 0 && done > 0 {
		ratio = fmt.Sprintf("  %s·%s  ratio %.2fx", p.dim, p.reset, float64(done)/float64(wb))
	}
	lines = append(lines, fmt.Sprintf("  %s%s/s%s  %s·%s  wire %s/s  %s·%s  ETA %s%s%s  %s·%s  elapsed %s%s",
		p.bold, Bytes(int64(raw)), p.reset, p.dim, p.reset, Bytes(int64(wire)),
		p.dim, p.reset, p.bold, u.eta(raw), p.reset, p.dim, p.reset,
		Duration(time.Since(u.startLive)), ratio))

	nb := s.Batches.Load()
	stage := func(name string, v int64, c string) string {
		return fmt.Sprintf("%s%s%s %s%d%s/%d", p.dim, name, p.reset, c, v, p.reset, nb)
	}
	extra := ""
	if r := s.Retries.Load(); r > 0 {
		extra += fmt.Sprintf("   %sretries %d%s", p.yellow, r, p.reset)
	}
	if f := s.Failed.Load(); f > 0 {
		extra += fmt.Sprintf("   %sfailed %d%s", p.red, f, p.reset)
	}
	lines = append(lines, fmt.Sprintf("  %s   %s   %s%s",
		stage("packed", s.Packed.Load(), p.blue), stage("sent", s.Sent.Load(), p.magenta),
		stage("done", s.Done.Load(), p.green), extra))

	tasks := s.snapshot()
	_, th, _ := term.GetSize(u.fd)
	maxTasks := 8
	if th > 0 && th-6 < maxTasks {
		maxTasks = th - 6
	}
	for i, t := range tasks {
		if i >= maxTasks {
			lines = append(lines, fmt.Sprintf("  %s… %d more%s", p.dim, len(tasks)-maxTasks, p.reset))
			break
		}
		lines = append(lines, u.taskLine(t, w))
	}
	for _, l := range lines {
		fmt.Fprintln(u.out, l)
	}
	u.lines = len(lines)
}

func (u *UI) taskLine(t *Task, w int) string {
	p := u.p
	color := map[string]string{"pack": p.blue, "send": p.magenta, "extract": p.green, "wait": p.dim}[t.Stage]
	d := t.Done.Load()
	prog := ""
	mini := ""
	if t.Total > 0 && t.Stage == "send" {
		mini = u.bar(float64(d)/float64(t.Total), 12, color) + " "
		el := time.Since(t.Start).Seconds()
		rate := ""
		if el > 0.3 {
			rate = fmt.Sprintf(" %s/s", Bytes(int64(float64(d)/el)))
		}
		prog = fmt.Sprintf("%9s/%-9s%-12s", Bytes(d), Bytes(t.Total), rate)
	} else {
		prog = fmt.Sprintf("%9s %s", Bytes(t.Total), strings.Repeat(" ", 21))
		mini = strings.Repeat(" ", 13)
	}
	head := fmt.Sprintf("  %s▸%s %s#%-4d%s %s%-7s%s %s", color, p.reset, p.dim, t.Seq, p.reset, color, t.Stage, p.reset, mini)
	room := w - visible(head) - visible(prog) - 2
	return head + prog + " " + p.dim + truncLeft(t.Label, room) + p.reset
}

// visible returns the printed width of s, ignoring ANSI escapes.
func visible(s string) int {
	n, esc := 0, false
	for _, r := range s {
		switch {
		case esc:
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				esc = false
			}
		case r == 0x1b:
			esc = true
		default:
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------- summary

// Row is one line of the final summary.
type Row struct{ Key, Value string }

// Summary prints a framed result block.
func (u *UI) Summary(ok bool, title string, rows []Row) {
	if u.quiet && ok {
		return
	}
	p := u.p
	icon, c := "✔", p.green
	if !ok {
		icon, c = "✖", p.red
	}
	var b strings.Builder
	fmt.Fprintf(&b, "\n%s%s%s %s%s\n", p.bold, c, icon, title, p.reset)
	kw := 0
	for _, r := range rows {
		if len(r.Key) > kw {
			kw = len(r.Key)
		}
	}
	for _, r := range rows {
		fmt.Fprintf(&b, "  %s│%s %s%-*s%s  %s\n", c, p.reset, p.dim, kw, r.Key, p.reset, r.Value)
	}
	u.print(strings.TrimSuffix(b.String(), "\n"))
}

// Bytes formats a byte count with binary units.
func Bytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit && exp < 4; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTP"[exp])
}

// Duration formats d as h:mm:ss or m:ss.
func Duration(d time.Duration) string {
	s := int64(d.Round(time.Second).Seconds())
	if s >= 3600 {
		return fmt.Sprintf("%d:%02d:%02d", s/3600, s/60%60, s%60)
	}
	return fmt.Sprintf("%d:%02d", s/60, s%60)
}

// Count formats an integer with thousands separators.
func Count(n int64) string {
	s := fmt.Sprint(n)
	if n < 0 {
		return s
	}
	var out []byte
	for i := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, s[i])
	}
	return string(out)
}
