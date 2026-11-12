package transport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"
)

// System drives the user's ssh binary (full ~/.ssh/config support:
// ProxyJump, ProxyCommand, certificates, ...). Each of the Conns connections
// is an independent ControlMaster, so parallel jobs really use separate TCP
// streams.
type System struct {
	cmd     []string // e.g. ["ssh", "-p", "2222"]
	dest    string   // [user@]host
	sockets []string
	next    atomic.Uint64
	label   string
}

// SystemOptions controls the system ssh transport.
type SystemOptions struct {
	Conns int
	ID    string // transfer id, used to name control sockets
	Port  int
	Identity string
	Logf  func(format string, args ...any)
}

func socketDir() string {
	if d := os.Getenv("XDG_RUNTIME_DIR"); d != "" {
		return d
	}
	return os.TempDir()
}

// BatchModeWorks reports whether ssh can log in without prompting.
func BatchModeWorks(ctx context.Context, cmd []string, dest string) bool {
	args := append(append([]string{}, cmd[1:]...), "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", dest, "true")
	c := exec.CommandContext(ctx, cmd[0], args...)
	return c.Run() == nil
}

// NewSystem opens opts.Conns control masters (each may prompt for a password
// when key authentication is not available).
func NewSystem(ctx context.Context, cmd []string, dest string, opts SystemOptions) (*System, error) {
	if len(cmd) == 0 {
		cmd = []string{"ssh"}
	}
	if opts.Port > 0 {
		cmd = append(cmd, "-p", strconv.Itoa(opts.Port))
	}
	if opts.Identity != "" {
		cmd = append(cmd, "-i", opts.Identity)
	}
	if opts.Conns < 1 {
		opts.Conns = 1
	}
	s := &System{cmd: cmd, dest: dest, label: dest + " (ssh)"}
	h := sha256.Sum256([]byte(opts.ID + dest + strconv.Itoa(os.Getpid())))
	tag := hex.EncodeToString(h[:4])
	for i := 0; i < opts.Conns; i++ {
		sock := filepath.Join(socketDir(), fmt.Sprintf("nimbus-%s-%d.sock", tag, i))
		s.sockets = append(s.sockets, sock)
		if err := s.master(ctx, sock); err != nil {
			s.Close()
			return nil, fmt.Errorf("ssh %s: %w", dest, err)
		}
	}
	return s, nil
}

func (s *System) baseArgs(sock string) []string {
	args := append([]string{}, s.cmd[1:]...)
	return append(args,
		"-o", "ControlPath="+sock,
		"-o", "ServerAliveInterval=15",
		"-o", "ServerAliveCountMax=3",
	)
}

func (s *System) master(ctx context.Context, sock string) error {
	args := append(s.baseArgs(sock),
		"-o", "ControlMaster=yes", "-o", "ControlPersist=300", "-o", "ConnectTimeout=20",
		"-f", "-N", s.dest)
	c := exec.CommandContext(ctx, s.cmd[0], args...)
	c.Stdin, c.Stdout, c.Stderr = os.Stdin, os.Stderr, os.Stderr
	return c.Run()
}

func (s *System) alive(sock string) bool {
	args := append(s.baseArgs(sock), "-O", "check", s.dest)
	return exec.Command(s.cmd[0], args...).Run() == nil
}

func (s *System) Name() string  { return s.label }
func (s *System) IsLocal() bool { return false }

// Reconnect restarts dead masters. With password authentication this would
// prompt, so it only uses BatchMode.
func (s *System) Reconnect(ctx context.Context) error {
	alive := 0
	var firstErr error
	for _, sock := range s.sockets {
		if s.alive(sock) {
			alive++
			continue
		}
		_ = os.Remove(sock)
		args := append(s.baseArgs(sock),
			"-o", "ControlMaster=yes", "-o", "ControlPersist=300", "-o", "BatchMode=yes",
			"-o", "ConnectTimeout=20", "-f", "-N", s.dest)
		if err := exec.CommandContext(ctx, s.cmd[0], args...).Run(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		alive++
	}
	if alive == 0 {
		return fmt.Errorf("%w: reconnect failed: %v", ErrConn, firstErr)
	}
	return nil
}

func (s *System) Run(ctx context.Context, argv []string, stdin io.Reader, stdout, stderr io.Writer) error {
	i := int(s.next.Add(1)) % len(s.sockets)
	sock := s.sockets[i]
	args := append(s.baseArgs(sock), "-o", "ControlMaster=no", "-o", "BatchMode=yes", "-T", s.dest, "--", ShellJoin(argv))
	c := exec.CommandContext(ctx, s.cmd[0], args...)
	c.Stdin, c.Stdout = stdin, stdout
	var eb tailBuffer
	c.Stderr = teeErr(stderr, &eb)
	c.WaitDelay = 5 * time.Second
	err := c.Run()
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		// ssh itself exits 255 on connection problems.
		if ee.ExitCode() == 255 {
			return fmt.Errorf("%w: %s", ErrConn, lastLine(eb.String()))
		}
		return &ExitError{Code: ee.ExitCode(), Stderr: eb.String()}
	}
	return fmt.Errorf("%w: %v", ErrConn, err)
}

func (s *System) Close() error {
	for _, sock := range s.sockets {
		args := append(s.baseArgs(sock), "-O", "exit", s.dest)
		c := exec.Command(s.cmd[0], args...)
		_ = c.Run()
	}
	return nil
}
