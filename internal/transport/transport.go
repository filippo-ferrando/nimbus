// Package transport runs agent commands on an endpoint: the local machine,
// a remote host through the native Go SSH client, or a remote host through the
// system ssh binary.
package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

// Transport executes argv on an endpoint. argv words must already be safe
// for a remote shell (see agent.Arg).
type Transport interface {
	Run(ctx context.Context, argv []string, stdin io.Reader, stdout, stderr io.Writer) error
	// Healthy reports false when connections are known to be broken; Reconnect
	// tries to restore them.
	Reconnect(ctx context.Context) error
	Name() string
	IsLocal() bool
	Close() error
}

// ExitError is returned when the remote command ran and exited non-zero.
type ExitError struct {
	Code   int
	Stderr string
}

func (e *ExitError) Error() string {
	msg := strings.TrimSpace(e.Stderr)
	if msg == "" {
		return fmt.Sprintf("exit status %d", e.Code)
	}
	return fmt.Sprintf("exit status %d: %s", e.Code, lastLine(msg))
}

func lastLine(s string) string {
	if i := strings.LastIndexByte(s, '\n'); i >= 0 {
		return s[i+1:]
	}
	return s
}

// ErrConn marks a failure of the connection itself (as opposed to the command).
var ErrConn = errors.New("connection error")

// IsConnErr reports whether err was caused by the transport, not the command.
func IsConnErr(err error) bool { return errors.Is(err, ErrConn) }

// ExitCode returns the remote exit code, or -1.
func ExitCode(err error) int {
	var e *ExitError
	if errors.As(err, &e) {
		return e.Code
	}
	return -1
}

// ---------------------------------------------------------------- local

type Local struct{}

func (Local) Name() string                        { return "local" }
func (Local) IsLocal() bool                       { return true }
func (Local) Close() error                        { return nil }
func (Local) Reconnect(ctx context.Context) error { return nil }

func (Local) Run(ctx context.Context, argv []string, stdin io.Reader, stdout, stderr io.Writer) error {
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	var eb tailBuffer
	cmd.Stderr = teeErr(stderr, &eb)
	err := cmd.Run()
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return &ExitError{Code: ee.ExitCode(), Stderr: eb.String()}
	}
	return err
}

// tailBuffer keeps the last 4 KiB written to it.
type tailBuffer struct{ b []byte }

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.b = append(t.b, p...)
	if len(t.b) > 4096 {
		t.b = t.b[len(t.b)-4096:]
	}
	return len(p), nil
}
func (t *tailBuffer) String() string { return string(t.b) }

func teeErr(w io.Writer, t *tailBuffer) io.Writer {
	if w == nil {
		return t
	}
	return io.MultiWriter(w, t)
}

// ShellJoin joins safe words into a remote command line. A leading
// "exec" avoids leaving an extra login shell around.
func ShellJoin(argv []string) string { return strings.Join(argv, " ") }
