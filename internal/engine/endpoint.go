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
	"strings"

	"git.fferrando.cc/rdfilippo/nimbus/internal/agent"
	"git.fferrando.cc/rdfilippo/nimbus/internal/transport"
)

// Endpoint is one side of a transfer.
type Endpoint struct {
	Spec  Spec
	T     transport.Transport
	Root  string // directory all entry paths are relative to
	Sub   string // top-level entry being transferred ("" = whole root)
	Stage string // staging directory (absolute, shell-safe)
	Info  map[string]string
}

func (ep *Endpoint) argv(cmd string, args ...string) []string {
	return append([]string{"sh", ep.Stage + "/agent.sh", cmd}, args...)
}

func (ep *Endpoint) call(ctx context.Context, cmd string, args []string, stdin io.Reader, stdout io.Writer) error {
	return ep.T.Run(ctx, ep.argv(cmd, args...), stdin, stdout, nil)
}

func (ep *Endpoint) callOut(ctx context.Context, cmd string, args []string, stdin io.Reader) ([]byte, error) {
	var out bytes.Buffer
	err := ep.call(ctx, cmd, args, stdin, &out)
	return out.Bytes(), err
}

// Name is a short label for messages.
func (ep *Endpoint) Name() string {
	if ep.T.IsLocal() {
		return "local"
	}
	return ep.Spec.Dest()
}

// Setup creates the staging directory, uploads the agent and probes the
// environment.
func (ep *Endpoint) Setup(ctx context.Context) error {
	if !agent.SafeWord(ep.Stage) || !strings.HasPrefix(ep.Stage, "/") {
		return fmt.Errorf("staging path %q must be absolute and contain only [A-Za-z0-9._/-]", ep.Stage)
	}
	if ep.T.IsLocal() {
		if err := os.MkdirAll(ep.Stage, 0o700); err != nil {
			return err
		}
		tmp := filepath.Join(ep.Stage, "agent.sh.tmp")
		if err := os.WriteFile(tmp, agent.Script, 0o600); err != nil {
			return err
		}
		if err := os.Rename(tmp, filepath.Join(ep.Stage, "agent.sh")); err != nil {
			return err
		}
	} else {
		steps := []struct {
			argv  []string
			stdin io.Reader
		}{
			{[]string{"mkdir", "-p", ep.Stage}, nil},
			{[]string{"chmod", "700", ep.Stage}, nil},
			{[]string{"cat", ">", ep.Stage + "/agent.sh.tmp"}, bytes.NewReader(agent.Script)},
			{[]string{"mv", "-f", ep.Stage + "/agent.sh.tmp", ep.Stage + "/agent.sh"}, nil},
		}
		for _, s := range steps {
			if err := ep.T.Run(ctx, s.argv, s.stdin, nil, nil); err != nil {
				return fmt.Errorf("%s: prepare staging %s: %w", ep.Name(), ep.Stage, err)
			}
		}
	}
	out, err := ep.callOut(ctx, "probe", nil, nil)
	if err != nil {
		return fmt.Errorf("%s: environment check failed: %w", ep.Name(), err)
	}
	ep.Info = map[string]string{}
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		if k, v, ok := strings.Cut(sc.Text(), "="); ok {
			ep.Info[k] = v
		}
	}
	return nil
}

// HasComp reports whether a compressor family is usable on this endpoint.
func (ep *Endpoint) HasComp(c string) bool {
	switch c {
	case "none":
		return true
	case "zstd":
		return ep.Info["zstd"] == "1"
	case "gzip":
		return ep.Info["gzip"] == "1" || ep.Info["pigz"] == "1"
	}
	return false
}

// List returns the entries under Root/Sub.
func (ep *Endpoint) List(ctx context.Context) ([]Entry, []string, error) {
	if ep.T.IsLocal() {
		return ListLocal(ep.Root, ep.Sub)
	}
	out, err := ep.callOut(ctx, "list", []string{agent.Arg(ep.Root), agent.Arg(ep.Sub)}, nil)
	if err != nil {
		if transport.ExitCode(err) == 4 {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}
	return ParseListing(out)
}

// Hash returns sha256 sums for paths (relative to Root), in order.
func (ep *Endpoint) Hash(ctx context.Context, paths []string) ([]string, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	out, err := ep.callOut(ctx, "hash", []string{agent.Arg(ep.Root)}, nulList(paths))
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimRight(string(out), "\n"), "\n")
	if len(lines) != len(paths) {
		return nil, fmt.Errorf("%s: hash returned %d sums for %d files", ep.Name(), len(lines), len(paths))
	}
	sums := make([]string, len(lines))
	for i, l := range lines {
		f := strings.Fields(strings.TrimPrefix(l, "\\"))
		if len(f) == 0 {
			return nil, errors.New("malformed hash output")
		}
		sums[i] = f[0]
	}
	return sums, nil
}

// rel turns an entry path into an agent path ("./x", "." for the root).
func rel(p string) string {
	if p == "" {
		return "."
	}
	return "./" + p
}

func nulList(paths []string) io.Reader {
	var b bytes.Buffer
	for _, p := range paths {
		b.WriteString(rel(p))
		b.WriteByte(0)
	}
	return &b
}

func nulFields(fields ...string) []byte {
	var b bytes.Buffer
	for _, f := range fields {
		b.WriteString(f)
		b.WriteByte(0)
	}
	return b.Bytes()
}
