package transport

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	sshconfig "github.com/kevinburke/ssh_config"
	"golang.org/x/crypto/ssh"
	sshagent "golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
	"golang.org/x/term"
)

// SSHTarget describes where to connect.
type SSHTarget struct {
	User string // may be empty: resolved from ssh config / current user
	Host string // alias or hostname as written by the user
	Port int    // 0: from ssh config or 22
	Identity string
}

// NativeOptions controls the native client.
type NativeOptions struct {
	Conns       int
	DialTimeout time.Duration
	Keepalive   time.Duration
	Logf        func(format string, args ...any)
}

// Native is a pool of independent SSH connections (separate TCP streams and
// cipher states) authenticated once.
type Native struct {
	target   SSHTarget
	addr     string
	user     string
	cfg      *ssh.ClientConfig
	opts     NativeOptions
	slots    []*slot
	mu       sync.Mutex
	closed   bool
	label    string
}

type slot struct {
	mu     sync.Mutex
	client *ssh.Client
	active int
	dead   bool
}

// authCache keeps secrets typed by the user so reconnects never prompt.
type authCache struct {
	mu        sync.Mutex
	password  string
	havePass  bool
	tty       *os.File
	interactive bool
}

func (a *authCache) getPassword(prompt string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.havePass {
		return a.password, nil
	}
	if !a.interactive || a.tty == nil {
		return "", errors.New("password required but no terminal available")
	}
	fmt.Fprint(a.tty, prompt)
	b, err := term.ReadPassword(int(a.tty.Fd()))
	fmt.Fprintln(a.tty)
	if err != nil {
		return "", err
	}
	a.password, a.havePass = string(b), true
	return a.password, nil
}

// customCfg is loaded from $NIMBUS_SSH_CONFIG when set (used by tests and
// for users who keep their ssh config elsewhere).
var customCfg = func() *sshconfig.Config {
	p := os.Getenv("NIMBUS_SSH_CONFIG")
	if p == "" {
		return nil
	}
	f, err := os.Open(p)
	if err != nil {
		return nil
	}
	defer f.Close()
	c, err := sshconfig.Decode(f)
	if err != nil {
		return nil
	}
	return c
}()

func cfgGet(alias, key string) string {
	if customCfg != nil {
		v, _ := customCfg.Get(alias, key)
		return v
	}
	v, _ := sshconfig.GetStrict(alias, key)
	return v
}

func cfgGetAll(alias, key string) []string {
	if customCfg != nil {
		v, _ := customCfg.GetAll(alias, key)
		return v
	}
	v, _ := sshconfig.GetAllStrict(alias, key)
	return v
}

func expandHome(p string) string {
	if strings.HasPrefix(p, "~/") {
		if h, err := os.UserHomeDir(); err == nil {
			return filepath.Join(h, p[2:])
		}
	}
	return p
}

// NewNative resolves ssh config, authenticates (prompting at most once per
// secret) and opens opts.Conns connections.
func NewNative(ctx context.Context, t SSHTarget, opts NativeOptions) (*Native, error) {
	if opts.Conns < 1 {
		opts.Conns = 1
	}
	if opts.DialTimeout == 0 {
		opts.DialTimeout = 20 * time.Second
	}
	if opts.Keepalive == 0 {
		opts.Keepalive = 15 * time.Second
	}
	if opts.Logf == nil {
		opts.Logf = func(string, ...any) {}
	}
	alias := t.Host
	if pj := cfgGet(alias, "ProxyJump"); pj != "" && !strings.EqualFold(pj, "none") {
		return nil, fmt.Errorf("host %q uses ProxyJump in ssh config, which the native client does not support; use --ssh-cmd ssh", alias)
	}
	if pc := cfgGet(alias, "ProxyCommand"); pc != "" && !strings.EqualFold(pc, "none") {
		return nil, fmt.Errorf("host %q uses ProxyCommand in ssh config, which the native client does not support; use --ssh-cmd ssh", alias)
	}
	hostname := cfgGet(alias, "HostName")
	if hostname == "" {
		hostname = alias
	}
	hostname = strings.ReplaceAll(hostname, "%h", alias)
	port := t.Port
	if port == 0 {
		if p, err := strconv.Atoi(cfgGet(alias, "Port")); err == nil && p > 0 {
			port = p
		} else {
			port = 22
		}
	}
	usr := t.User
	if usr == "" {
		usr = cfgGet(alias, "User")
	}
	if usr == "" {
		if u, err := user.Current(); err == nil {
			usr = u.Username
		}
	}
	addr := net.JoinHostPort(hostname, strconv.Itoa(port))

	hostKeyCB, algos, err := hostKeyCallback(alias, hostname, port)
	if err != nil {
		return nil, err
	}

	cache := &authCache{}
	if f, err := os.OpenFile("/dev/tty", os.O_RDWR, 0); err == nil {
		cache.tty = f
		cache.interactive = true
	}

	var auths []ssh.AuthMethod
	var signers []ssh.Signer
	if sock := os.Getenv("SSH_AUTH_SOCK"); sock != "" && !strings.EqualFold(cfgGet(alias, "IdentityAgent"), "none") {
		if c, err := net.Dial("unix", sock); err == nil {
			if ss, err := sshagent.NewClient(c).Signers(); err == nil {
				signers = append(signers, ss...)
			}
		}
	}
	idFiles := []string{}
	if t.Identity != "" {
		idFiles = append(idFiles, t.Identity)
	}
	{
		for _, f := range cfgGetAll(alias, "IdentityFile") {
			if f != "" && f != "~/.ssh/identity" {
				idFiles = append(idFiles, f)
			}
		}
	}
	if len(idFiles) == 0 || t.Identity == "" {
		idFiles = append(idFiles, "~/.ssh/id_ed25519", "~/.ssh/id_ecdsa", "~/.ssh/id_rsa")
	}
	seen := map[string]bool{}
	for _, f := range idFiles {
		f = expandHome(strings.ReplaceAll(f, "%d", "~"))
		f = expandHome(f)
		if seen[f] {
			continue
		}
		seen[f] = true
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		s, err := ssh.ParsePrivateKey(data)
		var missing *ssh.PassphraseMissingError
		if errors.As(err, &missing) {
			if !cache.interactive {
				continue
			}
			// Skip prompting when the agent already holds this key.
			if missing.PublicKey != nil && hasSigner(signers, missing.PublicKey) {
				continue
			}
			fmt.Fprintf(cache.tty, "Enter passphrase for key '%s': ", f)
			pass, perr := term.ReadPassword(int(cache.tty.Fd()))
			fmt.Fprintln(cache.tty)
			if perr != nil {
				continue
			}
			s, err = ssh.ParsePrivateKeyWithPassphrase(data, pass)
		}
		if err == nil && s != nil {
			signers = append(signers, s)
		}
	}
	if len(signers) > 0 {
		auths = append(auths, ssh.PublicKeys(signers...))
	}
	prompt := fmt.Sprintf("%s@%s's password: ", usr, hostname)
	auths = append(auths,
		ssh.KeyboardInteractive(func(name, instruction string, questions []string, echos []bool) ([]string, error) {
			ans := make([]string, len(questions))
			for i, q := range questions {
				if echos[i] {
					return nil, errors.New("interactive prompt not supported: " + q)
				}
				p, err := cache.getPassword(prompt)
				if err != nil {
					return nil, err
				}
				ans[i] = p
			}
			return ans, nil
		}),
		ssh.PasswordCallback(func() (string, error) { return cache.getPassword(prompt) }),
	)

	cfg := &ssh.ClientConfig{
		User:              usr,
		Auth:              auths,
		HostKeyCallback:   hostKeyCB,
		HostKeyAlgorithms: algos,
		Timeout:           opts.DialTimeout,
	}
	n := &Native{target: t, addr: addr, user: usr, cfg: cfg, opts: opts, label: usr + "@" + alias}
	n.slots = make([]*slot, opts.Conns)
	for i := range n.slots {
		n.slots[i] = &slot{dead: true}
	}
	// First connection: may prompt. The others reuse cached secrets in parallel.
	if err := n.connect(ctx, n.slots[0]); err != nil {
		return nil, fmt.Errorf("ssh %s: %w", addr, err)
	}
	cache.interactive = false // never prompt again (UI may be running)
	var wg sync.WaitGroup
	for _, s := range n.slots[1:] {
		wg.Add(1)
		go func(s *slot) {
			defer wg.Done()
			if err := n.connect(ctx, s); err != nil {
				opts.Logf("extra ssh connection failed: %v", err)
			}
		}(s)
	}
	wg.Wait()
	return n, nil
}

func hasSigner(ss []ssh.Signer, pk ssh.PublicKey) bool {
	m := pk.Marshal()
	for _, s := range ss {
		if string(s.PublicKey().Marshal()) == string(m) {
			return true
		}
	}
	return false
}

func knownHostsFiles(alias string) []string {
	var files []string
	if v := cfgGet(alias, "UserKnownHostsFile"); v != "" {
		for _, f := range strings.Fields(v) {
			files = append(files, expandHome(f))
		}
	} else if h, err := os.UserHomeDir(); err == nil {
		files = append(files, filepath.Join(h, ".ssh", "known_hosts"))
	}
	if v := cfgGet(alias, "GlobalKnownHostsFile"); v != "" {
		files = append(files, strings.Fields(v)...)
	} else {
		files = append(files, "/etc/ssh/ssh_known_hosts")
	}
	return files
}

// hostKeyCallback performs strict known_hosts verification, asking the user to
// trust unknown hosts (like ssh's StrictHostKeyChecking=ask). It also returns
// the host key algorithms already known for the host so the server offers a
// key type we can verify.
func hostKeyCallback(alias, hostname string, port int) (ssh.HostKeyCallback, []string, error) {
	files := knownHostsFiles(alias)
	var existing []string
	for _, f := range files {
		if _, err := os.Stat(f); err == nil {
			existing = append(existing, f)
		}
	}
	userFile := files[0]
	if len(existing) == 0 {
		_ = os.MkdirAll(filepath.Dir(userFile), 0o700)
		if f, err := os.OpenFile(userFile, os.O_CREATE|os.O_WRONLY, 0o600); err == nil {
			f.Close()
			existing = append(existing, userFile)
		}
	}
	kh, err := knownhosts.New(existing...)
	if err != nil {
		return nil, nil, fmt.Errorf("known_hosts: %w", err)
	}
	keyAlias := hostname
	if alias != hostname {
		// ssh uses HostKeyAlias or the HostName for lookups.
		if a := cfgGet(alias, "HostKeyAlias"); a != "" {
			keyAlias = a
		}
	}
	lookup := net.JoinHostPort(keyAlias, strconv.Itoa(port))

	var algos []string
	if pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl")); err == nil {
		var ke *knownhosts.KeyError
		if e := kh(lookup, &net.TCPAddr{IP: net.IPv4zero, Port: port}, pub); errors.As(e, &ke) {
			for _, k := range ke.Want {
				algos = append(algos, expandAlgo(k.Key.Type())...)
			}
		}
	}
	algos = dedup(algos)

	var mu sync.Mutex
	cb := func(_ string, remote net.Addr, key ssh.PublicKey) error {
		err := kh(lookup, remote, key)
		var ke *knownhosts.KeyError
		if err == nil || !errors.As(err, &ke) {
			return err
		}
		if len(ke.Want) > 0 {
			return fmt.Errorf("HOST KEY MISMATCH for %s (got %s %s): possible man-in-the-middle attack; fix %s",
				lookup, key.Type(), ssh.FingerprintSHA256(key), ke.Want[0].Filename)
		}
		mu.Lock()
		defer mu.Unlock()
		tty, terr := os.OpenFile("/dev/tty", os.O_RDWR, 0)
		if terr != nil {
			return fmt.Errorf("unknown host %s (%s %s) and no terminal to confirm", lookup, key.Type(), ssh.FingerprintSHA256(key))
		}
		defer tty.Close()
		fmt.Fprintf(tty, "The authenticity of host '%s' can't be established.\n%s key fingerprint is %s.\nAre you sure you want to continue connecting (yes/no)? ",
			lookup, key.Type(), ssh.FingerprintSHA256(key))
		ans, _ := bufio.NewReader(tty).ReadString('\n')
		if strings.TrimSpace(strings.ToLower(ans)) != "yes" {
			return errors.New("host key verification failed")
		}
		f, ferr := os.OpenFile(userFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
		if ferr != nil {
			return ferr
		}
		defer f.Close()
		_, ferr = fmt.Fprintln(f, knownhosts.Line([]string{knownhosts.Normalize(lookup)}, key))
		if ferr == nil {
			// Accept this key for the remaining connections of this run.
			kh2, e2 := knownhosts.New(existing...)
			if e2 == nil {
				kh = kh2
			}
		}
		return ferr
	}
	return cb, algos, nil
}

func expandAlgo(t string) []string {
	if t == ssh.KeyAlgoRSA {
		return []string{ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256, ssh.KeyAlgoRSA}
	}
	return []string{t}
}

func dedup(s []string) []string {
	seen := map[string]bool{}
	out := s[:0]
	for _, v := range s {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

func (n *Native) connect(ctx context.Context, s *slot) error {
	d := net.Dialer{Timeout: n.opts.DialTimeout, KeepAlive: 30 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", n.addr)
	if err != nil {
		return err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetReadBuffer(4 << 20)
		_ = tc.SetWriteBuffer(4 << 20)
	}
	_ = conn.SetDeadline(time.Now().Add(n.opts.DialTimeout + 60*time.Second))
	c, chans, reqs, err := ssh.NewClientConn(conn, n.addr, n.cfg)
	if err != nil {
		conn.Close()
		return err
	}
	_ = conn.SetDeadline(time.Time{})
	client := ssh.NewClient(c, chans, reqs)
	s.mu.Lock()
	if s.client != nil {
		s.client.Close()
	}
	s.client, s.dead = client, false
	s.mu.Unlock()
	go n.keepalive(s, client)
	return nil
}

func (n *Native) keepalive(s *slot, c *ssh.Client) {
	t := time.NewTicker(n.opts.Keepalive)
	defer t.Stop()
	done := make(chan struct{})
	go func() { _ = c.Wait(); close(done) }()
	for {
		select {
		case <-done:
			n.markDead(s, c)
			return
		case <-t.C:
			res := make(chan error, 1)
			go func() { _, _, err := c.SendRequest("keepalive@openssh.com", true, nil); res <- err }()
			select {
			case err := <-res:
				if err != nil {
					n.markDead(s, c)
					c.Close()
					return
				}
			case <-time.After(3 * n.opts.Keepalive):
				n.markDead(s, c)
				c.Close()
				return
			}
		}
	}
}

func (n *Native) markDead(s *slot, c *ssh.Client) {
	s.mu.Lock()
	if s.client == c {
		s.dead = true
	}
	s.mu.Unlock()
}

// pick returns the live slot with the fewest active sessions.
func (n *Native) pick() *slot {
	var best *slot
	for _, s := range n.slots {
		s.mu.Lock()
		ok := !s.dead && s.client != nil
		a := s.active
		s.mu.Unlock()
		if ok && (best == nil || a < best.active) {
			best = s
		}
	}
	return best
}

func (n *Native) Name() string  { return n.label }
func (n *Native) IsLocal() bool { return false }

// Reconnect re-establishes every dead slot using cached credentials.
func (n *Native) Reconnect(ctx context.Context) error {
	var firstErr error
	alive := 0
	for _, s := range n.slots {
		s.mu.Lock()
		dead := s.dead || s.client == nil
		s.mu.Unlock()
		if !dead {
			alive++
			continue
		}
		if err := n.connect(ctx, s); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		alive++
	}
	if alive == 0 {
		return fmt.Errorf("%w: %v", ErrConn, firstErr)
	}
	return nil
}

func (n *Native) Run(ctx context.Context, argv []string, stdin io.Reader, stdout, stderr io.Writer) error {
	s := n.pick()
	if s == nil {
		return fmt.Errorf("%w: no live ssh connection", ErrConn)
	}
	s.mu.Lock()
	client := s.client
	s.active++
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.active--; s.mu.Unlock() }()

	sess, err := client.NewSession()
	if err != nil {
		n.markDead(s, client)
		return fmt.Errorf("%w: %v", ErrConn, err)
	}
	defer sess.Close()
	sess.Stdin = stdin
	sess.Stdout = stdout
	var eb tailBuffer
	sess.Stderr = teeErr(stderr, &eb)

	done := make(chan error, 1)
	go func() { done <- sess.Run(ShellJoin(argv)) }()
	select {
	case <-ctx.Done():
		_ = sess.Signal(ssh.SIGKILL)
		sess.Close()
		<-done
		return ctx.Err()
	case err = <-done:
	}
	if err == nil {
		return nil
	}
	var ee *ssh.ExitError
	if errors.As(err, &ee) {
		return &ExitError{Code: ee.ExitStatus(), Stderr: eb.String()}
	}
	n.markDead(s, client)
	return fmt.Errorf("%w: %v", ErrConn, err)
}

func (n *Native) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil
	}
	n.closed = true
	for _, s := range n.slots {
		s.mu.Lock()
		if s.client != nil {
			s.client.Close()
		}
		s.mu.Unlock()
	}
	return nil
}
