// Package agent embeds the portable shell agent executed on every endpoint.
package agent

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"strconv"
	"strings"
)

//go:embed agent.sh
var Script []byte

// Version is a short content hash of the script, used to name the uploaded file
// so that a stale agent from an older nimbus is never reused.
var Version = func() string {
	h := sha256.Sum256(Script)
	return hex.EncodeToString(h[:6])
}()

// Arg encodes an argument so that it survives any remote login shell
// (sh, bash, zsh, fish, csh...): safe strings are passed as-is, anything else
// is octal-encoded and decoded by the agent.
func Arg(s string) string {
	if s != "" && isSafe(s) && s[0] != 'o' && s[0] != 'r' {
		return s
	}
	if s != "" && isSafe(s) {
		return "r" + s
	}
	var b strings.Builder
	b.Grow(1 + 3*len(s))
	b.WriteByte('o')
	for i := 0; i < len(s); i++ {
		o := strconv.FormatInt(int64(s[i]), 8)
		b.WriteString(strings.Repeat("0", 3-len(o)))
		b.WriteString(o)
	}
	return b.String()
}

func isSafe(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '_', c == '-', c == '/', c == '=', c == '+', c == ',':
		default:
			return false
		}
	}
	return true
}

// SafeWord reports whether s can be placed on a remote command line unquoted.
func SafeWord(s string) bool { return s != "" && isSafe(s) }
