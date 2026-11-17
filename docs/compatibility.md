# Compatibility

## Local machine

nimbus runs on Linux, macOS and FreeBSD (`amd64`, plus `arm64` on Linux and
macOS). It needs `sh` and `tar` locally as well, because local endpoints use
the same shell agent as remote ones.

## Remote host

Nothing has to be installed on the remote. It needs:

| Required                                    | Optional                     |
| ------------------------------------------- | ---------------------------- |
| a POSIX `sh`                                | `zstd` (fast compression)    |
| `tar` (GNU tar or bsdtar)                   | `pigz` or `gzip` (fallback compression) |
| `find` and `stat` (GNU, BusyBox or BSD)     |                              |
| a sha256 tool: `sha256sum`, `gsha256sum`, `shasum` or `sha256` |                              |
| an SSH server                               |                              |

The login shell can be anything (bash, zsh, fish, csh). nimbus encodes its
arguments so they reach the agent unchanged.

## Test status

| Platform / feature                                              | Status |
| --------------------------------------------------------------- | ------ |
| GNU/Linux (GNU tar, find, coreutils)                            | ✅ tested |
| bsdtar instead of GNU tar                                       | ✅ tested |
| BusyBox (no GNU `find -printf` / `tar --null`)                  | 🟡 code path tested with GNU tools in compatibility mode, not on real BusyBox |
| macOS / FreeBSD (`stat -f`, bsdtar)                             | 🟡 implemented, not yet tested on real systems |
| Built-in SSH: keys, ssh-agent, `known_hosts` (strict, prompt, key-type negotiation) | ✅ tested |
| Built-in SSH: password, key passphrase, keyboard-interactive    | 🟡 implemented, not yet tested |

Reports from macOS, FreeBSD and BusyBox systems are very welcome. See
[CONTRIBUTING](../CONTRIBUTING.md).

## Limitations

- **Ownership** is kept only when the destination side runs as root.
- **Not transferred:** hard links (linked files are copied separately), ACLs,
  extended attributes, and special files (devices, FIFOs, sockets).
- **File names containing newlines** are rejected on BusyBox/BSD remotes,
  which lack GNU `find -printf` / `tar --null`.
- **Timestamps** are compared and set with one-second precision.
- **Remote to remote** is not supported. Run nimbus on one of the two hosts.
- **No delta transfer:** a changed file is sent whole (in `--block`-sized
  parts), not as an rsync-style rolling diff. For small edits to huge files,
  rsync may move fewer bytes.
