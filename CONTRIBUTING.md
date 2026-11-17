# Contributing to nimbus

Thanks for your interest in nimbus! Bug reports, test results from platforms
we have not tried yet, documentation fixes and code are all welcome.

## Reporting bugs

Open an issue on the
[issue tracker](https://git.fferrando.cc/rdfilippo/nimbus/issues) and include:

- the output of `nimbus --version`,
- the operating system on **both** ends, and whether the remote uses GNU,
  BusyBox or BSD tools,
- the exact command line,
- the output of a run with `--plain -v`,
- the exit code.

Results from **macOS, FreeBSD and BusyBox** systems are particularly useful,
even when everything works. See [Compatibility](docs/compatibility.md) for
what is still untested.

## Development setup

You need:

- Go 1.27 or newer
- for the end-to-end suite: `sshd`, `ssh`, `ssh-keygen`, `tar`, `zstd` and GNU
  find/coreutils. `bsdtar` and `rsync` are optional (bsdtar tests, benchmark).

```sh
git clone https://git.fferrando.cc/rdfilippo/nimbus.git
cd nimbus
go build -o nimbus .
./nimbus --help
```

## Project layout

```
main.go                     CLI: flags, validation, wiring
internal/engine/            list, diff, plan batches, run the pipeline
internal/agent/agent.sh     portable shell agent run on every endpoint
internal/agent/agent.go     embeds the agent, encodes its arguments
internal/transport/         local, built-in SSH and system ssh transports
internal/retry/             exponential backoff with full jitter
internal/ui/                live terminal view and plain output
test/e2e.sh                 end-to-end suite
test/proxy/                 per-connection rate-limiting proxy for benchmarks
```

[How it works](docs/architecture.md) explains how these pieces fit together.

## Running the tests

```sh
go vet ./...
go test ./...            # unit tests, including the agent's BusyBox/BSD code path
```

The end-to-end suite builds nimbus, starts an unprivileged throwaway `sshd` on
`127.0.0.1` (with temporary keys, so nothing touches `~/.ssh`), generates test
trees with awkward names, long paths, symlinks and permissions, and checks the
results:

```sh
test/e2e.sh              # everything
test/e2e.sh resume       # only some groups:
                         #   local push pull sshcmd delete rename compat
                         #   checksum errors resume network bench
test/e2e.sh -k           # keep the work directory for inspection
test/e2e.sh -b           # also run the benchmark against rsync
```

It needs about 4 GB in `$NIMBUS_E2E_DIR` (default `/var/tmp`, because `/tmp` is
often a small tmpfs) and removes everything when it finishes.

CI ([`.forgejo/workflows/ci.yml`](.forgejo/workflows/ci.yml)) runs `gofmt`,
`go vet`, a `go mod tidy` check, `go test -race`, cross-compilation for every
release target and the full end-to-end suite on every push and pull request.
Please run `go test ./...` and the relevant e2e groups before opening a pull
request. Changes to `agent.sh` or the engine should pass the full suite.

## Guidelines

- **Keep the remote side tool-free.** `agent.sh` must stay POSIX `sh` and work
  with GNU, BusyBox and BSD/macOS tools. If you need a non-portable flag, add a
  fallback and cover it with a test.
- **Every step must be resumable.** Any operation can be interrupted at any
  point and run again. Make new steps idempotent.
- **Retry only transient errors.** Connection problems, stalls and checksum
  mismatches are retried. Everything else should fail fast with a clear
  message (wrap it with `retry.Permanent`).
- **Follow rsync** where nimbus overlaps with it (flags, path rules, exit
  codes), unless there is a good reason not to.
- Format with `gofmt` and keep `go vet` clean.
- Update the docs and `CHANGELOG.md` (under *Unreleased*) for user-visible
  changes.

## Commit messages

We use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: --exclude, single-file rename rule, --ssh-config
fix: idempotent extracts, stale markers, fast abort on permanent errors
docs: ...
test: ...
ci: ...
```

Explain *why* in the body when it is not obvious. Keep one logical change per
commit.

## Releases

Releases are cut by pushing a `v*` tag. The
[release workflow](.forgejo/workflows/release.yml) runs the unit tests, builds
static binaries for every platform and publishes them with a `SHA256SUMS`
file. The release notes are built from the tag's section of `CHANGELOG.md`, so
move the *Unreleased* entries under a new `## [x.y.z] - date` heading before
tagging. The notes also list the checksum of every binary. Re-running the
workflow for an existing tag rebuilds the binaries, rewrites the notes and
replaces all assets.

## License

By contributing, you agree that your contributions will be licensed under the
[GNU General Public License, version 3 only](LICENSE) (`GPL-3.0-only`).
