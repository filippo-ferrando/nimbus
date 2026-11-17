# Installation

nimbus is a single static binary. It only has to be installed on the machine
you run it from. The remote host needs no nimbus, only standard shell tools
(see [Compatibility](compatibility.md)).

## Release binaries

Each tagged release ships binaries for:

| OS      | Architectures      |
| ------- | ------------------ |
| Linux   | `amd64`, `arm64`   |
| macOS   | `amd64`, `arm64`   |
| FreeBSD | `amd64`            |

Download from the
[releases page](https://git.fferrando.cc/rdfilippo/nimbus/releases). Assets are
named `nimbus-<tag>-<os>-<arch>`:

```sh
VERSION=v0.2.0
TARGET=linux-amd64          # or linux-arm64, darwin-amd64, darwin-arm64, freebsd-amd64
BASE=https://git.fferrando.cc/rdfilippo/nimbus/releases/download/$VERSION

curl -fLO "$BASE/nimbus-$VERSION-$TARGET"
curl -fLO "$BASE/SHA256SUMS"
```

### Verify the download

Every release has a `SHA256SUMS` file:

```sh
sha256sum --ignore-missing -c SHA256SUMS      # Linux
shasum -a 256 --ignore-missing -c SHA256SUMS  # macOS / FreeBSD
```

### Install

```sh
chmod +x "nimbus-$VERSION-$TARGET"
sudo mv "nimbus-$VERSION-$TARGET" /usr/local/bin/nimbus
nimbus --version
```

> **macOS:** binaries downloaded with a browser get a quarantine flag. Clear it
> with `xattr -d com.apple.quarantine /usr/local/bin/nimbus`. Files fetched
> with `curl` do not have it.

## From source

You need Go 1.27 or newer.

```sh
go install git.fferrando.cc/rdfilippo/nimbus@latest
```

Or from a clone, with the version string embedded the way releases do it:

```sh
git clone https://git.fferrando.cc/rdfilippo/nimbus.git
cd nimbus
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$(git describe --tags)" -o nimbus .
```

Cross-compiling works as usual: `GOOS=darwin GOARCH=arm64 go build ...`.

## Optional tools

nimbus runs without these, but uses them when they are present:

| Tool                | Where             | Used for                                          |
| ------------------- | ----------------- | ------------------------------------------------- |
| `zstd`              | both ends         | fast multi-threaded compression (`--compress auto`) |
| `pigz`              | both ends         | parallel gzip when zstd is missing                |
| `ssh`               | local             | `ProxyJump`/`ProxyCommand` hosts and `--ssh-cmd`  |

## Uninstall

```sh
sudo rm /usr/local/bin/nimbus
```

nimbus keeps no configuration. Interrupted runs can leave staged data in
`/var/tmp/nimbus-*` (or your `--staging` directory) on either end. Delete it
there, or finish the run so nimbus cleans up after itself.
