# SSH

nimbus has two ways of reaching a remote host:

| Client | Used when | Strengths |
| ------ | --------- | --------- |
| **Built-in** (Go) | by default | One authentication shared by every connection, so you get at most one prompt. Fast to set up. |
| **System `ssh`**  | `--ssh-cmd`, or the host uses `ProxyJump` / `ProxyCommand` | Full OpenSSH feature set: `Match`, certificates, any option your `ssh` supports. |

Either way, nimbus opens `--conns` connections (one per job by default), and
each one is a separate TCP stream with its own cipher. That is where the speed
on throttled links comes from.

## Built-in client

### Configuration

The client reads `~/.ssh/config`, or the file given with `-F` /
`--ssh-config`, or `$NIMBUS_SSH_CONFIG`. It understands:

| Option                | Notes |
| --------------------- | ----- |
| `HostName`            | |
| `User`                | defaults to the local user |
| `Port`                | `--port` takes precedence |
| `IdentityFile`        | `--identity` is tried first |
| `IdentityAgent none`  | disables ssh-agent for the host |
| `UserKnownHostsFile`  | |
| `HostKeyAlias`        | replaces host and port in the `known_hosts` lookup, as in OpenSSH |
| `ProxyJump`, `ProxyCommand` | not handled by the built-in client: the host is handed to the system `ssh` automatically |

`Match` blocks are ignored. If you depend on them, or on any other option not
listed, pass `--ssh-cmd ssh`.

### Authentication

Methods are tried in this order:

1. Keys from `ssh-agent` (`$SSH_AUTH_SOCK`).
2. Key files: `--identity` and the host's `IdentityFile` entries. Without
   `--identity`, the defaults `~/.ssh/id_ed25519`, `id_ecdsa` and `id_rsa` are
   tried as well. You are asked for the passphrase of an encrypted key unless
   the agent already holds it.
3. Keyboard-interactive, then password. Only hidden (password-style)
   keyboard-interactive prompts are supported. Prompts that echo input, as
   some OTP setups use, need `--ssh-cmd ssh`.

Whatever you type is remembered for the rest of the run. Reconnects after a
network failure and every extra connection reuse it, so you are never asked
twice.

### Host keys

`known_hosts` is checked strictly:

- **Known host, matching key:** connects.
- **Known host, different key:** refuses to connect, like OpenSSH.
- **Unknown host:** shows the fingerprint and asks whether to trust it. If you
  accept, the key is added to `known_hosts`. With no terminal, the connection
  is refused.

nimbus negotiates the host key type you already have on file, so an existing
`ssh-ed25519` entry is not reported as a mismatch because the server offered
RSA first.

## System ssh

```sh
nimbus --ssh-cmd ssh ./data/ host:/data
nimbus -e "ssh -J bastion -o Compression=no" ./data/ internal:/data
```

`CMD` is split into words like a shell would do it and run with the usual
`ssh` arguments added. Each of the `--conns` connections is a separate
OpenSSH ControlMaster, so parallel jobs really get separate TCP streams.
Masters are closed when nimbus exits.

If key authentication is not available, nimbus uses a single connection so
you are prompted only once. That also means no parallel streams. Load your key
into `ssh-agent`, or pass `--conns N` to accept N prompts.

Hosts whose config has `ProxyJump` or `ProxyCommand` switch to the system
`ssh` automatically. `-F` / `--ssh-config` is passed on to it
(`$NIMBUS_SSH_CONFIG` is not, so set `-F` explicitly if the system ssh needs
the same file).

## Troubleshooting

| Symptom | What to do |
| ------- | ---------- |
| `host key mismatch` | The server key changed. Check why, then fix the entry with `ssh-keygen -R host`. |
| Prompted for a password many times | You are using system ssh without a key. Use `ssh-agent`, or let nimbus use the built-in client. |
| A config option seems ignored | It is probably not in the list above. Try `--ssh-cmd ssh`. |
| Connections drop on a flaky link | nimbus reconnects and retries on its own. Raise `--retries` / `--backoff-max` for long outages. |
