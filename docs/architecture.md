# How it works

This page describes what happens during a run and why nimbus can always be
interrupted and resumed safely.

```
        source                                        destination
  ┌────────────────────┐                        ┌────────────────────┐
  │ list               │                        │               list │
  └─────────┬──────────┘                        └──────────┬─────────┘
            └────────────► diff + plan (local) ◄───────────┘
                                   │
  ┌────────────────────┐           ▼            ┌────────────────────┐
  │ pack               │                        │ recv               │
  │ compress           │ ──── N × SSH ────────► │ verify sha256      │
  │ sha256             │       streams          │ extract            │
  │ (staging)          │                        │ (staging)          │
  └────────────────────┘                        └────────────────────┘
                finalize: attributes, --delete, cleanup
```

## 1. Agent

Remote hosts have nothing installed. At the start of a run nimbus uploads a
small POSIX shell script, the *agent*, to the staging directory on each remote
end and runs every filesystem operation through it. The agent uses only
standard tools (`sh`, `tar`, `find`/`stat`, a sha256 tool, and optionally
`zstd`/`pigz`). It detects GNU, BusyBox and BSD/macOS userlands and adapts its
commands to each.

The uploaded file is named after a hash of its contents, so a newer nimbus
never reuses an agent left behind by an older version. Arguments are encoded so
they survive any remote login shell (sh, bash, zsh, fish, csh).

Local endpoints run the same agent with the local `sh`, so local and remote
transfers follow the same code path.

## 2. List and diff

Both trees are listed with type, size, mtime, mode and symlink target. Exclude
patterns are applied, and nimbus computes what has to change on the
destination:

- files that are new or different (size + mtime, or sha256 with `-c`),
- directories and symlinks to create,
- attributes to fix on entries whose content is already right,
- with `--delete`, extraneous entries to remove.

`--dry-run` prints this plan and exits.

## 3. Batches

The files to send are grouped into **batches**, independent units of work of
about `--block` bytes:

- **Tar batches** hold many small files and symlinks in one archive.
- **Part batches** hold one `--block`-sized byte range of a large file.

Each batch has a **content key**, a hash of the path, size, mtime, mode and
link target of every entry in it. The same command over unchanged data gives
the same keys, which is what lets a later run recognise work that is already
done.

## 4. The pipeline

Batches flow through four stages that run at the same time, each limited to
`--jobs` workers:

1. **Pack**: the source builds the batch in its staging directory (`tar` or a
   byte-range read), compresses it, and records its sha256. On a remote
   source, packing runs detached from the SSH session, so a dropped connection
   does not kill it. If a packer is still running when the next run starts,
   nimbus waits for it. If it died (killed, reboot), nimbus takes over.
2. **Transfer**: the packed data streams from source staging to destination
   staging over one of the `--conns` SSH connections.
3. **Verify**: the destination recomputes the sha256. On a mismatch the data
   is discarded and the batch is retried. Nothing is extracted from data that
   failed the check.
4. **Extract**: tar batches are unpacked into the destination tree. Part
   batches are written in place into a hidden `.name.nimbus-part` file. When
   all parts of a file are written, it gets its attributes and is renamed over
   the target in one atomic step.

A window of about `2 × jobs` batches is in flight at a time, so packing stays
ahead of the network and extraction overlaps with the next transfers.

## 5. Retries

Every remote operation runs under a retry policy: up to `--retries` attempts,
exponential backoff from `--backoff` to `--backoff-max`, with full jitter so
parallel jobs do not retry in lockstep.

Only transient failures are retried:

| Retried | Not retried (aborts the run) |
| ------- | ---------------------------- |
| Connection lost or refused | Permission denied |
| Transfer stalled for `--timeout` | Disk full |
| Checksum mismatch | Unreadable source file |
| | Any other error from the agent |

Dead SSH connections are detected with keepalives and opened again with the
credentials from the first login, so you are not asked again. If a batch still
fails after its retries, the whole run stops: there is no point sending the
rest when the run will have to be repeated anyway.

## 6. Finalize

When every batch has succeeded:

1. Attribute fixes are applied (permissions, mtimes).
2. With `--delete`, extraneous entries are removed.
3. Directory mtimes and permissions are set, deepest directories first, so
   writing into a directory does not change its mtime afterwards.
4. Staging is cleaned up on both ends, unless `--keep-staging` is set.

All of these steps are idempotent: if the run is interrupted here, the next run
simply does them again.

## Resume

Each transfer has an **id** derived from the source, destination and paths.
Running the same command again gives the same id, and with it the same
staging directories. On start, nimbus asks both ends what they already have:

- batches that are **extracted** on the destination are skipped,
- batches that are **verified in destination staging** go straight to
  extraction,
- batches that are **packed in source staging** go straight to transfer,
- everything else starts from the beginning.

The unit of resume is the batch. At most the batches in flight when the run
stopped have to be redone, so a smaller `--block` loses less work.

A lock file in the staging directory stops two copies of the same transfer
from running at once. `--fresh` throws away whatever an earlier run left
behind.

## Staging

Each end has a staging directory, `<--staging>/nimbus-<id>` (default
`/var/tmp`). It holds the agent, packed or received batch data and resume
state. Batches are removed from staging as soon as they are extracted, so the
space needed stays around `2 × jobs × block` per side whatever the size of the
transfer. For local→local copies, both ends share one staging directory and
nothing is copied twice.

`/var/tmp` is used rather than `/tmp` because it is disk-backed on most
systems and survives reboots, both of which matter when you resume.
