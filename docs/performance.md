# Performance

## When nimbus is faster

rsync, scp and most sync tools send everything over **one TCP connection**.
When that one connection is the bottleneck, adding more connections helps. This
happens with:

- high-latency links (TCP window and congestion control limit one stream),
- per-flow traffic shaping by ISPs, cloud providers or corporate networks,
- VPNs and tunnels that cap each flow,
- lossy links, where one lost packet stalls one stream but not the others,
- SSH cipher throughput on slow CPUs (each connection encrypts on its own
  core).

If the **total** bandwidth of the link is the limit, more jobs will not help.
On a fast unthrottled link nimbus is about as fast as rsync. It writes data to
disk in staging, so on loopback it is a little slower.

## Benchmark

510 MB of data (one 200 MB file plus 2,000 small files), pushed through a proxy
that limits **every TCP connection** to 20 MB/s:

| Command        | Time   | Speed-up |
| -------------- | ------ | -------- |
| `rsync -a`     | 26.6 s | 1.0×     |
| `nimbus -j1`   | 26.6 s | 1.0×     |
| `nimbus -j4`   | 9.5 s  | 2.8×     |
| `nimbus -j8`   | 3.9 s  | 6.8×     |

You can reproduce it with the end-to-end suite (needs `rsync`):

```sh
test/e2e.sh -b bench
```

The rate-limiting proxy lives in [`test/proxy`](../test/proxy).

## Tuning

### `--jobs`

The number of batches packed, sent and extracted at once, and by default the
number of SSH connections. Start at 4 (the default). Raise it to 8–16 on
high-latency or per-flow-shaped links, and keep it low on slow disks or when
the remote CPU is busy. `--conns` sets the number of connections separately if
you want fewer connections than jobs.

### `--block`

The size of each batch.

| Larger blocks (`256M`, `1G`)            | Smaller blocks (`8M`, `16M`)                 |
| --------------------------------------- | -------------------------------------------- |
| less overhead per batch                 | less work lost on an interrupt               |
| better compression on many small files  | less staging space (`≈ 2 × jobs × block`)    |
| fewer round trips                       | more even load across jobs                   |

With a single large file, `--block` also limits parallelism: a 200 MB file in
64 MB blocks gives 4 parts, so `-j 16` cannot help much. Use smaller blocks for
a few large files.

### Compression

| Mode    | Use it for |
| ------- | ---------- |
| `auto`  | The default: zstd if both ends have it, otherwise gzip (pigz where installed), otherwise none. No compression for local→local. |
| `zstd`  | Text, logs, databases, source trees. `--level 1` for fast links, up to `--level 19` for very slow ones. |
| `gzip`  | Hosts without zstd. Install `pigz` so it runs on all cores. |
| `none`  | Already-compressed data (video, images, archives) and fast LANs where the CPU is the bottleneck. |

The live view shows the compression ratio and the wire rate next to the logical
throughput, so you can see whether compression is paying off.

### Staging location

Staging is written and read once per batch on each side. Put it on a fast disk
(`--staging /mnt/nvme/tmp`), and avoid small tmpfs mounts, which can fill up
and do not survive a reboot.
