#!/usr/bin/env bash
# End-to-end tests for nimbus.
#
# Builds nimbus, generates test trees and starts a throwaway unprivileged sshd
# on 127.0.0.1 (temporary host/user keys, nothing touches ~/.ssh). Everything
# lives in a temporary directory and is removed on exit, sshd included.
#
#   test/e2e.sh            run all tests
#   test/e2e.sh -k         keep the work directory for inspection
#   test/e2e.sh -b         also run the benchmark against rsync
#   test/e2e.sh NAME...    run only the named tests (see list at the bottom)
#
# Work directory: $NIMBUS_E2E_DIR (default /var/tmp), about 4 GB free needed.
#
# Requirements: go, sshd, ssh, ssh-keygen, tar, zstd, GNU find/coreutils.
# Optional: bsdtar (bsdtar tests), rsync (benchmark).
set -uo pipefail

KEEP=0
BENCH=0
ONLY=()
for a in "$@"; do
	case $a in
	-k) KEEP=1 ;;
	-b) BENCH=1 ;;
	-h | --help) sed -n '2,15p' "$0"; exit 0 ;;
	*) ONLY+=("$a") ;;
	esac
done

REPO=$(cd "$(dirname "$0")/.." && pwd)
# Needs ~4 GB: default to disk-backed /var/tmp (/tmp is often a small tmpfs).
W=$(mktemp -d "${NIMBUS_E2E_DIR:-/var/tmp}/nimbus-e2e.XXXXXX")
PORT=$((20000 + RANDOM % 20000))
PPORT=$((PORT + 1))
export SSH_AUTH_SOCK=
unset NIMBUS_SSH_CONFIG

PASS=0
FAIL=0
FAILED=()
PIDS=()

cleanup() {
	for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done
	[ -f "$W/sshd/sshd.pid" ] && kill "$(cat "$W/sshd/sshd.pid")" 2>/dev/null
	if [ $KEEP = 1 ]; then echo "work dir kept: $W"; else chmod -R u+w "$W" 2>/dev/null; rm -rf "$W"; fi
}
trap cleanup EXIT

c_ok=$'\e[32m' c_bad=$'\e[31m' c_dim=$'\e[2m' c_off=$'\e[0m'
[ -t 1 ] || { c_ok= c_bad= c_dim= c_off=; }

log() { printf '%s  %s%s\n' "$c_dim" "$*" "$c_off"; }
ok() { PASS=$((PASS + 1)); printf '%s✔%s %s\n' "$c_ok" "$c_off" "$1"; }
bad() { FAIL=$((FAIL + 1)); FAILED+=("$1"); printf '%s✖ %s%s\n' "$c_bad" "$1" "$c_off"; [ -n "${2:-}" ] && printf '   %s\n' "$2"; }

# check NAME COMMAND...: pass when COMMAND succeeds
check() {
	local name=$1
	shift
	local out
	if out=$("$@" 2>&1); then ok "$name"; else bad "$name" "$(printf '%s' "$out" | tail -n 5)"; fi
}

# same TREE_A TREE_B: identical content, types, modes, mtimes, link targets
same() {
	local meta='%y %m %s %T@ %p -> %l\n'
	diff <(cd "$1" && find . -printf "$meta" | sed -E 's/ ([0-9]+)\.[0-9]+ / \1 /' | sort) \
		<(cd "$2" && find . -printf "$meta" | sed -E 's/ ([0-9]+)\.[0-9]+ / \1 /' | sort) &&
		diff -r --no-dereference "$1" "$2" >/dev/null
}

# wire BYTES_LOG: print the "wire" amount (MiB) reported in a plain log
wire_mib() { grep -o 'wire [0-9.]* MiB' "$1" | head -1 | awk '{print int($2)}'; }

# descendants PID: all descendant pids
descendants() { local c; for c in $(pgrep -P "$1"); do echo "$c"; descendants "$c"; done; }

# ------------------------------------------------------------------ setup

log "work dir $W"
mkdir -p "$W/bin" "$W/sshd" "$W/stage" "$W/t"
(cd "$REPO" && go build -o "$W/bin/nimbus" . && go build -o "$W/bin/proxy" ./test/proxy) || { echo "build failed"; exit 1; }
N=$W/bin/nimbus

mktree() {
	local d=$1
	rm -rf "$d"
	mkdir -p "$d/dir with spaces/sub" "$d/emptydir" "$d/deep/a/b/c" "$d/ro_dir" "$d/-dash"
	echo hello >"$d/dir with spaces/file 1.txt"
	head -c 1000 /dev/zero | tr '\0' x >"$d/deep/a/b/c/small"
	head -c 5000000 /dev/urandom >"$d/big.bin"
	head -c 3500000 /dev/zero >"$d/zeros.bin"
	echo nl >"$d/$(printf 'new\nline')"
	echo tab >"$d/$(printf 'tab\there')"
	echo q >"$d/quote'd \"name\""
	echo star >"$d/st*r?[x]"
	echo dash >"$d/-dash/-file"
	echo pct >"$d/100%d"
	echo uni >"$d/ünïcødé ✓"
	ln -s "dir with spaces/file 1.txt" "$d/link_to_file"
	ln -s nonexistent "$d/dangling"
	ln -s deep "$d/dirlink"
	: >"$d/empty_file"
	echo exe >"$d/script.sh"
	chmod 755 "$d/script.sh"
	echo ro >"$d/ro_dir/f"
	chmod 555 "$d/ro_dir"
	chmod 600 "$d/dir with spaces/file 1.txt"
	touch -d '2001-02-03 04:05:06' "$d/deep/a/b/c/small"
	local L i
	L=$(printf 'd%.0s' {1..90})
	mkdir -p "$d/long/$L/$L/$L"
	echo deep >"$d/long/$L/$L/$L/$(printf 'f%.0s' {1..200}).txt"
	for i in $(seq 1 300); do echo "$i" >"$d/deep/a/f$i"; done
}
log "generating test data"
mktree "$W/t/src"
mkdir -p "$W/t/large/many"
for i in 1 2 3; do head -c 100M /dev/urandom >"$W/t/large/r$i.bin"; done
for i in $(seq 1 1000); do head -c $((RANDOM * 10)) /dev/urandom >"$W/t/large/many/f$i"; done

SSHD=$(command -v sshd || echo /usr/sbin/sshd)
S=$W/sshd
ssh-keygen -q -t ed25519 -N '' -f "$S/hostkey" </dev/null
ssh-keygen -q -t ed25519 -N '' -f "$S/userkey" </dev/null
ssh-keygen -q -t ed25519 -N '' -f "$S/wrongkey" </dev/null
cp "$S/userkey.pub" "$S/authorized_keys"
cat >"$S/sshd_config" <<EOF
Port $PORT
ListenAddress 127.0.0.1
HostKey $S/hostkey
PidFile $S/sshd.pid
AuthorizedKeysFile $S/authorized_keys
PasswordAuthentication no
KbdInteractiveAuthentication no
UsePAM no
StrictModes no
EOF
echo "[127.0.0.1]:$PORT $(cut -d' ' -f1-2 "$S/hostkey.pub")" >"$S/known_hosts"
echo "[127.0.0.1]:$PORT $(cut -d' ' -f1-2 "$S/wrongkey.pub")" >"$S/known_hosts_wrong"
cfg() { # cfg FILE ALIAS PORT KNOWN_HOSTS [extra lines]
	printf 'Host %s\n  HostName 127.0.0.1\n  Port %s\n  IdentityFile %s\n  UserKnownHostsFile %s\n  IdentitiesOnly yes\n%s\n' \
		"$2" "$3" "$S/userkey" "$4" "${5:-}" >>"$1"
}
cfg "$S/config" testhost "$PORT" "$S/known_hosts"
cfg "$S/config" jumped "$PORT" "$S/known_hosts" "  ProxyJump testhost"
cfg "$S/config" slow "$PPORT" "$S/known_hosts" "  HostKeyAlias [127.0.0.1]:$PORT"
cfg "$S/config_wrong" testhost "$PORT" "$S/known_hosts_wrong"
"$SSHD" -f "$S/sshd_config" -E "$S/log" || { echo "cannot start sshd"; exit 1; }
for _ in $(seq 50); do ssh -F "$S/config" -o BatchMode=yes testhost true 2>/dev/null && break; sleep 0.1; done
SP=$(cat "$S/sshd.pid")
F=(-F "$S/config")
H=testhost
T=$W/t
ST=(--staging "$W/stage")

# ------------------------------------------------------------------ tests

t_local() {
	check "local→local" "$N" -q -b 1M "${ST[@]}" "$T/src" "$T/l1"
	check "local→local identical" same "$T/src" "$T/l1/src"
	"$N" --plain -b 1M "${ST[@]}" "$T/src" "$T/l1" >"$W/noop.log" 2>&1
	check "rerun sends nothing" grep -q "0 items to send" "$W/noop.log"
}

t_push() {
	check "push (native ssh)" "$N" -q -b 1M "${F[@]}" "${ST[@]}" "$T/src/" "$H:$T/p1"
	check "push identical" same "$T/src" "$T/p1"
}

t_pull() {
	check "pull (native ssh, gzip)" "$N" -q -b 1M --compress gzip "${F[@]}" "${ST[@]}" "$H:$T/src" "$T/p2"
	check "pull identical" same "$T/src" "$T/p2/src"
}

t_sshcmd() {
	check "push (system ssh)" "$N" -q -b 1M "${F[@]}" -e ssh "${ST[@]}" "$T/src" "$H:$T/p3"
	check "system ssh identical" same "$T/src" "$T/p3/src"
	check "ProxyJump auto-switch" "$N" -q "${F[@]}" "${ST[@]}" "$T/src/zeros.bin" "jumped:$T/p4/"
	check "ProxyJump identical" cmp "$T/src/zeros.bin" "$T/p4/zeros.bin"
}

t_delete() {
	"$N" -q "${ST[@]}" "$T/src/" "$T/d1"
	echo junk >"$T/d1/junk"
	mkdir -p "$T/d1/junkdir/sub" && echo t >"$T/d1/junkdir/x.tmp" && echo y >"$T/d1/junkdir/sub/y"
	"$N" -q -n --delete "${ST[@]}" "$T/src/" "$T/d1" >"$W/dry.txt" 2>/dev/null
	check "dry-run lists deletions" grep -q "delete   junk" "$W/dry.txt"
	check "dry-run changes nothing" test -f "$T/d1/junk"
	check "--delete with --exclude" "$N" -q --delete --exclude '*.tmp' "${F[@]}" "${ST[@]}" "$T/src/" "$H:$T/d1"
	check "excluded entry kept, others deleted" bash -c "[ -f '$T/d1/junkdir/x.tmp' ] && [ ! -e '$T/d1/junkdir/sub' ] && [ ! -e '$T/d1/junk' ]"
	mkdir -p "$T/d2/src/emptydir/x"
	rm -rf "$T/srcb" && cp -a "$T/src" "$T/srcb" && rmdir "$T/srcb/emptydir" && echo f >"$T/srcb/emptydir"
	"$N" -q "${ST[@]}" "$T/srcb/" "$T/d2/src" 2>/dev/null
	check "blocked type change exits 23" test $? -eq 23
	check "--delete resolves type change" "$N" -q --delete "${ST[@]}" "$T/srcb/" "$T/d2/src"
	check "type change identical" same "$T/srcb" "$T/d2/src"
}

t_rename() {
	mkdir -p "$T/rn/existing_dir"
	check "single file renamed (local)" "$N" -q "${ST[@]}" "$T/src/zeros.bin" "$T/rn/renamed.bin"
	check "renamed content" cmp "$T/src/zeros.bin" "$T/rn/renamed.bin"
	check "single file into existing dir" "$N" -q "${ST[@]}" "$T/src/zeros.bin" "$T/rn/existing_dir"
	check "existing dir content" cmp "$T/src/zeros.bin" "$T/rn/existing_dir/zeros.bin"
	check "single file renamed (push)" "$N" -q "${F[@]}" "${ST[@]}" "$T/src/big.bin" "$H:$T/rn/pushed.bin"
	check "pushed content" cmp "$T/src/big.bin" "$T/rn/pushed.bin"
	check "empty file renamed" "$N" -q "${ST[@]}" "$T/src/empty_file" "$T/rn/empty"
	check "empty file exists" test -f "$T/rn/empty"
}

t_compat() {
	NIMBUS_FORCE_COMPAT=1 "$N" -q "${ST[@]}" "$T/src" "$T/c0" 2>/dev/null
	check "compat mode rejects newline names" test $? -ne 0
	rm -rf "$T/srcn" && cp -a "$T/src" "$T/srcn" && rm "$T/srcn/new"$'\n'"line"
	check "compat mode (no find -printf / tar --null)" env NIMBUS_FORCE_COMPAT=1 "$N" -q -b 1M "${ST[@]}" "$T/srcn" "$T/c1"
	check "compat identical" same "$T/srcn" "$T/c1/srcn"
	if command -v bsdtar >/dev/null; then
		mkdir -p "$W/shim" && printf '#!/bin/sh\nexec bsdtar "$@"\n' >"$W/shim/tar" && chmod +x "$W/shim/tar"
		check "bsdtar local" env PATH="$W/shim:$PATH" "$N" -q -b 1M "${ST[@]}" "$T/src" "$T/c2"
		check "bsdtar identical" same "$T/src" "$T/c2/src"
		check "bsdtar → GNU tar (push)" env PATH="$W/shim:$PATH" "$N" -q -b 1M "${F[@]}" "${ST[@]}" "$T/src" "$H:$T/c3"
		check "bsdtar push identical" same "$T/src" "$T/c3/src"
		check "GNU tar → bsdtar (pull)" env PATH="$W/shim:$PATH" "$N" -q -b 1M "${F[@]}" "${ST[@]}" "$H:$T/src" "$T/c4"
		check "bsdtar pull identical" same "$T/src" "$T/c4/src"
	else
		log "bsdtar not installed: skipping bsdtar tests"
	fi
}

t_checksum() {
	"$N" -q "${ST[@]}" "$T/src/" "$T/k1"
	printf 'HELLO\n' >"$T/k1/dir with spaces/file 1.txt"
	touch -r "$T/src/dir with spaces/file 1.txt" "$T/k1/dir with spaces/file 1.txt"
	"$N" --plain "${ST[@]}" "$T/src/" "$T/k1" >"$W/k1.log" 2>&1
	check "size+mtime misses same-size change" grep -q "0 items to send" "$W/k1.log"
	check "--checksum finds it" "$N" -q -c "${ST[@]}" "$T/src/" "$T/k1"
	check "checksum identical" same "$T/src" "$T/k1"
}

t_errors() {
	mkdir -p "$T/perm/ok" "$T/perm/noread" && echo a >"$T/perm/ok/a" && chmod 000 "$T/perm/noread"
	local s=$SECONDS
	"$N" -q "${ST[@]}" "$T/perm" "$T/perm_out" 2>/dev/null
	local rc=$?
	chmod 755 "$T/perm/noread"
	check "unreadable dir aborts (exit 1)" test $rc -eq 1
	check "abort is fast (no backoff)" test $((SECONDS - s)) -lt 5
	"$N" -q -F "$S/config_wrong" "${ST[@]}" "$T/src3" "$H:$T/x" </dev/null 2>"$W/mitm.err"
	check "wrong host key refused" grep -q "HOST KEY MISMATCH" "$W/mitm.err"
}

t_resume() {
	local X=(--plain -b 16M --compress zstd --level 19 "${F[@]}" "${ST[@]}")
	"$N" -j 1 "${X[@]}" "$T/large" "$H:$T/r1" >"$W/r1a.log" 2>&1 &
	local p=$!
	sleep 6
	kill -INT $p
	wait $p
	check "interrupt exits 130" test $? -eq 130
	check "resume" "$N" -q -j 4 "${X[@]}" "$T/large" "$H:$T/r1"
	check "resume identical" same "$T/large" "$T/r1/large"

	# single big file: second run must reuse extracted chunks
	"$N" -j 1 "${X[@]}" "$T/large/r1.bin" "$H:$T/r2/" >"$W/r2a.log" 2>&1 &
	p=$!
	sleep 6
	kill -INT $p
	wait $p
	"$N" -j 4 "${X[@]}" "$T/large/r1.bin" "$H:$T/r2/" >"$W/r2b.log" 2>&1
	check "single file resume completes" cmp "$T/large/r1.bin" "$T/r2/r1.bin"
	check "single file resume reuses chunks" test "$(wire_mib "$W/r2b.log")" -lt 100

	# stale chunk markers must not leave holes when the partial file vanished
	"$N" -j 2 "${X[@]}" "$T/large/r2.bin" "$H:$T/r3/" >/dev/null 2>&1 &
	p=$!
	sleep 6
	kill -INT $p
	wait $p
	rm -f "$T/r3/.r2.bin.nimbus-part"
	"$N" -j 1 "${X[@]}" "$T/large/r2.bin" "$H:$T/r3/" >/dev/null 2>&1 &
	p=$!
	sleep 3
	kill -INT $p
	wait $p
	"$N" -q -j 4 "${X[@]}" "$T/large/r2.bin" "$H:$T/r3/" >/dev/null 2>&1
	check "deleted partial + stale markers: no holes" cmp "$T/large/r2.bin" "$T/r3/r2.bin"
}

t_network() {
	local X=(--plain -b 8M --compress gzip --level 1 --backoff 300ms "${F[@]}" "${ST[@]}")
	"$N" -j 6 "${X[@]}" "$T/large" "$H:$T/n1" >"$W/n1.log" 2>&1 &
	local p=$!
	sleep 1.2
	pkill -P "$SP"
	sleep 0.8
	pkill -P "$SP"
	wait $p
	check "push survives killed connections" test $? -eq 0
	check "push after kills identical" same "$T/large" "$T/n1/large"

	"$N" -j 3 --level 19 --plain -b 16M --timeout 5s "${F[@]}" "${ST[@]}" "$H:$T/large" "$T/n2" >"$W/n2.log" 2>&1 &
	p=$!
	sleep 3
	local before after
	before=$(pgrep -f 'agent[.]sh _pack' | sort | tr '\n' ' ')
	pkill -P "$SP"
	sleep 0.5
	after=$(pgrep -f 'agent[.]sh _pack' | sort | tr '\n' ' ')
	wait $p
	check "pull survives killed connections" test $? -eq 0
	check "remote packers survive a dropped connection" test -n "$before" -a "$before" = "$after"
	check "pull after kills identical" same "$T/large" "$T/n2/large"

	"$N" -j 4 --plain -b 16M --level 9 --timeout 4s --backoff 300ms "${F[@]}" "${ST[@]}" "$T/large" "$H:$T/n3" >"$W/n3.log" 2>&1 &
	p=$!
	sleep 1.5
	local frozen
	frozen=$(descendants "$SP" | grep -v -x -f <(pgrep -f "zstd|tar|dd if" || echo none))
	kill -STOP $frozen 2>/dev/null
	local s=$SECONDS
	wait $p
	local rc=$?
	kill -CONT $frozen 2>/dev/null
	check "frozen connections: stall detected and recovered" test $rc -eq 0
	check "stall recovery under 60s" test $((SECONDS - s)) -lt 60
	check "frozen transfer identical" same "$T/large" "$T/n3/large"
}

t_bench() {
	command -v rsync >/dev/null || { log "rsync not installed: skipping benchmark"; return; }
	"$W/bin/proxy" "127.0.0.1:$PPORT" "127.0.0.1:$PORT" 20 &
	PIDS+=($!)
	sleep 0.3
	log "benchmark: every connection capped at 20 MB/s, $(du -sh "$T/large" | cut -f1)"
	local s
	mkdir -p "$T/b/rsync"
	s=$(date +%s%N)
	rsync -a -e "ssh -F $S/config" "$T/large" "slow:$T/b/rsync/"
	printf '   rsync -a      %6d ms\n' $((($(date +%s%N) - s) / 1000000))
	for j in 1 4 8; do
		s=$(date +%s%N)
		"$N" -q -j $j "${F[@]}" "${ST[@]}" "$T/large" "slow:$T/b/j$j"
		printf '   nimbus -j%-2d   %6d ms\n' $j $((($(date +%s%N) - s) / 1000000))
	done
	check "benchmark output identical" same "$T/large" "$T/b/j8/large"
}

ALL=(local push pull sshcmd delete rename compat checksum errors resume network)
[ $BENCH = 1 ] && ALL+=(bench)
[ ${#ONLY[@]} -gt 0 ] && ALL=("${ONLY[@]}")
mkdir -p "$T/src3" && echo x >"$T/src3/a"
for t in "${ALL[@]}"; do
	if ! declare -F "t_$t" >/dev/null; then
		echo "unknown test: $t (available: local push pull sshcmd delete rename compat checksum errors resume network bench)"
		exit 2
	fi
	printf '\n%s== %s%s\n' "$c_dim" "$t" "$c_off"
	"t_$t"
done

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
for f in "${FAILED[@]}"; do printf '  %s✖ %s%s\n' "$c_bad" "$f" "$c_off"; done
[ "$FAIL" = 0 ]
