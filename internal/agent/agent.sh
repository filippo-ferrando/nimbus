#!/bin/sh
# nimbus agent: runs on each endpoint (local or remote) and performs all
# filesystem work using only portable shell tools.
# Supported userlands: GNU/Linux, BusyBox, macOS / *BSD.
#
# Usage: sh agent.sh <command> [args...]
# Arguments prefixed with "o" are octal-encoded byte strings (see dec()).
# Exit codes: 0 ok, 1 generic error, 3 checksum mismatch, 4 not found,
#             5 pack failed, 6 unsupported environment.

set -u
umask 022
STAGE=$(cd "$(dirname "$0")" && pwd) || exit 1
LC_ALL=C
export LC_ALL

die() { printf 'nimbus-agent: %s\n' "$*" >&2; exit "${CODE:-1}"; }

# dec ARG: decode an argument. "o101102" -> "AB"; "r..." -> raw remainder.
dec() {
	case $1 in
	o*) printf "$(printf '%s' "${1#o}" | sed 's/.../\\&/g')" ;;
	r*) printf '%s' "${1#r}" ;;
	*) printf '%s' "$1" ;;
	esac
}

# root ARG: decode and expand a leading "~".
root() {
	_r=$(dec "$1")
	case $_r in
	"~") _r=$HOME ;;
	"~/"*) _r=$HOME/${_r#"~/"} ;;
	"") _r=. ;;
	esac
	printf '%s' "$_r"
}

load_env() {
	[ -f "$STAGE/env" ] || probe >/dev/null || exit 6
	. "$STAGE/env"
}

# ---------------------------------------------------------------- probe
probe() {
	os=$(uname -s 2>/dev/null || echo unknown)
	arch=$(uname -m 2>/dev/null || echo unknown)

	for t in tar gtar bsdtar; do
		if command -v "$t" >/dev/null 2>&1; then TAR=$t; break; fi
	done
	[ -n "${TAR:-}" ] || { CODE=6 die "no tar found"; }
	TARFLAVOR=other
	case $($TAR --version 2>/dev/null) in
	*"GNU tar"*) TARFLAVOR=gnu ;;
	*bsdtar*) TARFLAVOR=bsd ;;
	esac
	TARNULL=0
	if [ "${NIMBUS_FORCE_COMPAT:-0}" != 1 ] &&
		printf '' | $TAR --null -cf - -T - >/dev/null 2>&1; then TARNULL=1; fi

	GFIND=0
	if [ "${NIMBUS_FORCE_COMPAT:-0}" != 1 ] &&
		find . -maxdepth 0 -printf '' >/dev/null 2>&1; then GFIND=1; fi

	STATF=none
	if stat -c %s . >/dev/null 2>&1; then STATF=gnu
	elif stat -f %z . >/dev/null 2>&1; then STATF=bsd; fi
	[ "$GFIND" = 1 ] || [ "$STATF" != none ] || { CODE=6 die "need GNU find or stat"; }

	SHA=
	for c in sha256sum gsha256sum "shasum -a 256" "sha256 -r"; do
		set -- $c
		if command -v "$1" >/dev/null 2>&1 &&
			[ "$(printf abc | $c 2>/dev/null | cut -c1-8)" = ba7816bf ]; then
			SHA=$c; break
		fi
	done
	[ -n "$SHA" ] || { CODE=6 die "no sha256 tool (sha256sum/shasum/sha256)"; }

	ZSTD=0; PIGZ=0; GZIP=0; PV=0
	command -v zstd >/dev/null 2>&1 && ZSTD=1
	command -v pigz >/dev/null 2>&1 && PIGZ=1
	command -v gzip >/dev/null 2>&1 && GZIP=1

	NPROC=$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null ||
		sysctl -n hw.ncpu 2>/dev/null || echo 1)

	cat >"$STAGE/env.tmp" <<EOF
TAR='$TAR'
TARFLAVOR=$TARFLAVOR
TARNULL=$TARNULL
GFIND=$GFIND
STATF=$STATF
SHA='$SHA'
ZSTD=$ZSTD
PIGZ=$PIGZ
GZIP=$GZIP
EOF
	mv -f "$STAGE/env.tmp" "$STAGE/env"
	cat <<EOF
os=$os
arch=$arch
tar=$TARFLAVOR
tarnull=$TARNULL
gfind=$GFIND
stat=$STATF
sha=$SHA
zstd=$ZSTD
pigz=$PIGZ
gzip=$GZIP
nproc=$NPROC
home=$HOME
EOF
}

sha_of() { $SHA "$1" | { read -r h _ && printf '%s' "${h#\\}"; }; }

compressor() { # family level
	case $1 in
	zstd) echo "zstd -q -c -T0 -$2" ;;
	gzip) if [ "$PIGZ" = 1 ]; then echo "pigz -c -$2"; else echo "gzip -c -$2"; fi ;;
	none) echo cat ;;
	*) CODE=6 die "unknown compressor $1" ;;
	esac
}
decompressor() {
	case $1 in
	zstd) echo "zstd -q -dc" ;;
	gzip) if [ "$PIGZ" = 1 ]; then echo "pigz -dc"; else echo "gzip -dc"; fi ;;
	none) echo cat ;;
	*) CODE=6 die "unknown compressor $1" ;;
	esac
}

# ---------------------------------------------------------------- list
# list ROOT SUB: emit metadata for ROOT/SUB (or ROOT when SUB is empty).
list() {
	r=$(root "$1"); sub=$(dec "${2:-}")
	cd "$r" 2>/dev/null || exit 4
	start=.
	[ -n "$sub" ] && start=./$sub
	[ -e "$start" ] || [ -h "$start" ] || exit 4
	if [ "$GFIND" = 1 ]; then
		echo '#gfind'
		find "$start" -printf '%y %s %T@ %m\0%p\0%l\0'
	else
		echo "#stat-$STATF"
		if [ "$STATF" = gnu ]; then
			find "$start" -exec stat -c '%f %s %Y %n' {} +
		else
			find "$start" -exec stat -f '%p %z %m %N' {} +
		fi
		printf '#links\n'
		find "$start" -type l -exec sh -c \
			'for f; do printf "%s\0%s\0" "$f" "$(readlink "$f")"; done' sh {} +
	fi
}

# hash ROOT < NUL-list: one sha256 line per file, in input order.
hash() {
	r=$(root "$1")
	t=$STAGE/hash.$$
	cat >"$t"
	cd "$r" || exit 4
	xargs -0 $SHA <"$t"
	s=$?
	rm -f "$t"
	return $s
}

# mkdirs ROOT < NUL-list of ./relative dirs
mkdirs() {
	r=$(root "$1")
	mkdir -p "$r" || exit 1
	cd "$r" || exit 1
	t=$STAGE/mkd.$$
	cat >"$t"
	if [ -s "$t" ]; then xargs -0 mkdir -p <"$t"; s=$?; else s=0; fi
	rm -f "$t"
	return $s
}

# rmrf ROOT < NUL-list of ./relative paths
rmrf() {
	r=$(root "$1")
	cd "$r" || exit 0
	t=$STAGE/rm.$$
	cat >"$t"
	if [ -s "$t" ]; then xargs -0 rm -rf <"$t"; s=$?; else s=0; fi
	rm -f "$t"
	return $s
}

# meta ROOT < NUL triplets: mode touchtime path   (touch -t in UTC)
meta() {
	r=$(root "$1")
	cd "$r" || exit 1
	t=$STAGE/meta.$$
	cat >"$t"
	s=0
	if [ -s "$t" ]; then
		xargs -0 -n 3 sh -c 'TZ=UTC0 touch -c -t "$2" "$3" && chmod "$1" "$3"' sh <"$t" || s=1
	fi
	rm -f "$t"
	return $s
}

# finish ROOT < NUL pairs: partial final   (atomic rename into place)
finish() {
	r=$(root "$1")
	cd "$r" || exit 1
	t=$STAGE/fin.$$
	cat >"$t"
	s=0
	if [ -s "$t" ]; then
		xargs -0 -n 2 sh -c 'mv -f "$1" "$2"' sh <"$t" || s=1
	fi
	rm -f "$t"
	return $s
}

# ---------------------------------------------------------------- pack
# pack KEY KIND COMP LEVEL ROOT [SKIP COUNT] < list
# Packing runs detached (nohup) so it survives a dropped connection; this
# command only waits for it and prints "sha size".
pack() {
	key=$1
	d=$STAGE/$key
	if [ -f "$d.done" ]; then cat >/dev/null; cat "$d.done"; return 0; fi
	if mkdir "$d.lock" 2>/dev/null; then
		cat >"$d.list"
		rm -f "$d.fail"
		nohup sh "$STAGE/agent.sh" _pack "$@" </dev/null >"$d.log" 2>&1 &
		echo $! >"$d.lock/pid"
	else
		cat >/dev/null
	fi
	n=0
	while [ ! -f "$d.done" ]; do
		if [ -f "$d.fail" ]; then
			cat "$d.log" >&2 2>/dev/null
			rm -f "$d.fail"
			CODE=5 die "pack $key failed"
		fi
		pid=$(cat "$d.lock/pid" 2>/dev/null)
		if [ -n "$pid" ] && ! kill -0 "$pid" 2>/dev/null && [ ! -f "$d.done" ]; then
			sleep 0.2
			[ -f "$d.done" ] && break
			rm -rf "$d.lock"
			CODE=5 die "pack $key: packer died"
		fi
		if [ -z "$pid" ]; then
			n=$((n + 1))
			[ $n -gt 50 ] && { rm -rf "$d.lock"; CODE=5 die "pack $key: stale lock"; }
		fi
		sleep 0.2
	done
	cat "$d.done"
}

_pack() {
	key=$1 kind=$2 comp=$3 level=$4
	r=$(root "$5")
	d=$STAGE/$key
	st=$d.st
	rm -f "$st" "$d.data.tmp"
	c=$(compressor "$comp" "$level")
	cd "$r" || { touch "$d.fail"; rm -rf "$d.lock"; exit 1; }
	case $kind in
	tar)
		if [ "$TARNULL" = 1 ]; then
			{ $TAR -cf - --null -T "$d.list" || echo $? >"$st"; } | $c >"$d.data.tmp" || echo c >>"$st"
		else
			tr '\0' '\n' <"$d.list" >"$d.nl"
			{ $TAR -cf - -T "$d.nl" || echo $? >"$st"; } | $c >"$d.data.tmp" || echo c >>"$st"
			rm -f "$d.nl"
		fi
		# GNU tar exits 1 when a file changed while being read: not fatal,
		# the next run will see the new mtime and resend it.
		if [ -f "$st" ] && [ "$TARFLAVOR" = gnu ] && [ "$(cat "$st")" = 1 ]; then rm -f "$st"; fi
		;;
	part)
		p=$(cat "$d.list")
		{ dd if="$p" bs=1048576 skip="$6" count="$7" 2>/dev/null || echo dd >"$st"; } |
			$c >"$d.data.tmp" || echo c >>"$st"
		;;
	*) echo kind >"$st" ;;
	esac
	if [ -f "$st" ]; then
		echo "pack failed: $(cat "$st")" >&2
		rm -f "$d.data.tmp" "$st"
		touch "$d.fail"
		rm -rf "$d.lock"
		exit 1
	fi
	h=$(sha_of "$d.data.tmp")
	s=$(wc -c <"$d.data.tmp" | tr -d ' ')
	mv -f "$d.data.tmp" "$d.data"
	printf '%s %s\n' "$h" "$s" >"$d.done.tmp"
	mv -f "$d.done.tmp" "$d.done"
	rm -f "$d.list"
	rm -rf "$d.lock"
}

# ---------------------------------------------------------------- transfer
send() { # send KEY [OFFSET]
	f=$STAGE/$1.data
	[ -f "$f" ] || { CODE=4 die "missing $1"; }
	cat "$f"
}

recv() { # recv KEY SHA < data
	d=$STAGE/$1
	cat >"$d.recv" || { rm -f "$d.recv"; exit 1; }
	h=$(sha_of "$d.recv")
	if [ "$h" != "$2" ]; then
		rm -f "$d.recv"
		CODE=3 die "checksum mismatch for $1: got $h"
	fi
	mv -f "$d.recv" "$d.data"
}

# extract KEY KIND COMP ROOT [PATH SEEK]
extract() {
	key=$1 kind=$2 comp=$3
	r=$(root "$4")
	d=$STAGE/$key
	st=$d.xst
	rm -f "$st"
	[ -f "$d.data" ] || { CODE=4 die "missing $key"; }
	x=$(decompressor "$comp")
	cd "$r" || exit 1
	case $kind in
	tar)
		{ $x <"$d.data" || echo d >"$st"; } | $TAR -xpf - || echo t >>"$st"
		;;
	part)
		p=$(dec "$5")
		{ $x <"$d.data" || echo d >"$st"; } |
			dd of="$p" bs=1048576 seek="$6" conv=notrunc 2>/dev/null || echo w >>"$st"
		;;
	esac
	if [ -f "$st" ]; then
		rm -f "$st"
		die "extract $key failed"
	fi
	[ "$kind" = part ] && : >"$d.x"
	[ "${NIMBUS_KEEP_DATA:-0}" = 1 ] || rm -f "$d.data"
}

# state: list staged keys:  P key sha size (packed) | R key (received) | X key
state() {
	cd "$STAGE" || exit 0
	for f in *.done; do
		[ -f "$f" ] || continue
		printf 'P %s %s\n' "${f%.done}" "$(cat "$f")"
	done
	for f in *.data; do
		[ -f "$f" ] || continue
		printf 'R %s\n' "${f%.data}"
	done
	for f in *.x; do
		[ -f "$f" ] || continue
		printf 'X %s\n' "${f%.x}"
	done
}

drop() { # drop KEY...
	for k; do
		rm -f "$STAGE/$k.data" "$STAGE/$k.done" "$STAGE/$k.list" "$STAGE/$k.log"
	done
}

clean() {
	case $STAGE in
	*/nimbus-*) cd / && rm -rf "$STAGE" ;;
	*) die "refusing to remove $STAGE" ;;
	esac
}

cmd=${1:-}
[ $# -gt 0 ] && shift
case $cmd in
probe) probe ;;
list) load_env; list "$@" ;;
hash) load_env; hash "$@" ;;
mkdirs) mkdirs "$@" ;;
rmrf) rmrf "$@" ;;
meta) meta "$@" ;;
finish) finish "$@" ;;
pack) load_env; pack "$@" ;;
_pack) load_env; _pack "$@" ;;
send) send "$@" ;;
recv) load_env; recv "$@" ;;
extract) load_env; extract "$@" ;;
state) state ;;
drop) drop "$@" ;;
clean) clean ;;
*) die "unknown command '$cmd'" ;;
esac
