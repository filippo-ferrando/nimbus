#!/bin/bash

# ==============================================================================
# Nimbus: Multiplexed Parallel File Transfer
# Usage: ./nimbus.sh <source> <destination> <block_size_mb> <parallel_jobs>
# ==============================================================================

# --- Colors & UI ---
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'
BOLD='\033[1m'

log_phase() { echo -e "\n${CYAN}${BOLD}>>> $1${NC}"; }
log_info()  { echo -e "${NC}    $1"; }
log_err()   { echo -e "${RED}${BOLD}ERROR: $1${NC}"; }

# --- Configuration ---
TMP_ARCHIVE="nimbus_bundle.tar.gz"
TMP_REMOTE_DIR="/tmp/nimbus_transfer"
CHUNK_PREFIX="nimbus_part."
SUM_FILE="nimbus_manifest.sha256"
SSH_SOCKET="/tmp/nimbus_ssh_%h_%p_%r.sock"
MAX_RETRIES=5

# --- Validation & Dependencies ---
if [ "$#" -ne 4 ]; then
    echo -e "${YELLOW}Nimbus${NC} - Multiplexed parallel file transfer protocol"
    echo -e "Version: 0.3"
    echo "" 
    echo -e "${YELLOW}Usage: $0 <source_path> <destination_path> <block_size_mb> <parallel_jobs>${NC}"
    exit 1
fi

for cmd in pv parallel sha256sum bc; do
    if ! command -v $cmd &> /dev/null; then
        log_err "Missing local dependency: $cmd."
        exit 1
    fi
done

SOURCE_PATH="$1"
DEST_PATH="$2"
BLOCK_MB="$3"
JOBS="$4"
BLOCK_SIZE=$((BLOCK_MB * 1024 * 1024))
START_TIME=$(date +%s)

# --- Remote Host Detection ---
REMOTE_PATTERN="^([^@]+)@([^:]+):(.+)$"
IS_SRC_REMOTE=false
IS_DST_REMOTE=false

if [[ "$SOURCE_PATH" =~ $REMOTE_PATTERN ]]; then
    IS_SRC_REMOTE=true
    REMOTE_TARGET="${BASH_REMATCH[1]}@${BASH_REMATCH[2]}"
    SRC_REMOTE_DIR="${BASH_REMATCH[3]}"
fi

if [[ "$DEST_PATH" =~ $REMOTE_PATTERN ]]; then
    IS_DST_REMOTE=true
    REMOTE_TARGET="${BASH_REMATCH[1]}@${BASH_REMATCH[2]}"
    DST_REMOTE_DIR="${BASH_REMATCH[3]}"
fi

# --- Helper Functions ---

get_local_compressor() {
    if command -v pigz >/dev/null 2>&1; then echo "pigz"; else echo "gzip"; fi
}

setup_ssh_mux() {
    if [ -n "$REMOTE_TARGET" ]; then
        log_phase "SSH Handshake"
        log_info "Opening Master Connection to $REMOTE_TARGET..."
        ssh -M -S "$SSH_SOCKET" -fN -o ControlPersist=600 "$REMOTE_TARGET"
        if [ $? -ne 0 ]; then
            log_err "Failed to establish master connection."
            exit 1
        fi

        if ssh -S "$SSH_SOCKET" "$REMOTE_TARGET" "command -v pigz" &>/dev/null; then
            REMOTE_COMP="pigz"
            log_info "Remote: Using pigz (Parallel)"
        else
            REMOTE_COMP="gzip"
            log_info "Remote: Using gzip (Standard)"
        fi
    fi
}

close_ssh_mux() {
    if [ -n "$REMOTE_TARGET" ]; then
        ssh -S "$SSH_SOCKET" -O exit "$REMOTE_TARGET" 2>/dev/null
    fi
}

run_ssh() { ssh -S "$SSH_SOCKET" "$REMOTE_TARGET" "${@}"; }
run_scp() { scp -o ControlPath="$SSH_SOCKET" "$@"; }

get_invalid_chunks() {
    local dir="$1"
    local manifest="$2"
    (cd "$dir" && sha256sum -c "$manifest" --status 2>/dev/null)
    if [ $? -ne 0 ]; then
        cd "$dir" && sha256sum -c "$manifest" 2>&1 | grep "FAILED\|ls: cannot access" | cut -d: -f1
    fi
}

final_success_cleanup() {
    log_phase "Finalizing"

    if [ -f "$SUM_FILE" ]; then
        total_blocks=$(wc -l < "$SUM_FILE")
        total_mb=$((total_blocks * BLOCK_MB))
    else
        total_mb=0
    fi

    rm -f ./"$CHUNK_PREFIX"* "$TMP_ARCHIVE" "$SUM_FILE" 2>/dev/null

    if $IS_SRC_REMOTE || $IS_DST_REMOTE; then
        run_ssh "rm -rf $TMP_REMOTE_DIR" 2>/dev/null
    fi

    close_ssh_mux

    END_TIME=$(date +%s)
    TOTAL_DURATION=$((END_TIME - START_TIME))
    TRANSFER_DURATION=$((END_TIME - TRANSFER_START_TIME))
    [ "$TRANSFER_DURATION" -le 0 ] && TRANSFER_DURATION=1

    avg_speed=$(echo "scale=2; $total_mb / $TRANSFER_DURATION" | bc)

    echo -e "\n${GREEN}${BOLD}Success!${NC}"
    echo -e "Total Data:      ${YELLOW}${total_mb} MB${NC}"
    echo -e "Transfer Time:   ${YELLOW}${TRANSFER_DURATION}s${NC}"
    echo -e "Total Job Time:  ${YELLOW}${TOTAL_DURATION}s${NC}"
    echo -e "Avg Speed:       ${YELLOW}${avg_speed} MB/s${NC}\n"
}

# --- Execution Logic ---

LOCAL_COMP=$(get_local_compressor)
setup_ssh_mux

if $IS_SRC_REMOTE; then
    log_phase "Mode: Remote -> Local"
    RESUME_CHECK=$(run_ssh "[ -f $TMP_REMOTE_DIR/$SUM_FILE ] && echo 'exists' || echo 'missing'")
    if [ "$RESUME_CHECK" == "exists" ]; then
        log_info "Resuming from existing manifest..."
    else
        log_phase "Phase 1: Compression on Source"
        REMOTE_SIZE=$(run_ssh "du -sb '$SRC_REMOTE_DIR' | cut -f1")
        log_info "Remote processing..."
        run_ssh "mkdir -p $TMP_REMOTE_DIR && \
            tar -cf - -C \$(dirname '$SRC_REMOTE_DIR') \$(basename '$SRC_REMOTE_DIR') | $REMOTE_COMP | split -b $BLOCK_SIZE -d -a 4 - $TMP_REMOTE_DIR/$CHUNK_PREFIX && \
            cd $TMP_REMOTE_DIR && sha256sum $CHUNK_PREFIX* > $SUM_FILE"
    fi

    log_phase "Phase 2: Syncing Manifest"
    mkdir -p "$DEST_PATH"
    run_scp "$REMOTE_TARGET:$TMP_REMOTE_DIR/$SUM_FILE" "$DEST_PATH/" >/dev/null

    log_phase "Phase 3: Parallel Job (Transfer)"
    RETRY=0
    # FIX: Initialize start time once before the loop
    TRANSFER_START_TIME=$(date +%s)
    while [ $RETRY -lt $MAX_RETRIES ]; do
        INVALID=$(get_invalid_chunks "$DEST_PATH" "$SUM_FILE")
        [ -z "$INVALID" ] && break
        ((RETRY++))
        echo "$INVALID" | parallel -j "$JOBS" --bar "scp -o ControlPath=$SSH_SOCKET $REMOTE_TARGET:$TMP_REMOTE_DIR/{} $DEST_PATH/ >/dev/null 2>&1"
    done

    log_phase "Phase 4: Decompression on Destination"
    TOTAL_SIZE=$(du -cb "$DEST_PATH"/"$CHUNK_PREFIX"* | tail -n1 | cut -f1)
    cat "$DEST_PATH"/"$CHUNK_PREFIX"* | pv -s "$TOTAL_SIZE" --name "Decompressing" | $LOCAL_COMP -d | tar -xf - -C "$DEST_PATH"
    final_success_cleanup

elif $IS_DST_REMOTE; then
    log_phase "Mode: Local -> Remote"
    if [ -f "$SUM_FILE" ]; then
        log_info "Resuming from existing manifest..."
    else
        log_phase "Phase 1: Compression on Source"
        SRC_SIZE=$(du -sb "$SOURCE_PATH" | cut -f1)
        tar -cf - -C "$(dirname "$SOURCE_PATH")" "$(basename "$SOURCE_PATH")" | \
            pv -s "$SRC_SIZE" --name "Compressing" | \
            $LOCAL_COMP | split -b "$BLOCK_SIZE" -d -a 4 - "$CHUNK_PREFIX"
        sha256sum "$CHUNK_PREFIX"* > "$SUM_FILE"
    fi

    run_ssh "mkdir -p '$DST_REMOTE_DIR' '$TMP_REMOTE_DIR'"
    run_scp "$SUM_FILE" "$REMOTE_TARGET:$TMP_REMOTE_DIR/" >/dev/null

    log_phase "Phase 2: Parallel Job (Transfer)"
    RETRY=0
    # FIX: Initialize start time once before the loop
    TRANSFER_START_TIME=$(date +%s)
    while [ $RETRY -lt $MAX_RETRIES ]; do
        INVALID=$(run_ssh "cd $TMP_REMOTE_DIR && sha256sum -c $SUM_FILE 2>&1 | grep 'FAILED\|cannot access' | cut -d: -f1" | tr -d '\r')
        [ -z "$INVALID" ] && break
        ((RETRY++))
        echo "$INVALID" | parallel -j "$JOBS" --bar "scp -o ControlPath=$SSH_SOCKET {} $REMOTE_TARGET:$TMP_REMOTE_DIR/ >/dev/null 2>&1"
    done

    log_phase "Phase 3: Decompression on Destination"
    log_info "Processing final files on remote server..."
    run_ssh "cat $TMP_REMOTE_DIR/$CHUNK_PREFIX* | $REMOTE_COMP -d | tar -xf - -C $DST_REMOTE_DIR"
    final_success_cleanup

else
    log_phase "Mode: Local -> Local"
    if [ ! -f "$SUM_FILE" ]; then
        log_phase "Phase 1: Compression on Source"
        SRC_SIZE=$(du -sb "$SOURCE_PATH" | cut -f1)
        tar -cf - -C "$(dirname "$SOURCE_PATH")" "$(basename "$SOURCE_PATH")" | \
            pv -s "$SRC_SIZE" --name "Compressing" | \
            $LOCAL_COMP | split -b "$BLOCK_SIZE" -d -a 4 - "$CHUNK_PREFIX"
        sha256sum "$CHUNK_PREFIX"* > "$SUM_FILE"
        mkdir -p "$DEST_PATH"
        cp "$SUM_FILE" "$DEST_PATH/"
    fi

    log_phase "Phase 2: Parallel Job (Moving)"
    # FIX: Initialize start time once
    TRANSFER_START_TIME=$(date +%s)
    ls "$CHUNK_PREFIX"* | parallel -j "$JOBS" --bar cp -n {} "$DEST_PATH/"

    log_phase "Phase 3: Decompression on Destination"
    TOTAL_SIZE=$(du -cb "$DEST_PATH"/"$CHUNK_PREFIX"* | tail -n1 | cut -f1)
    cat "$DEST_PATH"/"$CHUNK_PREFIX"* | pv -s "$TOTAL_SIZE" --name "Decompressing" | $LOCAL_COMP -d | tar -xf - -C "$DEST_PATH"
    final_success_cleanup
fi
