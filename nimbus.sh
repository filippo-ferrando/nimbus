#!/bin/bash

# Usage: nimbus.sh <source_path> <destination_path> <block_size_mb> <parallel_jobs>
# The script detects remote hosts if paths are provided as "user@host:/path"
# NOTE: This script relies on SSH keys and an active SSH agent for non-interactive
# authentication to prevent 'scp' from hanging when run in parallel.

# --- Configuration ---
TMP_ARCHIVE="archive.tar"
TMP_REMOTE_DIR="/tmp/chunk_transfer" # Temporary directory on the remote host
CHUNK_PREFIX="archive.part."
MAX_RETRIES=5 # Maximum number of times to retry missing chunks
# ---------------------

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <source_path> <destination_path> <block_size_mb> <parallel_jobs>"
  exit 1
fi

# --- Argument Parsing ---
SOURCE_PATH="$1"
DEST_PATH="$2"
BLOCK_MB="$3"
JOBS="$4"

# Calculate block size in bytes
BLOCK_SIZE=$((BLOCK_MB * 1024 * 1024))

# --- Remote Host Detection & Parsing (omitted for brevity, assume correct) ---
REMOTE_PATTERN='^([^@]+)@([^:]+):(.+)$'
IS_SOURCE_REMOTE=false
IS_DEST_REMOTE=false
SOURCE_REMOTE_TARGET=""
DEST_REMOTE_TARGET=""

# Check Source Path
if [[ "$SOURCE_PATH" =~ $REMOTE_PATTERN ]]; then
  IS_SOURCE_REMOTE=true
  SOURCE_REMOTE_TARGET="${BASH_REMATCH[1]}@${BASH_REMATCH[2]}"
  SOURCE_REMOTE_DIR="${BASH_REMATCH[3]}"
  echo "Source is Remote: ${SOURCE_REMOTE_TARGET}"
fi

# Check Destination Path
if [[ "$DEST_PATH" =~ $REMOTE_PATTERN ]]; then
  IS_DEST_REMOTE=true
  DEST_REMOTE_TARGET="${BASH_REMATCH[1]}@${BASH_REMATCH[2]}"
  DEST_REMOTE_DIR="${BASH_REMATCH[3]}"
  echo "Destination is Remote: ${DEST_REMETE_TARGET}"
fi

# --- Validate Scenarios (omitted for brevity) ---
if $IS_SOURCE_REMOTE && $IS_DEST_REMOTE; then
  echo "Error: Transfer between two remote hosts is not supported in this script."
  exit 1
fi

if $IS_SOURCE_REMOTE; then
  REMOTE_TARGET="$SOURCE_REMOTE_TARGET"
  REMOTE_PATH_TO_ARCHIVE="$SOURCE_REMOTE_DIR"
  LOCAL_DEST="$DEST_PATH"
  echo "--- Remote-to-Local Transfer Mode ---"
elif $IS_DEST_REMOTE; then
  REMOTE_TARGET="$DEST_REMOTE_TARGET"
  REMOTE_DEST="$DEST_REMOTE_DIR"
  LOCAL_SOURCE="$SOURCE_PATH"
  echo "--- Local-to-Remote Transfer Mode ---"
else
  LOCAL_SOURCE="$SOURCE_PATH"
  LOCAL_DEST="$DEST_PATH"
  echo "--- Local-to-Local Transfer Mode ---"
fi

# --- Functions (omitted for brevity, assume correct) ---

run_remote() {
  if [ -n "$REMOTE_TARGET" ]; then
    ssh -t "$REMOTE_TARGET" "$@"
  else
    exit 1
  fi
}

# Generic cleanup function to use on hard failure (omitted for brevity)
final_cleanup() {
  echo "--- Final Cleanup and Exit ---"
  # Local cleanup
  rm -f ./"$CHUNK_PREFIX"*
  rm -f "$LOCAL_DEST"/"$CHUNK_PREFIX"*
  # Remote cleanup (attempt best effort)
  if [ -n "$REMOTE_TARGET" ]; then
    run_remote "rm -rf ${TMP_REMOTE_DIR}"
    run_remote "rm -f ${REMOTE_DEST}/${TMP_ARCHIVE}" 2>/dev/null
  fi
  exit 1
}

# --- Core Logic ---

if $IS_SOURCE_REMOTE; then
  # ==================================
  #         REMOTE-TO-LOCAL
  # ==================================

  echo "1. Preparing remote host (${REMOTE_TARGET})..."
  run_remote "mkdir -p ${TMP_REMOTE_DIR}"

  echo "2. Creating and splitting archive on remote host..."
  run_remote "tar -cvf ${TMP_REMOTE_DIR}/${TMP_ARCHIVE} -C $(dirname "$REMOTE_PATH_TO_ARCHIVE") $(basename "$REMOTE_PATH_TO_ARCHIVE")"
  run_remote "split -b ${BLOCK_SIZE} -d -a 4 ${TMP_REMOTE_DIR}/${TMP_ARCHIVE} ${TMP_REMOTE_DIR}/${CHUNK_PREFIX}"
  run_remote "rm -f ${TMP_REMOTE_DIR}/${TMP_ARCHIVE}"

  # Get the list of all created chunks on the remote host
  # CRITICAL FIX: Use 'tr -d "\r"' to remove carriage returns often introduced by SSH/terminal settings
  REMOTE_CHUNKS_LIST_PATHS=$(run_remote "ls ${TMP_REMOTE_DIR}/${CHUNK_PREFIX}* 2>/dev/null" | tr -d '\r')
  NUM_CHUNKS=$(echo "$REMOTE_CHUNKS_LIST_PATHS" | wc -l)
  echo "Expected chunks: ${NUM_CHUNKS}"

  if [ "$NUM_CHUNKS" -eq 0 ]; then
    echo "Error: No chunks were created on remote host. Check source path."
    final_cleanup
  fi

  echo "3. Transferring chunks to local destination (parallel=${JOBS})..."
  mkdir -p "$LOCAL_DEST"

  MISSING_CHUNKS="$REMOTE_CHUNKS_LIST_PATHS" # Start with all chunks
  RETRY_COUNT=0

  while [ -n "$MISSING_CHUNKS" ] && [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    echo "--- Transfer attempt #$RETRY_COUNT. Missing: $(echo "$MISSING_CHUNKS" | wc -l) ---"

    # Transfer only the missing chunks, suppressing stdout
    # FIX: Use -d '\n' with parallel to ensure it uses only newlines as delimiters, ignoring any left-over garbage
    echo "$MISSING_CHUNKS" | parallel -j "$JOBS" -d '\n' "scp ${REMOTE_TARGET}:{} ${LOCAL_DEST}/ > /dev/null"

    # --- INTEGRITY CHECK & FIND MISSING ---

    # 1. Get the list of chunk *names* (not full paths) expected
    # Fix: Use 'printf' to correctly pipe the list to xargs/basename
    REMOTE_CHUNKS_NAMES=$(printf "%s" "$REMOTE_CHUNKS_LIST_PATHS" | xargs -n 1 basename | sort)

    # 2. Get the list of chunk *names* currently transferred to local destination
    LOCAL_CHUNKS_NAMES=$(ls "$LOCAL_DEST"/"$CHUNK_PREFIX"* 2>/dev/null | xargs -n 1 basename | sort)

    ACTUAL_CHUNKS=$(echo "$LOCAL_CHUNKS_NAMES" | wc -l)

    if [ "$ACTUAL_CHUNKS" -eq "$NUM_CHUNKS" ]; then
      MISSING_CHUNKS="" # Transfer complete
      break
    fi

    # 3. Use 'comm' to find files present in the expected list but NOT in the local list (missing)
    MISSING_NAMES=$(comm -23 <(echo "$REMOTE_CHUNKS_NAMES") <(echo "$LOCAL_CHUNKS_NAMES"))

    # Reconstruct the full path list for the next scp attempt
    MISSING_CHUNKS=""
    for name in $MISSING_NAMES; do
      # This variable construction is robust against newlines
      MISSING_CHUNKS+="${TMP_REMOTE_DIR}/${name}\n"
    done

    MISSING_CHUNKS=$(echo -e "$MISSING_CHUNKS" | sed '/^$/d') # Clean up list

    if [ -n "$MISSING_CHUNKS" ]; then
      echo "Found $(echo "$MISSING_CHUNKS" | wc -l) chunks still missing. Retrying..."
    fi
  done

  if [ -n "$MISSING_CHUNKS" ]; then
    echo "ERROR: Failed to transfer all chunks after $MAX_RETRIES retries. Missing $(echo "$MISSING_CHUNKS" | wc -l) chunks."
    final_cleanup
  fi

  echo "All $NUM_CHUNKS chunks successfully transferred."

  # Final steps after successful transfer
  echo "4. Reassembling archive locally in destination..."
  cat "$LOCAL_DEST"/"$CHUNK_PREFIX"* >"$LOCAL_DEST"/"$TMP_ARCHIVE"

  echo "5. Extracting archive locally..."
  tar -xvf "$LOCAL_DEST/$TMP_ARCHIVE" -C "$LOCAL_DEST"

  echo "6. Cleaning up..."
  rm -f "$LOCAL_DEST"/"$CHUNK_PREFIX"*
  rm -f "$LOCAL_DEST/$TMP_ARCHIVE"
  run_remote "rm -rf ${TMP_REMOTE_DIR}"

elif $IS_DEST_REMOTE; then
  # ... (LOCAL-TO-REMOTE section remains the same, as local ls output is clean) ...

  echo "1. Creating and splitting archive locally..."
  tar -cvf "$TMP_ARCHIVE" -C "$(dirname "$LOCAL_SOURCE")" "$(basename "$LOCAL_SOURCE")"
  split -b "$BLOCK_SIZE" -d -a 4 "$TMP_ARCHIVE" "$CHUNK_PREFIX"
  rm -f "$TMP_ARCHIVE"

  # List of local chunks (Source count)
  LOCAL_CHUNKS_LIST_NAMES=$(ls "$CHUNK_PREFIX"* 2>/dev/null)
  NUM_CHUNKS=$(echo "$LOCAL_CHUNKS_LIST_NAMES" | wc -l)
  echo "Expected chunks: ${NUM_CHUNKS}"

  echo "2. Preparing remote host (${REMOTE_TARGET})..."
  run_remote "mkdir -p ${REMOTE_DEST}"
  run_remote "mkdir -p ${TMP_REMOTE_DIR}" # Temporary folder on remote for chunks

  echo "3. Transferring chunks to remote host (parallel=${JOBS})..."

  MISSING_CHUNKS="$LOCAL_CHUNKS_LIST_NAMES" # Start with all chunks
  RETRY_COUNT=0

  while [ -n "$MISSING_CHUNKS" ] && [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    echo "--- Transfer attempt #$RETRY_COUNT. Missing: $(echo "$MISSING_CHUNKS" | wc -l) ---"

    # Transfer only the missing chunks, suppressing stdout
    echo "$MISSING_CHUNKS" | parallel -j "$JOBS" -d '\n' "scp {} ${REMOTE_TARGET}:${TMP_REMOTE_DIR}/ > /dev/null"

    # --- INTEGRITY CHECK & FIND MISSING ---

    # 1. Get the list of chunk names expected (Source list)
    EXPECTED_NAMES=$(echo "$LOCAL_CHUNKS_LIST_NAMES" | sort)

    # 2. Get the list of chunk names currently transferred to remote destination (Destination list)
    # Clean remote ls output here too, just in case, though the transfer target is remote.
    REMOTE_CHUNKS_NAMES=$(run_remote "ls ${TMP_REMOTE_DIR}/${CHUNK_PREFIX}* 2>/dev/null" | tr -d '\r' | xargs -n 1 basename | sort)

    ACTUAL_CHUNKS=$(echo "$REMOTE_CHUNKS_NAMES" | wc -l)

    if [ "$ACTUAL_CHUNKS" -eq "$NUM_CHUNKS" ]; then
      MISSING_CHUNKS="" # Transfer complete
      break
    fi

    # 3. Use 'comm' to find files present in the expected list but NOT in the remote list (missing)
    MISSING_CHUNKS=$(comm -23 <(echo "$EXPECTED_NAMES") <(echo "$REMOTE_CHUNKS_NAMES"))

    MISSING_CHUNKS=$(echo -e "$MISSING_CHUNKS" | sed '/^$/d') # Clean up list

    if [ -n "$MISSING_CHUNKS" ]; then
      echo "Found $(echo "$MISSING_CHUNKS" | wc -l) chunks still missing. Retrying..."
    fi
  done

  if [ -n "$MISSING_CHUNKS" ]; then
    echo "ERROR: Failed to transfer all chunks after $MAX_RETRIES retries. Missing $(echo "$MISSING_CHUNKS" | wc -l) chunks."
    final_cleanup
  fi

  echo "All $NUM_CHUNKS chunks successfully transferred."

  # Final steps after successful transfer
  echo "4. Reassembling archive on remote host..."
  run_remote "cat ${TMP_REMOTE_DIR}/${CHUNK_PREFIX}* > ${REMOTE_DEST}/${TMP_ARCHIVE}"

  echo "5. Extracting archive on remote host..."
  run_remote "tar -xvf ${REMOTE_DEST}/${TMP_ARCHIVE} -C ${REMOTE_DEST}"

  echo "6. Cleaning up..."
  rm -f ./"$CHUNK_PREFIX"*
  run_remote "rm -rf ${TMP_REMOTE_DIR}"
  run_remote "rm -f ${REMOTE_DEST}/${TMP_ARCHIVE}"

else
  # ... (LOCAL-TO-LOCAL section remains the same) ...

  echo "1. Creating and splitting archive locally..."
  tar -cvf "$TMP_ARCHIVE" -C "$(dirname "$LOCAL_SOURCE")" "$(basename "$LOCAL_SOURCE")"
  split -b "$BLOCK_SIZE" -d -a 4 "$TMP_ARCHIVE" "$CHUNK_PREFIX"
  rm -f "$TMP_ARCHIVE"

  LOCAL_CHUNKS_LIST=$(ls "$CHUNK_PREFIX"* 2>/dev/null)
  NUM_CHUNKS=$(echo "$LOCAL_CHUNKS_LIST" | wc -l)
  echo "Expected chunks: ${NUM_CHUNKS}"

  echo "2. Transferring chunks to destination (parallel=${JOBS})..."
  mkdir -p "$LOCAL_DEST"

  # Use mv for local transfer
  echo "$LOCAL_CHUNKS_LIST" | parallel -j "$JOBS" mv {} "$LOCAL_DEST/"

  # --- INTEGRITY CHECK ---
  ACTUAL_CHUNKS=$(ls "$LOCAL_DEST"/"$CHUNK_PREFIX"* 2>/dev/null | wc -l)
  echo "Actual chunks transferred: ${ACTUAL_CHUNKS}"

  if [ "$ACTUAL_CHUNKS" -ne "$NUM_CHUNKS" ]; then
    echo "ERROR: Chunk count mismatch! Expected $NUM_CHUNKS, found $ACTUAL_CHUNKS."
    final_cleanup
  fi
  # -----------------------

  echo "3. Reassembling archive in destination..."
  cat "$LOCAL_DEST"/"$CHUNK_PREFIX"* >"$LOCAL_DEST"/"$TMP_ARCHIVE"

  echo "4. Extracting archive..."
  tar -xvf "$LOCAL_DEST/$TMP_ARCHIVE" -C "$LOCAL_DEST"

  echo "5. Cleaning up..."
  rm -f "$LOCAL_DEST"/"$CHUNK_PREFIX"*
  rm -f "$LOCAL_DEST/$TMP_ARCHIVE"
fi

echo "Done."
