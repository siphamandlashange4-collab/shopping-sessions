#!/bin/sh
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
# Let mimalloc back its segments with transparent huge pages when the kernel
# allows them: far fewer page faults (which contend on a shared lock across
# threads) and a much faster teardown at exit. Harmless where huge pages are
# disabled.
export MIMALLOC_ALLOW_LARGE_OS_PAGES=1
exec /app/pipeline
