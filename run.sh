#!/bin/sh
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
# Back glibc's heap with transparent huge pages when the kernel allows them:
# far fewer page faults, which contend on a shared lock across threads.
# Harmless where huge pages are disabled.
export GLIBC_TUNABLES="glibc.malloc.hugetlb=1"
exec /app/pipeline
