#!/bin/sh
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
# Reuse arena allocations and reduce page faults on large CSV inputs.
export GLIBC_TUNABLES="glibc.malloc.hugetlb=1:glibc.malloc.mmap_threshold=33554432:glibc.malloc.trim_threshold=4294967295:glibc.malloc.top_pad=67108864"
exec /app/pipeline
