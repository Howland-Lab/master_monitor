#!/usr/bin/env bash

set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${ROOT_DIR}/master_monitor.sh"

source <(sed -n '/^restart_lock_owner_active()/,/^}/p' "${SCRIPT}")
source <(sed -n '/^acquire_restart_lock()/,/^}/p' "${SCRIPT}")
source <(sed -n '/^release_restart_lock()/,/^}/p' "${SCRIPT}")

mlog() { :; }
squeue() { return 0; }

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

SLURM_JOB_ID=123
RESTART_LOCKFILE="${tmpdir}/restart.lock"
RESTART_LOCK_OWNED=0
RESTART_LOCK_TOKEN=""

acquire_restart_lock
owned_token="${RESTART_LOCK_TOKEN}"

if acquire_restart_lock; then
    printf 'FAIL: active lock was acquired twice\n' >&2
    exit 1
fi

RESTART_LOCK_OWNED=0
release_restart_lock
if [[ ! -d "${RESTART_LOCKFILE}" ]]; then
    printf 'FAIL: non-owner removed lock\n' >&2
    exit 1
fi

RESTART_LOCK_OWNED=1
RESTART_LOCK_TOKEN="${owned_token}"
release_restart_lock
if [[ -e "${RESTART_LOCKFILE}" ]]; then
    printf 'FAIL: owner did not remove lock\n' >&2
    exit 1
fi

mkdir "${RESTART_LOCKFILE}"
printf '999999 999999 remote-host stale-token\n' > "${RESTART_LOCKFILE}/owner"
acquire_restart_lock
if [[ "${RESTART_LOCK_OWNED}" -ne 1 ]]; then
    printf 'FAIL: stale lock was not recovered\n' >&2
    exit 1
fi
release_restart_lock

printf 'All restart-lock tests passed.\n'
