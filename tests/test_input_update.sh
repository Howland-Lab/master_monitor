#!/usr/bin/env bash

set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${ROOT_DIR}/master_monitor.sh"

source <(sed -n '/^set_namelist_value()/,/^}/p' "${SCRIPT}")
source <(sed -n '/^latest_common_restart_tid()/,/^}/p' "${SCRIPT}")
source <(sed -n '/^cleanup_input_stages()/,/^}/p' "${SCRIPT}")
source <(sed -n '/^commit_staged_input()/,/^}/p' "${SCRIPT}")
source <(sed -n '/^_update_input_files()/,/^}/p' "${SCRIPT}")
source <(sed -n '/^_restore_input_files()/,/^}/p' "${SCRIPT}")

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

mlog() { :; }
emergency_deadline_exceeded() { return 1; }
process_budget_restarts_for_file() {
    local mode="$6"
    if [[ "${mode}" == "discover" ]]; then
        echo 1500
    fi
}

PRIMARY_INPUTFILE="${tmpdir}/primary.dat"
PRECURSOR_INPUTFILE="${tmpdir}/precursor.dat"
PRIMARY_INPUTDIR="${tmpdir}/primary_restarts"
PRECURSOR_INPUTDIR="${tmpdir}/precursor_restarts"
PRIMARY_RUNID=7
PRECURSOR_RUNID=6
PRIMARY_RID_PAD=07
PRECURSOR_RID_PAD=06
SLURM_JOB_ID=123
MONITOR_LOG="${tmpdir}/monitor.log"
INPUT_UPDATE_IN_PROGRESS=0
PRIMARY_INPUT_STAGE=""
PRECURSOR_INPUT_STAGE=""

mkdir "${PRIMARY_INPUTDIR}" "${PRECURSOR_INPUTDIR}"
for tid in 1000 2000; do
    touch "${PRIMARY_INPUTDIR}/RESTART_Run07_info.$(printf '%06d' "${tid}")"
    touch "${PRECURSOR_INPUTDIR}/RESTART_Run06_info.$(printf '%06d' "${tid}")"
done

for file in "${PRIMARY_INPUTFILE}" "${PRECURSOR_INPUTFILE}"; do
    cat > "${file}" <<'EOF'
useRestartFile = .false.
restartFile_TID = 0
restartFile_RID = 0
EOF
    chmod 640 "${file}"
done

_update_input_files 2000

for file in "${PRIMARY_INPUTFILE}" "${PRECURSOR_INPUTFILE}"; do
    if ! grep -q '^restartFile_TID = 1000$' "${file}"; then
        printf 'FAIL: budget ceiling selected nonexistent solver TID in %s\n' "${file}" >&2
        exit 1
    fi
    if [[ "$(stat -c %a "${file}")" != "640" ]]; then
        printf 'FAIL: input mode was not preserved for %s\n' "${file}" >&2
        exit 1
    fi
done

printf 'All input-update tests passed.\n'
