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

for helper in read_budget_setting normalize_budget_logical read_budget_logical validate_compact_precursor; do
    source <(sed -n "/^${helper}()/,/^}/p" "${SCRIPT}")
done

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

# A discovery timeout must abort before either real input is committed.
cp "${PRIMARY_INPUTFILE}" "${tmpdir}/primary.expected"
cp "${PRECURSOR_INPUTFILE}" "${tmpdir}/precursor.expected"
process_budget_restarts_for_file() { return 2; }
rc=0
_update_input_files 2000 || rc=$?
if [[ "${rc}" -ne 2 ]] || ! cmp -s "${PRIMARY_INPUTFILE}" "${tmpdir}/primary.expected" || \
   ! cmp -s "${PRECURSOR_INPUTFILE}" "${tmpdir}/precursor.expected"; then
    printf 'FAIL: discovery timeout was swallowed or inputs changed\n' >&2
    exit 1
fi

# Drive phase 3 through real preflight and staged namelist writes. A counter
# mismatch must leave both originals untouched; matching counters may commit.
for helper in set_namelist_value_in_section update_budget_namelist; do
    source <(sed -n "/^${helper}()/,/^}/p" "${SCRIPT}")
done
cat >> "${PRIMARY_INPUTFILE}" <<'DATA'
&BUDGET_TIME_AVG_DEFICIT_COMPACT
 do_budgets=.true.
 do_budget3=.true.
 restart_budgets=.false.
 restart_tid=0
 restart_counter=0
 restart_rid=0
/
DATA
cat >> "${PRECURSOR_INPUTFILE}" <<'DATA'
&BUDGET_TIME_AVG
 tidx_compute=10000
 do_budgets=.true.
 budgetType=1
 restart_budgets=.false.
 restart_tid=0
 restart_counter=0
 restart_rid=0
/
DATA
cp "${PRIMARY_INPUTFILE}" "${tmpdir}/primary.expected"
cp "${PRECURSOR_INPUTFILE}" "${tmpdir}/precursor.expected"
PRE_TEST_COUNTER=4
process_budget_restarts_for_file() {
    if [[ "$6" == discover ]]; then
        echo 1500
    elif [[ "$2" == PRIMARY ]]; then
        update_budget_namelist "$1" BUDGET_TIME_AVG_DEFICIT_COMPACT 7 1000 3
    else
        update_budget_namelist "$1" BUDGET_TIME_AVG 6 1000 "${PRE_TEST_COUNTER}"
    fi
}
if _update_input_files 2000; then
    printf 'FAIL: mismatched compact/precursor counters committed\n' >&2
    exit 1
fi
if ! cmp -s "${PRIMARY_INPUTFILE}" "${tmpdir}/primary.expected" || \
   ! cmp -s "${PRECURSOR_INPUTFILE}" "${tmpdir}/precursor.expected"; then
    printf 'FAIL: counter mismatch changed original inputs\n' >&2
    exit 1
fi
BUDGET_RESTART_BLOCKED=0
PRE_TEST_COUNTER=3
if ! _update_input_files 2000; then
    printf 'FAIL: matching compact/precursor counters rejected\n' >&2
    exit 1
fi
if [[ "$(read_budget_setting "${PRIMARY_INPUTFILE}" BUDGET_TIME_AVG_DEFICIT_COMPACT restart_counter '')" != 3 || \
      "$(read_budget_setting "${PRECURSOR_INPUTFILE}" BUDGET_TIME_AVG restart_counter '')" != 3 ]]; then
    printf 'FAIL: matching counters were not committed\n' >&2
    exit 1
fi

printf 'All input-update tests passed.\n'
