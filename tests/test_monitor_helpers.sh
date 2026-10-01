#!/usr/bin/env bash

set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${ROOT_DIR}/master_monitor.sh"

source <(sed -n '/^normalize_fortran_real()/,/^}/p' "${SCRIPT}")
source <(sed -n '/^next_common_dump_after()/,/^}/p' "${SCRIPT}")
source <(sed -n '/^get_latest_sim_end_estimate()/,/^}/p' "${SCRIPT}")
source <(sed -n '/^parse_slurm_time_to_seconds()/,/^}/p' "${SCRIPT}")
source <(sed -n '/^get_live_job_time_left_seconds()/,/^}/p' "${SCRIPT}")
source <(sed -n '/^latest_common_restart_tid()/,/^}/p' "${SCRIPT}")
source <(sed -n '/^should_write_sim_done()/,/^}/p' "${SCRIPT}")
source <(sed -n '/^get_memory_units_per_task()/,/^}/p' "${SCRIPT}")
source <(sed -n '/^get_latest_logged_tidx()/,/^}/p' "${SCRIPT}")
source <(sed -n '/^get_end_shutdown_action()/,/^}/p' "${SCRIPT}")

failures=0

assert_eq() {
    local expected="$1"
    local actual="$2"
    local label="$3"
    if [[ "${actual}" != "${expected}" ]]; then
        printf 'FAIL: %s: expected=%s actual=%s\n' "${label}" "${expected}" "${actual}" >&2
        failures=$(( failures + 1 ))
    fi
}

assert_fails() {
    local label="$1"
    shift
    if "$@" >/dev/null 2>&1; then
        printf 'FAIL: %s: command unexpectedly succeeded\n' "${label}" >&2
        failures=$(( failures + 1 ))
    fi
}

assert_eq "16050" "$(normalize_fortran_real 16050)" "plain integer"
assert_eq "16050" "$(normalize_fortran_real 16050.0d0)" "Fortran D notation"
assert_eq "16050" "$(normalize_fortran_real 1.6050E+04)" "scientific E notation"
assert_fails "invalid real" normalize_fortran_real invalid

assert_eq "78000" \
    "$(next_common_dump_after 76364 1000 2000)" \
    "aligned unequal intervals"
assert_eq "3000" "$(next_common_dump_after 1500 1000 1500)" "LCM of unequal intervals"
assert_eq "50000" "$(next_common_dump_after 49196 1000 1000)" "off-cadence restart"
assert_eq "53000" "$(next_common_dump_after 52201 1000 1000)" "reported regression"
assert_eq "799" "$(( $(next_common_dump_after 52201 1000 1000) - 52201 ))" "steps to absolute dump"
assert_eq "54000" "$(next_common_dump_after 53000 1000 1000)" "strictly future dump"
assert_eq "1000" "$(next_common_dump_after 0 1000 1000)" "fresh simulation"
assert_fails "zero interval" next_common_dump_after 10 0 1000
assert_fails "negative interval" next_common_dump_after 10 1000 -1

# Exercise the actual monitor target-selection block without starting a job.
check_time_target() {
    local CURRENT_TIDX="$1" SIM_END_ESTIMATE_VALID="$2" STEPS_TO_END="$3"
    local PRIMARY_T_RESTART_DUMP=1000 PRECURSOR_T_RESTART_DUMP=1000
    local PRIMARY_RESTART_TID=49196 PRECURSOR_RESTART_TID=49196
    local CURRENT_SIM_TIME_FMT=0 CURRENT_SIM_DT_FMT=1
    local NEXT_COMMON_DUMP STEPS_TO_DUMP STEPS_TO_TARGET TARGET_KIND END_ESTIMATE_LOG
    source <(sed -n '/^        NEXT_COMMON_DUMP=/,/^        if \[\[ -z "${STEPS_TO_TARGET}" \]\]; then/p' "${SCRIPT}" | sed '$d')
    printf '%s:%s\n' "${TARGET_KIND}" "${STEPS_TO_TARGET}"
}
assert_eq "end:200" "$(check_time_target 53000 1 200)" "completion at 53200 precedes dump at 54000"
assert_eq "common_dump:799" "$(check_time_target 52201 1 999)" "dump precedes completion at 53200"
assert_eq "common_dump:1000" "$(check_time_target 53000 0 '')" "dump used when completion unknown"

SLURM_JOB_ID=123
scontrol() {
    printf 'JobId=123 RunTime=00:30:00 TimeLimit=02:00:00\n'
}
assert_eq "5400" "$(get_live_job_time_left_seconds)" "live Slurm time left"

restart_primary="$(mktemp -d)"
restart_precursor="$(mktemp -d)"
log_fixture=""
trap 'rm -f "${log_fixture}"; rm -rf "${restart_primary}" "${restart_precursor}"' EXIT
for tid in 1000 2000 3000; do
    touch "${restart_primary}/RESTART_Run07_info.$(printf '%06d' "${tid}")"
done
for tid in 1000 2000 4000; do
    touch "${restart_precursor}/RESTART_Run06_info.$(printf '%06d' "${tid}")"
done
assert_eq "2000" \
    "$(latest_common_restart_tid "${restart_primary}" 07 "${restart_precursor}" 06 2500)" \
    "common solver restart below budget ceiling"

if should_write_sim_done 0 0 0; then
    printf 'FAIL: premature clean exit would write SIM_DONE\n' >&2
    failures=$(( failures + 1 ))
fi
if ! should_write_sim_done 0 1 0; then
    printf 'FAIL: verified clean completion would not write SIM_DONE\n' >&2
    failures=$(( failures + 1 ))
fi

assert_eq "1" "$(get_memory_units_per_task core 2 2)" "two SMT CPUs share one physical core"
assert_eq "2" "$(get_memory_units_per_task core 3 2)" "three SMT CPUs occupy two physical cores"
assert_eq "2" "$(get_memory_units_per_task cpu 2 2)" "logical CPU accounting"
assert_eq "terminate-memory" \
    "$(get_end_shutdown_action 1 0 300 1000 180)" \
    "memory hard stop wins after tstop"
assert_eq "terminate-time" \
    "$(get_end_shutdown_action 0 300 300 1000 180)" \
    "shutdown grace expiration"
assert_eq "wait" \
    "$(get_end_shutdown_action 0 10 300 1000 180)" \
    "normal post-tstop supervision"

log_fixture="$(mktemp)"
cat > "${log_fixture}" <<'EOF'
> Primary Simulation Info:
> Time = 9.0E+00
    > TIDX: = 10
    > Current dt: = 5.0E-01
> Concurrent Simulation Info:
> Time = 8.0E+00
    > TIDX: = 10
    > Current dt: = 2.5E-01
EOF

ELAPSED_STEP_WINDOW=5
read -r frames sim_time sim_dt domains \
    <<< "$(get_latest_sim_end_estimate "${log_fixture}" 10 10)"
assert_eq "8" "${frames}" "maximum domain frames"
assert_eq "8" "${sim_time}" "conservative domain time"
assert_eq "0.25" "${sim_dt}" "conservative domain timestep"
assert_eq "2" "${domains}" "domain count"

cat > "${log_fixture}" <<'EOF'
> Primary Simulation Info:
> Time = 9.0E+00
    > TIDX: = 10
    > Current dt: = 5.0E-01
> Concurrent Simulation Info:
    > Current dt: = 1.0E-02
EOF
read -r frames _ _ domains \
    <<< "$(get_latest_sim_end_estimate "${log_fixture}" 10 10)"
assert_eq "2" "${frames}" "partial domain does not reuse stale fields"
assert_eq "1" "${domains}" "partial domain ignored"

{
    printf '    > TIDX: = 42\n'
    for _ in $(seq 1 150); do
        printf 'verbose diagnostic line\n'
    done
} > "${log_fixture}"
assert_eq "42" "$(get_latest_logged_tidx "${log_fixture}")" "TIDX beyond final 100 lines"

if [[ "${failures}" -ne 0 ]]; then
    printf '%d test(s) failed\n' "${failures}" >&2
    exit 1
fi

printf 'All monitor helper tests passed.\n'
