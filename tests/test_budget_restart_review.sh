#!/usr/bin/env bash
set -u
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${ROOT_DIR}/master_monitor.sh"
for helper in read_budget_setting normalize_budget_logical read_budget_logical resolve_compact_flags \
    get_deficit_compact_required_terms find_all_deficit_compact_budget_states find_all_time_avg_budget_states \
    find_common_budget_state resolve_budget_dir time_avg_states_for_file find_compact_restart_state \
    process_budget_restarts_for_file validate_compact_precursor set_namelist_value_in_section \
    disable_budget_restart update_budget_namelist set_namelist_value latest_common_restart_tid \
    cleanup_input_stages commit_staged_input _update_input_files _restore_input_files; do
    source <(sed -n "/^${helper}()/,/^}/p" "${SCRIPT}")
done
tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT
MONITOR_LOG="${tmpdir}/monitor.log"
mlog() { printf '%s\n' "$*" >> "${MONITOR_LOG}"; }
emergency_deadline_exceeded() { return 1; }
assert_equal() {
    [[ "$1" == "$2" ]] || { printf 'FAIL: %s: expected [%s], got [%s]\n' "$3" "$2" "$1" >&2; exit 1; }
}
assert_status() {
    local expected="$1" rc=0
    shift
    "$@" >/dev/null || rc=$?
    assert_equal "${rc}" "${expected}" "status of $*"
}
fixture="${tmpdir}/parser.in"
printf '&G a=1 /\n&G a=2 /\n' > "${fixture}"
assert_equal "$(read_budget_setting "${fixture}" G a fallback)" 1 'first group'
printf '&G a=1, a=2 /\n' > "${fixture}"
assert_equal "$(read_budget_setting "${fixture}" G a fallback)" 2 'last assignment'
printf '&G b="x!y/z" a=1 /\n' > "${fixture}"
assert_equal "$(read_budget_setting "${fixture}" G b fallback)" 'x!y/z' 'quoted punctuation'
assert_equal "$(read_budget_setting "${fixture}" G a fallback)" 1 'adjacent scalar'
printf '&G b="a""b" /\n' > "${fixture}"
assert_equal "$(read_budget_setting "${fixture}" G b fallback)" 'a"b' 'doubled quotes'
printf '&G_LONG a=9 /\n' > "${fixture}"
assert_equal "$(read_budget_setting "${fixture}" G a fallback)" fallback 'exact group name'
assert_status 3 read_budget_setting "${fixture}" G a fallback required
printf '&G a="unterminated\n' > "${fixture}"
assert_status 3 read_budget_setting "${fixture}" G a fallback
printf '&G\r\n a=1\r\n /\r\n' > "${fixture}"
assert_equal "$(read_budget_setting "${fixture}" G a fallback)" 1 'CRLF'
printf '&G a=1 &END\n&G a=2 /\n' > "${fixture}"
assert_equal "$(read_budget_setting "${fixture}" G a fallback)" 1 '&END closes first group'
printf '&G a=/abs/path /\n' > "${fixture}"
assert_status 3 read_budget_setting "${fixture}" G a fallback
printf '&G a= /\n' > "${fixture}"
assert_status 3 read_budget_setting "${fixture}" G a fallback
printf '&G a=2*.true. /\n' > "${fixture}"
assert_status 3 read_budget_logical "${fixture}" G a .false.

mkdir "${tmpdir}/compact" "${tmpdir}/average" "${tmpdir}/primary_restarts" "${tmpdir}/precursor_restarts"
PRIMARY_INPUTFILE="${tmpdir}/primary.in"
PRECURSOR_INPUTFILE="${tmpdir}/precursor.in"
PRIMARY_INPUTDIR="${tmpdir}/primary_restarts"
PRECURSOR_INPUTDIR="${tmpdir}/precursor_restarts"
PRIMARY_RUNID=7; PRIMARY_RID_PAD=07
PRECURSOR_RUNID=6; PRECURSOR_RID_PAD=06
SLURM_JOB_ID=123
INPUT_UPDATE_IN_PROGRESS=0
PRIMARY_INPUT_STAGE=""; PRECURSOR_INPUT_STAGE=""
BUDGET_RESTART_BLOCKED=0
BUDGET_FAILURE_POLICY=abort
cat > "${PRIMARY_INPUTFILE}" <<DATA
&INPUT
 useRestartFile=.false.
 restartFile_TID=0
 restartFile_RID=0
 outputdir='${tmpdir}/compact'
/
&BUDGET_TIME_AVG_DEFICIT_COMPACT
 do_budgets=.true.
 do_budget3=.true.
 tidx_compute=10
 budgets_dir='NULL'
 restart_budgets=.false.
 restart_tid=0
 restart_counter=0
 restart_rid=0
/
DATA
cat > "${PRECURSOR_INPUTFILE}" <<DATA
&INPUT
 useRestartFile=.false.
 restartFile_TID=0
 restartFile_RID=0
 outputdir='${tmpdir}/average'
/
&BUDGET_TIME_AVG
 do_budgets=.true.
 tidx_compute=10
 restart_budgets=.false.
 restart_tid=0
 restart_counter=0
 restart_rid=0
/
DATA
assert_equal "$(resolve_budget_dir "${PRIMARY_INPUTFILE}" BUDGET_TIME_AVG_DEFICIT_COMPACT)" "${tmpdir}/compact" 'NULL uses outputdir'
assert_equal "$(resolve_budget_dir "${PRECURSOR_INPUTFILE}" BUDGET_TIME_AVG)" "${tmpdir}/average" 'omitted directory uses outputdir'
assert_status 0 validate_compact_precursor "${PRIMARY_INPUTFILE}" "${PRECURSOR_INPUTFILE}" config
# budgetType is deliberately absent: the solver default of 1 satisfies budget3.
set_namelist_value_in_section "${PRECURSOR_INPUTFILE}" BUDGET_TIME_AVG tidx_compute 20
assert_status 3 validate_compact_precursor "${PRIMARY_INPUTFILE}" "${PRECURSOR_INPUTFILE}" config
set_namelist_value_in_section "${PRECURSOR_INPUTFILE}" BUDGET_TIME_AVG tidx_compute 10
set_namelist_value_in_section "${PRECURSOR_INPUTFILE}" BUDGET_TIME_AVG restart_dir "'${tmpdir}/wrong'"
assert_status 3 resolve_budget_dir "${PRECURSOR_INPUTFILE}" BUDGET_TIME_AVG
set_namelist_value_in_section "${PRECURSOR_INPUTFILE}" BUDGET_TIME_AVG restart_dir "'${tmpdir}/average/../average'"
assert_status 0 resolve_budget_dir "${PRECURSOR_INPUTFILE}" BUDGET_TIME_AVG

write_compact() {
    local counter="$1" budget term
    for budget in 0 1 2 3; do
        case "${budget}" in
            0) terms='1 2 3 4 6 7 8 9 10 11 12 13 14 15 16 18 19 20' ;;
            1|2) terms='1 2 3 4 5 6 7 8 9 10 11 12 13 14 15' ;;
            3) terms='1 2 3 4 5 6 7 8 9 13 14 15 16 17 18 19' ;;
        esac
        for term in ${terms}; do
            touch "${tmpdir}/compact/Run07_comp_deficit_budget${budget}_term$(printf '%02d' "${term}")_t001000_n$(printf '%06d' "${counter}").s3D"
        done
    done
}
write_average() {
    local counter="$1" term budget count
    for budget in 0 1; do
        count=31; [[ "${budget}" -eq 0 ]] || count=16
        for ((term=1; term<=count; term++)); do
            touch "${tmpdir}/average/Run06_budget${budget}_term$(printf '%02d' "${term}")_t001000_n$(printf '%06d' "${counter}").s3D"
        done
    done
}
write_compact 3
write_compact 5
write_average 3
write_average 4
assert_equal "$(find_all_deficit_compact_budget_states "${tmpdir}/compact" 07 0 1000 "${PRIMARY_INPUTFILE}")" $'1000,5\n1000,3' 'higher counter first'
assert_status 3 find_all_deficit_compact_budget_states "${tmpdir}/compact" 07 0 '' "${PRIMARY_INPUTFILE}"
assert_equal "$(find_compact_restart_state "${PRIMARY_INPUTFILE}" 07 1000 "${PRECURSOR_INPUTFILE}" 06)" '1000,3' 'select common pair rather than independent newest counters'
# Scoped scalar settings require 35 budget0 terms when two scalars are active.
cat >> "${PRECURSOR_INPUTFILE}" <<'DATA'
&PHYSICS useScalars=T /
&SCALARS num_scalars=2 /
&OTHER useScalars=F, num_scalars=99 /
DATA
assert_status 1 time_avg_states_for_file "${PRECURSOR_INPUTFILE}" 06 1000
for counter in 3 4; do
    for term in {32..35}; do
        touch "${tmpdir}/average/Run06_budget0_term${term}_t001000_n$(printf '%06d' "${counter}").s3D"
    done
done
assert_equal "$(time_avg_states_for_file "${PRECURSOR_INPUTFILE}" 06 1000)" $'1000,4\n1000,3' 'scoped scalar completeness'
touch "${PRIMARY_INPUTDIR}/RESTART_Run07_info.001000" "${PRECURSOR_INPUTDIR}/RESTART_Run06_info.001000"
assert_status 0 _update_input_files 1000
assert_equal "$(read_budget_setting "${PRIMARY_INPUTFILE}" BUDGET_TIME_AVG_DEFICIT_COMPACT restart_counter '')" 3 'committed shared compact counter'
assert_equal "$(read_budget_setting "${PRECURSOR_INPUTFILE}" BUDGET_TIME_AVG restart_counter '')" 3 'committed shared precursor counter'
# No common pair now: retire all precursor counter-3 files.
mkdir "${tmpdir}/retired"
mv "${tmpdir}"/average/*_n000003.s3D "${tmpdir}/retired/"
assert_status 5 _update_input_files 1000
assert_equal "${BUDGET_RESTART_BLOCKED}" 1 'abort marker set'
log_lines=$(wc -l < "${MONITOR_LOG}")
assert_status 5 _update_input_files 1000
assert_equal "$(wc -l < "${MONITOR_LOG}")" "${log_lines}" 'repeat abort performs no discovery/logging'
BUDGET_RESTART_BLOCKED=0
BUDGET_FAILURE_POLICY=disable-budgets
assert_status 0 _update_input_files 1000
assert_equal "$(read_budget_logical "${PRIMARY_INPUTFILE}" BUDGET_TIME_AVG_DEFICIT_COMPACT restart_budgets T)" .false. 'policy disables compact restart'
assert_equal "$(read_budget_logical "${PRECURSOR_INPUTFILE}" BUDGET_TIME_AVG restart_budgets T)" .false. 'policy disables precursor restart'
# I/O failure is retryable even under disable-budgets, and cannot commit resets.
cp "${PRIMARY_INPUTFILE}" "${tmpdir}/primary.expected"
find() { printf 'simulated directory I/O error\n' >&2; return 1; }
assert_status 4 _update_input_files 1000
assert_status 0 cmp -s "${PRIMARY_INPUTFILE}" "${tmpdir}/primary.expected"
assert_status 0 grep -q 'simulated directory I/O error' "${MONITOR_LOG}"
unset -f find
# Check squeezed output and the component-prefixed budget4 filename family.
full_dir="${tmpdir}/full-average"
mkdir "${full_dir}"
for term in {1..26} 31; do
    touch "${full_dir}/Run06_budget0_term$(printf '%02d' "${term}")_t001000_n000003.s3D"
done
assert_equal "$(find_all_time_avg_budget_states "${full_dir}" 06 4 1000 .true. 31)" '1000,3' 'squeezed required subset'
assert_status 1 find_all_time_avg_budget_states "${full_dir}" 06 4 1000 .false. 31
for budget in 0 1 2 3; do
    case "${budget}" in 0) count=31 ;; 1) count=16 ;; 2) count=10 ;; 3) count=8 ;; esac
    for ((term=1; term<=count; term++)); do
        touch "${full_dir}/Run06_budget${budget}_term$(printf '%02d' "${term}")_t001000_n000003.s3D"
    done
done
for component in 11 22 33 13 23; do
    for term in {1..10}; do
        touch "${full_dir}/Run06_budget4_${component}_term$(printf '%02d' "${term}")_t001000_n000003.s3D"
    done
done
assert_equal "$(find_all_time_avg_budget_states "${full_dir}" 06 4 1000 .false. 31)" '1000,3' 'complete budget4 components'
mv "${full_dir}/Run06_budget4_23_term10_t001000_n000003.s3D" "${tmpdir}/retired/"
assert_status 1 find_all_time_avg_budget_states "${full_dir}" 06 4 1000 .false. 31

printf 'All budget restart review tests passed.\n'
