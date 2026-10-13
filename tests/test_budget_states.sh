#!/usr/bin/env bash

set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${ROOT_DIR}/master_monitor.sh"

source <(sed -n '/^get_deficit_compact_required_terms()/,/^}/p' "${SCRIPT}")
source <(sed -n '/^find_all_deficit_compact_budget_states()/,/^}/p' "${SCRIPT}")

for helper in read_budget_setting normalize_budget_logical read_budget_logical resolve_compact_flags validate_compact_precursor; do
    source <(sed -n "/^${helper}()/,/^}/p" "${SCRIPT}")
done
mlog() { :; }
emergency_deadline_exceeded() { return 1; }

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT
MONITOR_LOG="${tmpdir}/monitor.log"
default_config="${tmpdir}/defaults.in"
touch "${default_config}"

for term in $(seq 1 8); do
    touch "${tmpdir}/Run07_comp_deficit_budget1_term$(printf '%02d' "${term}")_t001000_n000001.s3D"
done
for term in $(seq 9 15); do
    touch "${tmpdir}/Run07_comp_deficit_budget1_term$(printf '%02d' "${term}")_t001000_n000002.s3D"
done

mixed="$(find_all_deficit_compact_budget_states "${tmpdir}" 07 1 1000 "${default_config}" || true)"
if [[ -n "${mixed}" ]]; then
    printf 'FAIL: mixed counters incorrectly accepted: %s\n' "${mixed}" >&2
    exit 1
fi

for term in $(seq 1 15); do
    touch "${tmpdir}/Run07_comp_deficit_budget1_term$(printf '%02d' "${term}")_t001000_n000003.s3D"
done

complete="$(find_all_deficit_compact_budget_states "${tmpdir}" 07 1 1000 "${default_config}")"
if [[ "${complete}" != "1000,3" ]]; then
    printf 'FAIL: complete counter not detected: %s\n' "${complete}" >&2
    exit 1
fi

rm -f "${tmpdir}/Run07_comp_deficit_budget1_term15_t001000_n000003.s3D"
touch "${tmpdir}/Run07_comp_deficit_budget1_term99_t001000_n000003.s3D"
wrong_terms="$(find_all_deficit_compact_budget_states "${tmpdir}" 07 1 1000 "${default_config}" || true)"
if [[ -n "${wrong_terms}" ]]; then
    printf 'FAIL: wrong deficit term set incorrectly accepted: %s\n' "${wrong_terms}" >&2
    exit 1
fi

assert_equal() {
    if [[ "$1" != "$2" ]]; then
        printf 'FAIL: %s: expected [%s], got [%s]\n' "$3" "$2" "$1" >&2
        exit 1
    fi
}

# Explicit file IDs from the solver's dump/restart loops, exercising all
# turbine/pressure/Coriolis/stratification combinations and skipped IDs.
for turbines in .false. .true.; do
    for pressure in .false. .true.; do
        for coriolis in .false. .true.; do
            for stratified in .false. .true.; do
                case_dir="${tmpdir}/${turbines}_${pressure}_${coriolis}_${stratified}"
                mkdir "${case_dir}"
                config="${case_dir}/input.in"
                cat > "${config}" <<EOF
&WINDTURBINES
 useWindTurbines = ${turbines}, ! mixed case and inline assignments
/
&PRESSURE_CALC
 computeTurbinePressure = ${pressure^^},
/
&PHYSICS
 useCoriolis = ${coriolis}, isStratified = ${stratified},
/
EOF
                terms0=(1 2 3 4 6 7 8 9 10 11 12 13 14 18 19 20)
                terms3=(1 2 3 4 5 6 7 8 9 13 14 15 16 17 18 19)
                [[ "${coriolis}" == .true. ]] && terms0+=(15 16)
                if [[ "${stratified}" == .true. ]]; then
                    terms0+=(5 17)
                    terms3+=(10 11 12)
                fi
                if [[ "${turbines}" == .true. ]]; then
                    terms0+=(21 22)
                    terms3+=(20 21)
                    [[ "${pressure}" == .true. ]] && terms0+=(23)
                fi
                for budget in 0 3; do
                    if [[ "${budget}" -eq 0 ]]; then
                        terms=("${terms0[@]}")
                    else
                        terms=("${terms3[@]}")
                    fi
                    for term in "${terms[@]}"; do
                        touch "${case_dir}/Run07_comp_deficit_budget${budget}_term$(printf '%02d' "${term}")_t001000_n000003.s3D"
                    done
                    state=$(find_all_deficit_compact_budget_states "${case_dir}" 07 "${budget}" 1000 "${config}")
                    assert_equal "${state}" '1000,3' "complete budget ${budget}: ${case_dir}"
                    state=$(find_all_deficit_compact_budget_states "${case_dir}" 07 "${budget}" 999 "${config}" || true)
                    assert_equal "${state}" '' 'budget beyond solver restart ceiling'
                    # Replace a required file with an unrelated ID: count alone
                    # must not suffice, including for the appended turbine terms.
                    last_term="${terms[${#terms[@]}-1]}"
                    mv "${case_dir}/Run07_comp_deficit_budget${budget}_term$(printf '%02d' "${last_term}")_t001000_n000003.s3D" \
                       "${case_dir}/Run07_comp_deficit_budget${budget}_term99_t001000_n000003.s3D"
                    state=$(find_all_deficit_compact_budget_states "${case_dir}" 07 "${budget}" 1000 "${config}" || true)
                    assert_equal "${state}" '' "missing required budget ${budget} term ${last_term}"
                done
            done
        done
    done
done

assert_equal "$(get_deficit_compact_required_terms 0 "${default_config}")" \
    "$(printf '%s\n' 1 2 3 4 6 7 8 9 10 11 12 13 14 15 16 18 19 20)" 'igrid defaults for budget 0'
assert_equal "$(get_deficit_compact_required_terms 3 "${default_config}")" \
    "$(printf '%s\n' 1 2 3 4 5 6 7 8 9 13 14 15 16 17 18 19)" 'igrid defaults for budget 3'

# Exercise the actual discovery path: budget 3 implicitly requires budgets 0-2.
for helper in resolve_budget_dir time_avg_states_for_file find_compact_restart_state find_common_budget_state process_budget_restarts_for_file; do
    source <(sed -n "/^${helper}()/,/^}/p" "${SCRIPT}")
done
mlog() { :; }
integration_dir="${tmpdir}/integration"
mkdir "${integration_dir}"
config="${integration_dir}/input.in"
cat > "${config}" <<EOF
&WINDTURBINES
 useWindTurbines = .true.
/
&PRESSURE_CALC
 computeTurbinePressure = .true.
/
&PHYSICS
 useCoriolis = .true.
 isStratified = .true.
/
&BUDGET_TIME_AVG_DEFICIT_COMPACT
 do_budgets = .true.
 do_budget3 = .true.
 budgets_dir = '${integration_dir}'
/
EOF
for budget in 0 1 2 3; do
    case "${budget}" in
        0) count=23 ;;
        1|2) count=15 ;;
        3) count=21 ;;
    esac
    for term in $(seq 1 "${count}"); do
        touch "${integration_dir}/Run07_comp_deficit_budget${budget}_term$(printf '%02d' "${term}")_t001000_n000003.s3D"
    done
done
assert_equal "$(process_budget_restarts_for_file "${config}" PRIMARY 7 07 1000 discover)" '1000' 'discovery with turbine terms'
mv "${integration_dir}/Run07_comp_deficit_budget2_term15_t001000_n000003.s3D" \
   "${integration_dir}/Run07_comp_deficit_budget2_term15_t001000_n000004.s3D"
assert_equal "$(process_budget_restarts_for_file "${config}" PRIMARY 7 07 1000 discover)" '' 'implicit dependency with mismatched counter'

# Extra files must not disqualify an otherwise complete required subset.
mv "${integration_dir}/Run07_comp_deficit_budget2_term15_t001000_n000004.s3D" \
   "${integration_dir}/Run07_comp_deficit_budget2_term15_t001000_n000003.s3D"
touch "${integration_dir}/Run07_comp_deficit_budget0_term99_t001000_n000003.s3D"
assert_equal "$(process_budget_restarts_for_file "${config}" PRIMARY 7 07 1000 discover)" '1000' 'extra files allowed'
assert_equal "$(process_budget_restarts_for_file "${config}" PRECURSOR 7 07 1000 discover)" '' 'compact section ignored in precursor'

assert_status() {
    local expected="$1" rc=0
    shift
    "$@" >/dev/null || rc=$?
    assert_equal "${rc}" "${expected}" "status of $*"
}
assert_status 3 find_all_deficit_compact_budget_states "${integration_dir}" 07 0 1000
assert_status 3 get_deficit_compact_required_terms 0 "${tmpdir}/nonexistent"

parser_config="${tmpdir}/parser.in"
cat > "${parser_config}" <<'DATA'
&OTHER useCoriolis=.false., useWindTurbines=.false. /
&physics
 label='path!with/, and ''quoted'' text', useCoriolis=F, useCoriolis=T,
 isStratified=.t.,
/
&windturbines useWindTurbines=.TRUE /
&pressure_calc computeTurbinePressure=T /
&OTHER useCoriolis=.false., isStratified=.false., useWindTurbines=.false. /
&PHYSICS useCoriolis=.false. /
DATA
assert_equal "$(get_deficit_compact_required_terms 0 "${parser_config}")" "$(seq 1 23)" 'scoping, duplicate assignments, quotes, short logicals'
assert_equal "$(read_budget_setting "${parser_config}" PHYSICS label '')" "path!with/, and 'quoted' text" 'quoted punctuation'
for literal in T .t. .TRUE true .true. t.; do
    printf '&PHYSICS useCoriolis=%s /\n' "${literal}" > "${parser_config}"
    assert_equal "$(read_budget_logical "${parser_config}" PHYSICS useCoriolis .false.)" .true. "true literal ${literal}"
done
for literal in F .f. .FALSE false .false. f.; do
    printf '&PHYSICS useCoriolis=%s /\n' "${literal}" > "${parser_config}"
    assert_equal "$(read_budget_logical "${parser_config}" PHYSICS useCoriolis .true.)" .false. "false literal ${literal}"
done
for literal in .true.junk unknown 1; do
    printf '&PHYSICS useCoriolis=%s /\n' "${literal}" > "${parser_config}"
    assert_status 3 get_deficit_compact_required_terms 0 "${parser_config}"
done

# No deadline or parse failure may be reinterpreted as an empty state list.
emergency_deadline_exceeded() { return 0; }
assert_status 2 process_budget_restarts_for_file "${config}" PRIMARY 7 07 1000 discover
assert_status 2 process_budget_restarts_for_file "${config}" PRIMARY 7 07 1000 update
emergency_deadline_exceeded() { return 1; }
cp "${config}" "${tmpdir}/bad.in"
printf '\n&PHYSICS useCoriolis=invalid /\n' > "${tmpdir}/bad.in"
cat "${config}" >> "${tmpdir}/bad.in"
assert_status 3 process_budget_restarts_for_file "${tmpdir}/bad.in" PRIMARY 7 07 1000 update

# Verify the normal time-average discovery return code is preserved too.
# Mock only the scan, leaving the configuration/discovery path real.
parse_namelist_value() { echo 0; }
find_all_time_avg_budget_states() { return 2; }
pre_config="${tmpdir}/precursor.in"
printf '&BUDGET_TIME_AVG tidx_compute=10000, do_budgets=T, budgets_dir="%s" /\n' "${integration_dir}" > "${pre_config}"
assert_status 2 process_budget_restarts_for_file "${pre_config}" PRECURSOR 6 06 1000 update
find_all_time_avg_budget_states() {
    [[ "$3" == 1 ]] || { printf 'FAIL: wrong default budgetType\n' >&2; return 3; }
    echo '1000,3'
}
assert_equal "$(process_budget_restarts_for_file "${pre_config}" PRECURSOR 6 06 1000 discover)" 1000 'solver default budgetType=1'

# Config preflight and selected restart sample-history invariant.
assert_status 0 validate_compact_precursor "${config}" "${pre_config}" config
printf '&BUDGET_TIME_AVG tidx_compute=10000, do_budgets=T, budgetType=0, squeeze=F /\n' > "${pre_config}"
assert_status 3 validate_compact_precursor "${config}" "${pre_config}" config
printf '&BUDGET_TIME_AVG tidx_compute=10000, do_budgets=T, budgetType=0, squeeze=T /\n' > "${pre_config}"
assert_status 0 validate_compact_precursor "${config}" "${pre_config}" config
printf '&BUDGET_TIME_AVG tidx_compute=10000, do_budgets=F /\n' > "${pre_config}"
assert_status 3 validate_compact_precursor "${config}" "${pre_config}" config

compact_config="${tmpdir}/compact-restart.in"
cat > "${compact_config}" <<'DATA'
&BUDGET_TIME_AVG_DEFICIT_COMPACT
 do_budgets=T, do_budget3=T, restart_budgets=T,
 restart_tid=001000, restart_counter=000003
/
DATA
printf '&BUDGET_TIME_AVG tidx_compute=10000, do_budgets=T, restart_budgets=T, restart_tid=1000, restart_counter=3 /\n' > "${pre_config}"
assert_status 0 validate_compact_precursor "${compact_config}" "${pre_config}" restart
printf '&BUDGET_TIME_AVG tidx_compute=10000, do_budgets=T, restart_budgets=T, restart_tid=1000, restart_counter=4 /\n' > "${pre_config}"
assert_status 1 validate_compact_precursor "${compact_config}" "${pre_config}" restart
printf '&BUDGET_TIME_AVG tidx_compute=10000, do_budgets=T, restart_budgets=T, restart_tid=900, restart_counter=3 /\n' > "${pre_config}"
assert_status 1 validate_compact_precursor "${compact_config}" "${pre_config}" restart
printf '&BUDGET_TIME_AVG tidx_compute=10000, do_budgets=T, restart_budgets=F /\n' > "${pre_config}"
assert_status 1 validate_compact_precursor "${compact_config}" "${pre_config}" restart
printf '&BUDGET_TIME_AVG tidx_compute=10000, do_budgets=T, restart_budgets=T, restart_tid=1000 /\n' > "${pre_config}"
assert_status 3 validate_compact_precursor "${compact_config}" "${pre_config}" restart

printf 'All budget-state tests passed.\n'
