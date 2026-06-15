#!/usr/bin/env bash

set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${ROOT_DIR}/master_monitor.sh"

source <(sed -n '/^get_budget_deficit_compact_field_count()/,/^}/p' "${SCRIPT}")
source <(sed -n '/^deficit_term_set_complete()/,/^}/p' "${SCRIPT}")
source <(sed -n '/^find_all_deficit_compact_budget_states()/,/^}/p' "${SCRIPT}")

emergency_deadline_exceeded() { return 1; }

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

for term in $(seq 1 8); do
    touch "${tmpdir}/Run07_comp_deficit_budget1_term$(printf '%02d' "${term}")_t001000_n000001.s3D"
done
for term in $(seq 9 15); do
    touch "${tmpdir}/Run07_comp_deficit_budget1_term$(printf '%02d' "${term}")_t001000_n000002.s3D"
done

mixed="$(find_all_deficit_compact_budget_states "${tmpdir}" 07 1 1000 || true)"
if [[ -n "${mixed}" ]]; then
    printf 'FAIL: mixed counters incorrectly accepted: %s\n' "${mixed}" >&2
    exit 1
fi

for term in $(seq 1 15); do
    touch "${tmpdir}/Run07_comp_deficit_budget1_term$(printf '%02d' "${term}")_t001000_n000003.s3D"
done

complete="$(find_all_deficit_compact_budget_states "${tmpdir}" 07 1 1000)"
if [[ "${complete}" != "1000,3" ]]; then
    printf 'FAIL: complete counter not detected: %s\n' "${complete}" >&2
    exit 1
fi

rm -f "${tmpdir}/Run07_comp_deficit_budget1_term15_t001000_n000003.s3D"
touch "${tmpdir}/Run07_comp_deficit_budget1_term99_t001000_n000003.s3D"
wrong_terms="$(find_all_deficit_compact_budget_states "${tmpdir}" 07 1 1000 || true)"
if [[ -n "${wrong_terms}" ]]; then
    printf 'FAIL: wrong deficit term set incorrectly accepted: %s\n' "${wrong_terms}" >&2
    exit 1
fi

printf 'All budget-state tests passed.\n'
