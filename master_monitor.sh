# ==============================================================================
#  MASTER MONITOR / AUTO-RESUBMIT SCRIPT BODY
#
#  Date
#  ----
#  April 10, 2026
#
#  Contact
#  -------
#  For questions, issues, or maintenance inquiries, contact:
#      karimali@mit.edu
#
#  Purpose
#  -------
#  This script is intended to be sourced by a case-specific Slurm job script.
#  The local job script provides:
#    - #SBATCH directives
#    - case-specific file paths and monitoring parameters
#    - the platform-specific run_job() launcher
#
#  This master body then:
#    1) sources the machine/environment setup file,
#    2) launches the MPI simulation,
#    3) monitors progress through the solver output log and Slurm job metadata,
#    4) estimates whether enough wall time remains to reach the next restart dump,
#    5) watches for stalled/frozen jobs,
#    6) samples node-level memory usage via sstat each monitor cycle,
#    7) appends memory samples to a per-job CSV file,
#    8) updates restart settings in the input files when needed, and
#    9) submits a dependent child job so the simulation chain can continue.
#
#  High-level behavior
#  -------------------
#  - The script does NOT contain #SBATCH directives and is not meant to be
#    submitted directly.
#  - The parent/local job script is the actual submit target and remains the
#    resubmission target for all child jobs.
#  - Restart decisions are triggered by one of several conditions:
#      * hard wall-time cutoff,
#      * persistent estimate that not enough time remains to reach next dump,
#      * frozen/no-progress behavior,
#      * unsafe projected memory growth,
#      * emergency SIGTERM handling.
#  - When a restart is triggered, the script locates the latest common restart
#    time index between the primary and precursor simulations, edits the input
#    files to restart from that point, submits the next job in the chain, and
#    terminates the current MPI launcher cleanly.
#
#  Logging
#  -------
#  Monitor messages are written to a separate log file:
#      monitor.log.${SLURM_JOB_ID}
#  Memory samples are written to a per-job CSV file:
#      memdiag_job${SLURM_JOB_ID}.csv
#  The simulation's own output goes to the log file defined by the #SBATCH -o
#  directive in the local job script.
#
#  Memory sampling
#  ---------------
#  Memory is sampled once per monitor cycle (MONITOR_INTERVAL) via sstat.
#  The same sample feeds both the CSV file and the in-loop memory guard logic.
#  There is no separate background memory-watcher subshell.
#  When MEMORY_GUARD_SCOPE resolves to cgroup, the runtime ceiling is used
#  directly. Some clusters expose a per-step cgroup ceiling rather than a
#  per-task ceiling, so operators on those systems may still want to force
#  MEMORY_GUARD_SCOPE=node for a conservative node-based comparison.
#
#  Time-per-step estimate
#  ----------------------
#  The wall-time estimate used for restart decisions is taken preferentially
#  from the solver log by averaging the most recent elapsed-time entries
#  ("Elapsed time is ... seconds") over a configurable rolling window.
#  If that information is unavailable, the script falls back to a coarser
#  estimate based on wall-clock time and TIDX progress.
#
#  Safety / coordination features
#  ------------------------------
#  - A restart lock file is used to prevent duplicate restart actions from
#    multiple trigger paths.
#  - Input files are backed up before restart fields are modified.
#  - Persistent-sample logic is used to avoid reacting to single noisy or
#    transient bad estimates.
#  - Memory monitoring uses live sstat statistics and projected growth rather
#    than reacting only after an out-of-memory failure occurs.
#
#  Expected inputs from the local job script
#  -----------------------------------------
#  The local script is expected to define, at minimum:
#    MAIN_INPUTFILE
#    SOLVER
#    SOURCEDFILE
#    run_job()
#
#  In that arrangement, all auto-resubmitted child jobs also submit the same
#  local_case_job.sh file, while this master script provides the shared logic.
# ==============================================================================

# Resolve the local job script path
JOB_CONFIG_CMD="$(scontrol show job -o "${SLURM_JOB_ID}" | tr ' ' '\n' | awk -F= '$1=="Command"{print $2; exit}')"

if [[ -z "${JOB_CONFIG_CMD}" ]]; then
    echo "[ERROR] Could not determine config file path from scontrol for job ${SLURM_JOB_ID}" >&2
    exit 1
fi

if [[ "${JOB_CONFIG_CMD}" = /* ]]; then
    CONFIG_ABS="${JOB_CONFIG_CMD}"
else
    CONFIG_ABS="${SLURM_SUBMIT_DIR}/${JOB_CONFIG_CMD}"
fi

CONFIG_ABS="$(realpath "${CONFIG_ABS}")"

if [[ -z "${CONFIG_ABS}" || ! -f "${CONFIG_ABS}" ]]; then
    echo "[ERROR] Resolved config file does not exist: ${CONFIG_ABS}" >&2
    exit 1
fi

CONFIG_DIR="$(dirname "${CONFIG_ABS}")"
CONFIG_BASE="$(basename "${CONFIG_ABS}")"

[[ -f "${SOURCEDFILE}" ]] || { echo "[ERROR] Setup file not found: ${SOURCEDFILE}" >&2; exit 1; }
source "${SOURCEDFILE}"

export inputFile="${MAIN_INPUTFILE}"
export solver="${SOLVER}"

THIS_SCRIPT="${CONFIG_ABS}"
RESTART_LOCKFILE="${CONFIG_DIR}/.${CONFIG_BASE}.restart_lock"

# CSV file for per-cycle memory samples
MEM_CSV="${CONFIG_DIR}/memdiag_job${SLURM_JOB_ID}.csv"

# Monitoring log file
MONITOR_LOG="${CONFIG_DIR}/monitor.log.${SLURM_JOB_ID}"

RESTART_READY_FILE="${CONFIG_DIR}/.restart_ready"
SIM_DONE_FILE="${CONFIG_DIR}/.sim_done"

HARD_CUTOFF_SECONDS="${HARD_CUTOFF_SECONDS:-600}"
SAFETY_FACTOR="${SAFETY_FACTOR:-1.2}"
MONITOR_INTERVAL="${MONITOR_INTERVAL:-60}"
FROZEN_TIMEOUT_SECONDS="${FROZEN_TIMEOUT_SECONDS:-7200}"
ESTIMATE_PERSISTENCE_SAMPLES="${ESTIMATE_PERSISTENCE_SAMPLES:-10}"

# Initial sleep time before the first monitor cycle, to allow the job to start up and produce some output.
MONITOR_SETTLE_TIME="${MONITOR_SETTLE_TIME:-300}"  # Default 5 minutes (300 seconds)

# Memory-guard settings (node-based)
MEMORY_GUARD_ENABLED="${MEMORY_GUARD_ENABLED:-1}"
MEMORY_GUARD_LOOKAHEAD_INTERVALS="${MEMORY_GUARD_LOOKAHEAD_INTERVALS:-2}"
# Scope of the memory probe/limit interpretation:
#   auto = use runtime cgroup if finite, otherwise infer from site policy
#   node = treat RSS/limit as node-based
#   core/cpu = treat RSS/limit as CPU-based
MEMORY_GUARD_SCOPE="${MEMORY_GUARD_SCOPE:-auto}"

# Fraction of the per-node memory limit at which we trigger.
# 1.0 means "at the limit"; 0.95 means "95% of the limit".
MEMORY_GUARD_UTILIZATION="${MEMORY_GUARD_UTILIZATION:-0.95}"

# Critical utilization fraction for immediate hard stop (no persistence required).
# When memory reaches this fraction of the limit, restart immediately without waiting.
# Example: 0.97 means 97% of the memory limit.
MEMORY_GUARD_HARD_STOP_FRAC="${MEMORY_GUARD_HARD_STOP_FRAC:-0.97}"

# Explicit per-node memory limit in GB used by the memory guard.
# Example: 256 means 256 GB per node.
MEMORY_GUARD_NODE_LIMIT_GB="${MEMORY_GUARD_NODE_LIMIT_GB:-256}"

# Only restart if the memory projection is unsafe for N consecutive monitor cycles.
MEMORY_GUARD_PERSISTENCE_SAMPLES="${MEMORY_GUARD_PERSISTENCE_SAMPLES:-2}"

# Compute the slope from up to the last N samples.
MEMORY_RATE_WINDOW="${MEMORY_RATE_WINDOW:-4}"

# When looking at the output log for elapsed time per step, consider a rolling
# average over this many previous steps.
ELAPSED_STEP_WINDOW="${ELAPSED_STEP_WINDOW:-5}"

# Stack-dump diagnostics for frozen/no-progress jobs
STACK_DUMP_ENABLED="${STACK_DUMP_ENABLED:-1}"
STACK_DUMP_TIME_LIMIT="${STACK_DUMP_TIME_LIMIT:-00:10:00}"
STACK_DUMP_GDB_TIMEOUT_SECONDS="${STACK_DUMP_GDB_TIMEOUT_SECONDS:-60}"

# Export so the worker launched by srun can see the requested per-process gdb timeout.
# If a site strips the srun environment, the worker still falls back to 60 seconds.
export STACK_DUMP_GDB_TIMEOUT_SECONDS

# Parse wall-time from local case file
WALL_TIME_STR=$(
    awk '
        /^#SBATCH[[:space:]]+-t[[:space:]]+/      { print $3; exit }
        /^#SBATCH[[:space:]]+--time=/             { sub(/^#SBATCH[[:space:]]+--time=/,""); print; exit }
        /^#SBATCH[[:space:]]+--time[[:space:]]+/  { print $3; exit }
    ' "${CONFIG_ABS}"
)

if [[ -z "${WALL_TIME_STR}" ]]; then
    echo "[ERROR] Could not parse wall-time from ${CONFIG_ABS}" >&2
    exit 1
fi

# Parse stdout file pattern from local case file
OUTPUT_LOG_PATTERN=$(
    awk '
        /^#SBATCH[[:space:]]+-o[[:space:]]+/         { print $3; exit }
        /^#SBATCH[[:space:]]+--output=/              { sub(/^#SBATCH[[:space:]]+--output=/,""); print; exit }
        /^#SBATCH[[:space:]]+--output[[:space:]]+/   { print $3; exit }
    ' "${CONFIG_ABS}"
)

if [[ -z "${OUTPUT_LOG_PATTERN}" ]]; then
    echo "[ERROR] Could not parse stdout output pattern from ${CONFIG_ABS}" >&2
    exit 1
fi

OUTPUT_LOG="${OUTPUT_LOG_PATTERN//%j/${SLURM_JOB_ID}}"
OUTPUT_LOG="${OUTPUT_LOG//%J/${SLURM_JOB_ID}}"

if [[ "${OUTPUT_LOG}" != /* ]]; then
    OUTPUT_LOG="${CONFIG_DIR}/${OUTPUT_LOG}"
fi

# ==============================================================================
#  HELPERS
# ==============================================================================

# mlog: print a timestamped message to monitor log file
mlog() {
    printf '[%s] %s\n' "$(date '+%F %T')" "$*" >> "${MONITOR_LOG}"
}

# ==============================================================================
#  MPI STACK DUMP DIAGNOSTIC
#
#  Best-effort diagnostic used when the monitor detects a frozen/no-progress job.
#  It launches one diagnostic shell per allocated node and uses gdb to dump the
#  stack of each matching solver process owned by $USER.
#
#  Inputs are inferred from the monitor state:
#      SLURM_JOB_ID   : current Slurm job
#      solver         : full solver path; basename is used as pgrep pattern
#      CONFIG_DIR     : stack dump directory is created here
#      CONFIG_ABS     : local case script, used as fallback for #SBATCH parsing
#      mlog()         : monitor logging stream
# ==============================================================================

_parse_sbatch_directive_from_config() {
    local key="$1"
    local file="${2:-${CONFIG_ABS}}"

    [[ -f "${file}" ]] || return 1

    case "${key}" in
        nodes)
            awk '
                /^#SBATCH[[:space:]]+-N[[:space:]]+/              { print $3; exit }
                /^#SBATCH[[:space:]]+--nodes=/                    { sub(/^#SBATCH[[:space:]]+--nodes=/,""); print; exit }
                /^#SBATCH[[:space:]]+--nodes[[:space:]]+/         { print $3; exit }
            ' "${file}"
            ;;
        partition)
            awk '
                /^#SBATCH[[:space:]]+-p[[:space:]]+/              { print $3; exit }
                /^#SBATCH[[:space:]]+--partition=/                { sub(/^#SBATCH[[:space:]]+--partition=/,""); print; exit }
                /^#SBATCH[[:space:]]+--partition[[:space:]]+/     { print $3; exit }
            ' "${file}"
            ;;
        *)
            return 1
            ;;
    esac
}

_get_stackdump_nnodes() {
    local nnodes=""

    # Prefer live Slurm allocation.
    nnodes="$(scontrol show job -o "${SLURM_JOB_ID}" 2>/dev/null \
        | tr ' ' '\n' \
        | awk -F= '$1=="NumNodes"{print $2; exit}' || true)"

    # Fall back to environment.
    if [[ -z "${nnodes}" || ! "${nnodes}" =~ ^[0-9]+$ ]]; then
        nnodes="${SLURM_NNODES:-}"
    fi

    # Fall back to #SBATCH directives in the case script.
    if [[ -z "${nnodes}" || ! "${nnodes}" =~ ^[0-9]+$ ]]; then
        nnodes="$(_parse_sbatch_directive_from_config nodes || true)"
    fi

    [[ "${nnodes}" =~ ^[0-9]+$ ]] || return 1
    echo "${nnodes}"
}

_get_stackdump_partition() {
    local partition=""

    # Prefer live Slurm allocation.
    partition="$(scontrol show job -o "${SLURM_JOB_ID}" 2>/dev/null \
        | tr ' ' '\n' \
        | awk -F= '$1=="Partition"{print $2; exit}' || true)"

    # Fall back to environment.
    if [[ -z "${partition}" || "${partition}" == "(null)" ]]; then
        partition="${SLURM_JOB_PARTITION:-}"
    fi

    # Fall back to #SBATCH directives in the case script.
    if [[ -z "${partition}" || "${partition}" == "(null)" ]]; then
        partition="$(_parse_sbatch_directive_from_config partition || true)"
    fi

    [[ -n "${partition}" && "${partition}" != "(null)" ]] || return 1
    echo "${partition}"
}

dump_mpi_stacks_for_frozen_job() {
    local reason="${1:-unknown}"
    local solver_pattern
    local timestamp outdir worker nodes_file
    local nodelist_raw nnodes partition srun_rc
    local class_summary counts_file

    if [[ "${STACK_DUMP_ENABLED}" != "1" ]]; then
        mlog "[StackDump] Disabled by STACK_DUMP_ENABLED=${STACK_DUMP_ENABLED}."
        return 0
    fi

    if [[ -z "${SLURM_JOB_ID:-}" ]]; then
        mlog "[StackDump] ERROR: SLURM_JOB_ID is empty; cannot dump MPI stacks."
        return 1
    fi

    if [[ -z "${solver:-}" ]]; then
        mlog "[StackDump] ERROR: solver is empty; cannot infer solver pattern."
        return 1
    fi

    solver_pattern="$(basename "${solver}")"

    if [[ -z "${solver_pattern}" || "${solver_pattern}" == "." || "${solver_pattern}" == "/" ]]; then
        mlog "[StackDump] ERROR: invalid solver pattern inferred from solver='${solver}'."
        return 1
    fi

    timestamp="$(date +%Y%m%d_%H%M%S)"
    outdir="${CONFIG_DIR}/stackdump_job${SLURM_JOB_ID}_${timestamp}"
    nodes_file="${outdir}/nodes.txt"
    worker="${outdir}/worker_dump_stacks.sh"

    mkdir -p "${outdir}" || {
        mlog "[StackDump] ERROR: failed to create output directory: ${outdir}"
        return 1
    }

    mlog "================================================================"
    mlog "[StackDump] Starting MPI stack dump."
    mlog "[StackDump] Trigger reason : ${reason}"
    mlog "[StackDump] Job ID         : ${SLURM_JOB_ID}"
    mlog "[StackDump] Solver path    : ${solver}"
    mlog "[StackDump] Solver pattern : ${solver_pattern}"
    mlog "[StackDump] Output dir     : ${outdir}"

    nodelist_raw="$(get_job_nodelist || true)"
    if [[ -z "${nodelist_raw}" ]]; then
        nodelist_raw="$(squeue -j "${SLURM_JOB_ID}" -h -o "%N" 2>/dev/null || true)"
    fi

    if [[ -z "${nodelist_raw}" ]]; then
        mlog "[StackDump] ERROR: could not determine node list for job ${SLURM_JOB_ID}."
        return 1
    fi

    if ! scontrol show hostnames "${nodelist_raw}" > "${nodes_file}" 2>"${outdir}/scontrol_hostnames.err"; then
        mlog "[StackDump] ERROR: scontrol show hostnames failed. See ${outdir}/scontrol_hostnames.err"
        return 1
    fi

    nnodes="$(_get_stackdump_nnodes || true)"
    if [[ -z "${nnodes}" || ! "${nnodes}" =~ ^[0-9]+$ ]]; then
        nnodes="$(wc -l < "${nodes_file}" | tr -d ' ')"
    fi

    partition="$(_get_stackdump_partition || true)"
    if [[ -z "${partition}" ]]; then
        mlog "[StackDump] ERROR: could not determine partition for job ${SLURM_JOB_ID}."
        return 1
    fi

    mlog "[StackDump] NodeList       : ${nodelist_raw}"
    mlog "[StackDump] Nodes          : ${nnodes}"
    mlog "[StackDump] Partition      : ${partition}"
    mlog "[StackDump] Time limit     : ${STACK_DUMP_TIME_LIMIT}"

    cat > "${worker}" <<'EOF'
#!/usr/bin/env bash
set -u

SOLVER_PATTERN="$1"
REMOTE_OUTDIR="$2"

host="$(hostname -f 2>/dev/null || hostname)"
short_host="$(hostname)"

mkdir -p "${REMOTE_OUTDIR}"

node_summary="${REMOTE_OUTDIR}/summary_${short_host}.txt"
: > "${node_summary}"

{
    echo "===== NODE ${host} ====="
    echo "Time: $(date)"
    echo "Solver pattern: ${SOLVER_PATTERN}"
} | tee -a "${node_summary}"

if ! command -v gdb >/dev/null 2>&1; then
    echo "ERROR: gdb not found on ${host}" | tee -a "${node_summary}"
    exit 0
fi

ptrace_scope="$(cat /proc/sys/kernel/yama/ptrace_scope 2>/dev/null || echo unknown)"
echo "ptrace_scope: ${ptrace_scope}" | tee -a "${node_summary}"

if [[ "${ptrace_scope}" =~ ^[0-9]+$ && "${ptrace_scope}" -ge 2 ]]; then
    echo "WARNING: ptrace_scope=${ptrace_scope}; gdb attach may be blocked on this node." \
        | tee -a "${node_summary}"
fi

run_user="$(id -un 2>/dev/null || echo "${USER:-}")"

if [[ -z "${run_user}" ]]; then
    echo "ERROR: could not determine run user on ${host}" | tee -a "${node_summary}"
    exit 0
fi

mapfile -t pids < <(
    pgrep -u "${run_user}" -f "${SOLVER_PATTERN}" 2>/dev/null \
    | while read -r pid; do
        [[ -n "${pid}" ]] || continue

        cmd="$(ps -p "${pid}" -o cmd= 2>/dev/null || true)"

        case "${cmd}" in
            *grep*|*bash*|*dump_mpi_stacks*|*worker_dump_stacks*|*srun*|*ibrun*|*mpirun*|*mpiexec*|*hydra_pmi_proxy*)
                ;;
            *)
                echo "${pid}"
                ;;
        esac
    done
)

echo "Found ${#pids[@]} matching PID(s)." | tee -a "${node_summary}"

if [[ "${#pids[@]}" -eq 0 ]]; then
    ps -u "${run_user}" -o pid,ppid,stat,pcpu,pmem,cmd --sort=-pcpu \
        > "${REMOTE_OUTDIR}/ps_${short_host}.txt" 2>&1
    echo "No solver PIDs found. Wrote process snapshot." | tee -a "${node_summary}"
    exit 0
fi

for pid in "${pids[@]}"; do
    stackfile="${REMOTE_OUTDIR}/stack_${short_host}_${pid}.txt"
    classfile="${REMOTE_OUTDIR}/class_${short_host}_${pid}.txt"

    echo "Dumping PID ${pid} on ${short_host}" | tee -a "${node_summary}"

    gdb_timeout="${STACK_DUMP_GDB_TIMEOUT_SECONDS:-60}"

    if command -v timeout >/dev/null 2>&1; then
        timeout "${gdb_timeout}" gdb -batch -nx \
            -ex "set pagination off" \
            -ex "thread apply all bt" \
            -p "${pid}" \
            > "${stackfile}" 2>&1
        gdb_rc=$?
    else
        echo "NOTE: 'timeout' unavailable; gdb bounded only by srun wall-time limit." \
            >> "${stackfile}"
        gdb -batch -nx \
            -ex "set pagination off" \
            -ex "thread apply all bt" \
            -p "${pid}" \
            >> "${stackfile}" 2>&1
        gdb_rc=$?
    fi

    echo "gdb_rc=${gdb_rc}" >> "${stackfile}"

    if [[ "${gdb_rc}" -eq 124 ]]; then
        class="GDB_TIMEOUT"
    elif grep -qi "ptrace\|Operation not permitted\|No such process\|Attaching to process.*failed\|Permission denied" "${stackfile}"; then
        class="GDB_ATTACH_FAILED"
    elif grep -q "reductions_mp_p_minval_sca_" "${stackfile}"; then
        class="ALLREDUCE_MINVAL"
    elif grep -q "pmpi_allreduce_\|PMPI_Allreduce\|MPI_Allreduce" "${stackfile}"; then
        class="ALLREDUCE_OTHER"
    elif grep -q "MPI_File\|MPI_FILE\|nf90_put\|nf_put\|H5Dwrite\|ADIO" "${stackfile}"; then
        class="MPI_IO"
    elif grep -q "MPI_Alltoall\|MPI_Alltoallv\|transpose_x_to_y\|transpose_y_to_z\|transpose_y_to_x\|transpose_z_to_y" "${stackfile}"; then
        class="ALLTOALL_OR_TRANSPOSE"
    elif grep -q "fi_opx_cq_read\|fi_cq_read\|opal_progress\|MPIDI_OFI\|poll_cq" "${stackfile}"; then
        class="MPI_PROGRESS_ENGINE"
    elif grep -q "MPI_Wait\|MPI_Waitall\|PMPI_Wait\|PMPI_Waitall" "${stackfile}"; then
        class="WAIT"
    else
        class="OTHER"
    fi

    echo "${class} ${host} ${pid} ${stackfile}" > "${classfile}"
    echo "${class} PID=${pid}" | tee -a "${node_summary}"
done

echo "Done node ${host}" | tee -a "${node_summary}"
EOF

    chmod +x "${worker}" || {
        mlog "[StackDump] ERROR: failed to chmod worker script: ${worker}"
        return 1
    }

    mlog "[StackDump] Launching one diagnostic task per node with srun --overlap..."

    srun -p "${partition}" \
        --overlap \
        --jobid="${SLURM_JOB_ID}" \
        --nodes="${nnodes}" \
        --ntasks="${nnodes}" \
        --ntasks-per-node=1 \
        --time="${STACK_DUMP_TIME_LIMIT}" \
        bash "${worker}" "${solver_pattern}" "${outdir}" \
        > "${outdir}/srun_stackdump.out" \
        2> "${outdir}/srun_stackdump.err"

    srun_rc=$?

    if [[ "${srun_rc}" -ne 0 ]]; then
        mlog "[StackDump] WARNING: srun diagnostic step failed with rc=${srun_rc}."
        mlog "[StackDump] WARNING: This may indicate --overlap is unsupported or blocked on this cluster."
        mlog "[StackDump] WARNING: stdout=${outdir}/srun_stackdump.out"
        mlog "[StackDump] WARNING: stderr=${outdir}/srun_stackdump.err"
        return 1
    fi

    class_summary="${outdir}/classification_summary.txt"
    counts_file="${outdir}/classification_counts.txt"

    : > "${class_summary}"

    find "${outdir}" -name 'class_*.txt' -type f -print0 \
        | xargs -0 cat \
        | sort \
        > "${class_summary}" 2>/dev/null || true

    awk '{count[$1]++} END {for (c in count) print c, count[c]}' "${class_summary}" \
        | sort \
        > "${counts_file}" 2>/dev/null || true

    mlog "[StackDump] Stack dump complete."
    mlog "[StackDump] Nodes file             : ${nodes_file}"
    mlog "[StackDump] Classification summary : ${class_summary}"
    mlog "[StackDump] Classification counts  : ${counts_file}"
    mlog "[StackDump] Per-node summaries     : ${outdir}/summary_*.txt"
    mlog "[StackDump] Full stack files       : ${outdir}/stack_*.txt"
    mlog "================================================================"

    return 0
}

gb_to_kb() {
    local gb="$1"
    [[ -n "${gb}" ]] || return 1
    awk -v x="${gb}" '
        BEGIN {
            if (x ~ /^[0-9]+([.][0-9]+)?$/) {
                printf "%.0f\n", x * 1024 * 1024
            } else {
                exit 1
            }
        }
    '
}

min_positive() {
    local min=0
    local v
    for v in "$@"; do
        [[ -n "${v}" && "${v}" =~ ^[0-9]+$ && "${v}" -gt 0 ]] || continue
        if [[ "${min}" -eq 0 || "${v}" -lt "${min}" ]]; then
            min="${v}"
        fi
    done
    echo "${min}"
}

get_slurm_config_value() {
    local key="$1"
    [[ -n "${key}" ]] || return 1
    scontrol show config 2>/dev/null \
        | awk -v key="${key}" '
            $0 ~ ("^[[:space:]]*" key "[[:space:]]*=") {
                sub(/^[^=]*=[[:space:]]*/, "");
                print;
                exit
            }'
}

get_job_cpus_per_node_count() {
    local cpus_per_node
    cpus_per_node="${SLURM_JOB_CPUS_PER_NODE%%(*}"
    [[ "${cpus_per_node}" =~ ^[0-9]+$ ]] || return 1
    echo "${cpus_per_node}"
}

get_job_tasks_per_node_count() {
    local tasks_per_node
    # Supports homogeneous allocations like 16(x4). Heterogeneous lists such as
    # 16,8 are intentionally treated conservatively by falling back to 1.
    tasks_per_node="${SLURM_TASKS_PER_NODE%%(*}"
    [[ "${tasks_per_node}" =~ ^[0-9]+$ ]] || return 1
    echo "${tasks_per_node}"
}

get_job_cgroup_memory_limit_kb() {
    local cgroup_path
    local limit_file limit_value

    cgroup_path="$(awk -F: '$1 == 0 || $2 == "memory" { print $3; exit }' /proc/self/cgroup 2>/dev/null)"
    [[ -n "${cgroup_path}" ]] || return 1

    for limit_file in \
        "/sys/fs/cgroup${cgroup_path}/memory.max" \
        "/sys/fs/cgroup${cgroup_path}/memory.limit_in_bytes"
    do
        [[ -r "${limit_file}" ]] || continue
        limit_value="$(cat "${limit_file}" 2>/dev/null)"
        [[ -n "${limit_value}" && "${limit_value}" != "max" ]] || continue
        [[ "${limit_value}" =~ ^[0-9]+$ ]] || continue
        echo $(( limit_value / 1024 ))
        return 0
    done

    return 1
}

classify_memory_guard_scope() {
    local select_params reqmem

    if [[ "${MEMORY_GUARD_SCOPE}" != "auto" && -n "${MEMORY_GUARD_SCOPE}" ]]; then
        echo "${MEMORY_GUARD_SCOPE}"
        return 0
    fi

    reqmem="$(get_job_reqmem_raw || true)"
    case "${reqmem}" in
        *n) echo "node"; return 0 ;;
        *c) echo "cpu"; return 0 ;;
    esac

    select_params="$(get_slurm_config_value SelectTypeParameters || true)"

    case "${select_params}" in
        *CR_CORE_MEMORY*) echo "core"; return 0 ;;
        *CR_CPU_Memory*)  echo "cpu";  return 0 ;;
    esac

    echo "node"
    return 0
}

get_job_nodelist() {
    scontrol show job -o "${SLURM_JOB_ID}" 2>/dev/null \
        | tr ' ' '\n' \
        | awk -F= '$1=="NodeList"{print $2; exit}'
}

get_first_allocated_node() {
    local nodelist
    nodelist="$(get_job_nodelist)"
    [[ -n "${nodelist}" ]] || return 1
    scontrol show hostnames "${nodelist}" 2>/dev/null | head -1
}

get_node_realmemory_kb() {
    local node="$1"
    local mem_mb
    [[ -n "${node}" ]] || return 1
    mem_mb="$(scontrol show node "${node}" 2>/dev/null \
        | tr ' ' '\n' \
        | awk -F= '$1=="RealMemory"{print $2; exit}')"
    [[ "${mem_mb}" =~ ^[0-9]+$ ]] || return 1
    echo $(( mem_mb * 1024 ))
}

get_job_reqmem_raw() {
    scontrol show job -o "${SLURM_JOB_ID}" 2>/dev/null \
        | tr ' ' '\n' \
        | awk -F= '$1=="ReqMem"{print $2; exit}'
}

get_job_reqmem_kb_per_node() {
    local reqmem raw num unit suffix total_kb
    reqmem="$(get_job_reqmem_raw)"
    [[ -n "${reqmem}" ]] || return 1
    raw="${reqmem}"
    if [[ "${raw}" =~ ^([0-9]+)([KMGT])([cn])$ ]]; then
        num="${BASH_REMATCH[1]}"
        unit="${BASH_REMATCH[2]}"
        suffix="${BASH_REMATCH[3]}"
    else
        return 1
    fi
    case "${unit}" in
        K) total_kb=$(( num )) ;;
        M) total_kb=$(( num * 1024 )) ;;
        G) total_kb=$(( num * 1024 * 1024 )) ;;
        T) total_kb=$(( num * 1024 * 1024 * 1024 )) ;;
        *) return 1 ;;
    esac
    if [[ "${suffix}" == "n" ]]; then
        echo "${total_kb}"
        return 0
    fi
    if [[ "${suffix}" == "c" ]]; then
        local cpus_per_node
        cpus_per_node="${SLURM_JOB_CPUS_PER_NODE%%(*}"
        [[ "${cpus_per_node}" =~ ^[0-9]+$ ]] || cpus_per_node=1
        echo $(( total_kb * cpus_per_node ))
        return 0
    fi
    return 1
}

parse_slurm_time_to_seconds() {
    local t="$1"
    local days=0 hours=0 mins=0 secs=0
    local rest
    local f1="" f2="" f3="" extra=""

    if [[ -z "${t}" ]]; then
        echo "[ERROR] Empty Slurm time string." >&2
        return 1
    fi

    if [[ "${t}" == *-* ]]; then
        days="${t%%-*}"
        rest="${t#*-}"
    else
        rest="${t}"
    fi

    IFS=: read -r f1 f2 f3 extra <<< "${rest}"

    if [[ -n "${extra}" ]]; then
        echo "[ERROR] Invalid Slurm time format: ${t}" >&2
        return 1
    fi

    if [[ -n "${f1}" && -n "${f2}" && -n "${f3}" ]]; then
        hours="${f1}"; mins="${f2}"; secs="${f3}"
    elif [[ -n "${f1}" && -n "${f2}" ]]; then
        if [[ "${t}" == *-* ]]; then
            hours="${f1}"; mins="${f2}"
        else
            mins="${f1}"; secs="${f2}"
        fi
    elif [[ -n "${f1}" ]]; then
        if [[ "${t}" == *-* ]]; then
            hours="${f1}"
        else
            mins="${f1}"
        fi
    else
        echo "[ERROR] Invalid Slurm time format: ${t}" >&2
        return 1
    fi

    for v in "${days}" "${hours}" "${mins}" "${secs}"; do
        [[ "${v}" =~ ^[0-9]+$ ]] || {
            echo "[ERROR] Non-numeric Slurm time field in: ${t}" >&2
            return 1
        }
    done

    echo $(( 10#${days}*86400 + 10#${hours}*3600 + 10#${mins}*60 + 10#${secs} ))
}

get_recent_logged_elapsed_step_average() {
    local logfile="$1"
    local lines_per_step_block tail_lines avg

    [[ -f "${logfile}" ]] || return 1

    lines_per_step_block=60
    tail_lines=$(( ELAPSED_STEP_WINDOW * lines_per_step_block ))
    [[ "${tail_lines}" -gt 0 ]] || tail_lines=300

    avg=$(
        tail -n "${tail_lines}" "${logfile}" 2>/dev/null \
        | awk -v want="${ELAPSED_STEP_WINDOW}" '
            /Elapsed time is/ {
                for (i=1; i<=NF; i++) {
                    if ($i == "is" && (i+1) <= NF) {
                        vals[++n] = $(i+1)
                        break
                    }
                }
            }
            END {
                if (n == 0) exit 1
                start = n - want + 1
                if (start < 1) start = 1
                sum = 0; count = 0
                for (i = start; i <= n; i++) { sum += vals[i]; count++ }
                if (count > 0) printf "%.2f\n", sum / count
                else exit 1
            }
        '
    )

    [[ -n "${avg}" ]] || return 1
    [[ "${avg}" =~ ^[0-9]+([.][0-9]+)?$ ]] || return 1
    echo "${avg}"
}

push_mem_sample() {
    local epoch="$1"
    local rss_kb="$2"
    MEM_EPOCH_HISTORY+=("${epoch}")
    MEM_RSS_HISTORY+=("${rss_kb}")
    while [[ "${#MEM_EPOCH_HISTORY[@]}" -gt "${MEMORY_RATE_WINDOW}" ]]; do
        MEM_EPOCH_HISTORY=("${MEM_EPOCH_HISTORY[@]:1}")
        MEM_RSS_HISTORY=("${MEM_RSS_HISTORY[@]:1}")
    done
}

average_mem_rate_kbps() {
    local n="${#MEM_EPOCH_HISTORY[@]}"
    local i dt drss sum="0" count=0 rate

    if [[ "${n}" -lt 2 ]]; then
        echo ""
        return 0
    fi

    for (( i=1; i<n; i++ )); do
        dt=$(( MEM_EPOCH_HISTORY[i] - MEM_EPOCH_HISTORY[i-1] ))
        drss=$(( MEM_RSS_HISTORY[i] - MEM_RSS_HISTORY[i-1] ))
        if [[ "${dt}" -gt 0 ]]; then
            [[ "${drss}" -lt 0 ]] && drss=0
            rate=$(echo "scale=2; ${drss} / ${dt}" | bc 2>/dev/null)
            [[ -n "${rate}" ]] || continue
            sum=$(echo "scale=2; ${sum} + ${rate}" | bc 2>/dev/null)
            count=$(( count + 1 ))
        fi
    done

    if [[ "${count}" -gt 0 ]]; then
        echo "scale=2; ${sum} / ${count}" | bc 2>/dev/null
    else
        echo ""
    fi
}

parse_size_to_kb() {
    local s="$1"
    s="${s// /}"
    s="${s%%%}"
    [[ -z "${s}" || "${s}" == "0" || "${s}" == "Unknown" || "${s}" == "N/A" ]] && { echo 0; return 0; }
    awk -v x="${s}" '
        function scale(u) {
            if (u=="K" || u=="KB" || u=="KiB" || u=="") return 1
            if (u=="M" || u=="MB" || u=="MiB") return 1024
            if (u=="G" || u=="GB" || u=="GiB") return 1024*1024
            if (u=="T" || u=="TB" || u=="TiB") return 1024*1024*1024
            return 1
        }
        BEGIN {
            if (match(x, /^([0-9.]+)([A-Za-z]*)$/, a)) {
                printf "%.0f\n", a[1] * scale(a[2])
            } else {
                print 0
            }
        }
    '
}

# ==============================================================================
#  MEMORY SAMPLING VIA SSTAT (called once per monitor cycle)
#
#  Returns: sets globals SSTAT_MAXRSS_KB, SSTAT_AVERSS_KB,
#                         SSTAT_MAXVM_KB,  SSTAT_AVEVM_KB
#  Appends one row to MEM_CSV.
#  Returns 0 on success (at least one data row), 1 if sstat returned nothing.
# ==============================================================================

SSTAT_MAXRSS_KB=0
SSTAT_AVERSS_KB=0
SSTAT_MAXVM_KB=0
SSTAT_AVEVM_KB=0

sample_memory_sstat() {
    local ts epoch data target
    local peak_maxrss=0 peak_averss=0 peak_maxvm=0 peak_avevm=0
    local got_data=0

    ts=$(date '+%F %T')
    epoch=$(date +%s)
    data=""

    # First try the bare job ID (works on some clusters, e.g. Anvil),
    # then fall back to the batch step (needed on some others, e.g. Stampede3).
    for target in "${SLURM_JOB_ID}" "${SLURM_JOB_ID}.batch"; do
        data=$(sstat -j "${target}" \
            --format=JobID,MaxRSS,AveRSS,MaxVMSize,AveVMSize \
            --noheader 2>/dev/null || true)
        if [[ -n "${data}" ]] && echo "${data}" | grep -q '[^[:space:]]'; then
            break
        fi
    done

    if [[ -z "${data}" ]] || ! echo "${data}" | grep -q '[^[:space:]]'; then
        SSTAT_MAXRSS_KB=0
        SSTAT_AVERSS_KB=0
        SSTAT_MAXVM_KB=0
        SSTAT_AVEVM_KB=0
        return 1
    fi

    while IFS= read -r line; do
        [[ -z "${line// /}" ]] && continue

        # Guard against stale/buggy SLURM versions that emit a header despite --noheader.
        [[ "${line}" =~ ^[[:space:]]*JobID ]] && continue

        local step maxrss averss maxvm avevm
        # Assign before read so fields are never stale on a partial read.
        step="" maxrss="" averss="" maxvm="" avevm=""
        read -r step maxrss averss maxvm avevm <<< "${line}"

        # Skip malformed rows.
        [[ -z "${step}" ]] && continue
        if [[ -z "${maxrss}" && -z "${averss}" && -z "${maxvm}" && -z "${avevm}" ]]; then
            continue
        fi

        # Strip only Slurm's trailing "+" continuation marker.
        # Keep units intact for parse_size_to_kb().
        maxrss="${maxrss%%+}"
        averss="${averss%%+}"
        maxvm="${maxvm%%+}"
        avevm="${avevm%%+}"

        local mrss arss mvm avm
        mrss=$(parse_size_to_kb "${maxrss}"); [[ "${mrss}" =~ ^[0-9]+$ ]] || mrss=0
        arss=$(parse_size_to_kb "${averss}"); [[ "${arss}" =~ ^[0-9]+$ ]] || arss=0
        mvm=$(parse_size_to_kb "${maxvm}");  [[ "${mvm}"  =~ ^[0-9]+$ ]] || mvm=0
        avm=$(parse_size_to_kb "${avevm}");  [[ "${avm}"  =~ ^[0-9]+$ ]] || avm=0

        # Append this step row to CSV.
        echo "${ts},${epoch},${SLURM_JOB_ID},${step},${mrss},${arss},${mvm},${avm}" \
            >> "${MEM_CSV}" \
            || { echo "WARNING: sample_memory_sstat: failed to write to ${MEM_CSV}" >&2; return 1; }

        [[ "${mrss}" -gt "${peak_maxrss}" ]] && peak_maxrss="${mrss}"
        [[ "${arss}" -gt "${peak_averss}" ]] && peak_averss="${arss}"
        [[ "${mvm}"  -gt "${peak_maxvm}"  ]] && peak_maxvm="${mvm}"
        [[ "${avm}"  -gt "${peak_avevm}"  ]] && peak_avevm="${avm}"

        got_data=1
    done <<< "${data}"

    SSTAT_MAXRSS_KB="${peak_maxrss}"
    SSTAT_AVERSS_KB="${peak_averss}"
    SSTAT_MAXVM_KB="${peak_maxvm}"
    SSTAT_AVEVM_KB="${peak_avevm}"

    # Return 0 iff at least one valid data row was parsed.
    [[ "${got_data}" -eq 1 ]]
}

get_file_mtime_epoch() {
    local f="$1"
    if [[ -f "${f}" ]]; then
        stat -c %Y "${f}" 2>/dev/null || echo 0
    else
        echo 0
    fi
}

submit_from_config() {
    local dependency_mode="${1:-none}"
    local dependency_jobid="${2:-}"
    if [[ "${dependency_mode}" == "afterany" ]]; then
        sbatch --parsable --dependency="afterany:${dependency_jobid}" "${THIS_SCRIPT}"
    else
        sbatch --parsable "${THIS_SCRIPT}"
    fi
}

submit_with_retry() {
    local dependency_mode="${1:-none}"
    local dependency_jobid="${2:-}"
    local max_attempts="${3:-3}"
    local sleep_seconds="${4:-10}"

    local attempt=1
    local out rc

    while [[ "${attempt}" -le "${max_attempts}" ]]; do
        out="$(submit_from_config "${dependency_mode}" "${dependency_jobid}" 2>&1)"
        rc=$?

        if [[ "${rc}" -eq 0 ]]; then
            echo "${out}"
            return 0
        fi

        mlog "[Submit] Attempt ${attempt}/${max_attempts} failed."
        mlog "[Submit] sbatch output: ${out}"

        if [[ "${attempt}" -lt "${max_attempts}" ]]; then
            sleep "${sleep_seconds}"
        fi
        attempt=$(( attempt + 1 ))
    done

    return 1
}

stage_restart_handoff_and_exit() {
    local reason="$1"

    mlog "[Handoff] Self-submission unavailable or failed."
    mlog "[Handoff] Reason: ${reason}"
    mlog "[Handoff] Restart inputs already prepared. Signaling downstream dependent job."

    printf 'jobid=%s\nreason=%s\ntime=%s\n' \
        "${SLURM_JOB_ID}" "${reason}" "$(date '+%F %T')" \
        > "${RESTART_READY_FILE}"

    sync || true

    if [[ -n "${MPI_PID:-}" ]] && kill -0 "${MPI_PID}" 2>/dev/null; then
        mlog "[Handoff] Terminating MPI PID ${MPI_PID}."
        kill -TERM "${MPI_PID}" 2>/dev/null || true
        wait "${MPI_PID}" 2>/dev/null || true
    fi

    release_restart_lock
    mlog "[Handoff] Exiting current job to allow dependent child to continue."
    exit 0
}

parse_namelist_value() {
    local file="$1"
    local key="$2"

    if [[ ! -f "${file}" ]]; then
        mlog "[ERROR] parse_namelist_value: file not found: ${file}"
        return 1
    fi

    local raw
    raw=$(grep -i "^[[:space:]]*${key}[[:space:]]*=" "${file}" | head -1)

    if [[ -z "${raw}" ]]; then
        mlog "[ERROR] parse_namelist_value: key '${key}' not found in ${file}"
        return 1
    fi

    echo "${raw}" \
        | sed 's/.*=[[:space:]]*//' \
        | sed "s/[\"']//g" \
        | sed 's/!.*//' \
        | sed 's/,[[:space:]]*$//' \
        | tr -d '[:space:]'
}

set_namelist_value() {
    local file="$1"
    local key="$2"
    local new_val="$3"
    local tmpfile="${file}.tmp.$$"

    awk -v key="${key}" -v val="${new_val}" '
    BEGIN { found = 0 }
    tolower($0) ~ ("^[[:space:]]*" tolower(key) "[[:space:]]*=") {
        match($0, /^[[:space:]]*/)
        indent = substr($0, 1, RLENGTH)
        print indent key " = " val
        found = 1
        next
    }
    { print }
    END {
        if (!found) {
            print "[ERROR] set_namelist_value: key \"" key "\" not found in file" > "/dev/stderr"
            exit 2
        }
    }
    ' "${file}" > "${tmpfile}"

    local awk_exit=$?
    if [[ ${awk_exit} -ne 0 ]]; then
        rm -f "${tmpfile}"
        mlog "[ERROR] set_namelist_value: failed to update '${key}' in ${file}"
        return ${awk_exit}
    fi

    mv "${tmpfile}" "${file}"
}

latest_common_restart_tid() {
    local dir1="$1" rid1="$2" dir2="$3" rid2="$4"
    local list1 list2

    list1=$(find "${dir1}" -maxdepth 1 -type f \
                -name "RESTART_Run${rid1}_info.??????" \
                -printf "%f\n" 2>/dev/null \
            | sed 's/.*\.//' \
            | sort -n \
            | awk '{printf "%d\n", $1}')

    list2=$(find "${dir2}" -maxdepth 1 -type f \
                -name "RESTART_Run${rid2}_info.??????" \
                -printf "%f\n" 2>/dev/null \
            | sed 's/.*\.//' \
            | sort -n \
            | awk '{printf "%d\n", $1}')

    if [[ -z "${list1}" || -z "${list2}" ]]; then
        echo ""
        return 0
    fi

    comm -12 <(echo "${list1}" | sort) <(echo "${list2}" | sort) | sort -n | tail -1
}

validate_nonempty() {
    if [[ -z "${1}" ]]; then
        mlog "[ERROR] Required value is empty: ${2}"
        exit 1
    fi
}

validate_integer() {
    if [[ ! "${1}" =~ ^[0-9]+$ ]]; then
        mlog "[ERROR] Expected integer for '${2}', got: '${1}'"
        exit 1
    fi
}

acquire_restart_lock() {
    if ( set -o noclobber; > "${RESTART_LOCKFILE}" ) 2>/dev/null; then
        echo "${SLURM_JOB_ID} $$ $(date '+%F %T')" > "${RESTART_LOCKFILE}"
        return 0
    fi
    return 1
}

release_restart_lock() {
    rm -f "${RESTART_LOCKFILE}" 2>/dev/null || true
}

cleanup_watchers() {
    : # no background watchers in the unified design; kept for EXIT trap symmetry
}

validate_integer "${ESTIMATE_PERSISTENCE_SAMPLES}" "ESTIMATE_PERSISTENCE_SAMPLES"
validate_integer "${MEMORY_GUARD_LOOKAHEAD_INTERVALS}" "MEMORY_GUARD_LOOKAHEAD_INTERVALS"
validate_integer "${MEMORY_GUARD_PERSISTENCE_SAMPLES}" "MEMORY_GUARD_PERSISTENCE_SAMPLES"
validate_integer "${MEMORY_RATE_WINDOW}" "MEMORY_RATE_WINDOW"
validate_integer "${ELAPSED_STEP_WINDOW}" "ELAPSED_STEP_WINDOW"
validate_integer "${STACK_DUMP_GDB_TIMEOUT_SECONDS}" "STACK_DUMP_GDB_TIMEOUT_SECONDS"

[[ "${ESTIMATE_PERSISTENCE_SAMPLES}" -gt 0 ]]   || { mlog "[ERROR] ESTIMATE_PERSISTENCE_SAMPLES must be > 0"; exit 1; }
[[ "${STACK_DUMP_ENABLED}" =~ ^[01]$ ]] || { mlog "[ERROR] STACK_DUMP_ENABLED must be 0 or 1"; exit 1; }
[[ "${STACK_DUMP_GDB_TIMEOUT_SECONDS}" -gt 0 ]] || { mlog "[ERROR] STACK_DUMP_GDB_TIMEOUT_SECONDS must be > 0"; exit 1; }

[[ "${HARD_CUTOFF_SECONDS}" =~ ^[0-9]+$ ]]          || { mlog "[ERROR] HARD_CUTOFF_SECONDS must be an integer"; exit 1; }
[[ "${SAFETY_FACTOR}" =~ ^[0-9]+([.][0-9]+)?$ ]]    || { mlog "[ERROR] SAFETY_FACTOR must be a number"; exit 1; }
[[ "${MEMORY_GUARD_LOOKAHEAD_INTERVALS}" -gt 0 ]]   || { mlog "[ERROR] MEMORY_GUARD_LOOKAHEAD_INTERVALS must be > 0"; exit 1; }
[[ "${MEMORY_GUARD_PERSISTENCE_SAMPLES}" -gt 0 ]]   || { mlog "[ERROR] MEMORY_GUARD_PERSISTENCE_SAMPLES must be > 0"; exit 1; }
[[ "${MEMORY_RATE_WINDOW}" -ge 2 ]]                 || { mlog "[ERROR] MEMORY_RATE_WINDOW must be >= 2"; exit 1; }
[[ "${ELAPSED_STEP_WINDOW}" -ge 1 ]]                || { mlog "[ERROR] ELAPSED_STEP_WINDOW must be >= 1"; exit 1; }
[[ "${MONITOR_SETTLE_TIME}" =~ ^[0-9]+$ ]]          || { mlog "[ERROR] MONITOR_SETTLE_TIME must be an integer"; exit 1; }
[[ "${MEMORY_GUARD_HARD_STOP_FRAC}" =~ ^[0-9]+([.][0-9]+)?$ ]] || { mlog "[ERROR] MEMORY_GUARD_HARD_STOP_FRAC must be a number"; exit 1; }
# Validate that the fraction is between 0 and 1
HARD_STOP_CHECK=$(echo "${MEMORY_GUARD_HARD_STOP_FRAC} > 0 && ${MEMORY_GUARD_HARD_STOP_FRAC} <= 1" | bc 2>/dev/null)
[[ "${HARD_STOP_CHECK}" -eq 1 ]] || { mlog "[ERROR] MEMORY_GUARD_HARD_STOP_FRAC must be between 0 and 1 (e.g., 0.97 for 97%)"; exit 1; }
case "${MEMORY_GUARD_SCOPE}" in
    auto|node|core|cpu) ;;
    *) mlog "[ERROR] MEMORY_GUARD_SCOPE must be one of: auto, node, core, cpu"; exit 1 ;;
esac
[[ "${MEMORY_GUARD_UTILIZATION}" =~ ^[0-9]+([.][0-9]+)?$ ]] || { mlog "[ERROR] MEMORY_GUARD_UTILIZATION must be a positive number"; exit 1; }
UTIL_RANGE_CHECK=$(echo \
    "${MEMORY_GUARD_UTILIZATION} > 0 && \
     ${MEMORY_GUARD_UTILIZATION} < 1 && \
     ${MEMORY_GUARD_UTILIZATION} < ${MEMORY_GUARD_HARD_STOP_FRAC}" \
    | bc 2>/dev/null)
[[ "${UTIL_RANGE_CHECK}" -eq 1 ]] || { mlog "[ERROR] MEMORY_GUARD_UTILIZATION must be in (0,1) and < MEMORY_GUARD_HARD_STOP_FRAC"; exit 1; }

WALL_TIME_SECONDS=$(parse_slurm_time_to_seconds "${WALL_TIME_STR}") || {
    echo "[ERROR] Failed to parse wall-time string: ${WALL_TIME_STR}" >&2
    exit 1
}

parse_slurm_time_to_seconds "${STACK_DUMP_TIME_LIMIT}" > /dev/null || {
    mlog "[ERROR] STACK_DUMP_TIME_LIMIT is not a valid Slurm time string: ${STACK_DUMP_TIME_LIMIT}"
    exit 1
}

# Resolve memory guard limits
MEMORY_GUARD_NODE_LIMIT_RESOLVED_KB=0
MEMORY_GUARD_EFFECTIVE_LIMIT_KB=0
MEMORY_GUARD_COMPARE_LIMIT_KB=0
MEMORY_GUARD_TRIGGER_KB=0
MEMORY_GUARD_HARD_STOP_PCT=0
MEMORY_GUARD_SCOPE_RESOLVED="node"
MEMORY_GUARD_SCOPE_SOURCE="disabled"
MEMORY_GUARD_CPUS_PER_NODE=0
MEMORY_GUARD_TASKS_PER_NODE=0

if [[ "${MEMORY_GUARD_ENABLED}" -eq 1 ]]; then
    CGROUP_LIMIT_KB=0
    USER_NODE_LIMIT_KB=0
    SLURM_NODE_LIMIT_KB=0
    SLURM_REQMEM_NODE_KB=0

    if [[ -n "${MEMORY_GUARD_NODE_LIMIT_GB:-}" ]]; then
        USER_NODE_LIMIT_KB="$(gb_to_kb "${MEMORY_GUARD_NODE_LIMIT_GB}")" || {
            mlog "[ERROR] Invalid MEMORY_GUARD_NODE_LIMIT_GB='${MEMORY_GUARD_NODE_LIMIT_GB}'"
            exit 1
        }
    fi

    CGROUP_LIMIT_KB="$(get_job_cgroup_memory_limit_kb || echo 0)"
    if [[ "${CGROUP_LIMIT_KB}" =~ ^[0-9]+$ ]] && [[ "${CGROUP_LIMIT_KB}" -gt 0 ]]; then
        if [[ "${USER_NODE_LIMIT_KB}" -gt 0 ]]; then
            MEMORY_GUARD_EFFECTIVE_LIMIT_KB="$(min_positive \
                "${CGROUP_LIMIT_KB}" \
                "${USER_NODE_LIMIT_KB}")"
        else
            MEMORY_GUARD_EFFECTIVE_LIMIT_KB="${CGROUP_LIMIT_KB}"
        fi
        MEMORY_GUARD_SCOPE_RESOLVED="cgroup"
        MEMORY_GUARD_SCOPE_SOURCE="runtime-cgroup"
    fi

    FIRST_NODE="$(get_first_allocated_node || true)"
    if [[ -n "${FIRST_NODE}" ]]; then
        SLURM_NODE_LIMIT_KB="$(get_node_realmemory_kb "${FIRST_NODE}" || echo 0)"
    fi

    SLURM_REQMEM_NODE_KB="$(get_job_reqmem_kb_per_node || echo 0)"

    MEMORY_GUARD_NODE_LIMIT_RESOLVED_KB="$(min_positive \
        "${USER_NODE_LIMIT_KB}" \
        "${SLURM_NODE_LIMIT_KB}" \
        "${SLURM_REQMEM_NODE_KB}")"

    if [[ "${MEMORY_GUARD_EFFECTIVE_LIMIT_KB}" -le 0 ]]; then
        MEMORY_GUARD_SCOPE_RESOLVED="$(classify_memory_guard_scope || echo node)"
        MEMORY_GUARD_SCOPE_SOURCE="site-policy"
        MEMORY_GUARD_EFFECTIVE_LIMIT_KB="${MEMORY_GUARD_NODE_LIMIT_RESOLVED_KB}"
    fi

    if [[ "${MEMORY_GUARD_EFFECTIVE_LIMIT_KB}" =~ ^[0-9]+$ ]] && \
       [[ "${MEMORY_GUARD_EFFECTIVE_LIMIT_KB}" -gt 0 ]]; then
        MEMORY_GUARD_COMPARE_LIMIT_KB="${MEMORY_GUARD_EFFECTIVE_LIMIT_KB}"
        if [[ "${MEMORY_GUARD_SCOPE_RESOLVED}" == "node" ]]; then
            MEMORY_GUARD_TASKS_PER_NODE="$(get_job_tasks_per_node_count || echo 0)"
            if [[ "${MEMORY_GUARD_TASKS_PER_NODE}" =~ ^[0-9]+$ ]] && \
               [[ "${MEMORY_GUARD_TASKS_PER_NODE}" -gt 0 ]]; then
                MEMORY_GUARD_COMPARE_LIMIT_KB=$(awk \
                    -v lim="${MEMORY_GUARD_EFFECTIVE_LIMIT_KB}" \
                    -v denom="${MEMORY_GUARD_TASKS_PER_NODE}" \
                    'BEGIN { printf "%.0f\n", lim / denom }')
            fi
        elif [[ "${MEMORY_GUARD_SCOPE_RESOLVED}" == "core" || \
                "${MEMORY_GUARD_SCOPE_RESOLVED}" == "cpu" ]]; then
            MEMORY_GUARD_CPUS_PER_NODE="$(get_job_cpus_per_node_count || echo 0)"
            if [[ "${MEMORY_GUARD_CPUS_PER_NODE}" =~ ^[0-9]+$ ]] && \
               [[ "${MEMORY_GUARD_CPUS_PER_NODE}" -gt 0 ]]; then
                CPUS_PER_TASK="${SLURM_CPUS_PER_TASK:-1}"
                [[ "${CPUS_PER_TASK}" =~ ^[0-9]+$ ]] || CPUS_PER_TASK=1
                MEMORY_GUARD_COMPARE_LIMIT_KB=$(awk \
                    -v lim="${MEMORY_GUARD_EFFECTIVE_LIMIT_KB}" \
                    -v cpus_per_node="${MEMORY_GUARD_CPUS_PER_NODE}" \
                    -v cpus_per_task="${CPUS_PER_TASK}" \
                    'BEGIN { printf "%.0f\n", lim / cpus_per_node * cpus_per_task }')
            else
                MEMORY_GUARD_SCOPE_RESOLVED="node"
                MEMORY_GUARD_SCOPE_SOURCE="fallback-node"
                MEMORY_GUARD_TASKS_PER_NODE="$(get_job_tasks_per_node_count || echo 0)"
                if [[ "${MEMORY_GUARD_TASKS_PER_NODE}" =~ ^[0-9]+$ ]] && \
                   [[ "${MEMORY_GUARD_TASKS_PER_NODE}" -gt 0 ]]; then
                    MEMORY_GUARD_COMPARE_LIMIT_KB=$(awk \
                        -v lim="${MEMORY_GUARD_EFFECTIVE_LIMIT_KB}" \
                        -v denom="${MEMORY_GUARD_TASKS_PER_NODE}" \
                        'BEGIN { printf "%.0f\n", lim / denom }')
                fi
            fi
        fi
        MEMORY_GUARD_TRIGGER_KB=$(awk \
            -v lim="${MEMORY_GUARD_COMPARE_LIMIT_KB}" \
            -v frac="${MEMORY_GUARD_UTILIZATION}" \
            'BEGIN { printf "%.0f\n", lim * frac }')

        MEMORY_GUARD_HARD_STOP_PCT=$(awk -v frac="${MEMORY_GUARD_HARD_STOP_FRAC}" \
            'BEGIN { printf "%.0f", frac * 100 }')
    else
        MEMORY_GUARD_ENABLED=0
        MEMORY_GUARD_SCOPE_SOURCE="disabled"
    fi
fi

# ==============================================================================
#  PARSE AND VALIDATE INPUTS
# ==============================================================================

PRIMARY_INPUTFILE=$(parse_namelist_value "${MAIN_INPUTFILE}" "primary_inputfile")
validate_nonempty "${PRIMARY_INPUTFILE}" "primary_inputfile"

PRECURSOR_INPUTFILE=$(parse_namelist_value "${MAIN_INPUTFILE}" "precursor_inputfile")
validate_nonempty "${PRECURSOR_INPUTFILE}" "precursor_inputfile"

[[ -f "${PRIMARY_INPUTFILE}" ]]   || { mlog "[ERROR] Primary input not found: ${PRIMARY_INPUTFILE}"; exit 1; }
[[ -f "${PRECURSOR_INPUTFILE}" ]] || { mlog "[ERROR] Precursor input not found: ${PRECURSOR_INPUTFILE}"; exit 1; }

PRIMARY_RUNID=$(parse_namelist_value "${PRIMARY_INPUTFILE}" "RunID")
validate_nonempty "${PRIMARY_RUNID}" "primary RunID"
validate_integer  "${PRIMARY_RUNID}" "primary RunID"

PRECURSOR_RUNID=$(parse_namelist_value "${PRECURSOR_INPUTFILE}" "RunID")
validate_nonempty "${PRECURSOR_RUNID}" "precursor RunID"
validate_integer  "${PRECURSOR_RUNID}" "precursor RunID"

PRIMARY_INPUTDIR=$(parse_namelist_value "${PRIMARY_INPUTFILE}" "inputdir")
validate_nonempty "${PRIMARY_INPUTDIR}" "primary inputdir"

PRECURSOR_INPUTDIR=$(parse_namelist_value "${PRECURSOR_INPUTFILE}" "inputdir")
validate_nonempty "${PRECURSOR_INPUTDIR}" "precursor inputdir"

[[ -d "${PRIMARY_INPUTDIR}" ]]   || { mlog "[ERROR] Primary inputdir not found: ${PRIMARY_INPUTDIR}"; exit 1; }
[[ -d "${PRECURSOR_INPUTDIR}" ]] || { mlog "[ERROR] Precursor inputdir not found: ${PRECURSOR_INPUTDIR}"; exit 1; }

T_RESTART_DUMP=$(parse_namelist_value "${PRIMARY_INPUTFILE}" "t_restartDump")
validate_nonempty "${T_RESTART_DUMP}" "t_restartDump"
validate_integer  "${T_RESTART_DUMP}" "t_restartDump"

PRIMARY_RID_PAD=$(printf "%02d" "${PRIMARY_RUNID}")
PRECURSOR_RID_PAD=$(printf "%02d" "${PRECURSOR_RUNID}")

mlog "================================================================"
mlog "Job ID          : ${SLURM_JOB_ID}"
mlog "Config file     : ${CONFIG_ABS}"
mlog "Nodes           : ${SLURM_NNODES}"
mlog "Tasks total     : ${SLURM_NTASKS}"
mlog "Tasks per node  : ${SLURM_TASKS_PER_NODE}"
mlog "CPUs per task   : ${SLURM_CPUS_PER_TASK}"
mlog "Wall time limit : ${WALL_TIME_STR}  (${WALL_TIME_SECONDS}s)"
mlog "Hard cutoff     : ${HARD_CUTOFF_SECONDS}s remaining"
mlog "Frozen timeout  : ${FROZEN_TIMEOUT_SECONDS}s"
mlog "Stack dump enabled  : ${STACK_DUMP_ENABLED}"
mlog "Stack dump limit    : ${STACK_DUMP_TIME_LIMIT}"
mlog "GDB timeout/rank    : ${STACK_DUMP_GDB_TIMEOUT_SECONDS}s"
mlog "Estimate persist: ${ESTIMATE_PERSISTENCE_SAMPLES} samples"
mlog "Safety factor   : ${SAFETY_FACTOR}x"
mlog "Memory guard    : ${MEMORY_GUARD_ENABLED}"
mlog "Mem lookahead   : ${MEMORY_GUARD_LOOKAHEAD_INTERVALS} intervals"
mlog "Mem util frac   : ${MEMORY_GUARD_UTILIZATION}"
MEMORY_GUARD_HARD_STOP_PCT_DISPLAY=$(awk -v frac="${MEMORY_GUARD_HARD_STOP_FRAC}" 'BEGIN { printf "%.0f", frac * 100 }')
mlog "Mem hard stop   : ${MEMORY_GUARD_HARD_STOP_FRAC} (${MEMORY_GUARD_HARD_STOP_PCT_DISPLAY}%)"
mlog "Mem persist     : ${MEMORY_GUARD_PERSISTENCE_SAMPLES} samples"
mlog "Mem rate window : ${MEMORY_RATE_WINDOW} samples"
mlog "Mem user limit/node : ${MEMORY_GUARD_NODE_LIMIT_GB:-unset} GB (${USER_NODE_LIMIT_KB:-0} KB)"
mlog "Mem slurm real/node : ${SLURM_NODE_LIMIT_KB:-0} KB"
mlog "Mem slurm req/node  : ${SLURM_REQMEM_NODE_KB:-0} KB"
mlog "Mem scope       : ${MEMORY_GUARD_SCOPE_RESOLVED} (${MEMORY_GUARD_SCOPE_SOURCE})"
mlog "Mem cpus/node   : ${MEMORY_GUARD_CPUS_PER_NODE}"
mlog "Mem tasks/node   : ${MEMORY_GUARD_TASKS_PER_NODE}"
mlog "Mem limit/node  : ${MEMORY_GUARD_NODE_LIMIT_RESOLVED_KB} KB"
mlog "Mem limit/effective : ${MEMORY_GUARD_EFFECTIVE_LIMIT_KB} KB"
mlog "Mem limit/compare : ${MEMORY_GUARD_COMPARE_LIMIT_KB} KB"
mlog "Mem trigger     : ${MEMORY_GUARD_TRIGGER_KB} KB"
mlog "Mem CSV             : ${MEM_CSV}"
mlog "Monitor log         : ${MONITOR_LOG}"
mlog "Monitor settle time : ${MONITOR_SETTLE_TIME}s"
mlog "Script path     : ${THIS_SCRIPT}"
mlog "Primary input   : ${PRIMARY_INPUTFILE}"
mlog "Precursor input : ${PRECURSOR_INPUTFILE}"
mlog "Primary dir     : ${PRIMARY_INPUTDIR}"
mlog "Precursor dir   : ${PRECURSOR_INPUTDIR}"
mlog "t_restartDump   : ${T_RESTART_DUMP} steps"
mlog "Primary RunID   : ${PRIMARY_RUNID} (${PRIMARY_RID_PAD})"
mlog "Precursor RunID : ${PRECURSOR_RUNID} (${PRECURSOR_RID_PAD})"
mlog "Restart lock    : ${RESTART_LOCKFILE}"
mlog "Output log      : ${OUTPUT_LOG}"
mlog "================================================================"
mlog "Job startup complete."


# ==============================================================================
#  INPUT UPDATE HELPERS
# ==============================================================================

# Extract a complete namelist section from a Fortran input file
# More robust - handles trailing comments and extra whitespace
parse_namelist_section() {
    local file="$1"
    local namelist="$2"
    
    if [[ ! -f "${file}" ]]; then
        return 1
    fi
    
    awk -v nl="${namelist}" '
        BEGIN { in_section = 0; IGNORECASE = 1 }
        $0 ~ ("^[[:space:]]*&" nl "([[:space:]]|!|$)") { in_section = 1; next }
        in_section && $0 ~ /^[[:space:]]*\// { exit }
        in_section { print }
    ' "${file}"
}

# Parse a value from a namelist section (passed as string)
parse_budget_value() {
    local section="$1"
    local key="$2"
    
    echo "${section}" | awk -v key="${key}" '
        BEGIN { IGNORECASE = 1 }
        $0 ~ ("^[[:space:]]*" key "[[:space:]]*=") {
            sub(/^[[:space:]]*[^=]*=[[:space:]]*/, "")
            gsub(/[[:space:]]/, "")
            gsub(/["\047]/, "")  # Remove quotes
            sub(/!.*/, "")       # Remove comments
            sub(/,$/, "")        # Remove trailing comma
            print
            exit
        }
    '
}

# Get expected field counts for budget types
get_budget_deficit_compact_field_count() {
    local budget_num="$1"
    case "${budget_num}" in
        0) echo 20 ;;
        1) echo 15 ;;
        2) echo 15 ;;
        3) echo 19 ;;
        *) echo 0 ;;
    esac
}

# Find ALL valid (tidx,counter) pairs for a deficit compact budget
find_all_deficit_compact_budget_states() {
    local budget_dir="$1"
    local rid_pad="$2"
    local budget_num="$3"
    local max_tidx="$4"
    
    local field_count=$(get_budget_deficit_compact_field_count "${budget_num}")
    [[ "${field_count}" -eq 0 ]] && return 1
    
    local pattern="Run${rid_pad}_comp_deficit_budget${budget_num}_term*_t??????_n??????.s3D"
    
    # Get unique TIDXs, sorted descending
    local tidx_list=$(find "${budget_dir}" -maxdepth 1 -name "${pattern}" 2>/dev/null \
        | sed 's/.*_t\([0-9]\{6\}\)_n.*/\1/' \
        | sort -rn \
        | uniq)
    
    [[ -z "${tidx_list}" ]] && return 1
    
    # Find all complete (tidx,counter) pairs
    while IFS= read -r tidx; do
        local tidx_num=$((10#${tidx}))
        
        # Safety: must be less than or equal to the latest simulation TIDX
        [[ "${tidx_num}" -gt "${max_tidx}" ]] && continue
        
        # Count files for this TIDX
        local file_count=$(find "${budget_dir}" -maxdepth 1 \
            -name "Run${rid_pad}_comp_deficit_budget${budget_num}_term??_t${tidx}_n??????.s3D" \
            2>/dev/null | wc -l)
        
        if [[ "${file_count}" -eq "${field_count}" ]]; then
            local counter=$(find "${budget_dir}" -maxdepth 1 \
                -name "Run${rid_pad}_comp_deficit_budget${budget_num}_term*_t${tidx}_n??????.s3D" \
                2>/dev/null | head -1 | sed 's/.*_n\([0-9]\{6\}\)\.s3D/\1/')
            
            local counter_num=$((10#${counter}))
            echo "${tidx_num},${counter_num}"
        fi
    done <<< "${tidx_list}"
}

# Find ALL valid (tidx,counter) pairs for time avg budget
find_all_time_avg_budget_states() {
    local budget_dir="$1"
    local rid_pad="$2"
    local budget_type="$3"
    local max_tidx="$4"
    
    # For budgetType=0, check budget0 with terms 1-16,26,31
    if [[ "${budget_type}" == "0" ]]; then
        local required_terms="1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 26 31"
        local pattern="Run${rid_pad}_budget0_term??_t??????_n??????.s3D"
        
        local tidx_list=$(find "${budget_dir}" -maxdepth 1 -name "${pattern}" 2>/dev/null \
            | sed 's/.*_t\([0-9]\{6\}\)_n.*/\1/' \
            | sort -rn \
            | uniq)
        
        [[ -z "${tidx_list}" ]] && return 1
        
        while IFS= read -r tidx; do
            local tidx_num=$((10#${tidx}))
            
            [[ "${tidx_num}" -gt "${max_tidx}" ]] && continue
            
            local all_present=1
            for term in ${required_terms}; do
                local term_pad=$(printf "%02d" "${term}")
                local file_pattern="Run${rid_pad}_budget0_term${term_pad}_t${tidx}_n??????.s3D"
                local file_count=$(find "${budget_dir}" -maxdepth 1 -name "${file_pattern}" 2>/dev/null | wc -l)
                
                if [[ "${file_count}" -eq 0 ]]; then
                    all_present=0
                    break
                fi
            done
            
            if [[ "${all_present}" -eq 1 ]]; then
                local counter=$(find "${budget_dir}" -maxdepth 1 \
                    -name "Run${rid_pad}_budget0_term??_t${tidx}_n??????.s3D" \
                    2>/dev/null | head -1 | sed 's/.*_n\([0-9]\{6\}\)\.s3D/\1/')
                
                local counter_num=$((10#${counter}))
                echo "${tidx_num},${counter_num}"
            fi
        done <<< "${tidx_list}"
    fi
}

# Find common (tidx,counter) from multiple budget state lists
find_common_budget_state() {
    local -n state_lists=$1  # Array of state strings (one per budget)
    
    [[ "${#state_lists[@]}" -eq 0 ]] && return 1
    
    # If only one budget, return its latest state
    if [[ "${#state_lists[@]}" -eq 1 ]]; then
        echo "${state_lists[0]}" | head -1
        return 0
    fi
    
    # Find intersection of all state lists
    local common_states="${state_lists[0]}"
    
    for ((i=1; i<${#state_lists[@]}; i++)); do
        common_states=$(comm -12 \
            <(echo "${common_states}" | sort) \
            <(echo "${state_lists[i]}" | sort))
        
        [[ -z "${common_states}" ]] && return 1
    done
    
    # Return the latest (first in descending order)
    echo "${common_states}" | sort -t, -k1,1rn -k2,2rn | head -1
}

# Explicitly disable budget restart in a namelist
disable_budget_restart() {
    local file="$1"
    local namelist="$2"
    
    set_namelist_value_in_section "${file}" "${namelist}" "restart_budgets" ".false." || return 1
    set_namelist_value_in_section "${file}" "${namelist}" "restart_rid" "0" || return 1
    set_namelist_value_in_section "${file}" "${namelist}" "restart_tid" "0" || return 1
    set_namelist_value_in_section "${file}" "${namelist}" "restart_counter" "0" || return 1
    
    return 0
}

# Update budget restart settings in a namelist
update_budget_namelist() {
    local file="$1"
    local namelist="$2"
    local restart_rid="$3"
    local restart_tid="$4"
    local restart_counter="$5"
    
    set_namelist_value_in_section "${file}" "${namelist}" "restart_budgets" ".true." || return 1
    set_namelist_value_in_section "${file}" "${namelist}" "restart_rid" "${restart_rid}" || return 1
    set_namelist_value_in_section "${file}" "${namelist}" "restart_tid" "${restart_tid}" || return 1
    set_namelist_value_in_section "${file}" "${namelist}" "restart_counter" "${restart_counter}" || return 1
    
    return 0
}

# Set value in specific namelist section
set_namelist_value_in_section() {
    local file="$1"
    local namelist="$2"
    local key="$3"
    local new_val="$4"
    local tmpfile="${file}.tmp.$$"
    
    awk -v nl="${namelist}" -v key="${key}" -v val="${new_val}" '
    BEGIN { in_section = 0; found = 0; section_seen = 0; IGNORECASE = 1 }
    
    $0 ~ ("^[[:space:]]*&" nl "([[:space:]]|!|$)") {
        in_section = 1
        section_seen = 1
        print
        next
    }
    
    in_section && $0 ~ /^[[:space:]]*\// {
        in_section = 0
        if (!found) {
            print "    " key " = " val
        }
        print
        next
    }
    
    in_section && tolower($0) ~ ("^[[:space:]]*" tolower(key) "[[:space:]]*=") {
        match($0, /^[[:space:]]*/)
        indent = substr($0, 1, RLENGTH)
        print indent key " = " val
        found = 1
        next
    }
    
    { print }
    
    END {
        if (!section_seen) {
            print "[ERROR] Namelist section &" nl " not found in file" > "/dev/stderr"
            exit 2
        }
    }
    ' "${file}" > "${tmpfile}"
    
    local awk_exit=$?
    if [[ ${awk_exit} -ne 0 ]]; then
        rm -f "${tmpfile}"
        return 1
    fi
    
    mv "${tmpfile}" "${file}"
}

# Resolve budget directory path
# - Empty or unset: defaults to CONFIG_DIR (where job script is located)
# - Set: used as-is (expected to be absolute path)
resolve_budget_dir() {
    local budget_dir="$1"
    
    # If empty, use CONFIG_DIR
    if [[ -z "${budget_dir}" ]]; then
        echo "${CONFIG_DIR}"
        return 0
    fi
    
    # Otherwise return as-is (should be absolute)
    echo "${budget_dir}"
}

# Process budget restarts for a single input file
# Mode: "discover" = find constraints, return min TIDX via stdout, don't update files
#       "update"   = update files with budget restarts, return nothing
# Returns 0 on success, 1 on failure (in update mode)
# Note: In discover mode, the return value is via stdout (command substitution).
#       All diagnostics use mlog() to avoid stdout pollution.
process_budget_restarts_for_file() {
    local input_file="$1"
    local file_label="$2"
    local runid="$3"
    local rid_pad="$4"
    local max_tidx="$5"
    local mode="${6:-discover}"  # "discover" or "update"
    
    local min_budget_tidx=""
    local update_failed=0
    
    if [[ "${mode}" == "discover" ]]; then
        mlog "[Budget][${file_label}] Discovering budget constraints (max_tidx=${max_tidx})..."
    else
        mlog "[Budget][${file_label}] Updating budget restart settings (max_tidx=${max_tidx})..."
    fi
    
    # Process BUDGET_TIME_AVG_DEFICIT_COMPACT
    local section=$(parse_namelist_section "${input_file}" "BUDGET_TIME_AVG_DEFICIT_COMPACT")
    if [[ -n "${section}" ]]; then
        local do_budgets=$(parse_budget_value "${section}" "do_budgets")
        
        if [[ "${do_budgets}" == ".true." || "${do_budgets}" == ".TRUE." ]]; then
            if [[ "${mode}" == "discover" ]]; then
                mlog "[Budget][${file_label}] BUDGET_TIME_AVG_DEFICIT_COMPACT is active"
            fi
            
            local budget_dir_raw=$(parse_budget_value "${section}" "budgets_dir")
            local budget_dir=$(resolve_budget_dir "${budget_dir_raw}")
            
            if [[ ! -d "${budget_dir}" ]]; then
                mlog "[Budget][${file_label}] WARNING: DEFICIT_COMPACT budget directory not found: ${budget_dir}"
                if [[ "${mode}" == "update" ]]; then
                    mlog "[Budget][${file_label}] Disabling DEFICIT_COMPACT budget restart"
                    disable_budget_restart "${input_file}" "BUDGET_TIME_AVG_DEFICIT_COMPACT" || {
                        mlog "[Budget][${file_label}] ERROR: Failed to disable DEFICIT_COMPACT budget restart"
                        update_failed=1
                    }
                fi
            else
                # Find active budgets
                local active_budgets=()
                for i in 0 1 2 3; do
                    local do_budget=$(parse_budget_value "${section}" "do_budget${i}")
                    if [[ "${do_budget}" == ".true." || "${do_budget}" == ".TRUE." ]]; then
                        active_budgets+=("${i}")
                    fi
                done
                
                if [[ "${#active_budgets[@]}" -eq 0 ]]; then
                    if [[ "${mode}" == "discover" ]]; then
                        mlog "[Budget][${file_label}] No active DEFICIT_COMPACT budgets"
                    fi
                    if [[ "${mode}" == "update" ]]; then
                        disable_budget_restart "${input_file}" "BUDGET_TIME_AVG_DEFICIT_COMPACT" || {
                            mlog "[Budget][${file_label}] ERROR: Failed to disable DEFICIT_COMPACT budget restart"
                            update_failed=1
                        }
                    fi
                else
                    if [[ "${mode}" == "discover" ]]; then
                        mlog "[Budget][${file_label}] Active DEFICIT_COMPACT budgets: ${active_budgets[*]}"
                    fi
                    
                    # Get all valid states for each active budget
                    local -a all_states
                    local valid_budgets=0
                    
                    for budget_num in "${active_budgets[@]}"; do
                        local states=$(find_all_deficit_compact_budget_states "${budget_dir}" "${rid_pad}" "${budget_num}" "${max_tidx}")
                        
                        if [[ -n "${states}" ]]; then
                            all_states+=("${states}")
                            valid_budgets=$((valid_budgets + 1))
                            if [[ "${mode}" == "discover" ]]; then
                                mlog "[Budget][${file_label}] Budget${budget_num}: found $(echo "${states}" | wc -l) valid state(s)"
                            fi
                        else
                            if [[ "${mode}" == "discover" ]]; then
                                mlog "[Budget][${file_label}] WARNING: No valid states for budget${budget_num}"
                            fi
                        fi
                    done
                    
                    # Find common state across all active budgets
                    if [[ "${valid_budgets}" -eq "${#active_budgets[@]}" ]]; then
                        local common_state=$(find_common_budget_state all_states)
                        
                        if [[ -n "${common_state}" ]]; then
                            local tidx="${common_state%%,*}"
                            local counter="${common_state##*,}"
                            
                            mlog "[Budget][${file_label}] DEFICIT_COMPACT common state: TIDX=${tidx}, counter=${counter}"
                            
                            if [[ "${mode}" == "update" ]]; then
                                update_budget_namelist "${input_file}" "BUDGET_TIME_AVG_DEFICIT_COMPACT" \
                                    "${runid}" "${tidx}" "${counter}" || {
                                    mlog "[Budget][${file_label}] ERROR: Failed to update DEFICIT_COMPACT budget restart"
                                    update_failed=1
                                }
                            fi
                            
                            # Track minimum budget TIDX
                            if [[ -z "${min_budget_tidx}" || "${tidx}" -lt "${min_budget_tidx}" ]]; then
                                min_budget_tidx="${tidx}"
                            fi
                        else
                            mlog "[Budget][${file_label}] WARNING: No common state found for DEFICIT_COMPACT budgets"
                            if [[ "${mode}" == "update" ]]; then
                                mlog "[Budget][${file_label}] Disabling DEFICIT_COMPACT budget restart"
                                disable_budget_restart "${input_file}" "BUDGET_TIME_AVG_DEFICIT_COMPACT" || {
                                    mlog "[Budget][${file_label}] ERROR: Failed to disable DEFICIT_COMPACT budget restart"
                                    update_failed=1
                                }
                            fi
                        fi
                    else
                        mlog "[Budget][${file_label}] WARNING: Not all DEFICIT_COMPACT budgets have valid states"
                        if [[ "${mode}" == "update" ]]; then
                            mlog "[Budget][${file_label}] Disabling DEFICIT_COMPACT budget restart"
                            disable_budget_restart "${input_file}" "BUDGET_TIME_AVG_DEFICIT_COMPACT" || {
                                mlog "[Budget][${file_label}] ERROR: Failed to disable DEFICIT_COMPACT budget restart"
                                update_failed=1
                            }
                        fi
                    fi
                fi
            fi
        fi
    fi
    
    # Process BUDGET_TIME_AVG
    section=$(parse_namelist_section "${input_file}" "BUDGET_TIME_AVG")
    if [[ -n "${section}" ]]; then
        local do_budgets=$(parse_budget_value "${section}" "do_budgets")
        
        if [[ "${do_budgets}" == ".true." || "${do_budgets}" == ".TRUE." ]]; then
            if [[ "${mode}" == "discover" ]]; then
                mlog "[Budget][${file_label}] BUDGET_TIME_AVG is active"
            fi
            
            local budget_type=$(parse_budget_value "${section}" "budgetType")
            local squeeze=$(parse_budget_value "${section}" "squeeze")
            local budget_dir_raw=$(parse_budget_value "${section}" "budgets_dir")
            local budget_dir=$(resolve_budget_dir "${budget_dir_raw}")
            
            if [[ "${mode}" == "discover" ]]; then
                mlog "[Budget][${file_label}] budgetType=${budget_type}, squeeze=${squeeze}"
            fi
            
            if [[ ! -d "${budget_dir}" ]]; then
                mlog "[Budget][${file_label}] WARNING: TIME_AVG budget directory not found: ${budget_dir}"
                if [[ "${mode}" == "update" ]]; then
                    mlog "[Budget][${file_label}] Disabling TIME_AVG budget restart"
                    disable_budget_restart "${input_file}" "BUDGET_TIME_AVG" || {
                        mlog "[Budget][${file_label}] ERROR: Failed to disable TIME_AVG budget restart"
                        update_failed=1
                    }
                fi
            elif [[ "${budget_type}" == "0" && ("${squeeze}" == ".true." || "${squeeze}" == ".TRUE.") ]]; then
                local states=$(find_all_time_avg_budget_states "${budget_dir}" "${rid_pad}" "${budget_type}" "${max_tidx}")
                
                if [[ -n "${states}" ]]; then
                    # Get latest state
                    local latest_state=$(echo "${states}" | sort -t, -k1,1rn -k2,2rn | head -1)
                    local tidx="${latest_state%%,*}"
                    local counter="${latest_state##*,}"
                    
                    mlog "[Budget][${file_label}] TIME_AVG: TIDX=${tidx}, counter=${counter}"
                    
                    if [[ "${mode}" == "update" ]]; then
                        update_budget_namelist "${input_file}" "BUDGET_TIME_AVG" \
                            "${runid}" "${tidx}" "${counter}" || {
                            mlog "[Budget][${file_label}] ERROR: Failed to update TIME_AVG budget restart"
                            update_failed=1
                        }
                    fi
                    
                    # Track minimum budget TIDX
                    if [[ -z "${min_budget_tidx}" || "${tidx}" -lt "${min_budget_tidx}" ]]; then
                        min_budget_tidx="${tidx}"
                    fi
                else
                    mlog "[Budget][${file_label}] WARNING: No valid TIME_AVG budget states found"
                    if [[ "${mode}" == "update" ]]; then
                        mlog "[Budget][${file_label}] Disabling TIME_AVG budget restart"
                        disable_budget_restart "${input_file}" "BUDGET_TIME_AVG" || {
                            mlog "[Budget][${file_label}] ERROR: Failed to disable TIME_AVG budget restart"
                            update_failed=1
                        }
                    fi
                fi
            else
                if [[ "${mode}" == "discover" ]]; then
                    mlog "[Budget][${file_label}] Skipping TIME_AVG restart (unsupported budgetType or squeeze setting)"
                fi
                if [[ "${mode}" == "update" ]]; then
                    disable_budget_restart "${input_file}" "BUDGET_TIME_AVG" || {
                        mlog "[Budget][${file_label}] ERROR: Failed to disable TIME_AVG budget restart"
                        update_failed=1
                    }
                fi
            fi
        fi
    fi
    
    # Return appropriately based on mode
    if [[ "${mode}" == "discover" ]]; then
        # Return the minimum budget TIDX found (stdout is used as return channel)
        echo "${min_budget_tidx}"
        return 0
    else
        # Update mode: fail if any update operation failed
        if [[ "${update_failed}" -ne 0 ]]; then
            mlog "[Budget][${file_label}] Budget update FAILED - aborting restart sequence"
            return 1
        fi
        return 0
    fi
}

_update_input_files() {
    local common_restart_tid="$1"

    mlog "[Update] Ensuring original input backups exist (suffix: .bak_job${SLURM_JOB_ID})"

    [[ -f "${PRIMARY_INPUTFILE}.bak_job${SLURM_JOB_ID}" ]] || \
        cp "${PRIMARY_INPUTFILE}" "${PRIMARY_INPUTFILE}.bak_job${SLURM_JOB_ID}" || return 1

    [[ -f "${PRECURSOR_INPUTFILE}.bak_job${SLURM_JOB_ID}" ]] || \
        cp "${PRECURSOR_INPUTFILE}" "${PRECURSOR_INPUTFILE}.bak_job${SLURM_JOB_ID}" || return 1

    mlog "================================================================"
    mlog "[Update] Phase 1: Finding restart constraints from all systems..."
    mlog "================================================================"
    mlog "[Update] Restart files: common TID=${common_restart_tid}"
    
    # PHASE 1: Discover budget constraints (but don't update files yet)
    local primary_min_budget=$(process_budget_restarts_for_file \
        "${PRIMARY_INPUTFILE}" "PRIMARY" "${PRIMARY_RUNID}" "${PRIMARY_RID_PAD}" "${common_restart_tid}" "discover")
    
    local precursor_min_budget=$(process_budget_restarts_for_file \
        "${PRECURSOR_INPUTFILE}" "PRECURSOR" "${PRECURSOR_RUNID}" "${PRECURSOR_RID_PAD}" "${common_restart_tid}" "discover")
    
    # Compute absolute minimum across restart files and all budgets
    local final_restart_tid="${common_restart_tid}"
    
    if [[ -n "${primary_min_budget}" && "${primary_min_budget}" -lt "${final_restart_tid}" ]]; then
        mlog "[Update] PRIMARY budgets constrain restart to TIDX=${primary_min_budget}"
        final_restart_tid="${primary_min_budget}"
    fi
    
    if [[ -n "${precursor_min_budget}" && "${precursor_min_budget}" -lt "${final_restart_tid}" ]]; then
        mlog "[Update] PRECURSOR budgets constrain restart to TIDX=${precursor_min_budget}"
        final_restart_tid="${precursor_min_budget}"
    fi
    
    mlog "================================================================"
    mlog "[Update] Final common restart point: TIDX=${final_restart_tid}"
    mlog "================================================================"
    
    # PHASE 2: Update restart files with final common TIDX
    mlog "[Update] Phase 2: Writing restart file settings..."
    set_namelist_value "${PRIMARY_INPUTFILE}"   "useRestartFile"   ".true."             || return 1
    set_namelist_value "${PRIMARY_INPUTFILE}"   "restartFile_TID"  "${final_restart_tid}"             || return 1
    set_namelist_value "${PRIMARY_INPUTFILE}"   "restartFile_RID"  "${PRIMARY_RUNID}"   || return 1

    set_namelist_value "${PRECURSOR_INPUTFILE}" "useRestartFile"   ".true."             || return 1
    set_namelist_value "${PRECURSOR_INPUTFILE}" "restartFile_TID"  "${final_restart_tid}"             || return 1
    set_namelist_value "${PRECURSOR_INPUTFILE}" "restartFile_RID"  "${PRECURSOR_RUNID}" || return 1

    mlog "[Update] PRIMARY   — verified restart fields:"
    grep -iE 'useRestartFile|restartFile_TID|restartFile_RID' "${PRIMARY_INPUTFILE}" >> "${MONITOR_LOG}"
    mlog "[Update] PRECURSOR — verified restart fields:"
    grep -iE 'useRestartFile|restartFile_TID|restartFile_RID' "${PRECURSOR_INPUTFILE}" >> "${MONITOR_LOG}"
    
    # PHASE 3: Re-process budgets with final restart TID to ensure consistency
    mlog "================================================================"
    mlog "[Update] Phase 3: Writing budget restart settings (using final TIDX=${final_restart_tid})..."
    mlog "================================================================"
    
    # These must succeed or restart sequence aborts
    process_budget_restarts_for_file \
        "${PRIMARY_INPUTFILE}" "PRIMARY" "${PRIMARY_RUNID}" "${PRIMARY_RID_PAD}" "${final_restart_tid}" "update" >/dev/null || {
        mlog "[Update] CRITICAL ERROR: PRIMARY budget update failed - aborting restart"
        return 1
    }
    
    process_budget_restarts_for_file \
        "${PRECURSOR_INPUTFILE}" "PRECURSOR" "${PRECURSOR_RUNID}" "${PRECURSOR_RID_PAD}" "${final_restart_tid}" "update" >/dev/null || {
        mlog "[Update] CRITICAL ERROR: PRECURSOR budget update failed - aborting restart"
        return 1
    }
    
    mlog "[Update] All restart configurations complete and synchronized."
}

_restore_input_files() {
    mlog "[Restore] Reverting input files to pre-edit state..."
    cp "${PRIMARY_INPUTFILE}.bak_job${SLURM_JOB_ID}"   "${PRIMARY_INPUTFILE}"   2>/dev/null || true
    cp "${PRECURSOR_INPUTFILE}.bak_job${SLURM_JOB_ID}" "${PRECURSOR_INPUTFILE}" 2>/dev/null || true
    mlog "[Restore] Done."
}

# ==============================================================================
#  SIGNAL TRAP
# ==============================================================================

CHILD_SUBMITTED=0
CHILD_JOB_ID=""

emergency_resubmit() {
    local reason="${1:-SIGTERM}"
    local child_id submit_rc
    local emerg_tid=""

    # Do not let multiple trigger paths run restart logic simultaneously
    if ! acquire_restart_lock; then
        mlog "[Emergency] Restart lock already held. Another restart path is active. Exiting handler."
        return 0
    fi

    mlog "[Emergency] Triggered by ${reason}."
    mlog "[Emergency] Locating latest common restart TID..."

    emerg_tid="$(latest_common_restart_tid \
        "${PRIMARY_INPUTDIR}"   "${PRIMARY_RID_PAD}" \
        "${PRECURSOR_INPUTDIR}" "${PRECURSOR_RID_PAD}" || true)"

    if [[ -z "${emerg_tid}" || ! "${emerg_tid}" =~ ^[0-9]+$ ]]; then
        mlog "[Emergency] ERROR: No valid common restart TID found."
        mlog "[Emergency] Cannot prepare restart inputs safely."

        # Best effort shutdown of MPI before exiting
        if [[ -n "${MPI_PID:-}" ]] && kill -0 "${MPI_PID}" 2>/dev/null; then
            mlog "[Emergency] Terminating MPI PID ${MPI_PID}."
            kill -TERM "${MPI_PID}" 2>/dev/null || true
            wait "${MPI_PID}" 2>/dev/null || true
        fi

        release_restart_lock
        exit 1
    fi

    mlog "[Emergency] Common restart TID: ${emerg_tid}"
    mlog "[Emergency] Preparing input files for restart..."

    if ! _update_input_files "${emerg_tid}"; then
        mlog "[Emergency] ERROR: _update_input_files failed for TID ${emerg_tid}."
        mlog "[Emergency] Any downstream job could start from stale inputs. Aborting emergency handoff."

        if [[ -n "${MPI_PID:-}" ]] && kill -0 "${MPI_PID}" 2>/dev/null; then
            mlog "[Emergency] Terminating MPI PID ${MPI_PID}."
            kill -TERM "${MPI_PID}" 2>/dev/null || true
            wait "${MPI_PID}" 2>/dev/null || true
        fi

        release_restart_lock
        exit 1
    fi

    mlog "[Emergency] Input files prepared successfully."
    mlog "[Emergency] Attempting child submission..."

    child_id="$(submit_with_retry none "" 3 10)"
    submit_rc=$?

    if [[ "${submit_rc}" -eq 0 && -n "${child_id}" && "${child_id}" =~ ^[0-9]+$ ]]; then
        mlog "[Emergency] Child submitted successfully: ${child_id}"

        if [[ -n "${MPI_PID:-}" ]] && kill -0 "${MPI_PID}" 2>/dev/null; then
            mlog "[Emergency] Terminating MPI PID ${MPI_PID}."
            kill -TERM "${MPI_PID}" 2>/dev/null || true
            wait "${MPI_PID}" 2>/dev/null || true
        fi

        release_restart_lock
        exit 0
    fi

    mlog "[Emergency] Child submission failed after 3 attempts."
    mlog "[Emergency] Falling back to staged handoff."

    printf 'jobid=%s\nreason=%s\ntime=%s\n' \
        "${SLURM_JOB_ID}" "${reason}" "$(date '+%F %T')" \
        > "${RESTART_READY_FILE}"
    sync || true

    if [[ -n "${MPI_PID:-}" ]] && kill -0 "${MPI_PID}" 2>/dev/null; then
        mlog "[Emergency] Terminating MPI PID ${MPI_PID}."
        kill -TERM "${MPI_PID}" 2>/dev/null || true
        wait "${MPI_PID}" 2>/dev/null || true
    fi

    release_restart_lock
    mlog "[Emergency] Exiting current job after staged handoff."
    exit 0
}

trap emergency_resubmit TERM
trap cleanup_watchers EXIT

# ==============================================================================
#  LAUNCH
# ==============================================================================

# Write CSV header
echo "timestamp,epoch,jobid,step,maxrss(KB),averss(KB),maxvmsize(KB),avevmsize(KB)" > "${MEM_CSV}"

if [[ -f "${SIM_DONE_FILE}" ]]; then
    mlog "SIM_DONE marker found. Exiting without launching MPI."
    exit 0
fi

if [[ -f "${RESTART_READY_FILE}" ]]; then
    mlog "RESTART_READY marker found. Continuing with staged restart inputs."
    rm -f "${RESTART_READY_FILE}"
fi

mlog "Launching MPI simulation..."
run_job

if [[ -z "${MPI_PID:-}" || ! "${MPI_PID}" =~ ^[0-9]+$ ]]; then
    mlog "[ERROR] run_job() did not set a valid MPI_PID."
    exit 1
fi

JOB_START_EPOCH=$(date +%s)

mlog "MPI PID         : ${MPI_PID}"
mlog "Clock started."

# Initial settling period
if [[ "${MONITOR_SETTLE_TIME}" -gt 0 ]]; then
    mlog "================================================================"
    mlog "Waiting ${MONITOR_SETTLE_TIME}s for simulation to settle before monitoring starts..."
    mlog "================================================================"
    sleep "${MONITOR_SETTLE_TIME}"
    mlog "Settling period complete. Starting active monitoring."
fi

# ==============================================================================
#  MONITOR LOOP
# ==============================================================================

LAST_TIDX=0
LAST_TIDX_EPOCH=0
ELAPSED_PER_STEP=""
ELAPSED_PER_STEP_VALID=0

OUTPUT_LOG_SEEN=0
LAST_LOG_MTIME=0
LAST_PROGRESS_EPOCH=0

ESTIMATE_BAD_SAMPLES=0
ESTIMATE_LAST_BAD=0

MEMORY_BAD_SAMPLES=0
declare -a MEM_EPOCH_HISTORY=()
declare -a MEM_RSS_HISTORY=()

STACK_DUMP_DONE=0

ELAPSED_SOURCE=""

mlog "Monitor loop started. Polling '${OUTPUT_LOG}' every ${MONITOR_INTERVAL}s."
mlog "================================================================"

while kill -0 "${MPI_PID}" 2>/dev/null; do

    sleep "${MONITOR_INTERVAL}"

    if ! kill -0 "${MPI_PID}" 2>/dev/null; then
        mlog "[Monitor] MPI process finished normally."
        break
    fi

    NOW_EPOCH=$(date +%s)
    TRIGGER_REASON=""
    CURRENT_TIDX=""

    # ------------------------------------------------------------------
    #  MEMORY SAMPLING — one sstat call per cycle feeds CSV + guard logic
    # ------------------------------------------------------------------

    MEM_LOOKAHEAD_SECONDS=$(( MEMORY_GUARD_LOOKAHEAD_INTERVALS * MONITOR_INTERVAL ))

    if sample_memory_sstat; then
        CURRENT_MAXRSS_KB="${SSTAT_MAXRSS_KB}"

        # Calculate utilization percentage
        MEM_UTIL_PCT=0
        if [[ "${MEMORY_GUARD_ENABLED}" -eq 1 && "${MEMORY_GUARD_COMPARE_LIMIT_KB}" -gt 0 ]]; then
            MEM_UTIL_PCT=$(awk -v rss="${CURRENT_MAXRSS_KB}" -v lim="${MEMORY_GUARD_COMPARE_LIMIT_KB}" \
                'BEGIN { printf "%.0f", (rss / lim) * 100 }')
        fi

        if [[ "${MEMORY_GUARD_ENABLED}" -eq 1 && "${MEMORY_GUARD_TRIGGER_KB}" -gt 0 ]]; then

            # HARD STOP: immediate restart at critical utilization (no persistence required)
            # Convert fraction to percentage for comparison
            if [[ "${MEM_UTIL_PCT}" -ge "${MEMORY_GUARD_HARD_STOP_PCT}" ]]; then
                mlog "[Monitor][Mem] rss=${CURRENT_MAXRSS_KB}KB (${MEM_UTIL_PCT}%) | scope=${MEMORY_GUARD_SCOPE_RESOLVED} | cmp=${MEMORY_GUARD_COMPARE_LIMIT_KB}KB | trig=${MEMORY_GUARD_TRIGGER_KB}KB | STATUS: CRITICAL_HARD_STOP"
                TRIGGER_REASON="MEMORY GUARD HARD STOP (MaxRSS at ${MEM_UTIL_PCT}% >= ${MEMORY_GUARD_HARD_STOP_PCT}% critical threshold)"
                
            # Hard backstop: current RSS already at/above trigger
            elif [[ "${CURRENT_MAXRSS_KB}" -ge "${MEMORY_GUARD_TRIGGER_KB}" ]]; then
                MEMORY_BAD_SAMPLES=$(( MEMORY_BAD_SAMPLES + 1 ))
                mlog "[Monitor][Mem] rss=${CURRENT_MAXRSS_KB}KB (${MEM_UTIL_PCT}%) | scope=${MEMORY_GUARD_SCOPE_RESOLVED} | cmp=${MEMORY_GUARD_COMPARE_LIMIT_KB}KB | trig=${MEMORY_GUARD_TRIGGER_KB}KB | STATUS: AT_LIMIT bad=${MEMORY_BAD_SAMPLES}/${MEMORY_GUARD_PERSISTENCE_SAMPLES}"
                if [[ "${MEMORY_BAD_SAMPLES}" -ge "${MEMORY_GUARD_PERSISTENCE_SAMPLES}" ]]; then
                    TRIGGER_REASON="MEMORY GUARD (MaxRSS ${CURRENT_MAXRSS_KB} KB >= trigger ${MEMORY_GUARD_TRIGGER_KB} KB for ${MEMORY_BAD_SAMPLES} consecutive samples)"
                fi

            # Projection path
            else
                push_mem_sample "${NOW_EPOCH}" "${CURRENT_MAXRSS_KB}"
                MEM_RATE_KBPS="$(average_mem_rate_kbps || true)"

                if [[ -n "${MEM_RATE_KBPS}" ]]; then
                    if [[ "$(echo "${MEM_RATE_KBPS} <= 0" | bc 2>/dev/null)" -eq 1 ]]; then
                        [[ "${MEMORY_BAD_SAMPLES}" -gt 0 ]] && MEMORY_BAD_SAMPLES=0
                        mlog "[Monitor][Mem] rss=${CURRENT_MAXRSS_KB}KB (${MEM_UTIL_PCT}%) | scope=${MEMORY_GUARD_SCOPE_RESOLVED} | cmp=${MEMORY_GUARD_COMPARE_LIMIT_KB}KB | trig=${MEMORY_GUARD_TRIGGER_KB}KB | rate=${MEM_RATE_KBPS}KB/s | STATUS: DECREASING"
                    else
                        MEM_HEADROOM_KB=$(( MEMORY_GUARD_TRIGGER_KB - CURRENT_MAXRSS_KB ))
                        MEM_TIME_TO_LIMIT=$(echo "scale=0; ${MEM_HEADROOM_KB} / ${MEM_RATE_KBPS}" | bc 2>/dev/null)

                        MEM_UNSAFE=0
                        if [[ -n "${MEM_TIME_TO_LIMIT}" ]]; then
                            MEM_UNSAFE=$(echo "${MEM_TIME_TO_LIMIT} < ${MEM_LOOKAHEAD_SECONDS}" | bc 2>/dev/null)
                            MEM_UNSAFE="${MEM_UNSAFE:-0}"
                        fi

                        if [[ "${MEM_UNSAFE}" -eq 1 ]]; then
                            MEMORY_BAD_SAMPLES=$(( MEMORY_BAD_SAMPLES + 1 ))
                            mlog "[Monitor][Mem] rss=${CURRENT_MAXRSS_KB}KB (${MEM_UTIL_PCT}%) | scope=${MEMORY_GUARD_SCOPE_RESOLVED} | cmp=${MEMORY_GUARD_COMPARE_LIMIT_KB}KB | trig=${MEMORY_GUARD_TRIGGER_KB}KB | rate=${MEM_RATE_KBPS}KB/s | ttl=${MEM_TIME_TO_LIMIT}s < look=${MEM_LOOKAHEAD_SECONDS}s | STATUS: UNSAFE bad=${MEMORY_BAD_SAMPLES}/${MEMORY_GUARD_PERSISTENCE_SAMPLES}"
                            if [[ "${MEMORY_BAD_SAMPLES}" -ge "${MEMORY_GUARD_PERSISTENCE_SAMPLES}" ]]; then
                                TRIGGER_REASON="MEMORY GUARD (MaxRSS projected to reach ${MEMORY_GUARD_TRIGGER_KB} KB in ${MEM_TIME_TO_LIMIT}s < ${MEM_LOOKAHEAD_SECONDS}s, persisted for ${MEMORY_BAD_SAMPLES} samples)"
                            fi
                        else
                            [[ "${MEMORY_BAD_SAMPLES}" -gt 0 ]] && MEMORY_BAD_SAMPLES=0
                            mlog "[Monitor][Mem] rss=${CURRENT_MAXRSS_KB}KB (${MEM_UTIL_PCT}%) | scope=${MEMORY_GUARD_SCOPE_RESOLVED} | cmp=${MEMORY_GUARD_COMPARE_LIMIT_KB}KB | trig=${MEMORY_GUARD_TRIGGER_KB}KB | rate=${MEM_RATE_KBPS}KB/s | ttl=${MEM_TIME_TO_LIMIT}s > look=${MEM_LOOKAHEAD_SECONDS}s | STATUS: SAFE"
                        fi
                    fi
                else
                    [[ "${MEMORY_BAD_SAMPLES}" -gt 0 ]] && MEMORY_BAD_SAMPLES=0
                    mlog "[Monitor][Mem] rss=${CURRENT_MAXRSS_KB}KB (${MEM_UTIL_PCT}%) | scope=${MEMORY_GUARD_SCOPE_RESOLVED} | cmp=${MEMORY_GUARD_COMPARE_LIMIT_KB}KB | trig=${MEMORY_GUARD_TRIGGER_KB}KB | STATUS: WARMING (need 2+ samples)"
                fi
            fi
        else
            # Memory guard disabled - just log the sample
            mlog "[Monitor][Mem] rss=${CURRENT_MAXRSS_KB}KB | STATUS: GUARD_DISABLED"
        fi

    else
        # sstat returned nothing — job step may not be registered yet
        [[ "${MEMORY_BAD_SAMPLES}" -gt 0 ]] && MEMORY_BAD_SAMPLES=0
        MEM_EPOCH_HISTORY=()
        MEM_RSS_HISTORY=()
        mlog "[Monitor][Mem] STATUS: NO_DATA (job step not registered yet)"
    fi

    # ------------------------------------------------------------------
    #  OUTPUT LOG PROGRESS TRACKING
    # ------------------------------------------------------------------

    if [[ ! -f "${OUTPUT_LOG}" ]]; then
        mlog "[Monitor] Output log not yet visible, waiting..."
        continue
    fi

    CURRENT_LOG_MTIME=$(get_file_mtime_epoch "${OUTPUT_LOG}")

    if [[ "${OUTPUT_LOG_SEEN}" -eq 0 ]]; then
        OUTPUT_LOG_SEEN=1
        LAST_LOG_MTIME="${CURRENT_LOG_MTIME}"
        LAST_PROGRESS_EPOCH="${NOW_EPOCH}"
    fi

    if [[ "${CURRENT_LOG_MTIME}" -gt "${LAST_LOG_MTIME}" ]]; then
        LAST_LOG_MTIME="${CURRENT_LOG_MTIME}"
        LAST_PROGRESS_EPOCH="${NOW_EPOCH}"
    fi

    CURRENT_TIDX_RAW=$(tail -n 100 "${OUTPUT_LOG}" | grep 'TIDX:' | tail -1 | awk '{print $NF}')

    if [[ -n "${CURRENT_TIDX_RAW}" && "${CURRENT_TIDX_RAW}" =~ ^[0-9]+$ ]]; then
        CURRENT_TIDX=$(( 10#${CURRENT_TIDX_RAW} ))
    fi

    STEP_DELTA=0
    ELAPSED_PER_STEP_VALID=0
    ELAPSED_SOURCE=""

    if [[ -n "${CURRENT_TIDX}" && "${CURRENT_TIDX}" -gt "${LAST_TIDX}" ]]; then
        STEP_DELTA=$(( CURRENT_TIDX - LAST_TIDX ))

        LOGGED_ELAPSED_AVG="$(get_recent_logged_elapsed_step_average "${OUTPUT_LOG}" || true)"
        if [[ -n "${LOGGED_ELAPSED_AVG}" ]]; then
            ELAPSED_PER_STEP="${LOGGED_ELAPSED_AVG}"
            ELAPSED_PER_STEP_VALID=1
            ELAPSED_SOURCE="log_avg"
        fi

        if [[ "${ELAPSED_PER_STEP_VALID}" -eq 0 ]]; then
            global_wall_time=$(( NOW_EPOCH - LAST_TIDX_EPOCH ))
            if [[ "${LAST_TIDX_EPOCH}" -gt 0 && "${STEP_DELTA}" -gt 0 && "${global_wall_time}" -gt 0 ]]; then
                ELAPSED_PER_STEP=$(echo "scale=2; ${global_wall_time} / ${STEP_DELTA}" | bc 2>/dev/null)
                [[ -n "${ELAPSED_PER_STEP}" ]] && ELAPSED_PER_STEP_VALID=1
                ELAPSED_SOURCE="wall"
            fi
        fi

        LAST_PROGRESS_EPOCH="${NOW_EPOCH}"
    fi

    TIME_USED=$(( NOW_EPOCH - JOB_START_EPOCH ))
    TIME_LEFT=$(( WALL_TIME_SECONDS - TIME_USED ))

    IDLE_FOR=0
    if [[ "${LAST_PROGRESS_EPOCH}" -gt 0 ]]; then
        IDLE_FOR=$(( NOW_EPOCH - LAST_PROGRESS_EPOCH ))
    fi

    # ------------------------------------------------------------------
    #  TRIGGER EVALUATION
    # ------------------------------------------------------------------

    if [[ -z "${TRIGGER_REASON}" && "${LAST_PROGRESS_EPOCH}" -gt 0 && "${IDLE_FOR}" -ge "${FROZEN_TIMEOUT_SECONDS}" ]]; then
        TRIGGER_REASON="FROZEN (no log/TIDX progress for ${IDLE_FOR}s >= ${FROZEN_TIMEOUT_SECONDS}s)"

    elif [[ -z "${TRIGGER_REASON}" && "${TIME_LEFT}" -le "${HARD_CUTOFF_SECONDS}" ]]; then
        TRIGGER_REASON="HARD CUTOFF (${TIME_LEFT}s left <= threshold ${HARD_CUTOFF_SECONDS}s)"

    elif [[ -z "${TRIGGER_REASON}" && "${ELAPSED_PER_STEP_VALID}" -eq 1 && -n "${CURRENT_TIDX}" ]]; then
        STEPS_SINCE_DUMP=$(( CURRENT_TIDX % T_RESTART_DUMP ))
        STEPS_TO_DUMP=$(( T_RESTART_DUMP - STEPS_SINCE_DUMP ))
        TIME_NEEDED=$(echo "scale=2; ${SAFETY_FACTOR} * ${STEPS_TO_DUMP} * ${ELAPSED_PER_STEP}" | bc 2>/dev/null)

        [[ -z "${TIME_NEEDED}" ]] && { mlog "[Monitor][Time] WARNING: bc failed to compute TIME_NEEDED, skipping."; continue; }

        # Format time per step for cleaner display
        TIME_PER_STEP_FMT=$(echo "scale=1; ${ELAPSED_PER_STEP}" | bc 2>/dev/null)
        TIME_NEEDED_FMT=$(echo "scale=0; ${TIME_NEEDED}" | bc 2>/dev/null)

        NOT_ENOUGH=$(echo "${TIME_LEFT} < ${TIME_NEEDED}" | bc 2>/dev/null)
        NOT_ENOUGH="${NOT_ENOUGH:-0}"

        if [[ "${NOT_ENOUGH}" -eq 1 ]]; then
            ESTIMATE_BAD_SAMPLES=$(( ESTIMATE_BAD_SAMPLES + 1 ))
            ESTIMATE_LAST_BAD=1
            mlog "[Monitor][Time] TIDX=${CURRENT_TIDX} | left=${TIME_LEFT}s < need=${TIME_NEEDED_FMT}s | dump_in=${STEPS_TO_DUMP} @ ${TIME_PER_STEP_FMT}s/step | STATUS: UNSAFE bad=${ESTIMATE_BAD_SAMPLES}/${ESTIMATE_PERSISTENCE_SAMPLES} | idle=${IDLE_FOR}s"
            if [[ "${ESTIMATE_BAD_SAMPLES}" -ge "${ESTIMATE_PERSISTENCE_SAMPLES}" ]]; then
                TRIGGER_REASON="ESTIMATE persisted for ${ESTIMATE_BAD_SAMPLES} consecutive samples (${TIME_LEFT}s left < ${TIME_NEEDED}s needed)"
            fi
        else
            if [[ "${ESTIMATE_LAST_BAD}" -eq 1 || "${ESTIMATE_BAD_SAMPLES}" -gt 0 ]]; then
                mlog "[Monitor][Time] TIDX=${CURRENT_TIDX} | left=${TIME_LEFT}s > need=${TIME_NEEDED_FMT}s | dump_in=${STEPS_TO_DUMP} @ ${TIME_PER_STEP_FMT}s/step | STATUS: RECOVERED | idle=${IDLE_FOR}s"
                ESTIMATE_BAD_SAMPLES=0
                ESTIMATE_LAST_BAD=0
            else
                mlog "[Monitor][Time] TIDX=${CURRENT_TIDX} | left=${TIME_LEFT}s > need=${TIME_NEEDED_FMT}s | dump_in=${STEPS_TO_DUMP} @ ${TIME_PER_STEP_FMT}s/step | STATUS: SAFE | idle=${IDLE_FOR}s"
                ESTIMATE_BAD_SAMPLES=0
                ESTIMATE_LAST_BAD=0
            fi
        fi
    else
        # No valid estimate yet
        if [[ -n "${CURRENT_TIDX}" ]]; then
            mlog "[Monitor][Time] TIDX=${CURRENT_TIDX} | left=${TIME_LEFT}s | STATUS: WARMING (insufficient progress data) | idle=${IDLE_FOR}s"
        else
            mlog "[Monitor][Time] TIDX=unknown | left=${TIME_LEFT}s | STATUS: WARMING (insufficient progress data) | idle=${IDLE_FOR}s"
        fi
    fi

    if [[ -n "${CURRENT_TIDX}" && "${CURRENT_TIDX}" -gt "${LAST_TIDX}" ]]; then
        LAST_TIDX="${CURRENT_TIDX}"
        LAST_TIDX_EPOCH="${NOW_EPOCH}"
    fi

    # ------------------------------------------------------------------
    #  RESTART ACTION
    # ------------------------------------------------------------------

    if [[ -n "${TRIGGER_REASON}" && "${CHILD_SUBMITTED}" -eq 0 ]]; then
        mlog "================================================================"
        mlog "[Monitor] Restart trigger: ${TRIGGER_REASON}"
        mlog "================================================================"

        trap '' TERM
        if ! acquire_restart_lock; then
            trap 'emergency_resubmit TERM' TERM
            mlog "[Monitor] Restart lock already held. Another path is handling restart."
            continue
        fi
        trap 'emergency_resubmit TERM' TERM

        if [[ "${TRIGGER_REASON}" == FROZEN* ]]; then
            if [[ "${STACK_DUMP_DONE}" -eq 0 ]]; then
                mlog "[Monitor] Frozen trigger detected. Attempting MPI stack dump before restart sequence."

                dump_mpi_stacks_for_frozen_job "${TRIGGER_REASON}" || \
                    mlog "[Monitor] WARNING: MPI stack dump failed or was incomplete; continuing restart sequence."

                STACK_DUMP_DONE=1
            else
                mlog "[Monitor] Frozen trigger detected, but stack dump was already attempted for this job. Skipping repeated dump."
            fi
        fi

        # from here onward, this monitor path owns the restart sequence
        # compute COMMON_TID
        # update input files
        # try submit_with_retry
        # on failure -> stage_restart_handoff_and_exit
        # on success  -> terminate MPI and exit

        COMMON_TID_RAW=$(latest_common_restart_tid \
            "${PRIMARY_INPUTDIR}"   "${PRIMARY_RID_PAD}" \
            "${PRECURSOR_INPUTDIR}" "${PRECURSOR_RID_PAD}")

        if [[ -z "${COMMON_TID_RAW}" ]]; then
            mlog "[Monitor] ERROR: No common restart TID found."
            mlog "  Primary   dir : ${PRIMARY_INPUTDIR}  (RID ${PRIMARY_RID_PAD})"
            mlog "  Precursor dir : ${PRECURSOR_INPUTDIR}  (RID ${PRECURSOR_RID_PAD})"
            mlog "[Monitor] Will retry next cycle."
            release_restart_lock
            continue
        fi

        COMMON_TID=$(( 10#${COMMON_TID_RAW} ))
        mlog "[Monitor] Common restart TID: ${COMMON_TID}"

        if ! _update_input_files "${COMMON_TID}"; then
            mlog "[Monitor] ERROR: Input file update failed. Restoring backups and retrying next cycle."
            _restore_input_files
            release_restart_lock
            continue
        fi

        CHILD_JOB_ID="$(submit_with_retry afterany "${SLURM_JOB_ID}" 3 10 || true)"

        if [[ -z "${CHILD_JOB_ID}" || ! "${CHILD_JOB_ID}" =~ ^[0-9]+$ ]]; then
            mlog "[Monitor] sbatch failed after retries (compute-node submission blocked?)."
            mlog "[Monitor] Input files are already prepared. Falling back to staged handoff."
            stage_restart_handoff_and_exit "sbatch_unavailable_from_compute_node"
            # Does not return — stage_restart_handoff_and_exit kills MPI and exits
        fi

        mlog "[Monitor] Child job queued: ID=${CHILD_JOB_ID} (depends on ${SLURM_JOB_ID})"
        CHILD_SUBMITTED=1

        mlog "[Monitor] Terminating MPI launcher (PID=${MPI_PID})..."
        trap - TERM
        kill "${MPI_PID}" 2>/dev/null || true
        wait "${MPI_PID}" 2>/dev/null || true
        mlog "[Monitor] MPI launcher terminated."
        break
    fi

done

trap - TERM
cleanup_watchers
release_restart_lock

wait "${MPI_PID}" 2>/dev/null || true
MPI_EXIT=$?

mlog "================================================================"
if [[ "${CHILD_SUBMITTED}" -eq 1 ]]; then
    mlog "Status : Restarted — child job ID=${CHILD_JOB_ID} queued."
else
    mlog "Status : Simulation finished (MPI exit code ${MPI_EXIT})."
fi
mlog "================================================================"
