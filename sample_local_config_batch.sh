#!/bin/bash
#SBATCH --nodes=56
#SBATCH --partition=compute
#SBATCH --ntasks-per-node=64
#SBATCH --cpus-per-task=1
#SBATCH --exclusive
#SBATCH --time=12:00:00
#SBATCH --job-name=example_case
#SBATCH --output=output_.o%j
#SBATCH --error=error_.e%j
#SBATCH --mail-user=user@example.edu
#SBATCH --mail-type=all

# Path to the main input file that points to the coupled / case-specific inputs
MAIN_INPUTFILE="/path/to/case_directory/input_main.dat"

# Path to the solver executable
SOLVER="/path/to/solver_binary"

# Path to an environment setup script (modules, paths, compiler/MPI environment, etc.)
SOURCEDFILE="/path/to/setup_environment.sh"

# Path to the shared master monitor script
MASTER_MONITOR_SCRIPT="/path/to/master_monitor.sh"

# Polling interval for the main monitor loop (seconds).
# This controls:
#   - progress checks,
#   - frozen-job detection cadence,
#   - in-loop memory sampling cadence,
#   - memory-growth projection cadence.
MONITOR_INTERVAL=60

# If remaining wall time drops below this threshold, force a restart.
HARD_CUTOFF_SECONDS=300

# Safety factor applied to projected time needed to reach the next restart dump.
#   time_remaining < SAFETY_FACTOR * (steps_to_next_dump * wall_seconds_per_step)
SAFETY_FACTOR=1.2

# Frozen-job timeout, in seconds. If the output log shows no update / progress
# for longer than this, the run is treated as stalled and a restart is triggered.
FROZEN_TIMEOUT_SECONDS=7200

# Number of consecutive unsafe runtime estimates required before triggering a restart.
# This helps avoid reacting to isolated long steps or transient noise.
ESTIMATE_PERSISTENCE_SAMPLES=10

# Number of recent logged elapsed-step values to average when estimating
# wall-seconds per step from the solver output log.
ELAPSED_STEP_WINDOW=5

# Enable proactive memory-based restart logic.
# 1 = enabled, 0 = disabled
MEMORY_GUARD_ENABLED=1

# Per-node memory limit in GB used by the guard.
# The master script resolves the effective node limit conservatively using the
# minimum of:
#   - this user-provided value,
#   - Slurm node RealMemory,
#   - Slurm requested memory per node (if available).
MEMORY_GUARD_NODE_LIMIT_GB=256

# Trigger fraction of the resolved per-node limit.
# Example: 0.95 means trigger logic is based on 95% of the node limit.
MEMORY_GUARD_UTILIZATION=0.95

# Restart if projected MaxRSS would reach the trigger within this many future
# monitor intervals.
# Example: with MONITOR_INTERVAL=60 and LOOKAHEAD_INTERVALS=3, the guard asks
# whether the trigger could be reached within the next 180 seconds.
MEMORY_GUARD_LOOKAHEAD_INTERVALS=3

# Number of consecutive unsafe memory projections required before restart.
MEMORY_GUARD_PERSISTENCE_SAMPLES=3

# Number of recent memory samples used to estimate memory growth rate.
MEMORY_RATE_WINDOW=4

# Define how the solver is launched on the local system.
# Examples:
#   mpirun -np "${SLURM_NTASKS}"           # common on many Slurm clusters
#   ibrun --distribution=block:block       # common on some TACC systems
#
# Keep "${solver}", "${inputFile}", and "MPI_PID=$!" unchanged unless you also
# update the monitor logic that depends on them.
run_job() {
    mpirun -np "${SLURM_NTASKS}" "${solver}" "${inputFile}" &
    MPI_PID=$!
}

# Start the master monitoring script
source "${MASTER_MONITOR_SCRIPT}"
