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

# ==============================================================================
#  USER CONFIGURATION
#  Adjust these variables for each new simulation chain.
# ==============================================================================

# Path to the main input file that points to the coupled / case-specific inputs
MAIN_INPUTFILE="/path/to/case_directory/input_main.dat"

# Path to the solver executable
SOLVER="/path/to/solver_binary"

# Path to an environment setup script (modules, paths, compiler/MPI environment, etc.)
SOURCEDFILE="/path/to/setup_environment.sh"

# Path to the shared master monitor script body sourced by this local job script
MASTER_MONITOR_SCRIPT="/path/to/master_monitor.sh"

# Safety margin used in the restart-time estimate:
#   time_remaining < SAFETY_FACTOR * (steps_to_next_dump * wall_seconds_per_step)
# 1.0 = aggressive, 2.0 = safer, larger values = more conservative
SAFETY_FACTOR=1.5

# How often, in seconds, the monitor loop checks runtime progress
MONITOR_INTERVAL=60

# How often, in seconds, memory usage is sampled for early OOM detection
MEMWATCH_INTERVAL=300

# Hard wall-time cutoff, in seconds. If remaining wall time drops below this
# threshold, the script forces a restart regardless of the runtime estimate.
# This should be comfortably larger than MONITOR_INTERVAL.
HARD_CUTOFF_SECONDS=600

# Frozen-job timeout, in seconds. If the output log shows no update / progress
# for longer than this, the run is treated as stalled and a restart is triggered.
FROZEN_TIMEOUT_SECONDS=7200

# Number of consecutive unsafe runtime estimates required before triggering a restart.
# This helps avoid reacting to isolated long steps or transient noise.
ESTIMATE_PERSISTENCE_SAMPLES=10

# Number of recent logged elapsed-step values to average when estimating
# wall-seconds per step from the solver output log.
ELAPSED_STEP_WINDOW=5

# ==============================================================================
#  MEMORY GUARD SETTINGS
# ==============================================================================

# Enable or disable memory-based restart protection
MEMORY_GUARD_ENABLED=1

# If projected memory usage is expected to reach the configured threshold within
# this many monitor intervals, the memory guard triggers a restart.
MEMORY_GUARD_LOOKAHEAD_INTERVALS=2

# Fraction of the inferred per-node memory limit at which the run is considered unsafe.
# 1.0 means "at the limit"; 0.95 means "95% of the limit".
MEMORY_GUARD_UTILIZATION=0.9

# Optional explicit per-node memory limit override, in GB.
# This can be useful if automatic limit detection is unavailable or unreliable.
MEMORY_GUARD_NODE_LIMIT_GB=256

# Number of consecutive unsafe memory projections required before restarting.
MEMORY_GUARD_PERSISTENCE_SAMPLES=2

# Number of recent memory samples used to estimate memory growth rate.
MEMORY_RATE_WINDOW=4

# ==============================================================================
#  PLATFORM-SPECIFIC LAUNCHER
# ==============================================================================

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

# ==============================================================================
#  END OF USER CONFIGURATION
#  The remainder of the script typically does not need to be modified.
# ==============================================================================

JOB_CONFIG_ABS="$(realpath "${BASH_SOURCE[0]}")"
JOB_CONFIG_DIR="$(dirname "${JOB_CONFIG_ABS}")"
JOB_CONFIG_BASE="$(basename "${JOB_CONFIG_ABS}")"

source "${MASTER_MONITOR_SCRIPT}"
